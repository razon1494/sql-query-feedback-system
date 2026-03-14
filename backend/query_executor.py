"""
Query Executor — Safely runs student/base SQL queries against SQLite databases.
Handles timeouts, safety checks, result comparison, and counterexample detection.
"""
import sqlite3
import os
import re
import time
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field

DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'database')
MAIN_DB = os.path.join(DB_DIR, 'main.db')
EDGE_DB = os.path.join(DB_DIR, 'edge_{}.db')
MAX_ROWS = 500
TIMEOUT_SECONDS = 5


@dataclass
class ExecutionResult:
    success: bool
    rows: List[Dict] = field(default_factory=list)
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    error: Optional[str] = None
    execution_time_ms: float = 0.0
    db_name: str = "main"

    def to_dict(self):
        return {
            "success": self.success,
            "rows": self.rows,
            "columns": self.columns,
            "row_count": self.row_count,
            "error": self.error,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "db_name": self.db_name,
        }


@dataclass
class ComparisonResult:
    are_equivalent: bool
    missing_rows: List[Dict] = field(default_factory=list)   # in base, not in student
    extra_rows: List[Dict] = field(default_factory=list)     # in student, not in base
    matching_rows: List[Dict] = field(default_factory=list)
    base_count: int = 0
    student_count: int = 0
    jaccard_similarity: float = 0.0
    is_subset: bool = False    # student ⊆ base
    is_superset: bool = False  # student ⊇ base

    def to_dict(self):
        return {
            "are_equivalent": self.are_equivalent,
            "missing_rows": self.missing_rows,
            "extra_rows": self.extra_rows,
            "matching_rows": self.matching_rows,
            "base_count": self.base_count,
            "student_count": self.student_count,
            "jaccard_similarity": round(self.jaccard_similarity, 3),
            "is_subset": self.is_subset,
            "is_superset": self.is_superset,
        }


# ══════════════════════════════════════════════════════════════════════
#  SAFETY CHECKS
# ══════════════════════════════════════════════════════════════════════

FORBIDDEN_PATTERNS = [
    r'\bDROP\b', r'\bDELETE\b', r'\bINSERT\b', r'\bUPDATE\b',
    r'\bALTER\b', r'\bCREATE\b', r'\bTRUNCATE\b', r'\bEXEC\b',
    r'\bATTACH\b', r'\bDETACH\b', r'\bPRAGMA\b',
]

def is_safe_query(sql: str) -> Tuple[bool, Optional[str]]:
    """Check if query is safe to execute (SELECT only)."""
    clean = sql.strip().upper()
    if not clean.startswith('SELECT'):
        return False, "Only SELECT queries are allowed."
    for pat in FORBIDDEN_PATTERNS:
        if re.search(pat, clean):
            word = re.search(pat, clean).group()
            return False, f"Forbidden keyword detected: {word}"
    return True, None


# ══════════════════════════════════════════════════════════════════════
#  EXECUTOR
# ══════════════════════════════════════════════════════════════════════

def execute_query(sql: str, db_path: str = None, db_name: str = "main") -> ExecutionResult:
    """Execute a SQL query safely against the specified database."""
    if db_path is None:
        db_path = MAIN_DB

    safe, reason = is_safe_query(sql)
    if not safe:
        return ExecutionResult(success=False, error=reason, db_name=db_name)

    start = time.time()
    try:
        conn = sqlite3.connect(db_path, timeout=TIMEOUT_SECONDS)
        conn.row_factory = sqlite3.Row
        # Make DB read-only by not committing and using immutable pragma
        conn.execute("PRAGMA query_only = ON")
        cursor = conn.cursor()
        cursor.execute(sql)
        raw_rows = cursor.fetchmany(MAX_ROWS)
        columns = [d[0] for d in cursor.description] if cursor.description else []
        rows = [dict(zip(columns, row)) for row in raw_rows]
        conn.close()
        elapsed = (time.time() - start) * 1000

        return ExecutionResult(
            success=True,
            rows=rows,
            columns=columns,
            row_count=len(rows),
            execution_time_ms=elapsed,
            db_name=db_name,
        )
    except sqlite3.OperationalError as e:
        msg = str(e)
        # Make SQLite errors more student-friendly
        if "no such table" in msg:
            # Could be a misspelled keyword used as table name
            bad = msg.split("no such table:")[-1].strip()
            hint = f"Syntax Error: no such table '{bad}'. "
            hint += "This may be a misspelled keyword (e.g. 'FRM' instead of 'FROM')."
            return ExecutionResult(success=False, error=hint, db_name=db_name)
        if "syntax error" in msg.lower():
            return ExecutionResult(success=False, error=f"SQL Syntax Error: {msg}", db_name=db_name)
        return ExecutionResult(success=False, error=f"SQL Error: {msg}", db_name=db_name)
    except sqlite3.DatabaseError as e:
        return ExecutionResult(success=False, error=f"Database Error: {str(e)}", db_name=db_name)
    except Exception as e:
        return ExecutionResult(success=False, error=f"Unexpected error: {str(e)}", db_name=db_name)


def execute_on_all_edge_dbs(sql: str) -> Dict[str, ExecutionResult]:
    """Run a query on all edge-case databases."""
    edge_names = [
        "empty_courses", "partial_match", "all_enrolled",
        "single_course", "no_students"
    ]
    results = {}
    for name in edge_names:
        path = EDGE_DB.format(name)
        if os.path.exists(path):
            results[name] = execute_query(sql, db_path=path, db_name=name)
        else:
            results[name] = ExecutionResult(
                success=False, error="Edge DB not initialized", db_name=name
            )
    return results


# ══════════════════════════════════════════════════════════════════════
#  COMPARISON
# ══════════════════════════════════════════════════════════════════════

def _row_key(row: Dict) -> str:
    """Canonical string key for a row (for set comparison)."""
    return str(sorted((k.upper(), str(v).upper()) for k, v in row.items()))


def compare_results(base: ExecutionResult, student: ExecutionResult) -> ComparisonResult:
    """
    Compare two query result sets using set semantics.
    Returns missing rows, extra rows, and similarity metrics.
    """
    if not base.success or not student.success:
        return ComparisonResult(are_equivalent=False)

    base_keys   = {_row_key(r): r for r in base.rows}
    student_keys = {_row_key(r): r for r in student.rows}

    base_set    = set(base_keys.keys())
    student_set = set(student_keys.keys())

    missing  = [base_keys[k]    for k in base_set - student_set]
    extra    = [student_keys[k] for k in student_set - base_set]
    matching = [base_keys[k]    for k in base_set & student_set]

    union = base_set | student_set
    jaccard = len(base_set & student_set) / len(union) if union else 1.0

    return ComparisonResult(
        are_equivalent=(base_set == student_set),
        missing_rows=missing,
        extra_rows=extra,
        matching_rows=matching,
        base_count=len(base.rows),
        student_count=len(student.rows),
        jaccard_similarity=jaccard,
        is_subset=(student_set <= base_set),
        is_superset=(student_set >= base_set),
    )


def find_minimal_counterexample(base_sql: str, student_sql: str,
                                db_path: str = None) -> Optional[Dict]:
    """
    Find a minimal subset of the database that demonstrates the query difference.
    Inspired by RATest (Miao et al., SIGMOD 2019).
    Returns a dict with counterexample rows per table.
    """
    if db_path is None:
        db_path = MAIN_DB

    # First check there IS a difference
    base_res    = execute_query(base_sql, db_path)
    student_res = execute_query(student_sql, db_path)
    comp = compare_results(base_res, student_res)

    if comp.are_equivalent:
        return None  # No counterexample needed

    # Strategy: try progressively smaller subsets
    # (In a full system this uses provenance-guided pruning — here we use
    #  heuristic: focus on rows that appear in extra/missing results)

    counterexample = {
        "explanation": "Minimal database instance that reveals the query difference",
        "extra_in_student": comp.extra_rows,      # rows student returns but base doesn't
        "missing_from_student": comp.missing_rows, # rows base returns but student doesn't
        "key_tuples": _identify_key_tuples(comp, db_path),
    }
    return counterexample


def _identify_key_tuples(comp: ComparisonResult, db_path: str) -> List[Dict]:
    """Identify the specific tuples that cause the discrepancy."""
    key_tuples = []

    # For each extra row in student result, find its Takes entries
    for row in comp.extra_rows[:3]:  # limit to 3 for readability
        stu_id = row.get('StuID') or row.get('stuid')
        if stu_id:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                c = conn.cursor()
                c.execute("""
                    SELECT t.StuID, t.CourseID, co.Title, co."Group"
                    FROM Takes t JOIN Courses co ON t.CourseID = co.CourseID
                    WHERE t.StuID = ?
                """, (stu_id,))
                takes = [dict(r) for r in c.fetchall()]
                key_tuples.append({
                    "student": dict(row),
                    "enrolled_in": takes,
                    "issue": "Student included by student query but not by base query"
                })
            finally:
                conn.close()

    return key_tuples


# ══════════════════════════════════════════════════════════════════════
#  EDGE CASE ANALYSIS
# ══════════════════════════════════════════════════════════════════════

EDGE_CASE_META = {
    "empty_courses": {
        "title": "Empty DB Course Set",
        "description": "No courses with Group='DB' exist. Division over empty set.",
        "expected_base_behavior": "Returns all students (vacuous truth in NOT EXISTS) OR empty set depending on formulation.",
        "tests": "Reveals if student query uses IN (returns all students incorrectly) vs NOT EXISTS."
    },
    "partial_match": {
        "title": "Partial Enrollment",
        "description": "Nancy enrolled in only CS360, not CS460. Should NOT be in result.",
        "expected_base_behavior": "Returns only fully enrolled students.",
        "tests": "Reveals if student query catches partial matches."
    },
    "all_enrolled": {
        "title": "All Students Fully Enrolled",
        "description": "Every student has taken every DB course.",
        "expected_base_behavior": "Returns all students.",
        "tests": "Both queries may coincidentally agree — not sufficient for correctness."
    },
    "single_course": {
        "title": "Single DB Course",
        "description": "Only one DB course exists. Simpler division.",
        "expected_base_behavior": "Returns students enrolled in that single course.",
        "tests": "Reveals basic enrollment logic errors."
    },
    "no_students": {
        "title": "Empty Students Table",
        "description": "No students in the database.",
        "expected_base_behavior": "Returns empty result.",
        "tests": "Tests robustness to empty input."
    },
}


def full_edge_case_analysis(base_sql: str, student_sql: str) -> List[Dict]:
    """Run both queries on all edge case DBs and compare."""
    results = []
    for name, meta in EDGE_CASE_META.items():
        path = EDGE_DB.format(name)
        if not os.path.exists(path):
            continue
        base_res    = execute_query(base_sql, db_path=path, db_name=name)
        student_res = execute_query(student_sql, db_path=path, db_name=name)
        comp = compare_results(base_res, student_res)

        results.append({
            "name": name,
            "title": meta["title"],
            "description": meta["description"],
            "tests": meta["tests"],
            "base_result": base_res.to_dict(),
            "student_result": student_res.to_dict(),
            "comparison": comp.to_dict(),
            "passed": comp.are_equivalent,
        })
    return results


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'database'))
    from init_db import init_main_db, init_edge_dbs
    init_main_db()
    init_edge_dbs()

    base = """SELECT s.StuID, s.Name FROM Students s
    WHERE NOT EXISTS (
        SELECT c.CourseID FROM Courses c WHERE c."Group" = 'DB'
        AND c.CourseID NOT IN (
            SELECT t.CourseID FROM Takes t WHERE t.StuID = s.StuID
        )
    )"""

    wrong = """SELECT s.StuID, s.Name FROM Students s
    WHERE s.StuID IN (
        SELECT t.StuID FROM Takes t
        WHERE t.CourseID IN (
            SELECT c.CourseID FROM Courses c WHERE c."Group" = 'DB'
        )
    )"""

    print("=== Base Query ===")
    r = execute_query(base)
    print(f"  Rows: {r.rows}, Error: {r.error}")

    print("\n=== Student Query ===")
    r2 = execute_query(wrong)
    print(f"  Rows: {r2.rows}, Error: {r2.error}")

    print("\n=== Comparison ===")
    comp = compare_results(r, r2)
    print(f"  Equivalent : {comp.are_equivalent}")
    print(f"  Missing    : {comp.missing_rows}")
    print(f"  Extra      : {comp.extra_rows}")
    print(f"  Jaccard    : {comp.jaccard_similarity:.3f}")

    print("\n=== Edge Cases ===")
    ec = full_edge_case_analysis(base, wrong)
    for e in ec:
        status = "✓ PASS" if e['passed'] else "✗ FAIL"
        print(f"  {status} [{e['title']}]: base={e['base_result']['row_count']} rows, student={e['student_result']['row_count']} rows")

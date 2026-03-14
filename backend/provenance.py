"""
Provenance Tracer — Tracks how each output row is derived from input rows.
Inspired by I-REX (Miao et al., VLDB 2020) and Green et al. provenance semirings.

For each output row, we identify:
  - Which input rows (from each table) contributed to it
  - Which WHERE predicates passed/failed
  - Where the student query diverges from the base query
"""
import sqlite3
import os
import re
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field

DB_DIR = os.path.join(os.path.dirname(__file__), '..', 'database')
MAIN_DB = os.path.join(DB_DIR, 'main.db')


@dataclass
class ProvenanceRow:
    """Provenance record for a single output row."""
    output_row: Dict
    source_table: str
    contributing_rows: List[Dict] = field(default_factory=list)
    predicate_results: List[Dict] = field(default_factory=list)
    in_base: bool = True
    in_student: bool = True

    def to_dict(self):
        return {
            "output_row": self.output_row,
            "source_table": self.source_table,
            "contributing_rows": self.contributing_rows,
            "predicate_results": self.predicate_results,
            "in_base": self.in_base,
            "in_student": self.in_student,
        }


@dataclass
class ProvenanceTrace:
    """Full provenance trace for a query comparison."""
    query_type: str  # DIVISION | AGGREGATION | JOIN | SET_OP | SIMPLE
    base_rows: List[ProvenanceRow] = field(default_factory=list)
    student_rows: List[ProvenanceRow] = field(default_factory=list)
    divergence_points: List[Dict] = field(default_factory=list)
    steps: List[Dict] = field(default_factory=list)
    explanation: str = ""

    def to_dict(self):
        return {
            "query_type": self.query_type,
            "base_rows": [r.to_dict() for r in self.base_rows],
            "student_rows": [r.to_dict() for r in self.student_rows],
            "divergence_points": self.divergence_points,
            "steps": self.steps,
            "explanation": self.explanation,
        }


def get_conn(db_path=None):
    conn = sqlite3.connect(db_path or MAIN_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only = ON")
    return conn


# ══════════════════════════════════════════════════════════════════════
#  DIVISION QUERY PROVENANCE (Core Research Focus)
# ══════════════════════════════════════════════════════════════════════

def trace_division_provenance(base_sql: str, student_sql: str,
                               db_path: str = None) -> ProvenanceTrace:
    """
    Specialized provenance tracing for division queries.
    Implements the three-step evaluation trace:
      Step 1: Identify the divisor set (DB courses)
      Step 2: For each student, compute their covered courses
      Step 3: Check if covered ⊇ divisor (the division condition)
    """
    conn = get_conn(db_path)
    trace = ProvenanceTrace(query_type="DIVISION")

    try:
        c = conn.cursor()

        # ── Step 1: Divisor set (DB courses) ──
        c.execute('SELECT CourseID, Title, "Group" FROM Courses WHERE "Group" = \'DB\'')
        db_courses = [dict(r) for r in c.fetchall()]
        divisor_ids = {r['CourseID'] for r in db_courses}

        trace.steps.append({
            "step": 1,
            "title": "Identify Divisor Set (DB Courses)",
            "description": f"Find all courses where Group = 'DB'. These form the divisor set D.",
            "result": db_courses,
            "formula": "D = σ(Group='DB')(Courses)",
            "tuples": [{"label": f"{r['CourseID']} ({r['Title']})", "type": "divisor"} for r in db_courses],
        })

        # ── Step 2: Per-student course coverage ──
        c.execute("""
            SELECT s.StuID, s.Name, t.CourseID, co.Title, co."Group"
            FROM Students s
            LEFT JOIN Takes t ON s.StuID = t.StuID
            LEFT JOIN Courses co ON t.CourseID = co.CourseID
            ORDER BY s.StuID
        """)
        all_enrollments = [dict(r) for r in c.fetchall()]

        # Group by student
        student_courses = {}
        c.execute("SELECT StuID, Name FROM Students")
        for row in c.fetchall():
            student_courses[row['StuID']] = {
                "name": row['Name'],
                "courses": set(),
                "db_courses": set()
            }

        for enr in all_enrollments:
            sid = enr['StuID']
            if sid and enr['CourseID']:
                student_courses[sid]['courses'].add(enr['CourseID'])
                if enr['Group'] == 'DB':
                    student_courses[sid]['db_courses'].add(enr['CourseID'])

        step2_tuples = []
        for sid, info in student_courses.items():
            covered = info['db_courses']
            missing = divisor_ids - covered
            step2_tuples.append({
                "label": f"{sid} ({info['name']}): DB courses = {{{', '.join(sorted(covered)) or '∅'}}}",
                "type": "covered" if not missing else "partial",
                "missing": sorted(missing),
                "sid": sid,
            })

        trace.steps.append({
            "step": 2,
            "title": "Compute Per-Student DB Course Coverage",
            "description": "For each student s, find which DB courses they have taken: coverage(s) = π(CourseID)(σ(StuID=s)(Takes)) ∩ D",
            "tuples": step2_tuples,
            "formula": "coverage(s) = {c | Takes(s, c) ∧ c ∈ D}",
        })

        # ── Step 3: Division check ──
        step3_base_tuples = []
        step3_student_tuples = []

        # Execute base and student queries
        c2 = conn.cursor()
        try:
            c2.execute(base_sql)
            base_results = {r['StuID'] for r in c2.fetchall()}
        except Exception:
            base_results = set()

        try:
            c2.execute(student_sql)
            student_results = {r['StuID'] for r in c2.fetchall()}
        except Exception:
            student_results = set()

        for sid, info in student_courses.items():
            missing = divisor_ids - info['db_courses']
            in_base = sid in base_results
            in_student = sid in student_results

            base_label = f"{sid} ({info['name']}): " + ("✓ INCLUDED" if in_base else "✗ EXCLUDED")
            stu_label  = f"{sid} ({info['name']}): " + ("✓ INCLUDED" if in_student else "✗ EXCLUDED")

            base_type = "correct_include" if in_base else "correct_exclude"
            stu_type  = "correct_include" if in_student == in_base else \
                        ("false_positive" if in_student else "false_negative")

            step3_base_tuples.append({"label": base_label, "type": base_type,
                                       "missing_courses": sorted(missing)})
            step3_student_tuples.append({"label": stu_label, "type": stu_type,
                                          "missing_courses": sorted(missing)})

        trace.steps.append({
            "step": 3,
            "title": "Apply Division Condition",
            "description": "Check if each student's coverage equals the divisor set (i.e., no DB course is missing).",
            "base_tuples": step3_base_tuples,
            "student_tuples": step3_student_tuples,
            "formula": "Result = {s | ∀c ∈ D: Takes(s, c)} ≡ {s | ¬∃c ∈ D: ¬Takes(s, c)}",
            "base_uses": "NOT EXISTS (double negation — correct universal quantification)",
            "student_uses": _detect_student_pattern(student_sql),
        })

        # ── Divergence points ──
        for sid, info in student_courses.items():
            in_base = sid in base_results
            in_student = sid in student_results
            if in_base != in_student:
                missing = sorted(divisor_ids - info['db_courses'])
                trace.divergence_points.append({
                    "student": sid,
                    "name": info['name'],
                    "db_courses_taken": sorted(info['db_courses']),
                    "missing_courses": missing,
                    "in_base": in_base,
                    "in_student": in_student,
                    "type": "false_positive" if in_student else "false_negative",
                    "explanation": _explain_divergence(sid, info['name'],
                                                        info['db_courses'], missing,
                                                        in_student, in_base)
                })

        trace.explanation = _build_division_explanation(
            db_courses, student_courses, base_results, student_results
        )

    finally:
        conn.close()

    return trace


def _detect_student_pattern(sql: str) -> str:
    upper = sql.upper()
    if 'NOT EXISTS' in upper:
        return "NOT EXISTS (correct — universal quantification)"
    elif 'NOT IN' in upper:
        return "NOT IN (may work but less robust than NOT EXISTS)"
    elif ' IN ' in upper:
        return "IN (incorrect — existential quantification, gives partial matches)"
    elif 'GROUP BY' in upper and 'HAVING' in upper:
        return "GROUP BY / HAVING COUNT (correct if threshold matches |D| exactly)"
    else:
        return "Unknown pattern"


def _explain_divergence(sid, name, db_taken, missing, in_student, in_base) -> str:
    if in_student and not in_base:
        if missing:
            return (
                f"{name} ({sid}) is incorrectly included by the student query. "
                f"They enrolled in {sorted(db_taken)} but are missing {missing} from the DB course set. "
                f"The student query's IN condition returns True because {name} took AT LEAST ONE DB course, "
                f"but the correct logic requires ALL DB courses to be covered."
            )
        return f"{name} ({sid}) is included by student query but excluded by base for an unknown reason."
    elif in_base and not in_student:
        return (
            f"{name} ({sid}) is incorrectly excluded by the student query despite meeting all requirements. "
            f"They enrolled in {sorted(db_taken)} which covers all required DB courses {sorted(db_taken)}."
        )
    return ""


def _build_division_explanation(db_courses, student_courses, base_results, student_results) -> str:
    divisor = {r['CourseID'] for r in db_courses}
    false_positives = [sid for sid in student_results - base_results]
    false_negatives = [sid for sid in base_results - student_results]

    parts = []
    parts.append(f"The divisor set D contains {len(divisor)} DB course(s): {sorted(divisor)}.")

    if false_positives:
        names = [student_courses[s]['name'] for s in false_positives if s in student_courses]
        parts.append(
            f"False positives: {', '.join(names)} are incorrectly returned by the student query "
            f"because they enrolled in at least one (but not all) DB courses. "
            f"The IN operator checks ∃c ∈ D: Takes(s, c) instead of ∀c ∈ D: Takes(s, c)."
        )

    if false_negatives:
        names = [student_courses[s]['name'] for s in false_negatives if s in student_courses]
        parts.append(
            f"False negatives: {', '.join(names)} are missing from the student query result "
            f"despite satisfying all division conditions."
        )

    if not false_positives and not false_negatives:
        parts.append("Both queries produce identical results on this database instance.")

    return " ".join(parts)


# ══════════════════════════════════════════════════════════════════════
#  GENERIC PROVENANCE TRACER
# ══════════════════════════════════════════════════════════════════════

def trace_join_provenance(base_sql: str, student_sql: str,
                           db_path: str = None) -> ProvenanceTrace:
    """Provenance trace for JOIN queries."""
    conn = get_conn(db_path)
    trace = ProvenanceTrace(query_type="JOIN")

    try:
        c = conn.cursor()
        c.execute(base_sql)
        base_rows = [dict(r) for r in c.fetchall()]
        c.execute(student_sql)
        stu_rows = [dict(r) for r in c.fetchall()]

        base_keys = {str(sorted(r.items())) for r in base_rows}
        stu_keys  = {str(sorted(r.items())) for r in stu_rows}

        trace.steps.append({
            "step": 1, "title": "Execute Base Query",
            "result": base_rows,
            "tuples": [{"label": str(r), "type": "match"} for r in base_rows]
        })
        trace.steps.append({
            "step": 2, "title": "Execute Student Query",
            "result": stu_rows,
            "tuples": [{"label": str(r), "type": "correct_include" if str(sorted(r.items())) in base_keys else "false_positive"} for r in stu_rows]
        })

        # Divergence
        for r in base_rows:
            if str(sorted(r.items())) not in stu_keys:
                trace.divergence_points.append({
                    "row": r, "type": "false_negative",
                    "explanation": f"Row {r} is in base result but missing from student result."
                })
        for r in stu_rows:
            if str(sorted(r.items())) not in base_keys:
                trace.divergence_points.append({
                    "row": r, "type": "false_positive",
                    "explanation": f"Row {r} is in student result but not in base result."
                })

        trace.explanation = (
            f"Base query returned {len(base_rows)} row(s), student query returned {len(stu_rows)} row(s). "
            + (f"{len(trace.divergence_points)} divergence point(s) found." if trace.divergence_points
               else "Results are identical.")
        )

    finally:
        conn.close()

    return trace


def compute_provenance(base_sql: str, student_sql: str,
                        query_type: str = "DIVISION",
                        db_path: str = None) -> ProvenanceTrace:
    """Dispatch to the appropriate provenance tracer."""
    if query_type == "DIVISION":
        return trace_division_provenance(base_sql, student_sql, db_path)
    else:
        return trace_join_provenance(base_sql, student_sql, db_path)


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

    trace = compute_provenance(base, wrong, "DIVISION")
    print(f"Query type : {trace.query_type}")
    print(f"Steps      : {len(trace.steps)}")
    print(f"Divergence : {len(trace.divergence_points)}")
    print(f"Explanation: {trace.explanation}")
    for dp in trace.divergence_points:
        print(f"  → {dp['explanation']}")

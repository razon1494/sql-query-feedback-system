"""
The NULL-injected test database of the ICWL 2026 paper (Sec. 5.3, talk slide 11).

A NOT IN over a subquery that can return NULL is UNKNOWN for every row, so on
NULL-free data the error stays hidden. A copy of the problem's database with
NULL values added to the link table makes it observable:

  * one link row with a NULL outer key and a non-divisor value, exactly as the
    paper's harness builds it (external/spider/gen_division.py,
    make_null_edge); it exposes the NOT-IN-in-NOT-IN form;
  * one link row with a NULL course per outer row, which exposes a NOT IN over
    the correlated set of courses taken.

A reference written with NOT EXISTS is unaffected: a NULL never joins. The copy
is used only for division problems whose submission uses NOT IN and whose
reference does not (a NOT IN reference would itself be distorted by the NULLs).
The rule M3 already reads a failed test database as its evidence.
"""
import os
import re
import shutil
import sqlite3
from typing import Iterable, Optional

from backend.query_executor import compare_results, execute_query

NULL_EDGE_NAME = "null_link"
NULL_EDGE_META = {
    "title": "NULL in the Enrolment Table",
    "description": ("A copy of this problem's database with NULL values added to the link "
                    "table (enrolments, or teaching assignments)."),
    "tests": "Reveals NOT IN over a subquery that can return NULL: the classic hidden NULL trap.",
}
NONEXISTENT_VALUE = "__nonexistent__"
# Link tables of the site's schemas: (table, outer key, divisor key, outer table, outer key).
LINK_TABLES = (
    ("Takes", "StuID", "CourseID", "Students", "StuID"),
    ("Teaches", "InstID", "CourseID", "Instructors", "InstID"),
)
APPLICABLE_TYPES = frozenset({"DIVISION"})
_NOT_IN = re.compile(r"\bNOT\s+IN\b", re.IGNORECASE)


def null_instance_path(source_path: str) -> str:
    """Where the NULL-injected copy of a database lives."""
    folder, name = os.path.split(source_path)
    return os.path.join(folder, "null_" + name)


def _tables(conn) -> set:
    return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def build_null_instance(source_path: str) -> Optional[str]:
    """Copy a database and add NULLs to its link tables. Returns the copy's path, or None."""
    target = null_instance_path(source_path)
    try:
        shutil.copyfile(source_path, target)
        conn = sqlite3.connect(target)
        try:
            present = _tables(conn)
            for link, lfo, lfd, outer, opk in LINK_TABLES:
                if link not in present or outer not in present:
                    continue
                conn.execute(f'INSERT INTO "{link}" ("{lfo}", "{lfd}") VALUES (NULL, ?)',
                             (NONEXISTENT_VALUE,))
                conn.execute(f'INSERT INTO "{link}" ("{lfo}", "{lfd}") '
                             f'SELECT "{opk}", NULL FROM "{outer}"')
            conn.commit()
        finally:
            conn.close()
        return target
    except (sqlite3.Error, OSError) as e:
        # A NOT NULL link column makes the instance unbuildable; the gate then
        # falls back as the paper describes (M3 releases on divergence).
        print(f"[DB] No NULL-injected copy of {source_path}: {e}")
        if os.path.exists(target):
            os.remove(target)
        return None


def init_null_instances(source_paths: Iterable[str]) -> list:
    built = [p for p in (build_null_instance(s) for s in source_paths) if p]
    for p in built:
        print(f"[DB] NULL-injected copy ready: {os.path.basename(p)}")
    return built


def applies(problem_type: str, base_sql: str, student_sql: str) -> bool:
    return (problem_type in APPLICABLE_TYPES
            and bool(_NOT_IN.search(student_sql or ""))
            and not _NOT_IN.search(base_sql or ""))


def run_null_instance(base_sql: str, student_sql: str, source_path: str) -> Optional[dict]:
    """Run both queries on the NULL-injected copy; an edge-case result, or None."""
    path = null_instance_path(source_path)
    if not os.path.exists(path):
        return None
    base = execute_query(base_sql, db_path=path, db_name=NULL_EDGE_NAME)
    student = execute_query(student_sql, db_path=path, db_name=NULL_EDGE_NAME)
    comparison = compare_results(base, student)
    return {
        "name": NULL_EDGE_NAME,
        **NULL_EDGE_META,
        "base_result": base.to_dict(),
        "student_result": student.to_dict(),
        "comparison": comparison.to_dict(),
        "passed": comparison.are_equivalent,
    }

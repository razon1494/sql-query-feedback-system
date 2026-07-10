"""
Phase 2 test — problem-type classifier on the mini fixture + unit cases.

Run:  python external/tests/test_phase2_classify.py
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture, DEV_EXAMPLES
from external.spider import ingest
from external.spider.classify import classify_type


# Unit cases that pin down the precedence rules explicitly.
UNIT_CASES = [
    ("SELECT name FROM artist WHERE country = 'UK' UNION SELECT name FROM artist WHERE country = 'US'", "SET_OP"),
    ("SELECT s.name FROM student s WHERE NOT EXISTS (SELECT 1 FROM course c WHERE c.dept='CS' "
     "AND c.cid NOT IN (SELECT t.cid FROM takes t WHERE t.sid = s.sid))", "DIVISION"),
    ("SELECT dept, COUNT(*) FROM student GROUP BY dept HAVING COUNT(*) > 5", "AGGREGATION"),
    ("SELECT AVG(age) FROM student", "AGGREGATION"),
    ("SELECT title FROM album WHERE artist_id = (SELECT artist_id FROM artist WHERE name = 'Queen')", "SUBQUERY"),
    ("SELECT name FROM student WHERE major IS NULL", "NULL"),
    ("SELECT s.name, c.title FROM student s JOIN takes t ON s.sid=t.sid JOIN course c ON t.cid=c.cid", "JOIN"),
    ("SELECT name FROM student WHERE age > 20", "JOIN"),  # plain filter defaults to JOIN
]


def main() -> int:
    failures = []

    # 1. explicit precedence unit cases
    for sql, expected in UNIT_CASES:
        got = classify_type(sql)["type"]
        status = "ok" if got == expected else "FAIL"
        print(f"  [{status}] expected={expected:<12} got={got:<12} :: {sql[:60]}")
        if got != expected:
            failures.append(f"unit: expected {expected}, got {got} for: {sql[:70]}")

    # 2. fixture examples must match their declared _problem_type
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    examples = ingest.load_examples(split="dev")
    tag = {d["query"]: d["_problem_type"] for d in DEV_EXAMPLES}
    print()
    for ex in examples:
        expected = tag[ex.gold_sql]
        got = classify_type(ex.gold_sql)["type"]
        status = "ok" if got == expected else "FAIL"
        print(f"  [{status}] fixture expected={expected:<12} got={got:<12}")
        if got != expected:
            failures.append(f"fixture: expected {expected}, got {got} for: {ex.gold_sql[:70]}")

    print()
    if failures:
        print("PHASE 2 CLASSIFY: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 2 CLASSIFY: PASS  (precedence unit cases + 4 fixture types)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

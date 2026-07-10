"""
Phase 3.2 test — wrong-query corruption operators on the mini fixture.

Run:  python external/tests/test_phase3_wrong.py

Each operator is exercised on a hand-picked fixture query where the corruption
must (a) transform the SQL as intended, (b) diverge by execution, and
(c) produce the documented detection outcome — including the KNOWN MISS of the
uncorrelated-EXISTS gap probe, which this test pins down as documented
behavior (failure-analysis material, not a hidden defect).
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture
from external.spider.gen_wrong import _OP_FN
from external.harness.generic_problem import GenericProblem, analyze
from backend.sql_parser import parse_sql, _extract_top_level_clauses

# (operator, gold_sql, problem_type, expected_key, expect_detected)
CASES = [
    ("drop_where",
     "SELECT title FROM album WHERE genre = 'grunge'",
     "JOIN", "MISSING_WHERE", True),
    ("inner_to_left",
     "SELECT al.title, t.title FROM album al JOIN track t ON al.album_id = t.album_id",
     "JOIN", "WRONG_JOIN_TYPE", True),
    ("join_to_cartesian",
     "SELECT a.name, al.title FROM artist a JOIN album al ON a.artist_id = al.artist_id",
     "JOIN", "CARTESIAN_PRODUCT", True),
    ("drop_group_by",
     ("SELECT a.name, COUNT(al.album_id) AS n FROM artist a "
      "JOIN album al ON a.artist_id = al.artist_id GROUP BY a.artist_id, a.name"),
     "AGGREGATION", "MISSING_GROUP_BY", True),
    ("having_to_where",
     ("SELECT a.name, COUNT(al.album_id) AS n FROM artist a "
      "JOIN album al ON a.artist_id = al.artist_id "
      "GROUP BY a.artist_id, a.name HAVING COUNT(al.album_id) > 1"),
     "AGGREGATION", "HAVING_vs_WHERE", True),
    ("swap_set_op",
     ("SELECT name FROM artist WHERE country = 'UK' "
      "UNION SELECT name FROM artist WHERE country = 'US'"),
     "SET_OP", "WRONG_SET_OP", True),
    # Documented gap: uncorrelated EXISTS is a real novice error the current
    # detector anchors past. expect_detected=False pins the miss as known.
    ("in_to_uncorrelated_exists",
     ("SELECT name FROM artist WHERE artist_id IN "
      "(SELECT artist_id FROM album WHERE genre = 'grunge')"),
     "SUBQUERY", "MISSING_CORRELATED_REF", False),
]


def main() -> int:
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    db_path = os.path.join(root, "database", "music_mini", "music_mini.sqlite")
    failures = []

    for op_name, gold, ptype, expected, expect_hit in CASES:
        parse = parse_sql(gold)
        clauses = _extract_top_level_clauses(gold)
        wrong = _OP_FN[op_name](gold, parse, clauses)
        if not wrong:
            failures.append(f"{op_name}: operator produced no output")
            print(f"  [FAIL] {op_name}: no corruption produced")
            continue

        prob = GenericProblem(f"fix::{op_name}", db_path, gold, ptype, "music_mini")
        res = analyze(prob, wrong)
        hit = expected in res.detected
        ok = (not res.are_equivalent) and (hit == expect_hit)
        tag = "ok" if ok else "FAIL"
        want = "HIT" if expect_hit else "documented MISS"
        got = "HIT" if hit else "miss"
        print(f"  [{tag}] {op_name:<28} equiv={res.are_equivalent} "
              f"want={want:<15} got={got:<5} detected={res.detected}")
        if res.are_equivalent:
            failures.append(f"{op_name}: corruption did not diverge on fixture data")
        if hit != expect_hit:
            failures.append(f"{op_name}: expected {want}, got {got} "
                            f"(detected={res.detected}, raw={res.raw})")

    print()
    if failures:
        print("PHASE 3.2 WRONG: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 3.2 WRONG: PASS  (6 operators detected; uncorrelated-EXISTS gap pinned)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

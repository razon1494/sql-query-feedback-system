"""
Phase 3.1 test — alternate-correct generator on the mini fixture.

Run:  python external/tests/test_phase3_alternates.py

Proves on the non-university music schema:
  (A) join_to_comma produces an execution-equivalent variant the detector does
      NOT flag (raw or user-facing).
  (B) dejoin_to_in produces an execution-equivalent variant that the RAW shape
      classifier flags MISSING_JOIN but the result-aware filter SUPPRESSES.
      (This is the core false-positive-avoidance claim, on a foreign schema.)
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture
from external.spider import ingest
from external.spider.classify import classify_type
from external.spider.gen_alternates import op_join_to_comma, op_dejoin_to_in
from external.harness.generic_problem import GenericProblem, analyze
from backend.sql_parser import parse_sql


def main() -> int:
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    examples = ingest.load_examples(split="dev")
    db_path = examples[0].sqlite_path
    failures = []

    # ── (A) join_to_comma on the fixture JOIN gold ───────────────────────────
    join_gold = ("SELECT a.name, al.title FROM artist a "
                 "JOIN album al ON a.artist_id = al.artist_id")
    variant = op_join_to_comma(join_gold, parse_sql(join_gold))
    print(f"(A) variant: {variant}")
    if not variant or "," not in variant.split("WHERE")[0]:
        failures.append("(A) join_to_comma did not produce a comma-join")
    else:
        prob = GenericProblem("fix::JOIN", db_path, join_gold, "JOIN", "music_mini")
        r = analyze(prob, variant)
        print(f"(A) equiv={r.are_equivalent} raw={r.raw} detected={r.detected}")
        if not r.are_equivalent:
            failures.append("(A) comma-join not output-equivalent")
        if r.detected:
            failures.append(f"(A) unexpected user-facing flags: {r.detected}")

    # ── (B) dejoin_to_in: raw MISSING_JOIN, filter suppresses ────────────────
    dj_gold = ("SELECT T1.name FROM artist AS T1 "
               "JOIN album AS T2 ON T1.artist_id = T2.artist_id "
               "WHERE T2.genre = 'rock'")
    variant = op_dejoin_to_in(dj_gold, parse_sql(dj_gold))
    print(f"(B) variant: {variant}")
    if not variant or "IN (SELECT" not in variant.upper().replace("  ", " "):
        failures.append(f"(B) dejoin_to_in did not produce an IN-subquery: {variant}")
    else:
        prob = GenericProblem("fix::JOIN", db_path, dj_gold, "JOIN", "music_mini")
        r = analyze(prob, variant)
        print(f"(B) equiv={r.are_equivalent} raw={r.raw} detected={r.detected}")
        if not r.are_equivalent:
            failures.append("(B) IN-subquery rewrite not output-equivalent")
        if "MISSING_JOIN" not in r.raw:
            failures.append(f"(B) expected raw MISSING_JOIN, got raw={r.raw}")
        if r.detected:
            failures.append(f"(B) filter failed to suppress; user-facing={r.detected}")

    print()
    if failures:
        print("PHASE 3.1 ALTERNATES: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 3.1 ALTERNATES: PASS  (comma-join clean; dejoin raw-flagged then suppressed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

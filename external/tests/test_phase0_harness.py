"""
Phase 0 test — the generic harness runs the UNCHANGED detector against a
non-university (music) schema.

Run:  python external/tests/test_phase0_harness.py

Proves three things on a schema the detector has never seen:
  (A) detection      — a missing-GROUP-BY query is flagged MISSING_GROUP_BY
  (B) FP suppression — an output-equivalent comma-join rewrite is accepted as
                       alternate-correct (no user-facing misconception)
  (C) no self-FP     — the gold query fed back as the candidate raises nothing
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture
from external.spider import ingest
from external.harness.generic_problem import GenericProblem, analyze


def _problem(ex, ptype) -> GenericProblem:
    return GenericProblem(
        problem_id=f"{ex.db_id}:{ptype}",
        db_path=ex.sqlite_path,
        gold_sql=ex.gold_sql,
        problem_type=ptype,
        db_id=ex.db_id,
        source="spider_fixture",
    )


def main() -> int:
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    examples = ingest.load_examples(split="dev")
    by_type = {ex.gold_sql: ex for ex in examples}
    # locate examples by their declared type via the raw dev.json tags
    import json
    dev = json.load(open(os.path.join(root, "dev.json"), encoding="utf-8"))
    type_of = {d["query"]: d["_problem_type"] for d in dev}

    agg = next(e for e in examples if type_of[e.gold_sql] == "AGGREGATION")
    join = next(e for e in examples if type_of[e.gold_sql] == "JOIN")

    failures = []

    # ── (A) Detection: drop GROUP BY (and HAVING) on the aggregation problem ──
    wrong_agg = ("SELECT a.name, COUNT(al.album_id) AS n FROM artist a "
                 "JOIN album al ON a.artist_id = al.artist_id")
    r = analyze(_problem(agg, "AGGREGATION"), wrong_agg)
    print(f"(A) detection      detected={r.detected} equiv={r.are_equivalent}")
    if "MISSING_GROUP_BY" not in r.detected:
        failures.append(f"(A) expected MISSING_GROUP_BY, got {r.detected}")
    if r.are_equivalent:
        failures.append("(A) wrong query should not be output-equivalent")

    # ── (B) FP suppression: comma-join equi-join rewrite of the JOIN problem ──
    alt_join = ("SELECT a.name, al.title FROM artist a, album al "
                "WHERE a.artist_id = al.artist_id")
    r = analyze(_problem(join, "JOIN"), alt_join)
    print(f"(B) FP suppression detected={r.detected} raw={r.raw} "
          f"equiv={r.are_equivalent} alt_correct={r.is_alternate_correct}")
    if not r.are_equivalent:
        failures.append("(B) comma-join rewrite should be output-equivalent")
    if r.detected:
        failures.append(f"(B) expected no user-facing misconceptions, got {r.detected}")

    # ── (C) No self-false-positive: gold as its own candidate ────────────────
    r = analyze(_problem(join, "JOIN"), join.gold_sql)
    print(f"(C) self-check     detected={r.detected} equiv={r.are_equivalent}")
    if r.detected:
        failures.append(f"(C) gold-as-candidate raised {r.detected}")
    if not r.are_equivalent:
        failures.append("(C) gold-as-candidate should be equivalent")

    print()
    if failures:
        print("PHASE 0 HARNESS: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 0 HARNESS: PASS  (detection + FP-suppression + self-check on music schema)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

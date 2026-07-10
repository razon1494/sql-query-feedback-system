"""
Phase 4 test — baseline metric math + live division baseline on the fixture.

Run:  python external/tests/test_phase4_baselines.py
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.eval.run_baselines import metrics_alternates, metrics_wrong
from external.fixtures.build_mini_spider import build as build_fixture
from external.spider.gen_division import mine_problems, MUTANT_KEYS, make_null_edge
from external.harness.generic_problem import GenericProblem, analyze


def main() -> int:
    failures = []

    # ── unit: alternates metrics ─────────────────────────────────────────────
    alts = [
        {"raw": ["MISSING_JOIN"], "detected": []},   # raw-flagged, suppressed
        {"raw": [], "detected": []},                 # clean
        {"raw": [], "detected": []},
        {"raw": ["X"], "detected": ["X"]},           # a real user-facing FP
    ]
    m = metrics_alternates(alts)
    if m["fpr"] != {"output_only": 0.0, "shape_only": 50.0, "full_system": 25.0}:
        failures.append(f"alternates fpr wrong: {m['fpr']}")

    # ── unit: wrong-corpus metrics ───────────────────────────────────────────
    wrong = [
        {"outcome": "HIT", "expected": "A", "raw_keys": ["A"], "detected_keys": ["A"]},
        {"outcome": "MISS", "expected": "B", "raw_keys": [], "detected_keys": []},
        {"outcome": "HIT", "expected": "C", "raw_keys": ["C", "D"], "detected_keys": ["C", "D"]},
        {"outcome": "LATENT_SKIP", "expected": "A", "raw_keys": ["A"], "detected_keys": []},
        {"outcome": "LATENT_SKIP", "expected": "B", "raw_keys": [], "detected_keys": []},
    ]
    m = metrics_wrong(wrong)
    ok = (m["n_applied"] == 3 and m["n_latent"] == 2
          and m["wrong_flagged"] == {"output_only": 100.0, "shape_only": 66.7,
                                     "full_system": 66.7}
          and m["wrong_diagnosed"] == {"output_only": 0.0, "shape_only": 66.7,
                                       "full_system": 66.7}
          and m["latent_flagged"] == {"output_only": 0.0, "shape_only": 50.0,
                                      "full_system": 0.0})
    if not ok:
        failures.append(f"wrong metrics off: {m}")

    # ── integration: division baseline records on the fixture ───────────────
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    problems = mine_problems(root=root, all_dbs=True, per_db=2)
    if not problems:
        failures.append("no fixture division problems mined")
    else:
        p = problems[0]
        db_path = p["db_path"]
        edge = make_null_edge(db_path, p["triple"], "t4fix")
        prob = GenericProblem("t4::fix", db_path, p["queries"]["gold"], "DIVISION",
                              p["db_id"], edge_db_paths=[edge] if edge else [])
        recs = []
        for key in MUTANT_KEYS:
            r = analyze(prob, p["queries"][key])
            recs.append({"expected": key,
                         "outcome": ("LATENT_SKIP" if (r.are_equivalent and r.edges_ok)
                                     else ("HIT" if key in r.detected else "MISS")),
                         "raw_keys": r.raw, "detected_keys": r.detected})
        m = metrics_wrong(recs)
        # On the fixture, all 3 division mutants diverge and are diagnosed by
        # shape-only and full alike; output-only diagnoses none.
        if m["wrong_diagnosed"]["full_system"] != 100.0:
            failures.append(f"fixture division full diag != 100: {m}")
        if m["wrong_diagnosed"]["output_only"] != 0.0:
            failures.append("output-only diagnosis must be 0")

    print()
    if failures:
        print("PHASE 4 BASELINES: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 4 BASELINES: PASS  (metric math + fixture division baselines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

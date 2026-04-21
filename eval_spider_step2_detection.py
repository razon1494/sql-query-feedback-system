"""
Phase 4, Step 2 — Independent Detection-Rate Evaluation

For each wrong query in `spider_corpus_step2_wrong.py`:
  (1) execute against the main DB — if it errors out, report as corpus bug;
  (2) verify the query produces a DIFFERENT result from the reference
      (otherwise the filter in generate_feedback would correctly suppress
      misconceptions — but that would not be a detection failure here);
  (3) run the full `generate_feedback` pipeline and check whether the
      expected misconception key is in `report.misconceptions`.

Detection-rate definition: (#queries where expected key is in report.misconceptions)
                           / (#queries applied successfully)

This eval is INDEPENDENT of the Phase 3 mutator-based eval — each wrong
query is built by hand from Spider-style patterns, not by mutating our
own reference queries.

Usage:
    python3 eval_spider_step2_detection.py
"""
import os, sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.init_db import init_main_db, init_edge_dbs
init_main_db(); init_edge_dbs()

# Build the enriched test DB used for Step 2 so that wrong queries actually
# diverge from the reference (the small 5-student main DB masks many errors).
from build_step2_db import build as build_step2_db, DB_PATH as STEP2_DB_PATH
build_step2_db()

from backend.sql_parser import parse_sql
from backend.feedback_generator import generate_feedback
from backend.query_executor import execute_query, compare_results, full_edge_case_analysis
from backend.problems import get_problem

from spider_corpus_step2_wrong import CORPUS


def _exec(sql):
    return execute_query(sql, db_path=STEP2_DB_PATH, db_name="step2_enriched")


def main():
    per_mis = defaultdict(lambda: {"applied": 0, "detected": 0,
                                    "misses": [], "equiv_skipped": 0,
                                    "exec_err": 0})
    details = []

    for entry in CORPUS:
        pid       = entry["problem_id"]
        expected  = entry["misconception"]
        shape     = entry["shape"]
        wrong_sql = entry["query"].strip()

        problem = get_problem(pid)
        if problem is None:
            print(f"[SKIP] unknown problem_id: {pid}")
            continue

        base_sql = problem["base_query"]
        ptype    = problem["type"]

        # Parse + execute both against the enriched test DB.
        base_parse    = parse_sql(base_sql)
        student_parse = parse_sql(wrong_sql)
        base_result    = _exec(base_sql)
        student_result = _exec(wrong_sql)

        # Executional errors on the wrong query are OK as detectable issues
        # (the system should flag syntax/exec errors separately), but for
        # detection-rate accounting we treat them as "applied" with
        # exec_error handled.
        exec_error = student_result.error if not student_result.success else None

        # Compute comparison either way
        if student_result.success:
            comp = compare_results(base_result, student_result).to_dict()
        else:
            comp = {"are_equivalent": False, "missing_rows": [],
                    "extra_rows": [], "jaccard_similarity": 0.0}

        # If the wrong query happens to produce the correct result on the
        # main DB AND passes every edge case, the result-aware filter would
        # legitimately treat it as alt-correct. Only under those conditions
        # do we skip this entry from detection-rate accounting — otherwise
        # the misconception should fire and be counted.
        if comp.get("are_equivalent") and student_result.success:
            try:
                pre_edge = full_edge_case_analysis(base_sql, wrong_sql)
            except Exception:
                pre_edge = []
            all_edges_pass = not pre_edge or all(e.get("passed") for e in pre_edge)
            if all_edges_pass:
                per_mis[expected]["equiv_skipped"] += 1
                details.append({
                    "pid": pid, "shape": shape, "expected": expected,
                    "outcome": "EQUIV_SKIP",
                    "detail": "wrong SQL matches reference on main DB AND passes all edge cases",
                })
                continue

        per_mis[expected]["applied"] += 1
        if exec_error:
            per_mis[expected]["exec_err"] += 1

        # Run edge-case analysis against the published edge DBs. This lets
        # the result-aware filter correctly preserve misconceptions for
        # latent bugs (IN-vs-EXISTS, LEFT-vs-INNER, NOT-IN-vs-NOT-EXISTS)
        # that only diverge from the reference under NULLs or missing data.
        try:
            edge_results = full_edge_case_analysis(base_sql, wrong_sql)
        except Exception:
            edge_results = []

        report = generate_feedback(
            base_parse=base_parse,
            student_parse=student_parse,
            comparison=comp,
            edge_results=edge_results,
            provenance_trace=None,
            problem_type=ptype,
            execution_error=exec_error,
        )
        reported_keys = {m["key"] for m in report.misconceptions}
        raw_keys      = {m["key"] for m in report.raw_misconceptions}
        detected = expected in reported_keys

        if detected:
            per_mis[expected]["detected"] += 1
            details.append({
                "pid": pid, "shape": shape, "expected": expected,
                "outcome": "HIT",
                "detail": f"reported={sorted(reported_keys)}",
            })
        else:
            per_mis[expected]["misses"].append({
                "pid": pid, "shape": shape,
                "reported": sorted(reported_keys),
                "raw": sorted(raw_keys),
                "exec_error": exec_error,
            })
            details.append({
                "pid": pid, "shape": shape, "expected": expected,
                "outcome": "MISS",
                "detail": f"reported={sorted(reported_keys)} raw={sorted(raw_keys)}",
            })

    # ── Report ──────────────────────────────────────────────────────
    print()
    print("═══════════════════════════════════════════════════════════════════")
    print(" PHASE 4 / STEP 2  —  INDEPENDENT DETECTION-RATE EVALUATION")
    print("═══════════════════════════════════════════════════════════════════")
    print()
    print(f"{'Misconception':<28} {'Applied':>8} {'Detected':>10} {'Rate':>8} {'EquivSkip':>11} {'ExecErr':>8}")
    print("-" * 82)
    total_applied = 0
    total_detected = 0
    ordered_keys = [
        "MISSING_WHERE", "IN_vs_EXISTS", "NOT_IN_vs_NOT_EXISTS",
        "WRONG_JOIN_TYPE", "CARTESIAN_PRODUCT", "MISSING_GROUP_BY",
        "HAVING_vs_WHERE", "IN_FOR_DIVISION", "MISSING_CORRELATED_REF",
        "WRONG_SET_OP", "NULL_EQUALITY",
    ]
    for key in ordered_keys:
        s = per_mis[key]
        app, det = s["applied"], s["detected"]
        rate = (det / app * 100) if app else 0.0
        print(f"{key:<28} {app:>8} {det:>10} {rate:>7.1f}% "
              f"{s['equiv_skipped']:>11} {s['exec_err']:>8}")
        total_applied  += app
        total_detected += det
    print("-" * 82)
    overall = (total_detected / total_applied * 100) if total_applied else 0.0
    print(f"{'OVERALL':<28} {total_applied:>8} {total_detected:>10} {overall:>7.1f}%")

    print()
    print("─── M1–M10 subtotal (core taxonomy) ──────────────────────────────")
    m_keys = [k for k in ordered_keys if k != "NULL_EQUALITY"]
    m_applied  = sum(per_mis[k]["applied"]  for k in m_keys)
    m_detected = sum(per_mis[k]["detected"] for k in m_keys)
    m_rate = (m_detected / m_applied * 100) if m_applied else 0.0
    print(f"  M1-M10 only: {m_detected}/{m_applied} = {m_rate:.1f}%")

    # Miss details
    any_miss = any(per_mis[k]["misses"] for k in ordered_keys)
    if any_miss:
        print()
        print("─── Miss details ─────────────────────────────────────────────────")
        for key in ordered_keys:
            misses = per_mis[key]["misses"]
            if not misses:
                continue
            print(f"\n• {key}")
            for m in misses:
                print(f"    {m['pid']:<38} shape={m['shape']}")
                print(f"      reported={m['reported']}")
                print(f"      raw={m['raw']}")
                if m['exec_error']:
                    print(f"      exec_error: {m['exec_error']}")


if __name__ == "__main__":
    main()

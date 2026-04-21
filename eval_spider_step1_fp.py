"""
Phase 4, Step 1 — False-Positive Evaluation

For each alternate-correct query in `spider_corpus_step1_correct.py`:
  (1) execute both the reference base query and the alternate against
      the main DB and confirm they produce the same result set;
  (2) run the full detector on the pair and count any misconceptions
      that fire — every misconception here is a FALSE POSITIVE, because
      the alternate is by construction correct.

Reports:
  - Per-entry outcome (pass / fail with reason)
  - False-positive rate overall and per problem_type
  - List of the specific misconception keys that were raised spuriously,
    so detector weak points can be addressed.

Usage:
    python3 eval_spider_step1_fp.py
"""
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Ensure DBs are initialized
from database.init_db import init_main_db, init_edge_dbs
init_main_db()
init_edge_dbs()

from backend.sql_parser import parse_sql, compare_queries
from backend.feedback_generator import generate_feedback, _detect_misconceptions
from backend.query_executor import execute_query, compare_results
from backend.problems import get_problem

from spider_corpus_step1_correct import CORPUS


def main():
    # Organize
    by_type = defaultdict(lambda: {"total": 0, "exec_err": 0, "wrong_result": 0,
                                    "false_positives": 0, "clean": 0,
                                    "fp_entries": [], "err_entries": [],
                                    "raw_shape_flags": 0})
    totals = {"total": 0, "exec_err": 0, "wrong_result": 0,
              "false_positives": 0, "clean": 0, "raw_shape_flags": 0}
    per_miskey_fp = defaultdict(int)
    per_miskey_raw = defaultdict(int)
    per_entry_details = []

    for entry in CORPUS:
        pid = entry["problem_id"]
        label = entry["label"]
        pattern = entry["pattern"]
        student_sql = entry["query"].strip()

        problem = get_problem(pid)
        if problem is None:
            print(f"[SKIP] Unknown problem_id: {pid}")
            continue

        base_sql = problem["base_query"]
        ptype = problem["type"]
        by_type[ptype]["total"] += 1
        totals["total"] += 1

        # Parse
        base_parse = parse_sql(base_sql)
        student_parse = parse_sql(student_sql)

        # Execute both
        base_result    = execute_query(base_sql)
        student_result = execute_query(student_sql)

        # Flag executional errors
        if not student_result.success:
            by_type[ptype]["exec_err"] += 1
            totals["exec_err"] += 1
            by_type[ptype]["err_entries"].append((pid, label, student_result.error))
            per_entry_details.append({
                "pid": pid, "label": label, "outcome": "EXEC_ERR",
                "detail": student_result.error,
            })
            continue

        # Verify the alternate actually matches the reference
        comparison = compare_results(base_result, student_result)
        if not comparison.are_equivalent:
            by_type[ptype]["wrong_result"] += 1
            totals["wrong_result"] += 1
            per_entry_details.append({
                "pid": pid, "label": label, "outcome": "WRONG_RESULT",
                "detail": (f"base_rows={len(base_result.rows)}, "
                           f"student_rows={len(student_result.rows)}, "
                           f"extra={len(comparison.extra_rows)}, "
                           f"missing={len(comparison.missing_rows)}"),
            })
            continue

        # Run the full feedback pipeline (what the student actually sees).
        # No edge cases and no provenance for harness speed; the result-aware
        # filter behaves correctly when edge_results is empty.
        report = generate_feedback(
            base_parse=base_parse,
            student_parse=student_parse,
            comparison=comparison.to_dict(),
            edge_results=[],
            provenance_trace=None,
            problem_type=ptype,
            execution_error=None,
        )

        reported_mis = report.misconceptions
        raw_mis = report.raw_misconceptions
        raw_keys = [m["key"] for m in raw_mis]
        for k in raw_keys:
            per_miskey_raw[k] += 1
        if raw_mis:
            by_type[ptype]["raw_shape_flags"] += 1
            totals["raw_shape_flags"] += 1

        if reported_mis:
            by_type[ptype]["false_positives"] += 1
            totals["false_positives"] += 1
            keys = [m["key"] for m in reported_mis]
            for k in keys:
                per_miskey_fp[k] += 1
            by_type[ptype]["fp_entries"].append((pid, label, keys))
            per_entry_details.append({
                "pid": pid, "label": label, "outcome": "FALSE_POSITIVE",
                "detail": ", ".join(keys),
            })
        else:
            by_type[ptype]["clean"] += 1
            totals["clean"] += 1
            per_entry_details.append({
                "pid": pid, "label": label,
                "outcome": "CLEAN" + (" (raw-shape)" if raw_keys else ""),
                "detail": ", ".join(raw_keys) if raw_keys else "",
            })

    # ── Report ─────────────────────────────────────────────────
    print()
    print("═══════════════════════════════════════════════════════════════════")
    print(" PHASE 4 / STEP 1  —  FALSE-POSITIVE RATE (user-facing pipeline)")
    print("═══════════════════════════════════════════════════════════════════")
    print()
    print(f"Total alternate-correct queries tested: {totals['total']}")
    print(f"  Clean (report.misconceptions == [])  : {totals['clean']}")
    print(f"  False positives (user saw flags)     : {totals['false_positives']}")
    print(f"  Executional errors (bad corpus)      : {totals['exec_err']}")
    print(f"  Result-mismatch (not actually equiv) : {totals['wrong_result']}")
    if totals["total"]:
        valid = totals["total"] - totals["exec_err"] - totals["wrong_result"]
        fp_rate = totals["false_positives"] / valid * 100 if valid else 0
        print(f"\nUser-facing FP rate (excluding corpus errors): "
              f"{totals['false_positives']}/{valid} = {fp_rate:.1f}%")
        raw_rate = totals["raw_shape_flags"] / valid * 100 if valid else 0
        print(f"Raw shape-flag rate (pre-filter, for research): "
              f"{totals['raw_shape_flags']}/{valid} = {raw_rate:.1f}%")

    print()
    print("─── Per problem type ────────────────────────────────────────────")
    print(f"{'Type':<14} {'Total':>6} {'Clean':>6} {'FP':>4} {'RawShape':>9} {'ExecErr':>8} {'Wrong':>6}  FP%")
    for ptype, stats in sorted(by_type.items()):
        valid = stats["total"] - stats["exec_err"] - stats["wrong_result"]
        rate = (stats["false_positives"] / valid * 100) if valid else 0
        print(f"{ptype:<14} {stats['total']:>6} {stats['clean']:>6} "
              f"{stats['false_positives']:>4} {stats['raw_shape_flags']:>9} "
              f"{stats['exec_err']:>8} {stats['wrong_result']:>6}  {rate:>5.1f}%")

    # False positives by misconception key (post-filter — these are the ones
    # that survived the result-aware check)
    if per_miskey_fp:
        print()
        print("─── Surviving false-positive misconceptions ────────────────────")
        for k, c in sorted(per_miskey_fp.items(), key=lambda kv: -kv[1]):
            print(f"  {k:<30} {c} occurrence(s)")

    # Raw shape flags (for research / paper)
    if per_miskey_raw:
        print()
        print("─── Raw shape-level flags (pre-filter) ─────────────────────────")
        for k, c in sorted(per_miskey_raw.items(), key=lambda kv: -kv[1]):
            print(f"  {k:<30} {c} shape flag(s) across alt-correct queries")

    # Detail listing
    if totals["false_positives"]:
        print()
        print("─── User-facing false-positive details ─────────────────────────")
        for ptype, stats in sorted(by_type.items()):
            for pid, label, keys in stats["fp_entries"]:
                print(f"  [{ptype}] {pid:<38} alt={label:<28} → {keys}")

    if totals["exec_err"] or totals["wrong_result"]:
        print()
        print("─── Corpus problems (please review) ────────────────────────────")
        for d in per_entry_details:
            if d["outcome"] in ("EXEC_ERR", "WRONG_RESULT"):
                print(f"  [{d['outcome']}] {d['pid']} / {d['label']}:  {d['detail']}")


if __name__ == "__main__":
    main()

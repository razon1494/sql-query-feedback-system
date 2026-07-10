"""
Phase 3.2 — Literature-motivated wrong-query corpus over Spider dev.

Applies misconception-inducing corruption operators to real Spider gold
queries, producing the external DETECTION experiment (the counterpart of the
Phase 3.1 false-positive experiment).

Anti-circularity design
-----------------------
* Each operator is defined from a documented novice misconception in the
  empirical literature (citations below), NOT from the detector's rule
  branches. The operator set includes `in_to_uncorrelated_exists`, a
  well-documented novice error the current detector is NOT expected to catch —
  a detector-aware corpus would have omitted it; a literature-driven one
  cannot.
* Gold queries are deduplicated real Spider dev examples on 20 foreign
  schemas; corruption is purely syntactic/structural and schema-blind.
* Every corrupted query is execution-validated on the real Spider database:
  if its output equals gold's, it is a latent bug and counted as SKIPPED, not
  as a detection opportunity (same convention as the paper's Experiment 3).

Operators (expected misconception, literature basis)
----------------------------------------------------
  drop_where            M1  MISSING_WHERE           Taipalus 2018 (missing expressions);
                                                    Brass & Goldberg 2006
  inner_to_left         M4  WRONG_JOIN_TYPE         Miedema et al. 2021/2022
                                                    (join-semantics confusion)
  join_to_cartesian     M5  CARTESIAN_PRODUCT       Taipalus 2018; Miedema 2021
                                                    (missing join condition)
  drop_group_by         M6  MISSING_GROUP_BY        Taipalus 2018 (grouping errors)
  having_to_where       M7  HAVING_vs_WHERE         Miedema et al. 2022
  swap_set_op           M10 WRONG_SET_OP            Miedema et al. 2021
  in_to_uncorrelated_exists
                        M9  MISSING_CORRELATED_REF  Miedema et al. 2021 (EXISTS
                                                    misunderstood as set test)

M2/M3/NULL_EQUALITY have no corruptible gold in Spider dev (no EXISTS, no
NOT EXISTS, no IS NULL — see division_scan.py / classify.py); M3 and M9 are
additionally covered by the division-transfer experiment (gen_division.py).
Applicability is reported per operator so this coverage is explicit.

Usage:
    python external/spider/gen_wrong.py --split dev
"""
import os
import re
import sys
import json
from collections import defaultdict
from typing import Optional, Dict, List, Tuple

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from backend.sql_parser import (                             # noqa: E402
    parse_sql, _extract_top_level_clauses, _parse_joins, _keyword_split,
    _mask_nested_parens,
)
from backend.feedback_generator import _has_aggregate         # noqa: E402
from external.spider.classify import classify_examples        # noqa: E402
from external.spider.gen_alternates import _rebuild, _lead_table_text  # noqa: E402
from external.harness.generic_problem import GenericProblem, analyze   # noqa: E402

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")
_OUTER_JOIN_RE = re.compile(r"\b(LEFT|RIGHT|FULL|CROSS|NATURAL|OUTER)\b", re.IGNORECASE)


def _norm(sql: str) -> str:
    return " ".join((sql or "").upper().split())


# ── operators ────────────────────────────────────────────────────────────────

def op_drop_where(sql: str, parse, clauses) -> Optional[str]:
    """M1: the student forgets the row filter entirely."""
    if parse.set_operation or not clauses.get("WHERE"):
        return None
    return _rebuild(clauses, clauses["FROM"], None)


def op_inner_to_left(sql: str, parse, clauses) -> Optional[str]:
    """M4: inner join replaced by LEFT JOIN (outer-join semantics confusion)."""
    if parse.set_operation:
        return None
    if not re.search(r"\bJOIN\b", sql, re.IGNORECASE):
        return None
    if _OUTER_JOIN_RE.search(sql):
        return None                      # gold already uses outer joins
    out = re.sub(r"\bINNER\s+JOIN\b", "JOIN", sql, flags=re.IGNORECASE)
    out = re.sub(r"\bJOIN\b", "LEFT JOIN", out, flags=re.IGNORECASE)
    return out if _norm(out) != _norm(sql) else None


def op_join_to_cartesian(sql: str, parse, clauses) -> Optional[str]:
    """M5: the join condition is dropped (comma-join, no linking predicate)."""
    if parse.set_operation or not clauses.get("FROM"):
        return None
    from_text = clauses["FROM"]
    if _OUTER_JOIN_RE.search(from_text):
        return None
    joins = _parse_joins(from_text)
    if len(joins) != 1:                  # single-join golds only: exactly one
        return None                      # misconception is introduced
    j = joins[0]
    lead = _lead_table_text(from_text)
    entry = f"{j['table']} {j['alias']}".strip() if j.get("alias") else j["table"]
    return _rebuild(clauses, f"{lead}, {entry}", clauses.get("WHERE"))


def op_drop_group_by(sql: str, parse, clauses) -> Optional[str]:
    """M6: aggregate query written without GROUP BY."""
    if parse.set_operation or not clauses.get("GROUP BY"):
        return None
    if not parse.select_cols or len(parse.select_cols) < 2:
        return None                      # need agg + non-agg mix to be the error
    clauses2 = dict(clauses)
    clauses2.pop("GROUP BY", None)
    return _rebuild(clauses2, clauses2["FROM"], clauses2.get("WHERE"))


def op_having_to_where(sql: str, parse, clauses) -> Optional[str]:
    """M7: the aggregate group filter is written in WHERE."""
    having = clauses.get("HAVING")
    if parse.set_operation or not having or not _has_aggregate(having):
        return None
    clauses2 = dict(clauses)
    clauses2.pop("HAVING", None)
    where = clauses2.get("WHERE")
    new_where = f"{where.strip()} AND {having.strip()}" if where else having.strip()
    return _rebuild(clauses2, clauses2["FROM"], new_where)


_SET_OP_SWAP = {"UNION ALL": "INTERSECT", "UNION": "INTERSECT",
                "INTERSECT": "UNION", "EXCEPT": "INTERSECT"}


def op_swap_set_op(sql: str, parse, clauses) -> Optional[str]:
    """M10: the wrong set operator is chosen."""
    op = (parse.set_operation or "").upper()
    if op not in _SET_OP_SWAP:
        return None
    left, right = _keyword_split(sql, op)
    if right is None:
        return None
    return f"{left} {_SET_OP_SWAP[op]} {right}"


def op_in_to_uncorrelated_exists(sql: str, parse, clauses) -> Optional[str]:
    """M9 (gap probe): IN-subquery rewritten as an UNCORRELATED EXISTS — the
    documented novice belief that EXISTS tests set membership by itself. The
    result no longer depends on the outer row. The current detector anchors
    its correlation check on the reference shape, so this documented error
    pattern is expected to be MISSED; it is included because a literature-
    driven corpus cannot omit it."""
    if parse.set_operation or parse.where_type != "IN":
        return None
    where = (clauses.get("WHERE") or "").strip()
    # WHERE must be exactly `<col> IN (<subquery>)` — no other conjuncts.
    masked = " ".join(_mask_nested_parens(where).upper().split())
    if " AND " in f" {masked} " or " OR " in f" {masked} ":
        return None
    m = re.fullmatch(r"([\w.]+)\s+IN\s*\(\s*(SELECT\s.*)\)\s*", where,
                     re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return _rebuild(clauses, clauses["FROM"], f"EXISTS ({m.group(2).strip()})")


OPERATORS: List[Tuple[str, str, str]] = [
    # (operator name, expected misconception key, literature basis)
    ("drop_where", "MISSING_WHERE", "Taipalus 2018; Brass & Goldberg 2006"),
    ("inner_to_left", "WRONG_JOIN_TYPE", "Miedema et al. 2021/2022"),
    ("join_to_cartesian", "CARTESIAN_PRODUCT", "Taipalus 2018; Miedema 2021"),
    ("drop_group_by", "MISSING_GROUP_BY", "Taipalus 2018"),
    ("having_to_where", "HAVING_vs_WHERE", "Miedema et al. 2022"),
    ("swap_set_op", "WRONG_SET_OP", "Miedema et al. 2021"),
    ("in_to_uncorrelated_exists", "MISSING_CORRELATED_REF", "Miedema et al. 2021"),
]

_OP_FN = {
    "drop_where": op_drop_where,
    "inner_to_left": op_inner_to_left,
    "join_to_cartesian": op_join_to_cartesian,
    "drop_group_by": op_drop_group_by,
    "having_to_where": op_having_to_where,
    "swap_set_op": op_swap_set_op,
    "in_to_uncorrelated_exists": op_in_to_uncorrelated_exists,
}


# ── driver ───────────────────────────────────────────────────────────────────

def generate(root: Optional[str] = None, split: str = "dev",
             limit: Optional[int] = None) -> List[Dict]:
    """Build the wrong-query corpus: dedup gold, apply every applicable
    operator, keep all corruptions (validation happens at evaluation time so
    latent bugs are reported rather than hidden)."""
    records = classify_examples(root, split=split)
    seen = set()
    golds = []
    for r in records:
        key = (r["db_id"], _norm(r["gold_sql"]))
        if key in seen:
            continue
        seen.add(key)
        golds.append(r)
    if limit:
        golds = golds[:limit]

    corpus: List[Dict] = []
    for rec in golds:
        parse = parse_sql(rec["gold_sql"])
        if parse.error:
            continue
        clauses = _extract_top_level_clauses(rec["gold_sql"])
        for op_name, expected, lit in OPERATORS:
            wrong = _OP_FN[op_name](rec["gold_sql"], parse, clauses)
            if not wrong or _norm(wrong) == _norm(rec["gold_sql"]):
                continue
            corpus.append({
                "db_id": rec["db_id"],
                "sqlite_path": rec["sqlite_path"],
                "problem_type": rec["problem_type"],
                "question": rec["question"],
                "operator": op_name,
                "expected": expected,
                "literature": lit,
                "gold_sql": rec["gold_sql"],
                "wrong_sql": " ".join(wrong.split()),
            })
    return corpus


def evaluate(corpus: List[Dict], verbose: bool = True) -> Dict:
    per_op = {name: {"generated": 0, "applied": 0, "detected": 0,
                     "skipped": 0, "exec_err": 0, "misses": []}
              for name, _e, _l in OPERATORS}

    for entry in corpus:
        s = per_op[entry["operator"]]
        s["generated"] += 1
        problem = GenericProblem(
            problem_id=f"{entry['db_id']}::{entry['operator']}",
            db_path=entry["sqlite_path"],
            gold_sql=entry["gold_sql"],
            problem_type=entry["problem_type"],
            db_id=entry["db_id"],
            source="spider_wrong",
        )
        res = analyze(problem, entry["wrong_sql"])
        entry["detected_keys"] = res.detected
        entry["raw_keys"] = res.raw
        entry["are_equivalent"] = res.are_equivalent

        if res.are_equivalent and res.edges_ok:
            s["skipped"] += 1            # latent: output-identical on this data
            entry["outcome"] = "LATENT_SKIP"
            continue
        s["applied"] += 1
        if res.student_exec_error:
            s["exec_err"] += 1
        if entry["expected"] in res.detected:
            s["detected"] += 1
            entry["outcome"] = "HIT"
        else:
            entry["outcome"] = "MISS"
            s["misses"].append({"db_id": entry["db_id"],
                                "detected": res.detected, "raw": res.raw})

    summary = {"per_op": per_op,
               "corpus_size": len(corpus),
               "golds_touched": len({(e['db_id'], _norm(e['gold_sql'])) for e in corpus}),
               "dbs": len({e["db_id"] for e in corpus})}
    if verbose:
        _print_summary(summary)
    return summary


def _print_summary(s: Dict) -> None:
    print("=" * 78)
    print(" PHASE 3.2 - Literature-motivated wrong-query detection (Spider dev)")
    print("=" * 78)
    print(f"corpus: {s['corpus_size']} wrong queries from {s['golds_touched']} "
          f"distinct golds over {s['dbs']} databases")
    print("-" * 78)
    print(f"{'Operator':<28}{'Gen':>6}{'Applied':>9}{'Detected':>9}{'Rate':>8}"
          f"{'Skip*':>7}{'ExecE':>7}")
    tot_app = tot_det = 0
    for name, expected, _lit in OPERATORS:
        v = s["per_op"][name]
        rate = v["detected"] / v["applied"] * 100 if v["applied"] else 0.0
        print(f"{name:<28}{v['generated']:>6}{v['applied']:>9}{v['detected']:>9}"
              f"{rate:>7.1f}%{v['skipped']:>7}{v['exec_err']:>7}")
        tot_app += v["applied"]
        tot_det += v["detected"]
    print("-" * 78)
    overall = tot_det / tot_app * 100 if tot_app else 0.0
    print(f"{'OVERALL':<28}{'':>6}{tot_app:>9}{tot_det:>9}{overall:>7.1f}%")
    print("* skip = corruption produced output identical to gold on the real DB")
    for name, _e, _l in OPERATORS:
        misses = s["per_op"][name]["misses"]
        if misses:
            print(f"\n  {name}: {len(misses)} miss(es); first 3:")
            for m in misses[:3]:
                print(f"    {m['db_id']}: detected={m['detected']} raw={m['raw']}")


def main(root=None, split="dev", limit=None, save=True) -> Dict:
    corpus = generate(root, split=split, limit=limit)
    summary = evaluate(corpus, verbose=True)
    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        cpath = os.path.join(_DERIVED_DIR, f"spider_wrong_{split}.json")
        with open(cpath, "w", encoding="utf-8") as fh:
            json.dump([{k: v for k, v in e.items() if k != "sqlite_path"}
                       for e in corpus], fh, indent=1)
        spath = os.path.join(_DERIVED_DIR, f"spider_wrong_{split}_summary.json")
        with open(spath, "w", encoding="utf-8") as fh:
            json.dump({"per_op": summary["per_op"],
                       "corpus_size": summary["corpus_size"],
                       "golds_touched": summary["golds_touched"],
                       "dbs": summary["dbs"]}, fh, indent=1)
        print(f"\nWrote:\n  {cpath}\n  {spath}")
    return summary


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Generate + evaluate Spider wrong-query corpus")
    ap.add_argument("--root", default=None)
    ap.add_argument("--split", default="dev")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-save", action="store_true")
    args = ap.parse_args()
    main(root=args.root, split=args.split, limit=args.limit, save=not args.no_save)

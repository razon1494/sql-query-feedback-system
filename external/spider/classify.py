"""
Phase 2 — Problem-type auto-classifier for Spider gold queries.

Each Spider gold query is tagged with one of the six problem types the detector
dispatches on:

    DIVISION | JOIN | AGGREGATION | SET_OP | SUBQUERY | NULL

Why this matters
----------------
`generate_feedback` runs a per-type detector (`_detect_division`,
`_detect_aggregation`, ...) plus the type-independent `_detect_common`. The type
tag decides which type-specific misconception family is checked, so every Spider
problem needs a type before it can enter the evaluation.

Classification is structural, computed from the query's parse (via the project's
own `parse_sql`, so keywords inside subqueries are masked the same way the
detector sees them). When a query exhibits several structures at once, a fixed
precedence decides the label:

    SET_OP  >  DIVISION  >  AGGREGATION  >  SUBQUERY  >  NULL  >  JOIN

Rationale for the order:
* SET_OP is unambiguous (top-level UNION/INTERSECT/EXCEPT).
* DIVISION (universal quantification via double negation) is the rarest and most
  specific shape — checked before the aggregation/subquery structures it
  contains.
* AGGREGATION (top-level GROUP BY / HAVING / aggregate) dominates a query's
  misconception surface when present.
* SUBQUERY covers remaining nested / EXISTS / IN forms.
* NULL catches simple IS NULL filters.
* JOIN is the catch-all (multi-table or plain filter); note `_detect_common`
  still checks cartesian/join-type/null issues for EVERY type, so a JOIN default
  never loses those signals.

This precedence is a documented modeling choice, not ground truth — a stratified
sample is dumped for hand-verification (see `--sample`).
"""
import os
import sys
import json
from collections import Counter
from typing import Dict, List, Optional

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from backend.sql_parser import parse_sql                    # noqa: E402
from backend.feedback_generator import _has_aggregate        # noqa: E402
from external.spider.ingest import load_examples, resolve_root  # noqa: E402

PROBLEM_TYPES = ["DIVISION", "JOIN", "AGGREGATION", "SET_OP", "SUBQUERY", "NULL"]
_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")


def _norm(sql: str) -> str:
    return " ".join((sql or "").upper().split())


def _is_division_shape(raw_upper: str) -> bool:
    """Universal quantification expressed via double negation.

    Detects the `NOT EXISTS ( ... NOT EXISTS ... )` and
    `NOT EXISTS ( ... NOT IN ... )` idioms. Deliberately strict — this is near
    non-existent in Spider, and that scarcity is itself a reportable finding.
    """
    if "NOT EXISTS" not in raw_upper:
        return False
    return raw_upper.count("NOT EXISTS") >= 2 or "NOT IN" in raw_upper


def _has_top_level_aggregate(parse) -> bool:
    if parse.group_by or parse.having:
        return True
    return any(_has_aggregate(c) for c in parse.select_cols)


def _is_subquery_shape(parse) -> bool:
    if parse.subqueries:
        return True
    return parse.where_type in ("IN", "EXISTS", "NOT_IN", "NOT_EXISTS")


def _is_null_shape(parse) -> bool:
    wc = (parse.where_clause or "").upper()
    return " IS NULL" in wc or "IS NOT NULL" in wc


def _classify_raw(raw_upper: str) -> str:
    """Fallback when the parser errors: classify from normalized raw text."""
    if any(op in raw_upper for op in (" UNION ", " INTERSECT ", " EXCEPT ")):
        return "SET_OP"
    if _is_division_shape(raw_upper):
        return "DIVISION"
    if "GROUP BY" in raw_upper or "HAVING" in raw_upper or \
            any(f"{fn}(" in raw_upper.replace(" ", "") for fn in
                ("COUNT", "SUM", "AVG", "MIN", "MAX")):
        return "AGGREGATION"
    if "SELECT" in raw_upper[raw_upper.find("SELECT") + 6:]:  # a nested SELECT
        return "SUBQUERY"
    if " IS NULL" in raw_upper or "IS NOT NULL" in raw_upper:
        return "NULL"
    return "JOIN"


def classify_type(gold_sql: str) -> Dict:
    """Return {'type': <PROBLEM_TYPE>, 'parse_ok': bool} for one gold query."""
    parse = parse_sql(gold_sql)
    raw_upper = _norm(gold_sql)

    if parse.error:
        return {"type": _classify_raw(raw_upper), "parse_ok": False}

    if parse.set_operation:
        ptype = "SET_OP"
    elif _is_division_shape(raw_upper):
        ptype = "DIVISION"
    elif _has_top_level_aggregate(parse):
        ptype = "AGGREGATION"
    elif _is_subquery_shape(parse):
        ptype = "SUBQUERY"
    elif _is_null_shape(parse):
        ptype = "NULL"
    else:
        ptype = "JOIN"
    return {"type": ptype, "parse_ok": True}


def classify_examples(root: Optional[str] = None, split: str = "dev") -> List[Dict]:
    """Classify every example in a split; return enriched records."""
    examples = load_examples(root, split=split, require_sqlite=True)
    out: List[Dict] = []
    for ex in examples:
        c = classify_type(ex.gold_sql)
        out.append({
            "db_id": ex.db_id,
            "question": ex.question,
            "gold_sql": ex.gold_sql,
            "sqlite_path": ex.sqlite_path,
            "problem_type": c["type"],
            "parse_ok": c["parse_ok"],
        })
    return out


def _stratified_sample(records: List[Dict], per_type: int = 8) -> List[Dict]:
    buckets: Dict[str, List[Dict]] = {t: [] for t in PROBLEM_TYPES}
    for r in records:
        b = buckets[r["problem_type"]]
        if len(b) < per_type:
            b.append({"problem_type": r["problem_type"], "db_id": r["db_id"],
                      "question": r["question"], "gold_sql": r["gold_sql"]})
    sample: List[Dict] = []
    for t in PROBLEM_TYPES:
        sample.extend(buckets[t])
    return sample


def main(root: Optional[str] = None, split: str = "dev",
         save: bool = True, sample_per_type: int = 8) -> Dict:
    records = classify_examples(root, split=split)
    dist = Counter(r["problem_type"] for r in records)
    parse_fail = sum(1 for r in records if not r["parse_ok"])
    total = len(records)

    print("=" * 40)
    print(f" PHASE 2 - Spider {split} problem-type distribution")
    print("=" * 40)
    print(f"{'Type':<14}{'Count':>8}{'Share':>9}")
    print("-" * 31)
    for t in PROBLEM_TYPES:
        n = dist.get(t, 0)
        pct = (n / total * 100) if total else 0.0
        print(f"{t:<14}{n:>8}{pct:>8.1f}%")
    print("-" * 31)
    print(f"{'TOTAL':<14}{total:>8}{100.0:>8.1f}%")
    print(f"\nUnparseable by project parser (raw fallback used): {parse_fail}/{total}")
    print("\nCoverage note for the paper: Spider is dominated by JOIN/AGGREGATION/")
    print("SUBQUERY/SET_OP. DIVISION (universal quantification) is near-absent -")
    print("the in-house university corpus supplies division depth; Spider supplies")
    print("cross-schema breadth for the other misconception families.")

    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        full_path = os.path.join(_DERIVED_DIR, f"classified_{split}.json")
        with open(full_path, "w", encoding="utf-8") as fh:
            json.dump(records, fh, indent=1)
        sample = _stratified_sample(records, per_type=sample_per_type)
        sample_path = os.path.join(_DERIVED_DIR, f"classify_sample_{split}.json")
        with open(sample_path, "w", encoding="utf-8") as fh:
            json.dump(sample, fh, indent=1)
        print(f"\nWrote:\n  {full_path}\n  {sample_path}  (hand-verify these)")

    return {"distribution": dict(dist), "total": total, "parse_fail": parse_fail}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Classify Spider gold queries by problem type")
    ap.add_argument("--root", default=None)
    ap.add_argument("--split", default="dev")
    ap.add_argument("--no-save", action="store_true")
    ap.add_argument("--sample", type=int, default=8, help="samples per type to dump")
    args = ap.parse_args()
    main(root=args.root, split=args.split, save=not args.no_save,
         sample_per_type=args.sample)

"""
Phase 3 (part 1) — Alternate-correct query generator for Spider.

Produces *structurally different but semantically equivalent* rewrites of Spider
gold queries. These feed the FALSE-POSITIVE experiment: a correct query written
in a different shape must NOT raise a user-facing misconception.

Non-circularity & correctness guarantees
----------------------------------------
* The rewrite operators are generic SQL equivalences (inner-join <-> comma-join;
  join -> IN-subquery). They are defined from relational-algebra equivalence,
  with **no reference to the detector's branches**. This is deliberately unlike
  the original hand-authored corpus, which was written to trip specific detector
  keys.
* **Every candidate is execution-validated**: it is kept only if it produces the
  exact same result set as gold on the real Spider SQLite database. Anything a
  rewrite gets wrong is silently dropped, so the corpus is correct by
  construction.

What the two operators demonstrate
----------------------------------
* `join_to_comma`  : large, clean surface change; the detector should stay
  silent even at the raw (pre-filter) level.
* `dejoin_to_in`   : drops a top-level join in favour of an IN-subquery, which
  the raw shape classifier flags as MISSING_JOIN. Since output is unchanged, the
  result-aware filter must suppress it. The rate at which this happens is the
  external-schema analogue of the paper's 64.9% -> 0% finding.

Usage
-----
    python external/spider/gen_alternates.py --split dev
"""
import os
import re
import sys
import json
from collections import Counter, defaultdict
from typing import Optional, List, Dict, Tuple

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from backend.sql_parser import (                              # noqa: E402
    parse_sql, _extract_top_level_clauses, _parse_joins, _parse_select_cols,
)
from external.spider.classify import classify_examples        # noqa: E402
from external.harness.generic_problem import GenericProblem, analyze  # noqa: E402

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")
_OUTER_JOIN_RE = re.compile(r"\b(LEFT|RIGHT|FULL|CROSS|NATURAL|OUTER)\b", re.IGNORECASE)


# ── clause reassembly ────────────────────────────────────────────────────────

def _rebuild(clauses: Dict[str, str], from_text: str, where_text: Optional[str]) -> str:
    parts = [f"SELECT {clauses['SELECT'].strip()}", f"FROM {from_text.strip()}"]
    if where_text and where_text.strip():
        parts.append(f"WHERE {where_text.strip()}")
    for kw in ("GROUP BY", "HAVING", "ORDER BY", "LIMIT"):
        if clauses.get(kw):
            parts.append(f"{kw} {clauses[kw].strip()}")
    return " ".join(parts)


def _lead_table_text(from_text: str) -> str:
    """The FROM entry before the first JOIN, with a trailing INNER stripped."""
    lead = re.split(r"\bJOIN\b", from_text, maxsplit=1, flags=re.IGNORECASE)[0]
    return re.sub(r"\bINNER\b", "", lead, flags=re.IGNORECASE).strip()


# ── Operator A: inner-JOIN  ->  comma-join ──────────────────────────────────

def op_join_to_comma(gold_sql: str, parse) -> Optional[str]:
    if parse.error or parse.set_operation:
        return None
    clauses = _extract_top_level_clauses(gold_sql)
    if "SELECT" not in clauses or "FROM" not in clauses:
        return None
    from_text = clauses["FROM"]
    if not re.search(r"\bJOIN\b", from_text, re.IGNORECASE):
        return None
    if _OUTER_JOIN_RE.search(from_text):
        return None  # comma-join cannot express outer joins
    joins = _parse_joins(from_text)
    if not joins:
        return None

    entries = [_lead_table_text(from_text)]
    conds: List[str] = []
    for j in joins:
        alias = j.get("alias")
        entries.append(f"{j['table']} {alias}".strip() if alias else j["table"])
        cond = (j.get("condition") or "").strip()
        if not cond:
            return None
        conds.append(cond)

    comma_from = ", ".join(e for e in entries if e)
    join_pred = " AND ".join(f"({c})" for c in conds)
    existing_where = (clauses.get("WHERE") or "").strip()
    where_text = f"{join_pred} AND ({existing_where})" if existing_where else join_pred
    variant = _rebuild(clauses, comma_from, where_text)
    return variant if _norm(variant) != _norm(gold_sql) else None


# ── Operator B: drop one join  ->  IN-subquery ──────────────────────────────

def op_dejoin_to_in(gold_sql: str, parse) -> Optional[str]:
    if parse.error or parse.set_operation:
        return None
    if parse.group_by or parse.having:
        return None
    clauses = _extract_top_level_clauses(gold_sql)
    if "SELECT" not in clauses or "FROM" not in clauses:
        return None
    from_text = clauses["FROM"]
    if _OUTER_JOIN_RE.search(from_text):
        return None
    joins = _parse_joins(from_text)
    if len(joins) != 1:
        return None

    j = joins[0]
    m = re.fullmatch(r"\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*", j.get("condition") or "")
    if not m:
        return None
    a1, c1, a2, c2 = m.groups()

    lead = _lead_table_text(from_text)
    lm = re.fullmatch(r"(\w+)(?:\s+(?:AS\s+)?(\w+))?", lead.strip(), re.IGNORECASE)
    if not lm:
        return None
    lead_tab, lead_alias = lm.group(1), (lm.group(2) or lm.group(1))
    join_tab, join_alias = j["table"], (j.get("alias") or j["table"])

    # Orient the equi-join: one side must be the leading alias, the other the join alias.
    if a1.upper() == lead_alias.upper() and a2.upper() == join_alias.upper():
        lead_key, join_key = (lead_alias, c1), (join_alias, c2)
    elif a2.upper() == lead_alias.upper() and a1.upper() == join_alias.upper():
        lead_key, join_key = (lead_alias, c2), (join_alias, c1)
    else:
        return None

    # Every selected column must be a simple column qualified with the leading alias.
    sel_body = re.sub(r"^\s*DISTINCT\s+", "", clauses["SELECT"], flags=re.IGNORECASE)
    for col in _parse_select_cols(sel_body):
        if not re.fullmatch(rf"{lead_alias}\.\w+", col.strip(), re.IGNORECASE):
            return None

    # Partition WHERE conjuncts: join-table-only -> subquery, lead-only -> keep.
    keep_conj: List[str] = []
    sub_conj: List[str] = []
    where_text = clauses.get("WHERE")
    if where_text:
        for conj in re.split(r"\bAND\b", where_text, flags=re.IGNORECASE):
            conj = conj.strip()
            if not conj:
                continue
            refs_join = re.search(rf"\b{join_alias}\.", conj, re.IGNORECASE)
            refs_lead = re.search(rf"\b{lead_alias}\.", conj, re.IGNORECASE)
            if refs_join and not refs_lead:
                sub_conj.append(conj)
            elif refs_lead and not refs_join:
                keep_conj.append(conj)
            else:
                return None  # mixed / unqualified conjunct — not safe to move

    sub_where = f" WHERE {' AND '.join(sub_conj)}" if sub_conj else ""
    subq = (f"{lead_key[0]}.{lead_key[1]} IN "
            f"(SELECT {join_key[0]}.{join_key[1]} FROM {join_tab} {join_alias}{sub_where})")
    new_where = " AND ".join(keep_conj + [subq]) if keep_conj else subq
    from2 = lead_tab if lead_alias == lead_tab else f"{lead_tab} {lead_alias}"
    variant = _rebuild(clauses, from2, new_where)
    return variant if _norm(variant) != _norm(gold_sql) else None


OPERATORS = [
    ("join_to_comma", op_join_to_comma),
    ("dejoin_to_in", op_dejoin_to_in),
]


def _norm(sql: str) -> str:
    return " ".join((sql or "").upper().split())


# ── generation driver ────────────────────────────────────────────────────────

def generate(root: Optional[str] = None, split: str = "dev",
             limit: Optional[int] = None, verbose: bool = True) -> Tuple[List[Dict], Dict]:
    records = classify_examples(root, split=split)
    if limit:
        records = records[:limit]

    corpus: List[Dict] = []
    op_yield: Counter = Counter()
    op_attempt: Counter = Counter()
    for rec in records:
        parse = parse_sql(rec["gold_sql"])
        problem = GenericProblem(
            problem_id=f"{rec['db_id']}::{rec['problem_type']}",
            db_path=rec["sqlite_path"],
            gold_sql=rec["gold_sql"],
            problem_type=rec["problem_type"],
            db_id=rec["db_id"],
            source="spider",
        )
        for op_name, op in OPERATORS:
            variant = op(rec["gold_sql"], parse)
            if not variant:
                continue
            op_attempt[op_name] += 1
            res = analyze(problem, variant)
            # Keep only execution-validated equivalents (a real alternate-correct).
            if not res.are_equivalent or res.gold_exec_error or res.student_exec_error:
                continue
            op_yield[op_name] += 1
            corpus.append({
                "db_id": rec["db_id"],
                "problem_type": rec["problem_type"],
                "operator": op_name,
                "gold_sql": rec["gold_sql"],
                "alt_sql": variant,
                "raw": res.raw,             # pre-filter shape flags
                "detected": res.detected,   # user-facing (should be empty)
            })

    # ── metrics ──
    n = len(corpus)
    fp = sum(1 for e in corpus if e["detected"])
    raw_flagged = sum(1 for e in corpus if e["raw"])
    by_type = defaultdict(lambda: {"n": 0, "raw": 0, "fp": 0})
    for e in corpus:
        bt = by_type[e["problem_type"]]
        bt["n"] += 1
        bt["raw"] += 1 if e["raw"] else 0
        bt["fp"] += 1 if e["detected"] else 0

    summary = {
        "gold_processed": len(records),
        "alternates_kept": n,
        "op_attempt": dict(op_attempt),
        "op_yield": dict(op_yield),
        "user_facing_fp": fp,
        "fpr_pct": (fp / n * 100) if n else 0.0,
        "raw_flagged": raw_flagged,
        "raw_flag_rate_pct": (raw_flagged / n * 100) if n else 0.0,
        "by_type": {k: dict(v) for k, v in by_type.items()},
    }

    if verbose:
        _print_summary(summary)
    return corpus, summary


def _print_summary(s: Dict) -> None:
    print("=" * 60)
    print(" PHASE 3.1 - Spider alternate-correct corpus")
    print("=" * 60)
    print(f"gold queries processed : {s['gold_processed']}")
    print(f"alternates kept (valid): {s['alternates_kept']}")
    print(f"  per operator (kept/attempted):")
    for op_name, _ in OPERATORS:
        print(f"    {op_name:<16} {s['op_yield'].get(op_name,0):>5} / {s['op_attempt'].get(op_name,0):<5}")
    print("-" * 60)
    print(f"USER-FACING false positives : {s['user_facing_fp']} / {s['alternates_kept']} "
          f"= {s['fpr_pct']:.1f}%   <-- the headline")
    print(f"raw shape-flag rate (pre-filter): {s['raw_flagged']} / {s['alternates_kept']} "
          f"= {s['raw_flag_rate_pct']:.1f}%   <-- what the filter suppressed")
    print("-" * 60)
    print(f"{'type':<14}{'n':>6}{'raw%':>8}{'fp%':>7}")
    for t, v in sorted(s["by_type"].items()):
        rawp = (v["raw"] / v["n"] * 100) if v["n"] else 0.0
        fpp = (v["fp"] / v["n"] * 100) if v["n"] else 0.0
        print(f"{t:<14}{v['n']:>6}{rawp:>7.1f}%{fpp:>6.1f}%")


def main(root: Optional[str] = None, split: str = "dev",
         limit: Optional[int] = None, save: bool = True) -> Dict:
    corpus, summary = generate(root, split=split, limit=limit, verbose=True)
    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        path = os.path.join(_DERIVED_DIR, f"spider_alternates_{split}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(corpus, fh, indent=1)
        spath = os.path.join(_DERIVED_DIR, f"spider_alternates_{split}_summary.json")
        with open(spath, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1)
        print(f"\nWrote:\n  {path}\n  {spath}")
    return summary


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Generate Spider alternate-correct corpus")
    ap.add_argument("--root", default=None)
    ap.add_argument("--split", default="dev")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-save", action="store_true")
    args = ap.parse_args()
    main(root=args.root, split=args.split, limit=args.limit, save=not args.no_save)

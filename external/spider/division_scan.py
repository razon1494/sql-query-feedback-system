"""
Measure how many division-shaped (universal quantification) queries exist in
Spider itself. This turns "Spider lacks division" from an assumption into a
reported, reproducible finding that justifies the schema-transfer design of
gen_division.py.

Shapes counted (on whitespace-normalized uppercase SQL):
  * not_exists_any        : at least one NOT EXISTS
  * double_negation       : >= 2 NOT EXISTS  (the canonical division idiom)
  * not_exists_plus_not_in: NOT EXISTS combined with NOT IN (division variant)
  * nested_not_in         : NOT IN whose subquery contains another NOT IN
  * having_count_eq_subq  : HAVING COUNT(...) = (SELECT COUNT(...)  (division
                            by counting)

Usage:  python external/spider/division_scan.py
"""
import os
import re
import sys
import json

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.spider.ingest import resolve_root  # noqa: E402

SPLIT_FILES = ["dev.json", "train_spider.json", "train_others.json"]

_HAVING_CNT_RE = re.compile(r"HAVING\s+COUNT\s*\([^)]*\)\s*=\s*\(\s*SELECT\s+COUNT", re.IGNORECASE)
_NESTED_NOT_IN_RE = re.compile(r"NOT\s+IN\s*\((?:[^()]|\([^()]*\))*NOT\s+IN", re.IGNORECASE)


def _norm(sql: str) -> str:
    return " ".join((sql or "").upper().split())


def scan_split(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    c = {"total": len(data), "not_exists_any": 0, "double_negation": 0,
         "not_exists_plus_not_in": 0, "nested_not_in": 0, "having_count_eq_subq": 0}
    examples = {"double_negation": [], "having_count_eq_subq": []}
    for ex in data:
        q = _norm(ex.get("query", ""))
        ne = q.count("NOT EXISTS")
        if ne >= 1:
            c["not_exists_any"] += 1
        if ne >= 2:
            c["double_negation"] += 1
            if len(examples["double_negation"]) < 3:
                examples["double_negation"].append(q[:160])
        if ne >= 1 and "NOT IN" in q:
            c["not_exists_plus_not_in"] += 1
        if _NESTED_NOT_IN_RE.search(q):
            c["nested_not_in"] += 1
        if _HAVING_CNT_RE.search(q):
            c["having_count_eq_subq"] += 1
            if len(examples["having_count_eq_subq"]) < 3:
                examples["having_count_eq_subq"].append(q[:160])
    c["_examples"] = examples
    return c


def main(root=None) -> dict:
    root = resolve_root(root)
    out = {}
    print("=" * 68)
    print(" Division-shape scan over Spider splits")
    print("=" * 68)
    for fname in SPLIT_FILES:
        path = os.path.join(root, fname)
        if not os.path.exists(path):
            print(f"{fname:<20} [absent - skipped]")
            continue
        c = scan_split(path)
        out[fname] = c
        print(f"\n{fname}  (n={c['total']})")
        for k in ("not_exists_any", "double_negation", "not_exists_plus_not_in",
                  "nested_not_in", "having_count_eq_subq"):
            pct = c[k] / c["total"] * 100 if c["total"] else 0.0
            print(f"  {k:<24} {c[k]:>5}  ({pct:.2f}%)")
        for shape, exs in c["_examples"].items():
            for e in exs:
                print(f"    e.g. [{shape}] {e}")
    return out


if __name__ == "__main__":
    main()

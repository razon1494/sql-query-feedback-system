"""
Phase 4 — Unified baseline comparison on the external (Spider) corpora.

Compares three systems on the SAME corpora, producing the paper's baseline
table:

  OUTPUT-ONLY  : the classic autograder. Flags a submission iff its output
                 differs from the reference on the test instance(s). Emits no
                 misconception diagnosis (diagnosis rate is 0 by definition).
  SHAPE-ONLY   : the strict structural classifier alone — the system's
                 pre-filter `raw_misconceptions`. This is what an AST-only
                 feedback tool reports.
  FULL SYSTEM  : the two-tier architecture (shape classifier + result-aware
                 semantic filter) — the user-facing `misconceptions`.

Corpora (produced by Phase 3, all execution-validated on real Spider DBs):
  * alternates      : spider_alternates_dev.json      (357 correct rewrites)
  * wrong           : spider_wrong_dev.json           (748 corruptions)
  * division        : division_problems_all.json      (53 problems x 3 wrong
                      variants + 1 alternate; re-analyzed live because the
                      per-variant records are not persisted)

Metrics
-------
  FPR            : % of alternate-correct queries flagged with a misconception
                   (output-only cannot flag an output-equivalent query: its
                   FPR on this corpus is 0 by construction — reported as such)
  wrong flagged  : % of diverging wrong queries flagged at all
  wrong diagnosed: % of diverging wrong queries with the CORRECT misconception
  latent flagged : % of output-equivalent (latent-bug) corruptions flagged —
                   shape-only flags them (no filter), the full system
                   deliberately suppresses them, output-only cannot see them.
                   This row quantifies the two-tier trade-off.

Usage:
    python external/eval/run_baselines.py
"""
import os
import sys
import json
from typing import Dict, List, Optional

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.spider.ingest import resolve_root                    # noqa: E402
from external.harness.generic_problem import GenericProblem, analyze  # noqa: E402
from external.spider.gen_division import make_null_edge, MUTANT_KEYS  # noqa: E402

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")

SYSTEMS = ["output_only", "shape_only", "full_system"]


def _load(name: str) -> Optional[List[Dict]]:
    path = os.path.join(_DERIVED_DIR, name)
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _pct(num: int, den: int) -> Optional[float]:
    return round(num / den * 100, 1) if den else None


# ── corpus metric extractors ─────────────────────────────────────────────────

def metrics_alternates(entries: List[Dict], raw_field: str = "raw",
                       det_field: str = "detected") -> Dict:
    """FPR of each system on execution-equivalent correct rewrites."""
    n = len(entries)
    shape_fp = sum(1 for e in entries if e.get(raw_field))
    full_fp = sum(1 for e in entries if e.get(det_field))
    return {
        "n": n,
        "fpr": {
            "output_only": 0.0,        # equivalent output -> never flagged
            "shape_only": _pct(shape_fp, n),
            "full_system": _pct(full_fp, n),
        },
        "output_only_note": "0 by construction (corpus is output-equivalent)",
    }


def metrics_wrong(entries: List[Dict]) -> Dict:
    """Flagging + diagnosis rates on the corrupted-query corpus."""
    applied = [e for e in entries if e.get("outcome") in ("HIT", "MISS")]
    latent = [e for e in entries if e.get("outcome") == "LATENT_SKIP"]
    na, nl = len(applied), len(latent)

    shape_flag = sum(1 for e in applied if e.get("raw_keys"))
    full_flag = sum(1 for e in applied if e.get("detected_keys"))
    shape_diag = sum(1 for e in applied if e["expected"] in (e.get("raw_keys") or []))
    full_diag = sum(1 for e in applied if e["expected"] in (e.get("detected_keys") or []))
    latent_shape = sum(1 for e in latent if e.get("raw_keys"))

    return {
        "n_applied": na, "n_latent": nl,
        "wrong_flagged": {
            "output_only": _pct(na, na),      # diverging output -> always flagged
            "shape_only": _pct(shape_flag, na),
            "full_system": _pct(full_flag, na),
        },
        "wrong_diagnosed": {
            "output_only": 0.0,               # no diagnosis capability
            "shape_only": _pct(shape_diag, na),
            "full_system": _pct(full_diag, na),
        },
        "latent_flagged": {
            "output_only": 0.0,               # invisible to output comparison
            "shape_only": _pct(latent_shape, nl),
            "full_system": 0.0,               # filter suppresses by design
        },
    }


def metrics_division(root: Optional[str] = None) -> Optional[Dict]:
    """Re-analyze the division corpus (per-variant records are not persisted)."""
    problems = _load("division_problems_all.json")
    if not problems:
        return None
    root = resolve_root(root)

    wrong_records: List[Dict] = []
    alt_records: List[Dict] = []
    for i, p in enumerate(problems):
        db_path = os.path.join(root, "database", p["db_id"], f"{p['db_id']}.sqlite")
        if not os.path.exists(db_path):
            continue
        edge = make_null_edge(db_path, p["triple"], f"bl_{p['db_id']}_{i}")
        prob = GenericProblem(
            problem_id=f"bl::{p['db_id']}::{i}", db_path=db_path,
            gold_sql=p["queries"]["gold"], problem_type="DIVISION",
            db_id=p["db_id"], edge_db_paths=[edge] if edge else [],
        )
        for key in MUTANT_KEYS:
            r = analyze(prob, p["queries"][key])
            wrong_records.append({
                "expected": key,
                "outcome": ("LATENT_SKIP" if (r.are_equivalent and r.edges_ok)
                            else ("HIT" if key in r.detected else "MISS")),
                "raw_keys": r.raw, "detected_keys": r.detected,
            })
        r = analyze(prob, p["queries"]["alt"])
        if r.are_equivalent:
            alt_records.append({"raw": r.raw, "detected": r.detected})

    return {
        "wrong": metrics_wrong(wrong_records),
        "alternates": metrics_alternates(alt_records),
    }


# ── report ───────────────────────────────────────────────────────────────────

_SYS_LABEL = {"output_only": "Output-only autograder",
              "shape_only": "Shape-only classifier",
              "full_system": "Full two-tier system"}


def _fmt(v) -> str:
    return "   n/a" if v is None else f"{v:>5.1f}%"


def print_table(res: Dict) -> None:
    alt, wrong, div = res.get("alternates"), res.get("wrong"), res.get("division")
    print("=" * 78)
    print(" PHASE 4 - Baseline comparison on external (Spider) corpora")
    print("=" * 78)
    header = (f"{'System':<26}{'AltFPR':>8}{'WrFlag':>8}{'WrDiag':>8}"
              f"{'Latent':>8}{'DivFPR':>8}{'DivDiag':>9}")
    print(header)
    print("-" * 78)
    for s in SYSTEMS:
        cells = [
            _fmt(alt["fpr"][s]) if alt else "   n/a",
            _fmt(wrong["wrong_flagged"][s]) if wrong else "   n/a",
            _fmt(wrong["wrong_diagnosed"][s]) if wrong else "   n/a",
            _fmt(wrong["latent_flagged"][s]) if wrong else "   n/a",
            _fmt(div["alternates"]["fpr"][s]) if div else "   n/a",
            _fmt(div["wrong"]["wrong_diagnosed"][s]) if div else "   n/a",
        ]
        print(f"{_SYS_LABEL[s]:<26}{cells[0]:>8}{cells[1]:>8}{cells[2]:>8}"
              f"{cells[3]:>8}{cells[4]:>8}{cells[5]:>9}")
    print("-" * 78)
    if alt:
        print(f"AltFPR : false positives on {alt['n']} alternate-correct rewrites "
              f"(output-only: {alt['output_only_note']})")
    if wrong:
        print(f"WrFlag/WrDiag : flagged / correctly-diagnosed rate on "
              f"{wrong['n_applied']} diverging wrong queries")
        print(f"Latent : flagged rate on {wrong['n_latent']} output-equivalent "
              f"corruptions (two-tier suppresses by design)")
    if div:
        print(f"DivFPR/DivDiag : same metrics on the division schema-transfer corpus "
              f"({div['alternates']['n']} alternates / "
              f"{div['wrong']['n_applied']} applied wrong variants)")


def main(save: bool = True) -> Dict:
    res: Dict = {}
    alts = _load("spider_alternates_dev.json")
    if alts:
        res["alternates"] = metrics_alternates(alts)
    wrong = _load("spider_wrong_dev.json")
    if wrong:
        res["wrong"] = metrics_wrong(wrong)
    div = metrics_division()
    if div:
        res["division"] = div

    print_table(res)
    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        out = os.path.join(_DERIVED_DIR, "baseline_comparison.json")
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(res, fh, indent=1)
        print(f"\nWrote:\n  {out}")
    return res


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Baseline comparison on Spider corpora")
    ap.add_argument("--no-save", action="store_true")
    args = ap.parse_args()
    main(save=not args.no_save)

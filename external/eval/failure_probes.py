"""
Phase 5 — Systematic failure-analysis probe suite.

Deliberately probes the analyzer's known-or-suspected weak spots and RECORDS
what breaks. Perfect scores here are neither expected nor desirable: the
output is the paper's failure taxonomy, and every failure below is a
documented, reproducible limitation rather than a latent surprise for a
reviewer to find.

Probe categories
----------------
  F1 SYNTAX-COVERAGE : SQL constructs outside the clause-level analyzer's
                       grammar (CTEs, window functions, JOIN..USING, derived
                       tables). Expected: misparse or degraded analysis.
  F2 PARSER-PRECISION: constructs the regex clause-splitter can misread
                       (commas in function args, quoted identifiers,
                       keyword-like aliases).
  F3 DETECTOR-GAPS   : documented novice errors the rule set cannot diagnose
                       (uncorrelated EXISTS vs IN reference; weakened - not
                       dropped - WHERE predicate; wrong literal value).
  F4 FILTER-BLINDNESS: wrong-shape queries that are output-equivalent on the
                       test instance (latent bugs) - quantified at corpus
                       scale by Phases 3.1/3.2; probed here as single cases.
  F5 SEMANTIC-EDGE   : SQL three-valued-logic and duplicate-semantics corners
                       (NOT IN with NULL, UNION vs UNION ALL).

Each probe declares its EXPECTED outcome class so the suite doubles as a
regression harness: a probe whose behavior silently changes fails the suite
even if the new behavior is "better", forcing the documentation to be
updated deliberately.

Outcome classes
---------------
  OK        : pipeline behaves correctly and diagnosis is right
  MISPARSE  : parser error or structurally wrong parse on valid SQL
  MISSED    : genuinely wrong query, no user-facing misconception AND no
              output divergence signal reaches the student
  SPURIOUS  : correct query receives a user-facing misconception flag
  DEGRADED  : analysis completes with correct verdict but the diagnosis is
              incomplete/imprecise (e.g. execution catches it, shape does not)

Usage:
    python external/eval/failure_probes.py
"""
import os
import sys
import json
from typing import Dict, List, Optional

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture  # noqa: E402
from external.harness.generic_problem import GenericProblem, analyze     # noqa: E402
from backend.sql_parser import parse_sql                                  # noqa: E402

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")


# ── probe table ──────────────────────────────────────────────────────────────
# Each probe: id, category, description, gold, candidate, problem_type,
#             candidate_is_correct, expected_class, expected_note

PROBES: List[Dict] = [

    # ── F1 SYNTAX-COVERAGE ──────────────────────────────────────────────────
    {
        "id": "F1-cte",
        "category": "F1_SYNTAX_COVERAGE",
        "desc": "Correct answer written as a CTE (WITH clause)",
        "gold": "SELECT a.name, al.title FROM artist a JOIN album al ON a.artist_id = al.artist_id",
        "candidate": ("WITH pairs AS (SELECT a.name AS name, al.title AS title "
                      "FROM artist a JOIN album al ON a.artist_id = al.artist_id) "
                      "SELECT name, title FROM pairs"),
        "ptype": "JOIN", "candidate_is_correct": True,
        "expected_class": "MISPARSE",
        "note": "Clause-level analyzer has no WITH grammar; parser rejects or misreads.",
    },
    {
        "id": "F1-window",
        "category": "F1_SYNTAX_COVERAGE",
        "desc": "Correct answer using a window function instead of GROUP BY",
        "gold": ("SELECT a.name, COUNT(al.album_id) AS n FROM artist a "
                 "JOIN album al ON a.artist_id = al.artist_id "
                 "GROUP BY a.artist_id, a.name HAVING COUNT(al.album_id) > 1"),
        "candidate": ("SELECT DISTINCT name, n FROM (SELECT a.name AS name, "
                      "COUNT(*) OVER (PARTITION BY a.artist_id) AS n "
                      "FROM artist a JOIN album al ON a.artist_id = al.artist_id) "
                      "WHERE n > 1"),
        "ptype": "AGGREGATION", "candidate_is_correct": True,
        "expected_class": "SPURIOUS_OR_OK",
        "note": "Window functions are outside the shape grammar; outcome depends on filter.",
    },
    {
        "id": "F1-using",
        "category": "F1_SYNTAX_COVERAGE",
        "desc": "JOIN ... USING instead of ON",
        "gold": "SELECT al.title, t.title FROM album al JOIN track t ON al.album_id = t.album_id",
        "candidate": "SELECT al.title, t.title FROM album al JOIN track t USING (album_id)",
        "ptype": "JOIN", "candidate_is_correct": True,
        "expected_class": "OK",
        "note": "USING is executable; join extraction regex expects ON - may degrade shape info.",
    },
    {
        "id": "F1-derived",
        "category": "F1_SYNTAX_COVERAGE",
        "desc": "Correct answer via derived table in FROM",
        "gold": "SELECT title FROM album WHERE genre = 'grunge'",
        "candidate": "SELECT title FROM (SELECT * FROM album WHERE genre = 'grunge')",
        "ptype": "JOIN", "candidate_is_correct": True,
        "expected_class": "OK",
        "note": "Derived tables parse loosely; filter should accept on equivalence.",
    },

    # ── F2 PARSER-PRECISION ─────────────────────────────────────────────────
    {
        "id": "F2-func-comma",
        "category": "F2_PARSER_PRECISION",
        "desc": "Function call renames output column (comma inside args)",
        "gold": "SELECT name, country FROM artist",
        "candidate": "SELECT name, COALESCE(country, 'unknown') FROM artist",
        "ptype": "JOIN", "candidate_is_correct": False,   # header differs
        "expected_class": "DEGRADED",
        "note": "Result comparison keys on column NAMES: same data under a renamed "
                "header is judged non-equivalent, with no misconception label - "
                "column-name sensitivity of the output comparator (probe-discovered).",
    },
    {
        "id": "F2-quoted-ident",
        "category": "F2_PARSER_PRECISION",
        "desc": "Double-quoted identifier in WHERE",
        "gold": "SELECT title FROM album WHERE genre = 'rock'",
        "candidate": "SELECT title FROM album WHERE \"genre\" = 'rock'",
        "ptype": "JOIN", "candidate_is_correct": True,
        "expected_class": "OK",
        "note": "Quoted identifiers must not confuse the clause splitter.",
    },

    # ── F3 DETECTOR-GAPS ────────────────────────────────────────────────────
    {
        "id": "F3-uncorr-exists",
        "category": "F3_DETECTOR_GAPS",
        "desc": "Uncorrelated EXISTS where reference uses IN (documented gap)",
        "gold": ("SELECT name FROM artist WHERE artist_id IN "
                 "(SELECT artist_id FROM album WHERE genre = 'grunge')"),
        "candidate": ("SELECT name FROM artist WHERE EXISTS "
                      "(SELECT artist_id FROM album WHERE genre = 'grunge')"),
        "ptype": "SUBQUERY", "candidate_is_correct": False,
        "expected_class": "DEGRADED",
        "note": "M9 rule anchors on reference shape (base must be EXISTS/NOT EXISTS); "
                "wrongness is caught by execution only - no misconception label. "
                "Confirmed at corpus scale in Phase 3.2 (0/2).",
    },
    {
        "id": "F3-weakened-where",
        "category": "F3_DETECTOR_GAPS",
        "desc": "WHERE predicate weakened (wrong literal), not dropped",
        "gold": "SELECT title FROM album WHERE genre = 'grunge'",
        "candidate": "SELECT title FROM album WHERE genre = 'rock'",
        "ptype": "JOIN", "candidate_is_correct": False,
        "expected_class": "DEGRADED",
        "note": "M1 fires only on MISSING where-clause; wrong-literal errors have no "
                "misconception category - execution divergence is the only signal.",
    },
    {
        "id": "F3-extra-join",
        "category": "F3_DETECTOR_GAPS",
        "desc": "Extraneous join multiplying duplicate rows (student adds a table)",
        "gold": "SELECT a.name FROM artist a JOIN album al ON a.artist_id = al.artist_id",
        "candidate": ("SELECT a.name FROM artist a JOIN album al ON a.artist_id = al.artist_id "
                      "JOIN track t ON al.album_id = t.album_id"),
        "ptype": "JOIN", "candidate_is_correct": False,
        "expected_class": "MISSED",
        "note": "Double blindness (probe-discovered): MISSING_JOIN covers too-few "
                "tables only (too-many has no category), AND the set-semantics "
                "result comparison is duplicate-insensitive, so row multiplication "
                "is judged output-equivalent - the query passes as alternate-correct.",
    },

    # ── F4 FILTER-BLINDNESS ─────────────────────────────────────────────────
    {
        "id": "F4-left-latent",
        "category": "F4_FILTER_BLINDNESS",
        "desc": "LEFT JOIN wrong shape, output-equivalent under full FK coverage",
        "gold": "SELECT a.name, al.title FROM artist a JOIN album al ON a.artist_id = al.artist_id",
        "candidate": ("SELECT a.name, al.title FROM artist a "
                      "LEFT JOIN album al ON a.artist_id = al.artist_id"),
        "ptype": "JOIN", "candidate_is_correct": False,
        "expected_class": "MISSED",
        "note": "Every artist has an album in the fixture: outputs match, filter "
                "suppresses WRONG_JOIN_TYPE. Quantified at scale in Phase 3.2 "
                "(153/191 LEFT-join corruptions latent). Raw flag retained for research.",
    },

    # ── F5 SEMANTIC-EDGE ────────────────────────────────────────────────────
    {
        "id": "F5-notin-null",
        "category": "F5_SEMANTIC_EDGE",
        "desc": "NOT IN over subquery that can yield NULL (three-valued logic)",
        "gold": ("SELECT name FROM artist WHERE artist_id NOT IN "
                 "(SELECT artist_id FROM album WHERE artist_id IS NOT NULL)"),
        "candidate": ("SELECT name FROM artist WHERE artist_id NOT IN "
                      "(SELECT artist_id FROM album)"),
        "ptype": "SUBQUERY", "candidate_is_correct": False,
        "expected_class": "MISSED_OR_OK",
        "note": "Fixture album.artist_id has no NULLs: outputs match (latent). "
                "The division corpus catches this class via NULL-injected edges.",
    },
    {
        "id": "F5-union-all",
        "category": "F5_SEMANTIC_EDGE",
        "desc": "UNION ALL instead of UNION (duplicate semantics)",
        "gold": ("SELECT name FROM artist WHERE country = 'UK' "
                 "UNION SELECT name FROM artist WHERE country = 'US'"),
        "candidate": ("SELECT name FROM artist WHERE country = 'UK' "
                      "UNION ALL SELECT name FROM artist WHERE country = 'US'"),
        "ptype": "SET_OP", "candidate_is_correct": False,
        "expected_class": "MISSED_OR_OK",
        "note": "Set-comparison of results is duplicate-insensitive; UNION-vs-UNION ALL "
                "divergence is invisible unless the branches overlap. Detector "
                "normalizes ' ALL' away by design.",
    },
]


def _classify(probe: Dict, res) -> str:
    """Map an AnalysisResult onto an outcome class for this probe."""
    correct = probe["candidate_is_correct"]
    parse = parse_sql(probe["candidate"])
    if parse.error:
        return "MISPARSE"
    if correct:
        if res.detected:
            return "SPURIOUS"
        if not res.are_equivalent and not res.student_exec_error:
            return "MISPARSE"    # equivalent query judged non-equivalent
        return "OK"
    # candidate is wrong:
    if res.are_equivalent:
        return "MISSED"          # latent: no signal reaches the student
    if res.detected:
        return "OK"              # flagged with a misconception
    return "DEGRADED"            # divergence signal only, no diagnosis


_ACCEPT = {
    "OK": {"OK"},
    "MISPARSE": {"MISPARSE"},
    "MISSED": {"MISSED"},
    "SPURIOUS": {"SPURIOUS"},
    "DEGRADED": {"DEGRADED"},
    "SPURIOUS_OR_OK": {"SPURIOUS", "OK", "MISPARSE"},
    "MISSED_OR_OK": {"MISSED", "OK"},
}


def run(save: bool = True) -> Dict:
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    db_path = os.path.join(root, "database", "music_mini", "music_mini.sqlite")

    rows: List[Dict] = []
    unexpected = 0
    for p in PROBES:
        prob = GenericProblem(f"probe::{p['id']}", db_path, p["gold"],
                              p["ptype"], "music_mini")
        res = analyze(prob, p["candidate"])
        cls = _classify(p, res)
        as_expected = cls in _ACCEPT[p["expected_class"]]
        if not as_expected:
            unexpected += 1
        rows.append({
            "id": p["id"], "category": p["category"], "desc": p["desc"],
            "outcome": cls, "expected": p["expected_class"],
            "as_expected": as_expected,
            "detected": res.detected, "raw": res.raw,
            "equivalent": res.are_equivalent,
            "exec_error": res.student_exec_error,
            "note": p["note"],
        })

    print("=" * 78)
    print(" PHASE 5 - Failure-analysis probe suite")
    print("=" * 78)
    cur_cat = None
    for r in rows:
        if r["category"] != cur_cat:
            cur_cat = r["category"]
            print(f"\n[{cur_cat}]")
        mark = "=" if r["as_expected"] else "!"
        print(f"  {mark} {r['id']:<18} {r['outcome']:<10} (expected {r['expected']})")
        print(f"      {r['desc']}")
        if not r["as_expected"]:
            print(f"      UNEXPECTED: detected={r['detected']} raw={r['raw']} "
                  f"equiv={r['equivalent']} err={r['exec_error']}")
    n = len(rows)
    print("\n" + "-" * 78)
    print(f"probes: {n}   behaving as documented: {n - unexpected}   "
          f"unexpected: {unexpected}")
    print("('=' = documented behavior confirmed; failures here mean the taxonomy")
    print(" in the paper no longer matches the implementation)")

    out = {"probes": rows, "n": n, "unexpected": unexpected}
    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        path = os.path.join(_DERIVED_DIR, "failure_probes.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(out, fh, indent=1)
        print(f"\nWrote:\n  {path}")
    return out


if __name__ == "__main__":
    result = run()
    sys.exit(0 if result["unexpected"] == 0 else 1)

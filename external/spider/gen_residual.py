"""
Phase 2 (IJAIED journal) — residual probe corpus: families F3a, F3b, F3c.

A separate module from `gen_wrong.py`, deliberately.

`gen_wrong.py` asks *"the detector should emit label L for this corruption —
did it?"* and reports HIT / MISS / LATENT_SKIP. Its operator list and the
748 generated / 547 applied / 545 (99.6%) counts it produces are the numbers in
the submitted ICWL paper, so that list is **frozen**: adding an operator there
would silently move a figure in a paper under review.

This module asks the opposite question. The three F3 families have no element
in M1–M10 at all — the deterministic core can establish *that* the query is
wrong and has nothing to say about *why*. A case earns a place in this corpus
only when the core is confirmed to return **wrong-undiagnosed** on it:

  1. the candidate parses;
  2. it diverges from the reference on at least one instance — the main
     database or a constructed edge case — so the wrongness is *observed*
     rather than assumed; and
  3. the core emits zero misconceptions.

That conjunction is what makes a case a fair test of whether a constrained LLM
recovers a diagnosis the rules cannot (PIPELINE Phase 2). Cases where the core
*does* emit a label are not generator bugs — they are simply not residual, and
they are retained under outcome `NON_RESIDUAL` so every exclusion stays visible
instead of being dropped on the floor.

Families
  F3a  uncorrelated EXISTS where the reference correlates      (M9 residual)
  F3b  a WHERE boundary comparison shifted one position        (M1 residual)
  F3c  one more table joined than the task needs               (M5 residual)

Usage:
    python external/spider/gen_residual.py --split dev
    python external/spider/gen_residual.py --split dev --limit 50
"""
import os
import re
import sys
import json
import random
import sqlite3
import argparse
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from backend.sql_parser import (                                  # noqa: E402
    parse_sql, _extract_top_level_clauses, _parse_joins, _mask_nested_parens,
)
from external.spider.classify import classify_examples            # noqa: E402
from external.spider.gen_alternates import (                      # noqa: E402
    _rebuild, _lead_table_text,
)
from external.spider.gen_wrong import (                           # noqa: E402
    op_in_to_uncorrelated_exists, _norm,
)
from external.harness.generic_problem import (                    # noqa: E402
    GenericProblem, analyze,
)

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")
_OUTER_JOIN_RE = re.compile(r"\b(LEFT|RIGHT|FULL|CROSS|NATURAL|OUTER)\b",
                            re.IGNORECASE)


# ── F3b: boundary shift ──────────────────────────────────────────────────────

_BOUNDARY_FLIP = {">=": ">", "<=": "<", ">": ">=", "<": "<="}
# A depth-0 comparison operator. The lookbehind and lookahead keep `<>`, `!=`,
# `>=` and `<=` from being matched as their own single-character prefixes.
# `>` in the lookahead matters as much as `=`: without it the `<` of `<>` is
# taken for a boundary and shifted to `<=`, emitting `<=>`, which no engine
# accepts. `<>` is an inequality, not a boundary — there is nothing next to it
# to get wrong — so it belongs outside this family entirely.
_CMP_RE = re.compile(r"(?<![<>!=])(>=|<=|>|<)(?![=>])")


# ── F3a: decorrelated / uncorrelated EXISTS ──────────────────────────────────

# A depth-0 `<col> IN (` — located in the masked text, so an `IN` inside a
# subquery is never the one rewritten.
_IN_SUB_RE = re.compile(r"([\w.]+)\s+(NOT\s+)?IN\s*$", re.IGNORECASE)


def _find_in_subquery(where: str) -> Optional[Tuple[int, int, str, bool]]:
    """Locate the first depth-0 `<col> [NOT] IN (<subquery>)`.

    Returns (start, end, subquery_text, negated) spanning the whole
    `<col> [NOT] IN (...)`, or None. Parenthesis matching is done by counting
    rather than by regex, because the subquery may itself contain parentheses.

    `NOT IN` is captured rather than skipped. Dropping it would cost most of
    the family: across Spider dev and train the negated form outnumbers the
    plain one roughly six to one, so a corpus without it is not so much
    conservative as empty. The negation must be carried through, though —
    matching `NOT` as if it were the column name produces `<col> EXISTS (...)`,
    which is not valid SQL, and a submission the engine rejects is a syntax
    error rather than a misconception.
    """
    masked = _mask_nested_parens(where)   # length-preserving; indices align
    depth = 0
    for i, ch in enumerate(where):
        if ch == ")":
            depth -= 1
            continue
        if ch != "(":
            continue
        opens_at_top = depth == 0
        depth += 1
        if not opens_at_top:
            continue
        # Everything before a depth-0 `(` survives the mask unchanged, so a
        # match here is a genuine top-level `<col> IN`, never one nested.
        head = _IN_SUB_RE.search(masked[:i])
        if not head:
            continue
        inner_depth = 0
        for j in range(i, len(where)):
            if where[j] == "(":
                inner_depth += 1
            elif where[j] == ")":
                inner_depth -= 1
                if inner_depth == 0:
                    inner = where[i + 1:j].strip()
                    if not inner.upper().startswith("SELECT"):
                        return None
                    return (head.start(1), j + 1, inner,
                            head.group(2) is not None)
        return None                              # unbalanced parentheses
    return None


def op_uncorrelated_exists(sql: str, parse, clauses) -> Optional[str]:
    """F3a: an `IN`-subquery is rewritten as an `EXISTS` that ignores the outer
    row — the documented belief that `EXISTS` tests membership by itself.

    Deliberately **broader** than `gen_wrong.op_in_to_uncorrelated_exists`,
    which this module does not use for generation. That operator requires the
    WHERE clause to be *exactly* `<col> IN (<subquery>)` with no other
    conjunct, which is right for the ICWL probe — it isolates the corruption —
    but leaves only two cases in the whole Spider dev split, far too few for a
    per-family result. Here the `IN` term may sit among other conjuncts, and
    only that term is rewritten, so `a = 1 AND x IN (SELECT ...)` becomes
    `a = 1 AND EXISTS (SELECT ...)`. The misconception is identical; only the
    surrounding context is allowed to be richer.

    `NOT IN` is handled too, becoming an uncorrelated `NOT EXISTS`. That is the
    same belief negated — the membership test is dropped and what remains asks
    only whether the subquery returns anything at all. Whether the deterministic
    core happens to have a label for it is not decided here: if it emits one,
    `validate` records the case as NON_RESIDUAL and drops it, which is the
    measurement doing the judging rather than the generator.

    The frozen operator stays imported and untouched, so the ICWL corpus it
    feeds keeps reproducing.
    """
    if parse.set_operation:
        return None
    where = clauses.get("WHERE")
    if not where:
        return None
    found = _find_in_subquery(where)
    if not found:
        return None
    start, end, inner, negated = found
    keyword = "NOT EXISTS" if negated else "EXISTS"
    new_where = "%s%s (%s)%s" % (where[:start], keyword, inner, where[end:])
    return _rebuild(clauses, clauses["FROM"], new_where)


def _shift_boundary(text: str) -> Optional[str]:
    """Shift the first depth-0 boundary comparison by one position, or None.

    Only depth-0 operators are rewritten: a comparison inside a subquery is
    masked out, so the corruption is always the one the task's own filter
    makes, never one buried in a nested SELECT. The mask is length-preserving,
    so an index found in it addresses the same character in the original.
    """
    masked = _mask_nested_parens(text)
    m = _CMP_RE.search(masked)
    if not m:
        return None
    start, end = m.span(1)
    op = text[start:end]
    if op not in _BOUNDARY_FLIP:
        return None
    return text[:start] + _BOUNDARY_FLIP[op] + text[end:]


def op_weaken_predicate(sql: str, parse, clauses) -> Optional[str]:
    """F3b: one boundary comparison is shifted by a single position —
    `credits >= 3` becomes `credits > 3`.

    The filter is still there and the query shape is untouched, so no
    structural comparison against the reference distinguishes the two; only
    rows sitting exactly on the boundary differ. M1 fires on a *missing*
    WHERE clause, so a mis-set boundary has no category to fall into.

    WHERE is tried first and HAVING only when WHERE carries no boundary, so
    every case this operator produced before HAVING was considered is produced
    unchanged. A boundary in HAVING is the same misconception one level up —
    the filter is over groups rather than rows, and `COUNT(*) > 1` where the
    task says "at least one" is an error students actually make. M6 and M7
    cover a *misplaced* group filter, not a mis-set one, so a HAVING boundary
    is as uncategorised as a WHERE boundary.
    """
    if parse.set_operation:
        return None
    where = clauses.get("WHERE")
    if where:
        shifted = _shift_boundary(where)
        if shifted is not None:
            return _rebuild(clauses, clauses["FROM"], shifted)
    having = clauses.get("HAVING")
    if having:
        shifted = _shift_boundary(having)
        if shifted is not None:
            clauses2 = dict(clauses)
            clauses2["HAVING"] = shifted
            return _rebuild(clauses2, clauses2["FROM"], clauses2.get("WHERE"))
    return None


# ── F3c: extraneous join ─────────────────────────────────────────────────────

def _fk_edges(sqlite_path: str) -> List[Tuple[str, str, str, str]]:
    """Declared foreign keys as (child_table, child_col, parent_table, parent_col).

    Sorted, so operator output is a function of the database alone and a
    regenerated corpus is byte-identical.
    """
    edges: List[Tuple[str, str, str, str]] = []
    try:
        con = sqlite3.connect(sqlite_path)
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        for t in tables:
            for row in con.execute('PRAGMA foreign_key_list("%s")' % t).fetchall():
                parent, child_col, parent_col = row[2], row[3], row[4]
                if not parent or not child_col or not parent_col:
                    continue         # implicit PK reference; not resolvable here
                edges.append((t, child_col, parent, parent_col))
        con.close()
    except sqlite3.Error:
        return []
    return sorted(set(edges))


def _from_tables(from_text: str) -> Dict[str, str]:
    """table name (lowercased) -> the name a column must be qualified with.

    That is the alias where the FROM entry declares one, and the table name
    otherwise. Returns {} for anything this simple reader cannot account for.
    """
    present: Dict[str, str] = {}
    lead = _lead_table_text(from_text)
    parts = re.sub(r"\bAS\b", " ", lead, flags=re.IGNORECASE).split()
    if not parts:
        return {}
    present[parts[0].lower()] = parts[1] if len(parts) > 1 else parts[0]
    for j in _parse_joins(from_text):
        table = j["table"]
        present[table.lower()] = j.get("alias") or table
    return present


def op_extraneous_join(sql: str, parse, clauses,
                       fk_edges: List[Tuple[str, str, str, str]]) -> Optional[str]:
    """F3c: the student joins in one table more than the task needs.

    The extra table is chosen along a declared foreign key from a table already
    in FROM, so the join is *well formed* — it reads like a plausible answer and
    the ON condition is the natural one. What it changes is cardinality: a
    parent with several children multiplies its rows, a parent with none loses
    them. Both diverge, and neither is a misconception the rules name —
    `MISSING_JOIN` covers too-few tables only, so too-many has no category.
    """
    if parse.set_operation or not clauses.get("FROM"):
        return None
    from_text = clauses["FROM"]
    if _OUTER_JOIN_RE.search(from_text):
        return None                  # leave outer-join shapes alone
    if "," in _mask_nested_parens(from_text):
        return None                  # comma-joins: alias handling is not worth it
    present = _from_tables(from_text)
    if not present:
        return None
    for child, child_col, parent, parent_col in fk_edges:
        if parent.lower() in present and child.lower() not in present:
            ref = present[parent.lower()]
            extra = " JOIN %s ON %s.%s = %s.%s" % (
                child, child, child_col, ref, parent_col)
            return _rebuild(clauses, from_text.strip() + extra,
                            clauses.get("WHERE"))
    return None


# ── family registry ──────────────────────────────────────────────────────────

FAMILIES: List[Tuple[str, str, str, str]] = [
    # (family id, operator name, residual of, literature basis)
    ("F3a", "uncorrelated_exists", "M9",
     "Brass & Goldberg 2006 Error 21; Miedema et al. 2021"),
    ("F3b", "weaken_predicate", "M1",
     "Taipalus 2018 LOG-4; Brass & Goldberg 2006"),
    ("F3c", "extraneous_join", "M5",
     "Taipalus 2018 SEM-3; Brass & Goldberg 2006 Error 20"),
]


def _apply(op_name: str, sql: str, parse, clauses, fk_edges) -> Optional[str]:
    if op_name == "uncorrelated_exists":
        return op_uncorrelated_exists(sql, parse, clauses)
    if op_name == "weaken_predicate":
        return op_weaken_predicate(sql, parse, clauses)
    if op_name == "extraneous_join":
        return op_extraneous_join(sql, parse, clauses, fk_edges)
    raise ValueError("unknown operator %s" % op_name)


# ── driver ───────────────────────────────────────────────────────────────────

_UNIVERSITY_DB = os.path.join(_REPO_ROOT, "database", "main.db")
_EDGE_NAMES = ("empty_courses", "partial_match", "all_enrolled",
               "single_course", "no_students")


def _university_records() -> List[Dict]:
    """The seven-table teaching schema's problem bank, in Spider record shape.

    Two differences from Spider matter. Reference queries here end in a
    semicolon, which `_extract_top_level_clauses` keeps in the final clause and
    which then defeats the operators' anchored patterns — so the terminator is
    stripped. And this schema ships five constructed edge instances, which are
    passed through as `edge_db_paths`: a boundary shift invisible on the main
    database may well diverge on `partial_match` or `single_course`, and
    without them those cases would be discarded as latent when they are in fact
    observable. Spider has no such instances, which is why its F3b latent rate
    is the floor rather than the truth.
    """
    from backend.problems import PROBLEMS                       # noqa: E402
    edges = [os.path.join(_REPO_ROOT, "database", "edge_%s.db" % n)
             for n in _EDGE_NAMES]
    edges = [p for p in edges if os.path.exists(p)]
    return [{
        "db_id": "university",
        "problem_ref": p["id"],
        "question": p["question"],
        "gold_sql": " ".join(p["base_query"].split()).strip().rstrip(";").strip(),
        "sqlite_path": _UNIVERSITY_DB,
        "problem_type": p["type"],
        "edge_db_paths": edges,
    } for p in PROBLEMS]


def generate(root: Optional[str] = None, split: str = "dev",
             limit: Optional[int] = None,
             source: str = "both",
             families: Optional[Tuple[str, ...]] = None) -> List[Dict]:
    """Apply every applicable family to every deduplicated gold query.

    `families` restricts which of F3a/F3b/F3c are applied, which is what makes
    a reserve pass over a second split possible: the probe needs more F3a than
    one split yields, but has a surplus of F3c already.
    """
    wanted = tuple(families) if families else tuple(f for f, _o, _r, _l in FAMILIES)
    records: List[Dict] = []
    if source in ("spider", "both"):
        for r in classify_examples(root, split=split):
            r["split"] = split
            records.append(r)
    if source in ("university", "both"):
        records.extend(_university_records())
    seen = set()
    golds: List[Dict] = []
    for r in records:
        key = (r["db_id"], _norm(r["gold_sql"]))
        if key in seen:
            continue
        seen.add(key)
        golds.append(r)
    if limit:
        golds = golds[:limit]

    fk_cache: Dict[str, List[Tuple[str, str, str, str]]] = {}
    corpus: List[Dict] = []
    for rec in golds:
        parse = parse_sql(rec["gold_sql"])
        if parse.error:
            continue
        clauses = _extract_top_level_clauses(rec["gold_sql"])
        if "SELECT" not in clauses or "FROM" not in clauses:
            continue
        path = rec["sqlite_path"]
        if path not in fk_cache:
            fk_cache[path] = _fk_edges(path)
        for family, op_name, residual_of, lit in FAMILIES:
            if family not in wanted:
                continue
            cand = _apply(op_name, rec["gold_sql"], parse, clauses, fk_cache[path])
            if not cand or _norm(cand) == _norm(rec["gold_sql"]):
                continue
            corpus.append({
                "family": family,
                "operator": op_name,
                "residual_of": residual_of,
                "literature": lit,
                "db_id": rec["db_id"],
                "split": rec.get("split", "university"),
                "problem_ref": rec.get("problem_ref", ""),
                "sqlite_path": path,
                "edge_db_paths": rec.get("edge_db_paths", []),
                "problem_type": rec["problem_type"],
                "question": rec["question"],
                "gold_sql": rec["gold_sql"],
                "wrong_sql": " ".join(cand.split()),
            })
    return corpus


def validate(corpus: List[Dict], verbose: bool = True) -> Dict:
    """Execute every candidate and keep only the confirmed wrong-undiagnosed.

    Outcomes
      RESIDUAL      diverges on some instance, zero misconceptions — kept
      NON_RESIDUAL  diverges, but the core named a misconception — excluded
      LATENT        output-equivalent everywhere — excluded, wrongness unobserved
      EXEC_ERROR    the engine rejected the submission — excluded
      MISPARSE      the candidate does not parse — excluded
    """
    per_family = {f: defaultdict(int) for f, _o, _r, _l in FAMILIES}

    for entry in corpus:
        s = per_family[entry["family"]]
        s["generated"] += 1

        if parse_sql(entry["wrong_sql"]).error:
            entry["outcome"] = "MISPARSE"
            s["misparse"] += 1
            continue

        problem = GenericProblem(
            problem_id="%s::%s" % (entry["db_id"], entry["operator"]),
            db_path=entry["sqlite_path"],
            gold_sql=entry["gold_sql"],
            problem_type=entry["problem_type"],
            db_id=entry["db_id"],
            source="residual_probe",
            edge_db_paths=entry.get("edge_db_paths", []),
        )
        res = analyze(problem, entry["wrong_sql"])
        entry["detected_keys"] = list(res.detected)
        entry["raw_keys"] = list(res.raw)
        entry["are_equivalent"] = res.are_equivalent
        entry["edges_ok"] = res.edges_ok

        if res.student_exec_error:
            # The engine rejected the submission. That is a syntax or binding
            # error, which the deterministic system already reports precisely,
            # and it is not a misconception for an instructor to name. It also
            # diverges trivially, so without this branch a malformed generation
            # would be admitted as residual on the strength of its own failure.
            entry["outcome"] = "EXEC_ERROR"
            entry["exec_error"] = res.student_exec_error
            s["exec_error"] += 1
        elif res.are_equivalent and res.edges_ok:
            entry["outcome"] = "LATENT"
            s["latent"] += 1
        elif res.detected:
            entry["outcome"] = "NON_RESIDUAL"
            s["non_residual"] += 1
        else:
            entry["outcome"] = "RESIDUAL"
            s["residual"] += 1

    summary = {
        "families": {f: dict(per_family[f]) for f, _o, _r, _l in FAMILIES},
        "total_generated": len(corpus),
        "total_residual": sum(per_family[f]["residual"]
                              for f, _o, _r, _l in FAMILIES),
    }
    if verbose:
        _print_summary(summary)
    return summary


def _print_summary(summary: Dict) -> None:
    print("=" * 78)
    print("Residual probe corpus — F3 families (IJAIED Phase 2)")
    print("=" * 78)
    print("%-8s%-22s%6s%7s%9s%8s%8s%10s" % (
        "family", "op", "gen", "resid", "non-res", "latent", "execerr",
        "misparse"))
    for family, op_name, _r, _l in FAMILIES:
        s = summary["families"][family]
        print("%-8s%-22s%6d%7d%9d%8d%8d%10d" % (
            family, op_name, s.get("generated", 0), s.get("residual", 0),
            s.get("non_residual", 0), s.get("latent", 0),
            s.get("exec_error", 0), s.get("misparse", 0)))
    print("-" * 78)
    print("generated %d, confirmed wrong-undiagnosed %d" % (
        summary["total_generated"], summary["total_residual"]))
    print()
    print("RESIDUAL = diverges on at least one instance AND the deterministic")
    print("core emits no misconception. Only these are eligible for instructor")
    print("adjudication; every other outcome is excluded and counted above.")


def _dedup(corpus: List[Dict]) -> List[Dict]:
    """Drop repeats across passes, keyed on what an annotator would see."""
    seen = set()
    out: List[Dict] = []
    for e in corpus:
        key = (e["family"], e["db_id"], _norm(e["gold_sql"]), _norm(e["wrong_sql"]))
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


# ── stratified adjudication sample ───────────────────────────────────────────

SAMPLE_SEED = 20260916          # fixed for reproducibility; report it
_EXTRA_JOIN_RE = re.compile(r"JOIN\s+(\w+)\s+ON", re.IGNORECASE)


def _stratum(entry: Dict) -> Tuple:
    """The axis along which a family's cases repeat themselves.

    Sampling uniformly at random would be wrong here, and not for cosmetic
    reasons. F3c holds 251 cases but only ~41 distinct (database, extra table)
    pairs, so a random draw hands an annotator the same corruption six times
    over. Two items that look identical get the same answer, and an agreement
    statistic reads that as consensus — inflating the very number the
    adjudication exists to produce. Spreading the draw across strata is what
    keeps the agreement figure honest.
    """
    family = entry["family"]
    if family == "F3c":
        gold_tables = set(_EXTRA_JOIN_RE.findall(entry["gold_sql"]))
        added = [t for t in _EXTRA_JOIN_RE.findall(entry["wrong_sql"])
                 if t not in gold_tables]
        return (entry["db_id"], added[0].lower() if added else "?")
    if family == "F3b":
        site = "HAVING" if re.search(r"\bHAVING\b", entry["gold_sql"], re.I) else "WHERE"
        return (site, entry["db_id"])
    # F3a. The database alone is not the axis along which this family repeats.
    # Spider's IN-subqueries are negated about six times in seven, so a key on
    # the database returns almost nothing but `NOT IN -> NOT EXISTS` and a rater
    # reads one transformation thirty times over. Shape leads the key so it
    # becomes the balancing axis.
    shape = ("NOT EXISTS" if re.search(r"\bNOT\s+EXISTS\b", entry["wrong_sql"], re.I)
             else "EXISTS")
    return (shape, entry["db_id"])


def _allocate(total: int, capacity: Dict[Any, int],
              order: List[Any]) -> Dict[Any, int]:
    """Spread `total` as evenly as the capacities allow, one at a time."""
    quota = {k: 0 for k in order}
    remaining = total
    while remaining:
        progressed = False
        for k in order:
            if quota[k] < capacity[k]:
                quota[k] += 1
                remaining -= 1
                progressed = True
                if not remaining:
                    break
        if not progressed:
            break                        # every group is exhausted
    return quota


def _round_robin(buckets: Dict[Tuple, List[Dict]], keys: List[Tuple],
                 quota: int) -> List[Dict]:
    """Take one case from each stratum before any stratum gives a second."""
    picked: List[Dict] = []
    depth = 0
    while len(picked) < quota:
        progressed = False
        for k in keys:
            if depth < len(buckets[k]):
                picked.append(buckets[k][depth])
                progressed = True
                if len(picked) == quota:
                    break
        if not progressed:
            break
        depth += 1
    return picked


def sample(corpus: List[Dict], per_family: int = 30,
           seed: int = SAMPLE_SEED) -> List[Dict]:
    """Draw `per_family` residual cases per family, in two levels.

    The first element of a family's stratum is the **balancing** axis and the
    rest is the **diversity** axis. The quota is split as evenly as the corpus
    allows across the balancing values, and within each, strata are visited
    round-robin so one case comes from each before any gives a second.

    One level is not enough, and F3a is why. Keying on the database alone made
    every stratum distinct — the report said so — while 28 of 30 drawn cases
    were still the same `NOT IN -> NOT EXISTS` rewrite, because Spider has six
    negated subqueries for every plain one and therefore six times the strata.
    Distinctness of stratum is not distinctness of what a rater sees. Balancing
    first fixes that, and it costs the other families nothing: F3b balances
    HAVING against WHERE, F3c across databases.
    """
    rng = random.Random(seed)
    drawn: List[Dict] = []
    for family, _o, _r, _l in FAMILIES:
        pool = [e for e in corpus
                if e["family"] == family and e["outcome"] == "RESIDUAL"]
        buckets: Dict[Tuple, List[Dict]] = defaultdict(list)
        for e in pool:
            buckets[_stratum(e)].append(e)
        for k in buckets:
            rng.shuffle(buckets[k])

        groups: Dict[Any, List[Tuple]] = defaultdict(list)
        for k in sorted(buckets):
            groups[k[0]].append(k)
        order = sorted(groups)
        rng.shuffle(order)
        for g in order:
            rng.shuffle(groups[g])

        capacity = {g: sum(len(buckets[k]) for k in groups[g]) for g in order}
        quota = _allocate(per_family, capacity, order)
        for g in order:
            for e in _round_robin(buckets, groups[g], quota[g]):
                item = dict(e)
                item["stratum"] = list(_stratum(e))
                drawn.append(item)
    return drawn


def _print_sample_report(drawn: List[Dict], per_family: int) -> None:
    print("=" * 78)
    print("Adjudication sample — stratified, seed %d" % SAMPLE_SEED)
    print("=" * 78)
    print("%-8s%8s%9s%18s" % ("family", "drawn", "target", "distinct strata"))
    for family, _o, _r, _l in FAMILIES:
        f = [e for e in drawn if e["family"] == family]
        print("%-8s%8d%9d%18d" % (
            family, len(f), per_family, len({tuple(e["stratum"]) for e in f})))
    print("-" * 78)
    print("total %d items; at two independent raters that is %d judgements"
          % (len(drawn), 2 * len(drawn)))
    short = [f for f, _o, _r, _l in FAMILIES
             if len([e for e in drawn if e["family"] == f]) < per_family]
    if short:
        print("SHORT of target: %s — the corpus cannot supply the quota"
              % ", ".join(short))


def main(root=None, split="dev", limit=None, save=True,
         source="both", reserve_split=None,
         reserve_families=("F3a",), sample_n=None) -> Dict:
    """Primary pass, then an optional family-restricted pass over a second split.

    The reserve exists because adjudication *removes* cases: an instructor who
    marks an item Correct-Alternative or Ambiguous takes it out of the recall
    and false-label denominators. F3a is the family that cannot absorb that —
    one split yields 28, and a quarter lost puts it below the size at which a
    per-family number means anything. F3b and F3c have surplus already, so the
    reserve is restricted rather than run wholesale.

    A second split is an honest source in a way that writing new tasks is not:
    nothing here is trained, the splits are disjoint, and the cases are not
    chosen after seeing which families came out thin.
    """
    corpus = generate(root, split=split, limit=limit, source=source)
    if reserve_split:
        corpus.extend(generate(root, split=reserve_split, limit=limit,
                               source="spider", families=tuple(reserve_families)))
        corpus = _dedup(corpus)
    summary = validate(corpus)
    summary["source"] = source
    kept = [e for e in corpus if e["outcome"] == "RESIDUAL"]
    summary["residual_by_source"] = {
        "university": sum(1 for e in kept if e["db_id"] == "university"),
        "spider": sum(1 for e in kept if e["db_id"] != "university"),
    }
    if sample_n:
        drawn = sample(corpus, per_family=sample_n)
        _print_sample_report(drawn, sample_n)
        summary["sample"] = {
            "per_family_target": sample_n,
            "seed": SAMPLE_SEED,
            "drawn": len(drawn),
        }
        if save:
            os.makedirs(_DERIVED_DIR, exist_ok=True)
            sample_path = os.path.join(_DERIVED_DIR,
                                       "residual_sample_%s.json" % split)
            with open(sample_path, "w", encoding="utf-8") as fh:
                json.dump(drawn, fh, indent=2)
            print("  %s" % sample_path)

    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        corpus_path = os.path.join(_DERIVED_DIR, "residual_probe_%s.json" % split)
        summary_path = os.path.join(_DERIVED_DIR, "residual_summary_%s.json" % split)
        with open(corpus_path, "w", encoding="utf-8") as fh:
            json.dump(corpus, fh, indent=2)
        with open(summary_path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
        print("\nWrote:\n  %s\n  %s" % (corpus_path, summary_path))
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--split", default="dev")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--root", default=None)
    ap.add_argument("--source", default="both",
                    choices=("spider", "university", "both"))
    ap.add_argument("--reserve-split", default=None,
                    help="second split for a family-restricted reserve pass")
    ap.add_argument("--reserve-families", default="F3a",
                    help="comma-separated families for the reserve pass")
    ap.add_argument("--sample", type=int, default=None,
                    help="draw this many cases per family, stratified")
    ap.add_argument("--no-save", action="store_true")
    a = ap.parse_args()
    main(root=a.root, split=a.split, limit=a.limit, save=not a.no_save,
         source=a.source, reserve_split=a.reserve_split, sample_n=a.sample,
         reserve_families=tuple(x.strip() for x in a.reserve_families.split(",") if x.strip()))

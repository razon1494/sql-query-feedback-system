"""
Phase 3-DIV — Schema-transfer division evaluation over Spider databases.

Motivation
----------
Relational division (M8/M3/M9) is the system's flagship misconception family,
but Spider contains ZERO universal-quantification queries — measured across all
9,693 examples by `division_scan.py` (0 occurrences of NOT EXISTS in the whole
benchmark). Division problems therefore cannot be *sampled* from Spider; they
must be *authored over* Spider's schemas. That still delivers the external
validity that matters: the schemas, data distributions, and integrity quirks
are foreign to the detector, so this directly tests whether division detection
generalizes beyond the authors' own university schema (schema transfer).

Construction (fully automatic, detector-blind)
----------------------------------------------
1. Mine each Spider database's foreign keys for division triples
       Outer table O  <-(fk)-  Link table L  -(fk)->  Divisor table D
   (a link table is any table with two single-column FKs to distinct tables).
2. Author the gold query: canonical double-negation NOT EXISTS division,
   optionally with a divisor filter d.col = 'v' chosen from the data (divisor
   subsets make coverage achievable).
3. Validate against the real data — a problem is kept only if:
       * gold executes and returns a non-empty result,
       * the divisor set has >= 2 elements (division with 1 is trivial), and
       * the IN-based wrong query DIVERGES from gold on this data, i.e. the
         database contains a partial-match counterexample. This is the RATest
         discriminating-instance requirement, checked by execution.
4. For each kept problem, generate the three literature-documented wrong
   variants (one misconception each) and one alternate-correct rewrite:
       M8  IN_FOR_DIVISION        : existential IN instead of universal
       M3  NOT_IN_vs_NOT_EXISTS   : NULL-unsafe NOT-IN-in-NOT-IN form
       M9  MISSING_CORRELATED_REF : inner correlation to the outer tuple dropped
       ALT GROUP BY / HAVING COUNT(DISTINCT..) = (SELECT COUNT..)  (equivalent)
5. Build a NULL-injected edge instance per problem (a link row with NULL outer
   key and a non-divisor value) so the M3 latent NULL bug is observable, in the
   spirit of the university edge DBs but generated generically.

Every variant then runs through the UNCHANGED feedback pipeline via the Phase 0
harness. Detection accounting follows the paper's convention: a wrong variant
that is output-equivalent to gold on every instance (main + edge) is a latent
bug and is counted as skipped, not detected.

Usage:
    python external/spider/gen_division.py             # dev-split databases
    python external/spider/gen_division.py --all-dbs   # all 166 Spider DBs
"""
import os
import re
import sys
import json
import shutil
import sqlite3
from itertools import permutations
from typing import Optional, List, Dict, Tuple

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from backend.query_executor import execute_query               # noqa: E402
from external.spider.ingest import resolve_root, load_examples  # noqa: E402
from external.harness.generic_problem import GenericProblem, analyze  # noqa: E402

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")
_EDGE_DIR = os.path.join(_DERIVED_DIR, "division_edges")
_IDENT = re.compile(r"^\w+$")

# Instance-size caps. Triple-nested NOT EXISTS on unindexed Spider tables is
# nested-loop; without caps the mining hangs on the large sports/log databases.
# Skipped-oversized is reported, and the cap is defensible in the paper (the
# system targets interactive-latency educational instances).
_MAX_O_ROWS = 2000     # outer table
_MAX_D_ROWS = 300      # divisor table
_MAX_L_ROWS = 30000    # link table

# Statement guard: abort any validation query after ~500M SQLite VM steps
# (a few seconds) via the progress handler. Guarded-out problems are rejected.
_GUARD_EVERY_STEPS = 200_000
_GUARD_MAX_CALLS = 2500

MUTANT_KEYS = ["IN_FOR_DIVISION", "NOT_IN_vs_NOT_EXISTS", "MISSING_CORRELATED_REF"]


# ── schema mining ────────────────────────────────────────────────────────────

def _tables(conn) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    return [r[0] for r in cur.fetchall()]


def _cols(conn, table: str) -> List[Tuple[str, str, int]]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return [(r[1], (r[2] or ""), r[5]) for r in cur.fetchall()]  # name, type, pk


def _pk_of(conn, table: str) -> Optional[str]:
    for name, _typ, pk in _cols(conn, table):
        if pk:
            return name
    return None


def _single_col_fks(conn, table: str) -> List[Dict]:
    """Non-composite FKs of `table` as {target, frm, to} dicts."""
    cur = conn.execute(f'PRAGMA foreign_key_list("{table}")')
    rows = cur.fetchall()  # (id, seq, table, from, to, ...)
    by_id: Dict[int, List] = {}
    for r in rows:
        by_id.setdefault(r[0], []).append(r)
    out = []
    for _fid, grp in by_id.items():
        if len(grp) != 1:      # composite FK — skip
            continue
        r = grp[0]
        out.append({"target": r[2], "frm": r[3], "to": r[4]})
    return out


def find_triples(db_path: str) -> List[Dict]:
    """Division triples (O <- L -> D) mined from FK structure."""
    conn = sqlite3.connect(db_path)
    triples = []
    try:
        tables = {t.lower(): t for t in _tables(conn)}
        for L in tables.values():
            fks = _single_col_fks(conn, L)
            for fk_o, fk_d in permutations(fks, 2):
                O = tables.get((fk_o["target"] or "").lower())
                D = tables.get((fk_d["target"] or "").lower())
                if not O or not D or O == D or O == L or D == L:
                    continue
                opk = fk_o["to"] or _pk_of(conn, O)
                dpk = fk_d["to"] or _pk_of(conn, D)
                names = [L, O, D, fk_o["frm"], fk_d["frm"], opk, dpk]
                if not all(n and _IDENT.match(n) for n in names):
                    continue
                triples.append({"L": L, "lfo": fk_o["frm"], "O": O, "opk": opk,
                                "lfd": fk_d["frm"], "D": D, "dpk": dpk})
    finally:
        conn.close()
    return triples


def _name_col(conn, table: str, exclude: str) -> Optional[str]:
    for name, typ, _pk in _cols(conn, table):
        if name.lower() == exclude.lower() or not _IDENT.match(name):
            continue
        if "CHAR" in typ.upper() or "TEXT" in typ.upper():
            return name
    return None


def _divisor_filters(conn, D: str) -> List[Tuple[str, str]]:
    """Candidate (col, value) divisor filters: text columns whose value groups
    >= 2 divisor rows (so the filtered divisor stays non-trivial)."""
    out = []
    for name, typ, _pk in _cols(conn, D):
        if "CHAR" not in typ.upper() and "TEXT" not in typ.upper():
            continue
        if not _IDENT.match(name):
            continue
        try:
            cur = conn.execute(
                f'SELECT "{name}", COUNT(*) c FROM "{D}" '
                f'WHERE "{name}" IS NOT NULL GROUP BY "{name}" '
                f'HAVING c >= 2 ORDER BY c DESC LIMIT 3')
            for val, _c in cur.fetchall():
                sval = str(val)
                if "'" in sval or not sval.strip():
                    continue
                out.append((name, sval))
        except sqlite3.Error:
            continue
    return out


# ── query authoring ──────────────────────────────────────────────────────────

def _fsql(alias: str, filt: Optional[Tuple[str, str]]) -> str:
    return f"{alias}.{filt[0]} = '{filt[1]}'" if filt else ""


def build_queries(tr: Dict, filt: Optional[Tuple[str, str]],
                  name_col: Optional[str]) -> Dict[str, str]:
    L, lfo, O, opk, lfd, D, dpk = (tr["L"], tr["lfo"], tr["O"], tr["opk"],
                                   tr["lfd"], tr["D"], tr["dpk"])
    sel = f"o.{opk}" + (f", o.{name_col}" if name_col else "")
    dw = _fsql("d", filt)
    d_and = f"{dw} AND " if dw else ""
    d_where = f" WHERE {dw}" if dw else ""
    d2w = _fsql("d2", filt)
    d2_where = f" WHERE {d2w}" if d2w else ""

    gold = (f"SELECT {sel} FROM {O} o WHERE NOT EXISTS ("
            f"SELECT 1 FROM {D} d WHERE {d_and}NOT EXISTS ("
            f"SELECT 1 FROM {L} l WHERE l.{lfo} = o.{opk} AND l.{lfd} = d.{dpk}))")

    m8 = (f"SELECT {sel} FROM {O} o WHERE o.{opk} IN ("
          f"SELECT l.{lfo} FROM {L} l JOIN {D} d ON l.{lfd} = d.{dpk}{d_where})")

    m3 = (f"SELECT {sel} FROM {O} o WHERE o.{opk} NOT IN ("
          f"SELECT l.{lfo} FROM {L} l WHERE l.{lfd} NOT IN ("
          f"SELECT d.{dpk} FROM {D} d{d_where}))")

    m9 = (f"SELECT {sel} FROM {O} o WHERE NOT EXISTS ("
          f"SELECT 1 FROM {D} d WHERE {d_and}NOT EXISTS ("
          f"SELECT 1 FROM {L} l WHERE l.{lfd} = d.{dpk}))")

    alt = (f"SELECT {sel} FROM {O} o "
           f"JOIN {L} l ON l.{lfo} = o.{opk} "
           f"JOIN {D} d ON d.{dpk} = l.{lfd}{d_where} "
           f"GROUP BY {sel} "
           f"HAVING COUNT(DISTINCT d.{dpk}) = (SELECT COUNT(*) FROM {D} d2{d2_where})")

    return {"gold": gold, "IN_FOR_DIVISION": m8, "NOT_IN_vs_NOT_EXISTS": m3,
            "MISSING_CORRELATED_REF": m9, "alt": alt}


# ── validation ───────────────────────────────────────────────────────────────

def _rows_key(rows: List[Dict]) -> frozenset:
    return frozenset(
        tuple(sorted((k.lower(), str(v)) for k, v in r.items())) for r in rows)


def _exec_guarded(db_path: str, sql: str) -> Tuple[bool, List[Dict]]:
    """Execute with a VM-step guard so a pathological nested-loop query aborts
    after a few seconds instead of hanging the mining run."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    calls = {"n": 0}

    def _guard():
        calls["n"] += 1
        return 1 if calls["n"] > _GUARD_MAX_CALLS else 0   # nonzero aborts

    conn.set_progress_handler(_guard, _GUARD_EVERY_STEPS)
    try:
        cur = conn.execute(sql)
        rows = [dict(r) for r in cur.fetchmany(500)]
        return True, rows
    except sqlite3.Error:
        return False, []
    finally:
        conn.close()


def validate_problem(db_path: str, q: Dict[str, str], tr: Dict,
                     filt: Optional[Tuple[str, str]]) -> bool:
    ok, gold_rows = _exec_guarded(db_path, q["gold"])
    if not ok or not gold_rows:
        return False
    dw = _fsql("d", filt)
    cnt_sql = f"SELECT COUNT(*) AS c FROM {tr['D']} d" + (f" WHERE {dw}" if dw else "")
    ok, cnt_rows = _exec_guarded(db_path, cnt_sql)
    if not ok or not cnt_rows or int(list(cnt_rows[0].values())[0]) < 2:
        return False
    ok, m8_rows = _exec_guarded(db_path, q["IN_FOR_DIVISION"])
    if not ok:
        return False
    # Discriminating-instance requirement: the IN mutant must diverge here.
    return _rows_key(m8_rows) != _rows_key(gold_rows)


# ── NULL edge instance ───────────────────────────────────────────────────────

def make_null_edge(src_db: str, tr: Dict, tag: str) -> Optional[str]:
    """Copy the DB and insert a link row (lfo=NULL, lfd=<non-divisor value>).

    Gold (NOT EXISTS) is unaffected: a NULL outer key never joins. The M3
    NOT-IN-in-NOT-IN form now sees a NULL in its outer subquery output and
    collapses to the empty set — the classic latent NULL bug made observable.
    """
    os.makedirs(_EDGE_DIR, exist_ok=True)
    dst = os.path.join(_EDGE_DIR, f"{tag}_nulledge.sqlite")
    conn = None
    try:
        shutil.copyfile(src_db, dst)
        conn = sqlite3.connect(dst)
        conn.execute(
            f'INSERT INTO "{tr["L"]}" ("{tr["lfo"]}", "{tr["lfd"]}") '
            f"VALUES (NULL, '__nonexistent__')")
        conn.commit()
        conn.close()
        return dst
    except (sqlite3.Error, OSError):
        # e.g. NOT NULL constraint on the link FK column — no NULL edge is
        # constructible for this schema; M3 may then be a latent skip.
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        if os.path.exists(dst):
            try:
                os.remove(dst)
            except OSError:
                pass
        return None


# ── driver ───────────────────────────────────────────────────────────────────

def _rowcount(conn, table: str) -> Optional[int]:
    try:
        return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    except sqlite3.Error:
        return None


def mine_problems(root: Optional[str] = None, all_dbs: bool = False,
                  per_db: int = 3) -> List[Dict]:
    root = resolve_root(root)
    if all_dbs:
        db_dir = os.path.join(root, "database")
        db_ids = sorted(d for d in os.listdir(db_dir)
                        if os.path.isdir(os.path.join(db_dir, d)))
    else:
        db_ids = sorted({ex.db_id for ex in load_examples(root, split="dev")})

    problems: List[Dict] = []
    for db_id in db_ids:
        db_path = os.path.join(root, "database", db_id, f"{db_id}.sqlite")
        if not os.path.exists(db_path):
            continue
        conn = sqlite3.connect(db_path)
        kept = 0
        try:
            rc: Dict[str, Optional[int]] = {}
            for tr in find_triples(db_path):
                if kept >= per_db:
                    break
                # Size caps: skip triples whose nested-loop cost would be
                # pathological on unindexed tables (large sports/log DBs).
                for t in (tr["O"], tr["D"], tr["L"]):
                    if t not in rc:
                        rc[t] = _rowcount(conn, t)
                if (rc[tr["O"]] is None or rc[tr["O"]] > _MAX_O_ROWS or
                        rc[tr["D"]] is None or rc[tr["D"]] > _MAX_D_ROWS or
                        rc[tr["L"]] is None or rc[tr["L"]] > _MAX_L_ROWS or
                        rc[tr["O"]] == 0 or rc[tr["D"]] == 0 or rc[tr["L"]] == 0):
                    continue
                name_col = _name_col(conn, tr["O"], tr["opk"])
                for filt in [None] + _divisor_filters(conn, tr["D"]):
                    q = build_queries(tr, filt, name_col)
                    if validate_problem(db_path, q, tr, filt):
                        problems.append({
                            "db_id": db_id, "db_path": db_path, "triple": tr,
                            "filter": list(filt) if filt else None,
                            "queries": q,
                        })
                        kept += 1
                        break
        finally:
            conn.close()
    return problems


def evaluate(problems: List[Dict], verbose: bool = True) -> Dict:
    per_key = {k: {"applied": 0, "detected": 0, "skipped": 0, "misses": []}
               for k in MUTANT_KEYS}
    alt_stats = {"n": 0, "equivalent": 0, "raw_flagged": 0, "fp": 0}
    self_clean = 0

    for i, p in enumerate(problems):
        tag = f"{p['db_id']}_{i}"
        edge = make_null_edge(p["db_path"], p["triple"], tag)
        prob = GenericProblem(
            problem_id=f"div::{tag}", db_path=p["db_path"],
            gold_sql=p["queries"]["gold"], problem_type="DIVISION",
            db_id=p["db_id"], source="spider_division_transfer",
            edge_db_paths=[edge] if edge else [],
        )
        # Gold self-check: authored gold must not raise misconceptions.
        r = analyze(prob, p["queries"]["gold"])
        if not r.detected:
            self_clean += 1

        for key in MUTANT_KEYS:
            r = analyze(prob, p["queries"][key])
            if r.are_equivalent and r.edges_ok:
                per_key[key]["skipped"] += 1     # latent bug on this data
                continue
            per_key[key]["applied"] += 1
            if key in r.detected:
                per_key[key]["detected"] += 1
            else:
                per_key[key]["misses"].append(
                    {"db_id": p["db_id"], "detected": r.detected, "raw": r.raw})

        r = analyze(prob, p["queries"]["alt"])
        alt_stats["n"] += 1
        alt_stats["equivalent"] += 1 if r.are_equivalent else 0
        if r.are_equivalent:
            alt_stats["raw_flagged"] += 1 if r.raw else 0
            alt_stats["fp"] += 1 if r.detected else 0

    summary = {"problems": len(problems),
               "dbs": len({p["db_id"] for p in problems}),
               "gold_self_clean": self_clean,
               "per_key": per_key, "alt": alt_stats}
    if verbose:
        _print_summary(summary)
    return summary


def _print_summary(s: Dict) -> None:
    print("=" * 68)
    print(" PHASE 3-DIV - Schema-transfer division evaluation (Spider DBs)")
    print("=" * 68)
    print(f"division problems authored : {s['problems']} over {s['dbs']} databases")
    print(f"gold self-check clean      : {s['gold_self_clean']} / {s['problems']}")
    print("-" * 68)
    print(f"{'Misconception':<26}{'Applied':>8}{'Detected':>9}{'Rate':>8}{'Skip*':>7}")
    for k in MUTANT_KEYS:
        v = s["per_key"][k]
        rate = v["detected"] / v["applied"] * 100 if v["applied"] else 0.0
        print(f"{k:<26}{v['applied']:>8}{v['detected']:>9}{rate:>7.1f}%{v['skipped']:>7}")
    print("* skip = output-equivalent on every instance incl. NULL edge (latent)")
    a = s["alt"]
    print("-" * 68)
    print(f"alternate-correct (GROUP BY/HAVING form): n={a['n']} "
          f"equivalent={a['equivalent']}")
    if a["equivalent"]:
        print(f"  raw shape-flag rate : {a['raw_flagged']}/{a['equivalent']} "
              f"= {a['raw_flagged']/a['equivalent']*100:.1f}%")
        print(f"  user-facing FP      : {a['fp']}/{a['equivalent']} "
              f"= {a['fp']/a['equivalent']*100:.1f}%")
    for k in MUTANT_KEYS:
        for m in s["per_key"][k]["misses"]:
            print(f"  MISS {k} on {m['db_id']}: detected={m['detected']} raw={m['raw']}")


def main(root=None, all_dbs=False, per_db=2, save=True) -> Dict:
    problems = mine_problems(root, all_dbs=all_dbs, per_db=per_db)
    summary = evaluate(problems, verbose=True)
    if save:
        os.makedirs(_DERIVED_DIR, exist_ok=True)
        scope = "all" if all_dbs else "dev"
        ppath = os.path.join(_DERIVED_DIR, f"division_problems_{scope}.json")
        with open(ppath, "w", encoding="utf-8") as fh:
            json.dump([{k: v for k, v in p.items() if k != "db_path"}
                       for p in problems], fh, indent=1)
        spath = os.path.join(_DERIVED_DIR, f"division_summary_{scope}.json")
        with open(spath, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1)
        print(f"\nWrote:\n  {ppath}\n  {spath}")
    return summary


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Schema-transfer division evaluation")
    ap.add_argument("--root", default=None)
    ap.add_argument("--all-dbs", action="store_true",
                    help="mine all Spider DBs, not just the dev-split schemas")
    ap.add_argument("--per-db", type=int, default=2)
    ap.add_argument("--no-save", action="store_true")
    args = ap.parse_args()
    main(root=args.root, all_dbs=args.all_dbs, per_db=args.per_db,
         save=not args.no_save)

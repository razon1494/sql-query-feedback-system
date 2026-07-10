"""
Phase 1 — Spider corpus ingestion.

Loads a local Spider install into normalized records the rest of the external
pipeline consumes. Spider ships:

    <SPIDER_ROOT>/
        tables.json                       # schema metadata (200 DBs)
        dev.json                          # ~1034 dev examples
        train_spider.json                 # ~7000 train examples
        database/<db_id>/<db_id>.sqlite   # one real SQLite DB per schema

Each example is {db_id, question, query (gold SQL), query_toks, sql (parsed)}.

Design choices
--------------
* We read each schema **directly from the .sqlite file** via PRAGMA rather than
  trusting tables.json's flat-index column format. The live database is the
  ground truth we execute against anyway, and PRAGMA is far less error-prone.
* `load_examples` is tolerant: examples whose .sqlite is missing are skipped
  (reported), so a partial download still works.
* SPIDER_ROOT resolves from (1) explicit arg, (2) $SPIDER_ROOT env var,
  (3) the bundled default external/data/spider. Point it at the mini fixture
  for tests, or at a real Spider download for the real runs.
"""
import os
import sys
import json
import sqlite3
from dataclasses import dataclass, field
from typing import List, Dict, Optional

# external/spider/ingest.py -> external/spider -> external -> repo root
_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
# Make `backend.*` and `external.*` importable even when this file is run
# directly (e.g. `python ingest.py` from inside external/spider/).
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

DEFAULT_SPIDER_ROOT = os.path.join(_EXTERNAL_DIR, "data", "spider")

_SPLIT_FILES = {
    "dev": "dev.json",
    "train": "train_spider.json",
}


@dataclass
class SpiderExample:
    db_id: str
    question: str
    gold_sql: str
    split: str
    sqlite_path: str
    schema: Dict[str, List[str]] = field(default_factory=dict)  # table -> [columns]

    def to_dict(self) -> Dict:
        return {
            "db_id": self.db_id,
            "question": self.question,
            "gold_sql": self.gold_sql,
            "split": self.split,
            "sqlite_path": self.sqlite_path,
            "schema": self.schema,
        }


def resolve_root(root: Optional[str] = None) -> str:
    return root or os.environ.get("SPIDER_ROOT") or DEFAULT_SPIDER_ROOT


def _examples_file(root: str, split: str) -> str:
    if split not in _SPLIT_FILES:
        raise ValueError(f"split must be one of {list(_SPLIT_FILES)}, got {split!r}")
    return os.path.join(root, _SPLIT_FILES[split])


def sqlite_path_for(root: str, db_id: str) -> str:
    return os.path.join(root, "database", db_id, f"{db_id}.sqlite")


def is_present(root: Optional[str] = None, split: str = "dev") -> bool:
    """True if at least the examples file for `split` exists under root."""
    root = resolve_root(root)
    return os.path.exists(_examples_file(root, split))


def get_schema(sqlite_path: str) -> Dict[str, List[str]]:
    """Return {table_name: [column, ...]} read straight from the SQLite file."""
    schema: Dict[str, List[str]] = {}
    conn = sqlite3.connect(sqlite_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            cur.execute(f'PRAGMA table_info("{t}")')
            schema[t] = [row[1] for row in cur.fetchall()]
    finally:
        conn.close()
    return schema


def load_examples(root: Optional[str] = None, split: str = "dev",
                  require_sqlite: bool = True, with_schema: bool = False,
                  limit: Optional[int] = None) -> List[SpiderExample]:
    """Load normalized Spider examples for a split.

    Parameters
    ----------
    require_sqlite : skip examples whose .sqlite file is absent (default True).
    with_schema    : eagerly read each schema from its .sqlite (default False;
                     schemas are cached per-db to avoid re-reading).
    limit          : cap the number of examples returned (after filtering).
    """
    root = resolve_root(root)
    path = _examples_file(root, split)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Spider {split} examples not found at {path}. "
            f"Run external/spider/download.py for setup instructions, or set "
            f"$SPIDER_ROOT to your Spider install."
        )

    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    out: List[SpiderExample] = []
    schema_cache: Dict[str, Dict[str, List[str]]] = {}
    for ex in data:
        db_id = ex["db_id"]
        gold = ex.get("query") or ex.get("gold") or ex.get("sql_str") or ""
        sqlite_path = sqlite_path_for(root, db_id)
        if require_sqlite and not os.path.exists(sqlite_path):
            continue

        schema: Dict[str, List[str]] = {}
        if with_schema and os.path.exists(sqlite_path):
            if db_id not in schema_cache:
                schema_cache[db_id] = get_schema(sqlite_path)
            schema = schema_cache[db_id]

        out.append(SpiderExample(
            db_id=db_id,
            question=ex.get("question", ""),
            gold_sql=gold.strip(),
            split=split,
            sqlite_path=sqlite_path,
            schema=schema,
        ))
        if limit is not None and len(out) >= limit:
            break
    return out


def smoke_test(root: Optional[str] = None, split: str = "dev",
               limit: int = 50, verbose: bool = True) -> Dict:
    """Execute a sample of gold queries to confirm the install is usable.

    Returns counts of {total, executed_ok, exec_error}. Uses the same
    execute_query path the pipeline uses, so a pass here means the harness can
    run against these databases.
    """
    from backend.query_executor import execute_query

    examples = load_examples(root, split=split, require_sqlite=True, limit=limit)
    total = len(examples)
    ok = 0
    errors: List[Dict] = []
    for ex in examples:
        res = execute_query(ex.gold_sql, db_path=ex.sqlite_path, db_name=ex.db_id)
        if res.success:
            ok += 1
        else:
            errors.append({"db_id": ex.db_id, "error": res.error,
                           "sql": ex.gold_sql[:120]})

    summary = {"total": total, "executed_ok": ok, "exec_error": len(errors)}
    if verbose:
        print(f"[spider smoke] split={split} root={resolve_root(root)}")
        print(f"  examples sampled : {total}")
        print(f"  gold executed OK : {ok}")
        print(f"  gold exec errors : {len(errors)}")
        for e in errors[:5]:
            print(f"    - {e['db_id']}: {e['error']}")
    return summary


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Spider ingestion smoke test")
    ap.add_argument("--root", default=None, help="Spider install root (or $SPIDER_ROOT)")
    ap.add_argument("--split", default="dev", choices=list(_SPLIT_FILES))
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()
    smoke_test(root=args.root, split=args.split, limit=args.limit)

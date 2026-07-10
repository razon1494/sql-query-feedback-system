"""
Spider acquisition helper.

Spider is ~1.4 GB and is NOT vendored in this repo. This script documents the
two supported ways to obtain it and verifies a local install.

Option A — HuggingFace datasets (recommended, scriptable)
---------------------------------------------------------
    pip install datasets
    python external/spider/download.py --hf

This pulls the `xlangai/spider` dataset. NOTE: the HF dataset provides the
JSON examples; the per-database .sqlite files come from the official archive
(Option B). If you only need gold SQL + schema you can adapt, but the pipeline
EXECUTES queries, so the .sqlite files are required.

Option B — Official archive (provides the .sqlite databases)
------------------------------------------------------------
1. Download `spider_data.zip` from the official Spider page:
       https://yale-lily.github.io/spider
   (mirror: the dataset is also on HuggingFace `xlangai/spider` under files).
2. Unzip so the layout is:
       external/data/spider/tables.json
       external/data/spider/dev.json
       external/data/spider/train_spider.json
       external/data/spider/database/<db_id>/<db_id>.sqlite
3. Verify:
       python external/spider/download.py --verify

License: Spider is released under CC BY-SA 4.0 — fine for academic use; cite
Yu et al., EMNLP 2018 in the paper.
"""
import os
import sys

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_ROOT = os.path.join(_EXTERNAL_DIR, "data", "spider")


def verify(root: str = DEFAULT_ROOT) -> bool:
    checks = {
        "tables.json": os.path.join(root, "tables.json"),
        "dev.json": os.path.join(root, "dev.json"),
        "database/": os.path.join(root, "database"),
    }
    ok = True
    print(f"Verifying Spider install at: {root}")
    for label, path in checks.items():
        present = os.path.exists(path)
        ok = ok and present
        print(f"  [{'OK' if present else 'MISSING'}] {label}")
    if os.path.isdir(checks["database/"]):
        dbs = [d for d in os.listdir(checks["database/"])
               if os.path.isdir(os.path.join(checks["database/"], d))]
        print(f"  database subfolders: {len(dbs)}")
    if ok:
        print("Spider install looks usable. Run: python external/spider/ingest.py")
    else:
        print("Incomplete — see the module docstring for setup steps.")
    return ok


def hf_download(root: str = DEFAULT_ROOT) -> None:
    try:
        from datasets import load_dataset
    except ImportError:
        print("`datasets` not installed. Run: pip install datasets")
        sys.exit(1)
    import json
    os.makedirs(root, exist_ok=True)
    print("Loading xlangai/spider from HuggingFace ...")
    ds = load_dataset("xlangai/spider")
    for split_name, out_name in (("validation", "dev.json"), ("train", "train_spider.json")):
        if split_name not in ds:
            continue
        rows = [{"db_id": r["db_id"], "question": r["question"], "query": r["query"]}
                for r in ds[split_name]]
        with open(os.path.join(root, out_name), "w", encoding="utf-8") as fh:
            json.dump(rows, fh, indent=1)
        print(f"  wrote {out_name}: {len(rows)} examples")
    print("\nNOTE: HF gives JSON only. You still need the .sqlite databases from")
    print("the official archive (Option B in the docstring) under database/.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--verify", action="store_true", help="check a local install")
    ap.add_argument("--hf", action="store_true", help="fetch JSON via HuggingFace datasets")
    ap.add_argument("--root", default=DEFAULT_ROOT)
    args = ap.parse_args()
    if args.hf:
        hf_download(args.root)
    else:
        verify(args.root)

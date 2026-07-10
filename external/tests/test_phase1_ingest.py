"""
Phase 1 test — Spider ingestion against the mini fixture.

Run:  python external/tests/test_phase1_ingest.py
Proves: load_examples + get_schema + smoke_test all work on a Spider-shaped
install, using the same execute_query path the real pipeline uses.
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture
from external.spider import ingest


def main() -> int:
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root  # exercise env-var resolution too

    failures = []

    # 1. presence
    if not ingest.is_present(split="dev"):
        failures.append("is_present() returned False for the fixture")

    # 2. load_examples
    examples = ingest.load_examples(split="dev", with_schema=True)
    if len(examples) != 4:
        failures.append(f"expected 4 examples, got {len(examples)}")
    db_ids = {e.db_id for e in examples}
    if db_ids != {"music_mini"}:
        failures.append(f"unexpected db_ids: {db_ids}")

    # 3. schema read straight from the .sqlite
    schema = ingest.get_schema(examples[0].sqlite_path)
    if set(schema) != {"artist", "album", "track", "playlist", "playlist_track"}:
        failures.append(f"schema tables wrong: {sorted(schema)}")
    if schema.get("album") != ["album_id", "title", "artist_id", "year", "genre"]:
        failures.append(f"album columns wrong: {schema.get('album')}")

    # 4. smoke test — every gold query must execute
    summary = ingest.smoke_test(split="dev", limit=10, verbose=True)
    if summary["executed_ok"] != summary["total"] or summary["total"] != 4:
        failures.append(f"smoke_test summary off: {summary}")

    print()
    if failures:
        print("PHASE 1 INGEST: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 1 INGEST: PASS  (4 examples loaded, schema read, 4/4 gold executed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

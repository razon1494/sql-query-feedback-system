"""
Phase 3-DIV test — division-transfer generator on the mini fixture.

Run:  python external/tests/test_phase3_division.py

The fixture's playlist/playlist_track M:N is built so that "tracks appearing on
every playlist" is a real division problem with a partial-match counterexample
(track 2 is on one playlist only). Proves, on a foreign schema:
  (1) FK mining finds the division triple and validation accepts it
  (2) gold self-check raises nothing
  (3) M8 (IN), M3 (NOT IN), M9 (dropped correlation) are all detected
  (4) the GROUP BY/HAVING alternate is equivalent, raw-flagged, and suppressed
"""
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.fixtures.build_mini_spider import build as build_fixture
from external.spider.gen_division import (
    find_triples, mine_problems, evaluate, MUTANT_KEYS,
)


def main() -> int:
    root = build_fixture()
    os.environ["SPIDER_ROOT"] = root
    db_path = os.path.join(root, "database", "music_mini", "music_mini.sqlite")
    failures = []

    # (1) triple mining
    triples = find_triples(db_path)
    combos = {(t["O"], t["L"], t["D"]) for t in triples}
    print(f"(1) triples mined: {sorted(combos)}")
    if ("track", "playlist_track", "playlist") not in combos:
        failures.append(f"(1) expected (track, playlist_track, playlist) in {combos}")

    # fixture root is Spider-shaped but has no train file; mine via dev split
    problems = mine_problems(root=root, all_dbs=True, per_db=2)
    print(f"(1) problems accepted: {len(problems)}")
    if len(problems) < 1:
        failures.append("(1) no division problem passed validation on fixture")
        print("PHASE 3-DIV: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1

    # (2)-(4) full evaluation
    s = evaluate(problems, verbose=True)

    if s["gold_self_clean"] != s["problems"]:
        failures.append(f"(2) gold self-check not clean: {s['gold_self_clean']}/{s['problems']}")

    for key in MUTANT_KEYS:
        v = s["per_key"][key]
        if v["applied"] == 0 and v["skipped"] == 0:
            failures.append(f"(3) {key}: no variants ran at all")
        if v["applied"] and v["detected"] != v["applied"]:
            failures.append(f"(3) {key}: detected {v['detected']}/{v['applied']}")

    a = s["alt"]
    if a["equivalent"] != a["n"]:
        failures.append(f"(4) alternate not equivalent: {a['equivalent']}/{a['n']}")
    if a["equivalent"] and a["raw_flagged"] == 0:
        failures.append("(4) alternate should raise RAW shape flags (division shape differs)")
    if a["fp"] != 0:
        failures.append(f"(4) alternate produced user-facing FP: {a['fp']}")

    print()
    if failures:
        print("PHASE 3-DIV: FAIL")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PHASE 3-DIV: PASS  (mined, validated, M8/M3/M9 detected, alternate suppressed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

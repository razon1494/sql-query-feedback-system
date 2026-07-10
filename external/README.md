# External Validity Pipeline (Spider)

This package adds **real external validation** to the SQL feedback system using
the [Spider](https://yale-lily.github.io/spider) text-to-SQL benchmark, replacing
the original hand-authored "Spider-style" corpora that ran only on the in-house
seven-table university schema.

## Why

The detector's credibility hinges on showing it works on **schemas it was not
built for**. Spider provides 200 real databases with gold SQL — external schemas,
external data, real SQLite files we can execute against. (Spider does *not*
provide student errors or division problems, so the in-house university corpus is
**kept** for division depth; Spider supplies cross-schema breadth for
JOIN / aggregation / WHERE / set-op misconceptions. Report per-misconception
coverage honestly.)

## Layout

```
external/
  harness/generic_problem.py   Phase 0 — schema-agnostic wrapper over generate_feedback()
  spider/ingest.py             Phase 1 — load a Spider install into normalized records
  spider/download.py           Spider acquisition + verification helper
  fixtures/build_mini_spider.py  tiny music-schema fixture (no 1.4 GB download needed for tests)
  tests/                       runnable Phase 0 / Phase 1 checks
  data/spider/                 <- put the real Spider install here (gitignored)
```

## Status

| Phase | What | State |
|-------|------|-------|
| 0 | Decouple detector from university schema (`GenericProblem`, `analyze`) | ✅ done + tested |
| 1 | Spider ingestion (`load_examples`, `get_schema`, `smoke_test`) | ✅ 1034/1034 dev gold execute |
| 2 | Auto-classify Spider problems into the 6 types (`classify.py`) | ✅ AGG 50.4% / JOIN 37.2% / SET_OP 7.4% / SUBQ 5.0% / DIV 0 / NULL 0 |
| 3.1 | Alternate-correct corpus (`gen_alternates.py`) | ✅ 357 validated, 0.0% user-facing FPR, 11.8% raw |
| 3-DIV | Schema-transfer division eval (`gen_division.py` + `division_scan.py`) | ✅ 0 division shapes in all 9,693 Spider queries (measured); **53 authored problems over 29 foreign schemas** — M8 53/53, M3 52/52 (+1 latent skip), M9 53/53; alternate 53/53 equivalent, 0 FP |
| 3.2 | Literature-motivated wrong-query corpus (`gen_wrong.py` + `annotation.py`) | ✅ 748 wrong queries / 419 golds / 20 DBs; detection 538/540 = 99.6%; uncorrelated-EXISTS gap probe 0/2 (documented miss); blind κ sheets generated — **human labeling pending** |
| 3b | Bonus: harvest naturally-occurring errors from a text-to-SQL model | ⬜ |
| 4 | Generalized eval runner + baselines (output-only, shape-only) | ⬜ |
| 5 | Failure analysis + paper rewrite | ⬜ |

## Run the tests (no download required)

```bash
python external/tests/test_phase1_ingest.py    # ingestion on the mini fixture
python external/tests/test_phase0_harness.py   # detector on a non-university (music) schema
```

Both build a throwaway `fixtures/mini_spider/` Spider-shaped install and exercise
the real pipeline. `test_phase0_harness` proves detection, false-positive
suppression, and no self-false-positive on a schema the detector has never seen.

## Point the pipeline at real Spider

```bash
python external/spider/download.py            # setup instructions / --verify / --hf
# ...place the install under external/data/spider/ ...
python external/spider/ingest.py --split dev --limit 100   # smoke test gold execution
```

`SPIDER_ROOT` env var overrides the default `external/data/spider` location.

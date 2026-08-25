# SQL Query Feedback System

> **A Research Prototype for Automated SQL Query Evaluation and Feedback Generation**

**Developed by:** Mohammad Arifur Rahman  
**Submitted to:** Dr. Hasan Jamil, Department of Computer Science, University of Idaho

---

## 🌐 Live Demo

**🔗 [https://sql-query-feedback-system.onrender.com](https://sql-query-feedback-system.onrender.com)**

> Always on. The service runs on a paid Render instance, so there is no cold start and no waiting — the link responds immediately.

---

## What Is This?

This system automatically evaluates student SQL queries and generates rich, structured, pedagogical feedback. It goes far beyond simply marking answers right or wrong — it traces *why* a query is wrong, identifies the specific conceptual misconception, tests it against edge cases, and produces a graded report with actionable corrections.

The system has a **special focus on relational division queries** — the hardest SQL concept for students — where it implements a 3-step provenance trace to pinpoint exactly where a student's logic diverges from the correct universal quantification pattern (∀ vs ∃).

---

## 🚀 Quick Demo (Try This First)

1. Open **[https://sql-query-feedback-system.onrender.com](https://sql-query-feedback-system.onrender.com)**
2. Select **"Division: Students Who Took ALL DB Courses"** in the left sidebar
3. Click **"Load Wrong Answer"**
4. Click **▶ Analyze Query**
5. Score: **65/100** — Nancy is incorrectly included (she took CS360 but not CS460)
6. Click the **Provenance** tab to see exactly *why* the IN operator fails
7. Go back to Editor → Click **"Load Correct Answer"** → **100/100** ✓

---

## Features

| Feature | Description |
|---------|-------------|
| **Real SQL Execution** | Queries run against a live SQLite database — not simulated |
| **AST Comparison** | Parses both queries into Abstract Syntax Trees and diffs them structurally |
| **Syntax Validation** | Catches misspelled keywords (e.g. `FRM` → did you mean `FROM`?), unclosed parentheses, missing clauses |
| **Provenance Tracing** | 3-step trace: divisor set → per-student coverage → division check |
| **Edge Case Testing** | 5 specialized databases (empty courses, partial match, all enrolled, etc.) |
| **Misconception Detection** | Identifies patterns like IN-vs-NOT-EXISTS, missing HAVING, hardcoded thresholds |
| **Graded Feedback** | Syntax 20% / Logic 30% / Results 40% / Edge Cases 10% |
| **Evidence-Gated Diagnosis** | A diagnosis is released only when execution exhibits the effect that diagnosis specifically predicts, not merely that the query is wrong |
| **Alternate Solution Detection** | Recognizes structurally different but semantically correct queries |
| **30 Problem Sets** | Division, JOIN, Aggregation, Set Operation, Subquery, and NULL problems over a 7-table schema |
| **External Validation** | Evaluated against the real Spider benchmark (20–29 foreign schemas) — see below |

---

## How It Compares to Related Work

| Feature | RATest (SIGMOD'19) | I-REX (VLDB'20) | CaJaDE (VLDB'22) | **This System** |
|---------|--------------------|-----------------|------------------|-----------------|
| Real SQL execution | ✓ | ✓ | ✓ | ✓ |
| Small counterexamples | ✓ | ✓ | — | ✓ |
| Division query focus | — | — | — | **✓** |
| Edge case library | — | — | — | **✓** |
| Graded feedback | — | — | — | **✓** |
| Misconception detection | — | — | — | **✓** |
| Syntax validation | — | — | — | **✓** |
| Context-augmented provenance | — | — | ✓ | Planned |

---

## External Validation on the Spider Benchmark

The detector is evaluated against the real [Spider](https://yale-lily.github.io/spider)
text-to-SQL benchmark — foreign schemas and data the system was never tuned on.
The full pipeline, corpora, and reproduction instructions live in
[`external/README.md`](external/README.md). Headline results:

| Experiment | Corpus | Result |
|---|---|---|
| False positives (alternate-correct) | 357 execution-validated rewrites of Spider dev gold | **0.0% user-facing FPR** (11.8% raw shape-flag rate, all suppressed) |
| Wrong-query detection | 748 literature-motivated corruptions, 20 schemas | **545/547 = 99.6%** detected (2 misses = documented uncorrelated-EXISTS gap) |
| Attribution precision | same 547 diverging corruptions | **93.5% -> 99.8%** once release is gated on each diagnosis's predicted effect |
| Division schema transfer | 53 authored division problems, 29 schemas | M8 53/53, M3 52/52, M9 53/53; alternate rewrites 0 FP |
| Division scarcity scan | all 9,693 Spider queries | **no `EXISTS` in any form**, zero division-by-counting idioms, zero `IS NULL` filters (measured) |
| Baseline comparison | output-only vs shape-only vs two-tier, same corpora | two-tier keeps 99.6% diagnosis at 0% FPR; baselines trade one for the other |

Key methodology points: corruption operators are defined from the empirical
misconception literature (not from detector rules), every corpus entry is
execution-validated on the real Spider database, and latent
(output-equivalent) corruptions are reported as skips rather than counted.
No inter-rater agreement is claimed: the emitted labels have not been
validated against independent human annotation, which remains future work.

```bash
# reproduce (after placing Spider under external/data/spider/ - see external/spider/download.py)
python external/spider/ingest.py --split dev        # smoke test
python external/spider/classify.py --split dev     # problem-type distribution
python external/spider/gen_alternates.py           # FP experiment
python external/spider/gen_wrong.py                # detection experiment
python external/spider/gen_division.py --all-dbs --per-db 3   # division schema transfer
python external/spider/division_scan.py            # division scarcity scan
python external/eval/run_baselines.py              # Table 2 baseline comparison
python external/eval/failure_probes.py             # 12-probe failure suite
```

> `--per-db 3` is required to reproduce the published 53-problem division corpus.
> The command-line default of 2 yields 44 problems.

In-domain checks need no Spider download:

```bash
python eval_detection_rates.py                     # 73/73 single-misconception mutations
python eval_false_positives.py                     # 30 reference queries, 0 flags
python external/eval/in_domain_alternates.py       # 19 alternates: 8 raw, 0 user-facing
```

All offline tests (no Spider download needed) run against a generated
music-schema fixture: `python external/tests/test_phase*.py`.

---

## Run Locally

### Requirements
- Python 3.10+
- pip

### Install & Run
```bash
git clone https://github.com/razon1494/sql-query-feedback-system.git
cd sql-query-feedback-system
pip install flask
python app.py
```

Open your browser at **http://localhost:5000**

> The databases (SQLite) are created automatically on first run. No configuration needed.

---

## Project Structure

```
sql-query-feedback-system/
│
├── app.py                        ← Flask REST API server (entry point)
├── requirements.txt              ← Python dependencies (Flask + Gunicorn)
├── render.yaml                   ← Render.com deployment config
├── README.md
│
├── backend/
│   ├── sql_parser.py             ← SQL → AST parser + syntax validator + structural diff
│   ├── query_executor.py         ← Safe SQL execution, result comparison, counterexamples
│   ├── provenance.py             ← 3-step provenance trace engine (division-focused)
│   ├── feedback_generator.py     ← Grading engine + misconception detection
│   └── problems.py               ← Problem set definitions (add new problems here)
│
├── database/
│   └── init_db.py                ← Creates main.db + 5 edge-case databases on startup
│
├── external/                     ← Spider external-validation pipeline (see external/README.md)
│   ├── harness/                  ← Schema-agnostic wrapper over the feedback pipeline
│   ├── spider/                   ← Ingestion, classification, corpus generators
│   ├── eval/                     ← Baselines, failure probes, in-domain alternates
│   ├── fixtures/                 ← Offline music-schema test fixture (no download needed)
│   ├── tests/                    ← Runnable test suites for every phase
│   └── data/                     ← Spider install + derived corpora (gitignored)
│
└── frontend/
    └── templates/
        └── index.html            ← Full single-file UI (7 tabs)
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/problems` | List all 30 problems |
| `GET` | `/api/problems/<id>` | Problem details + reference query |
| `GET` | `/api/schema` | Database schema + sample rows |
| `POST` | `/api/analyze` | **Full 6-step analysis pipeline** |
| `POST` | `/api/execute` | Execute a single query |
| `GET` | `/api/health` | Health check |

### Example API Call

```bash
curl -X POST https://sql-query-feedback-system.onrender.com/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "problem_id": "div_db_courses",
    "student_query": "SELECT s.StuID, s.Name FROM Students s WHERE s.StuID IN (SELECT t.StuID FROM Takes t WHERE t.CourseID IN (SELECT c.CourseID FROM Courses c WHERE c.\"Group\" = '\''DB'\''))"
  }'
```

**Response includes:**
- `feedback.total_score` — numeric grade (0–100)
- `feedback.grade_letter` — A/B/C/D/F
- `feedback.items` — list of actionable feedback cards with syntax errors, logic issues, suggestions
- `feedback.misconceptions` — detected conceptual errors
- `parsing.diffs` — AST structural differences
- `provenance.steps` — step-by-step query trace
- `provenance.divergence_points` — specific tuples that expose the bug
- `edge_cases` — results across all 5 test databases
- `comparison.extra_rows` / `missing_rows` — result set differences

---

## Adding New Problems

Open `backend/problems.py` and append a new dict to the `PROBLEMS` list:

```python
{
    "id": "my_new_problem",           # unique string ID
    "title": "My Problem Title",       # shown in sidebar
    "description": "HTML description", # shown above editor
    "question": "One-line prompt",
    "type": "DIVISION",                # DIVISION | JOIN | AGGREGATION | SET_OP
    "difficulty": "hard",              # easy | medium | hard
    "concepts": ["NOT EXISTS", "..."],
    "base_query": """SELECT ...""",    # reference (correct) SQL
    "hint": "Tip for students",
    "common_mistakes": ["..."],
}
```

Save the file, restart the server — the problem appears automatically. **No other files need to be changed.**

---

## Problem Set (10 of the 30 Problems)

| # | Title | Type | Difficulty |
|---|-------|------|------------|
| 1 | Students Who Took ALL DB Courses | DIVISION | Hard |
| 2 | CS Students Who Took All CS-Group Courses | DIVISION | Hard |
| 3 | Instructors Who Teach ALL DB Courses | DIVISION | Hard |
| 4 | Students Enrolled in Intro DB (CS360) | JOIN | Easy |
| 5 | Instructors With Their Course Titles | JOIN | Easy |
| 6 | Students With Their Course Grades | JOIN | Easy |
| 7 | Students Enrolled in More Than 1 Course | AGGREGATION | Medium |
| 8 | Average Credits Per Student | AGGREGATION | Medium |
| 9 | Students NOT Enrolled in Any DB Course | SET_OP | Medium |
| 10 | Students in CS or DB Courses (UNION) | SET_OP | Medium |

---

## The Analysis Pipeline

```
Student Query
     │
     ▼
① PARSE & VALIDATE ── Syntax check: misspellings, missing clauses,
     │                  unmatched parentheses, unclosed quotes
     │                  SQL → AST (sql_parser.py)
     ▼
② DIFF ───────────── AST structural comparison
     │                WHERE.type, GROUP_BY, HAVING, SUBQUERY depth
     ▼
③ EXECUTE ─────────── Real SQLite execution (query_executor.py)
     │                 Safety checks, result rows, Jaccard similarity
     ▼
④ PROVENANCE ──────── 3-step trace (provenance.py)
     │                 Divisor set → Coverage → Division check
     │                 Identifies specific divergence tuples
     ▼
⑤ EDGE CASES ──────── 5 specialized databases (query_executor.py)
     │                 empty_courses, partial_match, all_enrolled,
     │                 single_course, no_students
     ▼
⑥ FEEDBACK ─────────── Graded report (feedback_generator.py)
                        Syntax / Logic / Results / Edge Cases
                        Structural tier PROPOSES a diagnosis; the evidence
                        gate RELEASES it only if the observed residual
                        matches what that diagnosis predicts
```

---

## Research Background

This prototype implements ideas from the following papers, all referenced in the system's Architecture tab:

1. Miao, Roy, Yang — *Explaining Wrong Queries Using Small Examples*, **SIGMOD 2019**
2. Miao et al. — *I-REX: Interactive Relational Query Explainer*, **VLDB 2020**
3. Li et al. — *CaJaDE: Explaining Query Results by Augmenting Provenance with Context*, **VLDB 2022**
4. Gilad et al. — *Understanding Queries by Conditional Instances*, **SIGMOD 2022**
5. Roy et al. — *How Database Theory Helps Teach Relational Queries*, **ICDT 2024**

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.10+, Flask 3.x |
| Production Server | Gunicorn |
| Hosting | Render.com (paid instance, always on) |
| Database | SQLite (via Python stdlib `sqlite3`) |
| SQL Parser | Custom-built (Python `re`, no external parser needed) |
| Frontend | Vanilla HTML/CSS/JS (zero npm dependencies) |
| Fonts | JetBrains Mono, Syne, Inter (Google Fonts) |

---

## Deployment Notes

This app is deployed on a **paid Render.com instance**:
- ✅ Public HTTPS URL, always on — no spin-down, no cold start
- ✅ Auto-deploys on every `git push` to `main`
- ℹ️ SQLite databases are re-created on each deploy (stateless — all data is seeded from `init_db.py`)

---

## License

This project is a research prototype developed with Dr. Hasan Jamil, Department of Computer Science, University of Idaho.  
© 2026 Mohammad Arifur Rahman. All rights reserved.

# SQL Query Feedback System

> **A Research Prototype for Automated SQL Query Evaluation and Feedback Generation**

**Developed by:** Mohammad Arifur Rahman  
**Submitted to:** Dr. Hasan Jamil, Department of Computer Science, University of Idaho

---

## 🌐 Live Demo

**🔗 [https://sql-query-feedback-system.onrender.com](https://sql-query-feedback-system.onrender.com)**

> ⏳ **Please allow 30–60 seconds on first load.**
> This app is hosted on Render's free tier, which automatically spins down the server after a period of inactivity to save resources. When you visit the link, the server wakes up and initializes the database — this takes about 30–60 seconds. Once loaded, the system runs at full speed. Thank you for your patience!

---

## What Is This?

This system automatically evaluates student SQL queries and generates rich, structured, pedagogical feedback. It goes far beyond simply marking answers right or wrong — it traces *why* a query is wrong, identifies the specific conceptual misconception, tests it against edge cases, and produces a graded report with actionable corrections.

The system has a **special focus on relational division queries** — the hardest SQL concept for students — where it implements a 3-step provenance trace to pinpoint exactly where a student's logic diverges from the correct universal quantification pattern (∀ vs ∃).

---

## 🚀 Quick Demo (Try This First)

1. Open **[https://sql-query-feedback-system.onrender.com](https://sql-query-feedback-system.onrender.com)** *(wait 30–60 seconds if loading)*
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
| **Alternate Solution Detection** | Recognizes structurally different but semantically correct queries |
| **10 Problem Sets** | Division, JOIN, Aggregation, and Set Operation problems |

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
└── frontend/
    └── templates/
        └── index.html            ← Full single-file UI (7 tabs)
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/problems` | List all 10 problems |
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

## Problem Set (10 Problems)

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
                        Misconception detection + fix suggestions
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
| Hosting | Render.com (free tier) |
| Database | SQLite (via Python stdlib `sqlite3`) |
| SQL Parser | Custom-built (Python `re`, no external parser needed) |
| Frontend | Vanilla HTML/CSS/JS (zero npm dependencies) |
| Fonts | JetBrains Mono, Syne, Inter (Google Fonts) |

---

## Deployment Notes

This app is deployed on **Render.com free tier**:
- ✅ Free hosting with public HTTPS URL
- ✅ Auto-deploys on every `git push` to `main`
- ⚠️ Spins down after 15 minutes of inactivity — first visit after sleep takes ~30–60 seconds to wake up
- ⚠️ SQLite database is re-created on each deploy (stateless — all data is seeded from `init_db.py`)

To upgrade to always-on hosting, Render's paid tier starts at $7/month.

---

## License

This project is a PhD research prototype submitted to Dr. Hasan Jamil at the University of Idaho.  
© 2025 Mohammad Arifur Rahman. All rights reserved.

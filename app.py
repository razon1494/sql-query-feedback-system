"""
Flask API Server — SQL Query Feedback System
Endpoints:
  GET  /api/problems          → list all problems
  GET  /api/problems/<id>     → get problem details
  GET  /api/schema            → get database schema + sample data
  POST /api/analyze           → full analysis pipeline
  POST /api/execute           → run a single query
  GET  /api/edge-cases/<id>   → edge case results for a problem
"""
import os
import sys
import json

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, request, jsonify, render_template, send_from_directory
from flask import Response

from backend.sql_parser import parse_sql, compare_queries
from backend.query_executor import (
    execute_query, full_edge_case_analysis, compare_results,
    find_minimal_counterexample, MAIN_DB
)
from backend.provenance import compute_provenance
from backend.feedback_generator import generate_feedback
from backend.problems import PROBLEMS, get_problem, get_all_problems
from database.init_db import init_main_db, init_edge_dbs, get_connection

app = Flask(
    __name__,
    template_folder="frontend/templates",
    static_folder="frontend/static",
)


# ══════════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════════

def initialize():
    """Initialize all databases on startup — always run on Render/production."""
    db_path = os.path.join(os.path.dirname(__file__), 'database', 'main.db')
    try:
        # On Render the filesystem resets each deploy, so always reinitialize
        print("[INIT] Initializing databases…")
        init_main_db()
        init_edge_dbs()
        print("[INIT] Done.")
    except Exception as e:
        print(f"[INIT] Warning: {e}")

# Initialize on import (runs when gunicorn loads the module)
initialize()


# ══════════════════════════════════════════════════════════════════════
#  UTILITY
# ══════════════════════════════════════════════════════════════════════

def success(data):
    return jsonify({"status": "ok", "data": data})

def error(msg, code=400):
    return jsonify({"status": "error", "message": msg}), code


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — STATIC / FRONTEND
# ══════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — PROBLEMS
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/problems", methods=["GET"])
def list_problems():
    return success(get_all_problems())


@app.route("/api/problems/<problem_id>", methods=["GET"])
def get_problem_detail(problem_id):
    p = get_problem(problem_id)
    if not p:
        return error(f"Problem '{problem_id}' not found", 404)
    return success(p)


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — SCHEMA & DATA
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/schema", methods=["GET"])
def get_schema():
    """Return schema info + sample data for all tables."""
    conn = get_connection()
    c = conn.cursor()

    tables = {}
    c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    table_names = [row[0] for row in c.fetchall()]

    for tbl in table_names:
        # Columns
        c.execute(f"PRAGMA table_info([{tbl}])")
        cols = [{"name": r[1], "type": r[2], "not_null": bool(r[3]),
                 "pk": bool(r[5])} for r in c.fetchall()]
        # Sample rows
        c.execute(f"SELECT * FROM [{tbl}] LIMIT 20")
        col_names = [d[0] for d in c.description]
        rows = [dict(zip(col_names, row)) for row in c.fetchall()]
        tables[tbl] = {"columns": cols, "rows": rows, "col_names": col_names}

    conn.close()
    return success(tables)


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — EXECUTE SINGLE QUERY
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/execute", methods=["POST"])
def execute():
    """Execute a single SQL query and return results."""
    body = request.get_json()
    if not body or "query" not in body:
        return error("Request body must contain 'query'")

    sql = body["query"].strip()
    result = execute_query(sql)
    return success(result.to_dict())


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — FULL ANALYSIS PIPELINE
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/analyze", methods=["POST"])
def analyze():
    """
    Full analysis pipeline:
      1. Parse both queries (AST)
      2. Compare structures (AST diff)
      3. Execute on main DB
      4. Compare results
      5. Provenance trace
      6. Edge case analysis
      7. Counterexample generation
      8. Feedback generation
    """
    body = request.get_json()
    if not body:
        return error("JSON body required")

    student_sql  = body.get("student_query", "").strip()
    problem_id   = body.get("problem_id", "div_db_courses")

    if not student_sql:
        return error("'student_query' is required")

    # Get problem
    problem = get_problem(problem_id)
    if not problem:
        return error(f"Unknown problem: {problem_id}", 404)

    base_sql    = problem["base_query"]
    problem_type = problem["type"]

    # ── Step 1 & 2: Parse + AST diff ──
    base_parse    = parse_sql(base_sql)
    student_parse = parse_sql(student_sql)
    diffs         = compare_queries(base_parse, student_parse)

    # ── Step 3: Execute ──
    base_result    = execute_query(base_sql)
    student_result = execute_query(student_sql)

    # ── Step 4: Compare results ──
    comparison = compare_results(base_result, student_result)

    # ── Step 5: Provenance trace ──
    try:
        prov_trace = compute_provenance(base_sql, student_sql,
                                         query_type=problem_type).to_dict()
    except Exception as e:
        prov_trace = {"error": str(e), "steps": [], "divergence_points": []}

    # ── Step 6: Edge cases ──
    try:
        edge_results = full_edge_case_analysis(base_sql, student_sql)
    except Exception as e:
        edge_results = []

    # ── Step 7: Counterexample ──
    try:
        counterexample = find_minimal_counterexample(base_sql, student_sql)
    except Exception:
        counterexample = None

    # ── Step 8: Feedback ──
    # Pass execution error so syntax grading catches it even if parser missed it
    exec_error = student_result.error if not student_result.success else None
    report = generate_feedback(
        base_parse=base_parse,
        student_parse=student_parse,
        comparison=comparison.to_dict(),
        edge_results=edge_results,
        provenance_trace=prov_trace,
        problem_type=problem_type,
        execution_error=exec_error,
    )

    return success({
        "problem": {
            "id": problem["id"],
            "title": problem["title"],
            "type": problem_type,
        },
        "parsing": {
            "base":    base_parse.to_dict(),
            "student": student_parse.to_dict(),
            "diffs":   [{"path": d.path, "base": d.base_value,
                          "student": d.student_value, "type": d.diff_type}
                         for d in diffs],
        },
        "execution": {
            "base":    base_result.to_dict(),
            "student": student_result.to_dict(),
        },
        "comparison": comparison.to_dict(),
        "provenance": prov_trace,
        "edge_cases": edge_results,
        "counterexample": counterexample,
        "feedback": report.to_dict(),
    })


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — EDGE CASES STANDALONE
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/edge-cases/<problem_id>", methods=["POST"])
def edge_cases(problem_id):
    """Run edge case analysis for a given problem and student query."""
    body = request.get_json()
    student_sql = body.get("student_query", "").strip()
    if not student_sql:
        return error("'student_query' required")

    problem = get_problem(problem_id)
    if not problem:
        return error("Problem not found", 404)

    results = full_edge_case_analysis(problem["base_query"], student_sql)
    return success(results)


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/health", methods=["GET"])
def health():
    return success({"status": "healthy", "version": "1.0.0"})


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  SQL Feedback System — Research Prototype v1.0")
    print("  Author: Mohammad Arifur Rahman")
    print("  Advisor: Dr. Hasan Jamil, University of Idaho")
    print("="*55)
    print(f"  → Open: http://localhost:5000")
    print("="*55 + "\n")
    app.run(debug=False, port=5000, host="0.0.0.0")

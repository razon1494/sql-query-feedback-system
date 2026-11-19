"""
Flask API Server — SQL Query Feedback System
Endpoints:
  GET  /api/session              → current role (student or instructor)
  POST /api/instructor/login     → sign in with the instructor PIN
  POST /api/instructor/logout    → back to the student view
  GET  /api/problems             → list all problems
  GET  /api/problems/<id>        → problem details (+ the samples this viewer may load)
  GET  /api/schema[?problem_id=] → schema + sample data of a problem's database
  POST /api/analyze              → full analysis pipeline
  POST /api/execute              → run a single query
  POST /api/edge-cases/<id>      → edge case results for a problem

Students never receive the reference query, the reference's result rows,
withheld proposals or the score (paper Sec. 3, "Two workflows"). Instructors,
signed in with the PIN from the INSTRUCTOR_PIN environment variable, receive
the full analysis.
"""
import os
import sys
import secrets
from datetime import timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, request, jsonify, render_template, session
from werkzeug.middleware.proxy_fix import ProxyFix

from backend.sql_parser import parse_sql, compare_queries
from backend.query_executor import (
    execute_query, full_edge_case_analysis, compare_results,
    find_minimal_counterexample,
)
from backend.provenance import compute_provenance
from backend.feedback_generator import generate_feedback
from backend.problems import get_problem, get_all_problems
from backend.samples import samples_for
from backend.student_view import (
    analysis_status, student_problem_view, student_analysis_view, student_edge_view,
)
from backend.instructor_auth import LoginThrottle, check_pin, pin_configured
from backend.release_card import observed_evidence, describe_released, describe_withheld
from database.init_db import init_main_db, init_edge_dbs, get_connection
from database.demo_dbs import init_demo_dbs, demo_db_path

INSTRUCTOR_ROLE = "instructor"
STUDENT_ROLE = "student"
SESSION_HOURS = 8
MAX_QUERY_LENGTH = 10_000
SCHEMA_SAMPLE_ROWS = 20


def _secret_key() -> str:
    key = os.environ.get("SECRET_KEY", "").strip()
    if key:
        return key
    print("[INIT] SECRET_KEY is not set: using a random key, so instructor "
          "sign-ins end whenever the server restarts.")
    return secrets.token_hex(32)


app = Flask(
    __name__,
    template_folder="frontend/templates",
    static_folder="frontend/static",
)
# Render terminates TLS in one proxy hop; trust exactly that hop for the
# client address (used to throttle PIN attempts) and the scheme.
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
app.config.update(
    SECRET_KEY=_secret_key(),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=bool(os.environ.get("RENDER")),
    PERMANENT_SESSION_LIFETIME=timedelta(hours=SESSION_HOURS),
)

_login_throttle = LoginThrottle()


# ══════════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════════

def initialize():
    """Initialize all databases on startup — always run on Render/production."""
    try:
        # On Render the filesystem resets each deploy, so always reinitialize
        print("[INIT] Initializing databases…")
        init_main_db()
        init_edge_dbs()
        init_demo_dbs()
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

def is_instructor() -> bool:
    return pin_configured() and session.get("role") == INSTRUCTOR_ROLE

def current_role() -> str:
    return INSTRUCTOR_ROLE if is_instructor() else STUDENT_ROLE

def problem_db_path(problem: dict):
    """The problem's own database, or None for the shared main database."""
    name = problem.get("database")
    return demo_db_path(name) if name else None

def read_student_query(body):
    """The submitted query, or an error message when it is unusable."""
    sql = body.get("student_query", "") if isinstance(body, dict) else ""
    if not isinstance(sql, str) or not sql.strip():
        return None, "'student_query' is required"
    if len(sql) > MAX_QUERY_LENGTH:
        return None, f"The query is longer than {MAX_QUERY_LENGTH} characters"
    return sql.strip(), None


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — STATIC / FRONTEND
# ══════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — INSTRUCTOR SIGN-IN
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/session", methods=["GET"])
def get_session():
    return success({"role": current_role(), "instructor_available": pin_configured()})


@app.route("/api/instructor/login", methods=["POST"])
def instructor_login():
    if not pin_configured():
        return error("Instructor mode is not set up on this server.", 503)
    client = request.remote_addr or "unknown"
    if _login_throttle.is_blocked(client):
        return error("Too many attempts. Wait a minute and try again.", 429)
    body = request.get_json(silent=True) or {}
    if not check_pin(body.get("pin")):
        _login_throttle.record_failure(client)
        return error("That PIN is not correct.", 401)
    _login_throttle.reset(client)
    session.clear()
    session["role"] = INSTRUCTOR_ROLE
    session.permanent = True
    return success({"role": INSTRUCTOR_ROLE})


@app.route("/api/instructor/logout", methods=["POST"])
def instructor_logout():
    session.clear()
    return success({"role": STUDENT_ROLE})


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
    instructor = is_instructor()
    view = dict(p) if instructor else student_problem_view(p)
    return success({**view, "samples": samples_for(problem_id, instructor)})


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — SCHEMA & DATA
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/schema", methods=["GET"])
def get_schema():
    """Return schema info + sample data for all tables of a problem's database."""
    db_path = None
    problem_id = request.args.get("problem_id")
    if problem_id:
        problem = get_problem(problem_id)
        if not problem:
            return error(f"Unknown problem: {problem_id}", 404)
        db_path = problem_db_path(problem)

    conn = get_connection(db_path)
    try:
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        table_names = [row[0] for row in c.fetchall()]

        tables = {}
        for tbl in table_names:
            c.execute(f"PRAGMA table_info([{tbl}])")
            cols = [{"name": r[1], "type": r[2], "not_null": bool(r[3]),
                     "pk": bool(r[5])} for r in c.fetchall()]
            c.execute(f"SELECT * FROM [{tbl}] LIMIT {SCHEMA_SAMPLE_ROWS}")
            col_names = [d[0] for d in c.description]
            rows = [dict(zip(col_names, row)) for row in c.fetchall()]
            tables[tbl] = {"columns": cols, "rows": rows, "col_names": col_names}
    finally:
        conn.close()
    return success(tables)


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — EXECUTE SINGLE QUERY
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/execute", methods=["POST"])
def execute():
    """Execute a single SQL query and return results."""
    body = request.get_json(silent=True)
    if not body or "query" not in body:
        return error("Request body must contain 'query'")

    sql = body["query"].strip()
    result = execute_query(sql)
    return success(result.to_dict())


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — FULL ANALYSIS PIPELINE
# ══════════════════════════════════════════════════════════════════════

def with_release_cards(report: dict, comparison: dict, provenance: dict,
                       edge_results: list, exec_error) -> dict:
    """
    Give each released diagnosis its label, evidence and repair direction
    (talk slide 6), and each withheld proposal its rule ID. The feedback items
    of the released diagnoses carry the same rule ID and evidence.
    """
    observed = observed_evidence(comparison, provenance, edge_results, exec_error)
    snippets = {i["title"]: i.get("code_snippet")
                for i in report["items"] if i.get("category") == "LOGIC"}
    released = describe_released(report["misconceptions"], observed, snippets)
    by_title = {m["title"]: m for m in released}

    def item_view(item):
        card = by_title.get(item["title"]) if item.get("category") == "LOGIC" else None
        if not card:
            return item
        label = f"{card['rule_id']} · {item['title']}" if card["rule_id"] else item["title"]
        body = f"{card['evidence']} {item['body']}" if card["evidence"] else item["body"]
        return {**item, "title": label, "body": body}

    return {
        **report,
        "misconceptions": released,
        "unsupported_misconceptions": describe_withheld(report["unsupported_misconceptions"]),
        "items": [item_view(i) for i in report["items"]],
    }


def run_analysis(problem: dict, student_sql: str) -> dict:
    """
    Full analysis pipeline (the instructor's view of it):
      1. Parse both queries (AST)      5. Provenance trace
      2. Compare structures (AST diff) 6. Edge case analysis
      3. Execute on the problem's DB   7. Counterexample generation
      4. Compare results               8. Feedback generation
    """
    base_sql = problem["base_query"]
    problem_type = problem["type"]
    db_path = problem_db_path(problem)

    base_parse    = parse_sql(base_sql)
    student_parse = parse_sql(student_sql)
    diffs         = compare_queries(base_parse, student_parse)

    base_result    = execute_query(base_sql, db_path=db_path)
    student_result = execute_query(student_sql, db_path=db_path)
    comparison = compare_results(base_result, student_result)

    try:
        prov_trace = compute_provenance(base_sql, student_sql, query_type=problem_type,
                                        db_path=db_path).to_dict()
    except Exception as e:
        prov_trace = {"error": str(e), "steps": [], "divergence_points": []}

    try:
        edge_results = full_edge_case_analysis(base_sql, student_sql)
    except Exception:
        edge_results = []

    try:
        counterexample = find_minimal_counterexample(base_sql, student_sql, db_path=db_path)
    except Exception:
        counterexample = None

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
    ).to_dict()
    report = with_release_cards(report, comparison.to_dict(), prov_trace, edge_results, exec_error)

    status = analysis_status(
        execution_ok=student_result.success and not student_parse.error,
        outputs_match=comparison.are_equivalent,
        failed_edges=sum(1 for e in edge_results if not e.get("passed")),
        released=len(report["misconceptions"]),
    )

    return {
        "role": INSTRUCTOR_ROLE,
        "problem": {"id": problem["id"], "title": problem["title"], "type": problem_type},
        "status": status,
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
        "feedback": report,
    }


@app.route("/api/analyze", methods=["POST"])
def analyze():
    body = request.get_json(silent=True)
    if not body:
        return error("JSON body required")

    student_sql, query_error = read_student_query(body)
    if query_error:
        return error(query_error)

    problem_id = body.get("problem_id", "div_db_courses")
    problem = get_problem(problem_id)
    if not problem:
        return error(f"Unknown problem: {problem_id}", 404)

    result = run_analysis(problem, student_sql)
    return success(result if is_instructor() else student_analysis_view(result))


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — EDGE CASES STANDALONE
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/edge-cases/<problem_id>", methods=["POST"])
def edge_cases(problem_id):
    """Run edge case analysis for a given problem and student query."""
    student_sql, query_error = read_student_query(request.get_json(silent=True))
    if query_error:
        return error(query_error)

    problem = get_problem(problem_id)
    if not problem:
        return error("Problem not found", 404)

    results = full_edge_case_analysis(problem["base_query"], student_sql)
    return success(results if is_instructor() else student_edge_view(results))


# ══════════════════════════════════════════════════════════════════════
#  ROUTES — HEALTH CHECK
# ══════════════════════════════════════════════════════════════════════

@app.route("/api/health", methods=["GET"])
def health():
    return success({"status": "healthy", "version": "1.1.0"})


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  EviSQL — Research Prototype v1.1")
    print("  Author: Mohammad Arifur Rahman")
    print("  Advisor: Dr. Hasan Jamil, University of Idaho")
    print("="*55)
    print(f"  → Open: http://localhost:5000")
    print("="*55 + "\n")
    app.run(debug=False, port=5000, host="0.0.0.0")

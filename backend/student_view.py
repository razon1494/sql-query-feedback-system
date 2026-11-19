"""
What a student may receive from the API.

The paper (Sec. 3, "Two workflows") and the talk promise that the student
reads a correctness status and, for each released diagnosis, its label,
evidence and repair direction, never the reference query. Withheld proposals,
the structural diff (unreleased proposals by another name), the reference's
result rows and the unvalidated score stay on the instructor side.

Every function returns a new object; inputs are never modified.
"""
import re

STUDENT_PROBLEM_FIELDS = ("id", "title", "description", "question", "type",
                          "difficulty", "concepts", "hint")

_SCORE_SENTENCE = re.compile(r"\s*Score: \d+/100\.?")
_PROVENANCE_REFERENCE_KEYS = ("base_tuples", "base_uses")


# ── Status ────────────────────────────────────────────────────────────

def analysis_status(execution_ok: bool, outputs_match: bool,
                    failed_edges: int, released: int) -> dict:
    """The correctness status shown to both views."""
    if not execution_ok:
        return {"code": "not_run", "label": "Did not run",
                "detail": "The database rejected this query, so there is no result to assess."}
    if not outputs_match:
        return {"code": "incorrect", "label": "Not correct yet",
                "detail": "Your result differs from the expected one. See the feedback for why."}
    if released:
        # The talk's slide 3: the course data hides the error, a test database exposes it.
        return {"code": "incorrect", "label": "Not correct yet",
                "detail": ("Your result matches on the course database, but a constructed test "
                           "database exposes an error. See the feedback for why.")}
    if failed_edges:
        return {"code": "matches_course_db", "label": "Correct on the course database",
                "detail": (f"On {failed_edges} constructed test database(s) your result differs "
                           "from the expected one. See Edge Cases.")}
    return {"code": "correct", "label": "Correct",
            "detail": "Your result matches on the course database and on every test database."}


# ── Problems ──────────────────────────────────────────────────────────

def student_problem_view(problem: dict) -> dict:
    return {k: problem[k] for k in STUDENT_PROBLEM_FIELDS if k in problem}


# ── Analysis pieces ───────────────────────────────────────────────────

def _student_comparison(comp: dict) -> dict:
    return {
        "are_equivalent": comp.get("are_equivalent", False),
        "extra_rows": list(comp.get("extra_rows", [])),
        "missing_count": len(comp.get("missing_rows", [])),
        "student_count": comp.get("student_count", 0),
    }


def _student_provenance(prov: dict) -> dict:
    if not prov or prov.get("error"):
        return dict(prov or {})
    # A step that executes the reference lists the reference's rows: drop it,
    # strip reference-side fields from the rest, and renumber what remains.
    kept = [step for step in prov.get("steps", [])
            if "base query" not in str(step.get("title", "")).lower()]
    steps = [{**{k: v for k, v in step.items() if k not in _PROVENANCE_REFERENCE_KEYS}, "step": i}
             for i, step in enumerate(kept, start=1)]
    return {
        "query_type": prov.get("query_type"),
        "steps": steps,
        "divergence_points": list(prov.get("divergence_points", [])),
        "explanation": prov.get("explanation", ""),
    }


def student_edge_view(edges: list) -> list:
    """Edge-case results without the reference's rows (its row count stays)."""
    return [{
        "name": e.get("name"),
        "title": e.get("title"),
        "description": e.get("description"),
        "tests": e.get("tests"),
        "passed": e.get("passed"),
        "base_result": {"row_count": e.get("base_result", {}).get("row_count", 0)},
        "student_result": e.get("student_result", {}),
    } for e in (edges or [])]


def _student_item(item: dict) -> dict:
    title = item.get("title", "")
    if item.get("category") == "RESULT" and "Missing Row" in title:
        count = title.split(" ", 1)[0]
        return {**item, "body": f"{count} row(s) that belong in the result are missing from yours."}
    if item.get("category") == "RESULT" and title == "Output Matches Reference":
        return {**item, "title": "Output Matches the Expected Result"}
    if item.get("category") == "EDGE_CASE":
        body = item.get("body", "").replace("Base query returned", "The expected result has")
        return {**item, "body": body}
    return dict(item)


def _student_misconception(m: dict) -> dict:
    return {k: v for k, v in m.items() if k != "penalty"}


def _student_feedback(fb: dict) -> dict:
    return {
        "is_alternate_correct": fb.get("is_alternate_correct", False),
        "items": [_student_item(i) for i in fb.get("items", [])],
        "misconceptions": [_student_misconception(m) for m in fb.get("misconceptions", [])],
        "summary": _SCORE_SENTENCE.sub("", fb.get("summary", "")).strip(),
    }


def student_analysis_view(result: dict) -> dict:
    """The full /api/analyze payload reduced to what a student may see."""
    return {
        "role": "student",
        "problem": dict(result["problem"]),
        "status": dict(result["status"]),
        "execution": {"student": result["execution"]["student"]},
        "comparison": _student_comparison(result["comparison"]),
        "provenance": _student_provenance(result.get("provenance")),
        "edge_cases": student_edge_view(result.get("edge_cases")),
        "edge_cases_apply": bool(result.get("edge_cases_apply")),
        "feedback": _student_feedback(result["feedback"]),
    }

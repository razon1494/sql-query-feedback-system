"""/api/analyze exposes the Stage 1 bundle and the LLM gate.

Exercised through Flask's test client so no server is started.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from app import app as flask_app
from backend.problems import get_problem


# The counting formulation. Agrees with the NOT EXISTS reference on the main
# instance but diverges on empty_courses, where the reference is vacuously true.
SUBMISSION_B = """SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'DB'
GROUP BY s.StuID, s.Name
HAVING COUNT(DISTINCT c.CourseID) = (SELECT COUNT(*) FROM Courses WHERE "Group" = 'DB')"""


@pytest.fixture
def client():
    flask_app.config["TESTING"] = True
    with flask_app.test_client() as c:
        yield c


def analyze(client, sql):
    resp = client.post("/api/analyze",
                       json={"student_query": sql, "problem_id": "div_db_courses"})
    assert resp.status_code == 200
    body = resp.get_json()
    assert body.get("status") == "ok", body
    return body["data"]


def test_reference_query_needs_no_llm(client):
    data = analyze(client, get_problem("div_db_courses")["base_query"])
    assert data["llm_gate"] == {"invoke": False, "reason": None}


def test_edge_divergence_opens_the_gate(client):
    data = analyze(client, SUBMISSION_B)
    assert data["comparison"]["are_equivalent"] is True, "should agree on main"
    assert data["llm_gate"]["invoke"] is True
    assert data["llm_gate"]["reason"] == "residual"


def test_the_counterexample_is_in_the_bundle(client):
    data = analyze(client, SUBMISSION_B)
    assert "cex.empty_courses" in data["evidence"]["channels"]["E_cex"]


def test_every_channel_is_present_and_the_packet_is_built(client):
    data = analyze(client, SUBMISSION_B)
    channels = data["evidence"]["channels"]
    assert set(channels) == {"E_str", "E_exe", "E_prov", "E_cex", "E_con", "E_hist"}
    assert data["evidence"]["packet"] is not None
    assert channels["E_con"], "schema facts should be collected"


def test_schema_facts_are_cached_across_requests(client):
    import app as app_module
    app_module._SCHEMA_ITEMS = None
    first = analyze(client, SUBMISSION_B)["evidence"]["channels"]["E_con"]
    cached = app_module._SCHEMA_ITEMS
    assert cached is not None
    second = analyze(client, SUBMISSION_B)["evidence"]["channels"]["E_con"]
    assert first == second
    assert app_module._SCHEMA_ITEMS is cached


def test_a_syntax_error_still_returns_a_bundle(client):
    data = analyze(client, "SELECT FROM WHERE")
    assert "error" not in data["evidence"]
    assert data["evidence"]["packet"] is not None

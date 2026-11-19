"""
The website behaves as the ICWL 2026 paper and talk describe it:
students never receive the reference query, withheld proposals or the score;
an instructor signed in with the PIN does; and the running example of the
paper (Fig. 1, slides 2, 3, 6, 7) reproduces on the site.
"""
import json

import pytest

import app as web
from backend.instructor_auth import LoginThrottle
from backend.problems import PROBLEMS, get_problem
from backend.samples import SAMPLE_ANSWERS
from backend.student_view import analysis_status, student_analysis_view

PIN = "2468"
LEAK_MARKERS = ("base_tuples", "base_rows", "unsupported_misconceptions",
                "raw_misconceptions", "total_score", "missing_rows", '"parsing"', "Score:")


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv("INSTRUCTOR_PIN", PIN)
    monkeypatch.setattr(web, "_login_throttle", LoginThrottle())
    return web.app.test_client()


def analyze(client, problem_id, sql):
    res = client.post("/api/analyze", json={"problem_id": problem_id, "student_query": sql})
    assert res.status_code == 200, res.get_json()
    return res.get_json()["data"]


def sign_in(client, pin=PIN):
    return client.post("/api/instructor/login", json={"pin": pin})


# ── Students never receive reference-side content ─────────────────────

def test_student_problem_detail_has_no_reference_or_correct_samples(client):
    for pid in ("ex_every_db_course", "div_db_courses", "join_cs360"):
        data = client.get(f"/api/problems/{pid}").get_json()["data"]
        assert "base_query" not in data
        assert "common_mistakes" not in data
        assert set(data["samples"]) <= {"wrong", "partial"}


@pytest.mark.parametrize("pid", ["ex_every_db_course", "ex_hidden_error", "div_db_courses",
                                 "join_cs360", "agg_multi_course", "set_no_db"])
def test_student_analysis_contains_no_reference_text(client, pid):
    reference = get_problem(pid)["base_query"]
    for kind, sql in SAMPLE_ANSWERS[pid].items():
        if kind not in ("wrong", "partial"):
            continue
        blob = json.dumps(analyze(client, pid, sql))
        assert reference not in blob
        assert json.dumps(reference)[1:-1] not in blob
        for marker in LEAK_MARKERS:
            assert marker not in blob, (pid, kind, marker)


def test_student_provenance_drops_the_reference_execution_step(client):
    data = analyze(client, "agg_multi_course", SAMPLE_ANSWERS["agg_multi_course"]["partial"])
    titles = [s["title"] for s in data["provenance"]["steps"]]
    assert titles and not any("Base Query" in t for t in titles)
    assert [s["step"] for s in data["provenance"]["steps"]] == list(range(1, len(titles) + 1))


def test_student_edge_cases_keep_counts_but_not_reference_rows(client):
    res = client.post("/api/edge-cases/div_db_courses",
                      json={"student_query": SAMPLE_ANSWERS["div_db_courses"]["wrong"]})
    for edge in res.get_json()["data"]:
        assert set(edge["base_result"]) == {"row_count"}


def test_page_source_holds_no_sample_answers(client):
    html = client.get("/").get_data(as_text=True)
    assert "sampleAnswers" not in html
    assert get_problem("div_db_courses")["base_query"] not in html


# ── Instructor sign-in ────────────────────────────────────────────────

def test_wrong_pin_is_refused_and_right_pin_unlocks_full_analysis(client):
    assert sign_in(client, "0000").status_code == 401
    assert client.get("/api/session").get_json()["data"]["role"] == "student"

    assert sign_in(client).status_code == 200
    assert client.get("/api/session").get_json()["data"]["role"] == "instructor"
    detail = client.get("/api/problems/ex_every_db_course").get_json()["data"]
    assert detail["base_query"] == get_problem("ex_every_db_course")["base_query"]
    assert set(detail["samples"]) == {"wrong", "correct", "alternate"}

    data = analyze(client, "ex_every_db_course", SAMPLE_ANSWERS["ex_every_db_course"]["alternate"])
    assert data["role"] == "instructor"
    assert "base" in data["execution"] and "total_score" in data["feedback"]


def test_sign_out_returns_to_the_student_view(client):
    sign_in(client)
    client.post("/api/instructor/logout")
    assert client.get("/api/session").get_json()["data"]["role"] == "student"
    assert "base_query" not in client.get("/api/problems/div_db_courses").get_json()["data"]


def test_instructor_mode_is_off_without_a_configured_pin(client, monkeypatch):
    monkeypatch.delenv("INSTRUCTOR_PIN")
    assert sign_in(client).status_code == 503
    assert client.get("/api/session").get_json()["data"] == {
        "role": "student", "instructor_available": False}


def test_repeated_wrong_pins_are_throttled(client):
    codes = [sign_in(client, "1111").status_code for _ in range(6)]
    assert codes[:5] == [401] * 5 and codes[5] == 429
    assert sign_in(client).status_code == 429   # even the right PIN waits


def test_throttle_window_expires():
    now = [0.0]
    throttle = LoginThrottle(max_failures=2, window=60, clock=lambda: now[0])
    throttle.record_failure("ip")
    throttle.record_failure("ip")
    assert throttle.is_blocked("ip")
    now[0] = 61.0
    assert not throttle.is_blocked("ip")


def test_overlong_query_is_rejected(client):
    res = client.post("/api/analyze", json={"problem_id": "div_db_courses",
                                            "student_query": "SELECT 1 " + "x" * 10_001})
    assert res.status_code == 400


# ── The running example reproduces the talk ───────────────────────────

def test_slide_2_and_6_submission_a_releases_m8_with_nancy_and_omar(client):
    data = analyze(client, "ex_every_db_course", SAMPLE_ANSWERS["ex_every_db_course"]["wrong"])
    assert [r["Name"] for r in data["execution"]["student"]["rows"]] == ["Alice", "Nancy", "Omar"]
    assert [m["key"] for m in data["feedback"]["misconceptions"]] == ["IN_FOR_DIVISION"]
    assert {d["name"] for d in data["provenance"]["divergence_points"]} == {"Nancy", "Omar"}
    assert data["status"]["code"] == "incorrect"


def test_slide_3_hidden_error_is_still_released_through_an_edge_case(client):
    data = analyze(client, "ex_hidden_error", SAMPLE_ANSWERS["ex_hidden_error"]["wrong"])
    assert data["comparison"]["are_equivalent"] is True
    failed = {e["name"] for e in data["edge_cases"] if not e["passed"]}
    assert "partial_match" in failed
    assert [m["key"] for m in data["feedback"]["misconceptions"]] == ["IN_FOR_DIVISION"]
    assert data["status"]["code"] == "incorrect"
    assert "test database exposes" in data["status"]["detail"]


def test_slide_7_counting_solution_shows_no_accusation_to_the_student(client):
    sql = SAMPLE_ANSWERS["ex_every_db_course"]["alternate"]
    student = analyze(client, "ex_every_db_course", sql)
    assert student["feedback"]["misconceptions"] == []
    assert student["status"]["code"] in ("correct", "matches_course_db")

    sign_in(client)
    instructor = analyze(client, "ex_every_db_course", sql)
    assert [m["key"] for m in instructor["feedback"]["unsupported_misconceptions"]] == ["MISSING_NOT_EXISTS"]


def test_running_example_schema_is_the_talks_data(client):
    tables = client.get("/api/schema?problem_id=ex_every_db_course").get_json()["data"]
    assert [r["Name"] for r in tables["Students"]["rows"]] == ["Alice", "Nancy", "Omar"]
    assert [r["CourseID"] for r in tables["Courses"]["rows"]] == ["DB1", "DB2"]


def test_evaluated_problem_set_is_unchanged():
    """The evaluation scripts iterate PROBLEMS; the demo problems stay outside it."""
    ids = [p["id"] for p in PROBLEMS]
    assert not any(pid.startswith("ex_") for pid in ids)
    assert len(ids) == len(set(ids))


# ── Pure helpers ──────────────────────────────────────────────────────

@pytest.mark.parametrize("args, code", [
    ((False, False, 0, 0), "not_run"),
    ((True, False, 0, 0), "incorrect"),
    ((True, True, 2, 1), "incorrect"),
    ((True, True, 1, 0), "matches_course_db"),
    ((True, True, 0, 0), "correct"),
])
def test_analysis_status(args, code):
    assert analysis_status(*args)["code"] == code


def test_student_view_does_not_modify_its_input():
    full = web.run_analysis(get_problem("div_db_courses"), SAMPLE_ANSWERS["div_db_courses"]["wrong"])
    before = json.dumps(full, sort_keys=True)
    student_analysis_view(full)
    assert json.dumps(full, sort_keys=True) == before

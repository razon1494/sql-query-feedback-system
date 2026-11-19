"""
The NULL-injected test database (paper Sec. 5.3, talk slide 11): a NOT IN
error that is hidden on NULL-free data is exposed on a copy of the database
with NULLs in the link table, and the M3 diagnosis is released.
"""
import sqlite3

import pytest

import app as web
from backend.null_instance import NULL_EDGE_NAME, applies, null_instance_path
from backend.problems import get_problem
from backend.samples import SAMPLE_ANSWERS
from database.demo_dbs import demo_db_path


def analysis(pid, kind):
    return web.run_analysis(get_problem(pid), SAMPLE_ANSWERS[pid][kind])


def null_edge(result):
    return next((e for e in result["edge_cases"] if e["name"] == NULL_EDGE_NAME), None)


def test_slide_11_not_in_is_hidden_on_the_data_and_exposed_by_the_null_copy():
    r = analysis("ex_hidden_error", "notin")
    assert [x["Name"] for x in r["execution"]["student"]["rows"]] == ["Alice"]
    assert r["comparison"]["are_equivalent"] is True
    edge = null_edge(r)
    assert edge and not edge["passed"]
    assert edge["student_result"]["row_count"] == 0 and edge["base_result"]["row_count"] == 1
    [card] = r["feedback"]["misconceptions"]
    assert card["key"] == "NOT_IN_vs_NOT_EXISTS" and card["rule_id"] == "M3"
    assert card["evidence"].startswith("On a test database where the enrolment table holds NULL values")
    assert r["status"]["code"] == "incorrect"


@pytest.mark.parametrize("kind", ["wrong", "alternate", "correct"])
def test_no_null_copy_without_not_in(kind):
    assert null_edge(analysis("ex_every_db_course", kind)) is None


def test_no_null_copy_when_the_reference_itself_uses_not_in():
    """The NULLs would distort a NOT IN reference, so the copy is not used."""
    assert "NOT IN" in get_problem("div_db_courses")["base_query"]
    assert null_edge(analysis("div_db_courses", "correct")) is None


@pytest.mark.parametrize("ptype, base, student, expected", [
    ("DIVISION", "... NOT EXISTS ...", "... x NOT IN (...) ...", True),
    ("DIVISION", "... NOT  in (...) ...", "... NOT IN (...) ...", False),
    ("DIVISION", "... NOT EXISTS ...", "... IN (...) ...", False),
    ("SUBQUERY", "... NOT EXISTS ...", "... NOT IN (...) ...", False),
])
def test_when_the_null_copy_applies(ptype, base, student, expected):
    assert applies(ptype, base, student) is expected


def test_null_copy_adds_nulls_to_the_copy_only():
    source = demo_db_path("demo_hidden_error")
    copy = null_instance_path(source)

    def nulls(path):
        with sqlite3.connect(path) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM Takes WHERE StuID IS NULL OR CourseID IS NULL").fetchone()[0]

    assert nulls(source) == 0
    assert nulls(copy) == 1 + 3   # one NULL-student row, one NULL-course row per student


def test_students_see_the_null_copy_without_its_reference_rows(monkeypatch):
    monkeypatch.delenv("INSTRUCTOR_PIN", raising=False)
    data = web.app.test_client().post("/api/analyze", json={
        "problem_id": "ex_hidden_error",
        "student_query": SAMPLE_ANSWERS["ex_hidden_error"]["notin"]}).get_json()["data"]
    edge = next(e for e in data["edge_cases"] if e["name"] == NULL_EDGE_NAME)
    assert set(edge["base_result"]) == {"row_count"}

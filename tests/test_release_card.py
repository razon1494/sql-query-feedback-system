"""
A released diagnosis reads as slide 6 of the talk shows it: a label with its
rule ID, the evidence that released it, and a repair direction.
"""
import json

import pytest

import app as web
from backend.feedback_generator import MISCONCEPTION_PATTERNS
from backend.problems import get_problem
from backend.release_card import (
    RULE_IDS, describe_released, evidence_sentence, observed_evidence, repair_direction,
)
from backend.samples import SAMPLE_ANSWERS


def released(pid, kind):
    report = web.run_analysis(get_problem(pid), SAMPLE_ANSWERS[pid][kind])["feedback"]
    return report["misconceptions"]


def test_running_example_names_nancy_and_omar_and_their_missing_course():
    [card] = released("ex_every_db_course", "wrong")
    assert card["rule_id"] == "M8"
    assert card["evidence"] == ("Nancy appears in your result without having taken DB2. "
                                "Omar appears in your result without having taken DB1.")
    assert card["repair"] == "Use the double-negation NOT EXISTS pattern."
    assert card["pattern"] and card["code_snippet"]


def test_hidden_error_cites_the_partial_enrolment_test_database():
    [card] = released("ex_hidden_error", "wrong")
    assert card["evidence"].startswith(
        "On a test database where a student took only some of the database courses, "
        "your query still returns that student")


@pytest.mark.parametrize("pid, kind", [(p, k) for p, s in SAMPLE_ANSWERS.items()
                                       for k in ("wrong", "partial") if k in s])
def test_every_released_card_has_evidence_and_a_repair_direction(pid, kind):
    for card in released(pid, kind):
        assert card["evidence"] and "None" not in card["evidence"], (pid, kind, card["key"])
        assert card["repair"].endswith("."), (pid, kind, card["key"])


def test_join_evidence_quotes_an_extra_row_not_a_course_gap():
    cards = released("join_instructor_courses", "wrong")
    assert all(c["evidence"].startswith("Your result has 10 row(s) the task excludes")
               for c in cards)


def test_rule_ids_follow_paper_table_1():
    assert sorted(set(RULE_IDS.values()), key=lambda m: int(m[1:])) == [f"M{i}" for i in range(1, 11)]
    assert set(RULE_IDS) <= set(MISCONCEPTION_PATTERNS) | {"IN_vs_EXISTS", "NOT_IN_vs_NOT_EXISTS",
                                                           "HAVING_vs_WHERE"}


@pytest.mark.parametrize("fix, direction, rest", [
    ("Add the WHERE clause.", "Add the WHERE clause.", ""),
    ("Use the X pattern:\nWHERE NOT EXISTS (...)", "Use the X pattern.", "WHERE NOT EXISTS (...)"),
    ("Replace the literal, e.g. HAVING COUNT(*) = 2.", "Replace the literal.", "For example, HAVING COUNT(*) = 2."),
    ("Add a HAVING clause (for example, HAVING COUNT(*) >= 2).", "Add a HAVING clause.",
     "For example, HAVING COUNT(*) >= 2."),
])
def test_repair_direction_splits_the_fix(fix, direction, rest):
    assert repair_direction(fix) == (direction, rest)


def test_missing_rows_are_counted_never_listed():
    obs = observed_evidence({"are_equivalent": False, "missing_rows": [{"StuID": "S4", "Name": "Diana"}],
                             "student_count": 3, "base_count": 2}, None, [], None)
    sentence = evidence_sentence("NULL_EQUALITY", obs)
    assert sentence == "Your result leaves out 1 row(s) that belong in it."
    assert "Diana" not in json.dumps(obs["missing_count"])


def test_engine_rejection_is_the_evidence_for_m7():
    obs = observed_evidence({"are_equivalent": False}, None, [], "misuse of aggregate: COUNT()")
    assert evidence_sentence("HAVING_vs_WHERE", obs) == (
        "The database rejected your query: misuse of aggregate: COUNT()")


def test_describe_released_does_not_modify_its_input():
    cards = [{"key": "MISSING_WHERE", "title": "Missing WHERE", "fix": "Add it. Then rerun."}]
    before = json.dumps(cards)
    describe_released(cards, observed_evidence({}, None, [], None), {})
    assert json.dumps(cards) == before


def test_students_receive_the_card_fields(monkeypatch):
    monkeypatch.delenv("INSTRUCTOR_PIN", raising=False)
    client = web.app.test_client()
    data = client.post("/api/analyze", json={
        "problem_id": "ex_every_db_course",
        "student_query": SAMPLE_ANSWERS["ex_every_db_course"]["wrong"]}).get_json()["data"]
    [card] = data["feedback"]["misconceptions"]
    assert card["rule_id"] == "M8" and "Nancy" in card["evidence"]
    logic = [i for i in data["feedback"]["items"] if i["category"] == "LOGIC"]
    assert logic[0]["title"].startswith("M8 · ") and "Nancy" in logic[0]["body"]

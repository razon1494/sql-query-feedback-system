"""A query that never ran cannot support a structural diagnosis.

Draft, Sec. 7: "Parse failures are facts; the LLM may explain them but cannot
override them." Shape detection nonetheless fires on an unparseable query, and
the result-aware filter cannot suppress it, because a query that never executed
trivially disagrees with the reference on every instance. The effect is a
released misconception justified by nothing.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from backend.sql_parser import parse_sql
from backend.query_executor import execute_query, compare_results, full_edge_case_analysis
from backend.feedback_generator import generate_feedback
from backend.evidence import llm_invocation_reason
from backend.problems import get_problem


BROKEN = "SELECT FROM WHERE"
UNKNOWN_TABLE = "SELECT * FROM NoSuchTable"


def report_for(student_sql):
    problem = get_problem("div_db_courses")
    base_sql = problem["base_query"]
    base_parse, student_parse = parse_sql(base_sql), parse_sql(student_sql)
    base_result, student_result = execute_query(base_sql), execute_query(student_sql)
    exec_error = student_result.error if not student_result.success else None
    return generate_feedback(
        base_parse=base_parse,
        student_parse=student_parse,
        comparison=compare_results(base_result, student_result).to_dict(),
        edge_results=full_edge_case_analysis(base_sql, student_sql),
        provenance_trace={"steps": [], "divergence_points": []},
        problem_type=problem["type"],
        execution_error=exec_error,
    ).to_dict()


@pytest.mark.parametrize("sql", [BROKEN, UNKNOWN_TABLE])
def test_a_query_that_never_ran_releases_no_misconception(sql):
    assert report_for(sql)["misconceptions"] == []


@pytest.mark.parametrize("sql", [BROKEN, UNKNOWN_TABLE])
def test_the_shape_proposals_are_retained_instructor_side(sql):
    """Withheld, not discarded -- the research eval still needs to see them."""
    report = report_for(sql)
    assert report["raw_misconceptions"], "shape detection still runs"
    withheld = {m["key"] for m in report["unsupported_misconceptions"]}
    assert {m["key"] for m in report["raw_misconceptions"]} == withheld


def test_a_working_query_is_unaffected():
    """The counting form still withholds MISSING_NOT_EXISTS and releases nothing."""
    report = report_for(get_problem("div_db_courses")["base_query"])
    assert report["misconceptions"] == []
    assert report["raw_misconceptions"] == []


def test_the_gate_calls_a_failed_query_narration_not_ambiguous():
    """The fault is already established; the LLM may explain it, not diagnose it."""
    reason = llm_invocation_reason({"are_equivalent": False}, [], [{"key": "MISSING_JOIN"}],
                                   [{"name": "e", "passed": False}], query_failed=True)
    assert reason == "narration"


def test_query_failed_defaults_to_false():
    assert llm_invocation_reason({"are_equivalent": True}, [], []) is None

"""Stage 1 gate: when may the LLM be invoked at all?

The draft fixes the answer in two places:

  * Sec. 2  -- the deterministic filter suppresses a flag "when the submission
    agrees with the reference on the main database *and a set of edge
    instances*".
  * Sec. 8  -- for a correct counting formulation, "execution and edge cases
    support the counting formulation, so the deterministic filter recognizes an
    alternate solution and suppresses the flag---the LLM is never invoked."

So agreement on the main instance alone is never enough to stay silent.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.evidence import llm_invocation_reason


EQUIVALENT = {"are_equivalent": True}
NOT_EQUIVALENT = {"are_equivalent": False}

PASSING_EDGE = {"name": "empty_courses", "passed": True}
FAILING_EDGE = {"name": "empty_courses", "passed": False,
                "description": "no course rows at all"}

ONE_LABEL = [{"code": "M4"}]
TWO_LABELS = [{"code": "M4"}, {"code": "M7"}]


def test_agrees_everywhere_and_unremarked_stays_silent():
    assert llm_invocation_reason(EQUIVALENT, [], [], [PASSING_EDGE]) is None


def test_agrees_on_main_but_fails_an_edge_is_not_silent():
    """The Submission B case. Main agrees, empty_courses diverges."""
    assert llm_invocation_reason(EQUIVALENT, [], [], [FAILING_EDGE]) == "residual"


def test_wrong_on_main_and_unlabelled_is_residual():
    assert llm_invocation_reason(NOT_EQUIVALENT, [], [], []) == "residual"


def test_competing_labels_beat_an_edge_divergence():
    assert llm_invocation_reason(NOT_EQUIVALENT, TWO_LABELS, [], [FAILING_EDGE]) == "multi_error"


def test_a_gated_proposal_is_ambiguous():
    assert llm_invocation_reason(NOT_EQUIVALENT, ONE_LABEL, [{"code": "M2"}], []) == "ambiguous"


def test_single_label_with_an_edge_divergence_is_narration():
    """The rule named the fault; the counterexample instance still needs telling."""
    assert llm_invocation_reason(EQUIVALENT, ONE_LABEL, [], [FAILING_EDGE]) == "narration"


def test_single_confident_label_with_no_divergence_stays_silent():
    assert llm_invocation_reason(NOT_EQUIVALENT, ONE_LABEL, [], [PASSING_EDGE]) is None


def test_untested_edges_are_not_read_as_agreement():
    """edge_cases omitted means no edge testing ran, not that it passed."""
    assert llm_invocation_reason(EQUIVALENT, [], []) is None


def test_malformed_edge_entries_are_ignored():
    assert llm_invocation_reason(EQUIVALENT, [], [], [None, "x", {"passed": None}]) is None

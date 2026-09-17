"""The hypothesis language: h = <M, phi, W, R, F, c, a>.

This module decides *well-formedness*, never truth. A hypothesis that parses is
one the verifier can now go and check; it is not a hypothesis that is correct.
Stage 3 verification is 1.4 and the acceptance rule is 1.5.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from backend.evidence import EvidenceBundle, EvidenceItem
from backend.hypothesis import (
    LABELS, UNCERTAIN, DETERMINISTIC_TO_LABEL, CLAIM_TYPES,
    Hypothesis, HypothesisError, parse_hypothesis, is_residual_label,
)


def bundle():
    return EvidenceBundle(
        task={"id": "div_db_courses"},
        E_str=[EvidenceItem("str.bind.17", "binding_graph", {})],
        E_exe=[EvidenceItem("exe.diff.04", "output_comparison", {})],
        E_cex=[EvidenceItem("cex.empty_courses", "constructed_instance", {})],
    )


WELL_FORMED = {
    "labels": ["CORRELATION_ERROR"],
    "claims": [{"type": "binding_deficit", "student_block": "S2",
                "reference_block": "R2", "missing_outer_alias": "s"}],
    "evidence_ids": ["str.bind.17", "exe.diff.04"],
    "repair": {"type": "add_correlation", "target_block": "S2",
               "candidate_predicate": "t.StuID = s.StuID"},
    "confidence": 0.97,
    "recommendation": "accept",
}


def parse(**overrides):
    raw = dict(WELL_FORMED)
    raw.update(overrides)
    return parse_hypothesis(raw, bundle())


# -- the vocabulary refines the deterministic taxonomy -------------------

def test_every_deterministic_key_has_exactly_one_label():
    """Theorem 1 needs D subset-of Y, so no detector may be homeless."""
    import backend.feedback_generator as fg
    for key in fg.MISCONCEPTION_EVIDENCE:
        assert key in DETERMINISTIC_TO_LABEL, f"{key} has no vocabulary element"
        assert DETERMINISTIC_TO_LABEL[key] in LABELS


def test_the_draft_mapping_is_preserved():
    """Section 3.1 fixes these by name; they are not ours to reassign."""
    expected = {
        "MISSING_WHERE":          "MISSING_OR_WEAKENED_PREDICATE",     # M1
        "IN_vs_EXISTS":           "EXISTENTIAL_MEMBERSHIP_CONFUSION",  # M2
        "NOT_IN_vs_NOT_EXISTS":   "EXISTENTIAL_MEMBERSHIP_CONFUSION",  # M3
        "WRONG_JOIN_TYPE":        "JOIN_TYPE_CONFUSION",               # M4
        "MISSING_JOIN":           "MISSING_JOIN_CONDITION",            # M5
        "MISSING_GROUP_BY":       "GROUPING_ERROR",                    # M6
        "HAVING_vs_WHERE":        "HAVING_WHERE_CONFUSION",            # M7
        "IN_FOR_DIVISION":        "QUANTIFICATION_ERROR",              # M8
        "MISSING_CORRELATED_REF": "CORRELATION_ERROR",                 # M9
        "WRONG_SET_OP":           "SET_OPERATION_MISUSE",              # M10
        "NULL_EQUALITY":          "NULL_SAFETY_ERROR",
    }
    for key, label in expected.items():
        assert DETERMINISTIC_TO_LABEL[key] == label, key


def test_a_residual_label_is_namespaced():
    assert is_residual_label("Other-Predicate-Boundary")
    assert not is_residual_label("CORRELATION_ERROR")


def test_an_unnamespaced_unknown_label_is_rejected():
    with pytest.raises(HypothesisError, match="vocabulary"):
        parse(labels=["PREDICATE_BOUNDARY"])


def test_a_residual_label_is_accepted():
    h = parse(labels=["Other-Predicate-Boundary"],
              evidence_ids={"Other-Predicate-Boundary": ["exe.diff.04"]})
    assert h.labels == ("Other-Predicate-Boundary",)


def test_uncertain_may_not_be_combined_with_a_diagnosis():
    with pytest.raises(HypothesisError, match="UNCERTAIN"):
        parse(labels=["CORRELATION_ERROR", UNCERTAIN])


def test_uncertain_needs_no_evidence():
    h = parse_hypothesis({"labels": [UNCERTAIN], "claims": [], "evidence_ids": [],
                          "confidence": 0.2, "recommendation": "abstain"}, bundle())
    assert h.labels == (UNCERTAIN,)


# -- claims are machine-checkable by construction ------------------------

def test_a_well_formed_hypothesis_parses():
    h = parse()
    assert isinstance(h, Hypothesis)
    assert h.labels == ("CORRELATION_ERROR",)
    assert h.claims[0]["type"] == "binding_deficit"
    assert h.confidence == 0.97
    assert h.recommendation == "accept"


def test_an_unregistered_claim_type_is_rejected():
    with pytest.raises(HypothesisError, match="claim type"):
        parse(claims=[{"type": "vibes", "note": "looks wrong"}])


def test_a_claim_missing_a_required_field_is_rejected():
    with pytest.raises(HypothesisError, match="binding_deficit"):
        parse(claims=[{"type": "binding_deficit", "student_block": "S2"}])


def test_free_form_prose_is_not_a_claim():
    """Section 3.1: the LLM may not support a diagnosis only with prose."""
    with pytest.raises(HypothesisError):
        parse(claims=["the subquery is uncorrelated"])


def test_every_registered_claim_type_declares_its_required_fields():
    for name, required in CLAIM_TYPES.items():
        assert isinstance(required, tuple) and required, name


# -- citations must resolve, or they support nothing ---------------------

def test_an_evidence_id_absent_from_the_bundle_is_rejected():
    with pytest.raises(HypothesisError, match=r"str\.bind\.99"):
        parse(evidence_ids=["str.bind.99"])


def test_a_diagnosis_must_cite_something():
    with pytest.raises(HypothesisError, match="evidence"):
        parse(evidence_ids=[])


def test_each_label_carries_its_own_citations():
    """Section 8.2: each label must have independent support."""
    h = parse(labels=["CORRELATION_ERROR", "JOIN_TYPE_CONFUSION"],
              evidence_ids={"CORRELATION_ERROR": ["str.bind.17"],
                            "JOIN_TYPE_CONFUSION": ["exe.diff.04"]})
    assert h.evidence_ids["CORRELATION_ERROR"] == ("str.bind.17",)
    assert h.evidence_ids["JOIN_TYPE_CONFUSION"] == ("exe.diff.04",)


def test_a_flat_citation_list_is_ambiguous_under_multiple_labels():
    with pytest.raises(HypothesisError, match="per label"):
        parse(labels=["CORRELATION_ERROR", "JOIN_TYPE_CONFUSION"])


def test_a_flat_list_is_normalised_for_a_single_label():
    h = parse()
    assert h.evidence_ids == {"CORRELATION_ERROR": ("str.bind.17", "exe.diff.04")}


def test_citing_a_label_that_was_not_diagnosed_is_rejected():
    with pytest.raises(HypothesisError, match="GROUPING_ERROR"):
        parse(evidence_ids={"CORRELATION_ERROR": ["str.bind.17"],
                            "GROUPING_ERROR": ["exe.diff.04"]})


# -- repair, hints, confidence, recommendation ---------------------------

def test_the_repair_is_optional():
    h = parse(repair=None)
    assert h.repair is None


def test_an_unregistered_repair_type_is_rejected():
    with pytest.raises(HypothesisError, match="repair type"):
        parse(repair={"type": "rewrite_everything"})


def test_a_hint_step_must_cite_a_claim_it_rests_on():
    """Section 7: every factual statement must map to a verified claim."""
    h = parse(hint_plan=[{"text": "Your subquery ignores the current student.",
                          "supported_by": [0]}])
    assert h.hint_plan[0]["supported_by"] == (0,)


def test_a_hint_citing_a_claim_that_does_not_exist_is_rejected():
    with pytest.raises(HypothesisError, match="supported_by"):
        parse(hint_plan=[{"text": "x", "supported_by": [7]}])


@pytest.mark.parametrize("bad", [-0.1, 1.1, "high", None])
def test_confidence_outside_the_unit_interval_is_rejected(bad):
    with pytest.raises(HypothesisError, match="confidence"):
        parse(confidence=bad)


def test_recommendation_is_limited_to_accept_or_abstain():
    with pytest.raises(HypothesisError, match="recommendation"):
        parse(recommendation="reject")


# -- the boundary this module must not cross -----------------------------

def test_parsing_says_nothing_about_truth():
    """A claim contradicted by the bundle still parses. Deciding that is 1.4."""
    h = parse(claims=[{"type": "binding_deficit", "student_block": "S99",
                       "reference_block": "R99", "missing_outer_alias": "zzz"}])
    assert h.claims[0]["student_block"] == "S99"


def test_a_hypothesis_is_immutable():
    h = parse()
    with pytest.raises(Exception):
        h.labels = ("GROUPING_ERROR",)

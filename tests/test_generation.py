"""Stage 2: the constraint on the model, and what happens when it is broken.

The test that earns its place above the rest is
`test_the_answer_format_shown_in_the_prompt_actually_parses`. The prompt
originally advertised `{"step": ..., "claims": [...]}` for a hint while the
parser required `{"text": ..., "supported_by": [0]}` with integer positions
into the claims array. Nothing failed loudly: the model would simply have been
told the wrong schema, every answer would have been rejected, and the stage
would have abstained on everything while looking like it was working. Pinning
the example against the real parser closes that gap permanently.

The rest cover the three ways out of `generate` — a hypothesis, a declared
abstention, and exhaustion — plus the properties that keep the model bounded:
it may cite only resolvable evidence, and a rejected answer is retried with the
parser's own complaint rather than quietly repaired.
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.evidence import build_bundle                          # noqa: E402
from backend.generation import (                                   # noqa: E402
    MAX_RESPONSE_CHARS, Attempt, GenerationError, StubModel,
    build_prompt, extract_json, generate,
)
from backend.hypothesis import (                                   # noqa: E402
    CLAIM_TYPES, LABELS, REPAIR_TYPES, HypothesisError, parse_hypothesis,
)

REASON = "wrong but undiagnosed"


@pytest.fixture
def bundle():
    return build_bundle(
        problem={"problem_id": "p",
                 "question": "Which students took all DB courses?",
                 "reference_query": "SELECT s.Name FROM Students s",
                 "problem_type": "DIVISION"},
        student_query="SELECT s.Name FROM Students s",
        parsing={}, execution={}, comparison={}, provenance={}, edge_cases=[],
    )


def _citable(bundle):
    return [item.id for item in bundle.all_items()][0]


def _good(bundle, **overrides):
    raw = {
        "labels": [LABELS[0]],
        "claims": [{"type": "divisor_element_missing",
                    "tuple": "(Alice)", "missing_element": "DB2"}],
        "evidence_ids": {LABELS[0]: [_citable(bundle)]},
        "confidence": 0.7,
        "recommendation": "accept",
        "hint_plan": [{"text": "Check every DB course is covered.",
                       "supported_by": [0]}],
    }
    raw.update(overrides)
    return json.dumps(raw)


# ── the prompt tells the truth about the schema ──────────────────────────────

def test_the_answer_format_shown_in_the_prompt_actually_parses(bundle):
    # The example in the prompt is the only schema the model ever sees. If it
    # drifts from the parser, every answer is rejected and the stage abstains
    # on everything while appearing to work.
    prompt = build_prompt(bundle, REASON)
    start = prompt.index('{\n  "labels"')
    example = json.loads(prompt[start:prompt.index("\n}", start) + 2])

    concrete = {
        "labels": [LABELS[0]],
        "claims": [{"type": "divisor_element_missing",
                    "tuple": "(Alice)", "missing_element": "DB2"}],
        "evidence_ids": {LABELS[0]: [_citable(bundle)]},
        "confidence": 0.5,
        "recommendation": "accept",
        "hint_plan": [{"text": "a hint", "supported_by": [0]}],
        # Optional in the schema, but filled here so the repair shape shown in
        # the prompt is checked against the parser too.
        "repair": {"type": "add_correlation",
                   "target_block": "the NOT EXISTS subquery",
                   "candidate_predicate": "t.StuID = s.StuID"},
    }
    assert set(example) == set(concrete), "prompt example and parser disagree on keys"
    assert set(example["hint_plan"][0]) == set(concrete["hint_plan"][0])
    parse_hypothesis(concrete, bundle)          # must not raise


def test_every_registered_claim_type_is_offered_with_its_fields(bundle):
    prompt = build_prompt(bundle, REASON)
    for name, required in CLAIM_TYPES.items():
        assert name in prompt
        for field_name in required:
            assert field_name in prompt, "%s: field %s never shown" % (name, field_name)


def test_every_repair_type_is_offered_with_its_fields(bundle):
    prompt = build_prompt(bundle, REASON)
    for name, required in REPAIR_TYPES.items():
        assert name in prompt
        for field_name in required:
            assert field_name in prompt


def test_the_closed_vocabulary_and_the_citable_ids_are_both_shown(bundle):
    prompt = build_prompt(bundle, REASON)
    for label in LABELS:
        assert label in prompt
    for item in bundle.all_items():
        assert item.id in prompt


def test_the_prompt_says_why_the_model_was_called(bundle):
    assert REASON in build_prompt(bundle, REASON)


# ── the three ways out of generate ───────────────────────────────────────────

def test_a_well_formed_hypothesis_is_returned(bundle):
    result = generate(bundle, StubModel([_good(bundle)]), reason=REASON)
    assert result.accepted
    assert result.hypothesis.labels == (LABELS[0],)
    assert len(result.attempts) == 1


def test_a_declared_abstention_is_taken_at_its_word(bundle):
    # Parsing it as a diagnosis would demand labels and citations the model
    # has just said it cannot supply.
    result = generate(bundle, StubModel(['{"recommendation": "abstain"}']),
                      reason=REASON)
    assert result.abstained and not result.accepted
    assert len(result.attempts) == 1


def test_exhausting_the_attempts_abstains_rather_than_raising(bundle):
    result = generate(bundle, StubModel(["junk"] * 3), reason=REASON)
    assert result.abstained and not result.accepted
    assert len(result.attempts) == 3
    assert "no well-formed hypothesis" in result.reason


# ── the constraint, and what happens when it bites ───────────────────────────

def test_a_citation_that_does_not_resolve_is_refused(bundle):
    bad = _good(bundle, evidence_ids={LABELS[0]: ["exe.no_such_id"]})
    result = generate(bundle, StubModel([bad] * 3), reason=REASON)
    assert not result.accepted
    assert "does not resolve" in result.attempts[0].error


def test_a_label_outside_the_vocabulary_is_refused(bundle):
    bad = _good(bundle, labels=["INVENTED_LABEL"])
    result = generate(bundle, StubModel([bad] * 3), reason=REASON)
    assert not result.accepted


def test_the_parsers_complaint_is_fed_back_verbatim(bundle):
    # Paraphrasing the complaint turns a retry into a negotiation; the parser
    # knows exactly which rule was broken.
    bad = _good(bundle, labels=["INVENTED_LABEL"])
    result = generate(bundle, StubModel([bad, _good(bundle)]), reason=REASON)
    assert result.accepted
    complaint = result.attempts[0].error
    assert complaint in result.attempts[1].prompt
    assert "REJECTED" in result.attempts[1].prompt


def test_a_rejected_attempt_is_kept_not_discarded(bundle):
    # Rejected attempts are how often the constraint actually bit, which the
    # paper reports and which would be invisible if only successes were kept.
    bad = _good(bundle, labels=["INVENTED_LABEL"])
    result = generate(bundle, StubModel([bad, _good(bundle)]), reason=REASON)
    assert len(result.attempts) == 2
    assert result.attempts[0].error and not result.attempts[1].error


def test_generate_refuses_without_a_deterministic_reason(bundle):
    # The deterministic path decides when this stage is worth entering; no
    # reason means that decision was never made.
    with pytest.raises(GenerationError):
        generate(bundle, StubModel([_good(bundle)]))


def test_a_transport_failure_is_raised_not_swallowed(bundle):
    class Broken:
        def complete(self, prompt):
            raise TimeoutError("upstream gone")

    with pytest.raises(GenerationError) as exc:
        generate(bundle, Broken(), reason=REASON)
    assert "upstream gone" in str(exc.value)


# ── unwrapping a reply ───────────────────────────────────────────────────────

def test_json_in_a_code_fence_is_unwrapped(bundle):
    wrapped = "Here is my analysis:\n```json\n%s\n```\nHope that helps." % _good(bundle)
    assert generate(bundle, StubModel([wrapped]), reason=REASON).accepted


def test_json_surrounded_by_prose_is_unwrapped(bundle):
    assert generate(bundle, StubModel(["I think:\n" + _good(bundle) + "\nDone."]),
                    reason=REASON).accepted


def test_a_reply_with_no_object_is_rejected():
    with pytest.raises(HypothesisError):
        extract_json("I'm afraid I can't help with that.")


def test_a_malformed_object_is_rejected_rather_than_repaired():
    # If the object itself is broken, that is the model's answer.
    with pytest.raises(HypothesisError):
        extract_json('{"labels": [unquoted]}')


def test_a_json_array_is_not_a_hypothesis():
    with pytest.raises(HypothesisError):
        extract_json("[1, 2, 3]")


def test_an_oversized_reply_is_refused_before_parsing():
    with pytest.raises(HypothesisError) as exc:
        extract_json("{" + "x" * (MAX_RESPONSE_CHARS + 10) + "}")
    assert "over the" in str(exc.value)


def test_the_stub_records_what_it_was_asked(bundle):
    model = StubModel([_good(bundle)])
    generate(bundle, model, reason=REASON)
    assert len(model.prompts) == 1
    assert REASON in model.prompts[0]


def test_attempt_defaults_to_no_error():
    assert Attempt("p", "r").error is None

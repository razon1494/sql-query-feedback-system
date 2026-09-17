"""Stage 2 hypothesis language: h = <M, phi, W, R, F, c, a>.

Implements the typed diagnostic hypothesis of the journal draft, Section 3.1:

    M       a possibly multi-label diagnosis drawn from a controlled vocabulary
    phi     machine-checkable claims
    W       the evidence items supporting each label
    R       an optional minimal repair
    F       a pedagogical hint plan
    c       the model's calibrated confidence, in [0, 1]
    a       the model's own recommendation, accept or abstain

Scope. This module decides **well-formedness only**. It answers "is this a
hypothesis the verifier can go and check?", never "is this hypothesis true?".
Checking claims against the bundle is Stage 3, and weighing support against the
acceptance contract is Equation (5). Nothing here may anticipate either, so a
claim the evidence flatly contradicts still parses.

`c` and `a` are recorded because the draft's procedure uses them to rank and
prune the candidate queue before verification. They carry no weight in
acceptance, and this module gives them no privileged treatment.
"""
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple

# ══════════════════════════════════════════════════════════════════════
#  M -- the controlled vocabulary
# ══════════════════════════════════════════════════════════════════════

UNCERTAIN = "UNCERTAIN"
RESIDUAL_PREFIX = "Other-"

# Section 3.1 names these and fixes their correspondence to the deterministic
# taxonomy. The vocabulary must *refine* that taxonomy rather than diverge from
# it, because Theorem 1 (conservative extension) states D(T,q) subset-of
# Y(T,q) over a single shared label space.
LABELS: Tuple[str, ...] = (
    "MISSING_OR_WEAKENED_PREDICATE",     # M1
    "EXISTENTIAL_MEMBERSHIP_CONFUSION",  # M2, M3
    "JOIN_TYPE_CONFUSION",               # M4
    "MISSING_JOIN_CONDITION",            # M5
    "GROUPING_ERROR",                    # M6
    "HAVING_WHERE_CONFUSION",            # M7
    "QUANTIFICATION_ERROR",              # M8
    "CORRELATION_ERROR",                 # M9
    "SET_OPERATION_MISUSE",              # M10
    "NULL_SAFETY_ERROR",                 # the Null_Equality detector
    "AGGREGATE_THRESHOLD_ERROR",         # see the note below
    UNCERTAIN,
)

# Every deterministic detector must land on exactly one label, or Theorem 1
# fails for the submissions that detector fires on: a deterministic diagnosis
# with no vocabulary element cannot appear in Y at all.
#
# Section 3.1 names eleven of the sixteen detectors the implementation carries.
# Four of the remainder fold into an element the draft already defines, on the
# strength of the taxonomy's own wording -- M5 is "fewer joined tables, or no
# join condition", M10 is "set operator differs *or absent*". The fifth,
# HARDCODED_THRESHOLD, has no home in M1-M10, so AGGREGATE_THRESHOLD_ERROR is
# added above rather than leaving the detector unmapped. That addition is ours,
# not the draft's, and Section 3.1 needs amending to match.
DETERMINISTIC_TO_LABEL: Mapping[str, str] = MappingProxyType({
    # named in the draft
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
    # folded in, on the taxonomy's own wording
    "CARTESIAN_PRODUCT":      "MISSING_JOIN_CONDITION",   # M5, "no join condition"
    "MISSING_SET_OP":         "SET_OPERATION_MISUSE",     # M10, "or absent"
    "MISSING_NOT_EXISTS":     "QUANTIFICATION_ERROR",     # universal quantification
    "MISSING_HAVING":         "GROUPING_ERROR",           # a filter over groups
    # outside M1-M10 entirely
    "HARDCODED_THRESHOLD":    "AGGREGATE_THRESHOLD_ERROR",
})


def is_residual_label(label: str) -> bool:
    """A pattern with no deterministic detector, namespaced per Section 3.1.

    Residuals stay visibly distinct from the audited core vocabulary and may be
    promoted to first-class labels only through the rule-induction path.
    """
    return (isinstance(label, str)
            and label.startswith(RESIDUAL_PREFIX)
            and len(label) > len(RESIDUAL_PREFIX))


# ══════════════════════════════════════════════════════════════════════
#  phi -- machine-checkable claims
# ══════════════════════════════════════════════════════════════════════

# Each entry names the fields a claim of that type must carry. A claim is
# machine-checkable by construction: it names the objects a verifier resolves,
# so Stage 3 never has to interpret prose. Every type below is traceable to a
# sentence of the draft; the registry is open for the rule-induction path but
# not for the model, which may only emit types listed here.
CLAIM_TYPES: Mapping[str, Tuple[str, ...]] = MappingProxyType({
    # Listing 1, verbatim fields.
    "binding_deficit": ("student_block", "reference_block", "missing_outer_alias"),
    # Sec. 3.1: "the submitted result contains tuple t that lacks divisor element d".
    "divisor_element_missing": ("tuple", "missing_element"),
    # Sec. 3.1: "replacing predicate p_s with p_r eliminates all observed
    # divergences without changing unrelated clauses".
    "repair_eliminates_divergence": ("student_predicate", "reference_predicate"),
    # Sec. 3.2, support obligation for a wrong-join-type diagnosis.
    "join_type_difference": ("student_join", "reference_join"),
    "unmatched_key_counterexample": ("instance", "unmatched_key"),
    # Sec. 3.2, support obligation for an IN-for-division diagnosis.
    "task_expresses_universal_coverage": ("source",),
    # Sec. 7, the conflict-checking example.
    "grouping_clause_present": ("block", "present"),
})

# Sec. 7.3 constrains a repair by edit location and edit budget. The draft names
# add_correlation in Listing 1 and predicate replacement in Section 8.1.
REPAIR_TYPES: Mapping[str, Tuple[str, ...]] = MappingProxyType({
    "add_correlation": ("target_block", "candidate_predicate"),
    "replace_predicate": ("target_block", "student_predicate", "candidate_predicate"),
})

RECOMMENDATIONS = ("accept", "abstain")


class HypothesisError(ValueError):
    """The model's output is not a hypothesis this system can reason about."""


@dataclass(frozen=True)
class Hypothesis:
    """A parsed, well-formed hypothesis. Says nothing about truth."""
    labels: Tuple[str, ...]
    claims: Tuple[Dict[str, Any], ...]
    evidence_ids: Mapping[str, Tuple[str, ...]]
    confidence: float
    recommendation: str
    repair: Optional[Dict[str, Any]] = None
    hint_plan: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)

    def cited_ids(self) -> Tuple[str, ...]:
        """Every evidence id cited, in a stable order, without duplicates."""
        seen: List[str] = []
        for label in self.labels:
            for item_id in self.evidence_ids.get(label, ()):
                if item_id not in seen:
                    seen.append(item_id)
        return tuple(seen)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "labels": list(self.labels),
            "claims": [dict(c) for c in self.claims],
            "evidence_ids": {k: list(v) for k, v in self.evidence_ids.items()},
            "repair": dict(self.repair) if self.repair else None,
            "hint_plan": [dict(s) for s in self.hint_plan],
            "confidence": self.confidence,
            "recommendation": self.recommendation,
        }


# ══════════════════════════════════════════════════════════════════════
#  Parsing
# ══════════════════════════════════════════════════════════════════════

def parse_hypothesis(raw: Dict[str, Any], bundle) -> Hypothesis:
    """Validate one model-emitted hypothesis against the language.

    `bundle` is the EvidenceBundle the packet was built from; it is consulted
    only to confirm that cited identifiers resolve. An unresolvable citation
    supports nothing, so it is a malformed hypothesis rather than a false one.

    Raises HypothesisError on the first violation, naming it.
    """
    if not isinstance(raw, dict):
        raise HypothesisError(f"hypothesis must be an object, got {type(raw).__name__}")

    labels = _parse_labels(raw.get("labels"))
    claims = _parse_claims(raw.get("claims") or [])

    return Hypothesis(
        labels=labels,
        claims=claims,
        evidence_ids=MappingProxyType(_parse_evidence(raw.get("evidence_ids"),
                                                      labels, bundle)),
        confidence=_parse_confidence(raw.get("confidence")),
        recommendation=_parse_recommendation(raw.get("recommendation")),
        repair=_parse_repair(raw.get("repair")),
        hint_plan=_parse_hint_plan(raw.get("hint_plan") or [], len(claims)),
    )


def _parse_labels(value: Any) -> Tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise HypothesisError("labels must be a non-empty list")
    for label in value:
        if label in LABELS or is_residual_label(label):
            continue
        raise HypothesisError(
            f"{label!r} is not in the controlled vocabulary; a pattern with no "
            f"deterministic detector must be namespaced {RESIDUAL_PREFIX}<type>")
    if UNCERTAIN in value and len(value) > 1:
        raise HypothesisError(
            f"{UNCERTAIN} states that the evidence identifies no misconception, "
            "so it cannot accompany one")
    if len(set(value)) != len(value):
        raise HypothesisError("labels contains a duplicate")
    return tuple(value)


def _parse_claims(value: Any) -> Tuple[Dict[str, Any], ...]:
    if not isinstance(value, (list, tuple)):
        raise HypothesisError("claims must be a list")
    claims: List[Dict[str, Any]] = []
    for index, claim in enumerate(value):
        if not isinstance(claim, dict):
            raise HypothesisError(
                f"claim {index} is not an object: a diagnosis may not be "
                "supported by free-form prose")
        kind = claim.get("type")
        required = CLAIM_TYPES.get(kind)
        if required is None:
            raise HypothesisError(
                f"claim {index} has unregistered claim type {kind!r}; "
                f"expected one of {', '.join(sorted(CLAIM_TYPES))}")
        missing = [f for f in required if f not in claim]
        if missing:
            raise HypothesisError(
                f"claim {index} of type {kind!r} is missing {', '.join(missing)}")
        claims.append(dict(claim))
    return tuple(claims)


def _parse_evidence(value: Any, labels: Tuple[str, ...], bundle
                    ) -> Dict[str, Tuple[str, ...]]:
    """Normalise W to one citation list per label.

    Section 8.2 requires that each label have independent support, so a flat
    list is only unambiguous when there is a single label to attribute it to.
    """
    if labels == (UNCERTAIN,):
        return {UNCERTAIN: ()}

    if isinstance(value, (list, tuple)):
        if len(labels) > 1:
            raise HypothesisError(
                "a multi-label diagnosis must cite evidence per label, since "
                "each label requires independent support")
        value = {labels[0]: value}

    if not isinstance(value, dict):
        raise HypothesisError("evidence_ids must be a list or an object keyed by label")

    unknown = set(value) - set(labels)
    if unknown:
        raise HypothesisError(
            f"evidence cited for {', '.join(sorted(unknown))}, which is not diagnosed")

    out: Dict[str, Tuple[str, ...]] = {}
    for label in labels:
        ids = value.get(label) or []
        if not isinstance(ids, (list, tuple)):
            raise HypothesisError(f"evidence for {label} must be a list of ids")
        if not ids:
            raise HypothesisError(f"{label} cites no evidence")
        for item_id in ids:
            if bundle is not None and bundle.resolve(item_id) is None:
                raise HypothesisError(
                    f"evidence id {item_id!r} does not resolve in the bundle")
        out[label] = tuple(ids)
    return out


def _parse_repair(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise HypothesisError("repair must be an object")
    kind = value.get("type")
    required = REPAIR_TYPES.get(kind)
    if required is None:
        raise HypothesisError(
            f"unregistered repair type {kind!r}; "
            f"expected one of {', '.join(sorted(REPAIR_TYPES))}")
    missing = [f for f in required if f not in value]
    if missing:
        raise HypothesisError(f"repair of type {kind!r} is missing {', '.join(missing)}")
    return dict(value)


def _parse_hint_plan(value: Any, claim_count: int) -> Tuple[Dict[str, Any], ...]:
    """The hint plan is ordered, and every step names the claims it rests on.

    Section 7: the renderer may phrase a hint freely, but "every factual
    statement must map to a verified claim". A step citing a claim that does
    not exist could never be rendered.
    """
    if not isinstance(value, (list, tuple)):
        raise HypothesisError("hint_plan must be a list of steps")
    steps: List[Dict[str, Any]] = []
    for index, step in enumerate(value):
        if not isinstance(step, dict) or "text" not in step:
            raise HypothesisError(f"hint step {index} must be an object with text")
        cites = step.get("supported_by") or ()
        if not isinstance(cites, (list, tuple)):
            raise HypothesisError(f"hint step {index}: supported_by must be a list")
        for claim_index in cites:
            if not isinstance(claim_index, int) or not 0 <= claim_index < claim_count:
                raise HypothesisError(
                    f"hint step {index}: supported_by names claim {claim_index}, "
                    f"but the hypothesis has {claim_count} claim(s)")
        steps.append({**step, "supported_by": tuple(cites)})
    return tuple(steps)


def _parse_confidence(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise HypothesisError(f"confidence must be a number in [0, 1], got {value!r}")
    if not 0.0 <= float(value) <= 1.0:
        raise HypothesisError(f"confidence {value} lies outside [0, 1]")
    return float(value)


def _parse_recommendation(value: Any) -> str:
    if value not in RECOMMENDATIONS:
        raise HypothesisError(
            f"recommendation must be one of {', '.join(RECOMMENDATIONS)}, got {value!r}")
    return value

"""
Stage 2 (Phase 1.3) — constrained hypothesis generation.

The bridge between the two halves that already exist: `evidence.py` builds the
bundle a model may see, `hypothesis.py` decides whether what comes back is a
hypothesis the verifier can go and check. This module carries a packet to a
model and carries a structured answer back, and it is responsible for exactly
one thing in between — making sure the model's freedom stays bounded.

**The model is never trusted, only constrained.** Three constraints, in the
order they bite:

1. *Before the call* — the prompt states the closed vocabulary, the claim-type
   registry, and the evidence ids that exist. Nothing else is offerable.
2. *After the call* — `parse_hypothesis` rejects any label outside the
   vocabulary, any claim type not registered, any claim missing a required
   field, and any citation that does not resolve in the bundle. That check is
   not repeated here; this module routes failures, it does not re-judge them.
3. *On failure* — a malformed answer is not a diagnosis. It is retried a
   bounded number of times with the parser's own complaint fed back, and then
   the module abstains. It never repairs an answer on the model's behalf and
   never lowers the bar to let one through: a hypothesis that had to be fixed
   up is one nobody verified.

**Whether to call at all is not decided here.** `evidence.llm_invocation_reason`
already answers that from the deterministic outcome, and `generate` refuses to
run without a reason, so the expensive path cannot be entered by accident.

**No vendor appears in this file.** A model is anything with a `complete`
method; `StubModel` implements it from a scripted list, which is what the tests
use and what makes the whole stage exercisable before any account exists. When
model access is settled, a real client is a small separate module and nothing
here changes.
"""
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from backend.hypothesis import (                                  # noqa: F401
    CLAIM_TYPES, LABELS, RECOMMENDATIONS, REPAIR_TYPES, RESIDUAL_PREFIX,
    Hypothesis, HypothesisError, parse_hypothesis,
)

# A malformed answer is cheap to retry and expensive to trust. Two retries is
# enough to clear a mis-quoted brace; past that the model is not going to
# produce something the verifier can check, and abstaining is the honest end.
MAX_ATTEMPTS = 3

# Guards against a runaway response burning the budget on one submission.
MAX_RESPONSE_CHARS = 20000


class GenerationError(RuntimeError):
    """The model could not be reached, or could not be induced to answer."""


@dataclass
class Attempt:
    """One exchange, kept whether it succeeded or not.

    The rejected attempts are the interesting ones: they are the evidence for
    how often the constraint actually bites, which is a number the paper needs
    and which would be invisible if only the accepted answer were recorded.
    """
    prompt: str
    response: str
    error: Optional[str] = None


@dataclass
class GenerationResult:
    hypothesis: Optional[Hypothesis]
    attempts: List[Attempt] = field(default_factory=list)
    abstained: bool = False
    reason: str = ""

    @property
    def accepted(self) -> bool:
        return self.hypothesis is not None


# ── the model interface ──────────────────────────────────────────────────────

class StubModel:
    """A model that replays a scripted list of responses.

    Not a mock in the testing sense — it is how Stage 2 is developed and
    measured before any account exists, and it stays useful afterwards for
    pinning the retry and abstention paths, which a live model would reach
    only by chance.
    """

    def __init__(self, responses: Sequence[str]):
        self._responses = list(responses)
        self.prompts: List[str] = []

    def complete(self, prompt: str) -> str:
        self.prompts.append(prompt)
        if not self._responses:
            raise GenerationError("StubModel ran out of scripted responses")
        return self._responses.pop(0)


# ── prompt construction ──────────────────────────────────────────────────────

def _registry_lines(registry) -> List[str]:
    """One line per registered type, naming the fields it must carry.

    Read straight out of 1.2's registry rather than written out here, so a type
    added there cannot be silently absent from what the model is told it may
    use — and so the fields the parser will demand are the fields the model was
    shown. Listing the names without their fields would leave the model to
    guess, and every guess comes back as a rejection.
    """
    return ["  %-34s requires: %s" % (name, ", ".join(registry[name]) or "(none)")
            for name in sorted(registry)]


def _evidence_id_lines(bundle) -> List[str]:
    """Every citable id, with its channel.

    The model is given the ids it may cite and nothing else. A citation that
    does not resolve is rejected downstream, but showing the list up front
    turns most of those rejections into non-events.
    """
    out = []
    for item in bundle.all_items():
        channel = bundle.channel_of(item.id) or "?"
        out.append("  %-22s [%s] %s" % (item.id, channel, item.kind))
    return out


def build_prompt(bundle, reason: str, complaint: Optional[str] = None) -> str:
    """The packet, the constraints, and the shape of an acceptable answer.

    `complaint` is the parser's own message from a previous attempt, passed
    back verbatim rather than paraphrased: the parser knows exactly which rule
    was broken, and softening that into advice is how a retry turns into a
    negotiation.
    """
    packet = bundle.to_packet()
    parts = [
        "You are diagnosing a student's SQL submission.",
        "",
        "The deterministic analyser has already run. It established that the "
        "submission is wrong and could not say why. Its reason for calling "
        "you: %s" % reason,
        "",
        "You may propose a diagnosis, or decline. Declining is a real answer "
        "and is preferred to a guess: a wrong diagnosis teaches the student "
        "something false.",
        "",
        "=== EVIDENCE ===",
        json.dumps(packet, indent=2, default=str),
        "",
        "=== CITABLE EVIDENCE IDS ===",
    ]
    parts.extend(_evidence_id_lines(bundle))
    parts.extend([
        "",
        "=== CLOSED LABEL VOCABULARY ===",
        "Use only these. For a pattern none of them covers, prefix your own "
        "label with '%s' (for example %sPredicate-Boundary)."
        % (RESIDUAL_PREFIX, RESIDUAL_PREFIX),
        "",
    ])
    parts.extend("  " + label for label in LABELS)
    parts.extend([
        "",
        "=== CLAIM TYPES ===",
        "Every claim must be one of these, with its required fields present. "
        "Free prose is not a claim and will not be read.",
        "",
    ])
    parts.extend(_registry_lines(CLAIM_TYPES))
    parts.extend([
        "",
        "=== REPAIR TYPES ===",
        "If you propose a repair it must be one of these, with its fields:",
        "",
    ])
    parts.extend(_registry_lines(REPAIR_TYPES))
    parts.extend([
        "",
        "=== ANSWER FORMAT ===",
        "Reply with one JSON object and nothing else:",
        "",
        json.dumps({
            "labels": ["<from the vocabulary above>"],
            "claims": [{"type": "<a claim type>", "...": "<its fields>"}],
            "evidence_ids": {"<label>": ["<citable id>"]},
            "confidence": 0.0,
            "recommendation": "|".join(RECOMMENDATIONS),
            "repair": {"type": "|".join(sorted(REPAIR_TYPES)),
                       "...": "<its fields>"},
            "hint_plan": [{"text": "<hint text>", "supported_by": [0]}],
        }, indent=2),
        "",
        "Every label needs its own entry in evidence_ids. In hint_plan, "
        "supported_by holds integer positions into your claims array, not "
        "claim type names. Cite only ids listed above.",
    ])
    if complaint:
        parts.extend([
            "",
            "=== YOUR PREVIOUS ANSWER WAS REJECTED ===",
            complaint,
            "",
            "Correct it, or set recommendation to 'abstain'.",
        ])
    return "\n".join(parts)


# ── response handling ────────────────────────────────────────────────────────

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)


def extract_json(response: str) -> Dict[str, Any]:
    """Pull the one JSON object out of a reply, or say why it cannot be done.

    Models wrap JSON in prose or in a code fence often enough that failing on
    it would turn a formatting habit into an abstention. Anything beyond
    unwrapping is refused: if the object itself is malformed, that is the
    model's answer and it gets rejected as such.
    """
    if len(response) > MAX_RESPONSE_CHARS:
        raise HypothesisError(
            "response is %d characters, over the %d limit"
            % (len(response), MAX_RESPONSE_CHARS))

    fenced = _FENCE_RE.search(response)
    candidate = fenced.group(1) if fenced else response

    start = candidate.find("{")
    end = candidate.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise HypothesisError("no JSON object found in the response")
    try:
        parsed = json.loads(candidate[start:end + 1])
    except ValueError as exc:
        raise HypothesisError("response is not valid JSON: %s" % exc)
    if not isinstance(parsed, dict):
        raise HypothesisError("expected a JSON object, got %s"
                              % type(parsed).__name__)
    return parsed


def _is_declared_abstention(raw: Dict[str, Any]) -> bool:
    return str(raw.get("recommendation", "")).strip().lower() == "abstain"


# ── the stage ────────────────────────────────────────────────────────────────

def generate(bundle, model, reason: Optional[str] = None,
             max_attempts: int = MAX_ATTEMPTS) -> GenerationResult:
    """Ask a model for a hypothesis the verifier can check, or abstain.

    `reason` is `evidence.llm_invocation_reason`'s output and is required: the
    deterministic path decides when this stage is worth entering, and passing
    None here means that decision was never made.

    Returns a result rather than raising, because an abstention is an outcome
    the pipeline reports and not an error it recovers from. Every attempt is
    kept, including the rejected ones.
    """
    if not reason:
        raise GenerationError(
            "generate() needs the deterministic reason for invoking the model; "
            "see evidence.llm_invocation_reason")

    attempts: List[Attempt] = []
    complaint: Optional[str] = None

    for _ in range(max(1, max_attempts)):
        prompt = build_prompt(bundle, reason, complaint)
        try:
            response = model.complete(prompt)
        except GenerationError:
            raise
        except Exception as exc:                      # transport, quota, timeout
            raise GenerationError("model call failed: %s" % exc)

        try:
            raw = extract_json(response)
        except HypothesisError as exc:
            complaint = str(exc)
            attempts.append(Attempt(prompt, response, complaint))
            continue

        # An abstention is accepted as given. Parsing it as a diagnosis would
        # demand labels and citations the model has just said it cannot supply.
        if _is_declared_abstention(raw):
            attempts.append(Attempt(prompt, response))
            return GenerationResult(None, attempts, abstained=True,
                                    reason="model declined to diagnose")

        try:
            hypothesis = parse_hypothesis(raw, bundle)
        except HypothesisError as exc:
            complaint = str(exc)
            attempts.append(Attempt(prompt, response, complaint))
            continue

        attempts.append(Attempt(prompt, response))
        return GenerationResult(hypothesis, attempts)

    return GenerationResult(
        None, attempts, abstained=True,
        reason="no well-formed hypothesis in %d attempts; last complaint: %s"
               % (len(attempts), complaint))

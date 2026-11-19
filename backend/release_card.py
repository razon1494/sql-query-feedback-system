"""
The three parts of a released diagnosis, as slide 6 of the ICWL 2026 talk
shows them: a label (with its rule ID from paper Table 1), the evidence that
released it, and a repair direction.

Everything here is built from what the pipeline has already computed (the
result comparison, the provenance trace, the edge-case runs, the execution
error). The rules, the evidence gate and the feedback templates are unchanged.
Evidence about missing rows is given as a count only, so the expected result
is not revealed to a student.

Every function returns new objects; inputs are never modified.
"""
from typing import Dict, List, Optional, Tuple

# Paper Table 1. Keys without an entry sit outside the taxonomy: the
# NULL_EQUALITY guard and the shape triggers (paper Sec. 4).
RULE_IDS = {
    "MISSING_WHERE": "M1",
    "IN_vs_EXISTS": "M2",
    "NOT_IN_vs_NOT_EXISTS": "M3",
    "WRONG_JOIN_TYPE": "M4",
    "CARTESIAN_PRODUCT": "M5",
    "MISSING_JOIN": "M5",
    "MISSING_GROUP_BY": "M6",
    "HAVING_vs_WHERE": "M7",
    "IN_FOR_DIVISION": "M8",
    "MISSING_CORRELATED_REF": "M9",
    "WRONG_SET_OP": "M10",
    "MISSING_SET_OP": "M10",
}

# What each constructed edge-case database holds, in a student's words.
EDGE_SETTINGS = {
    "partial_match": "a student took only some of the database courses",
    "empty_courses": "there are no database courses at all",
    "all_enrolled": "every student took every database course",
    "single_course": "there is only one database course",
    "no_students": "there are no students",
    "null_link": "the enrolment table holds NULL values",
}
EDGE_PREFERENCE = ("null_link", "partial_match", "empty_courses", "single_course", "all_enrolled",
                   "no_students")

# Which observations to name first, by the effect each rule predicts
# (feedback_generator.MISCONCEPTION_PREDICTS).
_EXTRA_FIRST = ("divergence", "extra", "edge", "differs")
EVIDENCE_ORDER = {
    "MISSING_WHERE": _EXTRA_FIRST,
    "MISSING_HAVING": _EXTRA_FIRST,
    "WRONG_JOIN_TYPE": _EXTRA_FIRST,
    "MISSING_JOIN": _EXTRA_FIRST,
    "CARTESIAN_PRODUCT": _EXTRA_FIRST,
    "IN_vs_EXISTS": _EXTRA_FIRST,
    "IN_FOR_DIVISION": _EXTRA_FIRST,
    "HAVING_vs_WHERE": ("error", "differs"),
    "MISSING_GROUP_BY": ("error", "single_group", "differs"),
    "MISSING_NOT_EXISTS": ("empty", "extra", "edge", "differs"),
    "MISSING_CORRELATED_REF": ("empty", "extra", "edge", "differs"),
    "NOT_IN_vs_NOT_EXISTS": ("edge", "missing", "empty", "differs"),
    "NULL_EQUALITY": ("empty", "missing", "differs"),
}
DEFAULT_ORDER = ("differs", "edge", "error")

MAX_NAMED_ROWS = 2
_DIRECTION_BREAKS = ("\n", ". ", ", e.g.", " — e.g.", " (for example", " — either")


# ── Label and repair direction ────────────────────────────────────────

def rule_id(key: str) -> Optional[str]:
    return RULE_IDS.get(key)


def repair_direction(fix: str) -> Tuple[str, str]:
    """Split a fix template into its first instruction and the rest."""
    text = (fix or "").strip()
    cut = min((i for i in (text.find(b) for b in _DIRECTION_BREAKS) if i > 0), default=-1)
    if cut < 0:
        return text, ""
    head = text[:cut].rstrip(" :—-")
    return head + ".", _tidy_rest(text[cut:].lstrip(" .,:—\n"))


def _tidy_rest(rest: str) -> str:
    """Make the remainder of a fix read as its own sentence."""
    if rest.startswith("(") and rest.endswith(")."):
        rest = rest[1:-2] + "."
    if rest.lower().startswith(("e.g.", "for example,")):
        rest = "For example, " + rest.split(" ", 1 if rest.lower().startswith("e.g.") else 2)[-1]
    return rest[:1].upper() + rest[1:]


# ── Observations ──────────────────────────────────────────────────────

def observed_evidence(comparison: dict, provenance: Optional[dict],
                      edge_results: List[dict], execution_error: Optional[str]) -> dict:
    """The facts a released diagnosis can cite, gathered once per analysis."""
    prov = provenance or {}
    failed = [e for e in (edge_results or []) if not e.get("passed")]
    rank = {name: i for i, name in enumerate(EDGE_PREFERENCE)}
    return {
        "are_equivalent": bool(comparison.get("are_equivalent")),
        "extra_rows": list(comparison.get("extra_rows", [])),
        "missing_count": len(comparison.get("missing_rows", [])),
        "student_count": comparison.get("student_count", 0),
        "expected_count": comparison.get("base_count", 0),
        # Only the division trace names the divisor elements a row is missing.
        "false_positives": [d for d in prov.get("divergence_points", [])
                            if d.get("type") == "false_positive"
                            and d.get("name") and d.get("missing_courses")],
        "failed_edges": sorted(failed, key=lambda e: rank.get(e.get("name"), len(rank))),
        "error": execution_error,
    }


def _row_text(row: dict) -> str:
    return "(" + ", ".join(str(v) for v in row.values()) + ")"


def _from_divergence(obs: dict) -> Optional[str]:
    points = obs["false_positives"]
    if not points:
        return None
    named = [f"{d['name']} appears in your result without having taken "
             f"{', '.join(d['missing_courses'])}."
             for d in points[:MAX_NAMED_ROWS]]
    more = len(points) - MAX_NAMED_ROWS
    if more > 0:
        named.append(f"{more} more row(s) in your result have the same gap.")
    return " ".join(named)


def _from_extra(obs: dict) -> Optional[str]:
    rows = obs["extra_rows"]
    if not rows:
        return None
    return (f"Your result has {len(rows)} row(s) the task excludes, "
            f"for example {_row_text(rows[0])}.")


def _from_missing(obs: dict) -> Optional[str]:
    n = obs["missing_count"]
    return f"Your result leaves out {n} row(s) that belong in it." if n else None


def _from_edge(obs: dict, key: str) -> Optional[str]:
    if not obs["failed_edges"]:
        return None
    e = obs["failed_edges"][0]
    setting = EDGE_SETTINGS.get(e.get("name"), (e.get("title") or "a constructed case").lower())
    got = e.get("student_result", {}).get("row_count", 0)
    want = e.get("base_result", {}).get("row_count", 0)
    if key == "IN_FOR_DIVISION" and e.get("name") == "partial_match" and got > want:
        return (f"On a test database where {setting}, your query still returns that student "
                f"({got} row(s) where {want} are expected).")
    return (f"On a test database where {setting}, your query returns {got} row(s) "
            f"where {want} are expected.")


def _from_error(obs: dict) -> Optional[str]:
    return f"The database rejected your query: {obs['error']}" if obs["error"] else None


def _from_empty(obs: dict) -> Optional[str]:
    if obs["error"] or obs["student_count"]:
        return None
    return "Your query returns no rows at all." if obs["expected_count"] else None


def _from_single_group(obs: dict) -> Optional[str]:
    if obs["student_count"] == 1 and obs["expected_count"] > 1:
        return (f"Your query collapses everything into one row, where the expected result "
                f"has {obs['expected_count']}.")
    return None


def _from_differs(obs: dict) -> Optional[str]:
    if obs["are_equivalent"] or obs["error"]:
        return None
    return (f"Your result differs from the expected one: {len(obs['extra_rows'])} extra "
            f"and {obs['missing_count']} missing row(s).")


_BUILDERS = {
    "divergence": lambda obs, key: _from_divergence(obs),
    "extra": lambda obs, key: _from_extra(obs),
    "missing": lambda obs, key: _from_missing(obs),
    "edge": _from_edge,
    "error": lambda obs, key: _from_error(obs),
    "empty": lambda obs, key: _from_empty(obs),
    "single_group": lambda obs, key: _from_single_group(obs),
    "differs": lambda obs, key: _from_differs(obs),
}


def evidence_sentence(key: str, obs: dict) -> Optional[str]:
    """The first observation, in this rule's order of relevance, that holds."""
    for source in EVIDENCE_ORDER.get(key, DEFAULT_ORDER):
        sentence = _BUILDERS[source](obs, key)
        if sentence:
            return sentence
    return None


# ── Cards ─────────────────────────────────────────────────────────────

def describe_released(misconceptions: List[dict], obs: dict,
                      snippets: Dict[str, Optional[str]]) -> List[dict]:
    """Released diagnoses with rule ID, evidence, repair direction and pattern."""
    cards = []
    for m in misconceptions or []:
        direction, rest = repair_direction(m.get("fix", ""))
        cards.append({
            **m,
            "rule_id": rule_id(m.get("key", "")),
            "evidence": evidence_sentence(m.get("key", ""), obs),
            "repair": direction,
            "pattern": rest,
            "code_snippet": snippets.get(m.get("title")),
        })
    return cards


def describe_withheld(misconceptions: List[dict]) -> List[dict]:
    """Withheld proposals keep their content and gain the rule ID."""
    return [{**m, "rule_id": rule_id(m.get("key", ""))} for m in (misconceptions or [])]

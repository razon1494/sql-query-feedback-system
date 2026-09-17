"""Typed evidence bundle — Stage 1 of the constrained-LLM architecture.

Implements the evidence bundle of the journal draft:

    E(T, q_s) = <E_str, E_exe, E_prov, E_cex, E_con, E_hist>

Two requirements from the draft shape this module:

1. *Stable identifiers.* A hypothesis cites the evidence items that support it
   (the W component), and a verifier must be able to resolve those citations.
   So every item carries an id such as ``str.diff.3`` or ``prov.step.1`` that is
   stable for a given submission.

2. *Bounded excerpts.* The LLM receives a serialized packet, not database
   access. ``to_packet()`` truncates row lists and long strings so the packet
   stays a bounded summary of the bundle.

This module only *reshapes* facts the deterministic pipeline already computes.
It executes nothing and decides nothing about diagnosis.
"""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Packet bounds. The bundle keeps everything; only the LLM packet is trimmed.
MAX_ROWS_IN_PACKET = 10
MAX_STRING_LEN = 400
# The queries are the subject of the diagnosis rather than evidence about it,
# so they get their own cap. It exists to keep a pathological paste out of the
# packet, not to excerpt.
MAX_QUERY_LEN = 4000
QUERY_FIELDS = ("student_query", "reference_query")


# ══════════════════════════════════════════════════════════════════════
#  Items
# ══════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class EvidenceItem:
    """One addressable fact. ``id`` is what a hypothesis cites."""
    id: str
    kind: str
    content: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "kind": self.kind, "content": self.content}


@dataclass(frozen=True)
class EvidenceBundle:
    """The six evidence channels, plus the task description they concern."""
    task: Dict[str, Any]
    E_str: List[EvidenceItem] = field(default_factory=list)
    E_exe: List[EvidenceItem] = field(default_factory=list)
    E_prov: List[EvidenceItem] = field(default_factory=list)
    E_cex: List[EvidenceItem] = field(default_factory=list)
    E_con: List[EvidenceItem] = field(default_factory=list)
    E_hist: List[EvidenceItem] = field(default_factory=list)

    CHANNELS = ("E_str", "E_exe", "E_prov", "E_cex", "E_con", "E_hist")

    # -- lookup ------------------------------------------------------------
    def all_items(self) -> List[EvidenceItem]:
        items: List[EvidenceItem] = []
        for name in self.CHANNELS:
            items.extend(getattr(self, name))
        return items

    def resolve(self, item_id: str) -> Optional[EvidenceItem]:
        """Look up a cited evidence id. Returns None if the id does not exist,
        which a verifier must treat as an unsupported citation."""
        for item in self.all_items():
            if item.id == item_id:
                return item
        return None

    def channel_of(self, item_id: str) -> Optional[str]:
        """Which channel an id belongs to, e.g. 'E_prov'. Used by the
        acceptance rule, whose thresholds are expressed per channel."""
        for name in self.CHANNELS:
            if any(i.id == item_id for i in getattr(self, name)):
                return name
        return None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"task": self.task}
        for name in self.CHANNELS:
            out[name] = [i.to_dict() for i in getattr(self, name)]
        return out

    def to_packet(self) -> Dict[str, Any]:
        """Bounded serialization for the LLM. Same ids, smaller payloads."""
        out: Dict[str, Any] = {"task": _bound_task(self.task)}
        for name in self.CHANNELS:
            out[name] = [_bound_item(i) for i in getattr(self, name)]
        return out


# ══════════════════════════════════════════════════════════════════════
#  Bounding
# ══════════════════════════════════════════════════════════════════════

def _bound_item(item: EvidenceItem) -> Dict[str, Any]:
    # `id` is deliberately not bounded: it is the handle a hypothesis cites and
    # a verifier resolves, so a truncated one would simply fail to resolve.
    return {"id": item.id, "kind": item.kind, "content": _bound(item.content)}


def _bound_task(task: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Bound the task block, exempting the queries from the excerpt limit.

    A query cut at 400 characters leaves the model reasoning about clauses it
    never saw, and a hypothesis about one of them is unfalsifiable rather than
    merely wrong. Everything else in the block is ordinary prose and is bounded
    as usual.
    """
    out: Dict[str, Any] = {}
    for key, value in (task or {}).items():
        if key in QUERY_FIELDS and isinstance(value, str):
            out[key] = _bound_string(value, MAX_QUERY_LEN)
        else:
            out[key] = _bound(value)
    return out


def _bound_string(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + f"... [{len(value)} chars total]"


def _bound(value: Any) -> Any:
    """Truncate long strings and long row lists, recursively."""
    if isinstance(value, str):
        return _bound_string(value, MAX_STRING_LEN)
    if isinstance(value, dict):
        return {k: _bound(v) for k, v in value.items()}
    if isinstance(value, list):
        if len(value) <= MAX_ROWS_IN_PACKET:
            return [_bound(v) for v in value]
        kept = [_bound(v) for v in value[:MAX_ROWS_IN_PACKET]]
        return kept + [f"... [{len(value) - MAX_ROWS_IN_PACKET} more of {len(value)}]"]
    return value


# ══════════════════════════════════════════════════════════════════════
#  E_con — schema and integrity constraints
# ══════════════════════════════════════════════════════════════════════

def collect_schema_facts(conn) -> List[EvidenceItem]:
    """Read tables, columns, keys and foreign keys from a live SQLite
    connection. These are the constraint facts a diagnosis may rely on, for
    example that a link column is NOT NULL and so no NULL-bearing instance can
    be constructed for it."""
    items: List[EvidenceItem] = []
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tables = [r[0] for r in cur.fetchall()]
    except Exception as exc:                      # defensive: schema is optional
        return [EvidenceItem("con.error", "schema_error", {"message": str(exc)})]

    for tbl in tables:
        try:
            cur.execute(f"PRAGMA table_info([{tbl}])")
            columns = [{"name": r[1], "type": r[2],
                        "not_null": bool(r[3]), "primary_key": bool(r[5])}
                       for r in cur.fetchall()]
            cur.execute(f"PRAGMA foreign_key_list([{tbl}])")
            foreign_keys = [{"column": r[3], "references_table": r[2],
                             "references_column": r[4]} for r in cur.fetchall()]
        except Exception as exc:                  # defensive: skip one bad table
            items.append(EvidenceItem(f"con.table.{tbl}", "schema_error",
                                      {"table": tbl, "message": str(exc)}))
            continue

        items.append(EvidenceItem(
            id=f"con.table.{tbl}",
            kind="table_schema",
            content={"table": tbl, "columns": columns, "foreign_keys": foreign_keys},
        ))
    return items


# ══════════════════════════════════════════════════════════════════════
#  Bundle construction
# ══════════════════════════════════════════════════════════════════════

def build_bundle(problem: Dict[str, Any],
                 student_query: str,
                 parsing: Dict[str, Any],
                 execution: Dict[str, Any],
                 comparison: Dict[str, Any],
                 provenance: Dict[str, Any],
                 edge_cases: List[Dict[str, Any]],
                 counterexample: Optional[Dict[str, Any]] = None,
                 schema_items: Optional[List[EvidenceItem]] = None,
                 history: Optional[List[Dict[str, Any]]] = None) -> EvidenceBundle:
    """Assemble the bundle from what /api/analyze already computes.

    Every argument is a dict the deterministic pipeline produced; nothing is
    recomputed here. A missing or malformed section degrades to an empty
    channel rather than raising, because the draft states the bundle may be
    incomplete and absence of evidence is itself a fact.
    """
    task = {
        "id": problem.get("id"),
        "title": problem.get("title"),
        "type": problem.get("type"),
        "question": problem.get("question"),
        "reference_query": problem.get("base_query"),
        "student_query": student_query,
    }

    return EvidenceBundle(
        task=task,
        E_str=_build_structural(parsing),
        E_exe=_build_execution(execution, comparison, edge_cases),
        E_prov=_build_provenance(provenance),
        E_cex=_build_counterexamples(edge_cases, counterexample),
        E_con=list(schema_items or []),
        E_hist=_build_history(history),
    )


def _build_structural(parsing: Dict[str, Any]) -> List[EvidenceItem]:
    """Parse trees and clause-level differences.

    Binding relations (does an inner block reference an enclosing alias?) are
    surfaced as their own items, because a correlation diagnosis must cite a
    binding deficit rather than mere output divergence.
    """
    items: List[EvidenceItem] = []
    if not isinstance(parsing, dict):
        return items

    for side in ("base", "student"):
        parsed = parsing.get(side)
        if not isinstance(parsed, dict):
            continue
        items.append(EvidenceItem(f"str.parse.{side}", "parse_tree",
                                  {"side": side, "parsed": parsed}))
        binding = _binding_facts(parsed)
        if binding:
            items.append(EvidenceItem(f"str.binding.{side}", "binding_relation",
                                      {"side": side, **binding}))

    for idx, diff in enumerate(parsing.get("diffs") or []):
        if not isinstance(diff, dict):
            continue
        items.append(EvidenceItem(
            id=f"str.diff.{idx}",
            kind="clause_difference",
            content={"path": diff.get("path"), "difference_type": diff.get("type"),
                     "reference": diff.get("base"), "student": diff.get("student")},
        ))
    return items


BINDING_KEYS = ("where_type", "has_correlated_ref", "correlated_refs",
                "subquery_depth", "aliases", "subqueries")


def _binding_facts(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Pull correlation-relevant fields out of a parsed query, when present."""
    return {k: parsed[k] for k in BINDING_KEYS if k in parsed}


def _build_execution(execution: Dict[str, Any],
                     comparison: Dict[str, Any],
                     edge_cases: List[Dict[str, Any]]) -> List[EvidenceItem]:
    """Execution status on the main instance, plus per-edge-instance outcomes."""
    items: List[EvidenceItem] = []

    if isinstance(execution, dict):
        items.append(EvidenceItem("exe.main", "execution_result", {
            "instance": "main",
            "reference": execution.get("base"),
            "student": execution.get("student"),
        }))
    if isinstance(comparison, dict):
        items.append(EvidenceItem("exe.comparison.main", "output_comparison",
                                  {"instance": "main", **comparison}))

    for case in edge_cases or []:
        if not isinstance(case, dict):
            continue
        name = case.get("name") or case.get("title") or "unnamed"
        items.append(EvidenceItem(
            id=f"exe.comparison.{name}",
            kind="output_comparison",
            content={"instance": name,
                     "description": case.get("description"),
                     "passed": case.get("passed"),
                     **(case.get("comparison") or {})},
        ))
    return items


def _build_provenance(provenance: Dict[str, Any]) -> List[EvidenceItem]:
    """Trace steps and divergence points, i.e. tuple witnesses."""
    items: List[EvidenceItem] = []
    if not isinstance(provenance, dict):
        return items

    if provenance.get("error"):
        items.append(EvidenceItem("prov.error", "provenance_error",
                                  {"message": provenance["error"]}))

    for idx, step in enumerate(provenance.get("steps") or []):
        items.append(EvidenceItem(f"prov.step.{idx}", "provenance_step",
                                  step if isinstance(step, dict) else {"value": step}))

    for idx, point in enumerate(provenance.get("divergence_points") or []):
        items.append(EvidenceItem(f"prov.divergence.{idx}", "tuple_witness",
                                  point if isinstance(point, dict) else {"value": point}))
    return items


def failing_edge_instances(edge_cases: Optional[List[Dict[str, Any]]]
                           ) -> List[Dict[str, Any]]:
    """Edge instances on which the student query and the reference diverge.

    An instance is a counterexample only when it was run and the two queries
    disagreed. `passed is None` means the instance was never evaluated, which
    is not evidence of agreement.
    """
    return [case for case in (edge_cases or [])
            if isinstance(case, dict) and case.get("passed") is False]


def _build_counterexamples(edge_cases: List[Dict[str, Any]],
                           counterexample: Optional[Dict[str, Any]]) -> List[EvidenceItem]:
    """Constructed instances that separate the student query from the reference.

    Only *failing* edge instances are counterexamples: an instance both queries
    agree on distinguishes nothing.
    """
    items: List[EvidenceItem] = []

    if isinstance(counterexample, dict) and counterexample:
        items.append(EvidenceItem("cex.minimal", "minimal_counterexample",
                                  counterexample))

    for case in failing_edge_instances(edge_cases):
        name = case.get("name") or "unnamed"
        items.append(EvidenceItem(
            id=f"cex.{name}",
            kind="constructed_instance",
            content={"instance": name,
                     "description": case.get("description"),
                     "tests": case.get("tests"),
                     "reference_rows": (case.get("base_result") or {}).get("rows"),
                     "student_rows": (case.get("student_result") or {}).get("rows")},
        ))
    return items


def _build_history(history: Optional[List[Dict[str, Any]]]) -> List[EvidenceItem]:
    """Prior attempts for the same learner and task. Optional per the draft."""
    items: List[EvidenceItem] = []
    for idx, attempt in enumerate(history or []):
        if not isinstance(attempt, dict):
            continue
        items.append(EvidenceItem(f"hist.attempt.{idx}", "prior_attempt", attempt))
    return items


# ══════════════════════════════════════════════════════════════════════
#  Stage 1 gate — when is the LLM invoked at all?
# ══════════════════════════════════════════════════════════════════════

def llm_invocation_reason(comparison: Dict[str, Any],
                          released: List[Dict[str, Any]],
                          withheld: List[Dict[str, Any]],
                          edge_cases: Optional[List[Dict[str, Any]]] = None,
                          query_failed: bool = False) -> Optional[str]:
    """Decide whether Stage 2 should run, per the draft:

        "Existing high-precision rules fire before the LLM ... The LLM is
        invoked for residual, ambiguous, multi-error, or narration cases
        rather than on every submission."

    Agreement is read over *every* instance that was executed, not the main
    database alone. The draft is explicit on both sides of this: the filter
    suppresses a flag only when the submission "agrees with the reference on
    the main database and a set of edge instances", and in the worked
    alternate-solution example the LLM "is never invoked" precisely because
    "execution and edge cases support the counting formulation". A submission
    that matches on the main instance but diverges on a constructed one has
    therefore not earned silence.

    `query_failed` short-circuits all of this: a submission that never ran has
    no row-level evidence for any hypothesis to cite.

    Returns the reason, or None when the deterministic core already answered
    and the LLM must not be consulted.
    """
    if query_failed:
        # The draft treats a parse or execution failure as a fact of its own:
        # "the LLM may explain them but cannot override them". There is nothing
        # to diagnose, so the only admissible role is to narrate the failure.
        return "narration"

    agrees_on_main = bool((comparison or {}).get("are_equivalent"))
    divergent_instances = failing_edge_instances(edge_cases)
    agrees_everywhere = agrees_on_main and not divergent_instances

    if not released:
        # Nothing to say, or something to say that the rules could not name.
        return None if agrees_everywhere else "residual"
    if len(released) > 1:
        return "multi_error"              # competing accounts to separate
    if withheld:
        return "ambiguous"                # a proposal was raised and gated
    if divergent_instances:
        return "narration"                # labelled, but the instance needs telling
    return None                           # single confident deterministic label

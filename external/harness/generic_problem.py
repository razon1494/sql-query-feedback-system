"""
Phase 0 — Generic, schema-agnostic problem harness.

The misconception detector in `backend/feedback_generator.generate_feedback`
is already purely *differential*: it compares a reference (gold) parse against
a student parse plus a result-comparison dict and a problem_type tag. It has no
dependency on the seven-table university schema. The only schema coupling in
the original project lives in:

  * backend/problems.py          (the 30 hardcoded university problems)
  * the five university edge DBs  (database/edge_*.db)
  * the division-only provenance trace

This module lifts a clean abstraction over that seam so the *same* pipeline can
run against an arbitrary SQLite database — in particular the Spider benchmark
databases — given only:

    (db_path, gold_sql, candidate_sql, problem_type)

Nothing in the detector changes. This is the foundation that lets Spider
(Phase 1+) plug into the existing evaluation.
"""
import os
import sys
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

# ── Make the repo root importable so `backend.*` resolves regardless of CWD ──
# external/harness/generic_problem.py -> external/harness -> external -> repo root
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from backend.sql_parser import parse_sql                       # noqa: E402
from backend.feedback_generator import generate_feedback        # noqa: E402
from backend.query_executor import execute_query, compare_results  # noqa: E402


VALID_PROBLEM_TYPES = {
    "DIVISION", "JOIN", "AGGREGATION", "SET_OP", "SUBQUERY", "NULL",
}


@dataclass
class GenericProblem:
    """A reference problem decoupled from any particular schema.

    `db_path` points at the SQLite instance the gold/candidate execute against
    (a university DB, a Spider DB, or a generated edge instance). `edge_db_paths`
    is optional: when supplied, the pipeline runs the latent-bug edge analysis
    against those instances; when empty, the single `db_path` instance is the
    only execution instance (which is the correct default for Spider, where the
    shipped database is the test instance).
    """
    problem_id: str
    db_path: str
    gold_sql: str
    problem_type: str = "JOIN"
    db_id: str = ""
    question: str = ""
    source: str = "unknown"          # 'university' | 'spider' | ...
    edge_db_paths: List[str] = field(default_factory=list)

    def __post_init__(self):
        pt = (self.problem_type or "").upper()
        if pt not in VALID_PROBLEM_TYPES:
            raise ValueError(
                f"problem_type {self.problem_type!r} not in {sorted(VALID_PROBLEM_TYPES)}"
            )
        self.problem_type = pt


@dataclass
class AnalysisResult:
    """The outcome of running one candidate query through the full pipeline."""
    problem_id: str
    db_id: str
    detected: List[str]              # user-facing misconception keys (post-filter)
    raw: List[str]                   # pre-filter shape-classifier keys
    are_equivalent: bool             # candidate output == gold output on db_path
    is_alternate_correct: bool
    total_score: int
    gold_exec_error: Optional[str]
    student_exec_error: Optional[str]
    edges_ok: bool = True            # all edge instances passed (or none ran)
    # Proposals withheld because their own predicted effect was absent from the
    # evidence, though the submission did diverge. Empty under a global filter.
    unsupported: List[str] = field(default_factory=list)
    report: Any = None               # full FeedbackReport (for deep inspection)

    def has(self, key: str) -> bool:
        return key in self.detected

    def raw_has(self, key: str) -> bool:
        return key in self.raw

    def to_dict(self) -> Dict[str, Any]:
        return {
            "problem_id": self.problem_id,
            "db_id": self.db_id,
            "detected": self.detected,
            "raw": self.raw,
            "are_equivalent": self.are_equivalent,
            "is_alternate_correct": self.is_alternate_correct,
            "total_score": self.total_score,
            "gold_exec_error": self.gold_exec_error,
            "student_exec_error": self.student_exec_error,
            "edges_ok": self.edges_ok,
        }


def _run_edges(problem: GenericProblem, candidate_sql: str) -> List[Dict]:
    """Run gold vs candidate over each optional edge instance.

    Produces the minimal shape `generate_feedback` consumes for the
    result-aware filter (it only reads `passed`; the extra fields make the
    feedback items render sensibly if inspected).
    """
    out: List[Dict] = []
    for i, edge_path in enumerate(problem.edge_db_paths):
        if not os.path.exists(edge_path):
            continue
        name = os.path.splitext(os.path.basename(edge_path))[0] or f"edge_{i}"
        g = execute_query(problem.gold_sql, db_path=edge_path, db_name=name)
        s = execute_query(candidate_sql, db_path=edge_path, db_name=name)
        comp = compare_results(g, s)
        out.append({
            "name": name,
            "title": name,
            "description": f"Generic edge instance {name}.",
            "tests": "",
            "base_result": g.to_dict(),
            "student_result": s.to_dict(),
            "comparison": comp.to_dict(),
            "passed": comp.are_equivalent,
        })
    return out


def analyze(problem: GenericProblem, candidate_sql: str,
            db_name: str = "instance") -> AnalysisResult:
    """Run one candidate query through the full feedback pipeline.

    Mirrors exactly what the deployed app does for a student submission, but
    parameterized on an arbitrary database and reference query.
    """
    gold_parse = parse_sql(problem.gold_sql)
    cand_parse = parse_sql(candidate_sql)

    gold_res = execute_query(problem.gold_sql, db_path=problem.db_path, db_name=db_name)
    cand_res = execute_query(candidate_sql, db_path=problem.db_path, db_name=db_name)

    gold_exec_error = gold_res.error if not gold_res.success else None
    student_exec_error = cand_res.error if not cand_res.success else None

    if gold_res.success and cand_res.success:
        comp = compare_results(gold_res, cand_res).to_dict()
    else:
        comp = {"are_equivalent": False, "missing_rows": [],
                "extra_rows": [], "jaccard_similarity": 0.0}

    edge_results = _run_edges(problem, candidate_sql) if problem.edge_db_paths else []
    edges_ok = not edge_results or all(e.get("passed") for e in edge_results)

    report = generate_feedback(
        base_parse=gold_parse,
        student_parse=cand_parse,
        comparison=comp,
        edge_results=edge_results,
        provenance_trace=None,                 # division provenance is university-only
        problem_type=problem.problem_type,
        execution_error=student_exec_error,
    )

    return AnalysisResult(
        problem_id=problem.problem_id,
        db_id=problem.db_id,
        detected=[m["key"] for m in report.misconceptions],
        raw=[m["key"] for m in report.raw_misconceptions],
        unsupported=[m["key"] for m in report.unsupported_misconceptions],
        are_equivalent=bool(comp.get("are_equivalent")),
        is_alternate_correct=report.is_alternate_correct,
        total_score=report.total_score,
        gold_exec_error=gold_exec_error,
        student_exec_error=student_exec_error,
        edges_ok=edges_ok,
        report=report,
    )


def analyze_pair(gold_sql: str, candidate_sql: str, db_path: str,
                 problem_type: str = "JOIN", problem_id: str = "adhoc",
                 db_id: str = "") -> AnalysisResult:
    """Convenience wrapper for one-off (gold, candidate) analysis."""
    problem = GenericProblem(
        problem_id=problem_id, db_path=db_path, gold_sql=gold_sql,
        problem_type=problem_type, db_id=db_id,
    )
    return analyze(problem, candidate_sql)

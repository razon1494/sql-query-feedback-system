"""
Feedback Generator — Produces graded, structured feedback from query analysis.
Combines: AST diff, result comparison, provenance trace, edge case results.
Inspired by the adaptive feedback framework in Arifur Rahman's proposal.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from backend.sql_parser import ParsedQuery, ASTDiff, compare_queries, queries_structurally_equal


# ══════════════════════════════════════════════════════════════════════
#  GRADING WEIGHTS
# ══════════════════════════════════════════════════════════════════════

WEIGHTS = {
    "syntax":     20,
    "logic":      30,
    "results":    40,
    "edge_cases": 10,
}


@dataclass
class GradeComponent:
    name: str
    score: int
    max_score: int
    notes: List[str] = field(default_factory=list)

    @property
    def pct(self):
        return round(self.score / self.max_score * 100)

    def to_dict(self):
        return {"name": self.name, "score": self.score,
                "max_score": self.max_score, "pct": self.pct, "notes": self.notes}


@dataclass
class FeedbackItem:
    level: str        # error | warning | info | success
    category: str     # SYNTAX | LOGIC | RESULT | EDGE_CASE | MISCONCEPTION
    title: str
    body: str
    suggestion: Optional[str] = None
    code_snippet: Optional[str] = None
    reference: Optional[str] = None   # paper citation

    def to_dict(self):
        return {
            "level": self.level,
            "category": self.category,
            "title": self.title,
            "body": self.body,
            "suggestion": self.suggestion,
            "code_snippet": self.code_snippet,
            "reference": self.reference,
        }


@dataclass
class FeedbackReport:
    total_score: int
    max_score: int = 100
    grade_letter: str = "F"
    is_alternate_correct: bool = False
    components: List[GradeComponent] = field(default_factory=list)
    items: List[FeedbackItem] = field(default_factory=list)
    misconceptions: List[Dict] = field(default_factory=list)
    summary: str = ""

    def to_dict(self):
        return {
            "total_score": self.total_score,
            "max_score": self.max_score,
            "grade_letter": self.grade_letter,
            "is_alternate_correct": self.is_alternate_correct,
            "components": [c.to_dict() for c in self.components],
            "items": [i.to_dict() for i in self.items],
            "misconceptions": self.misconceptions,
            "summary": self.summary,
        }


# ══════════════════════════════════════════════════════════════════════
#  MISCONCEPTION LIBRARY
# ══════════════════════════════════════════════════════════════════════

MISCONCEPTION_PATTERNS = {
    "IN_FOR_DIVISION": {
        "title": "Using IN (∃) Instead of NOT EXISTS (∀) for Division",
        "description": (
            "The IN operator tests for existential membership: it returns TRUE if at least one "
            "matching row exists. Relational division requires universal quantification: every "
            "element of the divisor set must be matched. Using IN gives partial matches (∃) "
            "instead of complete coverage (∀)."
        ),
        "fix": (
            "Use the double-negation NOT EXISTS pattern:\n"
            "WHERE NOT EXISTS (SELECT … FROM Divisor WHERE … NOT IN (SELECT … FROM Dividend WHERE …))\n"
            "This reads: 'no required course is missing from this student's enrollment.'"
        ),
        "reference": "Codd (1972) — Relational Completeness; Miao et al. (2019) RATest §3"
    },
    "MISSING_NOT_EXISTS": {
        "title": "Missing NOT EXISTS for Universal Quantification",
        "description": (
            "The query does not implement the 'for all' logic required by relational division. "
            "SQL does not have a FORALL quantifier, so division must be expressed via double negation."
        ),
        "fix": "Restructure using NOT EXISTS containing a subquery with NOT IN.",
        "reference": "Date (2003) — SQL and Relational Theory, Chapter 9"
    },
    "MISSING_HAVING": {
        "title": "GROUP BY Without HAVING (Aggregation Error)",
        "description": (
            "You used GROUP BY to group rows but forgot the HAVING clause to filter groups "
            "by aggregate condition. Without HAVING, all groups are returned regardless of count."
        ),
        "fix": "Add HAVING COUNT(DISTINCT CourseID) = (SELECT COUNT(*) FROM Courses WHERE Group='DB')",
        "reference": None
    },
    "HARDCODED_THRESHOLD": {
        "title": "Hardcoded COUNT Threshold in HAVING",
        "description": (
            "Using a hardcoded number (e.g., HAVING COUNT(...) >= 2) is brittle — it breaks "
            "whenever the number of DB courses changes. The correct approach computes the threshold dynamically."
        ),
        "fix": "Replace hardcoded value with: HAVING COUNT(DISTINCT t.CourseID) = (SELECT COUNT(*) FROM Courses WHERE \"Group\"='DB')",
        "reference": None
    },
    "WRONG_JOIN_TYPE": {
        "title": "Incorrect JOIN Type",
        "description": (
            "Using the wrong JOIN type can include NULL rows (OUTER JOIN) or exclude valid rows. "
            "For division queries, INNER JOIN is usually appropriate."
        ),
        "fix": "Verify whether INNER JOIN or a correlated subquery is more appropriate for your query intent.",
        "reference": None
    },
    "MISSING_CORRELATED_REF": {
        "title": "Missing Correlated Reference in Subquery",
        "description": (
            "The inner subquery must reference the outer query's student ID (e.g., WHERE t.StuID = s.StuID) "
            "to make it a correlated subquery. Without this, the subquery is evaluated once for all students."
        ),
        "fix": "Add the correlated reference: WHERE t.StuID = s.StuID inside the innermost subquery.",
        "reference": "I-REX (Miao et al., VLDB 2020) — Block-level tracing of correlated subqueries"
    },
}


# ══════════════════════════════════════════════════════════════════════
#  GRADING LOGIC
# ══════════════════════════════════════════════════════════════════════

def _grade_syntax(student_parse: ParsedQuery, execution_error: str = None) -> GradeComponent:
    if student_parse.error:
        return GradeComponent("Syntax", 0, WEIGHTS["syntax"],
                              notes=[f"Syntax error: {student_parse.error}"])
    if execution_error:
        return GradeComponent("Syntax", 0, WEIGHTS["syntax"],
                              notes=[f"Execution error: {execution_error}"])
    return GradeComponent("Syntax", WEIGHTS["syntax"], WEIGHTS["syntax"],
                          notes=["Query is syntactically valid."])


def _grade_logic(base_parse: ParsedQuery, student_parse: ParsedQuery,
                 diffs: List[ASTDiff], problem_type: str) -> GradeComponent:
    max_s = WEIGHTS["logic"]

    if student_parse.error:
        return GradeComponent("Logic", 0, max_s, notes=["Cannot assess logic: syntax error."])

    score = max_s
    notes = []

    # WHERE type match
    where_diff = next((d for d in diffs if d.path == "WHERE.type"), None)
    if where_diff:
        if where_diff.base_value == "NOT_EXISTS":
            if where_diff.student_value == "IN":
                score -= 18
                notes.append("Critical: Using IN (existential) instead of NOT EXISTS (universal). "
                              "This is the fundamental division logic error.")
            elif where_diff.student_value == "NOT_IN":
                score -= 8
                notes.append("WHERE uses NOT IN — may be correct but less robust than NOT EXISTS for correlated cases.")
            elif where_diff.student_value is None or where_diff.student_value == "SIMPLE":
                score -= 20
                notes.append("No subquery quantification detected. Division pattern is missing entirely.")
        elif where_diff.base_value == "NOT_IN" and where_diff.student_value != "NOT_IN":
            score -= 10

    # GROUP BY / HAVING
    gb_diff = next((d for d in diffs if d.path == "GROUP_BY"), None)
    hv_diff = next((d for d in diffs if d.path == "HAVING"), None)
    if gb_diff and gb_diff.diff_type == "MISSING":
        score -= 10
        notes.append("Missing GROUP BY clause.")
    if hv_diff and hv_diff.diff_type == "MISSING":
        score -= 8
        notes.append("Missing HAVING clause.")

    # Subquery depth
    sq_diff = next((d for d in diffs if d.path == "SUBQUERY.depth"), None)
    if sq_diff and sq_diff.base_value > sq_diff.student_value:
        score -= 5
        notes.append(f"Base query uses {sq_diff.base_value} subquery level(s); student uses {sq_diff.student_value}.")

    if not notes:
        notes.append("Query structure matches the reference closely.")

    score = max(0, min(score, max_s))
    return GradeComponent("Logic", score, max_s, notes=notes)


def _grade_results(comparison: Dict) -> GradeComponent:
    max_s = WEIGHTS["results"]
    if comparison.get("are_equivalent"):
        return GradeComponent("Results", max_s, max_s,
                              notes=["Query output exactly matches the reference."])

    jaccard = comparison.get("jaccard_similarity", 0.0)
    score = round(jaccard * max_s)
    missing = len(comparison.get("missing_rows", []))
    extra   = len(comparison.get("extra_rows", []))
    notes = []
    if extra:
        notes.append(f"{extra} extra (incorrect) row(s) in your output.")
    if missing:
        notes.append(f"{missing} row(s) from the expected output are missing.")
    if not notes:
        notes.append("Partial match.")

    return GradeComponent("Results", score, max_s, notes=notes)


def _grade_edge_cases(edge_results: List[Dict]) -> GradeComponent:
    max_s = WEIGHTS["edge_cases"]
    if not edge_results:
        return GradeComponent("Edge Cases", 0, max_s, notes=["No edge case data available."])

    passed = sum(1 for e in edge_results if e.get("passed"))
    total  = len(edge_results)
    score  = round((passed / total) * max_s)
    return GradeComponent("Edge Cases", score, max_s,
                           notes=[f"{passed}/{total} edge cases passed."])


def _score_to_letter(score: int) -> str:
    if score >= 93: return "A"
    if score >= 90: return "A-"
    if score >= 87: return "B+"
    if score >= 83: return "B"
    if score >= 80: return "B-"
    if score >= 77: return "C+"
    if score >= 70: return "C"
    if score >= 60: return "D"
    return "F"


# ══════════════════════════════════════════════════════════════════════
#  FEEDBACK ITEM GENERATION
# ══════════════════════════════════════════════════════════════════════

def _generate_feedback_items(base_parse, student_parse, diffs,
                              comparison, edge_results, provenance_trace,
                              problem_type, **kwargs) -> List[FeedbackItem]:
    items = []

    # ── Syntax ──
    exec_err = kwargs.get("execution_error")
    if student_parse.error:
        items.append(FeedbackItem(
            level="error", category="SYNTAX",
            title="Syntax Error Detected",
            body=f"{student_parse.error}",
            suggestion="Check your SQL keywords carefully. Common mistakes: 'FRM' instead of 'FROM', 'SELCT' instead of 'SELECT', unmatched parentheses, or unclosed quotes."
        ))
        return items
    if exec_err:
        items.append(FeedbackItem(
            level="error", category="SYNTAX",
            title="SQL Execution Error",
            body=f"{exec_err}",
            suggestion="Your query could not run. Check for misspelled table names, column names, or keywords."
        ))
        return items
    items.append(FeedbackItem(
        level="success", category="SYNTAX",
        title="Syntax Correct",
        body="Your query is syntactically valid and can be executed without errors."
    ))

    # ── WHERE type / Division logic ──
    where_diff = next((d for d in diffs if d.path == "WHERE.type"), None)
    if where_diff:
        bv, sv = where_diff.base_value, where_diff.student_value
        if bv == "NOT_EXISTS" and sv == "IN":
            items.append(FeedbackItem(
                level="error", category="LOGIC",
                title="Division Logic Error: IN vs NOT EXISTS",
                body=(
                    "Your query uses the IN operator, which tests for existential membership: "
                    "'Is this student enrolled in at least one DB course?' (∃). "
                    "The correct logic for relational division requires universal quantification: "
                    "'Is this student enrolled in ALL DB courses?' (∀). "
                    "This means Nancy (enrolled only in CS360) is incorrectly included by your query "
                    "because she took at least one DB course."
                ),
                suggestion="Replace your IN-based condition with the NOT EXISTS double-negation pattern.",
                code_snippet=(
                    "-- Correct pattern (reads: 'no DB course is missing'):\n"
                    "WHERE NOT EXISTS (\n"
                    "  SELECT c.CourseID FROM Courses c\n"
                    "  WHERE c.\"Group\" = 'DB'\n"
                    "  AND c.CourseID NOT IN (\n"
                    "    SELECT t.CourseID FROM Takes t\n"
                    "    WHERE t.StuID = s.StuID   -- correlated reference!\n"
                    "  )\n"
                    ")"
                ),
                reference="Miao et al. (2019) RATest, SIGMOD — §3 Division Query Testing"
            ))
        elif bv == "NOT_EXISTS" and sv == "NOT_IN":
            items.append(FeedbackItem(
                level="warning", category="LOGIC",
                title="NOT IN Used — May Work But NOT EXISTS Preferred",
                body=(
                    "NOT IN can implement division but fails when NULLs are present in the subquery result, "
                    "because NULL comparisons in SQL are three-valued (TRUE/FALSE/UNKNOWN). "
                    "NOT EXISTS correctly handles NULLs."
                ),
                suggestion="Prefer NOT EXISTS over NOT IN for correlated subqueries to avoid NULL-related bugs.",
                reference="Date (2003) — SQL and Relational Theory, Chapter 6"
            ))
        elif bv == "NOT_EXISTS" and sv in (None, "SIMPLE"):
            items.append(FeedbackItem(
                level="error", category="LOGIC",
                title="Division Pattern Completely Missing",
                body=(
                    "Your query does not implement the 'for all' (∀) logic required by relational division. "
                    "SQL lacks a FORALL quantifier, so division must be expressed via double negation: "
                    "NOT EXISTS (… NOT IN …)."
                ),
                suggestion="Restructure your query to use the NOT EXISTS pattern for universal quantification.",
                code_snippet=(
                    "-- Template for relational division:\n"
                    "SELECT s.StuID, s.Name FROM Students s\n"
                    "WHERE NOT EXISTS (\n"
                    "  SELECT 1 FROM Courses c\n"
                    "  WHERE c.\"Group\" = 'DB'\n"
                    "  AND NOT EXISTS (\n"
                    "    SELECT 1 FROM Takes t\n"
                    "    WHERE t.StuID = s.StuID AND t.CourseID = c.CourseID\n"
                    "  )\n"
                    ");"
                ),
                reference="Codd (1972) — Relational Completeness of Data Base Sublanguages"
            ))

    # ── GROUP BY / HAVING ──
    hv_diff = next((d for d in diffs if d.path == "HAVING"), None)
    gb_diff = next((d for d in diffs if d.path == "GROUP_BY"), None)
    if hv_diff and hv_diff.diff_type == "MISSING" and student_parse.group_by:
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title="GROUP BY Without HAVING",
            body="You grouped rows but didn't filter the groups. Without HAVING, all groups are returned.",
            suggestion="Add: HAVING COUNT(DISTINCT t.CourseID) = (SELECT COUNT(*) FROM Courses WHERE \"Group\"='DB')"
        ))

    # ── Results ──
    missing = comparison.get("missing_rows", [])
    extra   = comparison.get("extra_rows", [])

    if extra:
        names = [r.get("Name") or r.get("StuID", str(r)) for r in extra[:3]]
        items.append(FeedbackItem(
            level="error", category="RESULT",
            title=f"{len(extra)} Extra Row(s) in Your Output",
            body=(
                f"Your query incorrectly returns: {', '.join(names)}. "
                f"These students are included because your condition accepts partial enrollment "
                f"(enrolled in at least one DB course). The correct condition requires "
                f"complete coverage of all DB courses."
            ),
            suggestion="These are your counterexample — trace why each student passes your condition but shouldn't."
        ))

    if missing:
        names = [r.get("Name") or r.get("StuID", str(r)) for r in missing[:3]]
        items.append(FeedbackItem(
            level="error", category="RESULT",
            title=f"{len(missing)} Missing Row(s) from Expected Output",
            body=f"Your query is missing: {', '.join(names)}, who should be in the result.",
        ))

    if not extra and not missing:
        items.append(FeedbackItem(
            level="success", category="RESULT",
            title="Output Matches Reference",
            body=(
                "Your query produces the correct output on this database instance! "
                "Note: verify edge cases (especially empty divisor set) to ensure "
                "this is not a coincidental match."
            )
        ))

    # ── Provenance ──
    if provenance_trace and provenance_trace.get("divergence_points"):
        for dp in provenance_trace["divergence_points"][:2]:
            items.append(FeedbackItem(
                level="info", category="PROVENANCE",
                title=f"Provenance Divergence: {dp.get('name', dp.get('student', ''))}",
                body=dp.get("explanation", ""),
                reference="I-REX (Miao et al., VLDB 2020) — Block-level SQL tracing"
            ))

    # ── Edge cases ──
    failed_edge = [e for e in edge_results if not e.get("passed")]
    if failed_edge:
        for e in failed_edge[:3]:
            items.append(FeedbackItem(
                level="warning", category="EDGE_CASE",
                title=f"Edge Case Failed: {e['title']}",
                body=(
                    f"{e['description']} "
                    f"Base query returned {e['base_result']['row_count']} row(s); "
                    f"your query returned {e['student_result']['row_count']} row(s). "
                    f"{e['tests']}"
                ),
                reference="RATest (Miao et al., SIGMOD 2019) — Minimal counterexample generation"
            ))
    else:
        if edge_results:
            items.append(FeedbackItem(
                level="success", category="EDGE_CASE",
                title="All Edge Cases Passed",
                body=f"Your query correctly handles all {len(edge_results)} edge case scenarios."
            ))

    return items


# ══════════════════════════════════════════════════════════════════════
#  MISCONCEPTION DETECTION
# ══════════════════════════════════════════════════════════════════════

def _detect_misconceptions(base_parse: ParsedQuery,
                            student_parse: ParsedQuery,
                            diffs: List[ASTDiff]) -> List[Dict]:
    found = []
    su = student_parse.raw.upper()

    # IN for division
    if base_parse.where_type == "NOT_EXISTS" and student_parse.where_type == "IN":
        m = MISCONCEPTION_PATTERNS["IN_FOR_DIVISION"]
        found.append({"key": "IN_FOR_DIVISION", **m})

    # Missing NOT EXISTS
    elif base_parse.where_type == "NOT_EXISTS" and student_parse.where_type not in ("NOT_EXISTS", "NOT_IN"):
        m = MISCONCEPTION_PATTERNS["MISSING_NOT_EXISTS"]
        found.append({"key": "MISSING_NOT_EXISTS", **m})

    # GROUP BY without HAVING
    if student_parse.group_by and not student_parse.having:
        m = MISCONCEPTION_PATTERNS["MISSING_HAVING"]
        found.append({"key": "MISSING_HAVING", **m})

    # Hardcoded threshold
    import re
    if student_parse.having and re.search(r'COUNT\s*\(.*?\)\s*(>=|=|>)\s*\d+', student_parse.having, re.IGNORECASE):
        m = MISCONCEPTION_PATTERNS["HARDCODED_THRESHOLD"]
        found.append({"key": "HARDCODED_THRESHOLD", **m})

    # Missing correlated reference
    if base_parse.where_type == "NOT_EXISTS" and student_parse.where_type == "NOT_EXISTS":
        if "S.STUID" not in su and "STU.STUID" not in su:
            # Check if correlated ref is missing in nested subquery
            if student_parse.subqueries:
                innermost = student_parse.subqueries[0]
                if innermost.subqueries:
                    deepest = innermost.subqueries[0]
                    if not deepest.where_clause or "STUID" not in (deepest.where_clause or "").upper():
                        m = MISCONCEPTION_PATTERNS["MISSING_CORRELATED_REF"]
                        found.append({"key": "MISSING_CORRELATED_REF", **m})

    return found


# ══════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def generate_feedback(
    base_parse: ParsedQuery,
    student_parse: ParsedQuery,
    comparison: Dict,
    edge_results: List[Dict],
    provenance_trace: Optional[Dict],
    problem_type: str = "DIVISION",
    execution_error: str = None
) -> FeedbackReport:
    """
    Generate a complete FeedbackReport from all analysis components.
    """
    diffs = compare_queries(base_parse, student_parse)
    is_struct_equal = queries_structurally_equal(base_parse, student_parse)

    # Grade components
    syntax_grade  = _grade_syntax(student_parse, execution_error)
    logic_grade   = _grade_logic(base_parse, student_parse, diffs, problem_type)
    results_grade = _grade_results(comparison)
    edge_grade    = _grade_edge_cases(edge_results)

    total = syntax_grade.score + logic_grade.score + results_grade.score + edge_grade.score

    # Alternate correct solution detection
    is_alt_correct = (
        comparison.get("are_equivalent") and
        not is_struct_equal and
        all(e.get("passed") for e in edge_results)
    )
    if is_alt_correct:
        total = min(total + 5, 100)   # bonus for creative correct solution

    # Feedback items
    items = _generate_feedback_items(
        base_parse, student_parse, diffs,
        comparison, edge_results, provenance_trace, problem_type,
        execution_error=execution_error
    )

    # Misconceptions
    misconceptions = _detect_misconceptions(base_parse, student_parse, diffs)

    # Summary
    if student_parse.error:
        summary = "Your query has a syntax error and could not be evaluated."
    elif is_alt_correct:
        summary = (
            f"Excellent! Your query produces the correct output and passes all edge cases. "
            f"While structurally different from the reference, it is recognized as an alternate correct solution. "
            f"Score: {total}/100."
        )
    elif comparison.get("are_equivalent"):
        summary = (
            f"Your query produces the correct output on the main database. "
            f"Check edge cases to ensure full correctness. Score: {total}/100."
        )
    else:
        err_count = len([i for i in items if i.level == "error"])
        summary = (
            f"Your query has {err_count} error(s) to address. "
            f"The primary issue is {diffs[0].path if diffs else 'structural'} mismatch. "
            f"Score: {total}/100. Review the feedback items below."
        )

    return FeedbackReport(
        total_score=total,
        grade_letter=_score_to_letter(total),
        is_alternate_correct=is_alt_correct,
        components=[syntax_grade, logic_grade, results_grade, edge_grade],
        items=items,
        misconceptions=misconceptions,
        summary=summary,
    )

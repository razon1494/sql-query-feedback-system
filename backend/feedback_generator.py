"""
Feedback Generator — Produces graded, structured feedback from query analysis.
Combines: AST diff, result comparison, provenance trace, edge case results.

Misconception taxonomy (M1-M10) grounded in:
  - Miedema, Aivaloglou & Fletcher (ICER 2021)
  - Miedema, Aivaloglou & Fletcher (ACM TOCE 2022)
  - Taipalus, Siponen & Vartiainen (ACM TOCE 2018)
  - Brass & Goldberg (JSS 2006)
  - Miao, Roy & Yang (SIGMOD 2019 / VLDB 2020)
"""
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from backend.sql_parser import ParsedQuery, ASTDiff, compare_queries, queries_structurally_equal


# ======================================================================
#  GRADING WEIGHTS
# ======================================================================

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
    level: str
    category: str
    title: str
    body: str
    suggestion: Optional[str] = None
    code_snippet: Optional[str] = None
    reference: Optional[str] = None

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


# ======================================================================
#  MISCONCEPTION LIBRARY  (M1 - M10)
# ======================================================================

MISCONCEPTION_PATTERNS = {

    "MISSING_WHERE": {
        "id": "M1",
        "title": "Missing or Incomplete WHERE Condition",
        "description": (
            "The query omits a required filter predicate, causing it to return "
            "too many rows — often the entire table."
        ),
        "fix": (
            "Add the missing WHERE condition matching the problem requirements."
        ),
        "reference": "Brass & Goldberg (2006) JSS; Taipalus et al. (2018) TOCE LOG-4"
    },

    "IN_VS_EXISTS": {
        "id": "M2",
        "title": "IN vs EXISTS Confusion",
        "description": (
            "The query uses IN where EXISTS (or vice versa) is required. "
            "IN tests membership in a fixed list. EXISTS evaluates a correlated "
            "subquery per outer row. They are not interchangeable."
        ),
        "fix": (
            "Use EXISTS when the subquery references the outer query. "
            "Use IN for non-correlated membership tests."
        ),
        "reference": "Miedema et al. (ICER 2021) -- generalization-based misconception"
    },

    "NOT_IN_VS_NOT_EXISTS": {
        "id": "M3",
        "title": "NOT IN vs NOT EXISTS Confusion (NULL Safety)",
        "description": (
            "NOT IN silently returns zero rows when the subquery contains NULLs "
            "because SQL three-valued logic returns UNKNOWN, not FALSE. "
            "NOT EXISTS handles NULLs correctly."
        ),
        "fix": "Replace NOT IN with NOT EXISTS for NULL-safe negation.",
        "reference": "Miedema et al. (ICER 2021) -- language-based misconception"
    },

    "WRONG_JOIN_TYPE": {
        "id": "M4",
        "title": "Wrong JOIN Type (INNER vs OUTER)",
        "description": (
            "The query uses the wrong JOIN type. INNER JOIN discards unmatched rows. "
            "LEFT/RIGHT OUTER JOIN preserves them with NULLs. "
            "Using the wrong type silently changes the result set."
        ),
        "fix": (
            "Check whether unmatched rows must be preserved (LEFT JOIN) "
            "or only matched rows are needed (INNER JOIN)."
        ),
        "reference": "Miedema et al. (ICER 2021) -- incomplete mental model of JOIN"
    },

    "CARTESIAN_PRODUCT": {
        "id": "M5",
        "title": "Missing Join Condition (Implicit Cartesian Product)",
        "description": (
            "Multiple tables appear in FROM without a linking condition, "
            "producing a Cartesian product: every row paired with every other row."
        ),
        "fix": "Add a JOIN or WHERE condition linking the tables on their related keys.",
        "reference": "Brass & Goldberg (2006) JSS Error 20; Taipalus et al. (2018) TOCE SEM-3"
    },

    "MISSING_GROUP_BY": {
        "id": "M6",
        "title": "Missing GROUP BY Clause",
        "description": (
            "An aggregate function is used without GROUP BY, collapsing all rows "
            "into one result instead of computing per-group values."
        ),
        "fix": "Add GROUP BY listing all non-aggregated columns in the SELECT list.",
        "reference": "Miedema et al. (ICER 2021) -- primary aggregation error; Taipalus et al. (2018) SYN-5"
    },

    "MISSING_HAVING": {
        "id": "M7",
        "title": "HAVING vs WHERE Confusion -- Missing HAVING",
        "description": (
            "The query uses GROUP BY but lacks the HAVING clause needed to filter "
            "groups by an aggregate condition. All groups are returned without filtering."
        ),
        "fix": "Add HAVING to filter groups after aggregation. Example: HAVING COUNT(t.CourseID) > 1",
        "reference": "Miedema et al. (TOCE 2022) -- HAVING vs WHERE confusion"
    },

    "HARDCODED_THRESHOLD": {
        "id": "M7",
        "title": "Hardcoded Threshold in HAVING",
        "description": (
            "A literal number in HAVING (e.g., HAVING COUNT(*) >= 2) makes the query "
            "brittle. If the data changes, the query silently breaks."
        ),
        "fix": (
            "Replace the literal with a subquery. "
            "Example: HAVING COUNT(DISTINCT t.CourseID) = "
            "(SELECT COUNT(*) FROM Courses WHERE \"Group\" = 'DB')"
        ),
        "reference": "Taipalus et al. (2018) TOCE LOG-4"
    },

    "IN_FOR_DIVISION": {
        "id": "M8",
        "title": "IN-FOR-DIVISION: Using IN instead of NOT EXISTS",
        "description": (
            "IN tests existential membership (at least one match). "
            "Relational division requires universal quantification (all must match). "
            "Using IN includes students who took only some required courses."
        ),
        "fix": (
            "Use the NOT EXISTS double-negation pattern:\n"
            "WHERE NOT EXISTS (\n"
            "  SELECT CourseID FROM Courses WHERE Group='DB'\n"
            "  AND CourseID NOT IN (\n"
            "    SELECT CourseID FROM Takes WHERE StuID = s.StuID\n"
            "  )\n"
            ")"
        ),
        "reference": "Miao et al. (SIGMOD 2019) RATest; Miao et al. (VLDB 2020) I-Rex"
    },

    "MISSING_NOT_EXISTS": {
        "id": "M8",
        "title": "Missing NOT EXISTS for Universal Quantification",
        "description": (
            "The query does not implement 'for all' logic. "
            "SQL has no FORALL quantifier -- division requires NOT EXISTS (... NOT IN ...)."
        ),
        "fix": "Restructure using NOT EXISTS containing a subquery with NOT IN.",
        "reference": "Miao et al. (SIGMOD 2019) RATest; Miao et al. (VLDB 2020) I-Rex"
    },

    "MISSING_CORRELATED_REF": {
        "id": "M9",
        "title": "Missing Correlated Reference in Subquery",
        "description": (
            "The innermost subquery has no condition linking it to the outer query. "
            "Without correlation, the subquery runs once for the whole database "
            "instead of once per outer row."
        ),
        "fix": "Add a correlated reference. Example: WHERE t.StuID = s.StuID",
        "reference": "Miedema et al. (ICER 2021) -- subquery nesting errors; Miao et al. (VLDB 2020) I-Rex"
    },

    "WRONG_SET_OP": {
        "id": "M10",
        "title": "Set Operation Misuse (UNION / INTERSECT / EXCEPT)",
        "description": (
            "The wrong set operation is used. "
            "UNION = OR logic. INTERSECT = AND logic. EXCEPT = NOT logic."
        ),
        "fix": (
            "A OR B -> UNION | A AND B -> INTERSECT | A but NOT B -> EXCEPT"
        ),
        "reference": "Miedema et al. (ICER 2021) -- language-based misconception on set semantics"
    },
}

# Aggregate function regex used in multiple places
_AGG_RE = re.compile(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', re.IGNORECASE)
_HARDCODE_RE = re.compile(
    r'(COUNT|SUM|AVG|MIN|MAX)\s*\(.*?\)\s*(>=|<=|=|>|<)\s*\d+',
    re.IGNORECASE)
_JOIN_PRED_RE = re.compile(r'\w+\.\w+\s*=\s*\w+\.\w+', re.IGNORECASE)


# ======================================================================
#  GRADING LOGIC
# ======================================================================

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

    # WHERE type mismatch
    where_diff = next((d for d in diffs if d.path == "WHERE.type"), None)
    if where_diff:
        bv, sv = where_diff.base_value, where_diff.student_value
        if bv == "NOT_EXISTS":
            if sv == "IN":
                score -= 18
                notes.append("Critical: IN used instead of NOT EXISTS (division logic error).")
            elif sv == "NOT_IN":
                score -= 8
                notes.append("NOT IN used -- may fail with NULLs; prefer NOT EXISTS.")
            elif sv in (None, "SIMPLE"):
                score -= 20
                notes.append("Division pattern entirely absent.")
        elif bv in ("IN", "EXISTS") and sv != bv:
            score -= 8
            notes.append(f"Expected {bv} pattern but student used {sv}.")

    # Missing WHERE (M1)
    if base_parse.where_clause and not student_parse.where_clause:
        score -= 15
        notes.append("WHERE clause missing entirely.")

    # GROUP BY / HAVING (M6, M7)
    gb_diff = next((d for d in diffs if d.path == "GROUP_BY"), None)
    hv_diff = next((d for d in diffs if d.path == "HAVING"), None)
    if gb_diff and gb_diff.diff_type == "MISSING":
        score -= 10
        notes.append("Missing GROUP BY -- aggregate functions require grouping.")
    if hv_diff and hv_diff.diff_type == "MISSING":
        score -= 8
        notes.append("Missing HAVING -- group filter absent.")

    # Subquery depth (M9)
    sq_diff = next((d for d in diffs if d.path == "SUBQUERY.depth"), None)
    if sq_diff and sq_diff.base_value > sq_diff.student_value:
        score -= 5
        notes.append(f"Reference uses {sq_diff.base_value} subquery level(s); "
                     f"student uses {sq_diff.student_value}.")

    # JOIN count mismatch (M4, M5)
    join_diff = next((d for d in diffs if d.path == "JOINS.count"), None)
    if join_diff and join_diff.base_value > join_diff.student_value:
        score -= 8
        notes.append(f"Missing {join_diff.base_value - join_diff.student_value} JOIN(s).")

    # Set operation mismatch (M10)
    set_diff = next((d for d in diffs if d.path == "SET_OPERATION"), None)
    if set_diff and set_diff.base_value != set_diff.student_value:
        score -= 12
        notes.append(f"Wrong set op: reference={set_diff.base_value}, "
                     f"student={set_diff.student_value}.")

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
        notes.append(f"{extra} extra (incorrect) row(s) in output.")
    if missing:
        notes.append(f"{missing} expected row(s) missing.")
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


# ======================================================================
#  MISCONCEPTION DETECTION  (M1 - M10)
# ======================================================================

def _has_cartesian_product(student_parse: ParsedQuery) -> bool:
    """True if student lists 2+ tables with no explicit join condition."""
    if len(student_parse.from_tables) < 2:
        return False
    if student_parse.joins:
        return False
    # Check WHERE for an implicit join predicate (tbl1.col = tbl2.col)
    wc = student_parse.where_clause or ""
    return not bool(_JOIN_PRED_RE.search(wc))


def _find_deepest_subquery(pq: ParsedQuery) -> Optional[ParsedQuery]:
    """Return the innermost ParsedQuery (deepest subquery level)."""
    if not pq.subqueries:
        return pq
    return _find_deepest_subquery(pq.subqueries[0])


def _detect_misconceptions(base_parse: ParsedQuery,
                            student_parse: ParsedQuery,
                            diffs: List[ASTDiff],
                            problem_type: str = "DIVISION") -> List[Dict]:
    """
    Detect all applicable misconceptions M1-M10.
    Works across all problem types (DIVISION, JOIN, AGGREGATION, SET_OP, SUBQUERY, NULL).
    Returns a list of matched misconception dicts.
    """
    found = []
    su = student_parse.raw.upper()
    bu = base_parse.raw.upper()

    # -- M1: Missing WHERE Condition ----------------------------------
    if base_parse.where_clause and not student_parse.where_clause:
        found.append({"key": "MISSING_WHERE", **MISCONCEPTION_PATTERNS["MISSING_WHERE"]})
    elif (base_parse.where_clause and student_parse.where_clause and
          len(student_parse.where_clause.strip()) <
          len(base_parse.where_clause.strip()) * 0.3):
        found.append({"key": "MISSING_WHERE", **MISCONCEPTION_PATTERNS["MISSING_WHERE"]})

    # -- M2: IN vs EXISTS Confusion (non-division only) ---------------
    if problem_type != "DIVISION":
        base_exists = "EXISTS" in bu and "NOT EXISTS" not in bu
        stu_in      = student_parse.where_type == "IN"
        base_in     = base_parse.where_type == "IN"
        stu_exists  = "EXISTS" in su and "NOT EXISTS" not in su
        if (base_exists and stu_in) or (base_in and stu_exists):
            found.append({"key": "IN_VS_EXISTS",
                          **MISCONCEPTION_PATTERNS["IN_VS_EXISTS"]})

    # -- M3: NOT IN vs NOT EXISTS -------------------------------------
    if (base_parse.where_type == "NOT_EXISTS" and
            student_parse.where_type == "NOT_IN"):
        found.append({"key": "NOT_IN_VS_NOT_EXISTS",
                      **MISCONCEPTION_PATTERNS["NOT_IN_VS_NOT_EXISTS"]})

    # -- M4: Wrong JOIN Type ------------------------------------------
    base_join_types = {j.get("type", "").upper() for j in base_parse.joins}
    stu_join_types  = {j.get("type", "").upper() for j in student_parse.joins}
    base_outer = any("LEFT" in t or "RIGHT" in t or "OUTER" in t for t in base_join_types)
    stu_outer  = any("LEFT" in t or "RIGHT" in t or "OUTER" in t for t in stu_join_types)
    stu_inner  = bool(student_parse.joins) and not stu_outer

    if base_outer and stu_inner:
        found.append({"key": "WRONG_JOIN_TYPE",
                      **MISCONCEPTION_PATTERNS["WRONG_JOIN_TYPE"]})
    elif not base_outer and stu_outer and base_parse.joins:
        found.append({"key": "WRONG_JOIN_TYPE",
                      **MISCONCEPTION_PATTERNS["WRONG_JOIN_TYPE"]})

    # -- M5: Cartesian Product ----------------------------------------
    if _has_cartesian_product(student_parse):
        # Only flag if reference does have explicit joins or a linking WHERE
        base_has_link = bool(base_parse.joins) or bool(
            _JOIN_PRED_RE.search(base_parse.where_clause or ""))
        if base_has_link:
            found.append({"key": "CARTESIAN_PRODUCT",
                          **MISCONCEPTION_PATTERNS["CARTESIAN_PRODUCT"]})

    # -- M6: Missing GROUP BY -----------------------------------------
    stu_has_agg = bool(_AGG_RE.search(student_parse.raw))
    if stu_has_agg and not student_parse.group_by and base_parse.group_by:
        found.append({"key": "MISSING_GROUP_BY",
                      **MISCONCEPTION_PATTERNS["MISSING_GROUP_BY"]})

    # -- M7a: Missing HAVING ------------------------------------------
    if (student_parse.group_by and
            not student_parse.having and
            base_parse.having):
        found.append({"key": "MISSING_HAVING",
                      **MISCONCEPTION_PATTERNS["MISSING_HAVING"]})

    # -- M7b: Hardcoded Threshold in HAVING ---------------------------
    if student_parse.having and _HARDCODE_RE.search(student_parse.having):
        found.append({"key": "HARDCODED_THRESHOLD",
                      **MISCONCEPTION_PATTERNS["HARDCODED_THRESHOLD"]})

    # -- M7c: Aggregate in WHERE (WHERE used instead of HAVING) -------
    if (student_parse.where_clause and
            _AGG_RE.search(student_parse.where_clause) and
            not any(m["key"] == "MISSING_HAVING" for m in found)):
        found.append({"key": "MISSING_HAVING",
                      **MISCONCEPTION_PATTERNS["MISSING_HAVING"]})

    # -- M8: IN-FOR-DIVISION / Missing NOT EXISTS ---------------------
    if problem_type == "DIVISION" and base_parse.where_type == "NOT_EXISTS":
        if student_parse.where_type == "IN":
            found.append({"key": "IN_FOR_DIVISION",
                          **MISCONCEPTION_PATTERNS["IN_FOR_DIVISION"]})
        elif student_parse.where_type not in ("NOT_EXISTS", "NOT_IN"):
            found.append({"key": "MISSING_NOT_EXISTS",
                          **MISCONCEPTION_PATTERNS["MISSING_NOT_EXISTS"]})

    # -- M9: Missing Correlated Reference -----------------------------
    if (base_parse.where_type == "NOT_EXISTS" and
            student_parse.where_type == "NOT_EXISTS" and
            student_parse.subqueries):
        deepest = _find_deepest_subquery(student_parse)
        if deepest and deepest is not student_parse:
            wc = (deepest.where_clause or "").upper()
            if not _JOIN_PRED_RE.search(wc):
                found.append({"key": "MISSING_CORRELATED_REF",
                              **MISCONCEPTION_PATTERNS["MISSING_CORRELATED_REF"]})

    # -- M10: Set Operation Misuse ------------------------------------
    set_diff = next((d for d in diffs if d.path == "SET_OPERATION"), None)
    if set_diff:
        if set_diff.base_value and set_diff.base_value != set_diff.student_value:
            found.append({"key": "WRONG_SET_OP",
                          **MISCONCEPTION_PATTERNS["WRONG_SET_OP"]})

    return found


# ======================================================================
#  FEEDBACK ITEM GENERATION
# ======================================================================

def _generate_feedback_items(base_parse, student_parse, diffs,
                              comparison, edge_results, provenance_trace,
                              problem_type, **kwargs) -> List[FeedbackItem]:
    items = []
    exec_err = kwargs.get("execution_error")

    # -- Syntax -------------------------------------------------------
    if student_parse.error:
        items.append(FeedbackItem(
            level="error", category="SYNTAX",
            title="Syntax Error Detected",
            body=student_parse.error,
            suggestion=(
                "Check SQL keywords carefully. Common mistakes: "
                "'FRM' instead of 'FROM', 'SELCT' instead of 'SELECT', "
                "unmatched parentheses, unclosed quotes."
            )
        ))
        return items
    if exec_err:
        items.append(FeedbackItem(
            level="error", category="SYNTAX",
            title="SQL Execution Error",
            body=exec_err,
            suggestion="Check table names, column names, and keyword spelling."
        ))
        return items
    items.append(FeedbackItem(
        level="success", category="SYNTAX",
        title="Syntax Correct",
        body="Your query is syntactically valid and executes without errors."
    ))

    # -- M1: Missing WHERE --------------------------------------------
    if base_parse.where_clause and not student_parse.where_clause:
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title="Missing WHERE Clause (M1)",
            body=(
                "Your query has no WHERE clause but the reference requires one. "
                "Without a filter the query returns all rows from the table."
            ),
            suggestion="Add a WHERE clause to filter rows per the problem conditions.",
            reference="Brass & Goldberg (2006) JSS; Taipalus et al. (2018) TOCE LOG-4"
        ))

    # -- M2/M3/M8: WHERE type / Division logic ------------------------
    where_diff = next((d for d in diffs if d.path == "WHERE.type"), None)
    if where_diff:
        bv, sv = where_diff.base_value, where_diff.student_value

        if bv == "NOT_EXISTS" and sv == "IN":
            items.append(FeedbackItem(
                level="error", category="LOGIC",
                title="Division Logic Error: IN vs NOT EXISTS (M8)",
                body=(
                    "IN tests existential membership (at least one match -- EXISTS). "
                    "Division requires universal quantification (ALL must match -- FORALL). "
                    "Students who took only some required courses pass your IN test incorrectly."
                ),
                suggestion="Replace IN with the NOT EXISTS double-negation pattern.",
                code_snippet=(
                    "-- Correct NOT EXISTS pattern:\n"
                    "WHERE NOT EXISTS (\n"
                    "  SELECT c.CourseID FROM Courses c\n"
                    "  WHERE c.\"Group\" = 'DB'\n"
                    "  AND c.CourseID NOT IN (\n"
                    "    SELECT t.CourseID FROM Takes t\n"
                    "    WHERE t.StuID = s.StuID  -- correlated reference\n"
                    "  )\n"
                    ")"
                ),
                reference="Miao et al. (SIGMOD 2019) RATest; Miao et al. (VLDB 2020) I-Rex"
            ))
        elif bv == "NOT_EXISTS" and sv == "NOT_IN":
            items.append(FeedbackItem(
                level="warning", category="LOGIC",
                title="NOT IN Used -- NOT EXISTS Preferred (M3)",
                body=(
                    "NOT IN fails silently when the subquery contains NULLs: "
                    "SQL three-valued logic returns UNKNOWN instead of FALSE, "
                    "causing zero rows to be returned. NOT EXISTS handles NULLs correctly."
                ),
                suggestion="Replace NOT IN with NOT EXISTS for NULL-safe universal quantification.",
                reference="Miedema et al. (ICER 2021) -- language-based misconception"
            ))
        elif bv == "NOT_EXISTS" and sv in (None, "SIMPLE"):
            items.append(FeedbackItem(
                level="error", category="LOGIC",
                title="Universal Quantification Pattern Missing (M8)",
                body=(
                    "Your query has no 'for all' logic. "
                    "SQL has no FORALL quantifier -- use NOT EXISTS (... NOT IN ...)."
                ),
                suggestion="Restructure using NOT EXISTS containing a NOT IN subquery.",
                code_snippet=(
                    "-- Division template:\n"
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
                reference="Miao et al. (SIGMOD 2019) RATest"
            ))
        elif bv in ("IN", "EXISTS") and sv != bv and problem_type != "DIVISION":
            items.append(FeedbackItem(
                level="warning", category="LOGIC",
                title="IN vs EXISTS Confusion (M2)",
                body=(
                    f"Reference uses {bv}; your query uses {sv}. "
                    "IN tests a fixed list (non-correlated). "
                    "EXISTS tests a correlated subquery per outer row."
                ),
                suggestion=f"Review whether {bv} or {sv} is correct for this query.",
                reference="Miedema et al. (ICER 2021) -- generalization-based misconception"
            ))

    # -- M5: Cartesian Product ----------------------------------------
    if _has_cartesian_product(student_parse):
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title="Implicit Cartesian Product (M5)",
            body=(
                "Multiple tables in FROM with no linking condition produce a "
                "Cartesian product: every row from one table paired with every row "
                "from the other -- almost never the intended result."
            ),
            suggestion="Add a JOIN condition. Example: JOIN Takes t ON s.StuID = t.StuID",
            reference="Brass & Goldberg (2006) JSS Error 20; Taipalus et al. (2018) TOCE SEM-3"
        ))

    # -- M6: Missing GROUP BY -----------------------------------------
    gb_diff = next((d for d in diffs if d.path == "GROUP_BY"), None)
    if gb_diff and gb_diff.diff_type == "MISSING":
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title="Missing GROUP BY Clause (M6)",
            body=(
                "An aggregate function is used without GROUP BY. "
                "This collapses all rows into one result instead of per-group values."
            ),
            suggestion="Add GROUP BY listing all non-aggregated SELECT columns.",
            reference="Miedema et al. (ICER 2021); Taipalus et al. (2018) TOCE SYN-5"
        ))

    # -- M7: HAVING vs WHERE ------------------------------------------
    hv_diff = next((d for d in diffs if d.path == "HAVING"), None)
    if hv_diff and hv_diff.diff_type == "MISSING" and student_parse.group_by:
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title="GROUP BY Without HAVING (M7)",
            body=(
                "Groups are formed correctly but not filtered. "
                "HAVING filters groups after aggregation (the GROUP BY equivalent of WHERE)."
            ),
            suggestion="Add a HAVING clause. Example: HAVING COUNT(t.CourseID) > 1",
            reference="Miedema et al. (TOCE 2022)"
        ))

    if student_parse.having and _HARDCODE_RE.search(student_parse.having):
        items.append(FeedbackItem(
            level="warning", category="LOGIC",
            title="Hardcoded Threshold in HAVING (M7)",
            body=(
                "A literal number in HAVING breaks whenever data changes. "
                "Compute the threshold dynamically."
            ),
            suggestion=(
                "Example: HAVING COUNT(DISTINCT t.CourseID) = "
                "(SELECT COUNT(*) FROM Courses WHERE \"Group\" = 'DB')"
            ),
            reference="Taipalus et al. (2018) TOCE LOG-4"
        ))

    # -- M9: Missing Correlated Reference -----------------------------
    if (base_parse.where_type == "NOT_EXISTS" and
            student_parse.where_type == "NOT_EXISTS" and
            student_parse.subqueries):
        deepest = _find_deepest_subquery(student_parse)
        if deepest and deepest is not student_parse:
            wc = (deepest.where_clause or "").upper()
            if not _JOIN_PRED_RE.search(wc):
                items.append(FeedbackItem(
                    level="error", category="LOGIC",
                    title="Missing Correlated Reference in Subquery (M9)",
                    body=(
                        "The innermost subquery has no condition linking it to the outer query. "
                        "Without correlation, the subquery runs once for the entire database "
                        "instead of once per outer row."
                    ),
                    suggestion="Add: WHERE t.StuID = s.StuID inside the innermost subquery.",
                    reference="Miedema et al. (ICER 2021); Miao et al. (VLDB 2020) I-Rex"
                ))

    # -- M10: Set Operation Misuse ------------------------------------
    set_diff = next((d for d in diffs if d.path == "SET_OPERATION"), None)
    if set_diff and set_diff.base_value != set_diff.student_value:
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title="Wrong Set Operation (M10)",
            body=(
                f"Reference uses {set_diff.base_value}; "
                f"your query uses {set_diff.student_value or 'no set operation'}. "
                "UNION=OR, INTERSECT=AND, EXCEPT=NOT."
            ),
            suggestion=(
                "A OR B -> UNION | A AND B -> INTERSECT | A but NOT B -> EXCEPT"
            ),
            reference="Miedema et al. (ICER 2021) -- set operation semantics"
        ))

    # -- Results ------------------------------------------------------
    missing = comparison.get("missing_rows", [])
    extra   = comparison.get("extra_rows", [])

    if extra:
        names = [r.get("Name") or r.get("StuID", str(r)) for r in extra[:3]]
        items.append(FeedbackItem(
            level="error", category="RESULT",
            title=f"{len(extra)} Extra Row(s) in Your Output",
            body=(
                f"Your query incorrectly returns: {', '.join(str(n) for n in names)}. "
                "These rows pass your condition but should not appear in the result."
            ),
            suggestion="Trace why each extra row satisfies your WHERE clause when it should not."
        ))

    if missing:
        names = [r.get("Name") or r.get("StuID", str(r)) for r in missing[:3]]
        items.append(FeedbackItem(
            level="error", category="RESULT",
            title=f"{len(missing)} Missing Row(s) from Expected Output",
            body=f"Your query is missing: {', '.join(str(n) for n in names)}."
        ))

    if not extra and not missing:
        items.append(FeedbackItem(
            level="success", category="RESULT",
            title="Output Matches Reference",
            body=(
                "Your query produces the correct output on the main database. "
                "Verify edge cases to confirm this is not a coincidental match."
            )
        ))

    # -- Provenance ---------------------------------------------------
    if provenance_trace and provenance_trace.get("divergence_points"):
        for dp in provenance_trace["divergence_points"][:2]:
            items.append(FeedbackItem(
                level="info", category="PROVENANCE",
                title=f"Provenance Divergence: {dp.get('name', dp.get('student', ''))}",
                body=dp.get("explanation", ""),
                reference="I-REX (Miao et al., VLDB 2020)"
            ))

    # -- Edge Cases ---------------------------------------------------
    failed_edge = [e for e in edge_results if not e.get("passed")]
    if failed_edge:
        for e in failed_edge[:3]:
            items.append(FeedbackItem(
                level="warning", category="EDGE_CASE",
                title=f"Edge Case Failed: {e['title']}",
                body=(
                    f"{e['description']} "
                    f"Base: {e['base_result']['row_count']} row(s); "
                    f"yours: {e['student_result']['row_count']} row(s). "
                    f"{e['tests']}"
                ),
                reference="RATest (Miao et al., SIGMOD 2019)"
            ))
    elif edge_results:
        items.append(FeedbackItem(
            level="success", category="EDGE_CASE",
            title="All Edge Cases Passed",
            body=f"Your query correctly handles all {len(edge_results)} edge case scenarios."
        ))

    return items


# ======================================================================
#  MAIN ENTRY POINT
# ======================================================================

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
    Detects misconceptions M1-M10 across all problem types.
    """
    diffs = compare_queries(base_parse, student_parse)
    is_struct_equal = queries_structurally_equal(base_parse, student_parse)

    syntax_grade  = _grade_syntax(student_parse, execution_error)
    logic_grade   = _grade_logic(base_parse, student_parse, diffs, problem_type)
    results_grade = _grade_results(comparison)
    edge_grade    = _grade_edge_cases(edge_results)

    total = syntax_grade.score + logic_grade.score + results_grade.score + edge_grade.score

    is_alt_correct = (
        comparison.get("are_equivalent") and
        not is_struct_equal and
        all(e.get("passed") for e in edge_results)
    )
    if is_alt_correct:
        total = min(total + 5, 100)

    items = _generate_feedback_items(
        base_parse, student_parse, diffs,
        comparison, edge_results, provenance_trace, problem_type,
        execution_error=execution_error
    )

    misconceptions = _detect_misconceptions(
        base_parse, student_parse, diffs, problem_type
    )

    if student_parse.error:
        summary = "Your query has a syntax error and could not be evaluated."
    elif is_alt_correct:
        summary = (
            f"Excellent! Your query produces the correct output and passes all edge cases. "
            f"Recognized as an alternate correct solution. Score: {total}/100."
        )
    elif comparison.get("are_equivalent"):
        summary = (
            f"Your query produces the correct output. "
            f"Check edge cases to ensure full correctness. Score: {total}/100."
        )
    else:
        mc_labels = [m.get("id", m["key"]) for m in misconceptions]
        mc_str = f" Detected: {', '.join(mc_labels)}." if mc_labels else ""
        err_count = len([i for i in items if i.level == "error"])
        summary = (
            f"Your query has {err_count} error(s) to fix.{mc_str} "
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
"""
Feedback Generator — Produces graded, structured feedback from query analysis.
Combines: AST diff, result comparison, provenance trace, edge case results.

v2: Problem-type-aware misconception detection aligned with the 10-type
taxonomy (M1-M10) + NULL handling.

Taxonomy coverage
-----------------
  M1  MISSING_WHERE              — missing filter predicate
  M2  IN_vs_EXISTS               — IN used where EXISTS expected
  M3  NOT_IN_vs_NOT_EXISTS       — NOT IN used where NOT EXISTS expected
  M4  WRONG_JOIN_TYPE            — INNER vs OUTER mismatch
  M5  CARTESIAN_PRODUCT          — multiple tables without join condition
  M6  MISSING_GROUP_BY           — aggregate in SELECT without GROUP BY
  M7  HAVING_vs_WHERE            — post-aggregation filter in wrong clause
  M8  IN_FOR_DIVISION            — IN used for relational division
  M9  CORRELATED_SCOPE           — subquery missing correlated reference
  M10 WRONG_SET_OP               — wrong set operator (UNION/INTERSECT/EXCEPT)
  +   NULL_EQUALITY              — `= NULL` instead of `IS NULL`
"""
import re
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


# ══════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════

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
    category: str     # SYNTAX | LOGIC | RESULT | EDGE_CASE | MISCONCEPTION | PROVENANCE
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
    raw_misconceptions: List[Dict] = field(default_factory=list)  # pre-filter, for research eval
    # Proposals that survived the global filter but whose own predicted effect
    # was absent from the evidence. Retained instructor-side, and the quantity
    # the diagnosis-specific gate exists to measure.
    unsupported_misconceptions: List[Dict] = field(default_factory=list)
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
            "raw_misconceptions": self.raw_misconceptions,
            "unsupported_misconceptions": self.unsupported_misconceptions,
            "summary": self.summary,
        }


# ══════════════════════════════════════════════════════════════════════
#  MISCONCEPTION LIBRARY
# ══════════════════════════════════════════════════════════════════════

MISCONCEPTION_PATTERNS = {
    # ── M8: Division-specific ───────────────────────────────────────
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
            "WHERE NOT EXISTS (SELECT … FROM Divisor d WHERE d.x NOT IN (SELECT … FROM Dividend WHERE …))\n"
            "Read this as: 'no required element is missing from the covering set.'"
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
            "GROUP BY was used to group rows, but the HAVING clause that filters groups "
            "by aggregate condition is missing. Without HAVING, every group is returned."
        ),
        "fix": "Add a HAVING clause with the aggregate predicate (for example, HAVING COUNT(*) >= (SELECT COUNT(*) FROM Divisor)).",
        "reference": None
    },
    "HARDCODED_THRESHOLD": {
        "title": "Hardcoded Aggregate Threshold",
        "description": (
            "A literal numeric threshold was used in a HAVING or WHERE comparison (e.g. HAVING COUNT(*) >= 2). "
            "For division-by-count queries this is brittle — the value breaks whenever the divisor set changes. "
            "The correct formulation computes the threshold dynamically with a subquery."
        ),
        "fix": "Replace the literal with a scalar subquery, e.g. HAVING COUNT(DISTINCT t.CourseID) = (SELECT COUNT(*) FROM Courses WHERE \"Group\"='DB').",
        "reference": None
    },

    # ── M4 & JOIN-related ───────────────────────────────────────────
    "WRONG_JOIN_TYPE": {
        "title": "Incorrect JOIN Type",
        "description": (
            "The join type differs from the reference (for example, LEFT JOIN where INNER JOIN is expected). "
            "LEFT/RIGHT OUTER joins preserve unmatched rows and can introduce NULL-filled rows that "
            "invalidate aggregates and filters designed for INNER join semantics."
        ),
        "fix": "Match the reference join type. For most row-preserving relational queries, INNER JOIN is the correct choice.",
        "reference": None
    },
    "MISSING_JOIN": {
        "title": "Missing JOIN — Required Table Not Included",
        "description": (
            "The reference query joins additional tables that are absent from your query. "
            "Without the missing join, predicates over the absent table cannot be evaluated and "
            "the result rows will be incomplete."
        ),
        "fix": "Add the missing JOIN (with an ON condition on the shared key) for every table referenced in the reference query that is not yet in yours.",
        "reference": None
    },

    # ── M5 ──────────────────────────────────────────────────────────
    "CARTESIAN_PRODUCT": {
        "title": "Cartesian Product — Missing Join Condition",
        "description": (
            "Two or more tables appear in FROM without a connecting join condition (either a JOIN … ON … "
            "or an equi-join predicate in WHERE). The result is the full cartesian product: every row of "
            "table A paired with every row of table B."
        ),
        "fix": "Add a join condition linking the tables — either `A JOIN B ON A.key = B.key` or a WHERE predicate `A.key = B.key`.",
        "reference": None
    },

    # ── M9 ──────────────────────────────────────────────────────────
    "MISSING_CORRELATED_REF": {
        "title": "Missing Correlated Reference in Subquery",
        "description": (
            "The subquery does not reference any column from the outer query, so it is evaluated once "
            "(as a constant) rather than per outer row. Division and existential patterns require the "
            "inner subquery to be correlated with the outer tuple."
        ),
        "fix": "Inside the innermost subquery, add an equality against the outer alias — e.g. WHERE t.StuID = s.StuID.",
        "reference": "I-REX (Miao et al., VLDB 2020) — Block-level tracing of correlated subqueries"
    },

    # ── M1 ──────────────────────────────────────────────────────────
    "MISSING_WHERE": {
        "title": "Missing WHERE Filter",
        "description": (
            "The reference query constrains rows with a WHERE predicate that is absent from your query. "
            "Without it, every row of the source tables is returned instead of the intended subset."
        ),
        "fix": "Add the WHERE clause that restricts the output to the rows the problem describes.",
        "reference": None
    },

    # ── M2 ──────────────────────────────────────────────────────────
    "IN_vs_EXISTS": {
        "title": "IN Used Where EXISTS Is Expected",
        "description": (
            "An IN subquery was written where a correlated EXISTS subquery is expected. "
            "IN is uncorrelated and tests membership in a pre-computed set; EXISTS is correlated and "
            "evaluates the subquery per outer row. The two have different semantics under NULLs and "
            "different efficiency characteristics."
        ),
        "fix": "Rewrite as WHERE EXISTS (SELECT 1 FROM … WHERE outer.key = inner.key AND …).",
        "reference": None
    },

    # ── M3 ──────────────────────────────────────────────────────────
    "NOT_IN_vs_NOT_EXISTS": {
        "title": "NOT IN Used Where NOT EXISTS Is Expected",
        "description": (
            "NOT IN fails silently when the subquery returns any NULL: the predicate evaluates to "
            "UNKNOWN for every outer row and the whole query returns the empty set. NOT EXISTS is "
            "NULL-safe and is the preferred pattern for set difference and universal quantification."
        ),
        "fix": "Replace `WHERE x NOT IN (SELECT y FROM …)` with `WHERE NOT EXISTS (SELECT 1 FROM … WHERE y = x)`.",
        "reference": "Date (2003) — SQL and Relational Theory, Chapter 6"
    },

    # ── M6 ──────────────────────────────────────────────────────────
    "MISSING_GROUP_BY": {
        "title": "Aggregate in SELECT Without GROUP BY",
        "description": (
            "The SELECT list mixes an aggregate function with non-aggregate columns, but no GROUP BY "
            "clause is present. Under standard SQL semantics, every non-aggregate column in SELECT must "
            "appear in GROUP BY."
        ),
        "fix": "Add GROUP BY listing every non-aggregate column from the SELECT clause.",
        "reference": None
    },

    # ── M7 ──────────────────────────────────────────────────────────
    "HAVING_vs_WHERE": {
        "title": "HAVING vs WHERE Confusion",
        "description": (
            "The filter clause is placed in the wrong position. WHERE filters individual rows BEFORE "
            "aggregation; HAVING filters entire groups AFTER aggregation. Aggregate predicates "
            "(COUNT, SUM, AVG, …) must go in HAVING; row-level predicates should stay in WHERE."
        ),
        "fix": "Move aggregate comparisons to HAVING, and keep row-level filters in WHERE.",
        "reference": None
    },

    # ── M10 ─────────────────────────────────────────────────────────
    "WRONG_SET_OP": {
        "title": "Incorrect Set Operator",
        "description": (
            "The reference query combines two result sets with a specific set operator "
            "(UNION for 'in either', INTERSECT for 'in both', EXCEPT for 'in A but not B'). "
            "Your query uses a different operator, so the combined set does not match the intended semantics."
        ),
        "fix": "Identify the intended semantics — union, intersection, or difference — and use the matching operator.",
        "reference": None
    },
    "MISSING_SET_OP": {
        "title": "Missing Set Operator",
        "description": (
            "The reference query uses a set operator (UNION / INTERSECT / EXCEPT) to combine two "
            "SELECT blocks, but your query contains only a single SELECT block."
        ),
        "fix": "Write two SELECT queries, one per branch, and combine them with the appropriate set operator.",
        "reference": None
    },

    # ── NULL handling ───────────────────────────────────────────────
    "NULL_EQUALITY": {
        "title": "Using '= NULL' Instead of IS NULL",
        "description": (
            "In SQL's three-valued logic, any comparison with NULL using `=` or `!=` evaluates to "
            "UNKNOWN, and rows filtered by UNKNOWN are excluded. `column = NULL` therefore always "
            "returns zero rows regardless of the data."
        ),
        "fix": "Use the IS NULL / IS NOT NULL predicates instead of = / != comparisons with NULL.",
        "reference": None
    },
}


# Penalty each misconception deducts from the 30-point Logic component.
MISCONCEPTION_PENALTIES = {
    "IN_FOR_DIVISION":       18,
    "MISSING_NOT_EXISTS":    20,
    "MISSING_HAVING":         8,
    "HARDCODED_THRESHOLD":    6,
    "WRONG_JOIN_TYPE":        8,
    "MISSING_JOIN":          10,
    "CARTESIAN_PRODUCT":     15,
    "MISSING_CORRELATED_REF":12,
    "MISSING_WHERE":         12,
    "IN_vs_EXISTS":          15,
    "NOT_IN_vs_NOT_EXISTS":   8,
    "MISSING_GROUP_BY":      12,
    "HAVING_vs_WHERE":       10,
    "WRONG_SET_OP":          15,
    "MISSING_SET_OP":        18,
    "NULL_EQUALITY":         10,
}


# ══════════════════════════════════════════════════════════════════════
#  HELPER PREDICATES
# ══════════════════════════════════════════════════════════════════════

_AGG_RE = re.compile(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', re.IGNORECASE)
_HARDCODED_RE = re.compile(
    r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\([^)]*\)\s*(>=|<=|<>|!=|=|>|<)\s*\d+(\.\d+)?',
    re.IGNORECASE,
)
_EQUIJOIN_RE = re.compile(r'\b\w+\.\w+\s*=\s*\w+\.\w+')
_NULL_EQ_RE  = re.compile(r'(=|!=|<>)\s*NULL\b', re.IGNORECASE)
_RAW_JOIN_RE = re.compile(
    r'\b(INNER|LEFT|RIGHT|FULL|CROSS)\s+(?:OUTER\s+)?JOIN\b|\bJOIN\b',
    re.IGNORECASE,
)


def _extract_join_types_raw(sql: Optional[str]) -> set:
    """
    Extract the set of JOIN types used anywhere in the raw SQL — including
    inside set-op branches and nested subqueries. This is needed because
    sql_parser.parse_sql() early-returns for SET_OP queries with empty
    `joins`, and deeply nested JOINs do not surface on the top-level
    ParsedQuery either.
    """
    types = set()
    for m in _RAW_JOIN_RE.finditer(sql or ""):
        tok = m.group(0).upper()
        if tok == "JOIN":
            types.add("INNER")
        else:
            first = tok.split()[0]
            types.add(first)
    return types


def _has_aggregate(text: Optional[str]) -> bool:
    """Return True if the text contains a SQL aggregate function call."""
    return bool(text) and bool(_AGG_RE.search(text))


def _has_hardcoded_threshold(text: Optional[str]) -> bool:
    """Detect an aggregate compared against a numeric literal."""
    return bool(text) and bool(_HARDCODED_RE.search(text))


def _normalize_join_type(jt: str) -> str:
    """Normalize 'JOIN' to 'INNER', uppercase everything, strip 'OUTER'."""
    if not jt:
        return "INNER"
    tokens = [t for t in jt.upper().split() if t not in ("OUTER", "JOIN")]
    return tokens[0] if tokens else "INNER"


def _has_cartesian_product(parse: ParsedQuery) -> bool:
    """
    Detect a cartesian product among the FROM tables.

    Heuristic: for N ≥ 2 tables, every table (or alias) must participate in
    at least one equi-join predicate (`a.x = b.y`) in WHERE. A single
    equi-join predicate among, say, 5 comma-joined tables still leaves the
    other tables cartesian — the original "any equi-join ⇒ no cartesian"
    check missed those cases.
    """
    if len(parse.from_tables) < 2:
        return False
    w = parse.where_clause or ""

    # Collect alias → table tokens from the raw FROM chunk. We look at
    # comma-separated entries in the FROM text up to the first WHERE /
    # GROUP / HAVING / ORDER.
    m = re.search(r'\bFROM\b(.+?)(?:\bWHERE\b|\bGROUP\b|\bHAVING\b|\bORDER\b|$)',
                  parse.raw or "", re.IGNORECASE | re.DOTALL)
    from_chunk = m.group(1) if m else ""
    # Split only at top-level commas (no regex depth-tracking needed for
    # simple comma-joins — JOIN … ON … cannot contain commas at this layer).
    aliases = []
    for part in from_chunk.split(','):
        part = part.strip()
        # Drop any trailing JOIN…ON… that belongs to the first table's
        # explicit JOIN chain (present when `A JOIN B ON …, C` is written).
        part = re.split(r'\b(INNER|LEFT|RIGHT|FULL|CROSS|NATURAL)?\s*JOIN\b',
                         part, maxsplit=1, flags=re.IGNORECASE)[0].strip()
        if not part:
            continue
        toks = re.split(r'\s+(?:AS\s+)?', part, flags=re.IGNORECASE)
        toks = [t for t in toks if t]
        if len(toks) >= 2:
            aliases.append(toks[-1])   # alias
        elif toks:
            aliases.append(toks[0])    # bare table name (its own alias)

    # If fewer than 2 comma-separated FROM entries, no comma-pair cartesian.
    if len(aliases) < 2:
        return False

    # Every alias must appear on at least one side of an equi-join predicate.
    equi_pattern = r'\b({al})\.\w+\s*=\s*\w+\.\w+|\b\w+\.\w+\s*=\s*({al})\.\w+'
    for alias in aliases:
        pat = equi_pattern.format(al=re.escape(alias))
        if not re.search(pat, w, re.IGNORECASE):
            return True  # alias never linked — cartesian on this alias
    return False


def _block_own_predicate_text(parse: ParsedQuery) -> str:
    """Return the text of a query block's *own* predicates, with the text of
    any nested subqueries masked out.

    `where_clause` for a block includes the raw text of its nested
    subqueries. To decide whether *this specific block* is correlated to an
    enclosing scope, the nested subquery text must be removed first —
    otherwise an alias reference belonging to a deeper block would be
    miscredited to this one. Parenthesised regions (depth >= 1) are blanked.
    """
    wc = parse.where_clause or ""
    out = []
    depth = 0
    for ch in wc:
        if ch == '(':
            depth += 1
            out.append(' ')
        elif ch == ')':
            if depth > 0:
                depth -= 1
            out.append(' ')
        elif depth > 0:
            out.append(' ')
        else:
            out.append(ch)
    return ''.join(out)


def _block_defined_aliases(parse: ParsedQuery) -> set:
    """Aliases (and bare table names) introduced in this block's own FROM."""
    names = set()
    for t in parse.from_tables:
        names.add(t.upper())
    # Pull aliases from the FROM section of this block's own raw text only.
    m = re.search(r'\bFROM\b(.+?)(?:\bWHERE\b|\bGROUP\b|\bHAVING\b|\bORDER\b|\(|$)',
                  parse.raw or "", re.IGNORECASE | re.DOTALL)
    if m:
        chunk = m.group(1)
        for alias in re.findall(r'\b\w+\s+(?:AS\s+)?(\w+)\b', chunk, re.IGNORECASE):
            if len(alias) <= 4:
                names.add(alias.upper())
    return names


def _innermost_block_chain(parse: ParsedQuery) -> List[ParsedQuery]:
    """Return the chain of blocks from the outer query down to the deepest
    nested subquery, following the most-deeply-nested branch at each level.

    The deepest block is the one that performs the innermost correlated
    test (the anti-join / division core); it is the block whose correlation
    the M9 misconception breaks.
    """
    chain = [parse]
    cur = parse
    while cur.subqueries:
        cur = cur.subqueries[-1]
        chain.append(cur)
    return chain


def _block_ancestor_refs(chain: List[ParsedQuery], idx: int) -> set:
    """For the block at position `idx` in a block chain, return the set of
    ancestor-scope aliases it references in its own predicates.

    Ancestor-scope aliases are those defined by every block above `idx`.
    A reference to one of them is a correlation from this block back into
    an enclosing scope. Returns an empty set for the outermost block
    (idx 0), which has no ancestors.
    """
    if idx <= 0 or idx >= len(chain):
        return set()
    ancestor_aliases = set()
    for blk in chain[:idx]:
        ancestor_aliases |= _block_defined_aliases(blk)
    own_text = _block_own_predicate_text(chain[idx]).upper()
    referenced = set()
    for name in ancestor_aliases:
        if re.search(rf'\b{re.escape(name)}\.\w+', own_text):
            referenced.add(name)
    return referenced


def _ancestor_aliases_referenced(chain: List[ParsedQuery]) -> set:
    """Ancestor-scope aliases referenced by the innermost block of a chain.

    Thin wrapper over `_block_ancestor_refs` for the deepest block; kept
    as a named helper because the innermost block is the single most
    diagnostic level for the IN-for-division / anti-join correlation.
    """
    if len(chain) < 2:
        return set()
    return _block_ancestor_refs(chain, len(chain) - 1)


def _correlation_scope_broken(base: ParsedQuery, student: ParsedQuery) -> bool:
    """Comparative M9 check: returns True when *any* nested block in the
    student query has lost a correlation that the corresponding block in
    the reference query carries.

    The earlier absolute helper asked only "is there a correlation
    somewhere?", which a complex query satisfies even after one of several
    correlations is broken — a non-broken correlation at another nesting
    level masks the loss. This check instead walks the reference and
    student block chains in parallel, level by level, and compares the set
    of ancestor-scope aliases each block references. If at any matching
    level the student block references a proper subset of what the
    reference block does, a correlation has been dropped at that level and
    M9 is flagged. Checking every level (not only the innermost) catches
    correlations broken in middle blocks of deeply nested division
    queries, which a single-level check would miss.
    """
    base_chain = _innermost_block_chain(base)
    student_chain = _innermost_block_chain(student)

    # No nested structure to correlate — nothing to check.
    if len(base_chain) < 2 or len(student_chain) < 2:
        return False

    # Compare level by level over the depth both chains share. Levels that
    # exist in only one chain are not comparable and are skipped; the
    # shared prefix is where a dropped correlation shows up as a shrunken
    # ancestor-reference set.
    common_depth = min(len(base_chain), len(student_chain))
    for idx in range(1, common_depth):
        base_refs = _block_ancestor_refs(base_chain, idx)
        student_refs = _block_ancestor_refs(student_chain, idx)
        if not base_refs:
            # Reference block carries no correlation at this level — there
            # is no requirement to violate here.
            continue
        if student_refs < base_refs:
            # Student block references strictly fewer ancestor scopes than
            # the reference block at the same level: correlation dropped.
            return True
    return False


def _has_correlated_ref(parse: ParsedQuery) -> bool:
    """
    Absolute structural check, retained for callers that only have the
    student query in hand: returns True when the innermost subquery block
    references at least one alias from an enclosing scope.

    This answers "does the innermost block carry *any* correlation?" — it
    is intentionally permissive. Detecting a *partially* broken correlation
    in a deeply nested query requires comparison against the reference and
    is handled by `_correlation_scope_broken`. Queries with no subqueries
    return False, preserving the original contract.
    """
    if not parse.subqueries or not parse.from_tables:
        return False
    chain = _innermost_block_chain(parse)
    return len(_ancestor_aliases_referenced(chain)) > 0


def _aggregate_in_where(parse: ParsedQuery) -> bool:
    """M7 shape: aggregate appears in WHERE clause."""
    return _has_aggregate(parse.where_clause)


def _has_bare_aggregate_without_group_by(parse: ParsedQuery) -> bool:
    """M6: SELECT mixes an aggregate with a non-aggregate column, no GROUP BY."""
    if parse.group_by:
        return False
    has_agg = any(_has_aggregate(c) for c in parse.select_cols)
    has_non_agg = any(
        c.strip() != "*" and not _has_aggregate(c)
        for c in parse.select_cols
    )
    return has_agg and has_non_agg


def _has_null_equality(parse: ParsedQuery) -> bool:
    """Detect `= NULL` / `!= NULL` anywhere in the query text."""
    return bool(_NULL_EQ_RE.search(parse.raw or ""))


# ══════════════════════════════════════════════════════════════════════
#  MISCONCEPTION DETECTION — PROBLEM-TYPE DISPATCH
# ══════════════════════════════════════════════════════════════════════

def _detect_division(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    found = []
    if student.where_type == "IN":
        found.append("IN_FOR_DIVISION")
    elif student.where_type == "NOT_IN" and base.where_type == "NOT_EXISTS":
        found.append("NOT_IN_vs_NOT_EXISTS")
    elif student.where_type in (None, "SIMPLE", "EXISTS"):
        found.append("MISSING_NOT_EXISTS")

    if student.where_type == "NOT_EXISTS" and (
        not _has_correlated_ref(student)
        or _correlation_scope_broken(base, student)
    ):
        found.append("MISSING_CORRELATED_REF")

    if _has_hardcoded_threshold(student.having) or _has_hardcoded_threshold(student.where_clause):
        found.append("HARDCODED_THRESHOLD")

    if student.group_by and not student.having and student.where_type != "NOT_EXISTS":
        found.append("MISSING_HAVING")

    return found


def _detect_join(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    found = []
    # Structural checks (CARTESIAN_PRODUCT, MISSING_JOIN, WRONG_JOIN_TYPE)
    # are handled by _detect_common and thus fire for every problem_type.
    if base.where_clause and not student.where_clause:
        found.append("MISSING_WHERE")
    return found


def _detect_aggregation(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    found = []
    if _has_bare_aggregate_without_group_by(student):
        found.append("MISSING_GROUP_BY")

    if _aggregate_in_where(student):
        found.append("HAVING_vs_WHERE")

    if base.having and not student.having:
        found.append("MISSING_HAVING")

    if base.having and student.having and not _has_aggregate(student.having):
        if "HAVING_vs_WHERE" not in found:
            found.append("HAVING_vs_WHERE")

    if base.where_clause and not student.where_clause:
        found.append("MISSING_WHERE")

    if base.group_by and not student.group_by and "MISSING_GROUP_BY" not in found:
        found.append("MISSING_GROUP_BY")

    return found


def _detect_setop(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    found = []
    if base.set_operation and not student.set_operation:
        found.append("MISSING_SET_OP")
        return found
    if base.set_operation and student.set_operation:
        base_op = base.set_operation.upper().replace(" ALL", "")
        student_op = student.set_operation.upper().replace(" ALL", "")
        if base_op != student_op:
            found.append("WRONG_SET_OP")
    return found


def _detect_subquery(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    found = []
    if base.where_type == "EXISTS" and student.where_type == "IN":
        found.append("IN_vs_EXISTS")
    if base.where_type == "NOT_EXISTS" and student.where_type == "NOT_IN":
        found.append("NOT_IN_vs_NOT_EXISTS")
    if base.where_type == "NOT_EXISTS" and student.where_type in ("IN", "SIMPLE", None):
        if student.where_type == "IN":
            if "IN_vs_EXISTS" not in found:
                found.append("IN_vs_EXISTS")
        elif student.where_type in ("SIMPLE", None):
            found.append("MISSING_NOT_EXISTS")
    if base.where_type in ("EXISTS", "NOT_EXISTS") and student.subqueries:
        if not _has_correlated_ref(student) or _correlation_scope_broken(base, student):
            found.append("MISSING_CORRELATED_REF")
    if base.where_clause and not student.where_clause:
        found.append("MISSING_WHERE")
    return found


def _detect_null(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    found = []
    if _has_null_equality(student):
        found.append("NULL_EQUALITY")
    if base.where_clause and not student.where_clause:
        found.append("MISSING_WHERE")
    return found


def _detect_common(base: ParsedQuery, student: ParsedQuery) -> List[str]:
    """Structural checks that apply regardless of problem_type."""
    found = []
    if _has_null_equality(student):
        found.append("NULL_EQUALITY")
    if _has_cartesian_product(student):
        found.append("CARTESIAN_PRODUCT")

    # Wrong join type — applies whenever both queries have explicit JOINs.
    # Uses a raw-SQL scan so the check reaches JOINs nested inside set-op
    # branches (UNION/INTERSECT/EXCEPT) or subqueries, which the parser's
    # top-level `joins` field does not surface.
    base_join_types = _extract_join_types_raw(base.raw)
    student_join_types = _extract_join_types_raw(student.raw)
    if base_join_types and student_join_types and base_join_types != student_join_types:
        found.append("WRONG_JOIN_TYPE")
    elif base.joins and student.joins:
        base_types = {_normalize_join_type(j['type']) for j in base.joins}
        student_types = {_normalize_join_type(j['type']) for j in student.joins}
        if base_types != student_types and "WRONG_JOIN_TYPE" not in found:
            found.append("WRONG_JOIN_TYPE")

    # Missing join — reference includes more joined tables than student.
    base_total = len(base.from_tables) + len(base.joins)
    student_total = len(student.from_tables) + len(student.joins)
    if student_total < base_total:
        found.append("MISSING_JOIN")

    return found


_DETECTOR_DISPATCH = {
    "DIVISION":    _detect_division,
    "JOIN":        _detect_join,
    "AGGREGATION": _detect_aggregation,
    "SET_OP":      _detect_setop,
    "SUBQUERY":    _detect_subquery,
    "NULL":        _detect_null,
}


def _detect_misconceptions(base: ParsedQuery, student: ParsedQuery,
                            diffs: List[ASTDiff], problem_type: str) -> List[Dict]:
    """
    Return a de-duplicated list of misconception dicts in detection order.
    Each dict contains the key, library entry, and per-misconception penalty.
    """
    if student.error:
        return []

    keys: List[str] = []
    detector = _DETECTOR_DISPATCH.get((problem_type or "").upper())
    if detector:
        keys.extend(detector(base, student))
    for k in _detect_common(base, student):
        if k not in keys:
            keys.append(k)

    out = []
    for k in keys:
        pattern = MISCONCEPTION_PATTERNS.get(k)
        if not pattern:
            continue
        out.append({
            "key":      k,
            "penalty":  MISCONCEPTION_PENALTIES.get(k, 5),
            "predicts": MISCONCEPTION_PREDICTS.get(
                k, "the result would differ from the reference"),
            **pattern,
        })
    return out


# ══════════════════════════════════════════════════════════════════════
#  DIAGNOSIS-SPECIFIC EVIDENCE GATING
# ══════════════════════════════════════════════════════════════════════
#
# A single global test — "the student's output differs from the reference"
# — establishes that the submission is wrong. It does not establish that any
# *particular* structural proposal caused the wrongness. A submission with two
# structural deviations, one a legitimate alternative formulation and one a
# genuine error, diverges on output; a global gate releases both proposals, so
# execution corroborates the incorrectness of the query but not the diagnosis.
#
#     Release(m)  <=>  structural_trigger(m)  AND  evidence_supports(m)
#
# where evidence_supports(m) is the predicted *observable effect of m*, not
# merely Q_s(D) != Q_r(D). Each detector declares the residual signature its
# misconception implies; a proposal whose predicted effect is absent from the
# observed evidence is withheld.
#
# Two invariants hold by construction:
#
#   1. This gate is AND-ed with the pre-existing global filter, so it can only
#      withhold a proposal that filter would have released, never release one
#      it suppressed. Measured false-positive rates on alternate-correct
#      submissions therefore cannot rise.
#   2. Where a discriminating signal is unavailable — no provenance trace was
#      computed, no edge instances exist — the predicate falls back to a
#      weaker but still diagnosis-specific test rather than withholding.
#      Absence of a signal is not evidence against a diagnosis.


class _Evidence:
    """Normalised view of the execution evidence available for one submission."""

    __slots__ = ("extra", "missing", "base_count", "student_count",
                 "exec_error", "edges", "provenance", "aggregates")

    def __init__(self, comparison, edge_results, provenance_trace, execution_error,
                 base=None, student=None):
        comparison = comparison or {}
        self.extra         = len(comparison.get("extra_rows") or [])
        self.missing       = len(comparison.get("missing_rows") or [])
        self.base_count    = comparison.get("base_count")
        self.student_count = comparison.get("student_count")
        self.exec_error    = execution_error
        self.edges         = edge_results or []
        self.provenance    = provenance_trace
        # Whether either query aggregates. This decides whether the *direction*
        # of a residual is predictable at all: see _ev_less_restrictive.
        self.aggregates = bool(
            (base is not None and (base.group_by or base.having))
            or (student is not None and (student.group_by or student.having))
            or (base is not None and _has_aggregate(base.raw or ""))
            or (student is not None and _has_aggregate(student.raw or ""))
        )

    @property
    def failed_edges(self):
        return [e for e in self.edges if not e.get("passed")]

    @property
    def over_returns(self):
        """Student returned at least one row the reference does not.

        An aggregate evaluated over a wider input shows up here too: the
        changed value is a row the reference never returned.
        """
        return self.extra > 0

    @property
    def under_returns(self):
        """Student omitted at least one row the reference returns."""
        return self.missing > 0

    @property
    def collapsed(self):
        """Student produced at most one group where the reference produced many.

        Zero counts as collapsed: a single aggregate row that a HAVING clause
        then rejects leaves no output at all, which is the same event observed
        one step later.
        """
        return (self.student_count is not None
                and self.student_count <= 1
                and (self.base_count or 0) > 1)

    @property
    def returned_nothing(self):
        return self.student_count == 0 and (self.base_count or 0) > 0

    @property
    def diverges(self):
        return bool(self.extra or self.missing or self.exec_error
                    or self.failed_edges)


def _ev_rejected_by_engine(ev):
    """The engine refuses to run the query at all."""
    return ev.exec_error is not None


def _ev_less_restrictive(ev):
    """A constraint the reference applies is absent, so the input relation grows.

    Withheld when the query never executed: a query the engine rejected
    produced no rows and so offers no evidence that it was under-restrictive.

    The *direction* of the residual is only predictable when the query does not
    aggregate. Under aggregation a wider input does not add output rows; it
    changes aggregate values, and an aggregate filter can then reject groups
    that previously passed, so the student returns fewer rows rather than more.
    A cartesian product under `HAVING count(*) < k` is the standard case. Where
    the query aggregates, divergence is therefore the strongest
    direction-independent evidence the diagnosis predicts.
    """
    if ev.exec_error is not None:
        return False
    if ev.over_returns:
        return True
    return ev.aggregates and ev.diverges


def _ev_having_vs_where(ev):
    """An aggregate moved into WHERE is rejected outright by standard engines.

    Where an engine tolerates it, the predicate filters rows instead of groups
    and the result over-returns.
    """
    return _ev_rejected_by_engine(ev) or ev.over_returns


def _ev_missing_group_by(ev):
    """Dropping GROUP BY collapses the result to a single aggregate row.

    Engines that reject a mixed SELECT list refuse the query instead.
    """
    return _ev_rejected_by_engine(ev) or ev.collapsed or ev.over_returns


def _ev_all_or_nothing(ev):
    """An uncorrelated subquery is evaluated once, as a constant.

    The outer predicate is then invariant across rows, so either every
    candidate row qualifies or none does.
    """
    return (ev.returned_nothing or ev.over_returns
            or _ev_rejected_by_engine(ev))


def _ev_null_trap(ev):
    """NOT IN over a NULL-bearing subquery is UNKNOWN for every row.

    The NULL-injected edge instance is the discriminating evidence, and where
    one was constructed its failure settles the diagnosis. On data holding no
    NULLs the trap is dormant, and what remains observable is that substituting
    NOT IN for a correlated negation changed the result at all; the direction
    of that change follows from the divisor contents, not from the diagnosis.
    Where no NULL-bearing instance could be built at all — the link table is
    declared NOT NULL — absence of the discriminating signal is not evidence
    against the diagnosis, so the gate falls back to divergence.
    """
    if ev.failed_edges:
        return True
    if ev.under_returns or ev.returned_nothing:
        return True
    if not ev.edges:
        return ev.diverges
    return False


def _ev_null_equality(ev):
    """`= NULL` is never true, so the predicate eliminates every row."""
    return ev.returned_nothing or ev.under_returns


def _ev_in_for_division(ev):
    """IN tests existential membership, so partial matchers are admitted.

    Where a provenance trace exists it discriminates directly, naming the
    divisor elements a returned row is missing; otherwise the observable
    consequence is that the student returns rows the reference excludes.
    """
    if ev.provenance:
        if ev.provenance.get("incomplete_rows") or ev.provenance.get("violations"):
            return True
    return ev.over_returns or bool(ev.failed_edges)


def _ev_any_divergence(ev):
    """Bidirectional diagnoses, whose residual direction is not predicted.

    Swapping one set operator for another can add or remove rows depending on
    the operands, so divergence is the strongest diagnosis-consistent evidence
    available. Recorded explicitly so the weaker gate is visible, not implied.
    """
    return ev.diverges


# Human-readable form of each rule's predicted effect. This is the same content
# as MISCONCEPTION_EVIDENCE below, phrased for a student rather than for code, so
# the interface can say *why* a diagnosis was released rather than only naming it.
MISCONCEPTION_PREDICTS = {
    "MISSING_WHERE":          "rows the reference excludes would appear in your result",
    "MISSING_HAVING":         "groups the reference filters out would appear in your result",
    "WRONG_JOIN_TYPE":        "rows the reference excludes would appear in your result",
    "MISSING_JOIN":           "rows the reference excludes would appear in your result",
    "CARTESIAN_PRODUCT":      "rows the reference excludes would appear in your result",
    "IN_vs_EXISTS":           "rows the reference excludes would appear in your result",
    "HAVING_vs_WHERE":        "the database engine would reject the query outright",
    "MISSING_GROUP_BY":       "the engine would reject the query, or it would collapse to a single group where the reference returns many",
    "MISSING_NOT_EXISTS":     "the result would be all-or-nothing: every candidate row qualifies, or none does",
    "MISSING_CORRELATED_REF": "the result would be all-or-nothing: every candidate row qualifies, or none does",
    "NOT_IN_vs_NOT_EXISTS":   "a NULL-bearing instance would lose rows the reference keeps",
    "NULL_EQUALITY":          "the predicate would eliminate every row",
    "IN_FOR_DIVISION":        "a returned row would be missing a required element of the divisor set",
    "MISSING_SET_OP":         "the result would differ from the reference",
    "WRONG_SET_OP":           "the result would differ from the reference",
    "HARDCODED_THRESHOLD":    "the result would differ from the reference",
}


MISCONCEPTION_EVIDENCE = {
    "MISSING_WHERE":          _ev_less_restrictive,
    "MISSING_HAVING":         _ev_less_restrictive,
    "WRONG_JOIN_TYPE":        _ev_less_restrictive,
    "MISSING_JOIN":           _ev_less_restrictive,
    "CARTESIAN_PRODUCT":      _ev_less_restrictive,
    "IN_vs_EXISTS":           _ev_less_restrictive,
    "HAVING_vs_WHERE":        _ev_having_vs_where,
    "MISSING_GROUP_BY":       _ev_missing_group_by,
    "MISSING_NOT_EXISTS":     _ev_all_or_nothing,
    "MISSING_CORRELATED_REF": _ev_all_or_nothing,
    "NOT_IN_vs_NOT_EXISTS":   _ev_null_trap,
    "NULL_EQUALITY":          _ev_null_equality,
    "IN_FOR_DIVISION":        _ev_in_for_division,
    "MISSING_SET_OP":         _ev_any_divergence,
    "WRONG_SET_OP":           _ev_any_divergence,
    "HARDCODED_THRESHOLD":    _ev_any_divergence,
}


def evidence_supports(key: str, ev: "_Evidence") -> bool:
    """Does the observed evidence exhibit the effect this diagnosis predicts?"""
    predicate = MISCONCEPTION_EVIDENCE.get(key)
    if predicate is None:
        # Uncharacterised detector: fall back to the global divergence test
        # rather than withholding on a signature that was never written.
        return ev.diverges
    return predicate(ev)


# ══════════════════════════════════════════════════════════════════════
#  GRADING
# ══════════════════════════════════════════════════════════════════════

def _grade_syntax(student_parse: ParsedQuery, execution_error: Optional[str] = None) -> GradeComponent:
    if student_parse.error:
        return GradeComponent("Syntax", 0, WEIGHTS["syntax"],
                              notes=[f"Syntax error: {student_parse.error}"])
    if execution_error:
        return GradeComponent("Syntax", 0, WEIGHTS["syntax"],
                              notes=[f"Execution error: {execution_error}"])
    return GradeComponent("Syntax", WEIGHTS["syntax"], WEIGHTS["syntax"],
                          notes=["Query is syntactically valid."])


def _grade_logic(base: ParsedQuery, student: ParsedQuery, diffs: List[ASTDiff],
                 problem_type: str, misconceptions: List[Dict]) -> GradeComponent:
    max_s = WEIGHTS["logic"]
    if student.error:
        return GradeComponent("Logic", 0, max_s, notes=["Cannot assess logic: syntax error."])

    score = max_s
    notes: List[str] = []

    for m in misconceptions:
        score -= m["penalty"]
        notes.append(f"{m['title']} (−{m['penalty']})")

    if not misconceptions:
        sel_diff = any(d.path.startswith("SELECT.columns") for d in diffs)
        from_diff = any(d.path.startswith("FROM.tables") for d in diffs)
        if sel_diff or from_diff:
            score -= 5
            notes.append("Minor structural mismatch in SELECT or FROM (−5).")

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

def _code_snippet_for(key: str) -> Optional[str]:
    """Return a ready-to-paste code snippet for common misconceptions."""
    snippets = {
        "IN_FOR_DIVISION": (
            "-- Universal quantification via double negation:\n"
            "WHERE NOT EXISTS (\n"
            "  SELECT 1 FROM <Divisor> d\n"
            "  WHERE d.<filter> = ...\n"
            "  AND d.<key> NOT IN (\n"
            "    SELECT t.<key> FROM <Link> t\n"
            "    WHERE t.<outer_fk> = <outer_alias>.<outer_pk>\n"
            "  )\n"
            ")"
        ),
        "MISSING_NOT_EXISTS": (
            "-- Division template:\n"
            "SELECT o.<cols>\n"
            "FROM <Outer> o\n"
            "WHERE NOT EXISTS (\n"
            "  SELECT 1 FROM <Divisor> d\n"
            "  WHERE NOT EXISTS (\n"
            "    SELECT 1 FROM <Link> t\n"
            "    WHERE t.<outer_fk> = o.<outer_pk>\n"
            "    AND t.<divisor_fk> = d.<divisor_pk>\n"
            "  )\n"
            ");"
        ),
        "CARTESIAN_PRODUCT": (
            "-- Replace the comma-join with an explicit ON condition:\n"
            "FROM A\n"
            "JOIN B ON A.<key> = B.<key>"
        ),
        "HAVING_vs_WHERE": (
            "-- Aggregate predicate → HAVING; row-level predicate → WHERE\n"
            "SELECT col, COUNT(*)\n"
            "FROM t\n"
            "WHERE row_level_predicate       -- e.g. t.year = 2024\n"
            "GROUP BY col\n"
            "HAVING COUNT(*) > 2;            -- aggregate predicate"
        ),
        "MISSING_GROUP_BY": (
            "-- Every non-aggregate column in SELECT must be in GROUP BY:\n"
            "SELECT col_a, COUNT(*)\n"
            "FROM t\n"
            "GROUP BY col_a;"
        ),
        "NULL_EQUALITY": (
            "-- Correct NULL predicates:\n"
            "WHERE col IS NULL      -- never  col = NULL\n"
            "WHERE col IS NOT NULL  -- never  col != NULL"
        ),
        "WRONG_SET_OP": (
            "-- UNION      : in either set\n"
            "-- INTERSECT  : in both sets\n"
            "-- EXCEPT     : in left set but not right"
        ),
        "IN_vs_EXISTS": (
            "-- Replace the IN subquery with a correlated EXISTS:\n"
            "WHERE EXISTS (\n"
            "  SELECT 1 FROM <inner> i\n"
            "  WHERE i.<fk> = <outer>.<pk> AND i.<filter> = ...\n"
            ")"
        ),
        "NOT_IN_vs_NOT_EXISTS": (
            "-- NULL-safe version:\n"
            "WHERE NOT EXISTS (\n"
            "  SELECT 1 FROM <inner> i\n"
            "  WHERE i.<key> = <outer>.<key>\n"
            ")"
        ),
    }
    return snippets.get(key)


def _preview_row(row: Any) -> str:
    """Produce a compact preview string for a row record."""
    if isinstance(row, dict):
        for k in ("Name", "StuID", "InstID", "CourseID", "Title"):
            if k in row and row[k] is not None:
                return str(row[k])
        if row:
            return str(next(iter(row.values())))
    return str(row)


def _generate_feedback_items(base_parse: ParsedQuery, student_parse: ParsedQuery,
                              diffs: List[ASTDiff], comparison: Dict,
                              edge_results: List[Dict],
                              provenance_trace: Optional[Dict],
                              problem_type: str,
                              misconceptions: List[Dict],
                              execution_error: Optional[str] = None) -> List[FeedbackItem]:
    items: List[FeedbackItem] = []

    # ── Syntax ────────────────────────────────────────────────────
    if student_parse.error:
        items.append(FeedbackItem(
            level="error", category="SYNTAX",
            title="Syntax Error Detected",
            body=student_parse.error,
            suggestion="Check SQL keywords carefully. Common mistakes: 'FRM' for 'FROM', 'SELCT' for 'SELECT', unmatched parentheses, or unclosed quotes."
        ))
        return items
    if execution_error:
        items.append(FeedbackItem(
            level="error", category="SYNTAX",
            title="SQL Execution Error",
            body=execution_error,
            suggestion="Your query could not run. Check for misspelled table names, column names, or keywords."
        ))
        return items

    items.append(FeedbackItem(
        level="success", category="SYNTAX",
        title="Syntax Correct",
        body="Your query is syntactically valid and can be executed without errors."
    ))

    # ── Misconception → LOGIC feedback items ───────────────────────
    for m in misconceptions:
        items.append(FeedbackItem(
            level="error", category="LOGIC",
            title=m["title"],
            body=m["description"],
            suggestion=m["fix"],
            code_snippet=_code_snippet_for(m["key"]),
            reference=m.get("reference"),
        ))

    # ── Results ────────────────────────────────────────────────────
    missing = comparison.get("missing_rows", [])
    extra   = comparison.get("extra_rows", [])

    if extra:
        preview = [_preview_row(r) for r in extra[:3]]
        items.append(FeedbackItem(
            level="error", category="RESULT",
            title=f"{len(extra)} Extra Row(s) in Your Output",
            body=(
                f"Your query returns rows that are not in the expected output: {', '.join(preview)}. "
                "These are counterexamples — tracing why each one satisfies your predicate but should "
                "not usually reveals the underlying logic error."
            ),
            suggestion="Inspect the WHERE/HAVING/set-operator logic against one of these rows."
        ))

    if missing:
        preview = [_preview_row(r) for r in missing[:3]]
        items.append(FeedbackItem(
            level="error", category="RESULT",
            title=f"{len(missing)} Missing Row(s) from Expected Output",
            body=f"Your query is missing: {', '.join(preview)}, which should appear in the result."
        ))

    if not extra and not missing:
        items.append(FeedbackItem(
            level="success", category="RESULT",
            title="Output Matches Reference",
            body=(
                "Your query produces the correct output on this database instance. "
                "Verify edge cases to confirm this is not a coincidental match on this data."
            )
        ))

    # ── Provenance ─────────────────────────────────────────────────
    if provenance_trace and provenance_trace.get("divergence_points"):
        for dp in provenance_trace["divergence_points"][:2]:
            items.append(FeedbackItem(
                level="info", category="PROVENANCE",
                title=f"Provenance Divergence: {dp.get('name', dp.get('student', ''))}",
                body=dp.get("explanation", ""),
                reference="I-REX (Miao et al., VLDB 2020) — Block-level SQL tracing"
            ))

    # ── Edge cases ─────────────────────────────────────────────────
    failed_edge = [e for e in edge_results if not e.get("passed")]
    if failed_edge:
        for e in failed_edge[:3]:
            items.append(FeedbackItem(
                level="warning", category="EDGE_CASE",
                title=f"Edge Case Failed: {e.get('title','')}",
                body=(
                    f"{e.get('description','')} "
                    f"Base query returned {e.get('base_result',{}).get('row_count','?')} row(s); "
                    f"your query returned {e.get('student_result',{}).get('row_count','?')} row(s). "
                    f"{e.get('tests','')}"
                ),
                reference="RATest (Miao et al., SIGMOD 2019) — Minimal counterexample generation"
            ))
    elif edge_results:
        items.append(FeedbackItem(
            level="success", category="EDGE_CASE",
            title="All Edge Cases Passed",
            body=f"Your query correctly handles all {len(edge_results)} edge case scenarios."
        ))

    return items


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
    execution_error: Optional[str] = None,
) -> FeedbackReport:
    """Generate a complete FeedbackReport from all analysis components."""
    diffs = compare_queries(base_parse, student_parse)
    is_struct_equal = queries_structurally_equal(base_parse, student_parse)

    # Shape-level misconception classification (strict, pre-filter).
    raw_misconceptions = _detect_misconceptions(base_parse, student_parse, diffs, problem_type)

    # Result-aware filter: if the student's output matches the reference AND
    # (no edge cases ran OR every edge case passed), treat the query as an
    # alternate correct solution and drop the structural misconceptions. A
    # shape difference between two queries that agree on every tested
    # instance is a stylistic choice, not a bug.
    results_ok = bool(comparison.get("are_equivalent"))
    edges_ok   = not edge_results or all(e.get("passed") for e in edge_results)
    suppressed = results_ok and edges_ok

    # Release(m) <=> structural_trigger(m) AND evidence_supports(m). The global
    # filter above establishes that *something* is wrong; the per-proposal gate
    # below establishes that this particular diagnosis is what the evidence
    # shows. A proposal whose predicted effect is absent is withheld from the
    # student and retained instructor-side.
    evidence = _Evidence(comparison, edge_results, provenance_trace, execution_error,
                         base=base_parse, student=student_parse)
    if suppressed:
        misconceptions, unsupported = [], []
    else:
        misconceptions = [m for m in raw_misconceptions
                          if evidence_supports(m["key"], evidence)]
        unsupported    = [m for m in raw_misconceptions
                          if not evidence_supports(m["key"], evidence)]

    syntax_grade  = _grade_syntax(student_parse, execution_error)
    logic_grade   = _grade_logic(base_parse, student_parse, diffs, problem_type, misconceptions)
    results_grade = _grade_results(comparison)
    edge_grade    = _grade_edge_cases(edge_results)

    total = syntax_grade.score + logic_grade.score + results_grade.score + edge_grade.score

    is_alt_correct = (
        results_ok and edges_ok and not is_struct_equal
    )
    if is_alt_correct:
        total = min(total + 5, 100)

    items = _generate_feedback_items(
        base_parse, student_parse, diffs, comparison, edge_results,
        provenance_trace, problem_type, misconceptions,
        execution_error=execution_error,
    )

    if student_parse.error:
        summary = "Your query has a syntax error and could not be evaluated."
    elif is_alt_correct:
        summary = (
            f"Excellent. Your query produces the correct output and passes all edge cases. "
            f"It is structurally different from the reference — accepted as an alternate correct solution. "
            f"Score: {total}/100."
        )
    elif comparison.get("are_equivalent"):
        summary = (
            f"Your query produces the correct output on the main database. "
            f"Review any edge-case failures to ensure full correctness. Score: {total}/100."
        )
    else:
        err_count = len([i for i in items if i.level == "error"])
        primary = misconceptions[0]["title"] if misconceptions else (
            diffs[0].path if diffs else "structural"
        )
        summary = (
            f"Your query has {err_count} issue(s) to address. "
            f"Primary issue: {primary}. "
            f"Score: {total}/100. Review the feedback items below."
        )

    return FeedbackReport(
        total_score=total,
        grade_letter=_score_to_letter(total),
        is_alternate_correct=is_alt_correct,
        components=[syntax_grade, logic_grade, results_grade, edge_grade],
        items=items,
        misconceptions=misconceptions,
        raw_misconceptions=raw_misconceptions,
        unsupported_misconceptions=unsupported,
        summary=summary,
    )

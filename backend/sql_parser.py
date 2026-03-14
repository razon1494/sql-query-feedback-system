"""
SQL Parser — Builds a structured AST from SQL queries using Python's re module.
Handles: SELECT, FROM, WHERE, JOIN, GROUP BY, HAVING, ORDER BY,
         Subqueries (correlated and non-correlated), NOT EXISTS, IN, NOT IN.
"""
import re
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any


# ══════════════════════════════════════════════════════════════════════
#  SYNTAX VALIDATOR
# ══════════════════════════════════════════════════════════════════════

# Required keywords every SELECT query must have
REQUIRED_KEYWORDS = ['SELECT', 'FROM']

# Valid SQL clause keywords (misspellings of these will be caught)
VALID_CLAUSE_KEYWORDS = {
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'GROUP', 'BY', 'HAVING',
    'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT',
    'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'CROSS', 'NATURAL',
    'NOT', 'EXISTS', 'IN', 'AND', 'OR', 'AS', 'DISTINCT', 'ALL',
    'IS', 'NULL', 'LIKE', 'BETWEEN', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COALESCE',
}

# Common misspellings → correct keyword mapping
COMMON_MISSPELLINGS = {
    'FRM':      'FROM',
    'FORM':     'FROM',
    'FRON':     'FROM',
    'FTOM':     'FROM',
    'SELCT':    'SELECT',
    'SLECT':    'SELECT',
    'SELET':    'SELECT',
    'SELCET':   'SELECT',
    'WHER':     'WHERE',
    'WHRE':     'WHERE',
    'HWERE':    'WHERE',
    'GORUP':    'GROUP',
    'GRUOP':    'GROUP',
    'GRPUP':    'GROUP',
    'HAVNG':    'HAVING',
    'HAIVNG':   'HAVING',
    'HWERE':    'WHERE',
    'ORDDER':   'ORDER',
    'ORDR':     'ORDER',
    'JION':     'JOIN',
    'HVAING':   'HAVING',
    'EXSITS':   'EXISTS',
    'EXISITS':  'EXISTS',
    'EXIXTS':   'EXISTS',
    'WHERER':   'WHERE',
    'WEHRE':    'WHERE',
}


def validate_sql_syntax(sql: str):
    """
    Validate SQL syntax before execution.
    Returns (is_valid: bool, error_message: str or None)
    Catches: missing FROM, misspelled keywords, unmatched parentheses,
             missing SELECT, unclosed quotes.
    """
    if not sql or not sql.strip():
        return False, "Query is empty."

    cleaned = sql.strip().rstrip(';')
    upper   = cleaned.upper()
    tokens  = re.split(r'\s+', upper.strip())

    errors = []

    # 1. Must start with SELECT
    if not tokens or tokens[0] != 'SELECT':
        if tokens and tokens[0] in COMMON_MISSPELLINGS:
            errors.append(f"Misspelled keyword: '{tokens[0]}' — did you mean '{COMMON_MISSPELLINGS[tokens[0]]}'?")
        else:
            errors.append("Query must start with SELECT.")
        return False, errors[0]

    # 2. Must contain FROM (check for misspellings)
    top_tokens = _get_top_level_tokens(cleaned)
    has_from = 'FROM' in top_tokens
    if not has_from:
        # Check for misspelling of FROM
        for tok in top_tokens:
            if tok in COMMON_MISSPELLINGS and COMMON_MISSPELLINGS[tok] == 'FROM':
                return False, f"Misspelled keyword: '{tok}' — did you mean 'FROM'?"
            # Fuzzy check: very close to FROM
            if len(tok) in (3, 4) and tok not in VALID_CLAUSE_KEYWORDS and _similar_to(tok, 'FROM'):
                return False, f"Misspelled keyword: '{tok}' — did you mean 'FROM'?"
        return False, "Missing FROM clause. Every SELECT query requires a FROM clause."

    # 3. Check all top-level tokens for misspellings
    for tok in top_tokens:
        # Skip tokens that look like table names, aliases, values (contain dots, are quoted, etc.)
        if not tok.isalpha():
            continue
        if len(tok) < 2:
            continue
        if tok in VALID_CLAUSE_KEYWORDS:
            continue
        # Check misspelling dict
        if tok in COMMON_MISSPELLINGS:
            correct = COMMON_MISSPELLINGS[tok]
            errors.append(f"Misspelled keyword: '{tok}' — did you mean '{correct}'?")
            continue

    # 4. Unmatched parentheses
    depth = 0
    in_string = False
    quote_char = None
    for ch in cleaned:
        if in_string:
            if ch == quote_char:
                in_string = False
        elif ch in ('"', "'"):
            in_string = True
            quote_char = ch
        elif ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth < 0:
                return False, "Syntax error: unexpected ')' — unmatched closing parenthesis."
    if depth > 0:
        return False, f"Syntax error: {depth} unclosed parenthesis(es) — missing ')'."
    if in_string:
        return False, "Syntax error: unclosed string literal — missing closing quote."

    # 5. GROUP must be followed by BY
    if 'GROUP' in top_tokens:
        idx = top_tokens.index('GROUP')
        if idx + 1 >= len(top_tokens) or top_tokens[idx + 1] != 'BY':
            errors.append("Syntax error: GROUP must be followed by BY (GROUP BY).")

    # 6. ORDER must be followed by BY
    if 'ORDER' in top_tokens:
        idx = top_tokens.index('ORDER')
        if idx + 1 >= len(top_tokens) or top_tokens[idx + 1] != 'BY':
            errors.append("Syntax error: ORDER must be followed by BY (ORDER BY).")

    if errors:
        return False, " | ".join(errors)

    return True, None


def _get_top_level_tokens(sql: str) -> list:
    """Extract whitespace-split tokens at depth 0 only (not inside subqueries)."""
    tokens = []
    depth = 0
    current = []
    in_string = False
    quote_char = None

    for ch in sql:
        if in_string:
            if ch == quote_char:
                in_string = False
            current.append(ch)
        elif ch in ('"', "'"):
            in_string = True
            quote_char = ch
            current.append(ch)
        elif ch == '(':
            depth += 1
            if depth == 1 and current:
                tokens.append(''.join(current).strip().upper())
                current = []
        elif ch == ')':
            depth -= 1
        elif depth == 0:
            if ch in (' ', '\t', '\n'):
                if current:
                    tokens.append(''.join(current).strip().upper())
                    current = []
            else:
                current.append(ch)

    if current:
        tokens.append(''.join(current).strip().upper())

    return [t for t in tokens if t]


def _similar_to(word: str, target: str, max_diff: int = 1) -> bool:
    """Simple edit-distance check — is word within max_diff edits of target?"""
    if abs(len(word) - len(target)) > max_diff:
        return False
    # Count character differences
    diffs = sum(1 for a, b in zip(word, target) if a != b)
    diffs += abs(len(word) - len(target))
    return diffs <= max_diff



# ══════════════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ══════════════════════════════════════════════════════════════════════

@dataclass
class ASTNode:
    node_type: str
    value: Optional[str] = None
    children: List['ASTNode'] = field(default_factory=list)
    attributes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        return {
            "node_type": self.node_type,
            "value": self.value,
            "attributes": self.attributes,
            "children": [c.to_dict() for c in self.children]
        }


@dataclass
class ParsedQuery:
    raw: str
    normalized: str
    select_cols: List[str] = field(default_factory=list)
    from_tables: List[str] = field(default_factory=list)
    joins: List[Dict] = field(default_factory=list)
    where_clause: Optional[str] = None
    where_type: Optional[str] = None   # NOT_EXISTS | NOT_IN | IN | EXISTS | SIMPLE
    group_by: List[str] = field(default_factory=list)
    having: Optional[str] = None
    order_by: Optional[str] = None
    subqueries: List['ParsedQuery'] = field(default_factory=list)
    has_distinct: bool = False
    set_operation: Optional[str] = None  # UNION | INTERSECT | EXCEPT
    ast: Optional[ASTNode] = None
    error: Optional[str] = None

    def to_dict(self):
        d = {
            "raw": self.raw,
            "select_cols": self.select_cols,
            "from_tables": self.from_tables,
            "joins": self.joins,
            "where_clause": self.where_clause,
            "where_type": self.where_type,
            "group_by": self.group_by,
            "having": self.having,
            "order_by": self.order_by,
            "has_distinct": self.has_distinct,
            "set_operation": self.set_operation,
            "subquery_count": len(self.subqueries),
            "error": self.error,
            "ast": self.ast.to_dict() if self.ast else None,
        }
        return d


# ══════════════════════════════════════════════════════════════════════
#  TOKENIZER HELPERS
# ══════════════════════════════════════════════════════════════════════

def normalize_sql(sql: str) -> str:
    """Normalize whitespace and case for comparison."""
    sql = re.sub(r'\s+', ' ', sql.strip())
    return sql


def _keyword_split(sql: str, keyword: str) -> tuple:
    """Split SQL at a top-level keyword (not inside parentheses)."""
    upper = sql.upper()
    depth = 0
    i = 0
    kw = keyword.upper()
    while i < len(sql):
        ch = sql[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and upper[i:i+len(kw)] == kw:
            # Make sure it's a word boundary
            before = sql[i-1] if i > 0 else ' '
            after = sql[i+len(kw)] if i+len(kw) < len(sql) else ' '
            if not before.isalnum() and before != '_' and not after.isalnum() and after != '_':
                return sql[:i].strip(), sql[i+len(kw):].strip()
        i += 1
    return sql.strip(), None


def _extract_top_level_clauses(sql: str) -> Dict[str, str]:
    """
    Extract top-level SQL clauses: SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY.
    Ignores clause keywords that appear inside subquery parentheses.
    """
    clauses = {}
    upper = sql.upper().strip()

    # Detect SET operations at top level
    for op in ['UNION ALL', 'UNION', 'INTERSECT', 'EXCEPT']:
        left, right = _keyword_split(sql, op)
        if right is not None:
            clauses['SET_OP'] = op
            clauses['SET_LEFT'] = left
            clauses['SET_RIGHT'] = right
            return clauses

    # Ordered list of clause keywords to find
    clause_keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']
    positions = []

    for kw in clause_keywords:
        pos = _find_top_level_keyword(upper, kw)
        if pos >= 0:
            positions.append((pos, kw))

    positions.sort(key=lambda x: x[0])

    for idx, (pos, kw) in enumerate(positions):
        start = pos + len(kw)
        end = positions[idx+1][0] if idx+1 < len(positions) else len(sql)
        clauses[kw] = sql[start:end].strip()

    return clauses


def _find_top_level_keyword(upper_sql: str, keyword: str) -> int:
    """Find first occurrence of keyword at depth 0."""
    depth = 0
    kw = keyword.upper()
    i = 0
    while i < len(upper_sql):
        ch = upper_sql[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0 and upper_sql[i:i+len(kw)] == kw:
            before = upper_sql[i-1] if i > 0 else ' '
            after  = upper_sql[i+len(kw)] if i+len(kw) < len(upper_sql) else ' '
            if (not before.isalnum() and before != '_') and (not after.isalnum() and after != '_'):
                return i
        i += 1
    return -1


def _extract_subqueries(text: str) -> List[str]:
    """Extract all subquery strings from a text (content inside parentheses containing SELECT)."""
    subs = []
    depth = 0
    start = -1
    for i, ch in enumerate(text):
        if ch == '(':
            if depth == 0:
                start = i + 1
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0 and start >= 0:
                inner = text[start:i].strip()
                if 'SELECT' in inner.upper():
                    subs.append(inner)
                start = -1
    return subs


def _detect_where_type(where_clause: str) -> str:
    """Detect the primary pattern used in the WHERE clause."""
    upper = where_clause.upper()
    if 'NOT EXISTS' in upper:
        return 'NOT_EXISTS'
    elif 'NOT IN' in upper:
        return 'NOT_IN'
    elif 'EXISTS' in upper:
        return 'EXISTS'
    elif ' IN ' in upper or upper.startswith('IN '):
        return 'IN'
    else:
        return 'SIMPLE'


def _parse_select_cols(select_str: str) -> List[str]:
    """Parse SELECT column list handling function calls."""
    if not select_str:
        return []
    cols = []
    depth = 0
    current = ''
    for ch in select_str:
        if ch == '(':
            depth += 1
            current += ch
        elif ch == ')':
            depth -= 1
            current += ch
        elif ch == ',' and depth == 0:
            cols.append(current.strip())
            current = ''
        else:
            current += ch
    if current.strip():
        cols.append(current.strip())
    return cols


def _parse_from_tables(from_str: str) -> List[str]:
    """Extract table names from FROM clause (before any JOIN)."""
    # Remove JOIN parts
    join_pattern = re.compile(r'\b(INNER|LEFT|RIGHT|FULL|CROSS|NATURAL)?\s*JOIN\b.*', re.IGNORECASE | re.DOTALL)
    clean = join_pattern.sub('', from_str).strip()
    tables = []
    for part in clean.split(','):
        part = part.strip()
        # Handle aliases: "Students s" or "Students AS s"
        tokens = re.split(r'\s+AS\s+|\s+', part, flags=re.IGNORECASE)
        if tokens:
            tables.append(tokens[0].strip().rstrip(';'))
    return [t for t in tables if t]


def _parse_joins(from_str: str) -> List[Dict]:
    """Extract JOIN clauses."""
    joins = []
    pattern = re.compile(
        r'((?:INNER|LEFT|RIGHT|FULL|CROSS|NATURAL)?\s*JOIN)\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+(.+?)(?=(?:INNER|LEFT|RIGHT|FULL|CROSS|NATURAL)?\s*JOIN|$)',
        re.IGNORECASE | re.DOTALL
    )
    for m in pattern.finditer(from_str):
        joins.append({
            "type": m.group(1).strip().upper(),
            "table": m.group(2),
            "alias": m.group(3),
            "condition": m.group(4).strip()
        })
    return joins


# ══════════════════════════════════════════════════════════════════════
#  MAIN PARSE FUNCTION
# ══════════════════════════════════════════════════════════════════════

def parse_sql(sql: str) -> ParsedQuery:
    """
    Parse a SQL SELECT statement into a ParsedQuery structure.
    Returns ParsedQuery with error set if parsing fails.
    """
    try:
        sql = sql.strip().rstrip(';')
        upper = sql.upper()

        # ── Run syntax validator first ──
        is_valid, syntax_error = validate_sql_syntax(sql)
        if not is_valid:
            return ParsedQuery(raw=sql, normalized=normalize_sql(sql), error=syntax_error)

        # Basic validation (fallback)
        if not upper.strip().startswith('SELECT') and 'SELECT' not in upper:
            return ParsedQuery(raw=sql, normalized=normalize_sql(sql), error="Query must start with SELECT")

        clauses = _extract_top_level_clauses(sql)

        pq = ParsedQuery(raw=sql, normalized=normalize_sql(sql))

        # SET operations
        if 'SET_OP' in clauses:
            pq.set_operation = clauses['SET_OP']
            return pq

        # SELECT
        sel = clauses.get('SELECT', '*')
        if sel.upper().startswith('DISTINCT'):
            pq.has_distinct = True
            sel = sel[8:].strip()
        pq.select_cols = _parse_select_cols(sel)

        # FROM
        from_raw = clauses.get('FROM', '')
        pq.from_tables = _parse_from_tables(from_raw)
        pq.joins = _parse_joins(from_raw)

        # WHERE
        where_raw = clauses.get('WHERE')
        if where_raw:
            pq.where_clause = where_raw
            pq.where_type = _detect_where_type(where_raw)
            pq.subqueries = [parse_sql(s) for s in _extract_subqueries(where_raw)]

        # GROUP BY
        gb = clauses.get('GROUP BY', '')
        if gb:
            pq.group_by = [c.strip() for c in gb.split(',')]

        # HAVING
        pq.having = clauses.get('HAVING')

        # ORDER BY
        pq.order_by = clauses.get('ORDER BY')

        # Build AST
        pq.ast = _build_ast(pq)

        return pq

    except Exception as e:
        return ParsedQuery(raw=sql, normalized=normalize_sql(sql), error=str(e))


def _build_ast(pq: ParsedQuery) -> ASTNode:
    """Build a tree of ASTNode from ParsedQuery."""
    root = ASTNode(node_type="SELECT_STMT")

    # SELECT
    sel_node = ASTNode(node_type="SELECT",
                       attributes={"distinct": pq.has_distinct})
    for col in pq.select_cols:
        sel_node.children.append(ASTNode(node_type="COLUMN", value=col))
    root.children.append(sel_node)

    # FROM
    from_node = ASTNode(node_type="FROM")
    for tbl in pq.from_tables:
        from_node.children.append(ASTNode(node_type="TABLE", value=tbl))
    for j in pq.joins:
        jn = ASTNode(node_type="JOIN", value=j['type'],
                     attributes={"table": j['table'], "on": j['condition']})
        from_node.children.append(jn)
    root.children.append(from_node)

    # WHERE
    if pq.where_clause:
        where_node = ASTNode(node_type="WHERE",
                             attributes={"type": pq.where_type})
        where_node.children.append(ASTNode(node_type="CONDITION",
                                           value=pq.where_clause[:120]))
        for sq in pq.subqueries:
            sub_ast = sq.ast or _build_ast(sq)
            sub_ast.node_type = f"SUBQUERY[{sq.where_type or 'SIMPLE'}]"
            where_node.children.append(sub_ast)
        root.children.append(where_node)

    # GROUP BY
    if pq.group_by:
        gb_node = ASTNode(node_type="GROUP_BY")
        for col in pq.group_by:
            gb_node.children.append(ASTNode(node_type="COLUMN", value=col))
        root.children.append(gb_node)

    # HAVING
    if pq.having:
        root.children.append(ASTNode(node_type="HAVING", value=pq.having[:80]))

    return root


# ══════════════════════════════════════════════════════════════════════
#  AST COMPARISON
# ══════════════════════════════════════════════════════════════════════

@dataclass
class ASTDiff:
    path: str
    base_value: Any
    student_value: Any
    diff_type: str  # MISSING | EXTRA | CHANGED | TYPE_CHANGE


def compare_queries(base: ParsedQuery, student: ParsedQuery) -> List[ASTDiff]:
    """
    Compare two parsed queries and return a list of differences.
    """
    diffs = []

    # SELECT columns
    base_cols = set(c.upper() for c in base.select_cols)
    stu_cols  = set(c.upper() for c in student.select_cols)
    for col in base_cols - stu_cols:
        diffs.append(ASTDiff("SELECT.columns", col, None, "MISSING"))
    for col in stu_cols - base_cols:
        diffs.append(ASTDiff("SELECT.columns", None, col, "EXTRA"))

    # FROM tables
    base_tbls = set(t.upper() for t in base.from_tables)
    stu_tbls  = set(t.upper() for t in student.from_tables)
    for t in base_tbls - stu_tbls:
        diffs.append(ASTDiff("FROM.tables", t, None, "MISSING"))
    for t in stu_tbls - base_tbls:
        diffs.append(ASTDiff("FROM.tables", None, t, "EXTRA"))

    # WHERE type (most important for division)
    if base.where_type != student.where_type:
        diffs.append(ASTDiff("WHERE.type", base.where_type, student.where_type, "TYPE_CHANGE"))

    # GROUP BY
    if bool(base.group_by) != bool(student.group_by):
        diffs.append(ASTDiff("GROUP_BY", base.group_by or None, student.group_by or None,
                             "MISSING" if base.group_by else "EXTRA"))

    # HAVING
    if bool(base.having) != bool(student.having):
        diffs.append(ASTDiff("HAVING", base.having, student.having,
                             "MISSING" if base.having else "EXTRA"))

    # JOINs
    if len(base.joins) != len(student.joins):
        diffs.append(ASTDiff("JOINS.count", len(base.joins), len(student.joins), "CHANGED"))

    # Subquery depth
    if len(base.subqueries) != len(student.subqueries):
        diffs.append(ASTDiff("SUBQUERY.depth", len(base.subqueries), len(student.subqueries), "CHANGED"))

    # Set operation
    if base.set_operation != student.set_operation:
        diffs.append(ASTDiff("SET_OPERATION", base.set_operation, student.set_operation, "CHANGED"))

    return diffs


def queries_structurally_equal(base: ParsedQuery, student: ParsedQuery) -> bool:
    """Check if two queries are structurally identical."""
    return (
        base.where_type == student.where_type and
        set(c.upper() for c in base.select_cols) == set(c.upper() for c in student.select_cols) and
        set(t.upper() for t in base.from_tables) == set(t.upper() for t in student.from_tables) and
        bool(base.group_by) == bool(student.group_by) and
        bool(base.having) == bool(student.having)
    )


if __name__ == "__main__":
    test_queries = [
        # Division query (correct)
        """SELECT s.StuID, s.Name FROM Students s
        WHERE NOT EXISTS (
            SELECT c.CourseID FROM Courses c
            WHERE c.Group = 'DB'
            AND c.CourseID NOT IN (
                SELECT t.CourseID FROM Takes t WHERE t.StuID = s.StuID
            )
        )""",
        # Wrong query (IN instead of NOT EXISTS)
        """SELECT s.StuID, s.Name FROM Students s
        WHERE s.StuID IN (
            SELECT t.StuID FROM Takes t
            WHERE t.CourseID IN (
                SELECT c.CourseID FROM Courses c WHERE c.Group = 'DB'
            )
        )""",
    ]

    parsed = [parse_sql(q) for q in test_queries]
    for i, p in enumerate(parsed):
        print(f"\n=== Query {i+1} ===")
        print(f"  Where type : {p.where_type}")
        print(f"  Select cols: {p.select_cols}")
        print(f"  From tables: {p.from_tables}")
        print(f"  Subqueries : {len(p.subqueries)}")
        print(f"  Error      : {p.error}")

    print("\n=== DIFFS ===")
    diffs = compare_queries(parsed[0], parsed[1])
    for d in diffs:
        print(f"  [{d.diff_type}] {d.path}: base={d.base_value!r} student={d.student_value!r}")

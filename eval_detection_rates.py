"""
Detection-rate evaluation harness for the SQL feedback generator.

For each of the 25 problems, generates a set of mutated wrong queries —
one per applicable misconception (M1-M10 + NULL_EQUALITY) — then runs
them through _detect_misconceptions() and reports the per-misconception
detection rate.

Usage:
    python3 eval_detection_rates.py
"""
import os
import sys
import re
import json
from collections import defaultdict, OrderedDict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.sql_parser import parse_sql, compare_queries
from backend.feedback_generator import _detect_misconceptions
from backend.problems import PROBLEMS


# ══════════════════════════════════════════════════════════════════════
#  MUTATORS — transform a correct base query into a wrong one
#             exhibiting a specific misconception.
#  Each returns a mutated SQL string, or None if the mutation does not
#  apply to the given base query.
# ══════════════════════════════════════════════════════════════════════

def mut_drop_where(sql):
    """M1 MISSING_WHERE — drop the WHERE clause."""
    m = re.search(r'\bWHERE\b', sql, re.IGNORECASE)
    if not m:
        return None
    # find end of WHERE clause = just before GROUP / HAVING / ORDER / ; / end
    start = m.start()
    tail = sql[m.end():]
    end_match = re.search(r'\b(GROUP\s+BY|HAVING|ORDER\s+BY|LIMIT)\b|;|$', tail, re.IGNORECASE)
    end = m.end() + (end_match.start() if end_match else len(tail))
    out = sql[:start] + sql[end:]
    return out.strip() or None


def mut_exists_to_in(sql):
    """M2 IN_vs_EXISTS — replace EXISTS with IN."""
    # Match `EXISTS (SELECT ...)` not preceded by NOT
    if re.search(r'\bNOT\s+EXISTS\b', sql, re.IGNORECASE):
        return None  # keep M3 separate
    if not re.search(r'(?<!NOT\s)\bEXISTS\b', sql, re.IGNORECASE):
        return None
    out = re.sub(r'WHERE\s+EXISTS\s*\(', 'WHERE 1 IN (', sql, count=1, flags=re.IGNORECASE)
    return out if out != sql else None


def _find_outermost_not_exists(sql):
    """Return the (start, end) span of the first `NOT EXISTS (` token that
    sits at parenthesis depth 0, or None if no depth-0 NOT EXISTS exists.

    The M3 mutation (NOT EXISTS -> NOT IN) is only a *top-level* shape
    change when the NOT EXISTS being replaced is the outermost predicate.
    Replacing a NOT EXISTS that is nested inside another subquery does not
    change the outer WHERE-type and therefore is not a valid M3 mutation;
    such problems should be skipped rather than counted as detector misses.
    """
    depth = 0
    for m in re.finditer(r'[()]|NOT\s+EXISTS\s*\(', sql, re.IGNORECASE):
        tok = m.group(0)
        if tok == '(':
            depth += 1
        elif tok == ')':
            if depth > 0:
                depth -= 1
        else:  # matched "NOT EXISTS ("
            if depth == 0:
                # span covers "NOT EXISTS" but not the trailing "("
                paren = m.end() - 1
                return (m.start(), paren)
            depth += 1  # the "(" consumed by this token opens a new level
    return None


def mut_notexists_to_notin(sql):
    """M3 NOT_IN_vs_NOT_EXISTS — replace the *outermost* NOT EXISTS with NOT IN.

    Only applies when the base query has a genuine depth-0 NOT EXISTS
    predicate. Problems whose outermost predicate is something else
    (e.g. a plain EXISTS wrapping a nested NOT EXISTS) are skipped: the
    mutation is not applicable there, so returning None keeps the M3
    detection rate honest rather than penalizing the detector for a
    mutation that never produced a top-level shape change.
    """
    span = _find_outermost_not_exists(sql)
    if span is None:
        return None
    start, end = span
    # Replace just the outermost "NOT EXISTS" with "1 NOT IN", leaving the
    # trailing "(" and subquery body intact.
    out = sql[:start] + '1 NOT IN' + sql[end:]
    return out if out != sql else None


def mut_inner_to_left(sql):
    """M4 WRONG_JOIN_TYPE — replace INNER JOIN (or plain JOIN) with LEFT JOIN."""
    if not re.search(r'\bJOIN\b', sql, re.IGNORECASE):
        return None
    out = re.sub(r'\bINNER\s+JOIN\b', 'LEFT JOIN', sql, flags=re.IGNORECASE)
    if out == sql:
        # plain "JOIN" — add LEFT qualifier (but not to LEFT/RIGHT/FULL/CROSS already)
        out = re.sub(r'(?<!LEFT\s)(?<!RIGHT\s)(?<!FULL\s)(?<!CROSS\s)(?<!NATURAL\s)\bJOIN\b',
                     'LEFT JOIN', sql, count=1, flags=re.IGNORECASE)
    return out if out != sql else None


def mut_make_cartesian(sql):
    """
    M5 CARTESIAN_PRODUCT — turn the first `JOIN ... ON ...` into a comma-join
    and remove its ON predicate. Requires a single-table base with at least
    one explicit JOIN.
    """
    # Match "JOIN tbl alias ON ..." or "JOIN tbl ON ..."
    m = re.search(r'\b(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+|NATURAL\s+)?JOIN\s+(\w+)(?:\s+(?:AS\s+)?(\w+))?\s+ON\s+',
                  sql, re.IGNORECASE)
    if not m:
        return None
    join_type_span = m.span()
    # find end of ON predicate (next JOIN / WHERE / GROUP / HAVING / ORDER / ; / end)
    tail = sql[m.end():]
    end_match = re.search(r'\b(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+|NATURAL\s+)?JOIN\b|\bWHERE\b|\bGROUP\b|\bHAVING\b|\bORDER\b|$',
                          tail, re.IGNORECASE)
    predicate_end = m.end() + (end_match.start() if end_match else len(tail))
    tbl = m.group(2)
    alias = m.group(3) or ""
    replacement = f", {tbl}" + (f" {alias}" if alias else "") + " "
    return sql[:join_type_span[0]] + replacement + sql[predicate_end:]


def mut_drop_group_by(sql):
    """
    M6 MISSING_GROUP_BY — drop GROUP BY when the SELECT contains
    both an aggregate and a non-aggregate column.
    """
    if not re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE):
        return None
    # Extract SELECT list
    m = re.search(r'\bSELECT\b(.+?)\bFROM\b', sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    select_cols = m.group(1)
    has_agg = bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', select_cols, re.IGNORECASE))
    # Count non-aggregate items roughly by splitting on top-level commas
    parts = re.split(r',(?![^()]*\))', select_cols)
    has_non_agg = any(not re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', p, re.IGNORECASE)
                      for p in parts if p.strip() and p.strip() != '*')
    if not (has_agg and has_non_agg):
        return None
    # Remove GROUP BY clause
    g = re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE)
    tail = sql[g.end():]
    end = re.search(r'\b(HAVING|ORDER\s+BY|LIMIT)\b|;|$', tail, re.IGNORECASE)
    stop = g.end() + (end.start() if end else len(tail))
    return (sql[:g.start()] + sql[stop:]).strip()


def mut_having_to_where(sql):
    """
    M7 HAVING_vs_WHERE — move the HAVING predicate into WHERE. The base
    query must have a HAVING clause.
    """
    m = re.search(r'\bHAVING\b(.+?)(\bORDER\s+BY\b|;|$)', sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    having_content = m.group(1).strip()
    # Drop HAVING entirely
    sql_no_having = (sql[:m.start()] + (sql[m.end() - len(m.group(2)):] if m.group(2) else '')).strip()
    # Splice the predicate into WHERE (or add a WHERE)
    w = re.search(r'\bWHERE\b', sql_no_having, re.IGNORECASE)
    if w:
        return sql_no_having[:w.end()] + f" {having_content} AND" + sql_no_having[w.end():]
    # No WHERE — inject one before GROUP BY
    gb = re.search(r'\bGROUP\s+BY\b', sql_no_having, re.IGNORECASE)
    if gb:
        return sql_no_having[:gb.start()] + f"WHERE {having_content} " + sql_no_having[gb.start():]
    return sql_no_having + f" WHERE {having_content}"


def mut_division_to_in(sql):
    """
    M8 IN_FOR_DIVISION — rewrite the division `NOT EXISTS (… NOT IN …)`
    into the classic IN-shape partial-match mistake:

        WHERE <outer_alias>.<outer_key> IN (
            SELECT <link_alias>.<outer_key>
            FROM <Link> <link_alias>
            JOIN Courses c ON <link_alias>.CourseID = c.CourseID
            WHERE c."Group" = '<group>'
        )

    Returns None if the base does not match the expected division shape.
    """
    pattern = re.compile(
        r"NOT\s+EXISTS\s*\(\s*"
        r"SELECT\s+\w+\.\w+\s+"
        r"FROM\s+Courses\s+c\s+"
        r"WHERE\s+c\.\"?Group\"?\s*=\s*'([^']+)'\s+"
        r"AND\s+c\.CourseID\s+NOT\s+IN\s*\(\s*"
        r"SELECT\s+(\w+)\.CourseID\s+"
        r"FROM\s+(\w+)\s+\2\s+"
        r"WHERE\s+\2\.(\w+)\s*=\s*(\w+)\.(\w+)\s*"
        r"\)\s*\)",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(sql)
    if not m:
        return None
    group_val     = m.group(1)
    link_alias    = m.group(2)
    link_table    = m.group(3)
    # m.group(4) is the link FK column (unused — we reference outer_key below).
    outer_alias   = m.group(5)
    outer_key     = m.group(6)

    replacement = (
        f"{outer_alias}.{outer_key} IN ("
        f"SELECT {link_alias}.{outer_key} "
        f"FROM {link_table} {link_alias} "
        f"JOIN Courses c ON {link_alias}.CourseID = c.CourseID "
        f"WHERE c.\"Group\" = '{group_val}'"
        f")"
    )
    return sql[:m.start()] + replacement + sql[m.end():]


def mut_remove_correlation(sql):
    """
    M9 CORRELATED_SCOPE — break the inner subquery's correlation to the outer
    by scrambling its correlated equality. Matches three shapes:
      WHERE a.c = b.c          (first predicate in subquery WHERE)
      AND   a.c = b.c          (later predicate)
            a.c = b.c AND …    (first predicate followed by AND)
    Replaces the RHS alias-qualified column with a literal so the predicate
    no longer references the outer query.
    """
    # Prefer matching inside a subquery's inner WHERE, but fall back to any
    # alias=alias equality we can find.
    patterns = [
        # `AND alias.col = alias.col` or `alias.col = alias.col AND`
        re.compile(r'\bAND\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\b', re.IGNORECASE),
        re.compile(r'\b(\w+\.\w+)\s*=\s*(\w+\.\w+)\s+AND\b', re.IGNORECASE),
        # `WHERE alias.col = alias.col` (the sole predicate)
        re.compile(r'\bWHERE\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)\b', re.IGNORECASE),
    ]
    for pat in patterns:
        m = pat.search(sql)
        if not m:
            continue
        # Replace the matched equality with a vacuously-true predicate that
        # drops the correlation.
        if pat.pattern.startswith(r'\bWHERE'):
            # Keep WHERE, neutralize the predicate.
            out = sql[:m.start()] + "WHERE 1=1" + sql[m.end():]
        else:
            # Drop the AND-joined predicate entirely.
            out = sql[:m.start()] + sql[m.end():]
        if out != sql:
            return out
    return None


def mut_union_to_intersect(sql):
    """M10 WRONG_SET_OP — flip UNION ↔ INTERSECT, or UNION → EXCEPT."""
    if re.search(r'\bUNION\s+ALL\b', sql, re.IGNORECASE):
        out = re.sub(r'\bUNION\s+ALL\b', 'INTERSECT', sql, flags=re.IGNORECASE)
    elif re.search(r'\bUNION\b', sql, re.IGNORECASE):
        out = re.sub(r'\bUNION\b', 'INTERSECT', sql, flags=re.IGNORECASE)
    elif re.search(r'\bINTERSECT\b', sql, re.IGNORECASE):
        out = re.sub(r'\bINTERSECT\b', 'UNION', sql, flags=re.IGNORECASE)
    elif re.search(r'\bEXCEPT\b', sql, re.IGNORECASE):
        out = re.sub(r'\bEXCEPT\b', 'UNION', sql, flags=re.IGNORECASE)
    else:
        return None
    return out


def mut_null_equality(sql):
    """NULL_EQUALITY — replace IS NULL with = NULL."""
    if not re.search(r'\bIS\s+NULL\b', sql, re.IGNORECASE):
        return None
    out = re.sub(r'\bIS\s+NULL\b', '= NULL', sql, flags=re.IGNORECASE)
    return out if out != sql else None


# ══════════════════════════════════════════════════════════════════════
#  MUTATION → MISCONCEPTION MAP
#  Maps the applied mutation to the misconception key(s) we expect
#  _detect_misconceptions() to return.
# ══════════════════════════════════════════════════════════════════════

MUTATIONS = [
    # (mutation_name, applicable_problem_types, mutator_fn, expected_key(s))
    ("M1_missing_where",    {"JOIN", "AGGREGATION", "SUBQUERY", "NULL"},     mut_drop_where,          "MISSING_WHERE"),
    ("M2_in_vs_exists",     {"SUBQUERY"},                                     mut_exists_to_in,        "IN_vs_EXISTS"),
    ("M3_notin_vs_notexists", {"DIVISION", "SUBQUERY"},                       mut_notexists_to_notin,  "NOT_IN_vs_NOT_EXISTS"),
    ("M4_wrong_join_type",  {"JOIN", "AGGREGATION", "SET_OP", "SUBQUERY"},   mut_inner_to_left,       "WRONG_JOIN_TYPE"),
    ("M5_cartesian",        {"JOIN", "AGGREGATION"},                          mut_make_cartesian,      "CARTESIAN_PRODUCT"),
    ("M6_missing_group_by", {"AGGREGATION"},                                  mut_drop_group_by,       "MISSING_GROUP_BY"),
    ("M7_having_vs_where",  {"AGGREGATION"},                                  mut_having_to_where,     "HAVING_vs_WHERE"),
    ("M8_in_for_division",  {"DIVISION"},                                     mut_division_to_in,      "IN_FOR_DIVISION"),
    ("M9_correlated_scope", {"DIVISION", "SUBQUERY"},                         mut_remove_correlation,  "MISSING_CORRELATED_REF"),
    ("M10_wrong_set_op",    {"SET_OP"},                                       mut_union_to_intersect,  "WRONG_SET_OP"),
    ("NULL_equality",       {"NULL"},                                         mut_null_equality,       "NULL_EQUALITY"),
]


# ══════════════════════════════════════════════════════════════════════
#  EVAL LOOP
# ══════════════════════════════════════════════════════════════════════

def evaluate():
    results = []  # list of dicts per attempted case
    by_mut = defaultdict(lambda: {"applied": 0, "mutation_failed": 0, "detected": 0, "misses": []})

    for prob in PROBLEMS:
        ptype = prob["type"]
        base_sql = prob["base_query"]
        base_parse = parse_sql(base_sql)
        if base_parse.error:
            print(f"[SKIP] problem {prob['id']} base query has parse error: {base_parse.error}")
            continue

        for mut_name, applicable_types, mutator, expected_key in MUTATIONS:
            if ptype not in applicable_types:
                continue
            mutated = mutator(base_sql)
            if mutated is None:
                by_mut[mut_name]["mutation_failed"] += 1
                continue
            by_mut[mut_name]["applied"] += 1
            try:
                student_parse = parse_sql(mutated)
                diffs = compare_queries(base_parse, student_parse)
                misconceptions = _detect_misconceptions(base_parse, student_parse, diffs, ptype)
                keys = {m["key"] for m in misconceptions}
            except Exception as e:
                keys = set()
                misconceptions = []
            detected = expected_key in keys
            if detected:
                by_mut[mut_name]["detected"] += 1
            else:
                by_mut[mut_name]["misses"].append({
                    "problem_id": prob["id"],
                    "detected_instead": sorted(list(keys)),
                })
            results.append({
                "problem_id": prob["id"],
                "problem_type": ptype,
                "mutation": mut_name,
                "expected": expected_key,
                "detected": sorted(list(keys)),
                "hit": detected,
            })

    # ── report ───────────────────────────────────────────────────
    print()
    print("═══════════════════════════════════════════════════════════════════")
    print(" PER-MISCONCEPTION DETECTION RATE")
    print("═══════════════════════════════════════════════════════════════════")
    print(f"{'Mutation':<25} {'Applied':>8} {'Detected':>10} {'Rate':>8}")
    print("-" * 67)
    total_applied = 0
    total_detected = 0
    for mut_name, _, _, _ in MUTATIONS:
        stats = by_mut[mut_name]
        applied = stats["applied"]
        detected = stats["detected"]
        rate = (detected / applied * 100) if applied else 0
        total_applied += applied
        total_detected += detected
        print(f"{mut_name:<25} {applied:>8} {detected:>10} {rate:>7.1f}%")
    print("-" * 67)
    overall = (total_detected / total_applied * 100) if total_applied else 0
    print(f"{'OVERALL':<25} {total_applied:>8} {total_detected:>10} {overall:>7.1f}%")
    print()

    # Show misses in detail for anything under 100%
    any_miss = False
    for mut_name, _, _, _ in MUTATIONS:
        misses = by_mut[mut_name]["misses"]
        if misses:
            if not any_miss:
                print("═══════════════════════════════════════════════════════════════════")
                print(" MISSES (expected not found)")
                print("═══════════════════════════════════════════════════════════════════")
                any_miss = True
            print(f"\n• {mut_name} → missed on:")
            for miss in misses:
                print(f"    {miss['problem_id']}  detected={miss['detected_instead'] or 'NONE'}")

    # Report mutations that could not be applied (for transparency)
    print()
    print("═══════════════════════════════════════════════════════════════════")
    print(" MUTATION APPLICABILITY")
    print("═══════════════════════════════════════════════════════════════════")
    for mut_name, applicable_types, _, _ in MUTATIONS:
        stats = by_mut[mut_name]
        total_possible = stats["applied"] + stats["mutation_failed"]
        print(f"{mut_name:<25} applied={stats['applied']:>3} / possible={total_possible:>3}   "
              f"types={sorted(applicable_types)}")

    return results, by_mut


if __name__ == "__main__":
    evaluate()

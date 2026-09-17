"""Operators and sampling for the F3 residual probe corpus.

Two properties are worth pinning above the rest, because both were broken in
ways that produced a *plausible-looking* corpus rather than an obviously broken
one.

First, every operator must emit SQL the engine accepts. `NOT IN` matched as if
`NOT` were the column name yielded `<col> EXISTS (...)`, which fails to run;
because a failed run diverges from the reference, those cases were admitted as
residual on the strength of their own syntax error. A submission an engine
rejects is not a misconception for an instructor to name.

Second, the sampler must spread across strata. F3c holds several times more
cases than distinct (database, extra table) patterns, so a uniform draw repeats
the same corruption; a rater gives repeated items the same answer and an
agreement statistic reads that as consensus.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.sql_parser import parse_sql, _extract_top_level_clauses   # noqa: E402
from external.spider.gen_residual import (                             # noqa: E402
    op_uncorrelated_exists, op_weaken_predicate, op_extraneous_join,
    _stratum, sample, FAMILIES,
)


def _apply(fn, sql, *extra):
    return fn(sql, parse_sql(sql), _extract_top_level_clauses(sql), *extra)


# ── F3a: uncorrelated EXISTS ─────────────────────────────────────────────────

def test_in_subquery_becomes_uncorrelated_exists():
    out = _apply(op_uncorrelated_exists,
                 "SELECT name FROM artist WHERE aid IN (SELECT aid FROM album)")
    assert "EXISTS (SELECT aid FROM album)" in out
    assert " IN " not in out


def test_not_in_keeps_its_negation():
    # Matching NOT as the column name produced `<col> EXISTS (...)`, which does
    # not parse. The negation has to survive the rewrite.
    out = _apply(op_uncorrelated_exists,
                 "SELECT a FROM t WHERE b NOT IN (SELECT c FROM u)")
    assert "NOT EXISTS (SELECT c FROM u)" in out
    assert parse_sql(out).error is None


def test_in_term_among_other_conjuncts_is_rewritten_alone():
    out = _apply(op_uncorrelated_exists,
                 "SELECT a FROM t WHERE b = 1 AND c IN (SELECT d FROM u)")
    assert "b = 1 AND EXISTS (SELECT d FROM u)" in out


def test_nested_in_is_left_alone():
    out = _apply(op_uncorrelated_exists,
                 "SELECT a FROM t WHERE x IN (SELECT y FROM u WHERE z IN "
                 "(SELECT w FROM v))")
    assert out.count("EXISTS") == 1
    assert "z IN (SELECT w FROM v)" in out


def test_value_list_is_not_a_subquery():
    assert _apply(op_uncorrelated_exists, "SELECT a FROM t WHERE x IN (1,2,3)") is None


def test_no_in_clause_yields_nothing():
    assert _apply(op_uncorrelated_exists, "SELECT a FROM t WHERE x = 5") is None


# ── F3b: boundary shift ──────────────────────────────────────────────────────

@pytest.mark.parametrize("before,after", [
    (">=", ">"), ("<=", "<"), (">", ">="), ("<", "<="),
])
def test_boundary_shifts_by_one_position(before, after):
    out = _apply(op_weaken_predicate, "SELECT a FROM t WHERE age %s 3" % before)
    assert out.endswith("WHERE age %s 3" % after)


def test_having_boundary_is_shifted_when_where_has_none():
    out = _apply(op_weaken_predicate,
                 "SELECT a FROM t WHERE b = 1 GROUP BY a HAVING COUNT(*) > 1")
    assert "HAVING COUNT(*) >= 1" in out
    assert "WHERE b = 1" in out          # the equality filter is untouched


def test_where_takes_precedence_over_having():
    # Pins the ordering that keeps every pre-HAVING case reproducing unchanged.
    out = _apply(op_weaken_predicate,
                 "SELECT a FROM t WHERE age >= 3 GROUP BY a HAVING COUNT(*) > 1")
    assert "WHERE age > 3" in out
    assert "HAVING COUNT(*) > 1" in out


def test_comparison_inside_a_subquery_is_not_touched():
    assert _apply(op_weaken_predicate,
                  "SELECT a FROM t WHERE b = (SELECT max(c) FROM u WHERE d > 2)") is None


def test_inequality_operators_are_not_boundaries():
    assert _apply(op_weaken_predicate, "SELECT a FROM t WHERE b <> 3") is None
    assert _apply(op_weaken_predicate, "SELECT a FROM t WHERE b != 3") is None


# ── F3c: extraneous join ─────────────────────────────────────────────────────

_FKS = [("Takes", "StuID", "Students", "StuID")]


def test_extraneous_join_adds_an_fk_reachable_table():
    out = _apply(op_extraneous_join, "SELECT Name FROM Students", _FKS)
    assert "JOIN Takes ON Takes.StuID = Students.StuID" in out


def test_alias_is_used_to_qualify_when_one_is_declared():
    out = _apply(op_extraneous_join, "SELECT s.Name FROM Students s", _FKS)
    assert "= s.StuID" in out


def test_table_already_in_from_is_not_added_again():
    sql = "SELECT s.Name FROM Students s JOIN Takes t ON s.StuID = t.StuID"
    assert _apply(op_extraneous_join, sql, _FKS) is None


def test_outer_join_shapes_are_skipped():
    sql = "SELECT s.Name FROM Students s LEFT JOIN Takes t ON s.StuID = t.StuID"
    assert _apply(op_extraneous_join, sql, _FKS) is None


# ── stratification and sampling ──────────────────────────────────────────────

def _entry(family, db, gold, wrong, outcome="RESIDUAL"):
    return {"family": family, "db_id": db, "gold_sql": gold,
            "wrong_sql": wrong, "outcome": outcome}


def test_f3c_stratum_is_the_database_and_the_added_table():
    e = _entry("F3c", "uni", "SELECT a FROM Students",
               "SELECT a FROM Students JOIN Takes ON Takes.StuID = Students.StuID")
    assert _stratum(e) == ("uni", "takes")


def test_f3b_stratum_separates_having_from_where():
    where = _entry("F3b", "uni", "SELECT a FROM t WHERE x >= 1",
                   "SELECT a FROM t WHERE x > 1")
    having = _entry("F3b", "uni", "SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1",
                    "SELECT a FROM t GROUP BY a HAVING COUNT(*) >= 1")
    assert _stratum(where)[0] == "WHERE"
    assert _stratum(having)[0] == "HAVING"


def _f3c_corpus(n_strata, per_stratum):
    return [_entry("F3c", "db%d" % s,
                   "SELECT a FROM base",
                   "SELECT a FROM base JOIN extra%d ON extra%d.k = base.k" % (s, s))
            for s in range(n_strata) for _ in range(per_stratum)]


def test_f3a_stratum_leads_with_the_rewrite_shape():
    # The database alone made every stratum "distinct" while 28 of 30 drawn
    # cases were the same NOT IN -> NOT EXISTS rewrite. Shape has to lead.
    neg = _entry("F3a", "uni", "SELECT a FROM t WHERE x NOT IN (SELECT y FROM u)",
                 "SELECT a FROM t WHERE NOT EXISTS (SELECT y FROM u)")
    plain = _entry("F3a", "uni", "SELECT a FROM t WHERE x IN (SELECT y FROM u)",
                   "SELECT a FROM t WHERE EXISTS (SELECT y FROM u)")
    assert _stratum(neg)[0] == "NOT EXISTS"
    assert _stratum(plain)[0] == "EXISTS"


def test_sample_balances_the_leading_axis_against_a_lopsided_corpus():
    # Twenty of one shape across twenty databases, five of the other across
    # five. Keyed on the database the majority would take four draws in five.
    corpus = [_entry("F3a", "db%d" % i,
                     "SELECT a FROM t WHERE x NOT IN (SELECT y FROM u)",
                     "SELECT a FROM t WHERE NOT EXISTS (SELECT y FROM u%d)" % i)
              for i in range(20)]
    corpus += [_entry("F3a", "p%d" % i,
                      "SELECT a FROM t WHERE x IN (SELECT y FROM u)",
                      "SELECT a FROM t WHERE EXISTS (SELECT y FROM u%d)" % i)
               for i in range(5)]
    shapes = [e["stratum"][0] for e in sample(corpus, per_family=10)]
    assert shapes.count("NOT EXISTS") == 5
    assert shapes.count("EXISTS") == 5


def test_sample_takes_the_surplus_when_one_side_runs_out():
    corpus = [_entry("F3a", "db%d" % i,
                     "SELECT a FROM t WHERE x NOT IN (SELECT y FROM u)",
                     "SELECT a FROM t WHERE NOT EXISTS (SELECT y FROM u%d)" % i)
              for i in range(20)]
    corpus += [_entry("F3a", "p0",
                      "SELECT a FROM t WHERE x IN (SELECT y FROM u)",
                      "SELECT a FROM t WHERE EXISTS (SELECT y FROM u0)")]
    shapes = [e["stratum"][0] for e in sample(corpus, per_family=10)]
    assert shapes.count("EXISTS") == 1        # only one exists
    assert shapes.count("NOT EXISTS") == 9    # the rest comes from the majority


def test_sample_prefers_distinct_strata_over_repeats():
    # Ten patterns, six copies each. A uniform draw of ten would repeat; the
    # round robin should return one of each before taking any second.
    drawn = sample(_f3c_corpus(10, 6), per_family=10)
    assert len(drawn) == 10
    assert len({tuple(e["stratum"]) for e in drawn}) == 10


def test_sample_is_deterministic():
    corpus = _f3c_corpus(10, 6)
    first = [e["wrong_sql"] for e in sample(corpus, per_family=7)]
    assert first == [e["wrong_sql"] for e in sample(corpus, per_family=7)]


def test_sample_stops_at_what_the_corpus_holds():
    drawn = sample(_f3c_corpus(3, 2), per_family=30)
    assert len(drawn) == 6            # asked for 30, corpus has 6


def test_sample_ignores_excluded_outcomes():
    corpus = _f3c_corpus(4, 1)
    for e in corpus[:2]:
        e["outcome"] = "LATENT"
    corpus[2]["outcome"] = "EXEC_ERROR"
    assert len(sample(corpus, per_family=30)) == 1


def test_every_family_is_registered_with_a_literature_basis():
    assert [f for f, _o, _r, _l in FAMILIES] == ["F3a", "F3b", "F3c"]
    for _f, _o, residual_of, literature in FAMILIES:
        assert residual_of.startswith("M")
        assert literature.strip()

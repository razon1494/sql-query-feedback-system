"""Agreement statistics and blindness guarantees for instructor adjudication.

The agreement figure is a reported result, so the things that can quietly
falsify it are what these tests pin.

`krippendorff_alpha` is checked against cases whose value can be worked out by
hand, including the negative one — alpha below zero is a real outcome for
systematic disagreement, not a bug to clamp away.

`_refuse_identical` guards the failure this project has already had: a Cohen's
kappa was discarded once because the second annotator's sheet had been derived
from the first, so the statistic measured the copying. Sheets agreeing on every
single item are that signature, and the tool must say so rather than print a
flattering number.

The sheet itself must carry no trace of the family, the operator or the
system's own output. A rater who can see what the system said is checking it,
not judging independently.
"""
import os
import sys
import csv
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from external.spider.adjudication import (                            # noqa: E402
    krippendorff_alpha, _refuse_identical, _read_sheet, agreement_report,
    execution_evidence, LABEL_VOCAB, JUDGEMENT_LABELS, SHEET_COLUMNS,
)


# ── Krippendorff's alpha ─────────────────────────────────────────────────────

def test_total_agreement_is_one():
    units = {"1": ["X", "X"], "2": ["X", "X"], "3": ["Y", "Y"], "4": ["Y", "Y"]}
    assert krippendorff_alpha(units) == 1.0


def test_systematic_disagreement_is_negative():
    # Two units, two coders, reversed every time: hand-computed alpha is -0.5.
    # Alpha is allowed below zero — disagreement worse than chance is a result.
    assert krippendorff_alpha({"1": ["X", "Y"], "2": ["Y", "X"]}) == -0.5


def test_a_single_rating_cannot_be_paired():
    assert krippendorff_alpha({"1": ["X"]}) is None


def test_blank_labels_drop_out_without_deleting_the_item():
    # Cohen's kappa would discard the whole unit; alpha lets the rest stand.
    units = {"1": ["X", "X"], "2": ["Y", ""], "3": ["Y", "Y"]}
    assert krippendorff_alpha(units) == 1.0


def test_three_raters_are_accepted():
    alpha = krippendorff_alpha({"1": ["X", "X", "Y"], "2": ["X", "X", "X"],
                                "3": ["Y", "Y", "Y"], "4": ["Y", "Y", "X"]})
    assert alpha is not None
    assert 0.0 < alpha < 1.0


def test_one_category_everywhere_is_not_a_division_by_zero():
    assert krippendorff_alpha({"1": ["X", "X"], "2": ["X", "X"]}) == 1.0


# ── independence guard ───────────────────────────────────────────────────────

def test_identical_sheets_are_refused():
    a = {"A001": "MISSING_WHERE", "A002": "WRONG_UNDIAGNOSED"}
    assert _refuse_identical([a, dict(a)], ["a.csv", "b.csv"]) is True


def test_sheets_that_differ_anywhere_are_accepted():
    a = {"A001": "MISSING_WHERE", "A002": "WRONG_UNDIAGNOSED"}
    b = {"A001": "MISSING_WHERE", "A002": "AMBIGUOUS"}
    assert _refuse_identical([a, b], ["a.csv", "b.csv"]) is False


def test_a_single_shared_item_is_too_little_to_call_copying():
    a = {"A001": "MISSING_WHERE", "A002": ""}
    b = {"A001": "MISSING_WHERE", "A003": "AMBIGUOUS"}
    assert _refuse_identical([a, b], ["a.csv", "b.csv"]) is False


def _write_sheet(path, labels):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["item_id", "label"])
        for item, lab in labels.items():
            w.writerow([item, lab])
    return str(path)


def test_agreement_report_refuses_rather_than_reporting(tmp_path, capsys):
    labels = {"A001": "MISSING_WHERE", "A002": "AMBIGUOUS", "A003": "OTHER"}
    a = _write_sheet(tmp_path / "a.csv", labels)
    b = _write_sheet(tmp_path / "b.csv", labels)
    out = agreement_report([a, b])
    assert out.get("refused") is True
    assert "alpha" not in out
    assert "REFUSED" in capsys.readouterr().out


def test_agreement_report_counts_the_disputed_items(tmp_path):
    a = _write_sheet(tmp_path / "a.csv",
                     {"A001": "MISSING_WHERE", "A002": "AMBIGUOUS",
                      "A003": "OTHER", "A004": "WRONG_UNDIAGNOSED"})
    b = _write_sheet(tmp_path / "b.csv",
                     {"A001": "MISSING_WHERE", "A002": "OTHER",
                      "A003": "OTHER", "A004": "CORRECT_ALTERNATIVE"})
    out = agreement_report([a, b])
    assert out["items_multi_rated"] == 4
    assert out["disputed"] == 2
    assert out["adjudication_rate"] == 0.5


def test_only_the_primary_label_is_read_from_a_multi_label_cell(tmp_path):
    path = _write_sheet(tmp_path / "a.csv", {"A001": "missing_where;other"})
    assert _read_sheet(path) == {"A001": "MISSING_WHERE"}


# ── vocabulary and blindness ─────────────────────────────────────────────────

def test_the_four_judgements_phase_three_requires_are_present():
    for label in ("CORRECT_ALTERNATIVE", "WRONG_UNDIAGNOSED", "AMBIGUOUS", "OTHER"):
        assert label in JUDGEMENT_LABELS
        assert label in LABEL_VOCAB


def test_the_vocabulary_is_closed_and_free_of_duplicates():
    assert len(LABEL_VOCAB) == len(set(LABEL_VOCAB))


def test_the_sheet_asks_for_a_label_and_offers_the_evidence():
    for column in ("task", "schema", "reference_query", "submission_query",
                   "execution_evidence", "label"):
        assert column in SHEET_COLUMNS


def test_the_sheet_has_no_column_for_the_systems_own_output():
    # A rater who can see the diagnosis is checking it, not supplying one.
    for column in SHEET_COLUMNS:
        assert "detect" not in column
        assert "family" not in column
        assert "operator" not in column


def _pets_db(tmp_path, rows):
    db = tmp_path / "t.sqlite"
    con = sqlite3.connect(str(db))
    con.execute("CREATE TABLE pets (petid INTEGER, pet_age INTEGER)")
    con.executemany("INSERT INTO pets VALUES (?, ?)", rows)
    con.commit()
    con.close()
    return str(db)


def test_execution_evidence_reports_rows_and_names_no_misconception(tmp_path):
    db = _pets_db(tmp_path, [(1, 1), (2, 3), (3, 5)])
    text = execution_evidence({
        "sqlite_path": db,
        "gold_sql": "SELECT petid FROM pets WHERE pet_age > 1",
        "wrong_sql": "SELECT petid FROM pets WHERE pet_age >= 1",
        "edge_db_paths": [],
    })
    assert "reference returned 2 row(s), submission returned 3 row(s)" in text
    assert "(1)" in text                        # the boundary row is shown
    for leak in ("MISSING_WHERE", "boundary", "misconception", "F3b", "should"):
        assert leak not in text


def test_execution_evidence_reports_an_engine_rejection_verbatim(tmp_path):
    db = _pets_db(tmp_path, [(1, 1)])
    text = execution_evidence({
        "sqlite_path": db,
        "gold_sql": "SELECT petid FROM pets",
        "wrong_sql": "SELECT petid FROM pets WHERE nope EXISTS (SELECT 1)",
        "edge_db_paths": [],
    })
    assert "the submission did not run" in text

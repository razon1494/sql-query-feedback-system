"""
Phase 3 (IJAIED journal) — instructor adjudication: blind sheets and agreement.

A separate module from `annotation.py`, which stays as it is. That harness
serves the ICWL wrong-query corpus, where every case carries an `intended_label`
— the misconception its corruption operator was built to induce — and the
question put to a rater is whether they agree with it.

Here there is no intended label, and that absence is the point. Every case in
the residual corpus is one the deterministic core found wrong and could not
name: no element of M1–M10 applies, by construction. What the instructors supply
is the ground truth that does not otherwise exist, against which a constrained
LLM's diagnosis can later be scored. So the sealed key records the *family* a
case came from, never a label, and a rater who writes WRONG_UNDIAGNOSED has
given a real answer rather than failed to give one.

**What a rater sees:** the task, the schema, the reference query, the
submission, and controlled execution evidence — what the two queries actually
returned, on the shipped database and on any constructed instances.

**What a rater never sees:** the system's output, the family, the operator, or
the other rater's sheet. Section 8.2 of the draft asks each label to have
independent support; an agreement figure computed over sheets that shared a
source measures the sharing, not the agreement. This project has already lost a
Cohen's kappa that way — the second annotator's sheet had been derived from the
first — so `PROTOCOL.md` is written out beside the sheets, and the agreement
command refuses sheets whose labels are identical on every shared item.

Agreement is reported as **Krippendorff's alpha**, which unlike Cohen's kappa
takes any number of raters and tolerates items a rater left blank. The
adjudication rate — the share of items the raters disagreed on, which a third
must resolve — is reported alongside, since it determines how much of the third
instructor's time the design actually costs.

Usage:
    python external/spider/adjudication.py make-sheets --raters 2
    python external/spider/adjudication.py agreement --sheets a.csv b.csv
    python external/spider/adjudication.py agreement --sheets a.csv b.csv c.csv
"""
import os
import re
import sys
import csv
import json
import sqlite3
import argparse
from collections import Counter, defaultdict
from typing import Optional, Dict, List, Tuple, Any

_EXTERNAL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_ROOT = os.path.dirname(_EXTERNAL_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from external.spider.ingest import get_schema                    # noqa: E402

_DERIVED_DIR = os.path.join(_EXTERNAL_DIR, "data", "derived")
_ADJ_DIR = os.path.join(_DERIVED_DIR, "adjudication")

MAX_EVIDENCE_ROWS = 3        # example tuples shown per direction
MAX_SCHEMA_TABLES = 12       # beyond this the schema is summarised


# ── the closed vocabulary ────────────────────────────────────────────────────

# The ten audited misconceptions, plus the NULL-equality detector, exactly as
# the taxonomy names them.
MISCONCEPTION_LABELS = [
    "MISSING_WHERE", "WRONG_JOIN_TYPE", "CARTESIAN_PRODUCT", "MISSING_GROUP_BY",
    "HAVING_vs_WHERE", "WRONG_SET_OP", "IN_FOR_DIVISION", "NOT_IN_vs_NOT_EXISTS",
    "MISSING_CORRELATED_REF", "IN_vs_EXISTS", "NULL_EQUALITY",
]

# The four judgements that are not misconceptions. PIPELINE Phase 3 requires all
# four: without CORRECT_ALTERNATIVE a rater cannot say the submission is simply
# a different correct answer, and without WRONG_UNDIAGNOSED they cannot say it
# is wrong for a reason the vocabulary does not cover — which for this corpus is
# a substantive finding, not a shrug.
JUDGEMENT_LABELS = [
    "CORRECT_ALTERNATIVE",   # different from the reference, but right
    "WRONG_UNDIAGNOSED",     # wrong, no listed misconception explains it
    "AMBIGUOUS",             # the task admits more than one reading
    "OTHER",                 # wrong for a nameable reason not listed
]

LABEL_VOCAB = MISCONCEPTION_LABELS + JUDGEMENT_LABELS

# One line per label, so two raters are at least reading the same definition.
# Left undefined, a label means whatever each instructor takes it to mean, and
# the disagreement that follows is about wording rather than about the query —
# which would show up in the agreement figure as if it were the real thing.
# The misconception wording follows Table 1 of the deterministic paper; the
# M-numbers are given so a rater can check the source.
LABEL_HELP = {
    "MISSING_WHERE": "(M1) The reference filters rows with a WHERE clause and "
                     "the submission does not, or the reverse.",
    "IN_vs_EXISTS": "(M2) The submission uses IN where the reference uses "
                    "EXISTS, or the reverse.",
    "NOT_IN_vs_NOT_EXISTS": "(M3) The submission uses NOT IN where the "
                            "reference uses NOT EXISTS, or the reverse — the "
                            "pair that diverges when the subquery yields NULL.",
    "WRONG_JOIN_TYPE": "(M4) The join types differ: an outer join where the "
                       "reference joins inner, or the reverse.",
    "CARTESIAN_PRODUCT": "(M5) The submission joins fewer tables than the "
                         "reference, or joins them with no linking condition.",
    "MISSING_GROUP_BY": "(M6) An aggregate is computed without the GROUP BY "
                        "the reference uses.",
    "HAVING_vs_WHERE": "(M7) An aggregate condition sits in WHERE instead of "
                       "HAVING.",
    "IN_FOR_DIVISION": "(M8) The task asks for *all* of something — relational "
                       "division — and the submission tests membership with IN.",
    "MISSING_CORRELATED_REF": "(M9) A subquery that should refer to the outer "
                              "row does not, so its result no longer depends "
                              "on the row being tested.",
    "WRONG_SET_OP": "(M10) The set operator differs from the reference's, or "
                    "is absent.",
    "NULL_EQUALITY": "`= NULL` or `<> NULL` is used where `IS NULL` / "
                     "`IS NOT NULL` is required.",
    "CORRECT_ALTERNATIVE": "The submission answers the task correctly, even "
                           "though it is written differently from the reference.",
    "WRONG_UNDIAGNOSED": "The submission is wrong, but none of the listed "
                         "misconceptions describes why. This is a real answer.",
    "AMBIGUOUS": "The task wording admits more than one defensible reading, or "
                 "the reference query does not answer the task, so the "
                 "submission cannot fairly be called wrong. Say which in notes.",
    "OTHER": "Wrong for a specific reason not in the list. Name it in notes.",
}


# ── schema and execution evidence ────────────────────────────────────────────

def _fk_lines(sqlite_path: str) -> List[str]:
    out: List[str] = []
    try:
        con = sqlite3.connect(sqlite_path)
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        for t in tables:
            for row in con.execute('PRAGMA foreign_key_list("%s")' % t):
                if row[2] and row[3] and row[4]:
                    out.append("%s.%s -> %s.%s" % (t, row[3], row[2], row[4]))
        con.close()
    except sqlite3.Error:
        return []
    return sorted(set(out))


def _schema_text(sqlite_path: str, queries: Tuple[str, ...] = ()) -> str:
    """The whole schema, not only the tables the reference happens to use.

    A rater judging whether an extra join is legitimate has to be able to see
    the table that was joined; showing only the reference's tables would make
    every extraneous-join case look like a reference to something that does not
    exist, which is a different error entirely.

    Large schemas are capped, but never in a way that hides a table one of the
    two queries names — those are pulled to the front first. Without that the
    cap and the corpus interact: the table an extraneous join adds is chosen by
    foreign key, not alphabetically, so on a wide database it can fall past the
    cut and leave the rater judging a join to a table they cannot see.
    """
    try:
        schema = get_schema(sqlite_path)
    except Exception:
        return "(schema unavailable)"
    mentioned = {t for t in schema
                 if any(re.search(r"%s" % re.escape(t), q, re.IGNORECASE)
                        for q in queries)}
    names = sorted(mentioned) + [t for t in sorted(schema) if t not in mentioned]
    lines = ["%s(%s)" % (t, ", ".join(schema[t])) for t in names[:MAX_SCHEMA_TABLES]]
    if len(names) > MAX_SCHEMA_TABLES:
        lines.append("... and %d further tables" % (len(names) - MAX_SCHEMA_TABLES))
    fks = _fk_lines(sqlite_path)
    if fks:
        lines.append("FOREIGN KEYS: " + "; ".join(fks))
    return "\n".join(lines)


def _run(db_path: str, sql: str) -> Tuple[Optional[List[Tuple]], Optional[str]]:
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute(sql).fetchall()
        con.close()
        return rows, None
    except sqlite3.Error as exc:
        return None, str(exc)


def _fmt_rows(rows: List[Tuple], limit: int = MAX_EVIDENCE_ROWS) -> str:
    shown = ["(%s)" % ", ".join(repr(v) for v in r) for r in rows[:limit]]
    if len(rows) > limit:
        shown.append("... and %d more" % (len(rows) - limit))
    return " ".join(shown)


def _compare(db_path: str, gold: str, cand: str, label: str) -> List[str]:
    """One instance's worth of factual divergence, naming no misconception."""
    g_rows, g_err = _run(db_path, gold)
    c_rows, c_err = _run(db_path, cand)
    if g_err:
        return ["%s: the reference query did not run (%s)." % (label, g_err)]
    if c_err:
        return ["%s: the submission did not run. The database reported: %s"
                % (label, c_err)]
    g_cnt, c_cnt = Counter(g_rows), Counter(c_rows)
    extra = list((c_cnt - g_cnt).elements())
    missing = list((g_cnt - c_cnt).elements())
    out = ["%s: reference returned %d row(s), submission returned %d row(s)."
           % (label, len(g_rows), len(c_rows))]
    if not extra and not missing:
        out.append("  The two results are identical on this instance.")
        return out
    if extra:
        out.append("  Submission returns %d row(s) the reference does not: %s"
                   % (len(extra), _fmt_rows(extra)))
    if missing:
        out.append("  Submission omits %d row(s) the reference returns: %s"
                   % (len(missing), _fmt_rows(missing)))
    return out


def execution_evidence(entry: Dict) -> str:
    """Controlled evidence: what ran, and what came back.

    Controlled means the rater is given the observation and not the conclusion.
    Row counts, the rows that differ and the engine's own error text are facts
    about an execution; naming the misconception behind them is the judgement
    being asked for, so it is absent here and from every other column.
    """
    parts = _compare(entry["sqlite_path"], entry["gold_sql"], entry["wrong_sql"],
                     "On the course database")
    for path in entry.get("edge_db_paths", []):
        name = os.path.splitext(os.path.basename(path))[0]
        name = re.sub(r"^edge_", "", name).replace("_", " ")
        parts.extend(_compare(path, entry["gold_sql"], entry["wrong_sql"],
                              "On the constructed instance '%s'" % name))
    return "\n".join(parts)


# ── sheet construction ───────────────────────────────────────────────────────

SHEET_COLUMNS = ["item_id", "task", "schema", "reference_query",
                 "submission_query", "execution_evidence",
                 "label", "confidence", "notes"]


def make_sheets(sample_path: Optional[str] = None, raters: int = 2,
                outdir: Optional[str] = None) -> Dict[str, Any]:
    """Write one identical blind sheet per rater, plus a sealed key."""
    sample_path = sample_path or os.path.join(
        _DERIVED_DIR, "residual_sample_dev.json")
    outdir = outdir or _ADJ_DIR
    with open(sample_path, "r", encoding="utf-8") as fh:
        items = json.load(fh)
    os.makedirs(outdir, exist_ok=True)

    rows: List[Dict[str, str]] = []
    key_rows: List[List[str]] = []
    for i, e in enumerate(items):
        item_id = "A%03d" % (i + 1)
        rows.append({
            "item_id": item_id,
            "task": e["question"],
            "schema": _schema_text(e["sqlite_path"],
                                   (e["gold_sql"], e["wrong_sql"])),
            "reference_query": e["gold_sql"],
            "submission_query": e["wrong_sql"],
            "execution_evidence": execution_evidence(e),
            "label": "", "confidence": "", "notes": "",
        })
        key_rows.append([item_id, e["family"], e["db_id"],
                         ";".join(e.get("detected_keys", [])),
                         e.get("split", ""), e.get("problem_ref", "")])

    written = []
    for r in range(1, raters + 1):
        path = os.path.join(outdir, "sheet_rater%d.csv" % r)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=SHEET_COLUMNS)
            w.writeheader()
            w.writerows(rows)
        written.append(path)

    key_path = os.path.join(outdir, "answer_key.csv")
    with open(key_path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["item_id", "family", "db_id", "detector_labels",
                    "split", "problem_ref"])
        w.writerows(key_rows)

    protocol_path = os.path.join(outdir, "PROTOCOL.md")
    with open(protocol_path, "w", encoding="utf-8") as fh:
        fh.write(_protocol_text(len(rows), raters))

    print("items          : %d" % len(rows))
    for p in written:
        print("blind sheet    : %s" % p)
    print("sealed key     : %s   (not to be opened before labelling ends)"
          % key_path)
    print("protocol       : %s" % protocol_path)
    return {"items": len(rows), "sheets": written, "key": key_path,
            "protocol": protocol_path}


def _protocol_text(n_items: int, raters: int) -> str:
    vocab = "\n".join(
        "- `%s`%s" % (lab, "  — " + LABEL_HELP[lab] if lab in LABEL_HELP else "")
        for lab in LABEL_VOCAB)
    return """# Adjudication protocol

Thank you for agreeing to do this. It is %d items and should take about two to
three hours. Nothing to install: one spreadsheet, filled in whenever suits you.

## What you are being asked

For each item you see a task, the database schema, a reference query, a
student-style submission, and what the two queries actually returned when run.
Decide **why the submission differs from the reference**, and put one or more
labels in the `label` column.

You are **not** checking a system's output. You are supplying the judgement
that does not otherwise exist.

## The vocabulary

Put the primary label first; separate multiple labels with a semicolon.

%s

`WRONG_UNDIAGNOSED` is a real answer, not a way of skipping an item. Some of
these submissions are wrong for reasons the list genuinely does not cover, and
recording that is as useful to us as any other label. The same goes for
`CORRECT_ALTERNATIVE`: if the submission answers the task correctly in its own
way, say so — we would far rather learn that than have it labelled as an error.

`confidence` is optional: `high`, `medium` or `low`. `notes` is free text and
is especially welcome on `OTHER` and `AMBIGUOUS`.

## If the reference query itself looks wrong

It sometimes will. These tasks and reference queries come from a public dataset
that we did not write, and a few of the references do not answer the task they
are paired with. You are not expected to defend them.

When that happens, label the item `AMBIGUOUS` and say in `notes` that the
problem is the reference rather than the submission. Do not try to guess what
the reference *meant* and judge the submission against that — tell us, and we
will drop the item. An item resting on a broken reference is one we should not
have sent you, and finding those is one of the useful things this pass does.

## Independence — please read this part

There are %d of you labelling the same items, and a third instructor will
resolve any disagreements. The agreement between your sheets is itself a
reported result, so it only means anything if the sheets are produced
independently.

Concretely, please:

- **do not** discuss items, labels or general approach with the other rater
  until both sheets are returned;
- **do not** work from a copy of anyone else's sheet, even a blank-looking one;
- **do not** ask us what we think an item should be — if something is unclear,
  say so in `notes` and use `AMBIGUOUS`;
- leave a label blank rather than guessing at what you think we want.

This is not a formality. An earlier agreement figure in this project had to be
discarded because the second sheet turned out to have been derived from the
first, which meant the statistic measured the copying rather than the
agreement. We would rather have a low honest number than a high meaningless
one.

## Returning the sheet

Send the CSV back with the `label` column filled. Keep the `item_id` values
exactly as they are — they are how the sheets are matched up.
""" % (n_items, vocab, raters)


# ── agreement ────────────────────────────────────────────────────────────────

def _read_sheet(path: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            item = (row.get("item_id") or "").strip()
            if not item:
                continue
            out[item] = (row.get("label") or "").split(";")[0].strip().upper()
    return out


def krippendorff_alpha(units: Dict[str, List[str]]) -> Optional[float]:
    """Nominal-scale alpha over items that at least two raters labelled.

    Chosen over Cohen's kappa because it takes any number of raters and does
    not require every rater to have labelled every item — a rater who leaves an
    item blank contributes nothing to that unit rather than deleting it from
    everyone's count.
    """
    coincidence: Dict[Tuple[str, str], float] = defaultdict(float)
    for values in units.values():
        vals = [v for v in values if v]
        m = len(vals)
        if m < 2:
            continue
        counts = Counter(vals)
        for c in counts:
            for k in counts:
                pairs = (counts[c] * (counts[c] - 1) if c == k
                         else counts[c] * counts[k])
                coincidence[(c, k)] += pairs / (m - 1)
    if not coincidence:
        return None
    marginal: Dict[str, float] = defaultdict(float)
    for (c, _k), v in coincidence.items():
        marginal[c] += v
    n = sum(marginal.values())
    if n <= 1:
        return None
    observed = sum(v for (c, k), v in coincidence.items() if c != k)
    expected = sum(marginal[c] * marginal[k]
                   for c in marginal for k in marginal if c != k) / (n - 1)
    if expected == 0:
        return 1.0
    return 1.0 - observed / expected


def _refuse_identical(sheets: List[Dict[str, str]], names: List[str]) -> bool:
    """Guard against the failure that invalidated this project's first kappa."""
    for i in range(len(sheets)):
        for j in range(i + 1, len(sheets)):
            a, b = sheets[i], sheets[j]
            shared = [k for k in a if k in b and a[k] and b[k]]
            if len(shared) >= 2 and all(a[k] == b[k] for k in shared):
                print("REFUSED: %s and %s agree on every one of %d labelled "
                      "items." % (names[i], names[j], len(shared)))
                print("Identical sheets are the signature of one being derived "
                      "from the other; see PROTOCOL.md. Confirm the sheets were "
                      "produced independently before reporting any agreement "
                      "figure from them.")
                return True
    return False


def agreement_report(sheet_paths: List[str], key_path: Optional[str] = None
                     ) -> Dict[str, Any]:
    """Alpha, adjudication rate, and per-label agreement across raters."""
    sheets = [_read_sheet(p) for p in sheet_paths]
    names = [os.path.basename(p) for p in sheet_paths]
    if _refuse_identical(sheets, names):
        return {"refused": True, "reason": "identical sheets"}

    units: Dict[str, List[str]] = defaultdict(list)
    for s in sheets:
        for item, label in s.items():
            units[item].append(label)

    labelled = {i: [v for v in vs if v] for i, vs in units.items()}
    multi = {i: vs for i, vs in labelled.items() if len(vs) >= 2}
    disputed = [i for i, vs in multi.items() if len(set(vs)) > 1]
    alpha = krippendorff_alpha(units)

    print("=" * 74)
    print("Adjudication agreement — %d raters" % len(sheets))
    print("=" * 74)
    print("items labelled by 2+ raters : %d" % len(multi))
    print("Krippendorff's alpha        : %s"
          % ("n/a" if alpha is None else round(alpha, 3)))
    print("adjudication rate           : %s"
          % ("n/a" if not multi
             else "%.1f%% (%d of %d need the third rater)"
                  % (100.0 * len(disputed) / len(multi), len(disputed), len(multi))))
    print()
    print("%-24s%8s%11s%10s" % ("label", "used", "unanimous", "disputed"))
    counts = Counter(v for vs in labelled.values() for v in vs)
    for lab in LABEL_VOCAB:
        if not counts[lab]:
            continue
        unan = sum(1 for vs in multi.values() if len(set(vs)) == 1 and vs[0] == lab)
        disp = sum(1 for i in disputed if lab in multi[i])
        print("%-24s%8d%11d%10d" % (lab, counts[lab], unan, disp))
    unknown = sorted(set(counts) - set(LABEL_VOCAB))
    if unknown:
        print("\noutside the closed vocabulary (check for typos): %s"
              % ", ".join(unknown))

    report = {"raters": len(sheets), "items_multi_rated": len(multi),
              "alpha": alpha, "disputed": len(disputed),
              "adjudication_rate": (len(disputed) / len(multi)) if multi else None,
              "label_counts": dict(counts)}
    if key_path and os.path.exists(key_path):
        report["by_family"] = _family_breakdown(multi, key_path)
    return report


def _family_breakdown(multi: Dict[str, List[str]], key_path: str) -> Dict:
    """Agreement per residual family — read only after labelling is done."""
    family: Dict[str, str] = {}
    with open(key_path, "r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            family[row["item_id"]] = row["family"]
    out: Dict[str, Dict] = {}
    for fam in sorted(set(family.values())):
        ids = [i for i in multi if family.get(i) == fam]
        if not ids:
            continue
        disputed = sum(1 for i in ids if len(set(multi[i])) > 1)
        out[fam] = {
            "items": len(ids),
            "alpha": krippendorff_alpha({i: multi[i] for i in ids}),
            "adjudication_rate": disputed / len(ids),
            "labels": dict(Counter(v for i in ids for v in multi[i])),
        }
    print()
    print("%-8s%8s%9s%20s" % ("family", "items", "alpha", "adjudication rate"))
    for fam, s in out.items():
        print("%-8s%8d%9s%19.1f%%" % (
            fam, s["items"],
            "n/a" if s["alpha"] is None else round(s["alpha"], 3),
            100.0 * s["adjudication_rate"]))
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Adjudication sheets and agreement")
    sub = ap.add_subparsers(dest="cmd", required=True)
    ms = sub.add_parser("make-sheets")
    ms.add_argument("--sample", default=None)
    ms.add_argument("--raters", type=int, default=2)
    ms.add_argument("--outdir", default=None)
    ag = sub.add_parser("agreement")
    ag.add_argument("--sheets", nargs="+", required=True)
    ag.add_argument("--key", default=os.path.join(_ADJ_DIR, "answer_key.csv"))
    a = ap.parse_args()
    if a.cmd == "make-sheets":
        make_sheets(sample_path=a.sample, raters=a.raters, outdir=a.outdir)
    else:
        agreement_report(a.sheets, key_path=a.key)

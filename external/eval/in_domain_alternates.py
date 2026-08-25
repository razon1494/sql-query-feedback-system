"""
In-domain alternate-correct corpus.

Counterpart of the Spider alternate-correct experiment (gen_alternates.py), but
over the 30 authored curriculum problems rather than Spider.

Protocol, identical in spirit to the Spider one:

  * For each problem an alternate formulation is authored by hand: a query that
    answers the same question by a different route (counting instead of double
    negation, comma joins instead of explicit JOIN, IN instead of EXISTS, and so
    on).
  * A candidate is ADMITTED only if it produces exactly the reference output on
    the instructor's database. Candidates that do not are discarded, exactly as
    join-to-IN rewrites are discarded in the Spider corpus.
  * Admitted candidates are then run through the full pipeline. We report the
    RAW shape-classifier flag rate and the USER-FACING rate after evidence
    gating.

The numbers are whatever they are; nothing here is selected to hit a target.

Usage:
    python external/eval/in_domain_alternates.py
"""
import io
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, ROOT)

from backend.problems import PROBLEMS                                 # noqa: E402
from external.harness.generic_problem import GenericProblem, analyze  # noqa: E402

DB = os.path.join(ROOT, "database", "main.db")
EDGE_DIR = os.path.join(ROOT, "database")
EDGE_FILES = [
    "edge_partial_match.db",
    "edge_all_enrolled.db",
    "edge_empty_courses.db",
    "edge_single_course.db",
    "edge_no_students.db",
]

# ---------------------------------------------------------------------------
# Authored alternates. Keyed by problem id; a problem may carry more than one.
# Each is a different *route* to the same answer, not a cosmetic rewrite.
# ---------------------------------------------------------------------------
ALTERNATES = {

    # ---- DIVISION: counting instead of double negation -------------------
    "div_db_courses": [
        """SELECT s.StuID, s.Name FROM Students s
           JOIN Takes t ON s.StuID = t.StuID
           JOIN Courses c ON t.CourseID = c.CourseID
           WHERE c."Group" = 'DB'
           GROUP BY s.StuID, s.Name
           HAVING COUNT(DISTINCT c.CourseID) =
                  (SELECT COUNT(*) FROM Courses WHERE "Group" = 'DB');""",
    ],
    "div_all_cs_courses": [
        """SELECT s.StuID, s.Name FROM Students s
           JOIN Takes t ON s.StuID = t.StuID
           JOIN Courses c ON t.CourseID = c.CourseID
           WHERE s.Major = 'CS' AND c."Group" = 'CS'
           GROUP BY s.StuID, s.Name
           HAVING COUNT(DISTINCT c.CourseID) =
                  (SELECT COUNT(*) FROM Courses WHERE "Group" = 'CS');""",
    ],
    "div_instructor_all_db": [
        """SELECT i.InstID, i.Name FROM Instructors i
           JOIN Teaches tc ON i.InstID = tc.InstID
           JOIN Courses c ON tc.CourseID = c.CourseID
           WHERE c."Group" = 'DB'
           GROUP BY i.InstID, i.Name
           HAVING COUNT(DISTINCT c.CourseID) =
                  (SELECT COUNT(*) FROM Courses WHERE "Group" = 'DB');""",
    ],
    "div_students_all_math": [
        """SELECT s.StuID, s.Name FROM Students s
           JOIN Takes t ON s.StuID = t.StuID
           JOIN Courses c ON t.CourseID = c.CourseID
           WHERE c."Group" = 'Math'
           GROUP BY s.StuID, s.Name
           HAVING COUNT(DISTINCT c.CourseID) =
                  (SELECT COUNT(*) FROM Courses WHERE "Group" = 'Math');""",
    ],
    "div_instructor_all_cs": [
        """SELECT i.InstID, i.Name FROM Instructors i
           JOIN Teaches tc ON i.InstID = tc.InstID
           JOIN Courses c ON tc.CourseID = c.CourseID
           WHERE c."Group" = 'CS'
           GROUP BY i.InstID, i.Name
           HAVING COUNT(DISTINCT c.CourseID) =
                  (SELECT COUNT(*) FROM Courses WHERE "Group" = 'CS');""",
    ],
    "div_dept_all_courses": [
        """SELECT s.StuID, s.Name FROM Students s
           JOIN Takes t ON s.StuID = t.StuID
           JOIN Courses c ON t.CourseID = c.CourseID
           WHERE c.DeptID = 'D1'
           GROUP BY s.StuID, s.Name
           HAVING COUNT(DISTINCT c.CourseID) =
                  (SELECT COUNT(*) FROM Courses WHERE DeptID = 'D1');""",
    ],
    "div_instructor_all_prereqs": [
        """SELECT i.InstID, i.Name FROM Instructors i
           JOIN Teaches tc ON i.InstID = tc.InstID
           WHERE tc.CourseID IN (SELECT p.PrereqID FROM Prerequisites p
                                 WHERE p.CourseID = 'CS460')
           GROUP BY i.InstID, i.Name
           HAVING COUNT(DISTINCT tc.CourseID) =
                  (SELECT COUNT(DISTINCT p.PrereqID) FROM Prerequisites p
                   WHERE p.CourseID = 'CS460');""",
    ],

    # ---- JOIN: comma joins and IN-subqueries -----------------------------
    "join_cs360": [
        """SELECT s.StuID, s.Name FROM Students s
           WHERE s.StuID IN (SELECT t.StuID FROM Takes t WHERE t.CourseID = 'CS360');""",
        """SELECT s.StuID, s.Name FROM Students s, Takes t
           WHERE s.StuID = t.StuID AND t.CourseID = 'CS360';""",
    ],
    "join_instructor_courses": [
        """SELECT i.Name AS InstructorName, c.Title AS CourseTitle
           FROM Instructors i, Teaches tc, Courses c
           WHERE i.InstID = tc.InstID AND tc.CourseID = c.CourseID
           ORDER BY i.Name;""",
    ],
    "join_student_grades": [
        """SELECT s.Name AS StudentName, c.Title AS CourseTitle, t.Grade
           FROM Students s, Takes t, Courses c
           WHERE s.StuID = t.StuID AND t.CourseID = c.CourseID
           ORDER BY s.Name, c.Title;""",
    ],
    "join_four_credit_courses": [
        """SELECT DISTINCT s.StuID, s.Name, c.Title
           FROM Students s, Takes t, Courses c
           WHERE s.StuID = t.StuID AND t.CourseID = c.CourseID AND c.Credits = 4
           ORDER BY s.Name, c.Title;""",
    ],
    "join_students_and_instructors": [
        """SELECT DISTINCT s.Name AS StudentName, c.Title AS CourseTitle,
                  i.Name AS InstructorName
           FROM Students s, Takes t, Courses c, Teaches tc, Instructors i
           WHERE s.StuID = t.StuID AND t.CourseID = c.CourseID
             AND c.CourseID = tc.CourseID AND tc.InstID = i.InstID;""",
    ],

    # ---- AGGREGATION: comma joins ----------------------------------------
    "agg_multi_course": [
        """SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount
           FROM Students s, Takes t WHERE s.StuID = t.StuID
           GROUP BY s.StuID, s.Name HAVING COUNT(t.CourseID) > 1;""",
    ],
    "agg_avg_credits": [
        """SELECT s.StuID, s.Name, AVG(c.Credits) AS AvgCredits
           FROM Students s, Takes t, Courses c
           WHERE s.StuID = t.StuID AND t.CourseID = c.CourseID
           GROUP BY s.StuID, s.Name;""",
    ],
    "agg_enrollment_per_course": [
        """SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
           FROM Courses c, Takes t WHERE c.CourseID = t.CourseID
           GROUP BY c.CourseID, c.Title ORDER BY EnrollmentCount DESC;""",
    ],
    "agg_popular_courses": [
        """SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
           FROM Courses c, Takes t WHERE c.CourseID = t.CourseID
           GROUP BY c.CourseID, c.Title HAVING COUNT(t.StuID) > 2
           ORDER BY EnrollmentCount DESC;""",
    ],

    # ---- SET_OP: membership tests instead of set operators ---------------
    "set_no_db": [
        """SELECT StuID, Name FROM Students
           WHERE StuID NOT IN (
             SELECT t.StuID FROM Takes t JOIN Courses c ON t.CourseID = c.CourseID
             WHERE c."Group" = 'DB');""",
    ],
    "set_union_cs_db": [
        """SELECT DISTINCT s.StuID, s.Name FROM Students s
           JOIN Takes t ON s.StuID = t.StuID
           JOIN Courses c ON t.CourseID = c.CourseID
           WHERE c."Group" IN ('CS', 'DB');""",
    ],
    "set_intersect_cs_and_db": [
        """SELECT DISTINCT s.StuID, s.Name FROM Students s
           WHERE s.StuID IN (SELECT t.StuID FROM Takes t
                             JOIN Courses c ON t.CourseID = c.CourseID
                             WHERE c."Group" = 'CS')
             AND s.StuID IN (SELECT t.StuID FROM Takes t
                             JOIN Courses c ON t.CourseID = c.CourseID
                             WHERE c."Group" = 'DB');""",
    ],
    "set_except_instructors_no_db": [
        """SELECT InstID, Name FROM Instructors
           WHERE InstID NOT IN (
             SELECT tc.InstID FROM Teaches tc
             JOIN Courses c ON tc.CourseID = c.CourseID
             WHERE c."Group" = 'DB');""",
    ],

    # ---- SUBQUERY ---------------------------------------------------------
    "sub_above_avg_credits": [
        """SELECT c.CourseID, c.Title, c.Credits
           FROM Courses c, (SELECT AVG(Credits) AS a FROM Courses) m
           WHERE c.Credits > m.a;""",
    ],
    "sub_in_db_courses": [
        """SELECT DISTINCT s.StuID, s.Name FROM Students s
           JOIN Takes t ON s.StuID = t.StuID
           JOIN Courses c ON t.CourseID = c.CourseID
           WHERE c."Group" = 'DB';""",
    ],
    "sub_exists_four_credit": [
        """SELECT s.StuID, s.Name FROM Students s
           WHERE s.StuID IN (
             SELECT t.StuID FROM Takes t JOIN Courses c ON t.CourseID = c.CourseID
             WHERE c.Credits = 4);""",
    ],

    # ---- NULL -------------------------------------------------------------
    "null_no_grade": [
        """SELECT s.StuID, s.Name, t.CourseID FROM Students s, Takes t
           WHERE s.StuID = t.StuID AND t.Grade IS NULL;""",
    ],
}


def main():
    edges = [os.path.join(EDGE_DIR, f) for f in EDGE_FILES
             if os.path.exists(os.path.join(EDGE_DIR, f))]
    by_id = {p["id"]: p for p in PROBLEMS}

    records = []
    for pid, alts in ALTERNATES.items():
        p = by_id.get(pid)
        if p is None:
            print("  ! unknown problem id:", pid)
            continue
        use_edges = edges if p["type"] == "DIVISION" else []
        prob = GenericProblem(
            problem_id=pid, db_path=DB, gold_sql=p["base_query"],
            problem_type=p["type"], db_id="university",
            source="in_domain", edge_db_paths=use_edges,
        )
        for alt in alts:
            alt = " ".join(alt.split())
            try:
                r = analyze(prob, alt)
            except Exception as e:
                records.append({"problem_id": pid, "type": p["type"], "alt_sql": alt,
                                "equivalent": False, "error": str(e)[:120],
                                "raw": [], "detected": []})
                continue
            records.append({
                "problem_id": pid, "type": p["type"], "alt_sql": alt,
                "equivalent": bool(r.are_equivalent),
                "edges_ok": bool(r.edges_ok),
                "raw": r.raw, "detected": r.detected,
                "unsupported": getattr(r, "unsupported", []),
                "student_exec_error": r.student_exec_error,
            })

    # Alternate-correct means correct, not merely coincident on one instance.
    # A candidate that matches the reference on the instructor's database but
    # diverges on a constructed edge instance is a latent bug, not an
    # alternative solution, and the gate is right to speak about it.
    admitted = [x for x in records if x["equivalent"] and x.get("edges_ok", True)]
    rejected = [x for x in records if not (x["equivalent"] and x.get("edges_ok", True))]
    raw_flagged = [x for x in admitted if x["raw"]]
    user_flagged = [x for x in admitted if x["detected"]]

    print("=" * 66)
    print(" IN-DOMAIN ALTERNATE-CORRECT CORPUS")
    print("=" * 66)
    print("candidates authored     : %d" % len(records))
    print("admitted (output-equiv) : %d" % len(admitted))
    print("rejected (not equiv)    : %d" % len(rejected))
    if admitted:
        print("-" * 66)
        print("RAW shape-flag rate     : %d / %d = %.1f%%"
              % (len(raw_flagged), len(admitted),
                 100.0 * len(raw_flagged) / len(admitted)))
        print("USER-FACING FP rate     : %d / %d = %.1f%%"
              % (len(user_flagged), len(admitted),
                 100.0 * len(user_flagged) / len(admitted)))
    if rejected:
        print("-" * 66)
        print("rejected candidates:")
        for x in rejected:
            print("   %-30s %s" % (x["problem_id"],
                                   x.get("error", "output differs")[:60]))
    if user_flagged:
        print("-" * 66)
        print("USER-FACING FALSE POSITIVES (should be none):")
        for x in user_flagged:
            print("   %-30s %s" % (x["problem_id"], x["detected"]))

    out = os.path.join(ROOT, "external", "data", "derived",
                       "in_domain_alternates.json")
    summary = {
        "candidates": len(records),
        "admitted": len(admitted),
        "rejected": len(rejected),
        "raw_flagged": len(raw_flagged),
        "raw_flag_rate_pct": (100.0 * len(raw_flagged) / len(admitted)) if admitted else None,
        "user_facing_fp": len(user_flagged),
        "user_facing_fp_pct": (100.0 * len(user_flagged) / len(admitted)) if admitted else None,
        "records": records,
    }
    with io.open(out, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(summary, indent=1))
    print("-" * 66)
    print("wrote", out)


if __name__ == "__main__":
    main()

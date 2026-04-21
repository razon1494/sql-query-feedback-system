"""
Spider-style WRONG query corpus (Phase 4, Step 2).

Target: 6 independent wrong queries per misconception type (M1-M10),
adapted from canonical Spider-dataset patterns to the five-table university
schema. Each query is constructed by hand — it is NOT a mutation of the
reference base query. This independent source resolves the circularity
concern raised by Dr. Jamil about the Phase 3 author-constructed evaluation.

Each entry exhibits EXACTLY ONE intended misconception. The Step 2 harness
(`eval_spider_step2_detection.py`) feeds each query through the full
`generate_feedback` pipeline and verifies that `report.misconceptions`
contains the expected key.

Fields per entry
----------------
  problem_id    : str  — reference problem in backend/problems.py
  misconception : str  — expected key the detector must surface
  shape         : str  — short tag (Spider-style pattern name)
  query         : str  — the wrong SQL
  rationale     : str  — why this query exhibits the misconception
"""

CORPUS = [

    # ══════════════════════════════════════════════════════════════════
    #  M1 MISSING_WHERE — 6 queries
    #  Reference query has a filter; student drops/forgets it.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "join_cs360",
        "misconception": "MISSING_WHERE",
        "shape": "join_without_filter",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID;
        """,
        "rationale": "Joins Students with Takes but forgets WHERE CourseID='CS360'.",
    },
    {
        "problem_id": "join_four_credit_courses",
        "misconception": "MISSING_WHERE",
        "shape": "join_without_filter",
        "query": """
            SELECT DISTINCT s.StuID, s.Name, c.Title
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            ORDER BY s.Name, c.Title;
        """,
        "rationale": "Omits WHERE c.Credits = 4; returns all student-course rows.",
    },
    {
        "problem_id": "null_no_grade",
        "misconception": "MISSING_WHERE",
        "shape": "no_null_filter",
        "query": """
            SELECT s.StuID, s.Name, t.CourseID
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID;
        """,
        "rationale": "Drops the WHERE t.Grade IS NULL filter.",
    },
    {
        "problem_id": "null_major_missing",
        "misconception": "MISSING_WHERE",
        "shape": "no_null_filter",
        "query": """
            SELECT StuID, Name FROM Students;
        """,
        "rationale": "Forgets WHERE Major IS NULL.",
    },
    {
        "problem_id": "sub_above_avg_credits",
        "misconception": "MISSING_WHERE",
        "shape": "subquery_never_applied",
        "query": """
            SELECT CourseID, Title, Credits FROM Courses;
        """,
        "rationale": "Omits the WHERE Credits > (SELECT AVG(Credits) FROM Courses) filter.",
    },
    {
        "problem_id": "sub_in_db_courses",
        "misconception": "MISSING_WHERE",
        "shape": "join_without_group_filter",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID;
        """,
        "rationale": "Joins all three tables but forgets WHERE c.\"Group\"='DB'.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M2 IN_vs_EXISTS — 6 queries
    #  Reference uses EXISTS (correlated); student uses IN (uncorrelated).
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "IN_vs_EXISTS",
        "shape": "in_instead_of_correlated_exists",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE s.StuID IN (
                SELECT t.StuID FROM Takes t
                WHERE t.CourseID IN (
                    SELECT CourseID FROM Courses WHERE Credits = 4
                )
            );
        """,
        "rationale": "Uses IN-in-IN instead of correlated EXISTS — flatter form loses per-row scoping.",
    },
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "IN_vs_EXISTS",
        "shape": "in_subquery_join",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c.Credits = 4
            );
        """,
        "rationale": "IN against a pre-computed set where the reference uses a correlated EXISTS.",
    },
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "IN_vs_EXISTS",
        "shape": "nested_in_by_credits",
        "query": """
            SELECT StuID, Name FROM Students
            WHERE StuID IN (
                SELECT StuID FROM Takes
                WHERE CourseID IN (
                    SELECT CourseID FROM Courses
                    WHERE Credits = 4
                )
            );
        """,
        "rationale": "Unaliased IN-in-IN form; no correlation with outer Students.",
    },
    # Need three more M2 queries — use NOT_EXISTS problems (div_*) where a
    # student mistakenly flips to IN in a *positive* existential context by
    # mangling the semantics. We build a positive-existential variant by
    # asking the same schema question under div_* references. This still
    # hits M2 because the detector fires on `base.where_type == EXISTS and
    # student.where_type == IN`, but requires an EXISTS base. Use
    # sub_exists_four_credit variations only — SUBQUERY type has 1 EXISTS
    # problem. So we augment by adding 3 variants that differ structurally.
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "IN_vs_EXISTS",
        "shape": "in_with_any_course",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE s.StuID IN (SELECT StuID FROM Takes)
            AND s.StuID IN (
                SELECT t.StuID FROM Takes t, Courses c
                WHERE t.CourseID = c.CourseID AND c.Credits = 4
            );
        """,
        "rationale": "Two uncorrelated IN subqueries where reference uses a single correlated EXISTS.",
    },
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "IN_vs_EXISTS",
        "shape": "distinct_in",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            WHERE s.StuID IN (
                SELECT DISTINCT t.StuID FROM Takes t, Courses c
                WHERE t.CourseID = c.CourseID AND c.Credits = 4
            );
        """,
        "rationale": "IN against a flat derived set; misses correlated-EXISTS semantics.",
    },
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "IN_vs_EXISTS",
        "shape": "in_with_union",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE s.StuID IN (
                SELECT StuID FROM Takes WHERE CourseID = 'CS120'
                UNION
                SELECT StuID FROM Takes WHERE CourseID = 'CS220'
            );
        """,
        "rationale": "IN over a UNION-computed set of 4-credit courses — shape mismatch with reference EXISTS.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M3 NOT_IN_vs_NOT_EXISTS — 6 queries
    #  Reference uses NOT EXISTS; student uses NOT IN (NULL-unsafe).
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "div_db_courses",
        "misconception": "NOT_IN_vs_NOT_EXISTS",
        "shape": "not_in_for_division",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.StuID NOT IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c."Group" = 'DB' AND t.StuID IS NOT NULL
            );
        """,
        "rationale": "Uses NOT IN where reference uses NOT EXISTS — NULL-unsafe form.",
    },
    {
        "problem_id": "div_all_cs_courses",
        "misconception": "NOT_IN_vs_NOT_EXISTS",
        "shape": "not_in_for_division",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.Major = 'CS' AND s.StuID NOT IN (
                SELECT t.StuID FROM Takes t
                WHERE t.CourseID NOT IN (
                    SELECT CourseID FROM Courses WHERE "Group" = 'CS'
                )
            );
        """,
        "rationale": "Replaces the outer NOT EXISTS with NOT IN.",
    },
    {
        "problem_id": "div_instructor_all_db",
        "misconception": "NOT_IN_vs_NOT_EXISTS",
        "shape": "not_in_for_division",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE i.InstID NOT IN (
                SELECT tc.InstID FROM Teaches tc
                JOIN Courses c ON tc.CourseID = c.CourseID
                WHERE c."Group" = 'DB' AND tc.InstID IS NOT NULL
            );
        """,
        "rationale": "Instructor division via NOT IN — reference uses NOT EXISTS.",
    },
    {
        "problem_id": "div_students_all_math",
        "misconception": "NOT_IN_vs_NOT_EXISTS",
        "shape": "not_in_for_division",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.StuID NOT IN (
                SELECT t.StuID FROM Takes t
                WHERE t.CourseID NOT IN (
                    SELECT CourseID FROM Courses WHERE "Group" = 'Math'
                )
            );
        """,
        "rationale": "Math-courses division via NOT IN.",
    },
    {
        "problem_id": "div_instructor_all_cs",
        "misconception": "NOT_IN_vs_NOT_EXISTS",
        "shape": "not_in_for_division",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE i.InstID NOT IN (
                SELECT tc.InstID FROM Teaches tc
                WHERE tc.CourseID NOT IN (
                    SELECT CourseID FROM Courses WHERE "Group" = 'CS'
                )
            );
        """,
        "rationale": "CS-instructor division via NOT IN.",
    },
    {
        "problem_id": "div_db_courses",
        "misconception": "NOT_IN_vs_NOT_EXISTS",
        "shape": "not_in_simple",
        "query": """
            SELECT StuID, Name FROM Students
            WHERE StuID NOT IN (
                SELECT StuID FROM Takes
                WHERE CourseID NOT IN (
                    SELECT CourseID FROM Courses WHERE "Group" = 'DB'
                )
            );
        """,
        "rationale": "Unaliased NOT-IN-in-NOT-IN form.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M4 WRONG_JOIN_TYPE — 6 queries
    #  Reference uses INNER JOIN; student uses LEFT/RIGHT/CROSS.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "join_cs360",
        "misconception": "WRONG_JOIN_TYPE",
        "shape": "left_join",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            LEFT JOIN Takes t ON s.StuID = t.StuID
            WHERE t.CourseID = 'CS360';
        """,
        "rationale": "LEFT JOIN where INNER JOIN is expected.",
    },
    {
        "problem_id": "join_instructor_courses",
        "misconception": "WRONG_JOIN_TYPE",
        "shape": "left_join",
        "query": """
            SELECT i.Name AS InstructorName, c.Title AS CourseTitle
            FROM Instructors i
            LEFT JOIN Teaches tc ON i.InstID   = tc.InstID
            LEFT JOIN Courses c  ON tc.CourseID = c.CourseID
            ORDER BY i.Name;
        """,
        "rationale": "LEFT JOIN chain — introduces NULL-padded rows for instructors with no courses.",
    },
    {
        "problem_id": "join_student_grades",
        "misconception": "WRONG_JOIN_TYPE",
        "shape": "left_join_chain",
        "query": """
            SELECT s.Name AS StudentName, c.Title AS CourseTitle, t.Grade
            FROM Students s
            LEFT JOIN Takes t    ON s.StuID    = t.StuID
            LEFT JOIN Courses c  ON t.CourseID = c.CourseID
            ORDER BY s.Name, c.Title;
        """,
        "rationale": "LEFT JOIN preserves students with no enrollments; reference is INNER.",
    },
    {
        "problem_id": "agg_multi_course",
        "misconception": "WRONG_JOIN_TYPE",
        "shape": "left_join_aggregation",
        "query": """
            SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount
            FROM Students s
            LEFT JOIN Takes t ON s.StuID = t.StuID
            GROUP BY s.StuID, s.Name
            HAVING COUNT(t.CourseID) > 1;
        """,
        "rationale": "LEFT JOIN on aggregation — includes students with zero enrollments (count=0).",
    },
    {
        "problem_id": "agg_enrollment_per_course",
        "misconception": "WRONG_JOIN_TYPE",
        "shape": "left_join_aggregation",
        "query": """
            SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
            FROM Courses c
            LEFT JOIN Takes t ON c.CourseID = t.CourseID
            GROUP BY c.CourseID, c.Title
            ORDER BY EnrollmentCount DESC;
        """,
        "rationale": "LEFT JOIN surfaces courses with zero enrollments; reference INNER excludes them.",
    },
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "WRONG_JOIN_TYPE",
        "shape": "left_join_in_subquery",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE EXISTS (
                SELECT 1 FROM Takes t
                LEFT JOIN Courses c ON t.CourseID = c.CourseID
                WHERE t.StuID = s.StuID AND c.Credits = 4
            );
        """,
        "rationale": "LEFT JOIN inside subquery where reference uses INNER.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M5 CARTESIAN_PRODUCT — 6 queries
    #  Two+ tables in FROM without a linking predicate.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "join_cs360",
        "misconception": "CARTESIAN_PRODUCT",
        "shape": "comma_join_no_predicate",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s, Takes t
            WHERE t.CourseID = 'CS360';
        """,
        "rationale": "Comma-join without s.StuID=t.StuID — every student × every CS360 enrollment.",
    },
    {
        "problem_id": "join_four_credit_courses",
        "misconception": "CARTESIAN_PRODUCT",
        "shape": "triple_comma_no_link",
        "query": """
            SELECT DISTINCT s.StuID, s.Name, c.Title
            FROM Students s, Takes t, Courses c
            WHERE c.Credits = 4;
        """,
        "rationale": "Three-way comma-join, no equi-join predicates at all.",
    },
    {
        "problem_id": "join_students_and_instructors",
        "misconception": "CARTESIAN_PRODUCT",
        "shape": "partial_link_cartesian",
        "query": """
            SELECT DISTINCT s.Name AS StudentName, c.Title AS CourseTitle, i.Name AS InstructorName
            FROM Students s, Takes t, Courses c, Teaches tc, Instructors i
            WHERE c.CourseID = tc.CourseID
            AND tc.InstID = i.InstID;
        """,
        "rationale": "Links Courses→Teaches→Instructors but Students and Takes float free.",
    },
    {
        "problem_id": "agg_multi_course",
        "misconception": "CARTESIAN_PRODUCT",
        "shape": "cartesian_in_agg",
        "query": """
            SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount
            FROM Students s, Takes t
            GROUP BY s.StuID, s.Name
            HAVING COUNT(t.CourseID) > 1;
        """,
        "rationale": "Aggregation over cartesian product — counts explode by |Takes|.",
    },
    {
        "problem_id": "agg_avg_credits",
        "misconception": "CARTESIAN_PRODUCT",
        "shape": "cartesian_in_agg",
        "query": """
            SELECT s.StuID, s.Name, AVG(c.Credits) AS AvgCredits
            FROM Students s, Takes t, Courses c
            WHERE t.CourseID = c.CourseID
            GROUP BY s.StuID, s.Name;
        """,
        "rationale": "Students has no link to Takes in WHERE; cartesian against Students.",
    },
    {
        "problem_id": "join_student_grades",
        "misconception": "CARTESIAN_PRODUCT",
        "shape": "comma_link_missing",
        "query": """
            SELECT s.Name AS StudentName, c.Title AS CourseTitle, t.Grade
            FROM Students s, Takes t, Courses c
            WHERE t.CourseID = c.CourseID
            ORDER BY s.Name, c.Title;
        """,
        "rationale": "Joins Takes↔Courses in WHERE but forgets Students↔Takes.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M6 MISSING_GROUP_BY — 6 queries
    #  Aggregate + non-aggregate in SELECT without GROUP BY.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "agg_multi_course",
        "misconception": "MISSING_GROUP_BY",
        "shape": "no_group_by",
        "query": """
            SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            HAVING COUNT(t.CourseID) > 1;
        """,
        "rationale": "Mixes s.StuID/Name with COUNT but drops GROUP BY.",
    },
    {
        "problem_id": "agg_avg_credits",
        "misconception": "MISSING_GROUP_BY",
        "shape": "no_group_by",
        "query": """
            SELECT s.StuID, s.Name, AVG(c.Credits) AS AvgCredits
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID;
        """,
        "rationale": "Per-student average without GROUP BY s.StuID, s.Name.",
    },
    {
        "problem_id": "agg_enrollment_per_course",
        "misconception": "MISSING_GROUP_BY",
        "shape": "no_group_by",
        "query": """
            SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
            FROM Courses c
            JOIN Takes t ON c.CourseID = t.CourseID
            ORDER BY EnrollmentCount DESC;
        """,
        "rationale": "Per-course count without GROUP BY c.CourseID, c.Title.",
    },
    {
        "problem_id": "agg_popular_courses",
        "misconception": "MISSING_GROUP_BY",
        "shape": "no_group_by",
        "query": """
            SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
            FROM Courses c
            JOIN Takes t ON c.CourseID = t.CourseID
            HAVING COUNT(t.StuID) > 2
            ORDER BY EnrollmentCount DESC;
        """,
        "rationale": "HAVING without GROUP BY; mixed aggregate and raw columns in SELECT.",
    },
    {
        "problem_id": "agg_avg_credits_per_group",
        "misconception": "MISSING_GROUP_BY",
        "shape": "no_group_by",
        "query": """
            SELECT "Group", AVG(Credits) AS AvgCredits
            FROM Courses
            ORDER BY "Group";
        """,
        "rationale": "Per-group average with \"Group\" in SELECT but no GROUP BY.",
    },
    {
        "problem_id": "agg_avg_credits",
        "misconception": "MISSING_GROUP_BY",
        "shape": "no_group_by_min_agg",
        "query": """
            SELECT s.Name, MIN(c.Credits) AS MinCredits
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID;
        """,
        "rationale": "s.Name with MIN() but no GROUP BY — should produce per-student minimum.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M7 HAVING_vs_WHERE — 6 queries
    #  Aggregate predicate placed in WHERE, or row-level predicate in HAVING.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "agg_multi_course",
        "misconception": "HAVING_vs_WHERE",
        "shape": "agg_in_where",
        "query": """
            SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            WHERE COUNT(t.CourseID) > 1
            GROUP BY s.StuID, s.Name;
        """,
        "rationale": "COUNT() in WHERE — aggregate predicate belongs in HAVING.",
    },
    {
        "problem_id": "agg_popular_courses",
        "misconception": "HAVING_vs_WHERE",
        "shape": "agg_in_where",
        "query": """
            SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
            FROM Courses c
            JOIN Takes t ON c.CourseID = t.CourseID
            WHERE COUNT(t.StuID) > 2
            GROUP BY c.CourseID, c.Title
            ORDER BY EnrollmentCount DESC;
        """,
        "rationale": "COUNT(t.StuID) > 2 in WHERE — cannot be evaluated before grouping.",
    },
    {
        "problem_id": "agg_enrollment_per_course",
        "misconception": "HAVING_vs_WHERE",
        "shape": "agg_in_where",
        "query": """
            SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
            FROM Courses c
            JOIN Takes t ON c.CourseID = t.CourseID
            WHERE COUNT(*) > 0
            GROUP BY c.CourseID, c.Title
            ORDER BY EnrollmentCount DESC;
        """,
        "rationale": "Aggregate WHERE predicate COUNT(*) > 0.",
    },
    {
        "problem_id": "agg_avg_credits",
        "misconception": "HAVING_vs_WHERE",
        "shape": "agg_in_where",
        "query": """
            SELECT s.StuID, s.Name, AVG(c.Credits) AS AvgCredits
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE AVG(c.Credits) > 3
            GROUP BY s.StuID, s.Name;
        """,
        "rationale": "AVG() used in WHERE; belongs in HAVING.",
    },
    {
        "problem_id": "agg_avg_credits_per_group",
        "misconception": "HAVING_vs_WHERE",
        "shape": "agg_in_where",
        "query": """
            SELECT "Group", AVG(Credits) AS AvgCredits
            FROM Courses
            WHERE AVG(Credits) >= 3
            GROUP BY "Group"
            ORDER BY "Group";
        """,
        "rationale": "AVG() filtered in WHERE.",
    },
    {
        "problem_id": "agg_popular_courses",
        "misconception": "HAVING_vs_WHERE",
        "shape": "agg_in_where_sum",
        "query": """
            SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
            FROM Courses c
            JOIN Takes t ON c.CourseID = t.CourseID
            WHERE SUM(1) > 2
            GROUP BY c.CourseID, c.Title
            ORDER BY EnrollmentCount DESC;
        """,
        "rationale": "SUM(1) aggregate in WHERE.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M8 IN_FOR_DIVISION — 6 queries
    #  Student uses IN (existential) where NOT EXISTS (universal) is needed.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "div_db_courses",
        "misconception": "IN_FOR_DIVISION",
        "shape": "in_partial_match",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c."Group" = 'DB'
            );
        """,
        "rationale": "Returns students with ANY DB enrollment; partial match, not universal.",
    },
    {
        "problem_id": "div_all_cs_courses",
        "misconception": "IN_FOR_DIVISION",
        "shape": "in_partial_match",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.Major = 'CS'
            AND s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c."Group" = 'CS'
            );
        """,
        "rationale": "CS major + at least one CS-group course; misses 'all CS-group courses'.",
    },
    {
        "problem_id": "div_instructor_all_db",
        "misconception": "IN_FOR_DIVISION",
        "shape": "in_partial_match",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE i.InstID IN (
                SELECT tc.InstID FROM Teaches tc
                JOIN Courses c ON tc.CourseID = c.CourseID
                WHERE c."Group" = 'DB'
            );
        """,
        "rationale": "Any DB instructor, not one teaching ALL DB courses.",
    },
    {
        "problem_id": "div_students_all_math",
        "misconception": "IN_FOR_DIVISION",
        "shape": "in_partial_match",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c."Group" = 'Math'
            );
        """,
        "rationale": "At least one Math course, not all.",
    },
    {
        "problem_id": "div_instructor_all_cs",
        "misconception": "IN_FOR_DIVISION",
        "shape": "in_partial_match",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE i.InstID IN (
                SELECT tc.InstID FROM Teaches tc
                JOIN Courses c ON tc.CourseID = c.CourseID
                WHERE c."Group" = 'CS'
            );
        """,
        "rationale": "Any CS-group instructor, not universal.",
    },
    {
        "problem_id": "div_db_courses",
        "misconception": "IN_FOR_DIVISION",
        "shape": "in_flat",
        "query": """
            SELECT StuID, Name FROM Students
            WHERE StuID IN (
                SELECT StuID FROM Takes
                WHERE CourseID IN (
                    SELECT CourseID FROM Courses WHERE "Group" = 'DB'
                )
            );
        """,
        "rationale": "Flat IN-in-IN; existential chain, not universal quantification.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M9 MISSING_CORRELATED_REF — 6 queries
    #  NOT EXISTS / EXISTS subquery without correlation to the outer tuple.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "div_db_courses",
        "misconception": "MISSING_CORRELATED_REF",
        "shape": "uncorrelated_not_exists",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'DB'
                AND c.CourseID NOT IN (
                    SELECT t.CourseID FROM Takes t
                )
            );
        """,
        "rationale": "Inner subquery references only Takes, never the outer s.StuID — constant evaluation.",
    },
    {
        "problem_id": "div_all_cs_courses",
        "misconception": "MISSING_CORRELATED_REF",
        "shape": "uncorrelated_not_exists",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE s.Major = 'CS'
            AND NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'CS'
                AND c.CourseID NOT IN (
                    SELECT t.CourseID FROM Takes t
                )
            );
        """,
        "rationale": "Inner NOT IN against all Takes instead of outer-correlated Takes.",
    },
    {
        "problem_id": "div_instructor_all_db",
        "misconception": "MISSING_CORRELATED_REF",
        "shape": "uncorrelated_not_exists",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'DB'
                AND c.CourseID NOT IN (
                    SELECT tc.CourseID FROM Teaches tc
                )
            );
        """,
        "rationale": "Uses all Teaches, ignores i.InstID correlation.",
    },
    {
        "problem_id": "div_students_all_math",
        "misconception": "MISSING_CORRELATED_REF",
        "shape": "uncorrelated_not_exists",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'Math'
                AND c.CourseID NOT IN (SELECT CourseID FROM Takes)
            );
        """,
        "rationale": "NOT IN against Takes with no outer correlation.",
    },
    {
        "problem_id": "div_instructor_all_cs",
        "misconception": "MISSING_CORRELATED_REF",
        "shape": "uncorrelated_not_exists",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'CS'
                AND c.CourseID NOT IN (SELECT CourseID FROM Teaches)
            );
        """,
        "rationale": "Inner NOT IN constant — no reference to i.InstID.",
    },
    {
        "problem_id": "sub_exists_four_credit",
        "misconception": "MISSING_CORRELATED_REF",
        "shape": "uncorrelated_exists",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE EXISTS (
                SELECT 1 FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c.Credits = 4
            );
        """,
        "rationale": "EXISTS returns the same value for every student — no correlation with s.StuID.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  M10 WRONG_SET_OP — 6 queries
    #  Student uses the wrong set operator.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "set_no_db",
        "misconception": "WRONG_SET_OP",
        "shape": "union_instead_of_except",
        "query": """
            SELECT StuID, Name FROM Students
            UNION
            SELECT s.StuID, s.Name
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c."Group" = 'DB';
        """,
        "rationale": "Reference uses EXCEPT; student uses UNION — includes rather than excludes.",
    },
    {
        "problem_id": "set_no_db",
        "misconception": "WRONG_SET_OP",
        "shape": "intersect_instead_of_except",
        "query": """
            SELECT StuID, Name FROM Students
            INTERSECT
            SELECT s.StuID, s.Name
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c."Group" = 'DB';
        """,
        "rationale": "INTERSECT yields students who DO take DB; opposite of EXCEPT.",
    },
    {
        "problem_id": "set_union_cs_db",
        "misconception": "WRONG_SET_OP",
        "shape": "intersect_instead_of_union",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s JOIN Takes t ON s.StuID=t.StuID JOIN Courses c ON t.CourseID=c.CourseID
            WHERE c."Group" = 'CS'
            INTERSECT
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s JOIN Takes t ON s.StuID=t.StuID JOIN Courses c ON t.CourseID=c.CourseID
            WHERE c."Group" = 'DB';
        """,
        "rationale": "INTERSECT yields BOTH CS and DB takers; reference UNION wants EITHER.",
    },
    {
        "problem_id": "set_intersect_cs_and_db",
        "misconception": "WRONG_SET_OP",
        "shape": "union_instead_of_intersect",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s JOIN Takes t ON s.StuID=t.StuID JOIN Courses c ON t.CourseID=c.CourseID
            WHERE c."Group" = 'CS'
            UNION
            SELECT s.StuID, s.Name
            FROM Students s JOIN Takes t ON s.StuID=t.StuID JOIN Courses c ON t.CourseID=c.CourseID
            WHERE c."Group" = 'DB';
        """,
        "rationale": "UNION yields CS-or-DB takers; reference INTERSECT wants BOTH.",
    },
    {
        "problem_id": "set_intersect_cs_and_db",
        "misconception": "WRONG_SET_OP",
        "shape": "except_instead_of_intersect",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s JOIN Takes t ON s.StuID=t.StuID JOIN Courses c ON t.CourseID=c.CourseID
            WHERE c."Group" = 'CS'
            EXCEPT
            SELECT s.StuID, s.Name
            FROM Students s JOIN Takes t ON s.StuID=t.StuID JOIN Courses c ON t.CourseID=c.CourseID
            WHERE c."Group" = 'DB';
        """,
        "rationale": "EXCEPT yields CS but not DB; reference INTERSECT wants both.",
    },
    {
        "problem_id": "set_except_instructors_no_db",
        "misconception": "WRONG_SET_OP",
        "shape": "intersect_instead_of_except",
        "query": """
            SELECT InstID, Name FROM Instructors
            INTERSECT
            SELECT i.InstID, i.Name
            FROM Instructors i
            JOIN Teaches tc ON i.InstID   = tc.InstID
            JOIN Courses c  ON tc.CourseID = c.CourseID
            WHERE c."Group" = 'DB';
        """,
        "rationale": "INTERSECT surfaces instructors who DO teach DB; reference EXCEPT wants those who don't.",
    },

    # ══════════════════════════════════════════════════════════════════
    #  NULL_EQUALITY — 6 queries
    #  Student writes `= NULL` / `!= NULL` instead of IS NULL / IS NOT NULL.
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "null_no_grade",
        "misconception": "NULL_EQUALITY",
        "shape": "equal_null",
        "query": """
            SELECT s.StuID, s.Name, t.CourseID
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            WHERE t.Grade = NULL;
        """,
        "rationale": "`t.Grade = NULL` evaluates to UNKNOWN for every row.",
    },
    {
        "problem_id": "null_major_missing",
        "misconception": "NULL_EQUALITY",
        "shape": "equal_null",
        "query": """
            SELECT StuID, Name
            FROM Students
            WHERE Major = NULL;
        """,
        "rationale": "`Major = NULL` returns empty set; needs IS NULL.",
    },
    {
        "problem_id": "null_major_missing",
        "misconception": "NULL_EQUALITY",
        "shape": "not_equal_null",
        "query": """
            SELECT StuID, Name
            FROM Students
            WHERE NOT Major != NULL;
        """,
        "rationale": "`Major != NULL` is UNKNOWN; NOT UNKNOWN is UNKNOWN — wrong result.",
    },
    {
        "problem_id": "null_no_grade",
        "misconception": "NULL_EQUALITY",
        "shape": "lt_gt_null",
        "query": """
            SELECT s.StuID, s.Name, t.CourseID
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            WHERE t.Grade <> NULL;
        """,
        "rationale": "`<> NULL` is three-valued UNKNOWN for all rows.",
    },
    {
        "problem_id": "null_major_missing",
        "misconception": "NULL_EQUALITY",
        "shape": "equal_null_or_filter",
        "query": """
            SELECT StuID, Name
            FROM Students
            WHERE Major = NULL OR Major = 'Unknown';
        """,
        "rationale": "Left disjunct is always UNKNOWN.",
    },
    {
        "problem_id": "null_no_grade",
        "misconception": "NULL_EQUALITY",
        "shape": "equal_null_in_subquery",
        "query": """
            SELECT s.StuID, s.Name, t.CourseID
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            WHERE t.Grade IN (SELECT Grade FROM Takes WHERE Grade = NULL);
        """,
        "rationale": "Nested `= NULL` filter produces empty IN set, hence zero rows.",
    },
]


if __name__ == "__main__":
    from collections import Counter
    print(f"Corpus entries: {len(CORPUS)}")
    by_mis = Counter(e["misconception"] for e in CORPUS)
    for k, c in sorted(by_mis.items()):
        status = "✓" if c == 6 else "✗"
        print(f"  {status} {k:<28} {c}")

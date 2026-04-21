"""
Spider-style corpus of alternate-correct SQL queries (Phase 4, Step 1).

Each entry is a CORRECT query that solves the same problem as the reference
base query, but uses a structurally different shape. Shapes are drawn from
canonical Spider-dataset patterns — GROUP BY + HAVING count, correlated
EXISTS / NOT EXISTS, IN / NOT IN subqueries, derived tables, scalar
subqueries, comma-joins with WHERE equi-join, etc. — adapted to the
five-table university schema.

The corpus is used by `eval_spider_step1_fp.py` to measure the
false-positive rate of the misconception detector: an alternate-correct
query SHOULD pass through with zero misconceptions raised.

Fields per entry
----------------
  problem_id : str   — matches one of the 25 IDs in backend/problems.py
  label      : str   — short tag for the alternate shape
  pattern    : str   — the Spider-style shape this emulates
  query      : str   — the SQL for the alternate correct query
"""

CORPUS = [
    # ══════════════════════════════════════════════════════════════════
    #  DIVISION — GROUP BY + HAVING count, and NOT EXISTS/NOT EXISTS
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "div_db_courses",
        "label":      "group_by_count",
        "pattern":    "GROUP BY + HAVING COUNT(DISTINCT ...) = (SELECT COUNT(*) ...)",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c."Group" = 'DB'
            GROUP BY s.StuID, s.Name
            HAVING COUNT(DISTINCT t.CourseID) = (SELECT COUNT(*) FROM Courses WHERE "Group" = 'DB');
        """,
    },
    {
        "problem_id": "div_db_courses",
        "label":      "double_not_exists",
        "pattern":    "NOT EXISTS / NOT EXISTS (pure existential double negation)",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'DB'
                AND NOT EXISTS (
                    SELECT 1 FROM Takes t
                    WHERE t.StuID = s.StuID AND t.CourseID = c.CourseID
                )
            );
        """,
    },
    {
        "problem_id": "div_all_cs_courses",
        "label":      "group_by_count",
        "pattern":    "GROUP BY + HAVING COUNT = scalar subquery",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE s.Major = 'CS' AND c."Group" = 'CS'
            GROUP BY s.StuID, s.Name
            HAVING COUNT(DISTINCT t.CourseID) = (SELECT COUNT(*) FROM Courses WHERE "Group" = 'CS');
        """,
    },
    {
        "problem_id": "div_all_cs_courses",
        "label":      "double_not_exists",
        "pattern":    "NOT EXISTS / NOT EXISTS",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            WHERE s.Major = 'CS'
            AND NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'CS'
                AND NOT EXISTS (
                    SELECT 1 FROM Takes t
                    WHERE t.StuID = s.StuID AND t.CourseID = c.CourseID
                )
            );
        """,
    },
    {
        "problem_id": "div_instructor_all_db",
        "label":      "group_by_count",
        "pattern":    "GROUP BY + HAVING COUNT = scalar subquery",
        "query": """
            SELECT i.InstID, i.Name
            FROM Instructors i
            JOIN Teaches tc ON i.InstID   = tc.InstID
            JOIN Courses c  ON tc.CourseID = c.CourseID
            WHERE c."Group" = 'DB'
            GROUP BY i.InstID, i.Name
            HAVING COUNT(DISTINCT tc.CourseID) = (SELECT COUNT(*) FROM Courses WHERE "Group" = 'DB');
        """,
    },
    {
        "problem_id": "div_instructor_all_db",
        "label":      "double_not_exists",
        "pattern":    "NOT EXISTS / NOT EXISTS",
        "query": """
            SELECT i.InstID, i.Name
            FROM Instructors i
            WHERE NOT EXISTS (
                SELECT 1 FROM Courses c
                WHERE c."Group" = 'DB'
                AND NOT EXISTS (
                    SELECT 1 FROM Teaches tc
                    WHERE tc.InstID = i.InstID AND tc.CourseID = c.CourseID
                )
            );
        """,
    },
    {
        "problem_id": "div_students_all_math",
        "label":      "group_by_count",
        "pattern":    "GROUP BY + HAVING COUNT = scalar subquery",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            LEFT JOIN Takes t ON s.StuID = t.StuID
            LEFT JOIN Courses c ON t.CourseID = c.CourseID AND c."Group" = 'Math'
            GROUP BY s.StuID, s.Name
            HAVING COUNT(DISTINCT c.CourseID) = (SELECT COUNT(*) FROM Courses WHERE "Group" = 'Math');
        """,
    },
    {
        "problem_id": "div_instructor_all_cs",
        "label":      "group_by_count",
        "pattern":    "GROUP BY + HAVING COUNT = scalar subquery",
        "query": """
            SELECT i.InstID, i.Name
            FROM Instructors i
            JOIN Teaches tc ON i.InstID   = tc.InstID
            JOIN Courses c  ON tc.CourseID = c.CourseID
            WHERE c."Group" = 'CS'
            GROUP BY i.InstID, i.Name
            HAVING COUNT(DISTINCT tc.CourseID) = (SELECT COUNT(*) FROM Courses WHERE "Group" = 'CS');
        """,
    },

    # ══════════════════════════════════════════════════════════════════
    #  JOIN — subquery forms, comma-join with WHERE equi-join, reordering
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "join_cs360",
        "label":      "in_subquery",
        "pattern":    "IN (SELECT key FROM link WHERE filter)",
        "query": """
            SELECT StuID, Name FROM Students
            WHERE StuID IN (SELECT StuID FROM Takes WHERE CourseID = 'CS360');
        """,
    },
    {
        "problem_id": "join_cs360",
        "label":      "correlated_exists",
        "pattern":    "WHERE EXISTS (SELECT 1 FROM link WHERE fk=outer.pk AND filter)",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE EXISTS (SELECT 1 FROM Takes t WHERE t.StuID = s.StuID AND t.CourseID = 'CS360');
        """,
    },
    {
        "problem_id": "join_instructor_courses",
        "label":      "reordered_joins",
        "pattern":    "FROM Courses → Teaches → Instructors (reversed)",
        "query": """
            SELECT i.Name AS InstructorName, c.Title AS CourseTitle
            FROM Courses c
            JOIN Teaches tc  ON c.CourseID = tc.CourseID
            JOIN Instructors i ON tc.InstID = i.InstID
            ORDER BY i.Name;
        """,
    },
    {
        "problem_id": "join_student_grades",
        "label":      "reordered_joins",
        "pattern":    "FROM Takes → Students + Courses (link-first join order)",
        "query": """
            SELECT s.Name AS StudentName, c.Title AS CourseTitle, t.Grade
            FROM Takes t
            JOIN Students s ON t.StuID = s.StuID
            JOIN Courses c  ON t.CourseID = c.CourseID
            ORDER BY s.Name, c.Title;
        """,
    },
    {
        "problem_id": "join_four_credit_courses",
        "label":      "filter_on_clause",
        "pattern":    "Filter predicate moved from WHERE to ON",
        "query": """
            SELECT DISTINCT s.StuID, s.Name, c.Title
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID AND c.Credits = 4
            ORDER BY s.Name, c.Title;
        """,
    },
    {
        "problem_id": "join_four_credit_courses",
        "label":      "in_subquery_filter",
        "pattern":    "IN (SELECT CourseID FROM Courses WHERE ...) for filter",
        "query": """
            SELECT DISTINCT s.StuID, s.Name, c.Title
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c.CourseID IN (SELECT CourseID FROM Courses WHERE Credits = 4)
            ORDER BY s.Name, c.Title;
        """,
    },
    {
        "problem_id": "join_students_and_instructors",
        "label":      "reordered_joins",
        "pattern":    "Teaches-first 4-way join",
        "query": """
            SELECT DISTINCT s.Name AS StudentName, c.Title AS CourseTitle, i.Name AS InstructorName
            FROM Teaches tc
            JOIN Instructors i ON tc.InstID  = i.InstID
            JOIN Courses c     ON tc.CourseID = c.CourseID
            JOIN Takes t       ON c.CourseID = t.CourseID
            JOIN Students s    ON t.StuID    = s.StuID
            ORDER BY s.Name, c.Title;
        """,
    },

    # ══════════════════════════════════════════════════════════════════
    #  AGGREGATION — subqueries, derived tables, alternate HAVING forms
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "agg_multi_course",
        "label":      "distinct_count_ge_2",
        "pattern":    "HAVING COUNT(DISTINCT ...) >= 2",
        "query": """
            SELECT s.StuID, s.Name, COUNT(DISTINCT t.CourseID) AS CourseCount
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            GROUP BY s.StuID, s.Name
            HAVING COUNT(DISTINCT t.CourseID) >= 2;
        """,
    },
    {
        "problem_id": "agg_avg_credits",
        "label":      "derived_table",
        "pattern":    "JOIN (SELECT ... GROUP BY ...) AS sub",
        "query": """
            SELECT s.StuID, s.Name, sub.avg_credits AS AvgCredits
            FROM Students s
            JOIN (
                SELECT t.StuID, AVG(c.Credits) AS avg_credits
                FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                GROUP BY t.StuID
            ) sub ON s.StuID = sub.StuID;
        """,
    },
    {
        "problem_id": "agg_enrollment_per_course",
        "label":      "scalar_subquery_count",
        "pattern":    "SELECT ..., (SELECT COUNT(*) ...) AS cnt",
        "query": """
            SELECT c.CourseID, c.Title,
                   (SELECT COUNT(*) FROM Takes t WHERE t.CourseID = c.CourseID) AS EnrollmentCount
            FROM Courses c
            WHERE EXISTS (SELECT 1 FROM Takes t WHERE t.CourseID = c.CourseID)
            ORDER BY EnrollmentCount DESC;
        """,
    },
    {
        "problem_id": "agg_popular_courses",
        "label":      "scalar_subquery_filter",
        "pattern":    "WHERE (SELECT COUNT(*) ...) > N",
        "query": """
            SELECT c.CourseID, c.Title,
                   (SELECT COUNT(*) FROM Takes t WHERE t.CourseID = c.CourseID) AS EnrollmentCount
            FROM Courses c
            WHERE (SELECT COUNT(*) FROM Takes t WHERE t.CourseID = c.CourseID) > 2
            ORDER BY EnrollmentCount DESC;
        """,
    },
    {
        "problem_id": "agg_avg_credits_per_group",
        "label":      "distinct_scalar_subquery",
        "pattern":    "SELECT DISTINCT col, (SELECT AVG ... WHERE ...) per row",
        "query": """
            SELECT DISTINCT c."Group",
                   (SELECT AVG(c2.Credits) FROM Courses c2 WHERE c2."Group" = c."Group") AS AvgCredits
            FROM Courses c
            ORDER BY c."Group";
        """,
    },

    # ══════════════════════════════════════════════════════════════════
    #  SET_OP — rewrite to IN / OR / NOT IN / NOT EXISTS equivalents
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "set_no_db",
        "label":      "not_in_subquery",
        "pattern":    "WHERE key NOT IN (SELECT key FROM link JOIN filter)",
        "query": """
            SELECT StuID, Name FROM Students
            WHERE StuID NOT IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c."Group" = 'DB'
            );
        """,
    },
    {
        "problem_id": "set_no_db",
        "label":      "correlated_not_exists",
        "pattern":    "WHERE NOT EXISTS (correlated subquery)",
        "query": """
            SELECT s.StuID, s.Name FROM Students s
            WHERE NOT EXISTS (
                SELECT 1 FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE t.StuID = s.StuID AND c."Group" = 'DB'
            );
        """,
    },
    {
        "problem_id": "set_union_cs_db",
        "label":      "in_list_disjunction",
        "pattern":    "WHERE c.\"Group\" IN ('CS','DB')",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c."Group" IN ('CS', 'DB');
        """,
    },
    {
        "problem_id": "set_union_cs_db",
        "label":      "or_disjunction",
        "pattern":    "WHERE c.\"Group\" = 'CS' OR c.\"Group\" = 'DB'",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c."Group" = 'CS' OR c."Group" = 'DB';
        """,
    },
    {
        "problem_id": "set_intersect_cs_and_db",
        "label":      "double_in",
        "pattern":    "WHERE key IN (…CS…) AND key IN (…DB…)",
        "query": """
            SELECT DISTINCT s.StuID, s.Name FROM Students s
            WHERE s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID WHERE c."Group" = 'CS'
            )
            AND s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID WHERE c."Group" = 'DB'
            );
        """,
    },
    {
        "problem_id": "set_intersect_cs_and_db",
        "label":      "double_exists",
        "pattern":    "WHERE EXISTS (…CS…) AND EXISTS (…DB…)",
        "query": """
            SELECT DISTINCT s.StuID, s.Name FROM Students s
            WHERE EXISTS (
                SELECT 1 FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE t.StuID = s.StuID AND c."Group" = 'CS'
            )
            AND EXISTS (
                SELECT 1 FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE t.StuID = s.StuID AND c."Group" = 'DB'
            );
        """,
    },
    {
        "problem_id": "set_except_instructors_no_db",
        "label":      "not_in_subquery",
        "pattern":    "WHERE key NOT IN (SELECT key FROM link WHERE filter)",
        "query": """
            SELECT InstID, Name FROM Instructors
            WHERE InstID NOT IN (
                SELECT tc.InstID FROM Teaches tc
                JOIN Courses c ON tc.CourseID = c.CourseID
                WHERE c."Group" = 'DB'
            );
        """,
    },
    {
        "problem_id": "set_except_instructors_no_db",
        "label":      "correlated_not_exists",
        "pattern":    "WHERE NOT EXISTS (correlated)",
        "query": """
            SELECT i.InstID, i.Name FROM Instructors i
            WHERE NOT EXISTS (
                SELECT 1 FROM Teaches tc
                JOIN Courses c ON tc.CourseID = c.CourseID
                WHERE tc.InstID = i.InstID AND c."Group" = 'DB'
            );
        """,
    },

    # ══════════════════════════════════════════════════════════════════
    #  SUBQUERY — IN↔EXISTS↔JOIN equivalents, derived-table forms
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "sub_above_avg_credits",
        "label":      "derived_table_join",
        "pattern":    "JOIN (SELECT AVG ...) avg_c ON Credits > avg_c.a",
        "query": """
            SELECT c.CourseID, c.Title, c.Credits
            FROM Courses c
            JOIN (SELECT AVG(Credits) AS a FROM Courses) avg_c
              ON c.Credits > avg_c.a;
        """,
    },
    {
        "problem_id": "sub_most_enrollments",
        "label":      "max_via_order_limit",
        "pattern":    "HAVING COUNT(*) = (SELECT COUNT … GROUP BY … ORDER BY … LIMIT 1)",
        "query": """
            SELECT s.StuID, s.Name
            FROM Students s
            JOIN Takes t ON s.StuID = t.StuID
            GROUP BY s.StuID, s.Name
            HAVING COUNT(t.CourseID) = (
                SELECT COUNT(*) AS cnt FROM Takes
                GROUP BY StuID
                ORDER BY cnt DESC
                LIMIT 1
            );
        """,
    },
    {
        "problem_id": "sub_in_db_courses",
        "label":      "correlated_exists",
        "pattern":    "WHERE EXISTS (correlated subquery)",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            WHERE EXISTS (
                SELECT 1 FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE t.StuID = s.StuID AND c."Group" = 'DB'
            );
        """,
    },
    {
        "problem_id": "sub_in_db_courses",
        "label":      "join_no_subquery",
        "pattern":    "JOIN approach (no subquery)",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c."Group" = 'DB';
        """,
    },
    {
        "problem_id": "sub_exists_four_credit",
        "label":      "in_subquery",
        "pattern":    "WHERE key IN (SELECT key FROM link WHERE filter)",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            WHERE s.StuID IN (
                SELECT t.StuID FROM Takes t
                JOIN Courses c ON t.CourseID = c.CourseID
                WHERE c.Credits = 4
            );
        """,
    },
    {
        "problem_id": "sub_exists_four_credit",
        "label":      "join_no_subquery",
        "pattern":    "Direct JOIN (no subquery at all)",
        "query": """
            SELECT DISTINCT s.StuID, s.Name
            FROM Students s
            JOIN Takes t   ON s.StuID    = t.StuID
            JOIN Courses c ON t.CourseID = c.CourseID
            WHERE c.Credits = 4;
        """,
    },

    # ══════════════════════════════════════════════════════════════════
    #  NULL — COALESCE/NULLIF alternates, subquery form
    # ══════════════════════════════════════════════════════════════════
    {
        "problem_id": "null_no_grade",
        "label":      "scalar_subquery_for_name",
        "pattern":    "FROM Takes + scalar subquery for Name",
        "query": """
            SELECT t.StuID,
                   (SELECT Name FROM Students WHERE StuID = t.StuID) AS Name,
                   t.CourseID
            FROM Takes t
            WHERE t.Grade IS NULL;
        """,
    },
    {
        "problem_id": "null_major_missing",
        "label":      "coalesce_sentinel",
        "pattern":    "WHERE COALESCE(col, sentinel) = sentinel",
        "query": """
            SELECT StuID, Name
            FROM Students
            WHERE COALESCE(Major, '__NULL__') = '__NULL__';
        """,
    },
    {
        "problem_id": "null_major_missing",
        "label":      "self_subquery",
        "pattern":    "WHERE StuID IN (SELECT StuID ... WHERE IS NULL)",
        "query": """
            SELECT StuID, Name
            FROM Students
            WHERE StuID IN (SELECT StuID FROM Students WHERE Major IS NULL);
        """,
    },
]


if __name__ == "__main__":
    # Quick sanity print
    from collections import Counter
    print(f"Corpus entries: {len(CORPUS)}")
    by_pid = Counter(e["problem_id"] for e in CORPUS)
    for pid, cnt in sorted(by_pid.items()):
        print(f"  {pid:<40} {cnt} alt(s)")

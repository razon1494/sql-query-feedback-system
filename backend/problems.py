"""
Problem set definitions — SQL problems for student practice.

To add a new problem: append a new dict to PROBLEMS.
Required fields: id, title, description, question, type, difficulty,
                 concepts, base_query, hint, common_mistakes
No other files need to be changed — problems appear automatically.
"""

PROBLEMS = [

    # ══ DIVISION ══════════════════════════════════════════
    {
        "id": "div_db_courses",
        "title": "Division: Students Who Took ALL DB Courses",
        "description": (
            "Find all students who have taken <strong>every</strong> database ('DB') course. "
            "This is the relational division operation. A student qualifies only if their "
            "enrollment covers the complete set of DB courses — not just some of them."
        ),
        "question": "Which students took all database (Group='DB') courses?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Correlated Subquery", "Relational Division", "Universal Quantification"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
WHERE NOT EXISTS (
    SELECT c.CourseID
    FROM Courses c
    WHERE c."Group" = 'DB'
    AND c.CourseID NOT IN (
        SELECT t.CourseID
        FROM Takes t
        WHERE t.StuID = s.StuID
    )
);""",
        "hint": (
            "Think in reverse: a student qualifies if there is NO DB course "
            "that they have NOT taken. Use NOT EXISTS with a correlated subquery."
        ),
        "common_mistakes": ["Using IN instead of NOT EXISTS", "Forgetting correlated reference", "Hardcoded HAVING threshold"],
    },
    {
        "id": "div_all_cs_courses",
        "title": "Division: CS Students Who Took All CS-Group Courses",
        "description": (
            "Find CS major students who have taken <strong>every</strong> CS-group course. "
            "Combines a WHERE filter on Major with the division pattern."
        ),
        "question": "Which CS-major students took all CS-group courses?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Division", "Filter + Division"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
WHERE s.Major = 'CS'
AND NOT EXISTS (
    SELECT c.CourseID
    FROM Courses c
    WHERE c."Group" = 'CS'
    AND c.CourseID NOT IN (
        SELECT t.CourseID
        FROM Takes t
        WHERE t.StuID = s.StuID
    )
);""",
        "hint": "First filter by Major='CS', then apply the NOT EXISTS division pattern on CS-group courses.",
        "common_mistakes": ["Missing Major filter", "Using wrong group", "IN instead of NOT EXISTS"],
    },
    {
        "id": "div_instructor_all_db",
        "title": "Division: Instructors Who Teach ALL DB Courses",
        "description": (
            "Find all instructors who teach <strong>every</strong> DB course. "
            "Apply the division pattern on the Teaches and Courses tables."
        ),
        "question": "Which instructors teach all DB courses?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Division", "Teaches Table", "Correlated Subquery"],
        "base_query": """SELECT i.InstID, i.Name
FROM Instructors i
WHERE NOT EXISTS (
    SELECT c.CourseID
    FROM Courses c
    WHERE c."Group" = 'DB'
    AND c.CourseID NOT IN (
        SELECT tc.CourseID
        FROM Teaches tc
        WHERE tc.InstID = i.InstID
    )
);""",
        "hint": "Same NOT EXISTS division pattern as the student version, but applied to Instructors and Teaches tables.",
        "common_mistakes": ["Using wrong tables", "Forgetting correlated reference to InstID", "IN instead of NOT EXISTS"],
    },
    {
        "id": "div_students_all_math",
        "title": "Division: Students Who Took ALL Math Courses",
        "description": (
            "Find all students who have taken <strong>every</strong> course in the 'Math' group. "
            "Apply the same universal-quantification pattern used for DB, but over Math-group courses."
        ),
        "question": "Which students took all Math-group courses?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Relational Division", "Universal Quantification", "Correlated Subquery"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
WHERE NOT EXISTS (
    SELECT c.CourseID
    FROM Courses c
    WHERE c."Group" = 'Math'
    AND c.CourseID NOT IN (
        SELECT t.CourseID
        FROM Takes t
        WHERE t.StuID = s.StuID
    )
);""",
        "hint": "Reuse the DB-courses division template, but change the group filter from 'DB' to 'Math'.",
        "common_mistakes": ["Using IN instead of NOT EXISTS", "Hardcoded COUNT threshold", "Wrong group literal"],
    },
    {
        "id": "div_instructor_all_cs",
        "title": "Division: Instructors Who Teach ALL CS Courses",
        "description": (
            "Find instructors who teach <strong>every</strong> CS-group course. "
            "Parallel to the DB version, but over the CS course group."
        ),
        "question": "Which instructors teach all CS-group courses?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Division", "Teaches Table", "Correlated Subquery"],
        "base_query": """SELECT i.InstID, i.Name
FROM Instructors i
WHERE NOT EXISTS (
    SELECT c.CourseID
    FROM Courses c
    WHERE c."Group" = 'CS'
    AND c.CourseID NOT IN (
        SELECT tc.CourseID
        FROM Teaches tc
        WHERE tc.InstID = i.InstID
    )
);""",
        "hint": "Mirror the DB-instructor division, but filter Courses on Group='CS'.",
        "common_mistakes": ["Using IN instead of NOT EXISTS", "Forgetting correlated reference to InstID", "Confusing Teaches with Takes"],
    },

    # ══ JOIN ══════════════════════════════════════════════
    {
        "id": "join_cs360",
        "title": "Join: Students Enrolled in Intro DB (CS360)",
        "description": (
            "Find all students enrolled in <strong>CS360 (Intro DB)</strong>. "
            "Use a JOIN between Students and Takes."
        ),
        "question": "Which students are enrolled in CS360?",
        "type": "JOIN",
        "difficulty": "easy",
        "concepts": ["INNER JOIN", "WHERE", "Table Alias"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
WHERE t.CourseID = 'CS360';""",
        "hint": "Join Students and Takes on StuID, then filter by CourseID.",
        "common_mistakes": ["Using subquery instead of JOIN", "Wrong JOIN condition", "Missing columns"],
    },
    {
        "id": "join_instructor_courses",
        "title": "Join: Instructors With Their Course Titles",
        "description": (
            "Retrieve each instructor's name alongside the <strong>title</strong> of every course they teach. "
            "Join Instructors → Teaches → Courses (three tables)."
        ),
        "question": "Show each instructor's name and the title of every course they teach.",
        "type": "JOIN",
        "difficulty": "easy",
        "concepts": ["INNER JOIN", "Multi-table JOIN", "Table Alias"],
        "base_query": """SELECT i.Name AS InstructorName, c.Title AS CourseTitle
FROM Instructors i
JOIN Teaches tc ON i.InstID   = tc.InstID
JOIN Courses c  ON tc.CourseID = c.CourseID
ORDER BY i.Name;""",
        "hint": "You need three tables: Instructors, Teaches (bridge table), and Courses. Chain two JOINs.",
        "common_mistakes": ["Skipping the bridge table Teaches", "Wrong join keys", "Missing ORDER BY"],
    },
    {
        "id": "join_student_grades",
        "title": "Join: Students With Their Course Grades",
        "description": (
            "Retrieve each student's name, the course title they enrolled in, and their grade. "
            "Join Students → Takes → Courses."
        ),
        "question": "Show each student name, course title, and their grade.",
        "type": "JOIN",
        "difficulty": "easy",
        "concepts": ["INNER JOIN", "Multi-table JOIN", "SELECT columns"],
        "base_query": """SELECT s.Name AS StudentName, c.Title AS CourseTitle, t.Grade
FROM Students s
JOIN Takes t    ON s.StuID    = t.StuID
JOIN Courses c  ON t.CourseID = c.CourseID
ORDER BY s.Name, c.Title;""",
        "hint": "Chain Students → Takes → Courses using two JOIN clauses.",
        "common_mistakes": ["Missing second JOIN", "Wrong ON conditions", "Selecting wrong column names"],
    },
    {
        "id": "join_four_credit_courses",
        "title": "Join: Students Enrolled in 4-Credit Courses",
        "description": (
            "Retrieve each student's name and the title of every <strong>4-credit</strong> course "
            "they are enrolled in. Requires a three-table JOIN with a predicate on Credits."
        ),
        "question": "Which students are enrolled in 4-credit courses, and which ones?",
        "type": "JOIN",
        "difficulty": "easy",
        "concepts": ["INNER JOIN", "Multi-table JOIN", "WHERE Filter", "DISTINCT"],
        "base_query": """SELECT DISTINCT s.StuID, s.Name, c.Title
FROM Students s
JOIN Takes t   ON s.StuID    = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c.Credits = 4
ORDER BY s.Name, c.Title;""",
        "hint": "Join all three tables, then filter on Courses.Credits = 4.",
        "common_mistakes": ["Missing join condition (cartesian product)", "Forgetting DISTINCT", "Filtering on the wrong column"],
    },
    {
        "id": "join_students_and_instructors",
        "title": "Join: Students With Their Course Instructors",
        "description": (
            "For each student, retrieve the <strong>instructor name</strong> and <strong>course title</strong> "
            "for every course they are enrolled in. Requires a four-way JOIN across "
            "Students, Takes, Teaches, and Instructors."
        ),
        "question": "Show each student with the instructors of the courses they are enrolled in.",
        "type": "JOIN",
        "difficulty": "medium",
        "concepts": ["INNER JOIN", "Multi-table JOIN", "Four-way JOIN", "Table Alias"],
        "base_query": """SELECT DISTINCT s.Name AS StudentName,
       c.Title AS CourseTitle,
       i.Name AS InstructorName
FROM Students s
JOIN Takes t      ON s.StuID    = t.StuID
JOIN Courses c    ON t.CourseID = c.CourseID
JOIN Teaches tc   ON c.CourseID = tc.CourseID
JOIN Instructors i ON tc.InstID = i.InstID
ORDER BY s.Name, c.Title;""",
        "hint": "Connect Students → Takes → Courses, then bridge Courses → Teaches → Instructors.",
        "common_mistakes": ["Missing join condition (cartesian product)", "Skipping the Teaches bridge table", "Wrong join key between Courses and Teaches"],
    },

    # ══ AGGREGATION ═══════════════════════════════════════
    {
        "id": "agg_multi_course",
        "title": "Aggregation: Students Enrolled in More Than 1 Course",
        "description": (
            "Find students who are enrolled in <strong>more than one</strong> course. "
            "Use GROUP BY and HAVING with COUNT."
        ),
        "question": "Which students have taken more than 1 course?",
        "type": "AGGREGATION",
        "difficulty": "medium",
        "concepts": ["GROUP BY", "HAVING", "COUNT", "INNER JOIN"],
        "base_query": """SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
GROUP BY s.StuID, s.Name
HAVING COUNT(t.CourseID) > 1;""",
        "hint": "Join Students with Takes, group by student, then filter groups with HAVING.",
        "common_mistakes": ["Missing HAVING", "Forgetting GROUP BY", "Using WHERE instead of HAVING"],
    },
    {
        "id": "agg_avg_credits",
        "title": "Aggregation: Average Credits Per Student",
        "description": (
            "Calculate the <strong>average number of credits</strong> each student has enrolled in. "
            "Join Students → Takes → Courses and use AVG with GROUP BY."
        ),
        "question": "What is the average course credits enrolled per student?",
        "type": "AGGREGATION",
        "difficulty": "medium",
        "concepts": ["GROUP BY", "AVG", "Multi-table JOIN"],
        "base_query": """SELECT s.StuID, s.Name, AVG(c.Credits) AS AvgCredits
FROM Students s
JOIN Takes t   ON s.StuID    = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
GROUP BY s.StuID, s.Name;""",
        "hint": "Join all three tables, group by student, and apply AVG on the Credits column from Courses.",
        "common_mistakes": ["Forgetting JOIN to Courses for Credits", "Using COUNT instead of AVG", "Missing GROUP BY"],
    },
    {
        "id": "agg_enrollment_per_course",
        "title": "Aggregation: Enrollment Count Per Course",
        "description": (
            "Return each course with the <strong>number of students</strong> enrolled in it. "
            "Group rows in Takes by CourseID and count students."
        ),
        "question": "How many students are enrolled in each course?",
        "type": "AGGREGATION",
        "difficulty": "medium",
        "concepts": ["GROUP BY", "COUNT", "INNER JOIN", "ORDER BY"],
        "base_query": """SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
FROM Courses c
JOIN Takes t ON c.CourseID = t.CourseID
GROUP BY c.CourseID, c.Title
ORDER BY EnrollmentCount DESC;""",
        "hint": "Join Courses with Takes, group by the course, and COUNT the matched Takes rows.",
        "common_mistakes": ["Missing GROUP BY", "Grouping by Title only (non-unique)", "Using COUNT(*) without a join"],
    },
    {
        "id": "agg_popular_courses",
        "title": "Aggregation: Courses With More Than 2 Enrollments",
        "description": (
            "Find courses with <strong>more than 2 students</strong> enrolled. "
            "Requires grouping, counting, and a HAVING filter — WHERE cannot filter aggregated values."
        ),
        "question": "Which courses have more than 2 students enrolled?",
        "type": "AGGREGATION",
        "difficulty": "medium",
        "concepts": ["GROUP BY", "HAVING", "COUNT", "Aggregate Filter"],
        "base_query": """SELECT c.CourseID, c.Title, COUNT(t.StuID) AS EnrollmentCount
FROM Courses c
JOIN Takes t ON c.CourseID = t.CourseID
GROUP BY c.CourseID, c.Title
HAVING COUNT(t.StuID) > 2
ORDER BY EnrollmentCount DESC;""",
        "hint": "You cannot filter COUNT in WHERE. Use HAVING after GROUP BY to filter aggregated values.",
        "common_mistakes": ["Using WHERE instead of HAVING", "Missing GROUP BY", "Filtering before aggregation"],
    },
    {
        "id": "agg_avg_credits_per_group",
        "title": "Aggregation: Average Credits Per Course Group",
        "description": (
            "Compute the <strong>average credit value</strong> of courses within each course group "
            "(e.g., CS, DB, Math). Requires GROUP BY on the Group attribute."
        ),
        "question": "What is the average number of credits for courses in each group?",
        "type": "AGGREGATION",
        "difficulty": "medium",
        "concepts": ["GROUP BY", "AVG", "Single-table Aggregation"],
        "base_query": """SELECT "Group", AVG(Credits) AS AvgCredits
FROM Courses
GROUP BY "Group"
ORDER BY "Group";""",
        "hint": "A single-table aggregation — group Courses by the Group column and apply AVG on Credits.",
        "common_mistakes": ["Missing GROUP BY", "Using SUM instead of AVG", "Forgetting to quote the reserved word \"Group\""],
    },

    # ══ SET OPERATIONS ════════════════════════════════════
    {
        "id": "set_no_db",
        "title": "Set Operation: Students NOT Enrolled in Any DB Course",
        "description": (
            "Find students who have <strong>not enrolled</strong> in any DB course. "
            "Use EXCEPT or NOT IN."
        ),
        "question": "Which students have taken no DB courses?",
        "type": "SET_OP",
        "difficulty": "medium",
        "concepts": ["EXCEPT", "NOT IN", "Subquery", "Set Difference"],
        "base_query": """SELECT StuID, Name FROM Students
EXCEPT
SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'DB';""",
        "hint": "Find all students, then subtract those who have taken at least one DB course.",
        "common_mistakes": ["Wrong set operation", "Missing join to Courses", "Confusing UNION with EXCEPT"],
    },
    {
        "id": "set_union_cs_db",
        "title": "Set Operation: Students in CS or DB Courses (UNION)",
        "description": (
            "Find all students who have taken <strong>at least one CS-group OR DB-group</strong> course. "
            "Use UNION to combine both sets without duplicates."
        ),
        "question": "Which students enrolled in at least one CS or DB group course?",
        "type": "SET_OP",
        "difficulty": "medium",
        "concepts": ["UNION", "Subquery", "Set Union", "Deduplication"],
        "base_query": """SELECT DISTINCT s.StuID, s.Name
FROM Students s
JOIN Takes t   ON s.StuID    = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'CS'
UNION
SELECT DISTINCT s.StuID, s.Name
FROM Students s
JOIN Takes t   ON s.StuID    = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'DB';""",
        "hint": "Write two SELECT statements — one for Group='CS', one for Group='DB' — and combine with UNION.",
        "common_mistakes": ["Using UNION ALL (includes duplicates)", "Using INTERSECT instead of UNION", "Missing JOIN to Courses"],
    },
    {
        "id": "set_intersect_cs_and_db",
        "title": "Set Operation: Students in BOTH CS and DB Courses (INTERSECT)",
        "description": (
            "Find students who have taken <strong>at least one CS course AND at least one DB course</strong>. "
            "Use INTERSECT to retain only the students appearing in both sets."
        ),
        "question": "Which students have taken at least one CS course and at least one DB course?",
        "type": "SET_OP",
        "difficulty": "medium",
        "concepts": ["INTERSECT", "Subquery", "Set Intersection"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t   ON s.StuID    = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'CS'
INTERSECT
SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t   ON s.StuID    = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'DB';""",
        "hint": "Compute CS-students and DB-students separately, then INTERSECT the two sets.",
        "common_mistakes": ["Using UNION instead of INTERSECT", "Using EXCEPT instead of INTERSECT", "Missing JOIN to Courses"],
    },
    {
        "id": "set_except_instructors_no_db",
        "title": "Set Operation: Instructors Who Do NOT Teach Any DB Course",
        "description": (
            "Find instructors who <strong>do not teach</strong> any DB-group course. "
            "Use EXCEPT to subtract DB-teaching instructors from the full instructor set."
        ),
        "question": "Which instructors teach no DB-group courses?",
        "type": "SET_OP",
        "difficulty": "medium",
        "concepts": ["EXCEPT", "Set Difference", "Subquery"],
        "base_query": """SELECT InstID, Name FROM Instructors
EXCEPT
SELECT i.InstID, i.Name
FROM Instructors i
JOIN Teaches tc ON i.InstID   = tc.InstID
JOIN Courses c  ON tc.CourseID = c.CourseID
WHERE c."Group" = 'DB';""",
        "hint": "Take all instructors, then subtract those who appear as teachers of any DB course.",
        "common_mistakes": ["Using NOT IN with NULLs (risky)", "Wrong set operator (UNION instead of EXCEPT)", "Missing JOIN to Courses"],
    },

    # ══ SUBQUERY ══════════════════════════════════════════
    {
        "id": "sub_above_avg_credits",
        "title": "Subquery: Courses With Above-Average Credits",
        "description": (
            "Find courses whose credit value is <strong>greater than the average</strong> credit "
            "across all courses. Requires a scalar subquery in the WHERE clause."
        ),
        "question": "Which courses have more credits than the average course?",
        "type": "SUBQUERY",
        "difficulty": "medium",
        "concepts": ["Scalar Subquery", "AVG", "WHERE Comparison"],
        "base_query": """SELECT CourseID, Title, Credits
FROM Courses
WHERE Credits > (
    SELECT AVG(Credits) FROM Courses
);""",
        "hint": "Compute AVG(Credits) in an inner SELECT, then compare each course's credits against it.",
        "common_mistakes": ["Using MAX instead of AVG", "Placing AVG in SELECT instead of WHERE", "Forgetting the outer query filter"],
    },
    {
        "id": "sub_most_enrollments",
        "title": "Subquery: Student(s) With the Most Enrollments",
        "description": (
            "Find the student(s) who are enrolled in the <strong>largest number</strong> of courses. "
            "Requires a nested aggregation: MAX over per-student COUNT values."
        ),
        "question": "Which student has the highest number of course enrollments?",
        "type": "SUBQUERY",
        "difficulty": "hard",
        "concepts": ["Nested Aggregation", "MAX", "COUNT", "GROUP BY", "HAVING"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
GROUP BY s.StuID, s.Name
HAVING COUNT(t.CourseID) = (
    SELECT MAX(cnt)
    FROM (
        SELECT COUNT(CourseID) AS cnt
        FROM Takes
        GROUP BY StuID
    ) sub
);""",
        "hint": "First compute per-student course counts in a derived table, then take MAX. Match with HAVING.",
        "common_mistakes": ["Using MAX(COUNT(...)) directly (not allowed)", "Missing derived table", "Using WHERE instead of HAVING"],
    },
    {
        "id": "sub_in_db_courses",
        "title": "Subquery: Students Enrolled in DB Courses (IN)",
        "description": (
            "Find students who are enrolled in <strong>any DB-group</strong> course, "
            "using an IN subquery to first fetch all DB CourseIDs."
        ),
        "question": "Which students are enrolled in at least one DB-group course (using IN)?",
        "type": "SUBQUERY",
        "difficulty": "medium",
        "concepts": ["IN Subquery", "Uncorrelated Subquery", "Set Membership"],
        "base_query": """SELECT DISTINCT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
WHERE t.CourseID IN (
    SELECT CourseID
    FROM Courses
    WHERE "Group" = 'DB'
);""",
        "hint": "The inner query returns the list of DB CourseIDs; the outer query filters Takes.CourseID by IN.",
        "common_mistakes": ["Using EXISTS without correlation (wrong semantics)", "Forgetting DISTINCT", "Confusing IN with =ANY subtleties under NULLs"],
    },
    {
        "id": "sub_exists_four_credit",
        "title": "Subquery: Students Who Have Taken a 4-Credit Course (EXISTS)",
        "description": (
            "Find students who have taken <strong>at least one 4-credit</strong> course. "
            "Use a correlated EXISTS subquery over Takes joined with Courses."
        ),
        "question": "Which students have taken at least one 4-credit course (using EXISTS)?",
        "type": "SUBQUERY",
        "difficulty": "medium",
        "concepts": ["EXISTS", "Correlated Subquery", "Existential Quantification"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
WHERE EXISTS (
    SELECT 1
    FROM Takes t
    JOIN Courses c ON t.CourseID = c.CourseID
    WHERE t.StuID = s.StuID
    AND c.Credits = 4
);""",
        "hint": "Use EXISTS with a correlated reference t.StuID = s.StuID. The inner query only needs to find one row.",
        "common_mistakes": ["Missing correlation (t.StuID = s.StuID)", "Using IN instead of EXISTS (loses correlation)", "Selecting full columns in EXISTS instead of SELECT 1"],
    },

    # ══ NULL HANDLING ═════════════════════════════════════
    {
        "id": "null_no_grade",
        "title": "NULL Handling: Enrollments Without a Grade",
        "description": (
            "Find all student–course enrollments where a grade has <strong>not yet been recorded</strong>. "
            "Requires the IS NULL predicate — equality comparisons with NULL always yield UNKNOWN."
        ),
        "question": "Which enrollments have no grade recorded?",
        "type": "NULL",
        "difficulty": "easy",
        "concepts": ["IS NULL", "Three-Valued Logic", "NULL Semantics"],
        "base_query": """SELECT s.StuID, s.Name, t.CourseID
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
WHERE t.Grade IS NULL;""",
        "hint": "Never use '= NULL' — it evaluates to UNKNOWN. Use the IS NULL predicate instead.",
        "common_mistakes": ["Using = NULL instead of IS NULL", "Using != NULL instead of IS NOT NULL", "Assuming NULL equals empty string"],
    },
    {
        "id": "null_major_missing",
        "title": "NULL Handling: Students With No Declared Major",
        "description": (
            "Find all students whose <strong>Major is missing</strong> (NULL). "
            "This tests correct use of IS NULL on a single-table query."
        ),
        "question": "Which students have no major declared?",
        "type": "NULL",
        "difficulty": "easy",
        "concepts": ["IS NULL", "Single-table Filter", "NULL Semantics"],
        "base_query": """SELECT StuID, Name
FROM Students
WHERE Major IS NULL;""",
        "hint": "Filter with 'Major IS NULL'. Remember: Major = NULL is always UNKNOWN and returns no rows.",
        "common_mistakes": ["Using = NULL instead of IS NULL", "Filtering for empty string '' instead of NULL", "Using COALESCE incorrectly as a filter"],
    },

    # ══ COMPLEX DIVISION OVER PREREQUISITES AND DEPARTMENTS ══════════════
    #  These five problems exercise the 7-table schema introduced in v2:
    #  Prerequisites and Departments. Each embeds universal quantification
    #  in a structurally more demanding form than the basic division problems
    #  above — the divisor set is either itself produced by a subquery
    #  (prereqs of a given course) or correlated with the outer tuple
    #  (courses in the student's own department).
    # ═════════════════════════════════════════════════════════════════════
    {
        "id": "div_prereq_chain",
        "title": "Complex Division: Students Who Took Every Prerequisite of Every Course They Enrolled In",
        "description": (
            "Find all students who, for <strong>every</strong> course they have taken, "
            "have also taken every prerequisite of that course. "
            "This problem embeds relational division inside an existential check: "
            "for each enrolled course, the student's enrollment must cover the full "
            "prerequisite set of that course."
        ),
        "question": "Which students have completed every prerequisite of every course they are enrolled in?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Nested Universal Quantification", "Prerequisites", "Correlated Subquery"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
WHERE NOT EXISTS (
    SELECT 1 FROM Takes t
    WHERE t.StuID = s.StuID
    AND EXISTS (
        SELECT 1 FROM Prerequisites p
        WHERE p.CourseID = t.CourseID
        AND NOT EXISTS (
            SELECT 1 FROM Takes t2
            WHERE t2.StuID = s.StuID AND t2.CourseID = p.PrereqID
        )
    )
);""",
        "hint": "The outer NOT EXISTS says 'there is no course the student took that has an unmet prerequisite'. You need a triple nesting.",
        "common_mistakes": [
            "Using IN for partial match instead of NOT EXISTS for universal coverage",
            "Omitting the outer correlation t.StuID = s.StuID",
            "Flattening the query and losing the per-course prerequisite check"
        ],
    },
    {
        "id": "div_dept_all_courses",
        "title": "Complex Division: Students Who Took Every Course in the CS Department",
        "description": (
            "Find all students who have taken <strong>every</strong> course offered by "
            "the Computer Science department (DeptID = 'D1'). "
            "The divisor set is defined by a department-level filter on Courses, and "
            "every course in that set must be present in the student's enrollment record."
        ),
        "question": "Which students have taken every course offered by the CS department?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Department Filter", "DeptID Foreign Key", "Universal Quantification"],
        "base_query": """SELECT s.StuID, s.Name
FROM Students s
WHERE NOT EXISTS (
    SELECT 1 FROM Courses c
    WHERE c.DeptID = 'D1'
    AND NOT EXISTS (
        SELECT 1 FROM Takes t
        WHERE t.StuID = s.StuID AND t.CourseID = c.CourseID
    )
);""",
        "hint": "The divisor set is 'courses in department D1'. Use double NOT EXISTS to verify full coverage.",
        "common_mistakes": [
            "Using IN (SELECT CourseID FROM Courses WHERE DeptID='D1') — only checks partial coverage",
            "Hardcoding the number of D1 courses in a HAVING clause",
            "Joining Students to Courses without the per-student universal check"
        ],
    },
    {
        "id": "div_instructor_all_prereqs",
        "title": "Complex Division: Instructors Who Teach Every Prerequisite of CS460",
        "description": (
            "Find all instructors who teach <strong>every</strong> prerequisite of course "
            "CS460 (Advanced Database). The divisor set itself is produced by a subquery over "
            "the Prerequisites table, and each prerequisite must appear in the instructor's "
            "Teaches record."
        ),
        "question": "Which instructors teach every prerequisite of CS460?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Subquery-Defined Divisor", "Prerequisites", "Teaches"],
        "base_query": """SELECT i.InstID, i.Name
FROM Instructors i
WHERE NOT EXISTS (
    SELECT 1 FROM Prerequisites p
    WHERE p.CourseID = 'CS460'
    AND NOT EXISTS (
        SELECT 1 FROM Teaches tc
        WHERE tc.InstID = i.InstID AND tc.CourseID = p.PrereqID
    )
);""",
        "hint": "The divisor is 'prerequisites of CS460' — this is itself a subquery. Verify coverage per instructor via NOT EXISTS.",
        "common_mistakes": [
            "Using IN against Prerequisites instead of NOT EXISTS",
            "Joining Instructors to Prerequisites directly without the per-instructor coverage check",
            "Missing the correlation tc.InstID = i.InstID"
        ],
    },
    {
        "id": "sub_missing_prereq",
        "title": "Subquery: Students Enrolled in Courses Whose Prerequisites They Have Not Completed",
        "description": (
            "Find students who are enrolled in at least one course for which they have "
            "<strong>not</strong> completed every prerequisite. "
            "This is the logical complement of the prereq-chain division problem and "
            "surfaces the same structural pattern in a positive existential form."
        ),
        "question": "Which students are taking a course without having completed all of its prerequisites?",
        "type": "SUBQUERY",
        "difficulty": "hard",
        "concepts": ["EXISTS", "NOT EXISTS", "Correlated Subquery", "Prerequisites"],
        "base_query": """SELECT DISTINCT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
WHERE EXISTS (
    SELECT 1 FROM Prerequisites p
    WHERE p.CourseID = t.CourseID
    AND NOT EXISTS (
        SELECT 1 FROM Takes t2
        WHERE t2.StuID = s.StuID AND t2.CourseID = p.PrereqID
    )
);""",
        "hint": "There EXISTS a (course, prereq) pair in your enrollment where the prereq is missing.",
        "common_mistakes": [
            "Replacing NOT EXISTS with NOT IN inside the prerequisite check (NULL-unsafe)",
            "Dropping the DISTINCT and producing duplicate student rows",
            "Omitting the correlation and returning every student whenever any prerequisite is globally missing"
        ],
    },
    {
        "id": "div_dept_covered",
        "title": "Complex Division: Departments Where Every Course Has At Least One Enrollment",
        "description": (
            "Find all departments for which every course offered by that department has "
            "at least one enrolled student. The outer tuple is the department; the divisor "
            "set is 'courses in this department'; each course must witness a row in Takes. "
            "Departments with no courses are excluded."
        ),
        "question": "Which departments have every one of their courses enrolled by at least one student?",
        "type": "DIVISION",
        "difficulty": "hard",
        "concepts": ["NOT EXISTS", "Correlated Divisor", "Department Coverage"],
        "base_query": """SELECT d.DeptID, d.Name
FROM Departments d
WHERE EXISTS (
    SELECT 1 FROM Courses c WHERE c.DeptID = d.DeptID
)
AND NOT EXISTS (
    SELECT 1 FROM Courses c
    WHERE c.DeptID = d.DeptID
    AND NOT EXISTS (
        SELECT 1 FROM Takes t WHERE t.CourseID = c.CourseID
    )
);""",
        "hint": "Two conditions: the department must have courses (EXISTS), and no course in the department may lack enrollment (NOT EXISTS / NOT EXISTS).",
        "common_mistakes": [
            "Omitting the EXISTS guard and incorrectly returning empty departments",
            "Using IN instead of NOT EXISTS for the coverage check",
            "Forgetting the correlation c.DeptID = d.DeptID"
        ],
    },
]


# ══════════════════════════════════════════════════════════════════════
#  HOW TO ADD YOUR OWN PROBLEM  (no other files need changing)
# ══════════════════════════════════════════════════════════════════════
#  Append a dict to PROBLEMS above with:
#
#   "id"             : unique snake_case string e.g. "div_my_problem"
#   "title"          : sidebar display title
#   "description"    : HTML description shown above the editor
#   "question"       : one-line question prompt
#   "type"           : DIVISION | JOIN | AGGREGATION | SET_OP | SUBQUERY | NULL
#   "difficulty"     : easy | medium | hard
#   "concepts"       : list of SQL concept strings
#   "base_query"     : the correct reference SQL (multiline string)
#   "hint"           : tip for students
#   "common_mistakes": list of common error strings
#
#  Save the file and restart the server — done!
# ══════════════════════════════════════════════════════════════════════


def get_problem(problem_id: str):
    return next((p for p in PROBLEMS if p["id"] == problem_id), None)


def get_all_problems():
    return [{
        "id": p["id"],
        "title": p["title"],
        "type": p["type"],
        "difficulty": p["difficulty"],
        "concepts": p["concepts"],
    } for p in PROBLEMS]

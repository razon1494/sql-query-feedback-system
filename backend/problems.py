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
#   "type"           : DIVISION | JOIN | AGGREGATION | SET_OP | SUBQUERY
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

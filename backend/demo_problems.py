"""
The running example of the ICWL 2026 paper (Fig. 1) and talk (slides 2, 3, 6, 7).

Kept apart from PROBLEMS on purpose: the evaluation scripts iterate PROBLEMS,
so adding these here leaves every reported number unchanged. The web API lists
them first. Each one names its own small database (see database/demo_dbs.py).
"""

# The reference of paper Fig. 1, written against this site's schema
# ("Group" = 'DB' where the paper writes Dept = 'DB').
RUNNING_EXAMPLE_REFERENCE = """SELECT s.StuID, s.Name
FROM Students s
WHERE NOT EXISTS (
    SELECT 1 FROM Courses c
    WHERE c."Group" = 'DB'
    AND NOT EXISTS (
        SELECT 1 FROM Takes t
        WHERE t.StuID = s.StuID AND t.CourseID = c.CourseID
    )
);"""

_SHARED = {
    "type": "DIVISION",
    "difficulty": "hard",
    "concepts": ["Relational Division", "Universal Quantification", "NOT EXISTS"],
    "base_query": RUNNING_EXAMPLE_REFERENCE,
    "hint": (
        "A student qualifies only if no database course is missing "
        "from that student's enrolments."
    ),
    "common_mistakes": ["Using IN instead of NOT EXISTS"],
}

DEMO_PROBLEMS = [
    {
        **_SHARED,
        "id": "ex_every_db_course",
        "title": "Running example: who completed every database course?",
        "description": (
            "Three students and two database courses (DB1, DB2). List the students who have "
            "completed <strong>every</strong> database course."
        ),
        "question": "Which students completed every database course?",
        "database": "demo_running_example",
    },
    {
        **_SHARED,
        "id": "ex_hidden_error",
        "title": "Running example: when the data hides the error",
        "description": (
            "The same task on different data: here nobody has completed only part of the "
            "database courses. List the students who have completed <strong>every</strong> "
            "database course."
        ),
        "question": "Which students completed every database course?",
        "database": "demo_hidden_error",
    },
]

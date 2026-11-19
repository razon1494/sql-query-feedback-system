"""
Sample submissions for each problem, served by the API instead of being
embedded in the page, so that a student cannot read a correct answer in the
page source.

Kinds:
  wrong, partial   incorrect submissions; shown to everyone
  correct          identical in meaning to the reference; instructor only
  alternate        correct by a different route (counting); instructor only
"""
from backend.demo_problems import RUNNING_EXAMPLE_REFERENCE

STUDENT_SAFE_KINDS = ("wrong", "partial")

# Submission A of paper Fig. 1: IN where universal quantification is needed.
_SUBMISSION_A = """SELECT s.StuID, s.Name
FROM Students s
WHERE s.StuID IN (
    SELECT t.StuID FROM Takes t
    WHERE t.CourseID IN (
        SELECT c.CourseID FROM Courses c WHERE c."Group" = 'DB'
    )
);"""
# Submission B of paper Fig. 1: correct by counting rather than negating.
_SUBMISSION_B = """SELECT s.StuID, s.Name
FROM Students s
JOIN Takes t ON s.StuID = t.StuID
JOIN Courses c ON t.CourseID = c.CourseID
WHERE c."Group" = 'DB'
GROUP BY s.StuID, s.Name
HAVING COUNT(DISTINCT c.CourseID) = (
    SELECT COUNT(*) FROM Courses WHERE "Group" = 'DB'
);"""
_RUNNING_EXAMPLE = {
    "wrong": _SUBMISSION_A,
    "correct": RUNNING_EXAMPLE_REFERENCE,
    "alternate": _SUBMISSION_B,
}

SAMPLE_ANSWERS = {
    "ex_every_db_course": _RUNNING_EXAMPLE,
    "ex_hidden_error": _RUNNING_EXAMPLE,
    **{'div_db_courses': {'wrong': 'SELECT s.StuID, s.Name\n'
                             'FROM Students s\n'
                             'WHERE s.StuID IN (\n'
                             '    SELECT t.StuID FROM Takes t\n'
                             '    WHERE t.CourseID IN (\n'
                             '        SELECT c.CourseID FROM Courses c WHERE c."Group" = \'DB\'\n'
                             '    )\n'
                             ');',
                    'correct': 'SELECT s.StuID, s.Name\n'
                               'FROM Students s\n'
                               'WHERE NOT EXISTS (\n'
                               '    SELECT c.CourseID FROM Courses c\n'
                               '    WHERE c."Group" = \'DB\'\n'
                               '    AND c.CourseID NOT IN (\n'
                               '        SELECT t.CourseID FROM Takes t WHERE t.StuID = s.StuID\n'
                               '    )\n'
                               ');',
                    'partial': 'SELECT s.StuID, s.Name\n'
                               'FROM Students s\n'
                               'JOIN Takes t ON s.StuID = t.StuID\n'
                               'JOIN Courses c ON t.CourseID = c.CourseID\n'
                               'WHERE c."Group" = \'DB\'\n'
                               'GROUP BY s.StuID, s.Name\n'
                               'HAVING COUNT(DISTINCT t.CourseID) >= 2;',
                    'alternate': 'SELECT s.StuID, s.Name\n'
                                 'FROM Students s\n'
                                 'JOIN Takes t ON s.StuID = t.StuID\n'
                                 'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                 'WHERE c."Group" = \'DB\'\n'
                                 'GROUP BY s.StuID, s.Name\n'
                                 'HAVING COUNT(DISTINCT c.CourseID) = (\n'
                                 '    SELECT COUNT(*) FROM Courses WHERE "Group" = \'DB\'\n'
                                 ');'},
 'div_all_cs_courses': {'wrong': 'SELECT s.StuID, s.Name FROM Students s\n'
                                 "WHERE s.Major = 'CS'\n"
                                 'AND s.StuID IN (\n'
                                 '    SELECT t.StuID FROM Takes t\n'
                                 '    JOIN Courses c ON t.CourseID = c.CourseID\n'
                                 '    WHERE c."Group" = \'CS\'\n'
                                 ');',
                        'correct': 'SELECT s.StuID, s.Name\n'
                                   'FROM Students s\n'
                                   "WHERE s.Major = 'CS'\n"
                                   'AND NOT EXISTS (\n'
                                   '    SELECT c.CourseID FROM Courses c\n'
                                   '    WHERE c."Group" = \'CS\'\n'
                                   '    AND c.CourseID NOT IN (\n'
                                   '        SELECT t.CourseID FROM Takes t WHERE t.StuID = s.StuID\n'
                                   '    )\n'
                                   ');',
                        'partial': "SELECT s.StuID, s.Name FROM Students s WHERE s.Major = 'CS';"},
 'div_instructor_all_db': {'wrong': 'SELECT i.InstID, i.Name FROM Instructors i\n'
                                    'WHERE i.InstID IN (\n'
                                    '    SELECT tc.InstID FROM Teaches tc\n'
                                    '    WHERE tc.CourseID IN (\n'
                                    '        SELECT c.CourseID FROM Courses c WHERE c."Group" = \'DB\'\n'
                                    '    )\n'
                                    ');',
                           'correct': 'SELECT i.InstID, i.Name\n'
                                      'FROM Instructors i\n'
                                      'WHERE NOT EXISTS (\n'
                                      '    SELECT c.CourseID FROM Courses c\n'
                                      '    WHERE c."Group" = \'DB\'\n'
                                      '    AND c.CourseID NOT IN (\n'
                                      '        SELECT tc.CourseID FROM Teaches tc WHERE tc.InstID = '
                                      'i.InstID\n'
                                      '    )\n'
                                      ');',
                           'partial': 'SELECT i.InstID, i.Name FROM Instructors i\n'
                                      'JOIN Teaches tc ON i.InstID = tc.InstID\n'
                                      'JOIN Courses c ON tc.CourseID = c.CourseID\n'
                                      'WHERE c."Group" = \'DB\';'},
 'join_cs360': {'wrong': "SELECT StuID, Name FROM Students WHERE StuID = 'S1';",
                'correct': 'SELECT s.StuID, s.Name\n'
                           'FROM Students s\n'
                           'JOIN Takes t ON s.StuID = t.StuID\n'
                           "WHERE t.CourseID = 'CS360';",
                'partial': "SELECT StuID FROM Takes WHERE CourseID = 'CS360';"},
 'join_instructor_courses': {'wrong': 'SELECT i.Name, c.Title FROM Instructors i, Courses c;',
                             'correct': 'SELECT i.Name AS InstructorName, c.Title AS CourseTitle\n'
                                        'FROM Instructors i\n'
                                        'JOIN Teaches tc ON i.InstID = tc.InstID\n'
                                        'JOIN Courses c ON tc.CourseID = c.CourseID\n'
                                        'ORDER BY i.Name;',
                             'partial': 'SELECT i.Name, tc.CourseID FROM Instructors i\n'
                                        'JOIN Teaches tc ON i.InstID = tc.InstID;'},
 'join_student_grades': {'wrong': 'SELECT s.Name, t.Grade FROM Students s, Takes t;',
                         'correct': 'SELECT s.Name AS StudentName, c.Title AS CourseTitle, t.Grade\n'
                                    'FROM Students s\n'
                                    'JOIN Takes t ON s.StuID = t.StuID\n'
                                    'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                    'ORDER BY s.Name, c.Title;',
                         'partial': 'SELECT s.Name, t.CourseID, t.Grade\n'
                                    'FROM Students s\n'
                                    'JOIN Takes t ON s.StuID = t.StuID;'},
 'agg_multi_course': {'wrong': 'SELECT s.StuID, s.Name FROM Students s\n'
                               'JOIN Takes t ON s.StuID = t.StuID\n'
                               'WHERE t.CourseID IS NOT NULL;',
                      'correct': 'SELECT s.StuID, s.Name, COUNT(t.CourseID) AS CourseCount\n'
                                 'FROM Students s\n'
                                 'JOIN Takes t ON s.StuID = t.StuID\n'
                                 'GROUP BY s.StuID, s.Name\n'
                                 'HAVING COUNT(t.CourseID) > 1;',
                      'partial': 'SELECT s.StuID, s.Name\n'
                                 'FROM Students s\n'
                                 'JOIN Takes t ON s.StuID = t.StuID\n'
                                 'GROUP BY s.StuID, s.Name;'},
 'agg_avg_credits': {'wrong': 'SELECT s.StuID, s.Name, COUNT(t.CourseID) AS AvgCredits\n'
                              'FROM Students s\n'
                              'JOIN Takes t ON s.StuID = t.StuID\n'
                              'GROUP BY s.StuID, s.Name;',
                     'correct': 'SELECT s.StuID, s.Name, AVG(c.Credits) AS AvgCredits\n'
                                'FROM Students s\n'
                                'JOIN Takes t ON s.StuID = t.StuID\n'
                                'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                'GROUP BY s.StuID, s.Name;',
                     'partial': 'SELECT s.StuID, s.Name, SUM(c.Credits) AS AvgCredits\n'
                                'FROM Students s\n'
                                'JOIN Takes t ON s.StuID = t.StuID\n'
                                'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                'GROUP BY s.StuID, s.Name;'},
 'set_no_db': {'wrong': 'SELECT StuID, Name FROM Students\nWHERE StuID NOT IN (SELECT StuID FROM Takes);',
               'correct': 'SELECT StuID, Name FROM Students\n'
                          'EXCEPT\n'
                          'SELECT s.StuID, s.Name\n'
                          'FROM Students s\n'
                          'JOIN Takes t ON s.StuID = t.StuID\n'
                          'JOIN Courses c ON t.CourseID = c.CourseID\n'
                          'WHERE c."Group" = \'DB\';',
               'partial': 'SELECT s.StuID, s.Name FROM Students s\n'
                          'WHERE s.StuID NOT IN (\n'
                          '    SELECT t.StuID FROM Takes t\n'
                          '    JOIN Courses c ON t.CourseID = c.CourseID\n'
                          '    WHERE c."Group" = \'DB\'\n'
                          ');'},
 'set_union_cs_db': {'wrong': 'SELECT DISTINCT s.StuID, s.Name FROM Students s\n'
                              'JOIN Takes t ON s.StuID = t.StuID\n'
                              'JOIN Courses c ON t.CourseID = c.CourseID\n'
                              'WHERE c."Group" IN (\'CS\',\'DB\');',
                     'correct': 'SELECT DISTINCT s.StuID, s.Name\n'
                                'FROM Students s\n'
                                'JOIN Takes t ON s.StuID = t.StuID\n'
                                'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                'WHERE c."Group" = \'CS\'\n'
                                'UNION\n'
                                'SELECT DISTINCT s.StuID, s.Name\n'
                                'FROM Students s\n'
                                'JOIN Takes t ON s.StuID = t.StuID\n'
                                'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                'WHERE c."Group" = \'DB\';',
                     'partial': 'SELECT DISTINCT s.StuID, s.Name FROM Students s\n'
                                'JOIN Takes t ON s.StuID = t.StuID\n'
                                'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                'WHERE c."Group" = \'CS\'\n'
                                'UNION ALL\n'
                                'SELECT DISTINCT s.StuID, s.Name FROM Students s\n'
                                'JOIN Takes t ON s.StuID = t.StuID\n'
                                'JOIN Courses c ON t.CourseID = c.CourseID\n'
                                'WHERE c."Group" = \'DB\';'}}}


def samples_for(problem_id: str, is_instructor: bool) -> dict:
    """The sample submissions this viewer may load (a new dict)."""
    samples = SAMPLE_ANSWERS.get(problem_id, {})
    if is_instructor:
        return dict(samples)
    return {k: v for k, v in samples.items() if k in STUDENT_SAFE_KINDS}

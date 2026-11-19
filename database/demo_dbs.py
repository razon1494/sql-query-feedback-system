"""
Small databases for the running example (paper Fig. 1, talk slides 2-3).

  demo_running_example  Alice took DB1 and DB2, Nancy took DB1, Omar took DB2.
  demo_hidden_error     Alice took DB1 and DB2; Nancy and Omar took no
                        database course (each took one programming course,
                        PL1), so nobody completed only part of the sequence
                        and the IN query returns only Alice. The PL1
                        enrolments also hide the NOT-IN-in-NOT-IN error of
                        talk slide 11: on this data it returns only Alice
                        too, and only the NULL-injected copy exposes it.

The schema matches the edge-case databases, so the same five edge instances
apply to both problems.
"""
import os
import sqlite3

DB_DIR = os.path.dirname(os.path.abspath(__file__))
DEMO_DB_PATH = os.path.join(DB_DIR, "{}.db")

_SCHEMA = """
    CREATE TABLE Departments (DeptID TEXT PRIMARY KEY, Name TEXT, Building TEXT);
    CREATE TABLE Students    (StuID TEXT PRIMARY KEY, Name TEXT NOT NULL, Age INTEGER, Major TEXT);
    CREATE TABLE Courses     (CourseID TEXT PRIMARY KEY, Title TEXT NOT NULL, Credits INTEGER,
                              "Group" TEXT, DeptID TEXT);
    CREATE TABLE Takes       (StuID TEXT, CourseID TEXT, Grade TEXT, PRIMARY KEY (StuID, CourseID));
"""

_DEPARTMENTS = [("D1", "Computer Science", "Brink Hall")]
_STUDENTS = [
    ("S1", "Alice", 20, "CS"),
    ("S2", "Nancy", 21, "CS"),
    ("S3", "Omar", 20, "CS"),
]
_DB_COURSES = [
    ("DB1", "Intro to Databases", 3, "DB", "D1"),
    ("DB2", "Advanced Databases", 3, "DB", "D1"),
]
_PL_COURSE = ("PL1", "Programming Basics", 3, "PL", "D1")

DEMO_DATABASES = {
    "demo_running_example": {
        "courses": _DB_COURSES,
        "takes": [
            ("S1", "DB1", "A"),
            ("S1", "DB2", "A-"),
            ("S2", "DB1", "B+"),
            ("S3", "DB2", "B"),
        ],
    },
    "demo_hidden_error": {
        "courses": [*_DB_COURSES, _PL_COURSE],
        "takes": [
            ("S1", "DB1", "A"),
            ("S1", "DB2", "A-"),
            ("S2", "PL1", "B"),
            ("S3", "PL1", "B+"),
        ],
    },
}


def demo_db_path(name: str) -> str:
    """Path of a demo database; raises KeyError for an unknown name."""
    if name not in DEMO_DATABASES:
        raise KeyError(f"Unknown demo database: {name}")
    return DEMO_DB_PATH.format(name)


def _build(path: str, data: dict) -> None:
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_SCHEMA)
        conn.executemany("INSERT INTO Departments VALUES (?,?,?)", _DEPARTMENTS)
        conn.executemany("INSERT INTO Students VALUES (?,?,?,?)", _STUDENTS)
        conn.executemany("INSERT INTO Courses VALUES (?,?,?,?,?)", data["courses"])
        conn.executemany("INSERT INTO Takes VALUES (?,?,?)", data["takes"])
        conn.commit()
    finally:
        conn.close()


def init_demo_dbs() -> list:
    """(Re)create every demo database. Returns their names."""
    for name, data in DEMO_DATABASES.items():
        _build(demo_db_path(name), data)
        print(f"[DB] Demo database '{name}' ready")
    return list(DEMO_DATABASES)


if __name__ == "__main__":
    init_demo_dbs()

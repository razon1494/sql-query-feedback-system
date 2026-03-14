"""
Database Initialization for SQL Feedback System
Creates SQLite databases: main sample DB + edge case test databases
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "main.db")
EDGE_DB_PATH = os.path.join(os.path.dirname(__file__), "edge_{}.db")


def get_connection(db_path=None):
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_main_db():
    """Create and populate the main teaching database. Always recreates cleanly."""
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = OFF")
    c = conn.cursor()
    c.executescript("""
        DROP TABLE IF EXISTS Takes;
        DROP TABLE IF EXISTS Courses;
        DROP TABLE IF EXISTS Students;
        DROP TABLE IF EXISTS Instructors;
        DROP TABLE IF EXISTS Teaches;

        CREATE TABLE Students (
            StuID   TEXT PRIMARY KEY,
            Name    TEXT NOT NULL,
            Age     INTEGER,
            Major   TEXT
        );

        CREATE TABLE Courses (
            CourseID TEXT PRIMARY KEY,
            Title    TEXT NOT NULL,
            Credits  INTEGER,
            "Group"  TEXT
        );

        CREATE TABLE Takes (
            StuID    TEXT REFERENCES Students(StuID),
            CourseID TEXT REFERENCES Courses(CourseID),
            Grade    TEXT,
            PRIMARY KEY (StuID, CourseID)
        );

        CREATE TABLE Instructors (
            InstID TEXT PRIMARY KEY,
            Name   TEXT NOT NULL,
            Dept   TEXT
        );

        CREATE TABLE Teaches (
            InstID   TEXT REFERENCES Instructors(InstID),
            CourseID TEXT REFERENCES Courses(CourseID),
            PRIMARY KEY (InstID, CourseID)
        );
    """)

    # Students
    c.executemany("INSERT INTO Students VALUES (?,?,?,?)", [
        ("S1", "Alice",   18, "CS"),
        ("S2", "Nancy",   19, "CS"),
        ("S3", "Peter",   19, "Math"),
        ("S4", "Diana",   20, "CS"),
        ("S5", "Edward",  21, "Physics"),
    ])

    # Courses
    c.executemany('INSERT INTO Courses VALUES (?,?,?,?)', [
        ("CS360", "Intro DB",      3, "DB"),
        ("CS460", "Adv. DB",       3, "DB"),
        ("CS120", "Python Prog",   3, "PL"),
        ("CS220", "Data Struct",   3, "CS"),
        ("CS480", "ML Basics",     3, "AI"),
    ])

    # Takes
    c.executemany("INSERT INTO Takes VALUES (?,?,?)", [
        ("S1", "CS360", "A"),
        ("S1", "CS460", "A-"),
        ("S2", "CS360", "B+"),
        ("S3", "CS120", "A"),
        ("S4", "CS360", "B"),
        ("S4", "CS460", "B+"),
        ("S4", "CS120", "A"),
        ("S5", "CS480", "A"),
    ])

    # Instructors
    c.executemany("INSERT INTO Instructors VALUES (?,?,?)", [
        ("I1", "Dr. Smith",  "CS"),
        ("I2", "Dr. Jones",  "CS"),
        ("I3", "Dr. Brown",  "Math"),
    ])

    # Teaches
    c.executemany("INSERT INTO Teaches VALUES (?,?)", [
        ("I1", "CS360"),
        ("I1", "CS460"),
        ("I2", "CS120"),
        ("I2", "CS220"),
        ("I3", "CS480"),
    ])

    conn.commit()
    conn.close()
    print(f"[DB] Main database initialized at {DB_PATH}")


def init_edge_dbs():
    """Create specialized edge-case databases for testing."""
    edge_cases = {
        "empty_courses": {
            "desc": "No DB courses exist in Courses table",
            "courses": [
                ("CS120", "Python Prog", 3, "PL"),
                ("CS220", "Data Struct", 3, "CS"),
            ],
            "students": [
                ("S1", "Alice", 18, "CS"),
                ("S2", "Nancy", 19, "CS"),
            ],
            "takes": [
                ("S1", "CS120", "A"),
            ]
        },
        "partial_match": {
            "desc": "Student takes only some DB courses (partial match)",
            "courses": [
                ("CS360", "Intro DB", 3, "DB"),
                ("CS460", "Adv. DB",  3, "DB"),
                ("CS120", "Python",   3, "PL"),
            ],
            "students": [
                ("S1", "Alice",  18, "CS"),
                ("S2", "Nancy",  19, "CS"),
            ],
            "takes": [
                ("S1", "CS360", "A"),
                ("S1", "CS460", "A"),
                ("S2", "CS360", "B"),   # Nancy only took CS360, not CS460
            ]
        },
        "all_enrolled": {
            "desc": "All students enrolled in all DB courses",
            "courses": [
                ("CS360", "Intro DB", 3, "DB"),
                ("CS460", "Adv. DB",  3, "DB"),
            ],
            "students": [
                ("S1", "Alice", 18, "CS"),
                ("S2", "Nancy", 19, "CS"),
            ],
            "takes": [
                ("S1", "CS360", "A"),
                ("S1", "CS460", "A"),
                ("S2", "CS360", "A"),
                ("S2", "CS460", "A"),
            ]
        },
        "single_course": {
            "desc": "Only one DB course exists",
            "courses": [
                ("CS360", "Intro DB", 3, "DB"),
                ("CS120", "Python",   3, "PL"),
            ],
            "students": [
                ("S1", "Alice", 18, "CS"),
                ("S2", "Nancy", 19, "CS"),
                ("S3", "Peter", 19, "Math"),
            ],
            "takes": [
                ("S1", "CS360", "A"),
                ("S2", "CS120", "B"),
            ]
        },
        "no_students": {
            "desc": "Students table is empty",
            "courses": [
                ("CS360", "Intro DB", 3, "DB"),
            ],
            "students": [],
            "takes": []
        },
    }

    for name, config in edge_cases.items():
        path = EDGE_DB_PATH.format(name)
        if os.path.exists(path):
            os.remove(path)
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE Students (StuID TEXT PRIMARY KEY, Name TEXT, Age INTEGER, Major TEXT);
            CREATE TABLE Courses  (CourseID TEXT PRIMARY KEY, Title TEXT, Credits INTEGER, "Group" TEXT);
            CREATE TABLE Takes    (StuID TEXT, CourseID TEXT, Grade TEXT, PRIMARY KEY(StuID,CourseID));
        """)
        c.executemany("INSERT INTO Students VALUES (?,?,?,?)", config["students"])
        c.executemany('INSERT INTO Courses VALUES (?,?,?,?)', config["courses"])
        c.executemany("INSERT INTO Takes VALUES (?,?,?)", config["takes"])
        conn.commit()
        conn.close()
        print(f"[DB] Edge case '{name}': {config['desc']}")

    return list(edge_cases.keys())


if __name__ == "__main__":
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_main_db()
    init_edge_dbs()
    print("[DB] All databases ready.")

"""
Build an enriched test database for Phase 4 / Step 2 evaluation.

The main DB in `database/main.db` is intentionally small (5 students) and
happens to lack 4-credit courses, NULL grades, and NULL majors. Many Step 2
wrong queries therefore produce the same result as the reference on this
DB, triggering the result-aware filter (a correct behaviour) but reducing
the Step 2 detection-rate sample size.

This module produces `database/step2_enriched.db` with:
  - More students (10), including some with NULL Major
  - Courses that include 4-credit entries
  - Takes entries with NULL Grade
  - More DB-group and CS-group spread so division/intersect queries diverge

Usage
-----
    python3 build_step2_db.py          # rebuild the DB
"""
import os, sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "database", "step2_enriched.db")


def build():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
        CREATE TABLE Departments (
            DeptID TEXT PRIMARY KEY, Name TEXT NOT NULL, Building TEXT
        );
        CREATE TABLE Students (
            StuID TEXT PRIMARY KEY, Name TEXT NOT NULL, Age INTEGER, Major TEXT
        );
        CREATE TABLE Courses (
            CourseID TEXT PRIMARY KEY, Title TEXT NOT NULL, Credits INTEGER,
            "Group" TEXT, DeptID TEXT REFERENCES Departments(DeptID)
        );
        CREATE TABLE Takes (
            StuID TEXT, CourseID TEXT, Grade TEXT,
            PRIMARY KEY (StuID, CourseID)
        );
        CREATE TABLE Instructors (
            InstID TEXT PRIMARY KEY, Name TEXT NOT NULL, Dept TEXT,
            DeptID TEXT REFERENCES Departments(DeptID)
        );
        CREATE TABLE Teaches (
            InstID TEXT, CourseID TEXT, PRIMARY KEY (InstID, CourseID)
        );
        CREATE TABLE Prerequisites (
            CourseID TEXT, PrereqID TEXT, PRIMARY KEY (CourseID, PrereqID)
        );
    """)

    # Departments
    c.executemany("INSERT INTO Departments VALUES (?,?,?)", [
        ("D1", "Computer Science",  "Brink Hall"),
        ("D2", "Mathematics",       "Brink Hall"),
        ("D3", "Physics",           "Engineering Phys."),
    ])

    # 10 students; two have NULL Major.
    c.executemany("INSERT INTO Students VALUES (?,?,?,?)", [
        ("S1",  "Alice",   18, "CS"),
        ("S2",  "Nancy",   19, "CS"),
        ("S3",  "Peter",   19, "Math"),
        ("S4",  "Diana",   20, "CS"),
        ("S5",  "Edward",  21, "Physics"),
        ("S6",  "Fatima",  20, "CS"),
        ("S7",  "George",  22, "Math"),
        ("S8",  "Hana",    19, None),       # NULL Major
        ("S9",  "Ibrahim", 18, None),       # NULL Major
        ("S10", "Julia",   23, "Math"),
    ])

    # Courses: mix of Credits including 4, DeptID FK added.
    c.executemany("INSERT INTO Courses VALUES (?,?,?,?,?)", [
        ("CS360", "Intro DB",       3, "DB",   "D1"),
        ("CS460", "Adv. DB",        3, "DB",   "D1"),
        ("CS120", "Python Prog",    3, "PL",   "D1"),
        ("CS220", "Data Struct",    3, "CS",   "D1"),
        ("CS480", "ML Basics",      4, "AI",   "D1"),    # 4-credit
        ("CS510", "Distributed DB", 4, "DB",   "D1"),    # 4-credit, DB
        ("MA210", "Linear Algebra", 4, "Math", "D2"),    # 4-credit, Math
        ("MA310", "Real Analysis",  3, "Math", "D2"),
    ])

    # Takes — same as before.
    c.executemany("INSERT INTO Takes VALUES (?,?,?)", [
        ("S1",  "CS360", "A"),
        ("S1",  "CS460", "A-"),
        ("S1",  "CS510", "B"),
        ("S2",  "CS360", "B+"),
        ("S3",  "CS120", "A"),
        ("S3",  "MA210", "A"),
        ("S3",  "MA310", "A-"),
        ("S4",  "CS360", "B"),
        ("S4",  "CS460", "B+"),
        ("S4",  "CS120", "A"),
        ("S4",  "CS510", "B+"),
        ("S5",  "CS480", "A"),
        ("S6",  "CS220", "A"),
        ("S6",  "CS480", None),
        ("S7",  "MA210", "A"),
        ("S7",  "MA310", "B"),
        ("S8",  "CS120", None),
        ("S10", "MA210", "A"),
        ("S10", "MA310", "A"),
    ])

    # Instructors — DeptID FK added.
    c.executemany("INSERT INTO Instructors VALUES (?,?,?,?)", [
        ("I1", "Dr. Smith",  "CS",   "D1"),
        ("I2", "Dr. Jones",  "CS",   "D1"),
        ("I3", "Dr. Brown",  "Math", "D2"),
        ("I4", "Dr. Khan",   "CS",   "D1"),
    ])

    c.executemany("INSERT INTO Teaches VALUES (?,?)", [
        ("I1", "CS360"),
        ("I1", "CS460"),
        ("I1", "CS510"),
        ("I2", "CS120"),
        ("I2", "CS220"),
        ("I2", "CS480"),
        ("I3", "MA210"),
        ("I3", "MA310"),
        ("I4", "CS220"),
    ])

    # Prerequisites — same chain as the main DB, extended for 4-credit courses.
    c.executemany("INSERT INTO Prerequisites VALUES (?,?)", [
        ("CS460", "CS360"),
        ("CS220", "CS120"),
        ("CS480", "CS220"),
        ("CS510", "CS460"),   # Distributed DB requires Adv. DB
        ("MA310", "MA210"),   # Real Analysis requires Linear Algebra
    ])

    conn.commit()
    conn.close()
    print(f"[DB] Enriched test database written to {DB_PATH}")


if __name__ == "__main__":
    build()

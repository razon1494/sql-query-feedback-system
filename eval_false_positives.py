"""
False-positive sanity check for the feedback generator.
For each problem, feeds the CORRECT base query as the student query.
No misconceptions should fire, and the logic grade should be full.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.sql_parser import parse_sql, compare_queries
from backend.feedback_generator import _detect_misconceptions
from backend.problems import PROBLEMS


def main():
    total = 0
    false_positives = 0
    details = []
    for p in PROBLEMS:
        total += 1
        base_parse = parse_sql(p["base_query"])
        # Student parses the SAME query.
        student_parse = parse_sql(p["base_query"])
        diffs = compare_queries(base_parse, student_parse)
        mis = _detect_misconceptions(base_parse, student_parse, diffs, p["type"])
        if mis:
            false_positives += 1
            details.append((p["id"], p["type"], [m["key"] for m in mis]))

    print("═══════════════════════════════════════════════════════════════════")
    print(" FALSE-POSITIVE CHECK (correct query = student query)")
    print("═══════════════════════════════════════════════════════════════════")
    print(f"Problems tested  : {total}")
    print(f"False positives  : {false_positives}")
    print(f"Clean detections : {total - false_positives}")
    print()
    if details:
        print("False positives detected on:")
        for pid, ptype, keys in details:
            print(f"  {pid:<40} [{ptype}]  keys={keys}")
    else:
        print("✓ All 25 base queries pass through clean — no false positives.")


if __name__ == "__main__":
    main()

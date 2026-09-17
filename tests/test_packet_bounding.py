"""The LLM packet is a bounded view of the bundle, not a different bundle.

Draft, Sec. 8: "The packet contains stable identifiers and bounded excerpts."
Two invariants follow. Identifiers must survive bounding, or a hypothesis that
cites `cex.empty_courses` cannot be verified against the bundle it came from.
And bounding is a property of the packet alone -- the bundle keeps everything,
because the verifier reads the bundle, not the packet.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import copy

import pytest

from backend.evidence import (
    MAX_ROWS_IN_PACKET, MAX_STRING_LEN, MAX_QUERY_LEN,
    EvidenceBundle, EvidenceItem, _bound,
)


def rows(n):
    return [{"StuID": i, "Name": f"student {i}"} for i in range(n)]


def bundle_with(item):
    return EvidenceBundle(task={"id": "t", "student_query": "SELECT 1",
                                "reference_query": "SELECT 1"},
                          E_exe=[item])


# ── strings ────────────────────────────────────────────────────────────

def test_a_string_at_the_limit_is_untouched():
    s = "x" * MAX_STRING_LEN
    assert _bound(s) == s


def test_a_string_over_the_limit_is_cut_and_says_so():
    s = "x" * (MAX_STRING_LEN + 1)
    out = _bound(s)
    assert out.startswith("x" * MAX_STRING_LEN)
    assert f"[{MAX_STRING_LEN + 1} chars total]" in out


# ── lists ──────────────────────────────────────────────────────────────

def test_a_list_at_the_limit_is_untouched():
    r = rows(MAX_ROWS_IN_PACKET)
    assert _bound(r) == r


def test_a_list_over_the_limit_keeps_the_head_and_counts_the_rest():
    r = rows(MAX_ROWS_IN_PACKET + 5)
    out = _bound(r)
    assert out[:MAX_ROWS_IN_PACKET] == r[:MAX_ROWS_IN_PACKET]
    assert out[-1] == f"... [5 more of {MAX_ROWS_IN_PACKET + 5}]"


def test_bounding_reaches_into_nested_structures():
    out = _bound({"result": {"rows": rows(50), "note": "y" * 500}})
    assert len(out["result"]["rows"]) == MAX_ROWS_IN_PACKET + 1
    assert "chars total" in out["result"]["note"]


# ── immutability ───────────────────────────────────────────────────────

def test_bounding_does_not_mutate_its_input():
    original = {"rows": rows(50), "note": "y" * 500}
    before = copy.deepcopy(original)
    _bound(original)
    assert original == before


def test_the_bundle_itself_keeps_everything():
    item = EvidenceItem("exe.student", "result", {"rows": rows(50)})
    b = bundle_with(item)
    assert len(b.to_packet()["E_exe"][0]["content"]["rows"]) == MAX_ROWS_IN_PACKET + 1
    assert len(b.to_dict()["E_exe"][0]["content"]["rows"]) == 50


# ── identifiers ────────────────────────────────────────────────────────

def test_every_id_in_the_packet_resolves_in_the_bundle():
    """The citation round-trip Stage 3 depends on."""
    b = EvidenceBundle(
        task={"id": "t"},
        E_exe=[EvidenceItem("exe.student", "result", {"rows": rows(99)})],
        E_cex=[EvidenceItem("cex.empty_courses", "constructed_instance",
                            {"student_rows": rows(99), "description": "d" * 900})],
    )
    packet = b.to_packet()
    for channel in b.CHANNELS:
        for entry in packet[channel]:
            assert b.resolve(entry["id"]) is not None, entry["id"]
            assert b.channel_of(entry["id"]) == channel


def test_a_long_id_is_never_truncated():
    long_id = "cex." + "n" * (MAX_STRING_LEN + 50)
    b = bundle_with(EvidenceItem(long_id, "constructed_instance", {}))
    assert b.to_packet()["E_exe"][0]["id"] == long_id


def test_no_channel_is_dropped_however_many_items_it_holds():
    """Only content inside an item is bounded; the channel list is not."""
    many = [EvidenceItem(f"exe.{i}", "result", {}) for i in range(MAX_ROWS_IN_PACKET + 7)]
    packet = EvidenceBundle(task={}, E_exe=many).to_packet()
    assert len(packet["E_exe"]) == len(many)
    assert [e["id"] for e in packet["E_exe"]] == [i.id for i in many]


# ── the query under diagnosis is the subject, not an excerpt ───────────

def test_the_student_query_is_not_truncated_at_the_excerpt_limit():
    """A hypothesis about a clause the model never saw cannot be verified."""
    sql = "SELECT s.StuID FROM Students s WHERE " + " OR ".join(
        f"s.StuID = {i}" for i in range(200))
    assert len(sql) > MAX_STRING_LEN
    packet = EvidenceBundle(task={"student_query": sql, "reference_query": sql}).to_packet()
    assert packet["task"]["student_query"] == sql
    assert packet["task"]["reference_query"] == sql


def test_a_pathological_query_is_still_capped():
    sql = "SELECT 1 -- " + "z" * (MAX_QUERY_LEN + 100)
    packet = EvidenceBundle(task={"student_query": sql}).to_packet()
    out = packet["task"]["student_query"]
    assert len(out) < len(sql)
    assert f"[{len(sql)} chars total]" in out


def test_non_query_task_fields_still_use_the_excerpt_limit():
    packet = EvidenceBundle(task={"question": "q" * (MAX_STRING_LEN + 1)}).to_packet()
    assert "chars total" in packet["task"]["question"]

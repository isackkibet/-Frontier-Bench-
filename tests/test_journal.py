"""Unit tests for the hash-chained audit journal.

Verifies the tamper-evident integrity properties the sealed grader relies on:
strictly increasing sequence, a correct SHA-256 hash chain, and append-only
behaviour. All tests run against a throwaway file in a temp directory.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "solution", "scheduler"))

import constants  # noqa: E402
import journal  # noqa: E402


def _make_journal():
    tmp = tempfile.mkdtemp()
    constants.JOURNAL_PATH = os.path.join(tmp, "journal.jsonl")
    constants.DATA_DIR = tmp
    return journal.Journal(constants.JOURNAL_PATH)


def _read_records(path):
    with open(path, encoding="utf-8") as f:
        raw = [ln for ln in f.read().split("\n") if ln.strip()]
    return [json.loads(ln) for ln in raw]


def test_append_is_sequential_and_hashed():
    j = _make_journal().open()
    events = [
        {"kind": "scheduler_start", "instance_id": "i1"},
        {"kind": "submitted", "job": "A"},
        {"kind": "job_done", "job": "A"},
    ]
    for e in events:
        j.append(e)
    j.close()

    recs = _read_records(constants.JOURNAL_PATH)
    assert [r["seq"] for r in recs] == [1, 2, 3]
    assert recs[0]["prev_hash"] is None
    for i in range(1, len(recs)):
        assert recs[i]["prev_hash"] == recs[i - 1]["hash"]


def test_hash_chain_detects_tampering():
    j = _make_journal().open()
    j.append({"kind": "submitted", "job": "A"})
    j.append({"kind": "job_done", "job": "A"})
    j.close()

    path = constants.JOURNAL_PATH
    recs = _read_records(path)
    # Mutate the recorded event of the first record, breaking the chain.
    recs[0]["event"]["job"] = "HACKED"
    with open(path, "w", encoding="utf-8") as f:
        for r in recs:
            f.write(json.dumps(r, sort_keys=True) + "\n")

    mutated = _read_records(path)
    # Recompute what the first record *should* hash to given the tampered
    # event and confirm it no longer matches the stored hash the second
    # record chains from.
    recomputed = journal.Journal._hash_of(
        mutated[0]["seq"], mutated[0]["prev_hash"], mutated[0]["event"]
    )
    assert recomputed != mutated[0]["hash"]
    assert mutated[1]["prev_hash"] == mutated[0]["hash"]


def test_open_reloads_from_existing_tail():
    j = _make_journal().open()
    j.append({"kind": "submitted", "job": "B"})
    j.close()

    # Re-open on the same path: next seq continues from the tail.
    j2 = journal.Journal(constants.JOURNAL_PATH).open()
    rec = j2.append({"kind": "job_done", "job": "B"})
    j2.close()

    assert rec["seq"] == 2
    recs = _read_records(constants.JOURNAL_PATH)
    assert recs[-1]["prev_hash"] == recs[0]["hash"]


def test_empty_journal_starts_at_one():
    j = _make_journal().open()
    rec = j.append({"kind": "scheduler_start"})
    j.close()
    assert rec["seq"] == 1

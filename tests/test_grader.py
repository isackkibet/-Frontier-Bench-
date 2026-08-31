"""Sealed verifier for the crash-resilient scheduler task.

Reads only the declared artifacts transferred into this container at their
original absolute paths:

    /logs/state.journal.jsonl   append-only hash-chained audit journal
    /logs/final_state/          snapshot of the scheduler state dir
    /logs/results.json          job status summary

and validates that the agent produced a *correct* crash-resilient scheduler
run. Every test returning a pass contributes to a reward of exactly 1 or 0.

The exact-once, dependency-ordering, priority, crash-recovery and restart-
persistence invariants below are what make hand-crafted (non-genuine)
helpers fail.
"""

import hashlib
import json
import os

import pytest

JOURNAL = "/logs/state.journal.jsonl"
RESULTS = "/logs/results.json"
PERSISTED = "/logs/persisted_state.json"

SCENARIO_JOBS = ["A", "B", "C", "D", "E", "P1", "P2"]


def _sha(seq, prev_hash, event):
    payload = f"{seq}|{prev_hash}|{json.dumps(event, sort_keys=True)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@pytest.fixture(scope="module")
def journal():
    with open(JOURNAL, encoding="utf-8") as f:
        raw = [ln for ln in f.read().split("\n") if ln.strip()]
    return [json.loads(ln) for ln in raw]


@pytest.fixture(scope="module")
def results():
    with open(RESULTS, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def persisted():
    with open(PERSISTED, encoding="utf-8") as f:
        return json.load(f)


def _job_events(journal, job, kind=None):
    out = []
    for rec in journal:
        ev = rec["event"]
        if ev.get("job") == job and (kind is None or ev.get("kind") == kind):
            out.append(rec)
    return out


# --------------------------------------------------------------------------
# 1. Artifact presence
# --------------------------------------------------------------------------
def test_artifacts_present():
    assert os.path.isfile(JOURNAL), "journal artifact missing"
    assert os.path.isfile(RESULTS), "results artifact missing"
    assert os.path.isfile(PERSISTED), "persisted_state artifact missing"
    assert os.path.getsize(JOURNAL) > 0, "journal is empty"
    assert os.path.getsize(RESULTS) > 0, "results is empty"


# --------------------------------------------------------------------------
# 2. Journal chain integrity
# --------------------------------------------------------------------------
def test_journal_chain_integrity(journal):
    prev = None
    seq_prev = 0
    ts_prev = 0
    for rec in journal:
        assert rec["seq"] == seq_prev + 1, "seq not strictly sequential"
        assert rec["prev_hash"] == prev, "prev_hash chain broken"
        expect = _sha(rec["seq"], rec["prev_hash"], rec["event"])
        assert rec["hash"] == expect, "hash mismatch (tampered record)"
        assert rec["ts_ms"] >= ts_prev, "timestamp went backwards"
        seq_prev = rec["seq"]
        ts_prev = rec["ts_ms"]
        prev = rec["hash"]


# --------------------------------------------------------------------------
# 3. All scenario jobs complete
# --------------------------------------------------------------------------
def test_all_jobs_done(results):
    for name in SCENARIO_JOBS:
        assert name in results["jobs"], f"job {name} missing from results"
        assert results["jobs"][name]["status"] == "DONE", \
            f"job {name} not DONE: {results['jobs'][name]['status']}"


# --------------------------------------------------------------------------
# 4. Exactly-once completion
# --------------------------------------------------------------------------
def test_exactly_once_done(journal):
    for job in SCENARIO_JOBS:
        done = _job_events(journal, job, "job_done")
        assert len(done) == 1, f"{job}: expected exactly one job_done, got {len(done)}"


def test_exactly_once_success(journal):
    for job in SCENARIO_JOBS:
        finished_ok = [
            r for r in _job_events(journal, job, "execution_finished")
            if r["event"].get("rc") == 0
        ]
        assert len(finished_ok) == 1, \
            f"{job}: expected exactly one successful execution, got {len(finished_ok)}"


# --------------------------------------------------------------------------
# 5. Worker crash recovery must have occurred for A
# --------------------------------------------------------------------------
def test_a_crash_recovered(journal):
    starts = _job_events(journal, "A", "execution_started")
    assert len(starts) >= 2, \
        "job A was not observed to crash and be re-attempted"
    ids = [r["event"]["execution_id"] for r in starts]
    assert len(set(ids)) == len(ids), "duplicate execution_id for A"
    leases = _job_events(journal, "A", "lease_expired")
    assert len(leases) >= 1, "job A never showed a lease-recovery event"


# --------------------------------------------------------------------------
# 6. Dependency ordering (B,C after A ; D after B,C)
# --------------------------------------------------------------------------
def test_dependency_ordering(journal):
    done_seq = {}
    for rec in journal:
        if rec["event"]["kind"] == "job_done":
            done_seq[rec["event"]["job"]] = rec["seq"]
    deps = {"B": ["A"], "C": ["A"], "D": ["B", "C"]}
    for job, depnames in deps.items():
        for dep in depnames:
            assert done_seq[job] > done_seq[dep], \
                f"{job} done before dependency {dep}"
    # execution of a dependent job must start after its deps are done
    for job, depnames in deps.items():
        start = _job_events(journal, job, "execution_started")[-1]["seq"]
        for dep in depnames:
            assert start > done_seq[dep], \
                f"{job} started executing before dependency {dep} finished"


# --------------------------------------------------------------------------
# 7. Priority ordering (P1 lower numeric priority, released first)
# --------------------------------------------------------------------------
def test_priority_order(journal):
    rel = {}
    for rec in journal:
        ev = rec["event"]
        if ev["kind"] == "release" and ev["job"] in ("P1", "P2"):
            rel[ev["job"]] = rec["seq"]
    assert "P1" in rel and "P2" in rel, "P1/P2 were never released"
    assert rel["P1"] < rel["P2"], \
        "priority violated: P1 (prio 1) must be released before P2 (prio 2)"


# --------------------------------------------------------------------------
# 8. Restart persistence
# --------------------------------------------------------------------------
def test_restart_persistence(journal):
    starts = [r for r in journal if r["event"]["kind"] == "scheduler_start"]
    assert len(starts) >= 2, \
        "coordinator must have started at least twice (restart test)"
    instances = [r["event"].get("instance") for r in starts]
    assert len(set(instances)) == len(instances), "duplicate scheduler instance id"


def test_state_survives_restart(results, persisted):
    # after restart A must still be recorded exactly once as DONE (not
    # re-run / duplicated / lost)
    a_rec = results["jobs"]["A"]
    assert a_rec["status"] == "DONE"
    # its independent persisted snapshot agrees
    a_disk = persisted["jobs"]["A"]
    assert a_disk["status"] == "DONE"


# --------------------------------------------------------------------------
# 9. No duplicate concurrent execution
# --------------------------------------------------------------------------
def test_no_duplicate_execution(journal):
    for job in SCENARIO_JOBS:
        starts = _job_events(journal, job, "execution_started")
        ids = [r["event"]["execution_id"] for r in starts]
        assert len(set(ids)) == len(ids), f"{job}: re-used an execution_id"
        finished = _job_events(journal, job, "execution_finished")
        fid = [r["event"]["execution_id"] for r in finished]
        assert len(set(fid)) == len(fid), f"{job}: duplicate finished execution"


# --------------------------------------------------------------------------
# 10. results.json consistent with journal
# --------------------------------------------------------------------------
def test_results_consistent_with_journal(journal, results):
    done = {r["event"]["job"] for r in journal if r["event"]["kind"] == "job_done"}
    result_done = {
        n for n, j in results["jobs"].items() if j["status"] == "DONE"
    }
    assert done == result_done, \
        "results.json status set does not match journal job_done set"

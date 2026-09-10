"""Persistent job store.

This module owns the on-disk representation of job records. It provides
crash-safe primitives for reading and atomically updating a single job.
The distributed scheduling logic (dependencies, priority, leases, retry,
exactly-once) lives in coordinator.py and worker.py and uses these helpers.

You are free to restructure this module, but you MUST keep the on-disk JSON
schema below and the journal event vocabulary documented in the instruction
so the provided harness and the sealed verifier can parse your output.
"""

import os

from constants import JOBS_DIR
from journal import Journal
from models import atomic_write_json, now_ms, read_json

JOB_FIELDS = [
    "name", "status", "priority", "deps", "cmd",
    "max_retries", "retry_count", "attempts", "execution_ids",
    "applied", "lease_token", "instance", "leader_ts",
]


def job_path(name):
    return os.path.join(JOBS_DIR, name + ".json")


class Store:
    """Loads all job records from disk; provides the journal handle."""

    def __init__(self, jobs_dir=JOBS_DIR, state_dir=None):
        self.jobs_dir = jobs_dir
        self._state_dir = state_dir
        self.journal = Journal()

    def open(self):
        os.makedirs(self.jobs_dir, exist_ok=True)
        self.journal.open()
        return self

    # ---------- job records ----------
    def load_jobs(self):
        """Return dict name -> job record for every job on disk."""
        jobs = {}
        if not os.path.isdir(self.jobs_dir):
            return jobs
        for fn in os.listdir(self.jobs_dir):
            if not fn.endswith(".json"):
                continue
            p = os.path.join(self.jobs_dir, fn)
            rec = read_json(p)
            if rec is not None:
                name = rec.get("name", fn[:-5])
                jobs[name] = rec
        return jobs

    def save_job(self, job, journal_event=None):
        """Persist a job record atomically and (optionally) drain a durable
        journal event describing the transition. Use save_job for every
        state transition so the audit trail and the on-disk record stay in
        sync."""
        atomic_write_json(job_path(job["name"]), job)
        if journal_event is not None:
            self.journal.append(journal_event)
        return job

    def new_job(self, name, cmd, priority=1, deps=(), max_retries=0):
        """Create a submitted job record (PENDING). Append a 'submitted'
        journal event."""
        job = {
            "name": name,
            "status": "PENDING",
            "priority": int(priority),
            "deps": list(deps),
            "cmd": cmd,
            "max_retries": int(max_retries),
            "retry_count": 0,
            "attempts": 0,
            "execution_ids": [],
            "submitted_ms": now_ms(),
            "released": False,
            "applied": False,
            "lease_token": None,
        }
        self.save_job(
            job,
            journal_event={
                "kind": "submitted", "job": name, "deps": job["deps"],
                "priority": job["priority"], "max_retries": job["max_retries"],
            },
        )
        return job

    def close(self):
        self.journal.close()

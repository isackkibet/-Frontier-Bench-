"""Persistent job store (reference implementation).

Owns on-disk job records with crash-safe atomic writes and a single append-
only hash-chained journal. Distributed scheduling logic lives in
coordinator.py and worker.py and uses these primitives.
"""

import os
from constants import JOBS_DIR, JOURNAL_PATH
from journal import Journal
from models import atomic_write_json, new_id, now_ms, read_json


def job_path(name):
    return os.path.join(JOBS_DIR, name + ".json")


class Store:
    def __init__(self, jobs_dir=JOBS_DIR, state_dir=None):
        self.jobs_dir = jobs_dir
        self.journal = Journal()

    def open(self):
        os.makedirs(self.jobs_dir, exist_ok=True)
        self.journal.open()
        return self

    def load_jobs(self):
        jobs = {}
        if not os.path.isdir(self.jobs_dir):
            return jobs
        for fn in os.listdir(self.jobs_dir):
            if not fn.endswith(".json"):
                continue
            p = os.path.join(self.jobs_dir, fn)
            rec = read_json(p)
            if rec is not None:
                jobs[rec.get("name", fn[:-5])] = rec
        return jobs

    def save_job(self, job, journal_event=None):
        atomic_write_json(job_path(job["name"]), job)
        if journal_event is not None:
            self.journal.append(journal_event)
        return job

    def new_job(self, name, cmd, priority=1, deps=(), max_retries=0):
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
            "worker": None,
            "execution_id": None,
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

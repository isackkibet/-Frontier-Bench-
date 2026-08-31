"""Worker process (reference implementation).

A worker heartbeats, atomically claims released jobs, runs their commands,
and journals execution_started / execution_finished / job_done transitions
such that each job is executed to completion exactly once even across worker
crashes and scheduler restarts.
"""

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from store import Store
from constants import (
    STATE_DIR, CLAIMS_DIR, HEARTBEATS_DIR,
    WORKER_HEARTBEAT_SEC, WORKER_CLAIM_POLL_SEC,
)
from models import new_id


class Worker:
    def __init__(self, worker_id, state_dir=STATE_DIR):
        self.worker_id = worker_id
        self.state_dir = state_dir
        self.store = Store(state_dir=state_dir)
        self._running = True

    def _heartbeat_path(self):
        return os.path.join(HEARTBEATS_DIR, self.worker_id)

    def _pidfile_path(self):
        return os.path.join(self.state_dir, "pids", self.worker_id)

    def start(self):
        self.store.open()
        os.makedirs(os.path.dirname(self._pidfile_path()), exist_ok=True)
        with open(self._pidfile_path(), "w") as f:
            f.write(str(os.getpid()))
        self._touch_heartbeat()

    def _touch_heartbeat(self):
        os.makedirs(HEARTBEATS_DIR, exist_ok=True)
        with open(self._heartbeat_path(), "a"):
            pass
        os.utime(self._heartbeat_path())

    def _job_status(self, name):
        jobs = self.store.load_jobs()
        return jobs.get(name)

    def _take_my_assigned_job(self):
        """Find a claim file that names this worker and whose job is still
        PENDING (i.e. not yet started, not DONE). Return that job or None.

        The coordinator is the sole creator of claim files (atomic
        O_CREAT|O_EXCL), so exactly one worker is ever assigned a given job
        at a time. A worker executes only jobs the coordinator has assigned
        to *it*, guarding on the on-disk status so a stale claim left by a
        previous incarnation of this worker id is never double-executed.
        """
        if not os.path.isdir(CLAIMS_DIR):
            return None
        for fn in os.listdir(CLAIMS_DIR):
            if not fn.endswith(".json"):
                continue
            try:
                with open(os.path.join(CLAIMS_DIR, fn)) as f:
                    claim = json.load(f)
            except Exception:
                continue
            if claim.get("worker") != self.worker_id:
                continue
            job = self._job_status(claim.get("job"))
            if job is not None and job["status"] == "PENDING":
                return job
        return None

    def _announce_start(self, job):
        execution_id = new_id("exec-")
        rec = dict(job)
        rec["execution_id"] = execution_id
        rec["execution_ids"] = rec.get("execution_ids", []) + [execution_id]
        rec["attempts"] = rec.get("attempts", 0) + 1
        rec["status"] = "RUNNING"
        self.store.save_job(
            rec,
            journal_event={
                "kind": "execution_started", "job": rec["name"],
                "worker": self.worker_id, "execution_id": execution_id,
            },
        )

    def _execute(self, job):
        return subprocess.call(job["cmd"], shell=True)

    def _remove_claim(self, job_name):
        p = os.path.join(CLAIMS_DIR, job_name + ".json")
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    def _finish(self, job, rc):
        name = job["name"]
        execution_id = job.get("execution_id")
        self.store.journal.append(
            {"kind": "execution_finished", "job": name,
             "worker": self.worker_id, "execution_id": execution_id, "rc": rc}
        )
        rec = self._job_status(name) or dict(job)
        if rc == 0:
            if rec["status"] != "DONE":
                rec["status"] = "DONE"
                rec["worker"] = None
                self.store.save_job(
                    rec,
                    journal_event={"kind": "job_done", "job": name},
                )
            else:
                self.store.save_job(rec)
            self._remove_claim(name)
        else:
            if rec.get("retry_count", 0) < rec.get("max_retries", 0):
                rec["status"] = "PENDING"
                rec["retry_count"] = rec.get("retry_count", 0) + 1
                rec["worker"] = None
                self.store.save_job(
                    rec,
                    journal_event={
                        "kind": "retry_scheduled", "job": name,
                        "attempt": rec["retry_count"],
                    },
                )
            else:
                rec["status"] = "FAILED"
                rec["worker"] = None
                self.store.save_job(
                    rec,
                    journal_event={"kind": "job_failed", "job": name},
                )
            self._remove_claim(name)

    def run(self):
        self.start()
        stop = [False]

        def _hb():
            while not stop[0]:
                self._touch_heartbeat()
                time.sleep(WORKER_HEARTBEAT_SEC)

        t = threading.Thread(target=_hb, daemon=True)
        t.start()
        try:
            while self._running and not stop[0]:
                job = self._take_my_assigned_job()
                if job is None:
                    time.sleep(WORKER_CLAIM_POLL_SEC)
                    continue
                try:
                    self._announce_start(job)
                    rc = self._execute(job)
                    self._finish(job, rc)
                except Exception as e:
                    self.store.journal.append(
                        {"kind": "worker_error", "worker": self.worker_id,
                         "job": job.get("name"), "error": repr(e)})
        finally:
            stop[0] = True


def main(argv=None):
    ap = argparse.ArgumentParser(prog="worker.py")
    ap.add_argument("--id", required=True, dest="worker_id")
    ap.add_argument("--state-dir", default=STATE_DIR)
    args = ap.parse_args(argv)
    w = Worker(worker_id=args.worker_id, state_dir=args.state_dir)
    w.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())

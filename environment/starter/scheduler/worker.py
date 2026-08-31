"""Worker process (runnable as its own OS subprocess).

A worker is a separate process that:

  * heartbeats to a file under the heartbeats dir every
    WORKER_HEARTBEAT_SEC so the coordinator knows it is alive;
  * writes its process id to /scheduler/data/pids/<id> at startup so the
    scenario harness can SIGKILL it to simulate a crash;
  * executes only jobs the coordinator has assigned to it via an atomic
    claim file (/scheduler/data/claims/<job>.json naming this worker). The
    coordinator is the sole creator of claim files, so no two workers ever
    execute the same job;
  * for each assigned job, runs the job's ``cmd`` as a subprocess, capturing
    its exit code, and journaling ``execution_started`` (with a unique
    execution_id) before running and ``execution_finished`` after;
  * marks the job DONE (writing ``job_done``) only if the exit code was 0;
    otherwise it leaves the job for retry/failure handling (respecting
    max_retries) and removes the claim so it can be re-assigned;
  * removes its claim file when it finishes a job.

Exactly-once is enforced by the unique execution_id token plus atomic
coordinator assignment: a job is claimed by exactly one live worker at a
time, and a crash-killed worker leaves a claim that the coordinator expires
and re-assigns. Never mark a job DONE twice — read the on-disk status before
writing ``job_done``.

You can run a worker directly for development:

    python3 /scheduler/worker.py --id w01
"""

import argparse
import json
import os
import subprocess
import sys
import time
from store import Store
from constants import (
    STATE_DIR, CLAIMS_DIR, HEARTBEATS_DIR, PIDS_DIR,
    WORKER_HEARTBEAT_SEC, WORKER_CLAIM_POLL_SEC,
)
import models


class Worker:
    def __init__(self, worker_id, state_dir=STATE_DIR):
        self.worker_id = worker_id
        self.state_dir = state_dir
        self.store = Store(state_dir=state_dir)
        self._running = True

    def _heartbeat_path(self):
        return os.path.join(self.state_dir, "heartbeats", self.worker_id)

    def _pidfile_path(self):
        return os.path.join(self.state_dir, "pids", self.worker_id)

    def start(self):
        self.store.open()
        os.makedirs(os.path.dirname(self._pidfile_path()), exist_ok=True)
        with open(self._pidfile_path(), "w") as f:
            f.write(str(os.getpid()))
        self._touch_heartbeat()

    # ---- to implement ----
    def _touch_heartbeat(self):
        """Refresh this worker's heartbeat file so the coordinator treats it
        as alive."""
        raise NotImplementedError("implement in worker")

    def _take_my_assigned_job(self):
        """Find a claim file that names this worker and whose job is still
        PENDING (not yet started, not DONE). Return that job record or None.

        The coordinator is the sole creator of claim files (atomic
        O_CREAT|O_EXCL), so exactly one worker is ever assigned a given job
        at a time. A worker executes only jobs the coordinator assigned to
        *it*, guarding on the on-disk status so a stale claim left by a
        previous incarnation of this worker id is never double-executed.
        """
        raise NotImplementedError("implement in worker")

    def _announce_start(self, job):
        """Create a unique execution_id, record it on the job, journal
        ``execution_started`` with {job, worker, execution_id}."""
        raise NotImplementedError("implement in worker")

    def _execute(self, job):
        """Run job['cmd'] via subprocess; return its exit code (0 => ok)."""
        return subprocess.call(job["cmd"], shell=True)

    def _finish(self, job, rc):
        """Journal execution_finished {job, worker, execution_id, rc};
        if rc==0, mark the job DONE once (journal job_done); else handle
        retry/failure and leave claim available for another attempt."""
        raise NotImplementedError("implement in worker")

    def run(self):
        """Heartbeat on a thread; claim + run jobs until stopped."""
        self.start()
        stop = [False]

        def _hb():
            while not stop[0]:
                self._touch_heartbeat()
                time.sleep(WORKER_HEARTBEAT_SEC)

        import threading
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
                except Exception as e:  # never die silently
                    exc = repr(e)
                    self.store.journal.append(
                        {"kind": "worker_error", "worker": self.worker_id,
                         "job": job.get("name"), "error": exc})
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

"""Scheduler coordinator: DAG + priority + lease recovery + retries.

This is the heart of the task. You must implement the coordinator loop that
runs continuously and is resilient to crashes and restarts.

Responsibilities
----------------
1. Load all job records from disk at startup (jobs persist across restarts).
2. Each tick:
   a. Detect jobs whose dependencies are all DONE and that are PENDING and
      not already handed to a worker, and mark them "ready" by appending a
      ``release`` journal event (see below) and setting the job's
      ``deps_ready`` flag.
   b. Hand each ready job to a live worker that currently holds no lease,
      by atomically creating a claim file (see worker.py) for the worker and
      job, and writing that claim to the journal. A worker must never be
      handed two jobs at once and no two workers may claim the same job.
3. Lease recovery: any job whose claim has expired (worker heartbeat gone)
   and is not DONE must be recovered: the old claim is removed and the job
   is returned to PENDING/ready (respecting max_retries and attempt count).
4. Priority: when multiple jobs are ready, hand them out in priority order
   (lower numeric ``priority`` first; ties by submission order). Concurrency
   must not break this ordering across recovery windows.
5. Durability: every transition is journaled and the job record saved.
   The coordinator must survive being SIGKILLed and restarted on the same
   state directory without losing or duplicating work.

Journal event vocabulary (event kinds) you MUST emit
---------------------------------------------------
  submitted           job submitted (already handled by Store)
  release             job name deps all DONE -> eligible for dispatch
  assign              coordinator assigned job to a worker: {job, worker}
  worker_heartbeat    worker alive (worker emits; coordinator may relay)
  execution_started   worker started executing: {job, worker, execution_id}
  execution_finished  worker finished executing: {job, worker, execution_id, rc}
  lease_expired       coordinator recovered an expired claim: {job, worker}
  retry_scheduled     job queued for another attempt after failure: {job, attempt}
  job_done            job reached DONE: {job}
  job_failed          job exhausted retries: {job}

Exactly-once contract
---------------------
A job whose final state is DONE must have exactly one ``job_done`` event and
exactly one completed ``execution_finished``. Crash-interrupted runs produce
an ``execution_started`` without a matching ``execution_finished``; that is
allowed. You must never emit two ``job_done`` events for the same job and
must never mark a job DONE twice. Guard with the on-disk status BEFORE you
journal ``job_done``.

The docstrings on the methods below are the specification; implement the
bodies.
"""

import os
import time
from store import Store
from constants import STATE_DIR, LEASE_RECHECK_SEC


class Coordinator:
    def __init__(self, workers=1, store=None, state_dir=STATE_DIR):
        self.workers = workers
        self.store = store or Store(state_dir=state_dir)
        self._running = True

    # ---- public API ----
    def start(self):
        self.store.open()

    def tick(self):
        """One recovery + dispatch pass. Called in a loop by run()."""
        self._recover_expired_claims()
        self._release_ready_jobs()
        self._dispatch()

    def run(self, rounds=None):
        """Main loop. If ``rounds`` is None, run forever (until SIGKILL)."""
        self.start()
        n = 0
        while self._running:
            self.tick()
            n += 1
            if rounds is not None and n >= rounds:
                break
            time.sleep(LEASE_RECHECK_SEC)
        return 0

    def stop(self):
        self._running = False

    # ---- to implement ----
    def _deps_satisfied(self, job, jobs):
        return all(jobs.get(d, {}).get("status") == "DONE" for d in job.get("deps", []))

    def _drain_ready(self, jobs):
        """Return list of ready job records in dispatch order."""
        ready = []
        for job in jobs.values():
            if job["status"] == "PENDING" and self._deps_satisfied(job, jobs):
                ready.append(job)
        ready.sort(key=lambda j: (j["priority"], j.get("submitted_ms", 0)))
        return ready

    def _workers_online(self):
        """Return list of worker ids that have a fresh heartbeat."""
        raise NotImplementedError("implement in coordinator")

    def _recover_expired_claims(self):
        """Reclaim jobs whose worker lease expired and is not DONE.
        Must respect max_retries and must not resurrect a DONE job."""
        raise NotImplementedError("implement in coordinator")

    def _release_ready_jobs(self):
        """Append a 'release' journal event for each job whose deps DONE
        and that is still PENDING (so workers may claim it)."""
        raise NotImplementedError("implement in coordinator")

    def _dispatch(self):
        """Assign ready jobs to idle live workers via atomic claim files.
        Never double-assign; never hand a worker two jobs at once."""
        raise NotImplementedError("implement in coordinator")

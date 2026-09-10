"""Scheduler coordinator (reference implementation).

Responsibilities:
  * load all job records from disk at startup (jobs persist across restarts);
  * each tick: recover expired claims, release jobs whose deps are DONE,
    dispatch ready jobs to idle live workers via atomic claim files;
  * priority order among ready jobs (lower number = higher priority);
  * lease recovery: any non-DONE job whose worker heartbeat is stale has its
    claim removed and is returned to PENDING so it can be re-dispatched;
  * every transition is journaled and job records saved crash-safely, so a
    SIGKILL + restart loses no committed work and never duplicates it.

Exactly-once: a job reaching DONE must have exactly one 'job_done' event and
exactly one completed execution. We guard the DONE transition by checking the
on-disk status before journaling.
"""

import json
import os
import time

from constants import (
    CLAIMS_DIR,
    DEFAULT_LEASE_SEC,
    HEARTBEATS_DIR,
    LEASE_RECHECK_SEC,
    STATE_DIR,
    WORKER_HEARTBEAT_SEC,
)
from models import new_id, now_ms, read_json
from store import Store


class Coordinator:
    def __init__(self, workers=1, store=None, state_dir=STATE_DIR):
        self.workers = workers
        self.store = store or Store(state_dir=state_dir)
        self._running = True

    def start(self):
        self.store.open()
        self.store.journal.append(
            {"kind": "scheduler_start", "instance": new_id("srv-")}
        )

    # ---- main loop ----
    def run(self, rounds=None):
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

    # ---- helpers ----
    def _deps_satisfied(self, job, jobs):
        return all(
            jobs.get(d, {}).get("status") == "DONE" for d in job.get("deps", [])
        )

    def _claim_is_live(self, claim):
        """A claim is live iff the claiming worker's heartbeat is fresh."""
        worker = claim.get("worker")
        hb = os.path.join(HEARTBEATS_DIR, worker) if worker else None
        if not hb or not os.path.exists(hb):
            return False
        try:
            age = time.time() - os.path.getmtime(hb)
        except OSError:
            return False
        return age < 3 * WORKER_HEARTBEAT_SEC + 1.0

    def _read_claim(self, job_name):
        p = os.path.join(CLAIMS_DIR, job_name + ".json")
        return read_json(p)

    def _remove_claim(self, job_name):
        p = os.path.join(CLAIMS_DIR, job_name + ".json")
        try:
            os.remove(p)
        except FileNotFoundError:
            pass

    # ---- tick phases ----
    def _recover_expired_claims(self):
        jobs = self.store.load_jobs()
        if not os.path.isdir(CLAIMS_DIR):
            return
        for fn in os.listdir(CLAIMS_DIR):
            if not fn.endswith(".json"):
                continue
            job_name = fn[:-5]
            claim = self._read_claim(job_name)
            if claim is None:
                continue
            job = jobs.get(job_name)
            if job is None:
                self._remove_claim(job_name)
                continue
            if job["status"] == "DONE":
                # already finished; just drop any stale claim
                self._remove_claim(job_name)
                continue
            if not self._claim_is_live(claim):
                # claiming worker is gone (crashed). Reclaim the job so it
                # can be re-dispatched. Do NOT consume a retry for a crash.
                self._remove_claim(job_name)
                self.store.journal.append(
                    {"kind": "lease_expired", "job": job_name,
                     "worker": claim.get("worker")}
                )
                self.store.save_job(
                    {**job, "status": "PENDING", "worker": None,
                     "execution_id": None},
                    journal_event=None,
                )

    def _release_ready_jobs(self):
        jobs = self.store.load_jobs()
        ready = [
            j for j in jobs.values()
            if j["status"] == "PENDING" and self._deps_satisfied(j, jobs)
        ]
        ready.sort(key=lambda j: (j["priority"], j.get("submitted_ms", 0)))
        for j in ready:
            if not j.get("released"):
                self.store.save_job(
                    {**j, "released": True},
                    journal_event={"kind": "release", "job": j["name"]},
                )

    def _idle_live_workers(self):
        """Return worker ids that have a fresh heartbeat and hold no live
        claim."""
        live = set()
        if os.path.isdir(HEARTBEATS_DIR):
            for fn in os.listdir(HEARTBEATS_DIR):
                try:
                    if time.time() - os.path.getmtime(
                        os.path.join(HEARTBEATS_DIR, fn)) < 3 * WORKER_HEARTBEAT_SEC + 1.0:
                        live.add(fn)
                except OSError:
                    continue
        busy = set()
        if os.path.isdir(CLAIMS_DIR):
            for fn in os.listdir(CLAIMS_DIR):
                claim = self._read_claim(fn[:-5])
                if claim and self._claim_is_live(claim):
                    busy.add(claim.get("worker"))
        return [w for w in live if w not in busy]

    def _try_claim(self, job, worker):
        """Atomically create a claim file; returns True if we won."""
        p = os.path.join(CLAIMS_DIR, job["name"] + ".json")
        token = new_id("tok-")
        payload = {
            "job": job["name"], "worker": worker, "token": token,
            "expires_at_ms": now_ms() + DEFAULT_LEASE_SEC * 1000,
        }
        os.makedirs(CLAIMS_DIR, exist_ok=True)
        try:
            fd = os.open(p, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        return True

    def _dispatch(self):
        jobs = self.store.load_jobs()
        ready = [
            j for j in jobs.values()
            if j["status"] == "PENDING"
            and j.get("released")
            and not os.path.exists(os.path.join(CLAIMS_DIR, j["name"] + ".json"))
        ]
        ready.sort(key=lambda j: (j["priority"], j.get("submitted_ms", 0)))

        idle = self._idle_live_workers()
        # deterministically order idle workers for fairness
        idle.sort()
        for job in ready:
            if not idle:
                break
            worker = idle.pop(0)
            if self._try_claim(job, worker):
                self.store.save_job(
                    {**job, "worker": worker},
                    journal_event={"kind": "assign", "job": job["name"],
                                   "worker": worker},
                )

    def tick(self):
        self._recover_expired_claims()
        self._release_ready_jobs()
        self._dispatch()

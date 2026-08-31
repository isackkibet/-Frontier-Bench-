#!/usr/bin/env python3
"""Deterministic crash-injection scenario for the scheduler task.

This script drives your scheduler implementation through a realistic
failure gauntlet and publishes the resulting evidence under /logs for the
sealed verifier. It:

  1. starts the coordinator + 3 worker processes (your implementation);
  2. submits a small DAG plus a priority pair;
  3. SIGKILLs a worker in the middle of executing a job (worker crash) and
     verifies the job is recovered and completed exactly once;
  4. SIGKILLs the coordinator and every worker, then restarts them, to prove
     committed state survives a full restart;
  5. finishes the remainder of the DAG;
  6. publishes /logs/state.journal.jsonl, /logs/results.json and
     /logs/final_state*/ for grading.

You should run this from the /scheduler directory:

    python3 /scheduler/harness/scenario.py

It must exit 0 with all scenario jobs DONE. Use it to iterate during
development. Do not modify this file; if your implementation is correct the
scenario passes as-is.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import time

STATE_DIR = "/scheduler/data"
LOGS_DIR = "/logs"
SCHED = "/scheduler/scheduler.py"
HARNESS_NAME = "scenario"

JOURNAL = os.path.join(STATE_DIR, "journal.jsonl")
PIDS_DIR = os.path.join(STATE_DIR, "pids")
HEARTBEATS = os.path.join(STATE_DIR, "heartbeats")
CLAIMS = os.path.join(STATE_DIR, "claims")
JOBS = os.path.join(STATE_DIR, "jobs")

OUT_JOURNAL = os.path.join(LOGS_DIR, "state.journal.jsonl")
OUT_RESULTS = os.path.join(LOGS_DIR, "results.json")
OUT_PERSISTED = os.path.join(LOGS_DIR, "persisted_state.json")
OUT_FINAL = os.path.join(LOGS_DIR, "final_state")


def log(msg):
    print(f"[scenario] {msg}", flush=True)


def reset():
    for d in (STATE_DIR, LOGS_DIR):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d, exist_ok=True)
    for d in (JOBS, CLAIMS, HEARTBEATS, PIDS_DIR):
        os.makedirs(d, exist_ok=True)


def submit(name, cmd, priority=1, deps=(), max_retries=1):
    args = [sys.executable, SCHED, "submit", "--name", name, "--cmd", cmd,
            "--priority", str(priority), "--max-retries", str(max_retries)]
    for d in deps:
        args += ["--dep", d]
    r = subprocess.run(args, capture_output=True, text=True)
    if r.returncode != 0:
        log(f"submit {name} failed: {r.stderr}")
        sys.exit(10)
    return r.stdout


def launch():
    return subprocess.Popen(
        [sys.executable, SCHED, "serve", "--workers", "3"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, cwd="/scheduler",
    )


class JournalTail:
    """Incrementally reads the journal into a single accumulating list so no
    event is ever lost by a query that returns early. All wait helpers query
    this same accumulated history."""

    def __init__(self, path):
        self.path = path
        self._offset = 0
        self.events = []  # accumulated raw records

    def _record(self, line):
        try:
            return json.loads(line)
        except Exception:
            return None

    def poll(self):
        try:
            with open(self.path, "r") as f:
                f.seek(self._offset)
                data = f.read()
                self._offset = f.tell()
        except FileNotFoundError:
            return
        for ln in data.split("\n"):
            ln = ln.strip()
            if not ln:
                continue
            rec = self._record(ln)
            if rec is not None:
                self.events.append(rec)

    def has(self, kinds, job=None):
        kinds = {kinds} if isinstance(kinds, str) else set(kinds)
        for rec in self.events:
            ev = rec["event"]
            if ev.get("kind") in kinds:
                if job is None or ev.get("job") == job:
                    return True
        return False


def wait_for(tail, pred, timeout, desc):
    deadline = time.time() + timeout
    while time.time() < deadline:
        tail.poll()
        if pred():
            return True
        time.sleep(0.2)
    log(f"TIMEOUT waiting for {desc}")
    return False


def wait_event(tail, kinds, job=None, timeout=120, desc=""):
    return wait_for(tail, lambda: tail.has(kinds, job), timeout, desc or f"{kinds} {job}")


def wait_all_done(tail, names, timeout=120):
    return wait_for(
        tail, lambda: all(tail.has("job_done", n) for n in names),
        timeout, f"all of {sorted(names)} to be DONE",
    )


def read_pids():
    pids = {}
    if os.path.isdir(PIDS_DIR):
        for fn in os.listdir(PIDS_DIR):
            with open(os.path.join(PIDS_DIR, fn)) as f:
                pids[fn] = int(f.read().strip())
    return pids


def kill_all():
    """SIGKILL every worker and the coordinator recorded in pids/."""
    pids = read_pids()
    for name, pid in pids.items():
        try:
            os.kill(pid, signal.SIGKILL)
            log(f"killed {name} pid {pid}")
        except ProcessLookupError:
            log(f"{name} pid {pid} already gone")
        except Exception as e:
            log(f"could not kill {name}: {e}")
    time.sleep(1)


def file_tail(path):
    try:
        with open(path) as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            if size < 3:
                return None
            f.seek(size - 3)
            return f.read()
    except Exception:
        return None


def main():
    reset()
    tail = JournalTail(JOURNAL)
    log(f"state dir reset; launching scheduler")
    srv = launch()
    time.sleep(3)

    # wait for scheduler_start #1
    if not wait_event(tail, "scheduler_start", timeout=30, desc="first boot"):
        log("coordinator did not start; dumping serve log")
        (srv.stdout or sys.stdout) and srv.stdout and log(srv.stdout.read())
        sys.exit(11)

    log("Phase 1: submit A (crash target) and E")
    submit("A", "sleep 8 && echo A-done", priority=1, max_retries=2)
    submit("E", "echo E-done", priority=1, max_retries=1)

    # wait until A is actually executing (worker claimed + started)
    if not wait_event(tail, "execution_started", job="A", timeout=60,
                      desc="A starts"):
        log("A never started executing")
        sys.exit(12)

    # who holds A? read its claim file
    claim_p = os.path.join(CLAIMS, "A.json")
    killer = None
    if os.path.exists(claim_p):
        claim = json.load(open(claim_p))
        killer = claim.get("worker")
    log(f"worker {killer} holds A; killing it mid-execution")
    pids = read_pids()
    if killer and killer in pids:
        try:
            os.kill(pids[killer], signal.SIGKILL)
            log(f"SIGKILL worker {killer} pid {pids[killer]}")
        except ProcessLookupError:
            log(f"{killer} already gone")

    # wait for lease recovery + A done exactly once
    if not wait_event(tail, "lease_expired", job="A", timeout=60,
                      desc="A lease recovery"):
        log("A lease was never recovered")
        sys.exit(13)
    if not wait_event(tail, "job_done", job="A", timeout=90, desc="A done"):
        log("A never reached DONE after recovery")
        sys.exit(14)
    log("Phase 2: A recovered and completed")

    log("Phase 3: submit B, C (depend on A) and priority pair P1/P2")
    submit("B", "sleep 2 && echo B-done", priority=1, max_retries=1, deps=["A"])
    submit("C", "sleep 2 && echo C-done", priority=1, max_retries=1, deps=["A"])
    submit("P1", "sleep 1 && echo p1", priority=1, max_retries=1)
    submit("P2", "sleep 1 && echo p2", priority=2, max_retries=1)

    if not wait_all_done(tail, ["B", "C", "P1", "P2"], timeout=90):
        log("some of B/C/P1/P2 never reached DONE")
        sys.exit(15)

    log("Phase 4: full restart (coordinator + workers)")
    kill_all()
    # drain/serve traces
    log("restarting scheduler from the same state dir")
    srv = launch()
    time.sleep(3)
    if not wait_event(tail, "scheduler_start", timeout=30, desc="second boot"):
        log("coordinator did not restart")
        sys.exit(16)

    log("Phase 5: submit D (depends on B and C)")
    submit("D", "sleep 3 && echo D-done", priority=1, max_retries=2,
           deps=["B", "C"])
    if not wait_event(tail, "job_done", job="D", timeout=90, desc="D done"):
        log("D never reached DONE")
        sys.exit(17)

    # small grace period so trailing transitions are flushed
    time.sleep(3)

    # publish outputs
    os.makedirs(OUT_FINAL, exist_ok=True)
    for item in os.listdir(STATE_DIR):
        s = os.path.join(STATE_DIR, item)
        d = os.path.join(OUT_FINAL, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, dirs_exist_ok=True)
        else:
            shutil.copy2(s, d)
    shutil.copy2(JOURNAL, OUT_JOURNAL)

    # results.json
    jobs = {}
    for fn in os.listdir(JOBS):
        if fn.endswith(".json"):
            rec = json.load(open(os.path.join(JOBS, fn)))
            jobs[rec["name"]] = {
                "status": rec["status"],
                "attempts": rec.get("attempts", 0),
                "execution_ids": rec.get("execution_ids", []),
                "priority": rec.get("priority"),
                "deps": rec.get("deps", []),
                "max_retries": rec.get("max_retries", 0),
                "retry_count": rec.get("retry_count", 0),
            }
    with open(OUT_RESULTS, "w") as f:
        json.dump({"jobs": jobs}, f, sort_keys=True, indent=2)

    # persisted_state.json: a snapshot taken from the actual per-job files
    # on disk (an evidence channel independent of the append-only journal).
    # Used by the verifier as a cross-check of durable state after restart.
    with open(OUT_PERSISTED, "w") as f:
        json.dump({"jobs": {n: {"status": j["status"]} for n, j in jobs.items()}},
                  f, sort_keys=True, indent=2)
    log("published " + OUT_JOURNAL + " and " + OUT_RESULTS +
        " and " + OUT_PERSISTED)

    # verify everything DONE
    names = ["A", "B", "C", "D", "E", "P1", "P2"]
    ok = all(jobs.get(n, {}).get("status") == "DONE" for n in names)
    log("ALL DONE" if ok else "some jobs not DONE")
    return 0 if ok else 20


if __name__ == "__main__":
    sys.exit(main())

"""Shared constants and paths for the Kepler Frontier Bench scheduler task.

You may adjust the tunables in this file, but you MUST keep the public
paths and the JSON schema described in the task instruction intact so that
the scenario harness and the sealed verifier can read your output.
"""
import os

STATE_DIR = os.environ.get("SCHED_STATE_DIR", "/scheduler/data")
LOGS_DIR = os.environ.get("SCHED_LOGS_DIR", "/logs")

JOBS_DIR = os.path.join(STATE_DIR, "jobs")
CLAIMS_DIR = os.path.join(STATE_DIR, "claims")
HEARTBEATS_DIR = os.path.join(STATE_DIR, "heartbeats")
PIDS_DIR = os.path.join(STATE_DIR, "pids")
JOURNAL_PATH = os.path.join(STATE_DIR, "journal.jsonl")
META_PATH = os.path.join(STATE_DIR, "meta.json")

# Final declared artifacts (read by the sealed verifier).
OUT_JOURNAL = os.path.join(LOGS_DIR, "state.journal.jsonl")
OUT_STATE_DIR = os.path.join(LOGS_DIR, "final_state")
OUT_RESULTS = os.path.join(LOGS_DIR, "results.json")

# ---------- Tunables ----------
DEFAULT_LEASE_SEC = 45          # a worker lease is valid this long
LEASE_RECHECK_SEC = 1.0         # coordinator recovery-scan period
WORKER_HEARTBEAT_SEC = 1.0      # worker heartbeat period
WORKER_CLAIM_POLL_SEC = 0.5     # worker claim-poll period

# Job status lifecycle.
PENDING = "PENDING"
RUNNING = "RUNNING"
DONE = "DONE"
FAILED = "FAILED"
CANCELLED = "CANCELLED"

# Crash-resilient distributed job scheduler

## Overview

You must complete a small production-style job scheduler in Python so that it
behaves correctly under real failures — worker processes that are killed
mid-job, and a scheduler that is killed and restarted on the same persistent
state. The scheduler must enforce dependency (DAG) ordering, priority-based
dispatch, retries with a maximum attempt budget, and exactly-once completion of
every job, while keeping all committed state durable across restarts.

A starter skeleton lives in `/scheduler/` (Python modules). The hard engineering
is yours: you implement the core scheduler logic inside those modules. A
deterministic crash-injection harness is provided at
`/scheduler/harness/scenario.py`; you run it as the last step to publish the
outputs that get graded.

## What is already provided

- `/scheduler/constants.py` — paths, tunables, status constants.
- `/scheduler/journal.py` — an append-only, hash-chained, fsync-ed audit
  journal with a process-safe `append(event)` (a `Journal` class). Use it for
  every durable transition; the journal is the main evidence that is checked.
- `/scheduler/models.py` — helpers (`now_ms`, `new_id`, `atomic_write_json`,
  `read_json`).
- `/scheduler/store.py` — a `Store` for job records (atomic writes) and the
  journal handle; `Store.new_job` already journals a `submitted` event.
- `/scheduler/scheduler.py` — CLI: `submit`, `serve`, `status`, `dump-state`.
  `serve --workers N` must launch a coordinator and N worker **processes** so
  the harness can kill individual workers.
- `/scheduler/coordinator.py`, `/scheduler/worker.py` — stubs whose docstrings
  are the specification. You implement the bodies.
- `/scheduler/harness/scenario.py` — the deterministic scenario; do not modify
  it. If your implementation is correct, it exits 0 with every job DONE.

## What to implement

Implement `/scheduler/coordinator.py` (`Coordinator`) and
`/scheduler/worker.py` (`Worker`) and finish the wiring in `scheduler.py`. The
scheduler must satisfy these invariants:

1. **Persistence.** Every job record is stored under
   `/scheduler/data/jobs/<name>.json` via crash-safe atomic writes (temp file +
   fsync + rename). All state lives under `/scheduler/data/` and survives a
   SIGKILL of the scheduler and its workers. On startup the coordinator reloads
   all job records from disk.

2. **Dependencies.** A job is *released* (eligible for dispatch) only after
   every job in its `deps` is `DONE`. A dependent job must not start executing
   before all of its dependencies finish. Release is recorded with a `release`
   journal event exactly once per job.

3. **Priority.** When multiple jobs are simultaneously ready, dispatch in order
   of `priority` (lower numeric value = higher priority) and, within equal
   priority, by submission order (`submitted_ms`).

4. **Lease-based claiming (exactly-once).** Ready jobs are claimed by workers
   through atomic claim files under `/scheduler/data/claims/<name>.json`
   created with `O_CREAT | O_EXCL` so no two workers ever claim the same job.
   Each execution uses a unique `execution_id`. A worker that crashes leaves
   its claim behind; the coordinator must detect that the claiming worker's
   heartbeat has gone stale, remove the claim, journal a `lease_expired` event,
   and return the job to an eligible state so it can be re-dispatched. A job is
   marked `DONE` (journal `job_done`) exactly once, and only after a successful
   run.

5. **Retries.** If a claimed job's command exits non-zero, count it against
   `max_retries`; if retries remain, re-enqueue it (journal `retry_scheduled`)
   so it can be attempted again; otherwise mark it `FAILED` (journal
   `job_failed`).

6. **Restart resilience.** Killing and restarting the scheduler + all workers
   must not lose committed work and must not cause any job to be completed
   twice. The coordinator journals a `scheduler_start` event (with a unique
   instance id) each time it boots.

The `Journal.append(event)` method takes a dict with a `"kind"` field. The event
vocabulary the harness and grader rely on is: `submitted`, `release`, `assign`,
`execution_started`, `execution_finished`, `lease_expired`, `retry_scheduled`,
`job_done`, `job_failed`, `scheduler_start`. Emit them as described in the
stub docstrings.

## How to run

From `/scheduler`:

    python3 /scheduler/harness/scenario.py

The harness starts your scheduler, submits jobs `A, B, C, D, E, P1, P2`
(A → B,C → D), SIGKILLs the worker executing `A` mid-job, waits for recovery,
SIGKILLs the coordinator and workers and restarts them, then finishes the DAG.
It must exit 0 with all jobs `DONE`.

## Required deliverables (absolute paths)

After a successful run, the harness writes these files under `/logs/`, which
are the graded artifacts:

- `/logs/state.journal.jsonl` — the audit journal (copied from the state dir).
- `/logs/results.json` — job status summary.
- `/logs/final_state/` — snapshot of `/scheduler/data/` (jobs, claims,
  heartbeats, journal, pids).

The grader reads only these paths. If they are missing or internally
inconsistent, the task is scored 0.

## Grading summary

The verifier checks that: all seven scenario jobs are `DONE`; the journal hash
chain is intact with strictly sequential `seq` and non-decreasing timestamps;
each job has exactly one `job_done` and exactly one successful
(`rc == 0`) `execution_finished`; `A` actually crashed and was re-attempted
(more than one distinct `execution_started` plus a `lease_expired`); every
dependent job finishes and starts only after its dependencies; `P1` is released
before `P2`; there were at least two distinct `scheduler_start` events (restart)
and `A`'s `DONE` state survived; no `execution_id` is reused or finished twice;
and `results.json` agrees with the set of `job_done` events in the journal.

Work outside `/scheduler` is irrelevant to the result. Make the real scheduler
correct; do not attempt to fabricate or reorder the journal, since the grader
validates its integrity and cross-file consistency.

You have 1800 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.

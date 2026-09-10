# Crash-resilient distributed job scheduler

## Overview

The starter Python scheduler is already in `/scheduler/`. The task is to
complete it so it can keep working when processes fail. A worker can be
killed while it is running a job, and the scheduler itself can also be killed
and started again using the same saved data. Jobs that were already committed
must not disappear or be executed twice.

The scheduler needs to handle the normal scheduling work as well: dependency
ordering, job priorities, retries with a maximum attempt limit, and worker
leases. These rules must continue to work after a crash or restart.

The crash test is provided in `/scheduler/harness/scenario.py`. Run that
harness after the implementation is complete. It generates the files used
for grading. Do not change the harness.

## What is already provided

The project already contains most of the supporting code. The main files are:

- `/scheduler/constants.py` — contains the paths, configuration values, and
  status constants used by the scheduler.

- `/scheduler/journal.py` — contains the append-only audit journal. It is
  hash-chained and uses `fsync` so entries are durable. The `Journal` class
  provides a process-safe `append(event)` method. Every durable state change
  needs to be recorded here because the journal is checked by the grader.

- `/scheduler/models.py` — contains common helper functions such as
  `now_ms`, `new_id`, `atomic_write_json`, and `read_json`.

- `/scheduler/store.py` — provides the `Store` used for job records and the
  journal. `Store.new_job` already records the `submitted` event.

- `/scheduler/scheduler.py` — provides the CLI commands `submit`, `serve`,
  `status`, and `dump-state`. The `serve --workers N` command must start one
  coordinator and N separate worker processes. The workers need to be
  separate processes because the test harness kills individual workers.

- `/scheduler/coordinator.py` and `/scheduler/worker.py` — contain the
  coordinator and worker stubs. Their docstrings describe the expected
  behaviour. The missing implementation needs to be added here.

- `/scheduler/harness/scenario.py` — contains the deterministic crash test.
  Do not modify this file. With a correct implementation, the scenario should
  exit with status 0 and all jobs should finish as `DONE`.

## What needs to be implemented

Implement `/scheduler/coordinator.py` (`Coordinator`) and
`/scheduler/worker.py` (`Worker`). You also need to finish the required
process wiring in `scheduler.py`.

The scheduler has to meet the following requirements.

### 1. Persistence

Every job must have its own persistent record at:

`/scheduler/data/jobs/<name>.json`

Use crash-safe atomic writes when updating these records. The write should
use a temporary file, `fsync`, and then rename the file into place.

All scheduler state must remain under `/scheduler/data/` and must still be
available after a `SIGKILL` of the scheduler or any worker.

When the coordinator starts, it must read the existing job records from disk.
It cannot depend only on state that was held in memory before the restart.

### 2. Dependencies

A job is only ready for dispatch after every job listed in its `deps` has
reached `DONE`.

For example, if `B` depends on `A`, `B` must not start while `A` is still
running or waiting for a retry. For the DAG in the test, `B` and `C` depend
on `A`, while `D` depends on both `B` and `C`.

When a job becomes eligible, the coordinator must record a `release` event.
A job should only have one release event.

### 3. Priority

When more than one job is ready at the same time, the coordinator must
choose them by priority.

A smaller numeric `priority` value means higher priority.

If two jobs have the same priority, the earlier `submitted_ms` value wins.
This gives jobs with the same priority a deterministic submission order.

### 4. Lease-based claiming and exactly-once completion

Ready jobs are claimed through files in:

`/scheduler/data/claims/<name>.json`

Claim creation must use:

`O_CREAT | O_EXCL`

The claim operation must be atomic so two workers cannot successfully claim
the same job.

Each execution must have a new and unique `execution_id`.

A worker that is killed can leave its claim file behind. The coordinator has
to detect that the worker's heartbeat is stale. When that happens, it must
remove the old claim, record a `lease_expired` event, and make the job
available for another attempt.

A job can only be marked `DONE` after a successful execution. The successful
completion must produce a `job_done` event.

A completed job must not receive another `job_done` event, even if the
scheduler is restarted. Execution IDs must also never be reused.

### 5. Retries

When a job command exits with a non-zero return code, that execution has
failed and must count against the job's `max_retries` limit.

If the job still has retries available, put it back into the state where it
can be dispatched again and record a `retry_scheduled` event.

If there are no retries left, mark the job as `FAILED` and record a
`job_failed` event.

A successful command must not be retried. It should proceed to `DONE`.

### 6. Restart recovery

The scheduler must be able to stop and start again without losing committed
state.

When the coordinator starts, it must recover the jobs from
`/scheduler/data/`. A job that was already completed before the restart must
remain completed and must not be completed a second time.

Every new coordinator process must write a `scheduler_start` event with a
unique instance ID.

The journal event names required by the harness and grader are:

`submitted`, `release`, `assign`, `execution_started`,
`execution_finished`, `lease_expired`, `retry_scheduled`, `job_done`,
`job_failed`, `scheduler_start`

Use these event names exactly and follow the event details described in the
stub docstrings.

## How to run the test

Run the scenario from `/scheduler`:

    python3 /scheduler/harness/scenario.py

The test creates these seven jobs:

`A, B, C, D, E, P1, P2`

The main dependency chain is:

`A → B,C → D`

The harness first allows the scheduler to start working, then kills the
worker that is running `A`. The scheduler has to notice that the worker's
lease expired and arrange for `A` to be attempted again.

Later, the harness kills the coordinator and all workers and starts the
scheduler again. The new processes must recover the existing state and
continue from there.

The expected result is that the harness exits with status 0 and all seven
jobs finish in the `DONE` state.

## Required deliverables

After a successful run, the harness creates these files under `/logs/`:

- `/logs/state.journal.jsonl` — the audit journal copied from the scheduler's
  state directory.

- `/logs/results.json` — contains the final status summary for the jobs.

- `/logs/persisted_state.json` — contains a snapshot of
  `/scheduler/data/`, including jobs, claims, heartbeats, journal data, and
  PID information.

These are the files the grader reads. If they are missing or their contents
do not agree with each other, the task receives a score of 0.
## Grading summary

All grading is done using the three `/logs/` files above. The verifier checks
that:

- all seven jobs (`A`, `B`, `C`, `D`, `E`, `P1`, and `P2`) finish as `DONE`;

- the journal chain is valid: sequence numbers increase by exactly one,
  timestamps never go backwards, and every hash correctly links to the
  previous journal entry;

- every job has exactly one `job_done` event and exactly one successful
  execution where `rc == 0`;

- job `A` was actually interrupted and recovered. The journal must show
  more than one distinct `execution_started` event for `A` and a
  `lease_expired` event for `A`;

- dependency ordering is respected. `B` and `C` cannot start before `A`
  finishes, and `D` cannot start before both `B` and `C` finish;

- `P1` is released before `P2`;

- there are at least two different `scheduler_start` events, showing that
  the scheduler was restarted, and the `DONE` state of `A` survives that
  restart;

- no `execution_id` is reused and no execution is recorded as finished more
  than once;

- `results.json` agrees with the set of `job_done` events recorded in the
  journal.

Anything outside `/scheduler/` is irrelevant to the result. The important
thing is that the scheduler itself handles the failures correctly.

Do not try to create fake journal entries or reorder existing entries to
make the output pass. The verifier checks the journal hash chain and also
compares the different output files, so inconsistent state will be detected.

You have 1800 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
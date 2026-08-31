"""CLI entry point for the scheduler.

Subcommands (used by the scenario harness and by you during development):

  submit --name A --cmd "CMD" [--priority N] [--dep B [--dep C ...]]
                                    [--max-retries N]
      Create a job. Jobs persist under the state dir.

  serve --workers N
      Launch the coordinator and N worker processes, all sharing the same
      state dir, and run until SIGKILLed/SIGTERMed (this is the main
      runtime).

  status --name A
      Print the current job record as JSON to stdout.

  dump-state --out DIR
      Copy the full state dir (jobs/, claims/, heartbeats/, journal) into
      DIR, and write results.json next to it summarizing each job. Used to
      produce the declared output artifacts under /logs.

The harness at /scheduler/harness/scenario.py drives these commands and
performs real crash injection. You should be able to test your
implementation locally by running the harness (see README).
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
from store import Store
from constants import STATE_DIR, PIDS_DIR


def cmd_submit(args, store):
    store.open()
    job = store.new_job(
        name=args.name,
        cmd=args.cmd,
        priority=args.priority,
        deps=args.dep,
        max_retries=args.max_retries,
    )
    print(json.dumps(job, sort_keys=True))


def cmd_status(args, store):
    store.open()
    jobs = store.load_jobs()
    job = jobs.get(args.name)
    if job is None:
        sys.exit(1)
    print(json.dumps(job, sort_keys=True))


def cmd_serve(args, store):
    from coordinator import Coordinator
    import signal

    store.open()
    os.makedirs(PIDS_DIR, exist_ok=True)
    with open(os.path.join(PIDS_DIR, "coordinator"), "w") as f:
        f.write(str(os.getpid()))

    # Spawn each worker as its own OS subprocess so the harness can SIGKILL
    # a single worker to simulate a crash.
    worker_procs = []
    here = os.path.dirname(os.path.abspath(__file__))
    for i in range(args.workers):
        wid = f"w{i + 1:02d}"
        p = subprocess.Popen(
            [sys.executable, os.path.join(here, "worker.py"),
             "--id", wid, "--state-dir", STATE_DIR],
            cwd=here,
        )
        worker_procs.append(p)

    coord = Coordinator(workers=args.workers, store=store)

    def _shutdown(signum, frame):
        coord.stop()
        for p in worker_procs:
            try:
                p.terminate()
            except Exception:
                pass

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    return coord.run()


def cmd_dump(args, store, logs_dir):
    import constants
    os.makedirs(args.out, exist_ok=True)
    # copy state dir contents
    if os.path.isdir(STATE_DIR):
        for item in os.listdir(STATE_DIR):
            s = os.path.join(STATE_DIR, item)
            d = os.path.join(args.out, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)
    # also guarantee the outputs referenced by the verifier exist
    shutil.copy2(constants.JOURNAL_PATH, constants.OUT_JOURNAL)
    # results.json
    jobs = store.load_jobs()
    results = {
        "jobs": {
            name: {"status": j["status"], "attempts": j["attempts"],
                   "execution_ids": j.get("execution_ids", [])}
            for name, j in jobs.items()
        }
    }
    with open(constants.OUT_RESULTS, "w") as f:
        json.dump(results, f, sort_keys=True, indent=2)
    shutil.copy2(constants.OUT_RESULTS, os.path.join(args.out, "results.json"))


def main(argv=None):
    parser = argparse.ArgumentParser(prog="scheduler.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_submit = sub.add_parser("submit")
    p_submit.add_argument("--name", required=True)
    p_submit.add_argument("--cmd", required=True)
    p_submit.add_argument("--priority", type=int, default=1)
    p_submit.add_argument("--dep", action="append", default=[])
    p_submit.add_argument("--max-retries", type=int, default=0)
    p_submit.set_defaults(func=cmd_submit)

    p_serve = sub.add_parser("serve")
    p_serve.add_argument("--workers", type=int, default=1)
    p_serve.set_defaults(func=cmd_serve)

    p_status = sub.add_parser("status")
    p_status.add_argument("--name", required=True)
    p_status.set_defaults(func=cmd_status)

    p_dump = sub.add_parser("dump-state")
    p_dump.add_argument("--out", required=True)
    p_dump.set_defaults(func=cmd_dump)

    store = Store()
    args = parser.parse_args(argv)
    args.func(args, store)
    return 0


if __name__ == "__main__":
    sys.exit(main())

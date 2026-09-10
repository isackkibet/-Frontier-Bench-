# Frontier Bench — Crash-Resilient Distributed Job Scheduler

[![CI](https://github.com/isackkibet/-Frontier-Bench-/actions/workflows/ci.yml/badge.svg)](https://github.com/isackkibet/-Frontier-Bench-/actions/workflows/ci.yml)

A production-grade benchmark task for evaluating AI coding agents on **distributed systems engineering**. The agent must implement a crash-resilient job scheduler that handles real process kills, persistent state, dependency DAGs, and exactly-once execution guarantees.

## Overview

This benchmark tests whether an AI agent can build systems that survive real failures — not toy examples, but production-style components with interacting invariants: crash-safe persistence, atomic lease-based claiming, dependency ordering, priority dispatch, retries, and recovery from both worker crashes and full scheduler restarts.

## Project Structure

```
frontier-bench/
├── README.md              # This file
├── instruction.md         # Task instructions for the agent
├── task.toml              # Benchmark metadata and configuration
├── ruff.toml              # Lint scope/ruleset
├── docker-compose.yml     # Local agent + verifier + test services
├── environment/
│   ├── Dockerfile         # Agent environment container
│   └── starter/           # Starter skeleton for the agent
├── solution/
│   ├── solve.sh           # Reference solution launcher
│   └── scheduler/         # Complete reference implementation
├── tests/
│   ├── Dockerfile         # Sealed verifier image
│   ├── test.sh            # Verifier launcher
│   ├── test_grader.py     # Grading logic (not visible to agent)
│   ├── test_models.py     # Unit tests for shared helpers (plain CI)
│   └── test_journal.py    # Unit tests for the audit journal (plain CI)
└── .github/
    ├── workflows/ci.yml   # Lint + unit test pipeline
    └── ISSUE_TEMPLATE/    # Bug/feature/docs/question templates
```

## Task Requirements

The agent must implement a coordinator and worker system that satisfies:

| Requirement | Description |
|-------------|-------------|
| **Persistence** | Crash-safe atomic writes (temp + fsync + rename) for all job state |
| **Dependencies** | Jobs released only after all deps are `DONE` (DAG ordering) |
| **Priority** | Lower numeric priority dispatched first; ties broken by submission order |
| **Lease-based claiming** | Atomic `O_CREAT|O_EXCL` claim files prevent duplicate execution |
| **Retries** | Failed commands counted against `max_retries` budget |
| **Restart resilience** | SIGKILL + restart must not lose work or duplicate completion |

## Verification

The sealed verifier checks:

- All scenario jobs reach `DONE` status
- Journal hash chain integrity (sequential seq, monotonic timestamps)
- Exactly-once completion (one `job_done` and one successful `execution_finished` per job)
- Crash recovery occurred (worker kill detected and re-dispatched)
- Dependency ordering preserved
- Priority release order respected
- Restart persistence verified (multiple `scheduler_start` events)
- No duplicate execution IDs
- `results.json` consistency with journal

## Local Validation

Run the reference solution (must score 1):

```bash
harbor run -p . -a oracle -e docker -m anthropic/claude-opus-4-8
```

Run the do-nothing baseline (must score 0):

```bash
harbor run -p . -a nop -e docker
```

Run implementation review:

```bash
harbor check . -m anthropic/claude-opus-4-8
```

## Difficulty

This is a genuinely hard systems engineering exercise. Naive implementations fail:

- **In-memory queues** → crash loses all state
- **Non-atomic claims** → duplicate execution
- **Eager dependency firing** → jobs run before deps complete
- **No lease recovery** → crashed workers' jobs stuck forever

Multiple valid implementations exist — there is no single "correct answer" to guess. The grader validates invariants, not implementation details.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/your-feature`)
3. Commit your changes (`git commit -m 'Add your feature'`)
4. Push to the branch (`git push origin feature/your-feature`)
5. Open a Pull Request

## License

This project is part of the Kepler Frontier Bench initiative for evaluating AI coding agents on complex software engineering tasks.

## Acknowledgments

Built for the Frontier Bench benchmark suite — testing the limits of AI-assisted software engineering on real-world distributed systems challenges.

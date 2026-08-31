#!/bin/bash
# Reference (oracle) solution for the crash-resilient scheduler task.
# Deploys the complete reference implementation into /scheduler and runs the
# provided deterministic crash scenario, publishing /logs artifacts.
set -euo pipefail

# Deploy the reference scheduler modules (this directory is /solution at run
# time). We copy every module over the starter stubs but keep the provided
# harness in /scheduler/harness untouched.
cp -f /solution/scheduler/*.py /scheduler/

# Make sure the harness is in place.
mkdir -p /scheduler/harness
if [ ! -f /scheduler/harness/scenario.py ]; then
  echo "FATAL: harness missing" >&2
  exit 1
fi

echo "Running deterministic crash scenario..."
python3 /scheduler/harness/scenario.py
rc=$?

echo "Scenario exit code: $rc"
exit $rc

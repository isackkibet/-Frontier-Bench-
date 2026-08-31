#!/bin/bash
# Sealed verifier entry point for the crash-resilient scheduler task.
# Runs the grader tests (baked into the verifier image) against the declared
# artifacts transferred in at their original absolute paths, produces a CTRF
# test report, and writes exactly 0 or 1 to /logs/verifier/reward.txt.
set -uo pipefail

REWARD=/logs/verifier/reward.txt
mkdir -p /logs/verifier

# Run the graded tests. pytest is pre-installed in the verifier image
# (never pip-install during test.sh).
set +e
python3 -m pytest /tests/test_grader.py \
  --tb=short -q \
  --ctrf /logs/verifier/ctrf.json
pytest_rc=$?
set -e

if [ "$pytest_rc" -eq 0 ]; then
  echo "1" > "$REWARD"
else
  echo "0" > "$REWARD"
fi

echo "Test suite exit code: $pytest_rc"
exit 0

# Environment for the crash-resilient distributed job scheduler task.
# The agent implements a persistent, crash-safe job scheduler in /scheduler
# and runs the provided scenario harness to produce the declared output
# artifacts under /logs.
FROM python:3.12-slim-bookworm

# Apt packages are intentionally NOT version-pinned.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        procps \
        coreutils \
        file \
    && rm -rf /var/lib/apt/lists/*

ENV SCHED_STATE_DIR=/scheduler/data \
    SCHED_LOGS_DIR=/logs \
    PYTHONUNBUFFERED=1

# Starter skeleton (the agent completes these modules).
COPY starter/scheduler /scheduler

# Ensure entry scripts are executable.
RUN chmod +x /scheduler/*.py /scheduler/harness/*.py 2>/dev/null || true \
    && mkdir -p /logs /scheduler/data

WORKDIR /scheduler

# No platform pins; open internet is the default (no allow_internet flag set).
CMD ["/bin/bash"]

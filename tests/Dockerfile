# Separate sealed verifier image for the crash-resilient scheduler task.
# Built from this tests/ directory; the grader and test script are baked in
# and are NOT visible to the agent. Reads only the declared artifacts copied
# in at their original absolute paths (/logs/...) and mounts /logs/verifier.
FROM python:3.13-slim-bookworm

# Verifier dependencies are pre-installed at build time (never in test.sh).
RUN python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir \
        pytest==9.1.1 \
        pytest-json-ctrf==0.5.2

# Bake the tests (grader + launcher) into the image.
COPY . /tests/

# Harbor does not upload tests/ for a separate verifier env; test.sh is
# provided by this image and executed from the working directory.
ENV PYTHONUNBUFFERED=1
WORKDIR /app
CMD ["/bin/bash"]

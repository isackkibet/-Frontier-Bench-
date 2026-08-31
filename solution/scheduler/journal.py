"""Append-only, hash-chained audit journal.

Every durable state transition in the scheduler is recorded here. The
journal is the primary evidence the sealed verifier inspects, so it must
satisfy strong integrity properties:

  * lines are appended only (never rewritten or reordered);
  * every line is JSON with a strictly increasing integer ``seq``;
  * every line carries ``prev_hash`` equal to a hash of the previous line,
    forming a tamper-evident hash chain;
  * each append is fsync-ed before the caller proceeds.

Multiple processes (coordinator, workers, and the submit CLI) append to the
same journal, so the global sequence number is derived from the current file
tail while holding an OS-level advisory lock. This guarantees a single,
uninterrupted sequence across every writer process even if a process is
SIGKILLed mid-append.
"""

import hashlib
import json
import os
import fcntl
from constants import JOURNAL_PATH
from models import now_ms


class Journal:
    def __init__(self, path=JOURNAL_PATH):
        self._path = path
        self._fd = None

    # ---- lifecycle ----
    def open(self):
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        self._fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
        return self

    @staticmethod
    def _hash_of(seq, prev_hash, event):
        payload = f"{seq}|{prev_hash}|{json.dumps(event, sort_keys=True)}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _tail(self):
        """Return (seq, hash) of the last complete record, or (0, None)."""
        f = self._fd
        size = os.lseek(f, 0, os.SEEK_END)
        if size == 0:
            return 0, None
        BUFSIZE = 4096
        # read backwards to the start of the last line
        read_start = max(0, size - BUFSIZE)
        data = os.pread(f, size - read_start, read_start).decode("utf-8", "replace")
        lines = [ln for ln in data.split("\n") if ln.strip()]
        if not lines:
            return 0, None
        try:
            rec = json.loads(lines[-1])
            return rec["seq"], rec["hash"]
        except (json.JSONDecodeError, KeyError):
            # last line incomplete (a writer was killed mid-append) -> use
            # the preceding complete line
            if len(lines) >= 2:
                try:
                    rec = json.loads(lines[-2])
                    return rec["seq"], rec["hash"]
                except (json.JSONDecodeError, KeyError):
                    pass
            return 0, None

    # ---- append ----
    def append(self, event):
        """Append an event dict and return the new record.
        Process-safe: seq/hash are derived from the on-file tail under an
        exclusive advisory lock, so concurrent writers produce one global
        sequence across all processes."""
        if self._fd is None:
            raise RuntimeError("journal not open")
        fcntl.flock(self._fd, fcntl.LOCK_EX)
        try:
            prev_seq, prev_hash = self._tail()
            seq = prev_seq + 1
            rec = {
                "seq": seq,
                "ts_ms": now_ms(),
                "prev_hash": prev_hash,
                "event": event,
            }
            rec["hash"] = self._hash_of(rec["seq"], rec["prev_hash"], rec["event"])
            line = json.dumps(rec, sort_keys=True) + "\n"
            os.write(self._fd, line.encode("utf-8"))
            os.fsync(self._fd)
            return rec
        finally:
            fcntl.flock(self._fd, fcntl.LOCK_UN)

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None

    @property
    def path(self):
        return self._path

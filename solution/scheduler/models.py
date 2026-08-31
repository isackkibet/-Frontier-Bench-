"""Small data helpers shared by scheduler modules."""

import json
import os
import random
import string
import time
import uuid


def now_ms():
    return int(time.time() * 1000)


def new_id(prefix=""):
    """Unique id. Used for execution tokens and instance ids."""
    return prefix + uuid.uuid4().hex


def atomic_write_json(path, obj):
    """Crash-safe JSON write: write temp, fsync, rename over target.

    This is the pattern you should use for all durable job records so a
    crash mid-write never leaves a truncated/corrupt file at the target
    path. Implement any additional locking your design requires.
    """
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = path + ".tmp." + new_id()
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def read_json(path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        return None

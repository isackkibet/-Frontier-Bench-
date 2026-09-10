"""Unit tests for the shared scheduler helpers.

These tests exercise the pure data helpers that are independent of the
distributed scenario harness, so they can run quickly in ordinary CI without
Docker or real process kills.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "solution", "scheduler"))

import models  # noqa: E402


def test_now_ms_is_monotonic():
    a = models.now_ms()
    b = models.now_ms()
    assert b >= a


def test_new_id_is_unique():
    ids = {models.new_id() for _ in range(1000)}
    assert len(ids) == 1000


def test_atomic_write_json_roundtrip(tmp_path):
    target = os.path.join(str(tmp_path), "state.json")
    models.atomic_write_json(target, {"job": "A", "status": "DONE"})
    assert models.read_json(target) == {"job": "A", "status": "DONE"}


def test_atomic_write_cleans_up_temp_files(tmp_path):
    target = os.path.join(str(tmp_path), "state.json")
    models.atomic_write_json(target, {"n": 1})
    leftovers = [f for f in os.listdir(str(tmp_path)) if ".tmp." in f]
    assert leftovers == []


def test_atomic_write_creates_parent_dirs(tmp_path):
    target = os.path.join(str(tmp_path), "a", "b", "c.json")
    models.atomic_write_json(target, {"x": 1})
    assert os.path.exists(target)


def test_read_json_missing_returns_default(tmp_path):
    target = os.path.join(str(tmp_path), "missing.json")
    assert models.read_json(target, default={}) == {}


def test_read_json_corrupt_returns_none(tmp_path):
    target = os.path.join(str(tmp_path), "bad.json")
    with open(target, "w", encoding="utf-8") as f:
        f.write("{not valid json")
    assert models.read_json(target) is None


def test_json_is_written_sorted(tmp_path):
    target = os.path.join(str(tmp_path), "sorted.json")
    models.atomic_write_json(target, {"b": 1, "a": 2})
    with open(target, encoding="utf-8") as f:
        content = f.read()
    # keys "a" and "b" must appear in sorted order
    assert content.index('"a"') < content.index('"b"')

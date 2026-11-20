"""Durable storage: per-run checkpoint + append-only ledger.

Layout per run::

    runs/<run_id>/
        checkpoint.json   # current step states; rewritten atomically after every change
        ledger.jsonl      # one JSON object per line, append-only, fsynced

The checkpoint is written with write-to-temp + fsync + os.replace + fsync
of the directory, so a SIGKILL at any moment leaves either the old or the
new checkpoint intact — never a torn file.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .errors import CheckpointCorruptError, CheckpointError


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_state(run_id: str, workflow_name: str, step_names: list[str]) -> dict:
    return {
        "run_id": run_id,
        "workflow": workflow_name,
        "status": "pending",  # pending | running | completed | failed
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "steps": {
            name: {
                "status": "pending",  # pending | running | succeeded | failed
                "attempts": 0,
                "result": None,
                "error": None,
                "started_at": None,
                "ended_at": None,
            }
            for name in step_names
        },
    }


class CheckpointStore:
    """Atomic JSON checkpoint for one run."""

    def __init__(self, run_dir: str | Path):
        self.run_dir = Path(run_dir)
        self.path = self.run_dir / "checkpoint.json"
        self.ledger_path = self.run_dir / "ledger.jsonl"

    def exists(self) -> bool:
        return self.path.exists()

    def save(self, state: dict) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        state["updated_at"] = utc_now()
        tmp = self.path.with_name(f".checkpoint.json.tmp.{os.getpid()}")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        dir_fd = os.open(self.run_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    def load(self) -> dict:
        if not self.path.exists():
            raise CheckpointError(f"no checkpoint at {self.path}")
        try:
            with open(self.path, encoding="utf-8") as f:
                state = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            raise CheckpointCorruptError(
                f"checkpoint unreadable: {self.path}: {exc}"
            ) from exc
        if not isinstance(state, dict) or "steps" not in state or "run_id" not in state:
            raise CheckpointCorruptError(
                f"checkpoint has unexpected shape: {self.path}"
            )
        return state


class Ledger:
    """Append-only JSONL event log for one run."""

    def __init__(self, run_dir: str | Path):
        self.run_dir = Path(run_dir)
        self.path = self.run_dir / "ledger.jsonl"
        self._seq = 0
        if self.path.exists():
            with open(self.path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        self._seq += 1

    def append(self, event_type: str, **fields) -> dict:
        self._seq += 1
        event = {"seq": self._seq, "ts": utc_now(), "event": event_type, **fields}
        self.run_dir.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        return event

    def events(self) -> list[dict]:
        if not self.path.exists():
            return []
        out = []
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    # A crash mid-append can tear the final line; skip it.
                    continue
        return out

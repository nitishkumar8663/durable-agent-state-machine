"""The runner: executes a workflow durably.

Rules:
- After every state change the checkpoint is saved atomically.
- A step is marked ``running`` (and checkpointed) before it executes, so
  a crash mid-step is visible on resume as an interrupted step.
- Succeeded steps are never re-executed on resume; their stored result is
  reused. Running/failed steps are re-executed from scratch.
- A step that raises is retried with exponential backoff up to
  ``max_attempts``. Exhaustion fails the run; ``resume`` retries the
  failed step with a fresh attempt budget (the ledger keeps the full
  history, so the extra attempts are inspectable).
- Steps run sequentially in dependency order (v1; no parallel execution).
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .errors import CheckpointError, RetryExhaustedError
from .store import CheckpointStore, Ledger, new_state, utc_now
from .workflow import Workflow


@dataclass
class RunResult:
    run_id: str
    status: str  # completed | failed
    workflow: str
    run_dir: Path
    steps: dict = field(default_factory=dict)


class Runner:
    def __init__(
        self,
        workflow: Workflow,
        runs_dir: str | Path,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        self.workflow = workflow
        self.runs_dir = Path(runs_dir)
        self.sleeper = sleeper

    # -- public API ----------------------------------------------------
    def run(self, run_id: str | None = None) -> RunResult:
        run_id = run_id or uuid.uuid4().hex[:12]
        store = CheckpointStore(self.runs_dir / run_id)
        if store.exists():
            raise CheckpointError(f"run {run_id!r} already exists; use resume()")
        ledger = Ledger(store.run_dir)
        state = new_state(
            run_id, self.workflow.name, [s.name for s in self.workflow.steps]
        )
        state["status"] = "running"
        store.save(state)
        ledger.append("run_started", run_id=run_id, workflow=self.workflow.name)
        return self._execute(store, ledger, state)

    def resume(self, run_id: str) -> RunResult:
        store = CheckpointStore(self.runs_dir / run_id)
        state = store.load()
        ledger = Ledger(store.run_dir)
        if state["status"] == "completed":
            ledger.append("run_resumed", run_id=run_id, note="already completed")
            return RunResult(
                run_id, "completed", state["workflow"], store.run_dir, state["steps"]
            )
        state["status"] = "running"
        store.save(state)
        ledger.append("run_resumed", run_id=run_id, workflow=state["workflow"])
        return self._execute(store, ledger, state)

    # -- internals -----------------------------------------------------
    def _execute(self, store: CheckpointStore, ledger: Ledger, state: dict) -> RunResult:
        results = {
            name: s["result"]
            for name, s in state["steps"].items()
            if s["status"] == "succeeded"
        }
        for step in self.workflow.order():
            st = state["steps"][step.name]
            if st["status"] == "succeeded":
                ledger.append(
                    "step_skipped",
                    step=step.name,
                    note="already succeeded; reusing stored result",
                )
                continue
            try:
                results[step.name] = self._execute_step(
                    step, st, store, ledger, state, results
                )
            except RetryExhaustedError as exc:
                state["status"] = "failed"
                store.save(state)
                ledger.append(
                    "run_failed",
                    failed_step=step.name,
                    error=str(exc.__cause__ or exc),
                )
                return RunResult(
                    state["run_id"], "failed", state["workflow"],
                    store.run_dir, state["steps"],
                )
        state["status"] = "completed"
        store.save(state)
        ledger.append("run_completed", run_id=state["run_id"])
        return RunResult(
            state["run_id"], "completed", state["workflow"],
            store.run_dir, state["steps"],
        )

    def _execute_step(self, step, st, store, ledger, state, results):
        interrupted = st["status"] == "running"
        st["status"] = "running"
        st["attempts"] = 0
        st["error"] = None
        st["started_at"] = utc_now()
        st["ended_at"] = None
        store.save(state)
        ledger.append(
            "step_started", step=step.name, resumed_after_crash=interrupted
        )
        while True:
            st["attempts"] += 1
            ctx = {
                "results": dict(results),
                "run_dir": store.run_dir,
                "step": step.name,
                "attempt": st["attempts"],
            }
            try:
                result = step.fn(ctx)
            except Exception as exc:  # noqa: BLE001 - retry policy handles all step errors
                st["error"] = f"{type(exc).__name__}: {exc}"
                if st["attempts"] < step.max_attempts:
                    delay = step.backoff_delay(st["attempts"])
                    ledger.append(
                        "step_retry", step=step.name,
                        attempt=st["attempts"], error=st["error"], delay_s=delay,
                    )
                    store.save(state)
                    self.sleeper(delay)
                    continue
                st["status"] = "failed"
                st["ended_at"] = utc_now()
                store.save(state)
                ledger.append(
                    "step_failed", step=step.name,
                    attempts=st["attempts"], error=st["error"],
                )
                raise RetryExhaustedError(step.name, exc) from exc
            try:
                json.dumps(result)  # results must be checkpointable
            except (TypeError, ValueError) as exc:
                st["status"] = "failed"
                st["error"] = f"non-serialisable result: {exc}"
                st["ended_at"] = utc_now()
                store.save(state)
                ledger.append(
                    "step_failed", step=step.name,
                    attempts=st["attempts"], error=st["error"],
                )
                raise RetryExhaustedError(step.name, exc) from exc
            st["status"] = "succeeded"
            st["result"] = result
            st["error"] = None
            st["ended_at"] = utc_now()
            store.save(state)
            ledger.append("step_succeeded", step=step.name, attempt=st["attempts"])
            return result

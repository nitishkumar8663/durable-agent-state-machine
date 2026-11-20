"""Durable agent state machine — agents that survive production failures.

A small, stdlib-only library for DAG workflows with durable checkpointing,
crash recovery, and exponential-backoff retries.
"""

from .errors import (
    CheckpointCorruptError,
    CheckpointError,
    CycleError,
    DasmError,
    DuplicateStepError,
    RetryExhaustedError,
    StepFailedError,
    UnknownDependencyError,
)
from .runner import RunResult, Runner
from .store import CheckpointStore, Ledger, new_state
from .workflow import Step, Workflow

__version__ = "0.1.0"

__all__ = [
    "CheckpointCorruptError",
    "CheckpointError",
    "CheckpointStore",
    "CycleError",
    "DasmError",
    "DuplicateStepError",
    "Ledger",
    "RetryExhaustedError",
    "RunResult",
    "Runner",
    "Step",
    "StepFailedError",
    "UnknownDependencyError",
    "Workflow",
    "new_state",
]

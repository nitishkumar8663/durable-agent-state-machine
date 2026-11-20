"""Error types for the durable agent state machine."""


class DasmError(Exception):
    """Base class for all dasm errors."""


class StepFailedError(DasmError):
    """A step failed after exhausting its retries."""

    def __init__(self, step_name, cause=None):
        self.step_name = step_name
        self.cause = cause
        super().__init__(f"step {step_name!r} failed: {cause}")


class RetryExhaustedError(StepFailedError):
    """A step failed after exhausting its retry budget."""


class CycleError(DasmError):
    """The workflow graph contains a cycle."""


class UnknownDependencyError(DasmError):
    """A step depends on a step that is not in the workflow."""


class DuplicateStepError(DasmError):
    """Two steps share a name."""


class CheckpointError(DasmError):
    """The checkpoint on disk is missing or unreadable."""


class CheckpointCorruptError(CheckpointError):
    """The checkpoint file exists but is not valid JSON / has a bad shape."""

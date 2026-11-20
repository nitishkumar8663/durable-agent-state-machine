"""DAG workflow definition: named steps with dependencies and retry policy."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from .errors import CycleError, DuplicateStepError, UnknownDependencyError


@dataclass
class Step:
    """One unit of work in a workflow.

    ``fn`` receives a context dict ``{"results": {...}, "run_dir": Path,
    "step": name, "attempt": n}`` and must return a JSON-serialisable
    result, which is checkpointed and passed to later steps via
    ``ctx["results"]``.
    """

    name: str
    fn: Callable[[dict], Any]
    depends_on: tuple[str, ...] = ()
    max_attempts: int = 1
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 60.0
    description: str = ""

    def __post_init__(self):
        self.depends_on = tuple(self.depends_on)
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.backoff_factor < 1 or self.max_delay < 0:
            raise ValueError("retry delays must be non-negative and factor >= 1")

    def backoff_delay(self, failed_attempt: int) -> float:
        """Delay before retrying after ``failed_attempt`` (1-based)."""
        return min(
            self.base_delay * (self.backoff_factor ** (failed_attempt - 1)),
            self.max_delay,
        )


class Workflow:
    """A named DAG of steps. Validated on construction."""

    def __init__(self, name: str, steps: list[Step]):
        self.name = name
        self.steps = list(steps)
        names = [s.name for s in self.steps]
        if len(set(names)) != len(names):
            raise DuplicateStepError("step names must be unique")
        self._by_name = {s.name: s for s in self.steps}
        for step in self.steps:
            for dep in step.depends_on:
                if dep not in self._by_name:
                    raise UnknownDependencyError(
                        f"step {step.name!r} depends on unknown step {dep!r}"
                    )
        self._order = self._topological_order()

    def _topological_order(self) -> list[Step]:
        indegree = {s.name: len(s.depends_on) for s in self.steps}
        dependents: dict[str, list[str]] = {s.name: [] for s in self.steps}
        for step in self.steps:
            for dep in step.depends_on:
                dependents[dep].append(step.name)
        ready = deque([s.name for s in self.steps if indegree[s.name] == 0])
        order = []
        while ready:
            name = ready.popleft()
            order.append(self._by_name[name])
            for child in dependents[name]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    ready.append(child)
        if len(order) != len(self.steps):
            raise CycleError("workflow contains a dependency cycle")
        return order

    def order(self) -> list[Step]:
        """Steps in dependency order (dependencies first)."""
        return list(self._order)

    def __getitem__(self, name: str) -> Step:
        return self._by_name[name]

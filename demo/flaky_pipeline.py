"""Demo workflow: a 4-step agent task with a flaky step and a slow step.

Steps:
  fetch   — quick, always succeeds.
  enrich  — flaky: fails the first 2 attempts, succeeds on the 3rd
            (exponential backoff between attempts).
  analyze — slow: sleeps ~12s, so a crash can be staged mid-step.
  report  — writes report.txt into the run directory.

Run it (from the project root)::

    python -m dasm --runs-dir runs/demo1 run demo.flaky_pipeline:build_workflow --run-id demo1
    python -m dasm --runs-dir runs/demo1 resume demo.flaky_pipeline:build_workflow --run-id demo1

Demo knobs (env, mainly for tests):
  DEMO_SLOW_S       — seconds the analyze step sleeps (default 12)
  DEMO_RETRY_BASE_S — base retry delay for enrich (default 1.0)
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from dasm.workflow import Step, Workflow

FAIL_TIMES = 2  # enrich fails this many times before succeeding


class _Flaky:
    def __init__(self):
        self.calls = 0

    def __call__(self, ctx):
        self.calls += 1
        if self.calls <= FAIL_TIMES:
            raise RuntimeError(f"flaky upstream error (call {self.calls}/{FAIL_TIMES + 1})")
        return {"enriched": True, "calls": self.calls}


def _fetch(ctx):
    time.sleep(0.5)
    return {"items": 3, "source": "demo"}


def _analyze(ctx):
    slow_s = float(os.environ.get("DEMO_SLOW_S", "12"))
    for _ in range(int(slow_s)):
        time.sleep(1)
    return {"summary": "3 items analyzed", "seconds": slow_s}


def _report(ctx):
    run_dir = Path(ctx["run_dir"])
    report = run_dir / "report.txt"
    report.write_text(
        "flaky-report\n"
        f"items: {ctx['results']['fetch']['items']}\n"
        f"enrich calls: {ctx['results']['enrich']['calls']}\n"
        f"analysis: {ctx['results']['analyze']['summary']}\n",
        encoding="utf-8",
    )
    return {"report": str(report)}


def build_workflow() -> Workflow:
    base = float(os.environ.get("DEMO_RETRY_BASE_S", "1.0"))
    return Workflow(
        "flaky-report",
        [
            Step("fetch", _fetch, description="Fetch 3 demo items"),
            Step(
                "enrich", _Flaky(), depends_on=("fetch",),
                max_attempts=4, base_delay=base, backoff_factor=2.0,
                description="Flaky enrichment: fails twice, then succeeds",
            ),
            Step(
                "analyze", _analyze, depends_on=("fetch", "enrich"),
                description="Slow analysis step",
            ),
            Step(
                "report", _report, depends_on=("analyze",),
                description="Write report.txt",
            ),
        ],
    )

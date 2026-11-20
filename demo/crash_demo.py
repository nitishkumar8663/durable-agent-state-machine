"""Crash-kill + resume demo. Run from the project root::

    python demo/crash_demo.py

Starts the flaky pipeline, SIGKILLs it mid-``analyze``, then resumes from
the checkpoint and verifies the run completes without re-running ``fetch``.
Writes a transcript to demo/evidence.txt.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNS_DIR = ROOT / "runs"
RUN_ID = "crash-demo"
EVIDENCE = Path(__file__).resolve().parent / "evidence.txt"

ENV = dict(os.environ, PYTHONPATH=str(ROOT), DEMO_SLOW_S="12", DEMO_RETRY_BASE_S="0.5")


def ledger_events():
    p = RUNS_DIR / RUN_ID / "ledger.jsonl"
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


def count(event, step=None):
    return sum(
        1
        for e in ledger_events()
        if e["event"] == event and (step is None or e.get("step") == step)
    )


def wait_for(predicate, timeout=30, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    lines = []

    def log(msg):
        print(msg, flush=True)
        lines.append(msg)

    shutil.rmtree(RUNS_DIR / RUN_ID, ignore_errors=True)
    log("== durable agent state machine: crash-kill + resume demo ==")
    log(f"runs dir: {RUNS_DIR / RUN_ID}")

    # 1. Start the run.
    proc = subprocess.Popen(
        [sys.executable, "-m", "dasm", "--runs-dir", str(RUNS_DIR), "run",
         "demo.flaky_pipeline:build_workflow", "--run-id", RUN_ID],
        env=ENV, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    log("started run; waiting for `analyze` to begin...")
    ok = wait_for(
        lambda: any(
            e["event"] == "step_started" and e.get("step") == "analyze"
            for e in ledger_events()
        )
    )
    if not ok:
        proc.kill()
        log("ERROR: analyze never started")
        return 1
    log("analyze is running — SIGKILLing the process now.")
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    log(f"process killed (returncode {proc.returncode})")
    log(
        f"ledger after kill: fetch started x{count('step_started', 'fetch')}, "
        f"enrich succeeded x{count('step_succeeded', 'enrich')}, "
        f"analyze started x{count('step_started', 'analyze')}"
    )

    # 2. Resume from the checkpoint.
    log("resuming from checkpoint...")
    resumed = subprocess.run(
        [sys.executable, "-m", "dasm", "--runs-dir", str(RUNS_DIR), "resume",
         "demo.flaky_pipeline:build_workflow", "--run-id", RUN_ID],
        env=ENV, capture_output=True, text=True,
    )
    log(resumed.stdout.strip())
    if resumed.returncode != 0:
        log(f"ERROR: resume failed (exit {resumed.returncode}): {resumed.stderr}")
        return 1

    # 3. Verify.
    report = RUNS_DIR / RUN_ID / "report.txt"
    fetch_starts = count("step_started", "fetch")
    analyze_starts = count("step_started", "analyze")
    log(
        f"final ledger: fetch started x{fetch_starts} (must be 1 — no re-execution), "
        f"analyze started x{analyze_starts} (must be 2 — interrupted, then re-run)"
    )
    log(f"report exists: {report.exists()}")
    assert fetch_starts == 1, "fetch was re-executed after resume"
    assert analyze_starts == 2, "analyze was not re-run after the kill"
    assert report.exists(), "report.txt missing"
    log("PASS: run completed from checkpoint; no completed step was re-executed.")
    EVIDENCE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log(f"transcript written to {EVIDENCE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

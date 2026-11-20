"""Command line interface: run, resume, inspect, list.

Usage (from the project root)::

    python -m dasm --runs-dir runs/demo1 run demo.flaky_pipeline:build_workflow --run-id demo1
    python -m dasm --runs-dir runs/demo1 resume demo.flaky_pipeline:build_workflow --run-id demo1
    python -m dasm --runs-dir runs/demo1 inspect --run-id demo1
    python -m dasm --runs-dir runs/demo1 list
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

from .errors import CheckpointError, DasmError
from .runner import Runner
from .store import CheckpointStore, Ledger
from .workflow import Workflow


def load_workflow(spec: str) -> Workflow:
    """Load a workflow from ``module:builder`` (builder takes no args)."""
    if ":" not in spec:
        raise DasmError("workflow spec must be 'module:builder'")
    module_name, attr = spec.split(":", 1)
    module = importlib.import_module(module_name)
    builder = getattr(module, attr)
    workflow = builder()
    if not isinstance(workflow, Workflow):
        raise DasmError(f"{spec!r} did not return a Workflow")
    return workflow


def cmd_run(args) -> int:
    workflow = load_workflow(args.workflow)
    runner = Runner(workflow, args.runs_dir)
    try:
        result = runner.run(args.run_id)
    except CheckpointError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"run {result.run_id}: {result.status}  ({result.run_dir})")
    return 0 if result.status == "completed" else 1


def cmd_resume(args) -> int:
    workflow = load_workflow(args.workflow)
    runner = Runner(workflow, args.runs_dir)
    try:
        result = runner.resume(args.run_id)
    except (CheckpointError, DasmError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"run {result.run_id}: {result.status}  ({result.run_dir})")
    return 0 if result.status == "completed" else 1


def cmd_inspect(args) -> int:
    run_dir = Path(args.runs_dir) / args.run_id
    store = CheckpointStore(run_dir)
    try:
        state = store.load()
    except CheckpointError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"run {state['run_id']}  workflow={state['workflow']}  status={state['status']}")
    for name, st in state["steps"].items():
        line = f"  {name}: {st['status']} (attempts={st['attempts']})"
        if st["error"]:
            line += f"  error={st['error']}"
        print(line)
    print("ledger:")
    for event in Ledger(run_dir).events():
        rest = {k: v for k, v in event.items() if k not in ("seq", "ts", "event")}
        print(f"  #{event['seq']} {event['ts']} {event['event']} {json.dumps(rest, sort_keys=True)}")
    return 0


def cmd_list(args) -> int:
    runs_dir = Path(args.runs_dir)
    if not runs_dir.exists():
        print("no runs yet")
        return 0
    for run_dir in sorted(runs_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        store = CheckpointStore(run_dir)
        try:
            state = store.load()
        except CheckpointError:
            print(f"  {run_dir.name}: (no readable checkpoint)")
            continue
        print(f"  {state['run_id']}: {state['workflow']} — {state['status']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="dasm", description="Durable agent state machine"
    )
    p.add_argument(
        "--runs-dir", default="runs",
        help="directory holding run state (default: runs)",
    )
    sub = p.add_subparsers(dest="command", required=True)
    r = sub.add_parser("run", help="start a new run")
    r.add_argument("workflow", help="workflow spec 'module:builder'")
    r.add_argument("--run-id", default=None)
    r.set_defaults(func=cmd_run)
    re_ = sub.add_parser("resume", help="resume an existing run from its checkpoint")
    re_.add_argument("workflow", help="workflow spec 'module:builder'")
    re_.add_argument("--run-id", required=True)
    re_.set_defaults(func=cmd_resume)
    i = sub.add_parser("inspect", help="show a run's checkpoint and ledger")
    i.add_argument("--run-id", required=True)
    i.set_defaults(func=cmd_inspect)
    l_ = sub.add_parser("list", help="list runs")
    l_.set_defaults(func=cmd_list)
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

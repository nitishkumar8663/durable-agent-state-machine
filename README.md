# Project 1 — Durable Agent State Machine

Agents that survive production failures: DAG workflows with durable
checkpointing, crash recovery, resumable long-running runs, and
exponential-backoff retries. Stdlib-only Python, no services, no models.

This is Project 1 of [Suraj Sharma's 12 agentic-AI projects](../README.md).

## How it works

A workflow is a DAG of named steps. Each step is a Python callable that
receives a context dict (`results` from earlier steps, `run_dir`,
`step` name, `attempt` number) and returns a JSON-serialisable result.

The runner checkpoints **after every state change**:

- Before a step executes, it is marked `running` and the checkpoint is
  saved — so a crash mid-step is visible on resume.
- After a step succeeds, its result is written to the checkpoint and the
  step is never re-executed on resume.
- Every event (run started/resumed/completed/failed, step started/
  succeeded/failed/retried/skipped) is appended to an append-only
  JSONL ledger, fsynced per event.

Checkpoint writes are atomic: write to a temp file, fsync, `os.replace`,
fsync the directory. A `SIGKILL` at any moment leaves either the old or
the new checkpoint intact — never a torn file.

Per-run layout:

```
runs/<run_id>/
    checkpoint.json   # current step states + run status
    ledger.jsonl      # append-only event history
    report.txt        # step artifacts (demo)
```

### Resume semantics

- Succeeded steps are **skipped**; their stored result is reused.
- `running` steps (killed mid-flight) and `failed` steps are **re-executed
  from scratch** with a fresh attempt budget. The ledger keeps the full
  history, so the extra attempts are inspectable.
- Resuming a completed run is a no-op.
- A step that raises is retried with exponential backoff
  (`delay = min(base * factor^(attempt-1), max_delay)`) up to
  `max_attempts`. Exhaustion fails the run; `resume` retries the failed
  step.

### What it is not (v1 limits)

- Steps run **sequentially** in dependency order; the DAG decides order,
  not concurrency. No parallel execution yet.
- No step timeouts, no distributed locking, no multi-process runs.
  One run = one process at a time.
- Step results must be JSON-serialisable (enforced at checkpoint time).

## Run the demo

```bash
# Crash-kill + resume, end to end (takes ~30s, writes demo/evidence.txt)
python demo/crash_demo.py

# Or by hand:
python -m dasm --runs-dir runs/demo1 run demo.flaky_pipeline:build_workflow --run-id demo1
python -m dasm --runs-dir runs/demo1 inspect --run-id demo1
python -m dasm --runs-dir runs/demo1 resume demo.flaky_pipeline:build_workflow --run-id demo1
python -m dasm --runs-dir runs/demo1 list
```

The demo workflow is a 4-step agent task: `fetch` → `enrich` (flaky:
fails twice, succeeds on the 3rd with backoff) → `analyze` (slow, ~12s,
so a crash can be staged mid-step) → `report` (writes `report.txt`).

`crash_demo.py` starts the run, waits until `analyze` is running,
`SIGKILL`s the process, then resumes from the checkpoint and asserts:
`fetch` started exactly once (not re-executed), `analyze` started twice
(interrupted, then re-run), and `report.txt` exists.

## Run the tests

```bash
python -m unittest discover -s tests -v
```

Covers: checkpoint write/restore, corrupt/missing checkpoints, torn
ledger lines, DAG ordering/dependencies/cycles, retry backoff and
exhaustion, resume-after-crash (unit-level and a real `SIGKILL` test).

## Layout

```
dasm/
    __init__.py     # public API
    errors.py       # DasmError, CycleError, RetryExhaustedError, ...
    store.py        # CheckpointStore (atomic JSON) + Ledger (JSONL)
    workflow.py     # Step, Workflow, topological sort, validation
    runner.py       # Runner: run / resume, retries, checkpointing
    cli.py          # run / resume / inspect / list
demo/
    flaky_pipeline.py   # 4-step demo workflow
    crash_demo.py       # SIGKILL + resume driver
    evidence.txt        # transcript of the last verified demo run
tests/
    test_checkpoint.py  # store + ledger
    test_dag.py         # ordering, cycles, unknown deps, backoff
    test_retry.py       # retries, exhaustion, serialisability
    test_resume.py      # resume semantics + real SIGKILL test
```

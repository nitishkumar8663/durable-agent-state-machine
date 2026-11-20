import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dasm.runner import Runner
from dasm.store import CheckpointStore
from dasm.workflow import Step, Workflow


class TestResumeUnit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.runs = Path(self.tmp.name) / "runs"

    def tearDown(self):
        self.tmp.cleanup()

    def test_succeeded_step_not_rerun_failed_step_rerun(self):
        calls = {"a": 0, "b": 0}

        def fa(ctx):
            calls["a"] += 1
            return {"a": 1}

        def fb(ctx):
            calls["b"] += 1
            return {"b": 2}

        wf = Workflow("t", [Step("a", fa), Step("b", fb, depends_on=("a",))])
        runner = Runner(wf, self.runs)
        result = runner.run("r1")
        self.assertEqual(result.status, "completed")

        # Simulate a crash: rewind step b to "running" as if killed mid-step.
        store = CheckpointStore(self.runs / "r1")
        state = store.load()
        state["steps"]["b"] = {
            "status": "running", "attempts": 1, "result": None,
            "error": None, "started_at": None, "ended_at": None,
        }
        state["status"] = "running"
        store.save(state)

        result2 = runner.resume("r1")
        self.assertEqual(result2.status, "completed")
        self.assertEqual(calls["a"], 1, "succeeded step a must not re-execute")
        self.assertEqual(calls["b"], 2, "interrupted step b must re-execute")

    def test_resume_completed_run_is_noop(self):
        wf = Workflow("t", [Step("a", lambda ctx: 1)])
        runner = Runner(wf, self.runs)
        runner.run("r1")
        result = runner.resume("r1")
        self.assertEqual(result.status, "completed")

    def test_resume_failed_run_retries_failed_step(self):
        calls = {"n": 0}

        def flaky(ctx):
            calls["n"] += 1
            if calls["n"] < 2:
                raise RuntimeError("fail once")
            return {"ok": True}

        wf = Workflow("t", [Step("a", flaky, max_attempts=1)])
        runner = Runner(wf, self.runs, sleeper=lambda s: None)
        result = runner.run("r1")
        self.assertEqual(result.status, "failed")
        result2 = runner.resume("r1")
        self.assertEqual(result2.status, "completed")
        self.assertEqual(calls["n"], 2)


class TestResumeKill(unittest.TestCase):
    """Real crash: SIGKILL a running process, resume from checkpoint."""

    def test_kill_mid_run_then_resume(self):
        root = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory() as tmp:
            runs_dir = Path(tmp) / "runs"
            env = dict(os.environ, PYTHONPATH=str(root),
                       DEMO_SLOW_S="3", DEMO_RETRY_BASE_S="0.01")
            proc = subprocess.Popen(
                [sys.executable, "-m", "dasm", "--runs-dir", str(runs_dir), "run",
                 "demo.flaky_pipeline:build_workflow", "--run-id", "killtest"],
                env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            ledger_path = runs_dir / "killtest" / "ledger.jsonl"
            deadline = time.monotonic() + 20
            started = False
            while time.monotonic() < deadline:
                if ledger_path.exists() and '"analyze"' in ledger_path.read_text():
                    started = True
                    break
                time.sleep(0.1)
            self.assertTrue(started, "analyze never started")
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            resumed = subprocess.run(
                [sys.executable, "-m", "dasm", "--runs-dir", str(runs_dir), "resume",
                 "demo.flaky_pipeline:build_workflow", "--run-id", "killtest"],
                env=env, capture_output=True, text=True,
            )
            self.assertEqual(resumed.returncode, 0, resumed.stdout + resumed.stderr)
            events = [json.loads(l) for l in ledger_path.read_text().splitlines() if l.strip()]
            fetch_starts = sum(
                1 for e in events
                if e["event"] == "step_started" and e.get("step") == "fetch"
            )
            analyze_starts = sum(
                1 for e in events
                if e["event"] == "step_started" and e.get("step") == "analyze"
            )
            self.assertEqual(fetch_starts, 1)
            self.assertEqual(analyze_starts, 2)
            self.assertTrue((runs_dir / "killtest" / "report.txt").exists())


if __name__ == "__main__":
    unittest.main()

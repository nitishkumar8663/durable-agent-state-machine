import tempfile
import unittest
from pathlib import Path

from dasm.runner import Runner
from dasm.workflow import Step, Workflow


class _Sleeper:
    def __init__(self):
        self.delays = []

    def __call__(self, seconds):
        self.delays.append(seconds)


class TestRetry(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.runs = Path(self.tmp.name) / "runs"

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, fn, **step_kwargs):
        wf = Workflow("t", [Step("s", fn, **step_kwargs)])
        sleeper = _Sleeper()
        result = Runner(wf, self.runs, sleeper=sleeper).run("r1")
        return result, sleeper

    def test_flaky_succeeds_after_retries(self):
        calls = {"n": 0}

        def fn(ctx):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("boom")
            return {"ok": True}

        result, sleeper = self._run(
            fn, max_attempts=4, base_delay=1.0, backoff_factor=2.0
        )
        self.assertEqual(result.status, "completed")
        self.assertEqual(calls["n"], 3)
        self.assertEqual(result.steps["s"]["attempts"], 3)
        self.assertEqual(sleeper.delays, [1.0, 2.0])  # exponential backoff

    def test_exhaustion_fails_run(self):
        def fn(ctx):
            raise RuntimeError("always fails")

        result, sleeper = self._run(
            fn, max_attempts=3, base_delay=0.5, backoff_factor=2.0
        )
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.steps["s"]["attempts"], 3)
        self.assertEqual(result.steps["s"]["status"], "failed")
        self.assertEqual(sleeper.delays, [0.5, 1.0])

    def test_non_serialisable_result_fails_step(self):
        result, _ = self._run(lambda ctx: object(), max_attempts=2)
        self.assertEqual(result.status, "failed")
        self.assertIn("serialisable", result.steps["s"]["error"])


if __name__ == "__main__":
    unittest.main()

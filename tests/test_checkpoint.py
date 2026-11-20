import json
import tempfile
import unittest
from pathlib import Path

from dasm.errors import CheckpointCorruptError, CheckpointError
from dasm.store import CheckpointStore, Ledger, new_state


class TestCheckpointStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.run_dir = Path(self.tmp.name) / "run1"

    def tearDown(self):
        self.tmp.cleanup()

    def test_round_trip(self):
        store = CheckpointStore(self.run_dir)
        state = new_state("run1", "wf", ["a", "b"])
        state["steps"]["a"]["status"] = "succeeded"
        state["steps"]["a"]["result"] = {"x": 1}
        store.save(state)
        loaded = store.load()
        self.assertEqual(loaded["steps"]["a"]["result"], {"x": 1})
        self.assertEqual(loaded["run_id"], "run1")

    def test_missing_raises(self):
        with self.assertRaises(CheckpointError):
            CheckpointStore(self.run_dir).load()

    def test_corrupt_raises(self):
        self.run_dir.mkdir(parents=True)
        (self.run_dir / "checkpoint.json").write_text("{not json", encoding="utf-8")
        with self.assertRaises(CheckpointCorruptError):
            CheckpointStore(self.run_dir).load()

    def test_wrong_shape_raises(self):
        self.run_dir.mkdir(parents=True)
        (self.run_dir / "checkpoint.json").write_text(
            json.dumps({"hello": 1}), encoding="utf-8"
        )
        with self.assertRaises(CheckpointCorruptError):
            CheckpointStore(self.run_dir).load()

    def test_stale_tmp_file_does_not_break_load(self):
        # A crash mid-write leaves a .tmp file; the last good checkpoint must load.
        store = CheckpointStore(self.run_dir)
        state = new_state("run1", "wf", ["a"])
        store.save(state)
        (self.run_dir / ".checkpoint.json.tmp.99999").write_text(
            "{torn", encoding="utf-8"
        )
        self.assertEqual(store.load()["run_id"], "run1")

    def test_ledger_appends_and_reads_in_order(self):
        ledger = Ledger(self.run_dir)
        ledger.append("run_started", run_id="run1")
        ledger.append("step_succeeded", step="a")
        events = ledger.events()
        self.assertEqual(
            [e["event"] for e in events], ["run_started", "step_succeeded"]
        )
        self.assertEqual([e["seq"] for e in events], [1, 2])

    def test_ledger_tolerates_torn_last_line(self):
        ledger = Ledger(self.run_dir)
        ledger.append("run_started", run_id="run1")
        with open(ledger.path, "a", encoding="utf-8") as f:
            f.write('{"seq": 2, "event": "step_sta')
        events = ledger.events()
        self.assertEqual(len(events), 1)


if __name__ == "__main__":
    unittest.main()

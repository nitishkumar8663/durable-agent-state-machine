import unittest

from dasm.errors import CycleError, DuplicateStepError, UnknownDependencyError
from dasm.workflow import Step, Workflow


def _wf(*steps):
    return Workflow("t", list(steps))


class TestDag(unittest.TestCase):
    def test_order_respects_dependencies(self):
        wf = _wf(
            Step("a", lambda c: 1),
            Step("b", lambda c: 2, depends_on=("a",)),
            Step("c", lambda c: 3, depends_on=("a",)),
            Step("d", lambda c: 4, depends_on=("b", "c")),
        )
        order = [s.name for s in wf.order()]
        self.assertLess(order.index("a"), order.index("b"))
        self.assertLess(order.index("a"), order.index("c"))
        self.assertLess(order.index("b"), order.index("d"))
        self.assertLess(order.index("c"), order.index("d"))

    def test_diamond_order(self):
        wf = _wf(
            Step("d", lambda c: 1, depends_on=("b", "c")),
            Step("b", lambda c: 1, depends_on=("a",)),
            Step("c", lambda c: 1, depends_on=("a",)),
            Step("a", lambda c: 1),
        )
        self.assertEqual([s.name for s in wf.order()], ["a", "b", "c", "d"])

    def test_cycle_raises(self):
        with self.assertRaises(CycleError):
            _wf(
                Step("a", lambda c: 1, depends_on=("b",)),
                Step("b", lambda c: 1, depends_on=("a",)),
            )

    def test_self_cycle_raises(self):
        with self.assertRaises(CycleError):
            _wf(Step("a", lambda c: 1, depends_on=("a",)))

    def test_unknown_dependency_raises(self):
        with self.assertRaises(UnknownDependencyError):
            _wf(Step("a", lambda c: 1, depends_on=("nope",)))

    def test_duplicate_name_raises(self):
        with self.assertRaises(DuplicateStepError):
            _wf(Step("a", lambda c: 1), Step("a", lambda c: 2))

    def test_backoff_delay(self):
        s = Step("a", lambda c: 1, base_delay=1.0, backoff_factor=2.0, max_delay=5.0)
        self.assertEqual(s.backoff_delay(1), 1.0)
        self.assertEqual(s.backoff_delay(2), 2.0)
        self.assertEqual(s.backoff_delay(3), 4.0)
        self.assertEqual(s.backoff_delay(10), 5.0)  # capped


if __name__ == "__main__":
    unittest.main()

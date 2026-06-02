import unittest

import wrap_dsqgen


class UnionTargetTests(unittest.TestCase):
    def test_default_targets(self) -> None:
        self.assertEqual([2, 5, 10, 20, 200], wrap_dsqgen._resolve_union_targets(None))

    def test_capped_targets(self) -> None:
        self.assertEqual([2, 5, 10, 20, 50], wrap_dsqgen._resolve_union_targets(50))

    def test_no_targets_when_cap_too_small(self) -> None:
        self.assertEqual([], wrap_dsqgen._resolve_union_targets(1))


if __name__ == "__main__":
    unittest.main()

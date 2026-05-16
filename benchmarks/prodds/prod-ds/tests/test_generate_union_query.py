import re
import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SCRIPT_PATH = REPO_ROOT / "workload" / "dsqgen" / "generate_union_query.py"


def _run_generator(inputs: int) -> str:
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--inputs", str(inputs)],
        text=True,
        capture_output=True,
        check=True,
    )
    return proc.stdout


class GenerateUnionQueryTests(unittest.TestCase):
    def test_min_inputs_enforced_and_ctes(self) -> None:
        sql = _run_generator(1)
        self.assertIn("inputs=2", sql.splitlines()[0])
        self.assertEqual(2, len(re.findall(r"u\d{4} AS", sql)))
        self.assertEqual(2, len(re.findall(r"SELECT \* FROM u\d{4}", sql)))
        self.assertIn("UNION ALL", sql)
        months = re.findall(r"d_moy = (\d+)", sql)
        self.assertEqual(["1", "2"], months)

    def test_months_cycle_and_branch_ids(self) -> None:
        sql = _run_generator(13)
        months = [int(m) for m in re.findall(r"d_moy = (\d+)", sql)]
        self.assertEqual(13, len(months))
        self.assertEqual(list(range(1, 13)) + [1], months)
        self.assertEqual(13, len(re.findall(r"u\d{4} AS", sql)))
        self.assertIn("13 AS union_branch", sql)


if __name__ == "__main__":
    unittest.main()

import sys
from pathlib import Path
import tempfile
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsqgen import split_queries as sq


SAMPLE = """-- start query 1 in stream 0 using template queryA.tpl
select 1;
-- end query 1 in stream 0 using template queryA.tpl
-- start query 2 in stream 0 using template queryB_ext.tpl
select 2;
-- end query 2 in stream 0 using template queryB_ext.tpl
"""


class SplitQueriesTests(unittest.TestCase):
    def test_split_queries_writes_individual_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = Path(tmpdir) / "query_0.sql"
            input_path.write_text(SAMPLE, encoding="utf-8")
            out_dir = Path(tmpdir) / "out"

            written = sq.split_queries(input_path, out_dir)

            self.assertEqual(2, written)
            q1 = (out_dir / "query_1.sql").read_text(encoding="utf-8")
            q2 = (out_dir / "query_2.sql").read_text(encoding="utf-8")
            self.assertIn("select 1;", q1)
            self.assertIn("select 2;", q2)
            self.assertIn("queryA.tpl", q1)
            self.assertIn("queryB_ext.tpl", q2)


if __name__ == "__main__":
    unittest.main()

import tempfile
from pathlib import Path
import sys

import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsqgen import template_resolver as tr


class TemplateResolverTests(unittest.TestCase):
    def test_resolve_prefers_ext_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tdir = Path(tmpdir)
            (tdir / "query1.tpl").write_text("-- base 1", encoding="utf-8")
            (tdir / "query2.tpl").write_text("-- base 2", encoding="utf-8")
            (tdir / "query2_ext.tpl").write_text("-- ext 2", encoding="utf-8")

            names = ["query1.tpl", "query2.tpl"]
            resolved = tr.resolve_templates(names, tdir, use_extensions=True)

            self.assertEqual(["query1.tpl", "query2_ext.tpl"], resolved)

    def test_resolve_ignores_ext_without_flag(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tdir = Path(tmpdir)
            (tdir / "query1.tpl").write_text("-- base 1", encoding="utf-8")
            (tdir / "query1_ext.tpl").write_text("-- ext 1", encoding="utf-8")

            names = ["query1.tpl"]
            resolved = tr.resolve_templates(names, tdir, use_extensions=False)

            self.assertEqual(["query1.tpl"], resolved)

    def test_resolve_falls_back_when_ext_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tdir = Path(tmpdir)
            (tdir / "query3.tpl").write_text("-- base 3", encoding="utf-8")

            resolved = tr.resolve_templates(["query3.tpl"], tdir, use_extensions=True)

            self.assertEqual(["query3.tpl"], resolved)

    def test_main_writes_output_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "tpls"
            base_dir.mkdir()
            (base_dir / "q.tpl").write_text("-- base q", encoding="utf-8")
            (base_dir / "q_ext.tpl").write_text("-- ext q", encoding="utf-8")

            input_lst = Path(tmpdir) / "templates.lst"
            input_lst.write_text("q.tpl\n", encoding="utf-8")
            output_lst = Path(tmpdir) / "out.lst"

            exit_code = tr.main(
                [
                    "--use-extended-queries",
                    "--input",
                    str(input_lst),
                    "--directory",
                    str(base_dir),
                    "--output",
                    str(output_lst),
                ]
            )

            self.assertEqual(0, exit_code)
            self.assertTrue(output_lst.exists())
            self.assertEqual("q_ext.tpl\n", output_lst.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

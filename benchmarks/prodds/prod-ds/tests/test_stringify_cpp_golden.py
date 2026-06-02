import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pytest

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CPP_BIN = REPO_ROOT / "workload" / "dsdgen" / "stringify_cpp"
CPP_DIR = CPP_BIN.parent


def _ensure_cpp_binary() -> None:
    subprocess.run(["make", "stringify_cpp"], cwd=str(CPP_DIR), check=True)


def _load_rules(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


@pytest.mark.needs_cpp
class StringifyCppGoldenTests(unittest.TestCase):
    def test_cpp_matches_python_rewrite(self) -> None:
        _ensure_cpp_binary()

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            py_dir = base / "py"
            cpp_dir = base / "cpp"
            py_dir.mkdir()
            cpp_dir.mkdir()

            from workload.dsdgen import stringify

            rules_path = base / "rules.yml"
            stringify.export_rewrite_rules(
                rules_path,
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
            )
            rules = _load_rules(rules_path)

            stringify_rules = rules.get("stringify", {}).get("rules", {}) or {}
            null_rules = rules.get("nulls", {}).get("rules", {}) or {}
            mcv_rules = rules.get("mcv", {}).get("rules", {}) or {}

            tables = []
            tables.extend(list(stringify_rules.keys()))
            for table in list(null_rules.keys()):
                if table not in tables:
                    tables.append(table)
            for table in list(mcv_rules.keys()):
                if table not in tables:
                    tables.append(table)

            if not tables:
                self.skipTest("No rewrite rules generated; skipping golden comparison.")

            table = tables[0]
            max_index = -1
            for rule in stringify_rules.get(table, []):
                max_index = max(max_index, int(rule["index"]))
            for rule in null_rules.get(table, []):
                max_index = max(max_index, int(rule["index"]))
            for rule in mcv_rules.get(table, []):
                max_index = max(max_index, int(rule["index"]))
            if max_index < 0:
                self.skipTest("No usable rules for golden test.")

            cols = max_index + 1
            rows = []
            for i in range(5):
                row = [str(i + 1)] * cols
                rows.append("|".join(row) + "|\n")

            for target in (py_dir, cpp_dir):
                (target / f"{table}.tbl").write_text("".join(rows), encoding="utf-8")

            stringify.rewrite_tbl_directory(
                py_dir,
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
            )

            subprocess.run(
                [str(CPP_BIN), "--output-dir", str(cpp_dir), "--rules-file", str(rules_path)],
                check=True,
            )

            py_text = (py_dir / f"{table}.tbl").read_text(encoding="utf-8")
            cpp_text = (cpp_dir / f"{table}.tbl").read_text(encoding="utf-8")

            self.assertEqual(py_text, cpp_text)


if __name__ == "__main__":
    unittest.main()

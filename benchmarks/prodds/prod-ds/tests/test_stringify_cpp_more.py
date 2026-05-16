import json
import os
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
class StringifyCppMoreTests(unittest.TestCase):
    def test_cpp_skips_when_no_rules(self) -> None:
        _ensure_cpp_binary()
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            data_dir = base / "data"
            data_dir.mkdir()
            rules_path = base / "rules.yml"
            rules_path.write_text(
                yaml.safe_dump(
                    {
                        "stringify": {"enabled": False, "rules": {}},
                        "nulls": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
                        "mcv": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            target = data_dir / "demo.tbl"
            target.write_text("1|2|\n", encoding="utf-8")

            subprocess.run(
                [str(CPP_BIN), "--output-dir", str(data_dir), "--rules-file", str(rules_path)],
                check=True,
            )

            self.assertEqual("1|2|\n", target.read_text(encoding="utf-8"))

    def test_cpp_matches_python_multiple_tables(self) -> None:
        _ensure_cpp_binary()
        from workload.dsdgen import stringify

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            py_dir = base / "py"
            cpp_dir = base / "cpp"
            py_dir.mkdir()
            cpp_dir.mkdir()

            rules_path = base / "rules.yml"
            stringify.export_rewrite_rules(
                rules_path,
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
            )
            rules = _load_rules(rules_path)
            tables = list((rules.get("stringify", {}).get("rules") or {}).keys())
            if len(tables) < 2:
                self.skipTest("Need at least two tables with rules for this test.")

            for table in tables[:2]:
                max_index = -1
                for group in ("stringify", "nulls", "mcv"):
                    for rule in rules.get(group, {}).get("rules", {}).get(table, []) or []:
                        max_index = max(max_index, int(rule["index"]))
                cols = max_index + 1
                rows = []
                for i in range(3):
                    row = [str(i + 7)] * cols
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

            for table in tables[:2]:
                self.assertEqual(
                    (py_dir / f"{table}.tbl").read_text(encoding="utf-8"),
                    (cpp_dir / f"{table}.tbl").read_text(encoding="utf-8"),
                )

    def test_cpp_respects_partition_label(self) -> None:
        _ensure_cpp_binary()
        from workload.dsdgen import stringify

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            py_dir = base / "py"
            cpp_dir = base / "cpp"
            py_dir.mkdir()
            cpp_dir.mkdir()

            rules_path = base / "rules.yml"
            stringify.export_rewrite_rules(
                rules_path,
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
            )
            rules = _load_rules(rules_path)
            table = next(iter((rules.get("nulls", {}).get("rules") or {}).keys()), None)
            if not table:
                self.skipTest("No null rules available for partition-label test.")

            max_index = -1
            for group in ("stringify", "nulls", "mcv"):
                for rule in rules.get(group, {}).get("rules", {}).get(table, []) or []:
                    max_index = max(max_index, int(rule["index"]))
            cols = max_index + 1

            row = ["123"] * cols
            filename = f"{table}_001.tbl"
            for target in (py_dir, cpp_dir):
                (target / filename).write_text("|".join(row) + "|\n", encoding="utf-8")

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

            self.assertEqual(
                (py_dir / filename).read_text(encoding="utf-8"),
                (cpp_dir / filename).read_text(encoding="utf-8"),
            )

    def test_cpp_skips_non_numeric_stringify(self) -> None:
        _ensure_cpp_binary()
        rules = {
            "stringify": {
                "enabled": True,
                "rules": {
                    "demo": [
                        {"index": 0, "prefix": "x", "pad_width": 4, "name": "demo_sk"},
                    ]
                },
            },
            "nulls": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
            "mcv": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            data_dir = base / "data"
            data_dir.mkdir()
            rules_path = base / "rules.yml"
            rules_path.write_text(yaml.safe_dump(rules, sort_keys=False), encoding="utf-8")
            target = data_dir / "demo.tbl"
            target.write_text("ABC|2|\n", encoding="utf-8")

            subprocess.run(
                [str(CPP_BIN), "--output-dir", str(data_dir), "--rules-file", str(rules_path)],
                check=True,
            )

            self.assertEqual("ABC|2|\n", target.read_text(encoding="utf-8"))

    def test_cpp_respects_stringify_max_workers_env(self) -> None:
        _ensure_cpp_binary()
        rules = {
            "stringify": {"enabled": False, "rules": {}},
            "nulls": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
            "mcv": {"enabled": False, "seed": 0, "null_marker": "", "rules": {}},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            data_dir = base / "data"
            data_dir.mkdir()
            rules_path = base / "rules.yml"
            rules_path.write_text(yaml.safe_dump(rules, sort_keys=False), encoding="utf-8")
            for i in range(3):
                (data_dir / f"demo_{i}.tbl").write_text("1|2|\n", encoding="utf-8")

            env = os.environ.copy()
            env["STRINGIFY_MAX_WORKERS"] = "2"
            subprocess.run(
                [str(CPP_BIN), "--output-dir", str(data_dir), "--rules-file", str(rules_path)],
                check=True,
                env=env,
            )

            for i in range(3):
                self.assertEqual("1|2|\n", (data_dir / f"demo_{i}.tbl").read_text(encoding="utf-8"))

    def test_python_api_cpp_backend_writes_manifest(self) -> None:
        _ensure_cpp_binary()
        from workload import stringification as stringification_cfg
        from workload.dsdgen import stringify

        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            (data_dir / "customer.dat").write_text("1|2|3|4|5|6|7|8|9|10|\n", encoding="utf-8")

            files, rows = stringify.rewrite_tbl_directory(
                data_dir,
                backend="cpp",
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
                max_workers=2,
            )

            self.assertEqual(1, files)
            self.assertEqual(1, rows)
            manifest = json.loads(
                (data_dir / stringification_cfg.DATA_MANIFEST_NAME).read_text(encoding="utf-8")
            )
            self.assertEqual("cpp", manifest.get("rewrite_backend"))
            self.assertEqual(1, manifest.get("files_rewritten"))
            self.assertEqual(1, manifest.get("rows_rewritten"))

    def test_cpp_matches_python_str_plus_amplification(self) -> None:
        _ensure_cpp_binary()
        from workload.dsdgen import stringify

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            py_dir = base / "py"
            cpp_dir = base / "cpp"
            py_dir.mkdir()
            cpp_dir.mkdir()

            row = "1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|\n"
            for target in (py_dir, cpp_dir):
                (target / "customer.dat").write_text(row, encoding="utf-8")

            rules_path = base / "rules.yml"
            stringify.export_rewrite_rules(
                rules_path,
                stringification_level=12,
                allow_extended_levels=True,
                str_plus_enabled=True,
                str_plus_max_level=20,
                str_plus_pad_step=2,
                str_plus_separator="_",
                str_plus_marker="X",
                enable_nulls=False,
                enable_mcv=False,
            )

            stringify.rewrite_tbl_directory(
                py_dir,
                backend="python",
                stringification_level=12,
                allow_extended_levels=True,
                str_plus_enabled=True,
                str_plus_max_level=20,
                str_plus_pad_step=2,
                str_plus_separator="_",
                str_plus_marker="X",
                enable_nulls=False,
                enable_mcv=False,
            )
            subprocess.run(
                [str(CPP_BIN), "--output-dir", str(cpp_dir), "--rules-file", str(rules_path)],
                check=True,
            )

            py_text = (py_dir / "customer.dat").read_text(encoding="utf-8")
            cpp_text = (cpp_dir / "customer.dat").read_text(encoding="utf-8")
            self.assertEqual(py_text, cpp_text)
            self.assertIn("_XXXX", cpp_text)

    def test_cpp_python_equivalence_all_rule_tables(self) -> None:
        _ensure_cpp_binary()
        from workload.dsdgen import stringify

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            py_dir = base / "py"
            cpp_dir = base / "cpp"
            py_dir.mkdir()
            cpp_dir.mkdir()

            rules_path = base / "rules.yml"
            stringify.export_rewrite_rules(
                rules_path,
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
            )
            rules = _load_rules(rules_path)
            all_tables = set()
            for group in ("stringify", "nulls", "mcv"):
                all_tables.update((rules.get(group, {}).get("rules") or {}).keys())
            if not all_tables:
                self.skipTest("No tables with rewrite rules.")

            for table in sorted(all_tables):
                max_index = -1
                for group in ("stringify", "nulls", "mcv"):
                    group_rules = rules.get(group, {}).get("rules", {}).get(table, []) or []
                    for rule in group_rules:
                        max_index = max(max_index, int(rule["index"]))
                if max_index < 0:
                    continue
                cols = max_index + 1
                rows = []
                for i in range(25):
                    values = [str(((i + 3) * (j + 7)) % 100000 + 1) for j in range(cols)]
                    rows.append("|".join(values) + "|\n")
                payload = "".join(rows)
                (py_dir / f"{table}.dat").write_text(payload, encoding="utf-8")
                (cpp_dir / f"{table}.dat").write_text(payload, encoding="utf-8")

            stringify.rewrite_tbl_directory(
                py_dir,
                backend="python",
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
                max_workers=4,
            )
            stringify.rewrite_tbl_directory(
                cpp_dir,
                backend="cpp",
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
                max_workers=8,
            )

            for table in sorted(all_tables):
                py_file = py_dir / f"{table}.dat"
                cpp_file = cpp_dir / f"{table}.dat"
                if not py_file.exists() and not cpp_file.exists():
                    continue
                self.assertEqual(py_file.read_text(encoding="utf-8"), cpp_file.read_text(encoding="utf-8"))

    def test_cpp_is_deterministic_across_worker_counts(self) -> None:
        _ensure_cpp_binary()
        from workload.dsdgen import stringify

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            src = base / "src"
            w1 = base / "w1"
            w8 = base / "w8"
            src.mkdir()
            w1.mkdir()
            w8.mkdir()

            for idx in range(4):
                rows = []
                for i in range(200):
                    rows.append(f"{i + 1}|{(i * 13 + idx) % 1000 + 1}|{(i * 17) % 1000 + 1}|\n")
                (src / f"customer_{idx}.dat").write_text("".join(rows), encoding="utf-8")

            for target in (w1, w8):
                for src_file in src.glob("*.dat"):
                    shutil.copy2(src_file, target / src_file.name)

            stringify.rewrite_tbl_directory(
                w1,
                backend="cpp",
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
                max_workers=1,
            )
            stringify.rewrite_tbl_directory(
                w8,
                backend="cpp",
                stringification_level=10,
                enable_nulls=True,
                enable_mcv=True,
                max_workers=8,
            )

            for file_w1 in sorted(w1.glob("*.dat")):
                file_w8 = w8 / file_w1.name
                self.assertEqual(file_w1.read_text(encoding="utf-8"), file_w8.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()

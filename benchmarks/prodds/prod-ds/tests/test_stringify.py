import os
import sys
import random
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsdgen import config as config_mod
from workload.dsdgen import stringify
from workload import stringification as stringification_cfg


class StringifyTests(unittest.TestCase):
    def test_stringify_row_rewrites_keys_and_preserves_non_keys(self) -> None:
        row = ["42", "payload", ""]
        rules = {
            "demo": {
                "demo_sk": {"index": 0, "prefix": "x", "pad_width": 5},
            }
        }

        rewritten = stringify.stringify_row(row[:], "demo", rules)

        self.assertEqual("x00042", rewritten[0])
        self.assertEqual("payload", rewritten[1])
        self.assertEqual("", rewritten[2])

    def test_prefix_defaults_and_custom_values(self) -> None:
        settings = config_mod.stringify_rules()
        prefixes = settings["prefixes"]

        # Custom prefix defined in the default config.
        self.assertEqual("c", prefixes["customer"])
        customer_rules = {
            "customer": {
                "customer_sk": {
                    "index": 0,
                    "prefix": prefixes["customer"],
                    "pad_width": settings["pad_width"],
                }
            }
        }
        customer_row = ["7", "keep"]
        self.assertEqual(
            "c00000007",
            stringify.stringify_row(customer_row[:], "customer", customer_rules)[0],
        )

        # Table without explicit configuration should fall back to first character.
        fallback_prefix = prefixes["store"]
        self.assertEqual("s", fallback_prefix)
        store_rules = {
            "store": {
                "store_sk": {
                    "index": 0,
                    "prefix": fallback_prefix,
                    "pad_width": settings["pad_width"],
                }
            }
        }
        store_row = ["11", "keep"]
        self.assertTrue(
            stringify.stringify_row(store_row[:], "store", store_rules)[0].startswith("s")
        )

    @pytest.mark.needs_tpcds_tools
    def test_extra_stringified_attributes_use_custom_prefixes(self) -> None:
        rules = stringify.build_rules()
        targets = {
            "item": {
                "i_manufact_id": "10",
            },
            "store_sales": {
                "ss_sold_time_sk": "13",
            },
            "catalog_sales": {
                "cs_sold_time_sk": "15",
            },
            "web_returns": {
                "wr_returned_time_sk": "17",
            },
        }

        for table_name, column_values in targets.items():
            self.assertIn(table_name, rules)
            table_rules = rules[table_name]
            for column_name in column_values:
                self.assertIn(column_name, table_rules)

            row_len = max(table_rules[col]["index"] for col in column_values) + 1
            row = ["noop"] * row_len
            for column_name, raw_value in column_values.items():
                row[table_rules[column_name]["index"]] = raw_value

            stringify.stringify_row(row, table_name, rules)

            for column_name, raw_value in column_values.items():
                cfg = table_rules[column_name]
                expected = stringify.stringify_value(raw_value, cfg["prefix"], cfg["pad_width"])
                self.assertEqual(expected, row[cfg["index"]])

    @pytest.mark.needs_tpcds_tools
    def test_foreign_key_domains_share_prefixes_across_tables(self) -> None:
        cfg = stringification_cfg.build_stringification_config(level=10)
        rules = stringify.build_rules(cfg)

        self.assertEqual("c", rules["customer"]["c_customer_sk"]["prefix"])
        self.assertEqual("c", rules["store_sales"]["ss_customer_sk"]["prefix"])
        self.assertEqual("c", rules["store_returns"]["sr_customer_sk"]["prefix"])

        self.assertEqual("i", rules["item"]["i_item_sk"]["prefix"])
        self.assertEqual("i", rules["store_sales"]["ss_item_sk"]["prefix"])
        self.assertEqual("i", rules["web_sales"]["ws_item_sk"]["prefix"])

        self.assertEqual("D_", rules["date_dim"]["d_date_sk"]["prefix"])
        self.assertEqual("D_", rules["store_sales"]["ss_sold_date_sk"]["prefix"])
        self.assertEqual("D_", rules["catalog_sales"]["cs_ship_date_sk"]["prefix"])

        self.assertEqual("T_", rules["time_dim"]["t_time_sk"]["prefix"])
        self.assertEqual("T_", rules["store_sales"]["ss_sold_time_sk"]["prefix"])
        self.assertEqual("T_", rules["catalog_returns"]["cr_returned_time_sk"]["prefix"])

    def test_process_tbl_counts_rows(self) -> None:
        rules = {
            "demo": {
                "demo_sk": {"index": 0, "prefix": "p", "pad_width": 4},
                "demo_id": {"index": 1, "prefix": "d", "pad_width": 3},
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            infile = Path(tmpdir) / "demo.tbl"
            outfile = Path(tmpdir) / "demo_out.tbl"
            infile.write_text("1|2|\n3|4|\n", encoding="utf-8")

            rows = stringify.process_tbl(infile, outfile, "demo", rules)

            self.assertEqual(2, rows)
            self.assertEqual("p0001|d002|\np0003|d004|\n", outfile.read_text())

    def test_process_tbl_skips_string_keys_without_modifying_rows(self) -> None:
        rules = {"demo": {"demo_sk": {"index": 0, "prefix": "x", "pad_width": 5}}}

        with tempfile.TemporaryDirectory() as tmpdir:
            infile = Path(tmpdir) / "demo.tbl"
            outfile = Path(tmpdir) / "demo_out.tbl"
            infile.write_text("AAAAAAAABAAAAAAA|2|\n", encoding="utf-8")

            rows = stringify.process_tbl(infile, outfile, "demo", rules)

            self.assertEqual(1, rows)
            self.assertEqual(
                "AAAAAAAABAAAAAAA|2|\n", outfile.read_text(), "string-based keys should remain intact"
            )

    def test_table_name_detection_handles_partition_suffixes(self) -> None:
        helper = stringify._table_name_from_filename  # type: ignore[attr-defined]
        self.assertEqual("store_sales", helper("store_sales_001.tbl"))
        self.assertEqual("inventory", helper("inventory.tbl"))
        self.assertIsNone(helper("123.tbl"))

    def test_stringify_value_preserves_preexisting_string_keys(self) -> None:
        source = "AAAAAAAABAAAAAAA"
        result = stringify.stringify_value(source, "p", 4)
        self.assertEqual(source, result)

    @pytest.mark.needs_tpcds_tools
    def test_rewrite_tbl_directory_only_modifies_configured_tables(self) -> None:
        rules = {
            "demo": {
                "demo_sk": {"index": 0, "prefix": "q", "pad_width": 3},
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            target = out_dir / "demo.tbl"
            skip = out_dir / "other.tbl"
            target.write_text("1|A|\n", encoding="utf-8")
            skip.write_text("2|B|\n", encoding="utf-8")

            with mock.patch.object(stringify, "build_rules", return_value=rules):
                files, rows = stringify.rewrite_tbl_directory(out_dir, min_ndv_for_injection=0)

            self.assertEqual(1, files)
            self.assertEqual(1, rows)
            self.assertEqual("q001|A|\n", target.read_text())
            self.assertEqual("2|B|\n", skip.read_text())

    @pytest.mark.needs_tpcds_tools
    def test_rewrite_tbl_directory_handles_dat_files(self) -> None:
        rules = {
            "demo": {
                "demo_sk": {"index": 0, "prefix": "q", "pad_width": 3},
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            target = out_dir / "demo.dat"
            skip = out_dir / "other.dat"
            target.write_text("1|A|\n", encoding="utf-8")
            skip.write_text("2|B|\n", encoding="utf-8")

            with mock.patch.object(stringify, "build_rules", return_value=rules):
                files, rows = stringify.rewrite_tbl_directory(out_dir, min_ndv_for_injection=0)

            self.assertEqual(1, files)
            self.assertEqual(1, rows)
            self.assertEqual("q001|A|\n", target.read_text())
            self.assertEqual("2|B|\n", skip.read_text())

    def test_resolve_max_workers_honors_env_override(self) -> None:
        env_key = stringify.WORKER_ENV_VAR  # type: ignore[attr-defined]
        with mock.patch.dict(os.environ, {env_key: "3"}):
            self.assertEqual(3, stringify._resolve_max_workers(None, 5))  # type: ignore[attr-defined]

        with mock.patch.dict(os.environ, {env_key: "invalid"}):
            derived = stringify._resolve_max_workers(None, 2)  # type: ignore[attr-defined]
            self.assertGreaterEqual(derived, 1)

    def test_process_tbl_preserves_bytes_with_invalid_utf8(self) -> None:
        rules = {
            "demo": {
                "demo_sk": {"index": 0, "prefix": "z", "pad_width": 4},
            }
        }

        latin1_bytes = b"1|caf\xe9|\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            infile = Path(tmpdir) / "demo.tbl"
            outfile = Path(tmpdir) / "demo_out.tbl"
            infile.write_bytes(latin1_bytes)

            rows = stringify.process_tbl(infile, outfile, "demo", rules)

            self.assertEqual(1, rows)
            self.assertEqual(b"z0001|caf\xe9|\n", outfile.read_bytes())

    def test_stringify_row_randomized_rules_and_rows(self) -> None:
        rng = random.Random(20240)
        for _ in range(200):
            row_len = rng.randint(1, 8)
            row = []
            for _ in range(row_len):
                choice = rng.choice(["int", "text", "blank", "nullish"])
                if choice == "int":
                    row.append(str(rng.randint(-5, 10**6)))
                elif choice == "text":
                    row.append(f"tok{rng.randint(0, 999)}")
                elif choice == "blank":
                    row.append("")
                else:
                    row.append("\\N")

            rules = {"fuzz": {}}
            sample_indices = list(range(row_len + 2))
            rng.shuffle(sample_indices)
            for cfg_idx in sample_indices[: min(3, len(sample_indices))]:
                rules["fuzz"][f"c{cfg_idx}"] = {
                    "index": cfg_idx,
                    "prefix": chr(ord("a") + (cfg_idx % 26)),
                    "pad_width": rng.randint(1, 6),
                }

            original = list(row)
            rewritten = stringify.stringify_row(row, "fuzz", rules)
            self.assertIs(rewritten, row, "stringify_row should modify rows in place and return the same list")

            for cfg in rules["fuzz"].values():
                idx = cfg["index"]
                if idx is None or idx >= len(original):
                    continue
                original_val = original[idx]
                if original_val in ("", "\\N"):
                    self.assertEqual(original_val, rewritten[idx])
                else:
                    expected = stringify.stringify_value(original_val, cfg["prefix"], cfg["pad_width"])
                    self.assertEqual(expected, rewritten[idx])

            untouched = set(range(len(original))) - {
                cfg["index"]
                for cfg in rules["fuzz"].values()
                if cfg.get("index") is not None and cfg["index"] < len(original)
            }
            for idx in untouched:
                self.assertEqual(original[idx], rewritten[idx])

    def test_table_name_detection_modifiers_and_partitions(self) -> None:
        helper = stringify._table_name_from_filename  # type: ignore[attr-defined]
        rng = random.Random(5150)
        for _ in range(50):
            base = rng.choice(["store_sales", "inventory", "customer_address", "web_sales"]).upper()
            suffix = "_".join(str(rng.randint(0, 999)).zfill(rng.randint(1, 3)) for _ in range(rng.randint(1, 3)))
            filename = f"{base}_{suffix}.tbl"
            expected = base.lower()
            self.assertEqual(expected, helper(filename))

    def test_resolve_max_workers_randomized_caps(self) -> None:
        env_key = stringify.WORKER_ENV_VAR  # type: ignore[attr-defined]
        rng = random.Random(777)
        with mock.patch.dict(os.environ, {env_key: "8"}):
            for task_count in range(0, 10):
                requested = rng.choice([None, -1, 0, rng.randint(1, 16)])
                derived = stringify._resolve_max_workers(requested, task_count)  # type: ignore[attr-defined]
                self.assertGreaterEqual(derived, 1)
                if task_count > 0:
                    self.assertLessEqual(derived, task_count)


if __name__ == "__main__":
    unittest.main()

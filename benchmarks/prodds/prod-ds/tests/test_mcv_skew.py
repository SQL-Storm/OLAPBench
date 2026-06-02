import shutil
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsdgen import stringify
from workload.dsdgen.config import mcv_skew_rules, MCV_TIER_ALIASES


@pytest.mark.needs_tpcds_tools
class MCVSkewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = stringify._schema_cache()  # type: ignore[attr-defined]

    def _make_rows(self, table: str, row_count: int) -> tuple[list[str], list[str]]:
        columns = self.schema[table]["columns"]
        rows = []
        for i in range(row_count):
            values = [str(i + idx + 1) for idx in range(len(columns))]
            rows.append("|".join(values) + "|\n")
        return columns, rows

    def _pick_table_with_mcv_rules(self, cfg: dict) -> str:
        cfg.setdefault("min_ndv_for_injection", 0)
        injector = stringify.MCVInjector(self.schema, cfg)  # type: ignore[attr-defined]
        tables = list(injector.tables_with_rules())
        if not tables:
            self.fail("No eligible tables found for MCV injection in schema")
        return tables[0]

    def test_mcv_deterministic_across_workers(self) -> None:
        overrides = {
            "column_selection_fraction": 1.0,
            "top20_buckets": [{"weight": 1.0, "min": 0.8, "max": 0.8}],
            "r_buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
            "seed": 1234,
        }
        table = self._pick_table_with_mcv_rules(overrides)
        _, rows = self._make_rows(table, 32)

        with tempfile.TemporaryDirectory() as tmpdir:
            base_dir = Path(tmpdir) / "base"
            base_dir.mkdir()
            (base_dir / f"{table}.tbl").write_text("".join(rows), encoding="utf-8")

            run_one = Path(tmpdir) / "run_one"
            run_two = Path(tmpdir) / "run_two"
            shutil.copytree(base_dir, run_one)
            shutil.copytree(base_dir, run_two)

            stringify.rewrite_tbl_directory(
                run_one,
                max_workers=1,
                enable_stringify=False,
                enable_nulls=False,
                enable_mcv=True,
                mcv_overrides=overrides,
                min_ndv_for_injection=0,
            )
            stringify.rewrite_tbl_directory(
                run_two,
                max_workers=4,
                enable_stringify=False,
                enable_nulls=False,
                enable_mcv=True,
                mcv_overrides=overrides,
                min_ndv_for_injection=0,
            )

            self.assertEqual(
                (run_one / f"{table}.tbl").read_bytes(), (run_two / f"{table}.tbl").read_bytes()
            )

    def test_disabling_mcv_matches_null_only_baseline(self) -> None:
        null_overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.3, "max": 0.3}],
        }
        mcv_overrides = {
            "column_selection_fraction": 0.0,
            "top20_buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
            "r_buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
        }
        table = "store_sales"
        _, rows = self._make_rows(table, 24)

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / "base.tbl"
            base.write_text("".join(rows), encoding="utf-8")

            without_mcv = Path(tmpdir) / "without_mcv.tbl"
            with_mcv_disabled = Path(tmpdir) / "with_mcv_disabled.tbl"
            shutil.copy(base, without_mcv)
            shutil.copy(base, with_mcv_disabled)

            stringify.rewrite_tbl_directory(
                without_mcv.parent,
                max_workers=2,
                enable_stringify=False,
                enable_nulls=True,
                null_marker="\\N",
                null_overrides=null_overrides,
                enable_mcv=False,
                min_ndv_for_injection=0,
            )
            stringify.rewrite_tbl_directory(
                with_mcv_disabled.parent,
                max_workers=2,
                enable_stringify=False,
                enable_nulls=True,
                null_marker="\\N",
                null_overrides=null_overrides,
                enable_mcv=True,
                mcv_overrides=mcv_overrides,
                min_ndv_for_injection=0,
            )

            self.assertEqual(without_mcv.read_bytes(), with_mcv_disabled.read_bytes())

    def test_mcv_preserves_keys_and_not_nulls(self) -> None:
        overrides = {
            "column_selection_fraction": 1.0,
            "top20_buckets": [{"weight": 1.0, "min": 0.9, "max": 0.9}],
            "r_buckets": [{"weight": 1.0, "min": 0.9, "max": 0.9}],
        }
        table = "catalog_sales"
        columns, rows = self._make_rows(table, 10)
        meta = self.schema[table]
        key_indices = {c["index"] for c in meta.get("key_like_columns", [])}
        key_indices.update({c["index"] for c in meta.get("varchar_keys", [])})
        not_null_indices = {c["index"] for c in meta.get("not_null_columns", [])}

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / f"{table}.tbl"
            path.write_text("".join(rows), encoding="utf-8")

            stringify.rewrite_tbl_directory(
                path.parent,
                max_workers=2,
                enable_stringify=False,
                enable_nulls=False,
                enable_mcv=True,
                mcv_overrides=overrides,
                min_ndv_for_injection=0,
            )

            rewritten = path.read_text(encoding="utf-8").splitlines()
            for original, line in zip(rows, rewritten):
                original_fields = original.split("|")[:-1]
                fields = line.split("|")[:-1]
                self.assertEqual(len(columns), len(fields))
                for idx in key_indices.union(not_null_indices):
                    self.assertEqual(
                        original_fields[idx],
                        fields[idx],
                        f"MCV skew should not modify key/NOT NULL column {idx}",
                    )

    def test_mcv_increases_top_counts(self) -> None:
        overrides = {
            "column_selection_fraction": 1.0,
            "top20_buckets": [{"weight": 1.0, "min": 0.8, "max": 0.8}],
            "r_buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
            "seed": 42,
        }
        table = self._pick_table_with_mcv_rules(overrides)
        columns, rows = self._make_rows(table, 200)

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / f"{table}.tbl"
            path.write_text("".join(rows), encoding="utf-8")

            stringify.rewrite_tbl_directory(
                path.parent,
                max_workers=2,
                enable_stringify=False,
                enable_nulls=False,
                enable_mcv=True,
                mcv_overrides=overrides,
                min_ndv_for_injection=0,
            )

            rewritten = path.read_text(encoding="utf-8").splitlines()
            # Inspect the first column that has an MCV rule.
            overrides["min_ndv_for_injection"] = 0
            injector = stringify.MCVInjector(self.schema, overrides)  # type: ignore[attr-defined]
            rule = injector.rules[table][0]
            counts = Counter()
            for line in rewritten:
                fields = line.split("|")[:-1]
                counts[fields[rule.index]] += 1

            non_null = sum(counts.values())
            most_common = counts.most_common(20)
            top1_frac = most_common[0][1] / non_null if most_common else 0.0
            top20_frac = sum(c for _, c in most_common) / non_null if most_common else 0.0

            self.assertGreater(top1_frac, 0.25)
            self.assertGreater(top20_frac, 0.5)
            self.assertLess(top1_frac, 0.95)


@pytest.mark.needs_tpcds_tools
class MCVTierTests(unittest.TestCase):
    """Tests for low/medium/high MCV skew tier profiles."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = stringify._schema_cache()  # type: ignore[attr-defined]

    TIER_PROFILES = [
        ("mcv_low", 0.35),
        ("mcv_fleet_default", 0.70),
        ("mcv_high", 0.90),
    ]

    def test_tier_profiles_load_successfully(self) -> None:
        for profile_name, expected_fraction in self.TIER_PROFILES:
            with self.subTest(profile=profile_name):
                cfg = mcv_skew_rules(profile=profile_name)
                self.assertAlmostEqual(
                    cfg["column_selection_fraction"], expected_fraction, places=2
                )
                top20 = cfg["top20_buckets"]
                self.assertTrue(len(top20) >= 2, f"{profile_name}: need at least 2 top20 buckets")
                total_weight = sum(b["weight"] for b in top20)
                self.assertAlmostEqual(total_weight, 1.0, delta=0.001)
                r = cfg["r_buckets"]
                self.assertTrue(len(r) >= 2, f"{profile_name}: need at least 2 r_buckets")
                r_weight = sum(b["weight"] for b in r)
                self.assertAlmostEqual(r_weight, 1.0, delta=0.001)

    def test_tier_monotonicity(self) -> None:
        seed = 42
        avg_f20s = []
        n_selected = []
        for profile_name, _ in self.TIER_PROFILES:
            cfg = mcv_skew_rules(profile=profile_name)
            cfg["seed"] = seed
            cfg["min_ndv_for_injection"] = 0
            injector = stringify.MCVInjector(self.schema, cfg)  # type: ignore[attr-defined]
            all_f20 = []
            total_cols = 0
            for table_rules in injector.rules.values():
                for rule in table_rules:
                    all_f20.append(rule.f20)
                    total_cols += 1
            avg = sum(all_f20) / len(all_f20) if all_f20 else 0.0
            avg_f20s.append(avg)
            n_selected.append(total_cols)

        self.assertLess(avg_f20s[0], avg_f20s[1], "avg_f20(low) should be < avg_f20(medium)")
        self.assertLess(avg_f20s[1], avg_f20s[2], "avg_f20(medium) should be < avg_f20(high)")
        self.assertLessEqual(n_selected[0], n_selected[1])
        self.assertLessEqual(n_selected[1], n_selected[2])

    def test_r_buckets_parity(self) -> None:
        configs = [mcv_skew_rules(profile=name) for name, _ in self.TIER_PROFILES]
        reference = configs[0]["r_buckets"]
        for cfg in configs[1:]:
            self.assertEqual(len(cfg["r_buckets"]), len(reference))
            for ref_b, cur_b in zip(reference, cfg["r_buckets"]):
                self.assertAlmostEqual(ref_b["weight"], cur_b["weight"], places=6)
                self.assertAlmostEqual(ref_b["min"], cur_b["min"], places=6)
                self.assertAlmostEqual(ref_b["max"], cur_b["max"], places=6)

    def test_alias_resolution(self) -> None:
        for alias, canonical in MCV_TIER_ALIASES.items():
            with self.subTest(alias=alias):
                via_alias = mcv_skew_rules(profile=alias)
                via_canonical = mcv_skew_rules(profile=canonical)
                self.assertEqual(via_alias["profile"], via_canonical["profile"])
                self.assertAlmostEqual(
                    via_alias["column_selection_fraction"],
                    via_canonical["column_selection_fraction"],
                    places=6,
                )
                self.assertEqual(len(via_alias["top20_buckets"]), len(via_canonical["top20_buckets"]))

    def test_exclusion_parity(self) -> None:
        configs = [mcv_skew_rules(profile=name) for name, _ in self.TIER_PROFILES]
        ref_tables = set(configs[0].get("exclude_tables", []))
        ref_columns = set(configs[0].get("exclude_qualified_columns", []))
        for cfg in configs[1:]:
            self.assertEqual(set(cfg.get("exclude_tables", [])), ref_tables)
            self.assertEqual(set(cfg.get("exclude_qualified_columns", [])), ref_columns)


if __name__ == "__main__":
    unittest.main()

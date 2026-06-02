import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsdgen import stringify
from workload.dsdgen.config import null_skew_rules, NULL_TIER_ALIASES


@pytest.mark.needs_tpcds_tools
class NullSkewTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = stringify._schema_cache()  # type: ignore[attr-defined]
        cls.default_rules = stringify.null_skew_rules()
        cls.default_rules["min_ndv_for_injection"] = 0

    def _make_rows(self, table: str, row_count: int) -> tuple[list[str], list[str]]:
        columns = self.schema[table]["columns"]
        rows = []
        for i in range(row_count):
            values = [str(i + idx + 1) for idx in range(len(columns))]
            rows.append("|".join(values) + "|\n")
        return columns, rows

    def test_null_skew_deterministic_across_workers(self) -> None:
        table = "date_dim"
        _, rows = self._make_rows(table, 12)

        overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.5, "max": 0.5}],
        }

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
                enable_nulls=True,
                null_marker="\\N",
                null_seed=987,
                null_overrides=overrides,
                min_ndv_for_injection=0,
            )
            stringify.rewrite_tbl_directory(
                run_two,
                max_workers=4,
                enable_stringify=False,
                enable_nulls=True,
                null_marker="\\N",
                null_seed=987,
                null_overrides=overrides,
                min_ndv_for_injection=0,
            )

            self.assertEqual(
                (run_one / f"{table}.tbl").read_bytes(), (run_two / f"{table}.tbl").read_bytes()
            )

    def test_null_skew_preserves_key_columns(self) -> None:
        table = "store_sales"
        columns, rows = self._make_rows(table, 20)
        key_indices = {col["index"] for col in self.schema[table].get("key_like_columns", [])}

        overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.9, "max": 0.9}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            (out_dir / f"{table}.tbl").write_text("".join(rows), encoding="utf-8")

            stringify.rewrite_tbl_directory(
                out_dir,
                max_workers=2,
                enable_stringify=False,
                enable_nulls=True,
                null_marker="\\N",
                null_overrides=overrides,
                min_ndv_for_injection=0,
            )

            rewritten = (out_dir / f"{table}.tbl").read_text(encoding="utf-8").splitlines()
            null_seen_non_key = False
            for line in rewritten:
                fields = line.split("|")[:-1]
                self.assertEqual(len(columns), len(fields))
                for idx in key_indices:
                    self.assertNotEqual("\\N", fields[idx], "PK/FK-like columns must never be NULL")
                if any(fields[idx] == "\\N" for idx in range(len(fields)) if idx not in key_indices):
                    null_seen_non_key = True

            self.assertTrue(null_seen_non_key, "Expected NULL injection on at least one non-key column")

    def test_null_probability_shape_tracks_realworld_quantiles(self) -> None:
        injector = stringify.NullInjector(  # type: ignore[attr-defined]
            self.schema, self.default_rules
        )
        per_column_probs = []
        for table_name, meta in self.schema.items():
            columns = meta["columns"]
            rule_map = {rule.name: rule.probability for rule in injector.rules.get(table_name, [])}
            for col in columns:
                per_column_probs.append(rule_map.get(col, 0.0))

        self.assertGreater(len(per_column_probs), 50, "Schema should expose enough columns for shape checks")
        sorted_probs = sorted(per_column_probs)

        def quantile(values: list[float], q: float) -> float:
            if not values:
                return 0.0
            q = max(0.0, min(1.0, q))
            idx = int(q * (len(values) - 1))
            return values[idx]

        qmap = {
            "q50": quantile(sorted_probs, 0.50),
            "q75": quantile(sorted_probs, 0.75),
            "q80": quantile(sorted_probs, 0.80),
            "q85": quantile(sorted_probs, 0.85),
            "q90": quantile(sorted_probs, 0.90),
            "q95": quantile(sorted_probs, 0.95),
            "q97": quantile(sorted_probs, 0.97),
            "q99": quantile(sorted_probs, 0.99),
            "max": max(sorted_probs),
        }

        fixture_path = Path(__file__).resolve().parent / "fixtures" / "realworld_null_quantiles.json"
        target = json.load(fixture_path.open("r", encoding="utf-8"))

        tolerances = {
            "q50": 0.01,
            "q75": 0.01,
            "q80": 0.05,
            "q85": 0.06,
            "q90": 0.05,
            "q95": 0.08,
            "q97": 0.08,
            "q99": 0.10,
            "max": 0.05,
        }

        for key, target_val in target.items():
            if key not in qmap:
                continue
            observed = qmap[key]
            tol = tolerances.get(key, 0.05)
            self.assertLessEqual(abs(observed - target_val), tol, f"{key} deviates from target by > {tol}")

    def test_selection_fraction_calibrates_to_overall_column_share(self) -> None:
        cfg = {
            "column_selection_fraction": 0.2,
            "selection_fraction_scope": "overall",
            "buckets": [{"weight": 1.0, "min": 0.1, "max": 0.1}],
            "min_ndv_for_injection": 0,
        }
        injector = stringify.NullInjector(self.schema, cfg)  # type: ignore[attr-defined]
        eligible_fraction = injector.eligible_fraction
        if eligible_fraction <= 0:
            self.assertEqual(0.0, injector.selection_fraction)
            return

        expected = min(1.0, 0.2 / eligible_fraction)
        self.assertAlmostEqual(expected, injector.selection_fraction, places=6)

    def test_rewrite_integration_smoke(self) -> None:
        tables = {"warehouse": 4, "store_sales": 3}
        overrides = {
            "column_selection_fraction": 1.0,
            "buckets": [{"weight": 1.0, "min": 0.4, "max": 0.4}],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = Path(tmpdir)
            for table, row_count in tables.items():
                _, rows = self._make_rows(table, row_count)
                (out_dir / f"{table}.tbl").write_text("".join(rows), encoding="utf-8")

            files, total_rows = stringify.rewrite_tbl_directory(
                out_dir,
                max_workers=2,
                enable_stringify=True,
                enable_nulls=True,
                null_marker="\\N",
                null_overrides=overrides,
                min_ndv_for_injection=0,
            )

            self.assertEqual(len(tables), files)
            self.assertEqual(sum(tables.values()), total_rows)

            for table, expected_rows in tables.items():
                path = out_dir / f"{table}.tbl"
                self.assertTrue(path.exists())
                lines = path.read_text(encoding="utf-8").splitlines()
                self.assertEqual(expected_rows, len(lines))

                columns = self.schema[table]["columns"]
                nulls_seen = 0
                for line in lines:
                    fields = line.split("|")[:-1]
                    self.assertEqual(len(columns), len(fields))
                    nulls_seen += sum(1 for value in fields if value == "\\N")

                    if table == "warehouse":
                        key_index = columns.index("w_warehouse_sk")
                        self.assertTrue(
                            fields[key_index].startswith("w"),
                            "stringified surrogate keys should retain their prefix",
                        )

                self.assertGreater(nulls_seen, 0, "Integration rewrite should inject NULL markers")


@pytest.mark.needs_tpcds_tools
class NullTierTests(unittest.TestCase):
    """Tests for low/medium/high null sparsity tier profiles."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = stringify._schema_cache()  # type: ignore[attr-defined]

    TIER_PROFILES = [
        ("null_low", 0.12),
        ("fleet_realworld_final", 0.24),
        ("null_high", 0.40),
    ]

    def test_tier_profiles_load_successfully(self) -> None:
        for profile_name, expected_fraction in self.TIER_PROFILES:
            with self.subTest(profile=profile_name):
                cfg = null_skew_rules(profile=profile_name)
                self.assertEqual(cfg["profile"], profile_name)
                self.assertAlmostEqual(
                    cfg["column_selection_fraction"], expected_fraction, places=4,
                    msg=f"{profile_name}: column_selection_fraction mismatch",
                )
                buckets = cfg.get("buckets", [])
                self.assertGreater(len(buckets), 0, f"{profile_name}: no buckets defined")
                raw_weight_sum = sum(b["weight"] for b in buckets)
                self.assertGreater(
                    raw_weight_sum, 0.0,
                    msg=f"{profile_name}: bucket weights sum to zero",
                )
                # After normalization (as the NullInjector does), weights must sum to 1.0.
                normalized_sum = sum(b["weight"] / raw_weight_sum for b in buckets)
                self.assertAlmostEqual(
                    normalized_sum, 1.0, places=3,
                    msg=f"{profile_name}: normalized bucket weights sum to {normalized_sum}, expected 1.0",
                )

    def test_tier_monotonicity(self) -> None:
        avg_probs = []
        selected_counts = []

        for profile_name, _ in self.TIER_PROFILES:
            cfg = null_skew_rules(profile=profile_name)
            cfg["seed"] = 42
            cfg["min_ndv_for_injection"] = 0
            injector = stringify.NullInjector(self.schema, cfg)
            probs = []
            for table_name in injector.rules:
                for rule in injector.rules[table_name]:
                    probs.append(rule.probability)
            avg_prob = sum(probs) / len(probs) if probs else 0.0
            avg_probs.append(avg_prob)
            selected_counts.append(len(probs))

        self.assertLess(
            avg_probs[0], avg_probs[1],
            f"avg_prob(low)={avg_probs[0]:.4f} should be < avg_prob(medium)={avg_probs[1]:.4f}",
        )
        self.assertLess(
            avg_probs[1], avg_probs[2],
            f"avg_prob(medium)={avg_probs[1]:.4f} should be < avg_prob(high)={avg_probs[2]:.4f}",
        )
        self.assertLessEqual(
            selected_counts[0], selected_counts[1],
            f"n_selected(low)={selected_counts[0]} should be <= n_selected(medium)={selected_counts[1]}",
        )
        self.assertLessEqual(
            selected_counts[1], selected_counts[2],
            f"n_selected(medium)={selected_counts[1]} should be <= n_selected(high)={selected_counts[2]}",
        )

    def test_tier_alias_resolution(self) -> None:
        for alias, canonical in NULL_TIER_ALIASES.items():
            with self.subTest(alias=alias):
                via_alias = null_skew_rules(profile=alias)
                via_canonical = null_skew_rules(profile=canonical)
                self.assertEqual(
                    via_alias["column_selection_fraction"],
                    via_canonical["column_selection_fraction"],
                    f"Alias '{alias}' should resolve to same fraction as '{canonical}'",
                )
                self.assertEqual(
                    via_alias["buckets"],
                    via_canonical["buckets"],
                    f"Alias '{alias}' should resolve to same buckets as '{canonical}'",
                )
                self.assertEqual(via_alias["profile"], canonical)

    def test_tier_exclusion_parity(self) -> None:
        exclusion_sets = {}
        for profile_name, _ in self.TIER_PROFILES:
            cfg = null_skew_rules(profile=profile_name)
            exclude_tables = frozenset(cfg.get("exclude_tables", []))
            exclude_qualified = frozenset(cfg.get("exclude_qualified_columns", []))
            exclusion_sets[profile_name] = (exclude_tables, exclude_qualified)

        ref_tables, ref_qualified = exclusion_sets["fleet_realworld_final"]
        for profile_name, (tables, qualified) in exclusion_sets.items():
            with self.subTest(profile=profile_name):
                self.assertEqual(
                    tables, ref_tables,
                    f"{profile_name}: exclude_tables differs from fleet_realworld_final",
                )
                self.assertEqual(
                    qualified, ref_qualified,
                    f"{profile_name}: exclude_qualified_columns differs from fleet_realworld_final",
                )


if __name__ == "__main__":
    unittest.main()

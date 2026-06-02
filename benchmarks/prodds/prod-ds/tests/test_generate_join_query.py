import random
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from workload.dsqgen import generate_join_query as gj

BASE_SALES_TEMPLATE = (REPO_ROOT / "workload" / "templates" / "base_sales.tpl").read_text()
BASE_RETURNS_TEMPLATE = (REPO_ROOT / "workload" / "templates" / "base_returns.tpl").read_text()
SCRIPT_PATH = REPO_ROOT / "workload" / "dsqgen" / "generate_join_query.py"
RETURNS_CFG_PATH = REPO_ROOT / "workload" / "config" / "returns.yml"

CALIBRATED_JOIN_OVERRIDES = {
    1: (0, 0),
    2: (0, 0),
    4: (0, 0),
    8: (0, 0),
    16: (1, 0),
    32: (2, 0),
    64: (5, 0),
    128: (11, 0),
    256: (22, 0),
    512: (22, 1),
    1024: (2, 30),
    2048: (5, 30),
}


class GenerateJoinQueryTests(unittest.TestCase):
    def _run_generator(
        self,
        cfg: dict,
        target: int,
        k: int | None = None,
        m: int | None = None,
        template_text: str | None = None,
    ) -> str:
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg_path = Path(tmpdir) / "cfg.yml"
            # Use yaml via the generator itself to avoid importing an extra dependency in tests.
            cfg_path.write_text(gj.yaml.safe_dump(cfg), encoding="utf-8")  # type: ignore[attr-defined]
            cmd = [sys.executable, str(SCRIPT_PATH), "--cfg", str(cfg_path), "--target", str(target)]
            if k is not None and m is not None:
                cmd.extend(["--k", str(k), "--m", str(m)])

            proc = subprocess.run(
                cmd,
                input=template_text if template_text is not None else BASE_SALES_TEMPLATE,
                text=True,
                capture_output=True,
                check=True,
            )
            return proc.stdout

    def test_solve_exact_32_prefers_k_only(self) -> None:
        k, m, J, aux = gj.solve_km_for_target_prefer_k(b=10, J_target=32, Kmax=30, Mmax=10)
        self.assertEqual((2, 0), (k, m))
        self.assertEqual(32, J)
        self.assertEqual("paper_exact", aux["strategy"])

    def test_solve_can_use_m_when_needed(self) -> None:
        b = 10
        Kmax = 2
        target = 120
        k, m, J, aux = gj.solve_km_for_target_prefer_k(b=b, J_target=target, Kmax=Kmax, Mmax=20)
        self.assertGreaterEqual(k, 0)
        self.assertGreater(m, 0)
        self.assertEqual(target, J)
        self.assertEqual("paper_exact", aux["strategy"])

    def test_lod_helpers(self) -> None:
        cte_sql = gj.lod_cte(1, "i_category", "sum", "sales")
        self.assertIn("LOD #1", cte_sql)
        self.assertIn("GROUP BY i_category", cte_sql)

        join_sql = gj.lod_join(2, "i_category")
        self.assertIn("LEFT JOIN", join_sql)
        self.assertIn("lod_02", join_sql)

        with self.assertRaises(ValueError):
            gj.lod_cte(1, "a,b", "sum", "sales")
        with self.assertRaises(ValueError):
            gj.lod_join(1, "a,b")

    def test_solver_randomized_bounds_and_best_fit_model(self) -> None:
        rng = random.Random(1337)
        for _ in range(250):
            b = rng.randint(1, 40)
            Kmax = rng.randint(1, 12)
            Mmax = rng.randint(1, 30)
            max_reachable = gj._effective_join_count(b, Kmax, Mmax)
            target = rng.randint(0, max_reachable + 20)

            k, m, J, aux = gj.solve_km_for_target_prefer_k(b=b, J_target=target, Kmax=Kmax, Mmax=Mmax)

            self.assertGreaterEqual(k, 0)
            self.assertLessEqual(k, Kmax)
            self.assertGreaterEqual(m, 0)
            self.assertLessEqual(m, Mmax)

            expected_J = gj._effective_join_count(b, k, m)
            self.assertEqual(expected_J, J)
            best_diff = min(
                abs(gj._effective_join_count(b, kk, mm) - target)
                for kk in range(Kmax + 1)
                for mm in range(Mmax + 1)
            )
            self.assertEqual(best_diff, abs(J - target))
            self.assertIn(aux["strategy"], {"paper_exact", "paper_floor", "paper_nearest"})

    def test_generator_end_to_end_zero_lods_and_filters(self) -> None:
        cfg = {
            "b": 3,
            "cap_joins": 50,
            "max_filts": 5,
            "measure": "metric",
            "agg_cycle": ["SUM"],
            "group_keys": ["g1", "g2"],
            "join_keys": ["k1"],
        }

        sql = self._run_generator(cfg, target=cfg["b"])

        self.assertIn("-- TARGET_JOINS=", sql)
        self.assertIn("-- AUTO-GENERATED JOIN SCALING (INLINE BASE BLOCKS)", sql)
        self.assertNotIn("lod_01", sql)
        self.assertNotIn("seg_001", sql)
        self.assertNotIn("[[", sql, "All placeholder markers must be replaced")
        self.assertRegex(sql, r"-- LOD_KEYS_USED:\s*\n")

    def test_generator_injects_expected_blocks_and_header(self) -> None:
        cfg = {
            "b": 4,
            "cap_joins": 1000,
            "max_filts": 5,
            "measure": "metric",
            "agg_cycle": ["SUM", "COUNT"],
            "group_keys": ["g1", "g2", "g3"],
            "join_keys": ["k1", "k2"],
        }
        target = 60
        k, m, expected_J, _ = gj.solve_km_for_target_prefer_k(
            b=cfg["b"], J_target=target, Kmax=len(cfg["group_keys"]), Mmax=cfg["max_filts"]
        )

        sql = self._run_generator(cfg, target=target)

        self.assertIn(f"-- LOD_KEYS_USED: {', '.join(cfg['group_keys'][:k])}", sql)
        self.assertEqual(m, sql.count("USING (k1, k2)"), "Unexpected number of segment joins")
        self.assertNotIn("[[", sql, "All placeholder markers must be replaced")
        self.assertIn(f"seg_{m:03d} USING (k1, k2)", sql)
        self.assertIn(f"1 AS seg_flag_{m:03d}", sql)
        header_match = re.search(r"EXPECTED_EFFECTIVE_JOINS=(\d+)", sql)
        self.assertIsNotNone(header_match, "Header should include expected join count")
        self.assertEqual(str(expected_J), header_match.group(1))

    def test_target_overrides_enable_distinct_low_levels(self) -> None:
        cfg = {
            "b": 12,
            "cap_joins": 10000,
            "max_filts": 10,
            "measure": "metric",
            "agg_cycle": ["SUM", "COUNT", "AVG"],
            "group_keys": [f"g{i}" for i in range(1, 20)],
            "join_keys": ["k1", "k2"],
            "target_overrides": {
                "1": {"k": 0, "m": 0},
                "2": {"k": 1, "m": 0},
                "4": {"k": 2, "m": 0},
                "8": {"k": 3, "m": 0},
                "16": {"k": 4, "m": 0},
            },
        }

        def _norm(sql: str) -> str:
            sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
            sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
            sql = re.sub(r"\s+", " ", sql).strip().lower()
            return sql

        sql_norm = {}
        for target in [1, 2, 4, 8, 16]:
            sql_norm[target] = _norm(self._run_generator(cfg, target=target))

        # Floor regression guard: low levels must not collapse to one normalized SQL shape.
        self.assertEqual(5, len(set(sql_norm.values())))

    def test_returns_config_includes_calibrated_join_overrides(self) -> None:
        cfg = gj.yaml.safe_load(RETURNS_CFG_PATH.read_text(encoding="utf-8"))  # type: ignore[attr-defined]
        self.assertIsInstance(cfg, dict)
        overrides = cfg.get("target_overrides")
        self.assertIsInstance(overrides, dict)
        for level, (expected_k, expected_m) in CALIBRATED_JOIN_OVERRIDES.items():
            raw = overrides.get(str(level), overrides.get(level))
            self.assertIsInstance(raw, dict, f"missing override for level {level}")
            self.assertEqual(expected_k, int(raw["k"]), f"k mismatch at level {level}")
            self.assertEqual(expected_m, int(raw["m"]), f"m mismatch at level {level}")

    def test_returns_overrides_are_applied_for_scaling_levels(self) -> None:
        cfg = gj.yaml.safe_load(RETURNS_CFG_PATH.read_text(encoding="utf-8"))  # type: ignore[attr-defined]
        self.assertIsInstance(cfg, dict)
        header_re = re.compile(
            r"--\s*STRATEGY=(?P<strategy>\S+)\s+chosen k=(?P<k>\d+)\s+m=(?P<m>\d+)\s+EXPECTED_EFFECTIVE_JOINS=(?P<j>\d+)",
            flags=re.I,
        )
        b_eff = int(cfg.get("b", 10))

        for level, (expected_k, expected_m) in CALIBRATED_JOIN_OVERRIDES.items():
            sql = self._run_generator(cfg, target=level, template_text=BASE_RETURNS_TEMPLATE)
            match = header_re.search(sql)
            self.assertIsNotNone(match, f"missing header for level {level}")
            self.assertEqual("target_override", match.group("strategy"))
            self.assertEqual(expected_k, int(match.group("k")))
            self.assertEqual(expected_m, int(match.group("m")))
            expected_eff = gj._effective_join_count(b_eff, expected_k, expected_m)
            self.assertEqual(expected_eff, int(match.group("j")))

    def test_returns_config_has_dimension_fingerprint_columns(self) -> None:
        cfg = gj.yaml.safe_load(RETURNS_CFG_PATH.read_text(encoding="utf-8"))  # type: ignore[attr-defined]
        self.assertIsInstance(cfg, dict)
        cols = cfg.get("dim_fingerprint_columns")
        self.assertIsInstance(cols, list)
        self.assertGreaterEqual(len(cols), 10)
        for col in cols:
            self.assertIsInstance(col, str)
            self.assertTrue(col.strip())

    def test_returns_sql_emits_fingerprint_and_removes_tautology_barrier(self) -> None:
        cfg = gj.yaml.safe_load(RETURNS_CFG_PATH.read_text(encoding="utf-8"))  # type: ignore[attr-defined]
        self.assertIsInstance(cfg, dict)
        # Use a level with m>0 to verify segment fingerprint propagation too.
        sql = self._run_generator(cfg, target=512, template_text=BASE_RETURNS_TEMPLATE)
        self.assertIn("AS dim_fp_", sql)
        self.assertIn("AS fp_lod_", sql)
        self.assertIn("AS seg_fp_", sql)
        self.assertNotIn("% 2147483629", sql)
        self.assertNotIn("= ((base_src.", sql)

    @unittest.skipUnless(shutil.which("duckdb"), "duckdb CLI not available")
    def test_manual_km_duckdb_explain_join_ops_monotone(self) -> None:
        cfg = {
            "b": 12,
            "cap_joins": 10000,
            "max_filts": 10,
            "measure": "sr_return_amt",
            "agg_cycle": ["SUM", "COUNT", "AVG", "MIN", "MAX"],
            "group_keys": [
                "i_brand",
                "i_category",
                "store_state",
                "store_name",
                "cust_state",
                "cd_education_status",
            ],
            "join_keys": ["sr_ticket_number", "sr_item_sk"],
        }
        km_by_target = {
            1: (0, 0),
            2: (1, 0),
            4: (2, 0),
            8: (3, 0),
            16: (4, 0),
        }

        ddl = """
        CREATE TABLE store_returns (
          sr_ticket_number BIGINT,
          sr_item_sk BIGINT,
          sr_return_amt DOUBLE,
          sr_returned_date_sk BIGINT,
          sr_return_time_sk BIGINT,
          sr_customer_sk BIGINT,
          sr_cdemo_sk BIGINT,
          sr_hdemo_sk BIGINT,
          sr_addr_sk BIGINT,
          sr_store_sk BIGINT,
          sr_reason_sk BIGINT
        );
        CREATE TABLE date_dim (d_date_sk BIGINT, d_year INT, d_moy INT, d_dow INT, d_week_seq INT);
        CREATE TABLE time_dim (t_time_sk BIGINT, t_hour INT, t_am_pm VARCHAR, t_shift VARCHAR);
        CREATE TABLE item (
          i_item_sk BIGINT,
          i_brand VARCHAR,
          i_category VARCHAR,
          i_class VARCHAR,
          i_manufact_id BIGINT,
          i_manufact VARCHAR,
          i_size VARCHAR,
          i_color VARCHAR
        );
        CREATE TABLE customer (c_customer_sk BIGINT, c_customer_id VARCHAR);
        CREATE TABLE customer_demographics (
          cd_demo_sk BIGINT,
          cd_education_status VARCHAR,
          cd_credit_rating VARCHAR,
          cd_dep_count INT
        );
        CREATE TABLE household_demographics (
          hd_demo_sk BIGINT,
          hd_buy_potential VARCHAR,
          hd_dep_count INT,
          hd_vehicle_count INT,
          hd_income_band_sk BIGINT
        );
        CREATE TABLE income_band (ib_income_band_sk BIGINT);
        CREATE TABLE customer_address (
          ca_address_sk BIGINT,
          ca_country VARCHAR,
          ca_state VARCHAR,
          ca_city VARCHAR,
          ca_county VARCHAR,
          ca_zip VARCHAR
        );
        CREATE TABLE store (
          s_store_sk BIGINT,
          s_store_name VARCHAR,
          s_state VARCHAR,
          s_company_name VARCHAR,
          s_division_name VARCHAR
        );
        CREATE TABLE reason (r_reason_sk BIGINT, r_reason_desc VARCHAR);
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            db = Path(tmpdir) / "join_monotone.duckdb"
            init = subprocess.run(["duckdb", str(db), "-c", ddl], text=True, capture_output=True, check=True)
            self.assertEqual(0, init.returncode)

            counts = []
            for target in [1, 2, 4, 8, 16]:
                k, m = km_by_target[target]
                sql = self._run_generator(
                    cfg,
                    target=target,
                    k=k,
                    m=m,
                    template_text=BASE_RETURNS_TEMPLATE,
                )
                p = subprocess.run(
                    ["duckdb", str(db), "-c", f"EXPLAIN {sql}"],
                    text=True,
                    capture_output=True,
                    check=True,
                )
                join_ops = p.stdout.count("HASH_JOIN") + p.stdout.count("NESTED_LOOP_JOIN") + p.stdout.count("MERGE_JOIN")
                counts.append(join_ops)

            self.assertEqual(sorted(counts), counts, f"Expected monotone non-decreasing join ops, got {counts}")
            self.assertGreater(counts[-1], counts[0], f"Expected strictly higher join complexity at high target, got {counts}")


if __name__ == "__main__":
    unittest.main()

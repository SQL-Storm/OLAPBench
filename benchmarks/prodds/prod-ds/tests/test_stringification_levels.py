import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import generate_tpcds_schema
from workload import stringification as stringification_cfg
from workload.dsdgen import stringify
from workload.dsqgen import template_resolver
import prodds_kit


@pytest.mark.needs_tpcds_tools
class StringificationLevelTests(unittest.TestCase):
    def test_level_one_schema_matches_base(self) -> None:
        base_lines = generate_tpcds_schema.DEFAULT_BASE_SCHEMA.read_text(encoding="utf-8").splitlines(
            keepends=True
        )
        config = stringification_cfg.build_stringification_config(level=1)
        rewrite_map = {col: config.schema_type_map[col] for col in config.schema_selected}
        output_lines = generate_tpcds_schema.rewrite_schema(base_lines, rewrite_map)
        self.assertEqual("".join(base_lines), "".join(output_lines))

    def test_level_ten_schema_matches_baseline_selection(self) -> None:
        config = stringification_cfg.build_stringification_config(level=10)
        self.assertEqual(config.k_schema, config.K_schema_max)
        self.assertEqual(len(config.schema_selected), config.K_schema_max)
        fixture = json.loads(
            (REPO_ROOT / "tests" / "fixtures" / "stringification_schema_manifest.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(fixture["recast_columns"], list(config.schema_selected))
        expected_type_map = {col: config.schema_type_map[col] for col in config.schema_selected}
        self.assertEqual(fixture["recast_type_map"], expected_type_map)

    def test_monotonicity_across_levels(self) -> None:
        levels = [1, 3, 5, 7, 10]
        schema_counts = [
            len(stringification_cfg.build_stringification_config(level=level).schema_selected)
            for level in levels
        ]
        self.assertEqual(schema_counts, sorted(schema_counts))

        rule_counts = []
        for level in levels:
            config = stringification_cfg.build_stringification_config(level=level)
            rules = stringify.build_rules(config)
            rule_counts.append(sum(len(cols) for cols in rules.values()))
        self.assertEqual(rule_counts, sorted(rule_counts))

        with tempfile.TemporaryDirectory() as tmpdir:
            tdir = Path(tmpdir)
            (tdir / "query1.tpl").write_text("-- base", encoding="utf-8")
            (tdir / "query2.tpl").write_text("-- base", encoding="utf-8")
            (tdir / "query3.tpl").write_text("-- base", encoding="utf-8")
            (tdir / "query2_ext.tpl").write_text("-- ext", encoding="utf-8")
            (tdir / "query3_ext.tpl").write_text("-- ext", encoding="utf-8")

            names = ["query1.tpl", "query2.tpl", "query3.tpl"]
            query_counts = [
                stringification_cfg.select_query_edits(names, tdir, level=level).k_query
                for level in levels
            ]
            self.assertEqual(query_counts, sorted(query_counts))

        fixture = json.loads(
            (REPO_ROOT / "tests" / "fixtures" / "stringification_query_manifest.json").read_text(
                encoding="utf-8"
            )
        )
        template_dir = REPO_ROOT / "query_templates"
        template_list = template_dir / "templates.lst"
        selection = stringification_cfg.select_query_edits(
            template_resolver.read_templates(template_list),
            template_dir,
            level=10,
        )
        enabled = [
            {
                "query_id": edit.query_id,
                "edit_id": edit.edit_id,
                "template": edit.ext_template,
            }
            for edit in selection.selected
        ]
        self.assertEqual(fixture["enabled_edits"], enabled)
        self.assertEqual(fixture["k_query"], selection.k_query)
        self.assertEqual(fixture["K_query_max"], selection.K_query_max)

    def test_partial_mode_midlevels_are_progressive(self) -> None:
        levels = [2, 4, 6, 8, 9]
        configs = [
            stringification_cfg.build_stringification_config(level=level, schema_selection_mode="partial")
            for level in levels
        ]

        counts = [cfg.k_schema for cfg in configs]
        self.assertEqual(counts, sorted(counts))
        self.assertEqual(len(set(counts)), len(counts))

        signatures = [tuple(cfg.schema_selected) for cfg in configs]
        self.assertEqual(len(set(signatures)), len(signatures))

    def test_partial_mode_frontloads_fact_tables(self) -> None:
        cfg = stringification_cfg.build_stringification_config(level=2, schema_selection_mode="partial")
        selected_tables = {col.split(".", 1)[0] for col in cfg.schema_selected}
        self.assertTrue({"web_sales", "catalog_sales", "store_sales"}.issubset(selected_tables))

    def test_partial_mode_selects_complete_domains(self) -> None:
        cfg = stringification_cfg.build_stringification_config(level=2, schema_selection_mode="partial")
        selected = set(cfg.schema_selected)
        candidates, _ = stringification_cfg.schema_recast_candidates()

        def domain_key(candidate: str) -> str:
            return stringification_cfg._schema_domain_key(candidate)  # noqa: SLF001

        by_domain = {}
        for candidate in candidates:
            by_domain.setdefault(domain_key(candidate), set()).add(candidate)

        touched_domains = {domain_key(candidate) for candidate in selected}
        for domain in touched_domains:
            self.assertTrue(
                by_domain[domain].issubset(selected),
                msg=f"domain {domain} should be selected atomically",
            )

    def test_default_mode_is_partial_when_not_overridden(self) -> None:
        cfg = stringification_cfg.build_stringification_config(level=2)
        self.assertEqual(cfg.schema_selection_mode, "partial")

    def test_smoke_explain_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            schema_out = tmp_path / "schema.sql"
            generate_tpcds_schema.main(
                [
                    "--stringification-level",
                    "1",
                    "--out",
                    str(schema_out),
                ]
            )

            data_dir = tmp_path / "data"
            data_dir.mkdir()
            (data_dir / "demo.tbl").write_text("1|2|\n", encoding="utf-8")
            stringify.rewrite_tbl_directory(
                data_dir,
                enable_nulls=False,
                enable_mcv=False,
                stringification_level=1,
            )

            template_dir = tmp_path / "templates"
            template_dir.mkdir()
            (template_dir / "query1.tpl").write_text("-- base", encoding="utf-8")
            (template_dir / "query1_ext.tpl").write_text("-- ext", encoding="utf-8")
            template_list = tmp_path / "templates.lst"
            template_list.write_text("query1.tpl\n", encoding="utf-8")

            selection = stringification_cfg.select_query_edits(
                template_resolver.read_templates(template_list),
                template_dir,
                level=1,
            )
            stringification_cfg.write_json(
                tmp_path / stringification_cfg.QUERY_MANIFEST_NAME,
                {
                    "stringification_level": 1,
                    "stringification_preset": "vanilla",
                    "intensity": stringification_cfg.intensity_from_level(1),
                    "k_query": selection.k_query,
                    "K_query_max": selection.K_query_max,
                    "enabled_edits": [],
                    "queries_with_edits": [],
                },
            )

            buf = io.StringIO()
            with redirect_stdout(buf):
                prodds_kit.main(
                    [
                        "explain-stringification",
                        "--stringification-level",
                        "1",
                        "--manifest-dir",
                        str(tmp_path),
                        "--template-dir",
                        str(template_dir),
                        "--template-input",
                        str(template_list),
                    ]
                )
            output = buf.getvalue()
            self.assertIn("Schema:", output)
            self.assertIn("Data:", output)
            self.assertIn("Query:", output)


if __name__ == "__main__":
    unittest.main()

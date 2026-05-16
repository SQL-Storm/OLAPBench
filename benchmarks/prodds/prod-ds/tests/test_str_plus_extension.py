import json
import tempfile
import unittest
from pathlib import Path

import pytest

from workload import stringification as stringification_cfg
from workload.dsdgen import stringify


class StrPlusExtensionTests(unittest.TestCase):
    def test_resolve_level_accepts_str_plus(self) -> None:
        level, preset = stringification_cfg.resolve_level(12, None)
        self.assertEqual(12, level)
        self.assertIsNone(preset)

    def test_resolve_level_rejects_above_max(self) -> None:
        with self.assertRaises(ValueError):
            stringification_cfg.resolve_level(25, None, max_level=20)

    @pytest.mark.needs_tpcds_tools
    def test_build_config_str_plus_metadata(self) -> None:
        cfg10 = stringification_cfg.build_stringification_config(level=10)
        self.assertFalse(cfg10.str_plus_enabled)
        self.assertEqual(0, cfg10.amplification_extra_pad)

        cfg12 = stringification_cfg.build_stringification_config(
            level=12,
            str_plus_max_level=20,
            str_plus_pad_step=3,
            str_plus_separator="_",
            str_plus_marker="X",
        )
        self.assertTrue(cfg12.str_plus_enabled)
        self.assertEqual(6, cfg12.amplification_extra_pad)
        self.assertEqual(cfg12.K_schema_max, cfg12.k_schema)

    def test_stringify_value_amplification_monotone(self) -> None:
        s10 = stringify.stringify_value("42", "c", 4)
        s12 = stringify.stringify_value(
            "42",
            "c",
            4,
            amplification_extra_pad=4,
            amplification_separator="_",
            amplification_marker="X",
        )
        s15 = stringify.stringify_value(
            "42",
            "c",
            4,
            amplification_extra_pad=10,
            amplification_separator="_",
            amplification_marker="X",
        )
        self.assertTrue(len(s10) < len(s12) < len(s15))
        self.assertTrue(s12.startswith(s10))
        self.assertTrue(s15.startswith(s10))

    @pytest.mark.needs_tpcds_tools
    def test_rewrite_manifest_records_str_plus(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir)
            # Minimal row width for customer table.
            (data_dir / "customer.dat").write_text("1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|\n", encoding="utf-8")

            files, rows = stringify.rewrite_tbl_directory(
                data_dir,
                backend="python",
                stringification_level=12,
                str_plus_max_level=20,
                str_plus_pad_step=2,
                str_plus_separator="_",
                str_plus_marker="X",
                enable_nulls=False,
                enable_mcv=False,
            )
            self.assertEqual(1, files)
            self.assertEqual(1, rows)

            manifest = json.loads(
                (data_dir / stringification_cfg.DATA_MANIFEST_NAME).read_text(encoding="utf-8")
            )
            self.assertTrue(manifest.get("str_plus_enabled"))
            amp = manifest.get("amplification", {})
            self.assertTrue(amp.get("enabled"))
            self.assertEqual(4, int(amp.get("extra_pad", 0)))
            self.assertEqual("python", manifest.get("rewrite_backend"))


if __name__ == "__main__":
    unittest.main()

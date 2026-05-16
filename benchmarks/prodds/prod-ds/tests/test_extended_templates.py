import sys
from pathlib import Path
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TEMPLATE_DIR = REPO_ROOT / "query_templates"


class ExtendedTemplateContentTests(unittest.TestCase):
    def _assert_contains(self, filename: str, substrings: list[str]) -> None:
        path = TEMPLATE_DIR / filename
        self.assertTrue(path.exists(), f"Missing template: {filename}")
        content = path.read_text(encoding="utf-8")
        for s in substrings:
            self.assertIn(s, content, f"{filename} should contain '{s}'")

    def test_query17_ext_has_string_labels(self) -> None:
        self._assert_contains(
            "query17_ext.tpl",
            [
                "any_item_category",
                "distinct_store_name_count",
                "any_market_desc",
            ],
        )

    def test_query18_ext_has_item_and_geo_labels(self) -> None:
        self._assert_contains(
            "query18_ext.tpl",
            [
                "any_item_desc",
                "any_item_category",
                "distinct_city_count",
            ],
        )

    def test_query25_ext_has_item_and_store_labels(self) -> None:
        self._assert_contains(
            "query25_ext.tpl",
            [
                "any_item_category",
                "any_item_brand",
                "any_market_desc",
            ],
        )

    def test_query5_ext_has_channel_labels(self) -> None:
        self._assert_contains(
            "query5_ext.tpl",
            [
                "store_name as label",
                "catalog_page_desc as label",
                "web_site_name as label",
                "any_channel_label",
            ],
        )

    def test_query50_ext_has_store_labels(self) -> None:
        self._assert_contains(
            "query50_ext.tpl",
            [
                "any_market_desc",
                "any_company_name",
                "any_store_manager",
            ],
        )

    def test_query72_ext_has_item_and_warehouse_labels(self) -> None:
        self._assert_contains(
            "query72_ext.tpl",
            [
                "any_item_category",
                "any_warehouse_city",
            ],
        )

    def test_query75_ext_has_product_labels(self) -> None:
        self._assert_contains(
            "query75_ext.tpl",
            [
                "brand_label",
                "category_label",
                "manufact_label",
                "product_name_count",
            ],
        )


if __name__ == "__main__":
    unittest.main()

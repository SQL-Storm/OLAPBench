import unittest

import pytest

from workload import stringification as stringification_cfg


@pytest.mark.needs_tpcds_tools
class DomainConsistencyTests(unittest.TestCase):
    def test_str1_and_str10_frozen(self) -> None:
        candidates, _ = stringification_cfg.schema_recast_candidates()
        cfg1 = stringification_cfg.build_stringification_config(level=1, schema_selection_mode="partial")
        cfg10 = stringification_cfg.build_stringification_config(level=10, schema_selection_mode="partial")

        self.assertEqual(0, cfg1.k_schema)
        self.assertEqual(tuple(candidates), tuple(cfg10.schema_selected))

    def test_monotone_superset_str1_to_str10(self) -> None:
        prev: set[str] = set()
        for level in range(1, 11):
            cfg = stringification_cfg.build_stringification_config(
                level=level,
                schema_selection_mode="partial",
            )
            cur = set(cfg.schema_selected)
            self.assertTrue(prev.issubset(cur), msg=f"STR{level} must include STR{level - 1}")
            if level >= 2:
                self.assertNotEqual(prev, cur, msg=f"STR{level} must add at least one column")
            prev = cur

    def test_web_site_domain_is_atomic(self) -> None:
        self.assertEqual(
            stringification_cfg._schema_domain_key("web_sales.ws_web_site_sk"),  # noqa: SLF001
            stringification_cfg._schema_domain_key("web_site.web_site_sk"),  # noqa: SLF001
        )

    def test_fk_pk_pairs_move_together(self) -> None:
        fk_pk = stringification_cfg.fk_pk_domain_pairs()
        self.assertTrue(fk_pk, msg="expected at least one FK/PK pair")

        for level in range(1, 16):
            cfg = stringification_cfg.build_stringification_config(
                level=level,
                schema_selection_mode="partial",
                allow_extended_levels=(level > 10),
                str_plus_enabled=(level > 10),
                str_plus_max_level=20,
            )
            selected = set(cfg.schema_selected)
            for fk, pk, _domain in fk_pk:
                self.assertEqual(
                    fk in selected,
                    pk in selected,
                    msg=f"STR{level}: FK/PK mismatch for {fk} <-> {pk}",
                )

    def test_partial_selection_is_domain_atomic(self) -> None:
        candidates, _ = stringification_cfg.schema_recast_candidates()
        by_domain: dict[str, set[str]] = {}
        for candidate in candidates:
            by_domain.setdefault(stringification_cfg._schema_domain_key(candidate), set()).add(candidate)  # noqa: SLF001

        for level in range(2, 11):
            cfg = stringification_cfg.build_stringification_config(
                level=level,
                schema_selection_mode="partial",
            )
            selected = set(cfg.schema_selected)
            for domain, members in by_domain.items():
                touched = selected.intersection(members)
                if touched:
                    self.assertEqual(
                        members,
                        touched,
                        msg=f"STR{level}: domain {domain} must be selected atomically",
                    )


if __name__ == "__main__":
    unittest.main()


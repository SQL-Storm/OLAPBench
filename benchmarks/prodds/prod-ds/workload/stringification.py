#!/usr/bin/env python3
"""
Shared stringification configuration and selection helpers.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Mapping, Sequence

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_SCHEMA = REPO_ROOT / "tools" / "tpcds.sql"
DEFAULT_PROD_SCHEMA = REPO_ROOT / "tools" / "prodds.sql"
DEFAULT_RI_SCHEMA = REPO_ROOT / "tools" / "tpcds_ri.sql"
CONFIG_PATH = REPO_ROOT / "config.yml"

SCHEMA_MANIFEST_NAME = "stringification_schema_manifest.json"
DATA_MANIFEST_NAME = "stringification_data_manifest.json"
QUERY_MANIFEST_NAME = "stringification_query_manifest.json"
BASE_MAX_LEVEL = 10
DEFAULT_STR_PLUS_MAX_LEVEL = 20

PRESET_LEVELS = {
    "vanilla": 1,
    "low": 3,
    "medium": 5,
    "high": 7,
    "production": BASE_MAX_LEVEL,
}

NUMERIC_PREFIXES = ("int", "integer", "bigint", "smallint", "decimal", "number", "numeric")

# Prioritize high-impact fact-table join keys first for progressive STR levels.
# This makes intermediate levels (STR2..STR9) meaningfully different in workload behavior.
PROGRESSIVE_TABLE_PRIORITY: Mapping[str, int] = {
    "store_sales": 0,
    "catalog_sales": 0,
    "web_sales": 0,
    "inventory": 1,
    "store_returns": 1,
    "catalog_returns": 1,
    "web_returns": 1,
    "customer": 2,
    "item": 2,
    "date_dim": 2,
    "time_dim": 2,
    "household_demographics": 3,
    "customer_demographics": 3,
    "customer_address": 3,
    "promotion": 3,
    "store": 4,
    "warehouse": 4,
    "call_center": 4,
    "web_site": 4,
    "web_page": 4,
    "ship_mode": 4,
    "reason": 4,
    "income_band": 4,
    "catalog_page": 4,
}

# Prioritize domain-wise recast to keep join domains type-consistent at partial levels.
# If we recast only one side of a join key (e.g., ws_item_sk but not i_item_sk),
# queries fail with implicit cast errors. Domain grouping avoids that.
PROGRESSIVE_DOMAIN_PRIORITY: Mapping[str, int] = {
    "item": 0,
    "customer": 1,
    "cdemo": 2,
    "hdemo": 3,
    "addr": 4,
    "date": 5,
    "time": 6,
    "warehouse": 7,
    "store": 8,
    "promo": 9,
    "ship_mode": 10,
    "reason": 11,
    "income_band": 12,
    "call_center": 13,
    "catalog_page": 14,
    "web_page": 15,
    "web_site": 16,
}

# Light query-traffic proxy: fact table keys tend to dominate runtime impact.
PROGRESSIVE_FACT_TABLE_WEIGHT: Mapping[str, int] = {
    "store_sales": 5,
    "catalog_sales": 5,
    "web_sales": 5,
    "store_returns": 4,
    "catalog_returns": 4,
    "web_returns": 4,
    "inventory": 4,
}

DEFAULT_QUERY_TEMPLATE_DIR = REPO_ROOT / "query_templates"
RI_FOREIGN_KEY_RE = re.compile(
    r"alter\s+table\s+([A-Za-z0-9_]+)\s+add\s+constraint\s+[A-Za-z0-9_]+\s+"
    r"foreign\s+key\s*\(\s*([A-Za-z0-9_]+)\s*\)\s+references\s+([A-Za-z0-9_]+)\s*"
    r"\(\s*([A-Za-z0-9_]+)\s*\)",
    re.IGNORECASE,
)

# Domain anchor (PK) columns. Every FK in the same join-domain must move with this PK.
DOMAIN_PRIMARY_KEYS: Mapping[str, str] = {
    "item": "item.i_item_sk",
    "customer": "customer.c_customer_sk",
    "cdemo": "customer_demographics.cd_demo_sk",
    "hdemo": "household_demographics.hd_demo_sk",
    "addr": "customer_address.ca_address_sk",
    "date": "date_dim.d_date_sk",
    "time": "time_dim.t_time_sk",
    "warehouse": "warehouse.w_warehouse_sk",
    "store": "store.s_store_sk",
    "promo": "promotion.p_promo_sk",
    "ship_mode": "ship_mode.sm_ship_mode_sk",
    "reason": "reason.r_reason_sk",
    "income_band": "income_band.ib_income_band_sk",
    "call_center": "call_center.cc_call_center_sk",
    "catalog_page": "catalog_page.cp_catalog_page_sk",
    "web_page": "web_page.wp_web_page_sk",
    "web_site": "web_site.web_site_sk",
}


@dataclass(frozen=True)
class PayloadConfig:
    pad_width: int
    min_pad_width: int
    base_pad_width: int
    regime: str


@dataclass(frozen=True)
class QueryEdit:
    query_id: str
    base_template: str
    ext_template: str
    edit_id: str = "use_ext"


@dataclass(frozen=True)
class QuerySelection:
    candidates: tuple[QueryEdit, ...]
    selected: tuple[QueryEdit, ...]
    k_query: int
    K_query_max: int

    @property
    def enabled_ext_templates(self) -> frozenset[str]:
        return frozenset(edit.ext_template for edit in self.selected)

    @property
    def enabled_queries(self) -> tuple[str, ...]:
        return tuple(edit.query_id for edit in self.selected)


@dataclass(frozen=True)
class StringificationConfig:
    level: int
    preset: str | None
    intensity: float
    schema_selection_mode: str
    schema_candidates: tuple[str, ...]
    schema_selected: tuple[str, ...]
    schema_type_map: Mapping[str, str]
    k_schema: int
    K_schema_max: int
    payload: PayloadConfig
    query_candidates: tuple[QueryEdit, ...]
    query_selected: tuple[QueryEdit, ...]
    k_query: int
    K_query_max: int
    str_plus_enabled: bool
    str_plus_max_level: int
    amplification_extra_pad: int
    amplification_pad_step: int
    amplification_separator: str
    amplification_marker: str


CREATE_TABLE_RE = re.compile(r"\s*create\s+table\s+([A-Za-z0-9_]+)", re.IGNORECASE)
COLUMN_DEF_RE = re.compile(r"(\s*)([A-Za-z0-9_]+)(\s+)([^,\s]+)(.*)")


def _round_half_up(value: float) -> int:
    if value <= 0:
        return 0
    return int(value + 0.5)


@lru_cache(maxsize=None)
def _load_stringification_defaults() -> Mapping[str, object]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        parsed = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    if not isinstance(parsed, dict):
        return {}
    section = parsed.get("stringification")
    return section if isinstance(section, dict) else {}


def resolve_level(
    level: int | None,
    preset: str | None,
    *,
    allow_extended: bool = True,
    max_level: int | None = None,
) -> tuple[int, str | None]:
    defaults = _load_stringification_defaults()
    default_level = defaults.get("level") if isinstance(defaults, Mapping) else None
    default_preset = defaults.get("preset") if isinstance(defaults, Mapping) else None

    resolved_level = level
    resolved_preset = preset

    if resolved_preset is None and isinstance(default_preset, str):
        resolved_preset = default_preset
    if resolved_level is None and isinstance(default_level, int):
        resolved_level = default_level

    if resolved_preset is not None:
        preset_key = str(resolved_preset).lower()
        if preset_key not in PRESET_LEVELS:
            raise ValueError(
                f"Unknown stringification preset '{resolved_preset}'. "
                f"Available: {', '.join(sorted(PRESET_LEVELS))}"
            )
        resolved_level = PRESET_LEVELS[preset_key]
        resolved_preset = preset_key

    if resolved_level is None:
        resolved_level = PRESET_LEVELS["production"]
        resolved_preset = "production"

    hard_max = BASE_MAX_LEVEL
    if allow_extended:
        configured_max = defaults.get("max_level") if isinstance(defaults, Mapping) else None
        if max_level is not None:
            hard_max = int(max_level)
        elif isinstance(configured_max, int):
            hard_max = int(configured_max)
        else:
            hard_max = DEFAULT_STR_PLUS_MAX_LEVEL
    if hard_max < 1:
        raise ValueError("max stringification level must be >= 1")

    if resolved_level < 1 or resolved_level > hard_max:
        if allow_extended:
            raise ValueError(f"stringification level must be in [1..{hard_max}]")
        raise ValueError(f"stringification level must be in [1..{BASE_MAX_LEVEL}]")

    return int(resolved_level), resolved_preset


def intensity_from_level(level: int) -> float:
    return max(0.0, min(1.0, (float(level) - 1.0) / float(BASE_MAX_LEVEL - 1)))


def derive_payload_config(
    *,
    intensity: float,
    base_pad_width: int,
    min_pad_width: int | None = None,
) -> PayloadConfig:
    base_pad_width = max(1, int(base_pad_width))
    if min_pad_width is None:
        min_pad_width = max(1, min(4, base_pad_width))
    min_pad_width = max(1, min(base_pad_width, int(min_pad_width)))
    pad_width = _round_half_up(min_pad_width + intensity * (base_pad_width - min_pad_width))
    pad_width = max(min_pad_width, min(base_pad_width, pad_width))

    if pad_width <= min_pad_width:
        regime = f"pad_width={pad_width} (low)"
    elif pad_width >= base_pad_width:
        regime = f"pad_width={pad_width} (high)"
    else:
        regime = f"pad_width={pad_width} (mid)"

    return PayloadConfig(
        pad_width=pad_width,
        min_pad_width=min_pad_width,
        base_pad_width=base_pad_width,
        regime=regime,
    )


def _is_numeric_type(token: str) -> bool:
    lowered = token.strip().lower()
    return lowered.startswith(NUMERIC_PREFIXES)


def _parse_schema(path: Path) -> list[tuple[str, list[tuple[str, str]]]]:
    tables: list[tuple[str, list[tuple[str, str]]]] = []
    current_table: str | None = None
    current_columns: list[tuple[str, str]] = []

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue

        match = CREATE_TABLE_RE.match(line)
        if match:
            if current_table is not None:
                tables.append((current_table, list(current_columns)))
            current_table = match.group(1).lower()
            current_columns = []
            continue

        if current_table and line.startswith(")"):
            tables.append((current_table, list(current_columns)))
            current_table = None
            current_columns = []
            continue

        if not current_table:
            continue
        lowered = line.lower()
        if lowered.startswith(("primary key", "unique", "constraint", "foreign key")):
            continue

        col_match = COLUMN_DEF_RE.match(line)
        if not col_match:
            continue

        col_name = col_match.group(2).lower()
        data_type = col_match.group(4).lower()
        current_columns.append((col_name, data_type))

    if current_table is not None:
        tables.append((current_table, list(current_columns)))

    return tables


@lru_cache(maxsize=None)
def schema_recast_candidates(
    base_schema_path: Path = DEFAULT_BASE_SCHEMA,
    prod_schema_path: Path = DEFAULT_PROD_SCHEMA,
) -> tuple[tuple[str, ...], Mapping[str, str]]:
    base_tables = _parse_schema(base_schema_path)
    prod_tables = {name: dict(cols) for name, cols in _parse_schema(prod_schema_path)}

    missing_cols: list[str] = []
    for table, columns in base_tables:
        prod_cols = prod_tables.get(table)
        if prod_cols is None:
            missing_cols.append(f"{table}.*")
            continue
        for col_name, _ in columns:
            if col_name not in prod_cols:
                missing_cols.append(f"{table}.{col_name}")
    if missing_cols:
        missing_display = ", ".join(sorted(missing_cols))
        raise ValueError(
            "Production schema is missing columns from the base schema. "
            f"Missing: {missing_display}"
        )

    ordered: list[str] = []
    type_map: dict[str, str] = {}

    for table, columns in base_tables:
        prod_cols = prod_tables.get(table, {})
        for col_name, base_type in columns:
            prod_type = prod_cols.get(col_name)
            if not prod_type:
                continue
            if base_type == prod_type:
                continue
            if not _is_numeric_type(base_type):
                continue
            if not prod_type.startswith("varchar") and not prod_type.startswith("char"):
                continue
            key = f"{table}.{col_name}"
            ordered.append(key)
            type_map[key] = prod_type

    return tuple(ordered), type_map


def _ext_template_name(name: str) -> str:
    if name.endswith(".tpl"):
        return f"{name[:-4]}_ext.tpl"
    return f"{name}_ext.tpl"


def _query_sort_key(name: str) -> tuple[int, str]:
    base = name
    if base.endswith(".tpl"):
        base = base[:-4]
    if base.endswith("_ext"):
        base = base[:-4]
    if base.startswith("query"):
        suffix = base[5:]
        digits = "".join(ch for ch in suffix if ch.isdigit())
        if digits:
            return (int(digits), base)
    return (10_000_000, base)


def _progressive_candidate_sort_key(
    candidate: str, ordinal: int, table_position: int
) -> tuple[int, int, int, int, int, str]:
    table, _, column = candidate.partition(".")
    table_l = table.lower()
    column_l = column.lower()
    table_rank = int(PROGRESSIVE_TABLE_PRIORITY.get(table_l, 20))
    if column_l.endswith("_sk"):
        suffix_rank = 0
    elif column_l.endswith("_id"):
        suffix_rank = 1
    elif column_l.endswith("_date_sk") or column_l.endswith("_time_sk"):
        suffix_rank = 2
    else:
        suffix_rank = 3
    # Prefer larger fact tables (ss/cs/ws) in ties for earlier query impact.
    fact_rank = 0 if column_l.startswith(("ss_", "cs_", "ws_")) else 1
    return (table_rank, table_position, suffix_rank, fact_rank, ordinal, candidate)


def _fallback_schema_domain_key(column: str) -> str:
    col = column.lower()
    if "cdemo_sk" in col or col == "cd_demo_sk":
        return "cdemo"
    if "hdemo_sk" in col or col == "hd_demo_sk":
        return "hdemo"
    if col.endswith("_addr_sk") or col.endswith("_address_sk") or col == "ca_address_sk":
        return "addr"
    if col.endswith("_customer_sk") or col == "c_customer_sk":
        return "customer"
    if col.endswith("_item_sk") or col == "i_item_sk":
        return "item"
    if col.endswith("_warehouse_sk") or col == "w_warehouse_sk":
        return "warehouse"
    if col.endswith("_store_sk") or col == "s_store_sk":
        return "store"
    if col.endswith("_promo_sk") or col == "p_promo_sk":
        return "promo"
    if col.endswith("_ship_mode_sk") or col == "sm_ship_mode_sk":
        return "ship_mode"
    if col.endswith("_reason_sk") or col == "r_reason_sk":
        return "reason"
    if col.endswith("_income_band_sk") or col == "ib_income_band_sk":
        return "income_band"
    if col.endswith("_call_center_sk") or col == "cc_call_center_sk":
        return "call_center"
    if col.endswith("_catalog_page_sk") or col == "cp_catalog_page_sk":
        return "catalog_page"
    if col.endswith("_web_page_sk") or col == "wp_web_page_sk":
        return "web_page"
    if col.endswith("_web_site_sk") or col == "web_site_sk":
        return "web_site"
    if col.endswith("_date_sk") or col == "d_date_sk":
        return "date"
    if col.endswith("_time_sk") or col == "t_time_sk":
        return "time"
    # Avoid gigantic catch-all ID domains; keep IDs independent.
    if col.endswith("_id"):
        return col
    if "_" in col:
        return col.split("_", 1)[1]
    return col


@lru_cache(maxsize=1)
def _ri_fk_pairs() -> tuple[tuple[str, str], ...]:
    if not DEFAULT_RI_SCHEMA.exists():
        return ()
    pairs: list[tuple[str, str]] = []
    for raw_line in DEFAULT_RI_SCHEMA.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        # Ignore inline SQL comments to keep parsing deterministic.
        if "--" in stripped:
            stripped = stripped.split("--", 1)[0].strip()
        if not stripped:
            continue
        match = RI_FOREIGN_KEY_RE.search(stripped)
        if not match:
            continue
        table, fk_col, ref_table, ref_col = (part.lower() for part in match.groups())
        fk = f"{table}.{fk_col}"
        pk = f"{ref_table}.{ref_col}"
        pairs.append((fk, pk))
    return tuple(sorted(set(pairs)))


@lru_cache(maxsize=1)
def _schema_domain_overrides() -> Mapping[str, str]:
    candidates, _ = schema_recast_candidates()
    candidate_set = {candidate.lower() for candidate in candidates}
    pk_to_domain = {pk.lower(): domain for domain, pk in DOMAIN_PRIMARY_KEYS.items()}

    overrides: dict[str, str] = {}
    for domain, pk in pk_to_domain.items():
        if pk in candidate_set:
            overrides[pk] = domain

    for fk, pk in _ri_fk_pairs():
        if fk not in candidate_set and pk not in candidate_set:
            continue
        domain = pk_to_domain.get(pk)
        if domain is None:
            _pk_table, _, pk_col = pk.partition(".")
            domain = _fallback_schema_domain_key(pk_col)
        if pk in candidate_set:
            overrides[pk] = domain
        if fk in candidate_set:
            overrides[fk] = domain
    return overrides


def fk_pk_domain_pairs() -> tuple[tuple[str, str, str], ...]:
    """
    Return inferred FK->PK pairs for recast candidates, grouped by canonical domain key.
    """
    candidates, _ = schema_recast_candidates()
    candidate_set = {candidate.lower() for candidate in candidates}
    pairs: set[tuple[str, str, str]] = set()

    for fk, pk in _ri_fk_pairs():
        if fk not in candidate_set or pk not in candidate_set:
            continue
        pairs.add((fk, pk, _schema_domain_key(pk)))

    for domain, pk in DOMAIN_PRIMARY_KEYS.items():
        pk_l = pk.lower()
        if pk_l not in candidate_set:
            continue
        for candidate in candidate_set:
            if candidate == pk_l:
                continue
            if _schema_domain_key(candidate) == domain:
                pairs.add((candidate, pk_l, domain))

    return tuple(sorted(pairs))


def _schema_domain_key(candidate: str) -> str:
    candidate_l = candidate.lower()
    domain = _schema_domain_overrides().get(candidate_l)
    if domain:
        return domain
    _table, _, column = candidate_l.partition(".")
    return _fallback_schema_domain_key(column)


@lru_cache(maxsize=1)
def _base_query_templates() -> tuple[tuple[str, str], ...]:
    if not DEFAULT_QUERY_TEMPLATE_DIR.exists():
        return ()
    paths = [
        p
        for p in DEFAULT_QUERY_TEMPLATE_DIR.glob("query*.tpl")
        if p.is_file() and not p.name.endswith("_ext.tpl")
    ]
    paths.sort(key=lambda p: _query_sort_key(p.name))
    out: list[tuple[str, str]] = []
    for path in paths:
        text = path.read_text(encoding="utf-8", errors="replace").lower()
        out.append((path.name, text))
    return tuple(out)


@lru_cache(maxsize=1)
def _domain_query_traffic_scores(candidates: tuple[str, ...]) -> Mapping[str, int]:
    domains: dict[str, list[str]] = {}
    for candidate in candidates:
        domains.setdefault(_schema_domain_key(candidate), []).append(candidate)

    templates = _base_query_templates()
    scores: dict[str, int] = {}
    for domain, members in domains.items():
        columns = sorted({member.split(".", 1)[1].lower() for member in members})
        patterns = [
            re.compile(rf"\b{re.escape(col)}\b", flags=re.IGNORECASE)
            for col in columns
        ]
        query_hits = 0
        member_hits = 0
        if templates:
            for _name, text in templates:
                matched = 0
                for pattern in patterns:
                    if pattern.search(text):
                        matched += 1
                if matched:
                    query_hits += 1
                    member_hits += matched
        fact_weight = 0
        for member in members:
            table = member.split(".", 1)[0].lower()
            fact_weight += int(PROGRESSIVE_FACT_TABLE_WEIGHT.get(table, 1))
        # Query-template coverage is primary; per-column hits and fact-table exposure
        # make intermediate levels more workload-sensitive.
        score = query_hits * 1000 + member_hits * 50 + fact_weight * 20 + len(members)
        scores[domain] = int(score)
    return scores


def _progressive_domain_sort_key(
    domain: str,
    members: Sequence[str],
    traffic_scores: Mapping[str, int],
) -> tuple[int, int, int, int, str]:
    traffic = int(traffic_scores.get(domain, 0))
    domain_rank = int(PROGRESSIVE_DOMAIN_PRIORITY.get(domain, 100))
    table_rank = min(
        int(PROGRESSIVE_TABLE_PRIORITY.get(member.split(".", 1)[0].lower(), 20))
        for member in members
    )
    return (-traffic, domain_rank, table_rank, -len(members), domain)


def _target_domain_count_for_level(level: int, total_domains: int) -> int:
    if level <= 1 or total_domains <= 0:
        return 0
    if total_domains == 1:
        return 1
    if level >= BASE_MAX_LEVEL:
        return total_domains
    # STR2..STR10: keep strict level-by-level progression and avoid single huge
    # domains swallowing all early levels.
    span = BASE_MAX_LEVEL - 1
    proportional = ((level - 1) * total_domains + span - 1) // span
    strict_min = level - 1
    return min(total_domains, max(strict_min, proportional))


def _progressive_schema_selection(
    candidates: Sequence[str],
    k_schema: int,
    *,
    level: int | None = None,
) -> tuple[str, ...]:
    if k_schema <= 0:
        return ()
    if k_schema >= len(candidates):
        # Preserve canonical full-coverage order for backwards-compatible manifests.
        return tuple(candidates)

    domains: dict[str, list[str]] = {}
    for candidate in candidates:
        domain = _schema_domain_key(candidate)
        domains.setdefault(domain, []).append(candidate)

    traffic_scores = _domain_query_traffic_scores(tuple(candidates))
    ranked_domains = sorted(
        domains.keys(),
        key=lambda domain: _progressive_domain_sort_key(domain, domains[domain], traffic_scores),
    )

    # Domains needed to satisfy target column count.
    by_k_count = 0
    by_k_domains = 0
    for domain in ranked_domains:
        if by_k_count >= k_schema:
            break
        by_k_domains += 1
        by_k_count += len(domains[domain])

    by_level_domains = 0
    if level is not None and level > 1:
        by_level_domains = _target_domain_count_for_level(level, len(ranked_domains))

    domains_to_take = max(by_k_domains, by_level_domains)
    selected_domains = set(ranked_domains[:domains_to_take])

    # Preserve canonical candidate order while selecting full domains.
    return tuple(
        candidate
        for candidate in candidates
        if _schema_domain_key(candidate) in selected_domains
    )


def query_edit_candidates(
    template_names: Iterable[str],
    template_dir: Path,
) -> tuple[QueryEdit, ...]:
    candidates: list[QueryEdit] = []
    for name in template_names:
        ext_name = _ext_template_name(name)
        if not (template_dir / ext_name).exists():
            continue
        base = name[:-4] if name.endswith(".tpl") else name
        query_id = base
        candidates.append(QueryEdit(query_id=query_id, base_template=name, ext_template=ext_name))

    candidates.sort(key=lambda edit: _query_sort_key(edit.query_id))
    return tuple(candidates)


def select_query_edits(
    template_names: Iterable[str],
    template_dir: Path,
    *,
    level: int | None = None,
    preset: str | None = None,
    allow_extended_levels: bool = False,
    str_plus_max_level: int = DEFAULT_STR_PLUS_MAX_LEVEL,
) -> QuerySelection:
    resolved_level, resolved_preset = resolve_level(
        level,
        preset,
        allow_extended=allow_extended_levels,
        max_level=str_plus_max_level,
    )
    intensity = intensity_from_level(resolved_level)
    candidates = query_edit_candidates(template_names, template_dir)
    total = len(candidates)
    k_query = _round_half_up(intensity * total)
    selected = candidates[:k_query]
    return QuerySelection(
        candidates=candidates,
        selected=tuple(selected),
        k_query=k_query,
        K_query_max=total,
    )


def build_stringification_config(
    *,
    level: int | None = None,
    preset: str | None = None,
    base_schema_path: Path = DEFAULT_BASE_SCHEMA,
    prod_schema_path: Path = DEFAULT_PROD_SCHEMA,
    template_names: Iterable[str] | None = None,
    template_dir: Path | None = None,
    base_pad_width: int = 8,
    min_pad_width: int | None = None,
    schema_selection_mode: str | None = None,
    allow_extended_levels: bool = True,
    str_plus_enabled: bool | None = None,
    str_plus_max_level: int = DEFAULT_STR_PLUS_MAX_LEVEL,
    str_plus_pad_step: int = 2,
    str_plus_separator: str = "_",
    str_plus_marker: str = "X",
) -> StringificationConfig:
    resolved_level, resolved_preset = resolve_level(
        level,
        preset,
        allow_extended=allow_extended_levels,
        max_level=str_plus_max_level,
    )
    if str_plus_enabled is None:
        str_plus_enabled = resolved_level > BASE_MAX_LEVEL
    intensity = intensity_from_level(resolved_level)

    candidates, type_map = schema_recast_candidates(base_schema_path, prod_schema_path)
    defaults = _load_stringification_defaults()
    resolved_mode = schema_selection_mode
    if resolved_mode is None and isinstance(defaults, Mapping):
        configured = defaults.get("schema_selection") or defaults.get("schema_selection_mode")
        if isinstance(configured, str) and configured.strip():
            resolved_mode = configured
    if resolved_mode is None:
        # Keep intermediate STR levels informative by default:
        # level>1 should not immediately jump to full recast coverage.
        resolved_mode = "partial"
    resolved_mode = str(resolved_mode).strip().lower()
    if resolved_mode not in ("full", "partial"):
        raise ValueError("schema_selection_mode must be 'full' or 'partial'")

    if resolved_level <= 1:
        schema_selected = ()
    elif resolved_mode == "full":
        schema_selected = candidates
    else:
        target_k_schema = _round_half_up(intensity * len(candidates))
        schema_selected = _progressive_schema_selection(
            candidates,
            target_k_schema,
            level=resolved_level,
        )
    k_schema = len(schema_selected)

    payload = derive_payload_config(
        intensity=intensity,
        base_pad_width=base_pad_width,
        min_pad_width=min_pad_width,
    )

    query_candidates: tuple[QueryEdit, ...] = ()
    query_selected: tuple[QueryEdit, ...] = ()
    k_query = 0
    total_query = 0
    if template_dir is not None and template_names is not None:
        selection = select_query_edits(
            template_names,
            template_dir,
            level=resolved_level,
            preset=resolved_preset,
            allow_extended_levels=allow_extended_levels or str_plus_enabled,
            str_plus_max_level=str_plus_max_level,
        )
        query_candidates = selection.candidates
        query_selected = selection.selected
        k_query = selection.k_query
        total_query = selection.K_query_max

    return StringificationConfig(
        level=resolved_level,
        preset=resolved_preset,
        intensity=intensity,
        schema_selection_mode=resolved_mode,
        schema_candidates=candidates,
        schema_selected=tuple(schema_selected),
        schema_type_map=type_map,
        k_schema=k_schema,
        K_schema_max=len(candidates),
        payload=payload,
        query_candidates=query_candidates,
        query_selected=query_selected,
        k_query=k_query,
        K_query_max=total_query,
        str_plus_enabled=bool(str_plus_enabled and resolved_level > BASE_MAX_LEVEL),
        str_plus_max_level=int(str_plus_max_level),
        amplification_extra_pad=max(0, resolved_level - BASE_MAX_LEVEL) * max(1, int(str_plus_pad_step)),
        amplification_pad_step=max(1, int(str_plus_pad_step)),
        amplification_separator=str(str_plus_separator),
        amplification_marker=str(str_plus_marker),
    )


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

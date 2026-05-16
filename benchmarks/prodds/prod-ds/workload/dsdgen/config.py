#!/usr/bin/env python3
"""
Lightweight configuration loader for TPCDS-Kit-PlusPlus helpers.
"""

from __future__ import annotations

import copy
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Mapping

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config.yml"
NULL_PROFILES_PATH = REPO_ROOT / "config" / "null_profiles.yml"
MCV_PROFILES_PATH = REPO_ROOT / "config" / "mcv_profiles.yml"

# "fleet_realworld_final" equals the "medium" null sparsity tier.
DEFAULT_NULL_PROFILE_NAME = "fleet_realworld_final"
# "mcv_fleet_default" equals the "medium" MCV skew tier.
DEFAULT_MCV_PROFILE_NAME = "mcv_fleet_default"

MCV_TIER_ALIASES: Dict[str, str] = {
    "low": "mcv_low",
    "medium": "mcv_fleet_default",
    "high": "mcv_high",
}

NULL_TIER_ALIASES: Dict[str, str] = {
    "low": "null_low",
    "medium": "fleet_realworld_final",
    "high": "null_high",
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "stringify": {
        "enabled": True,
        "prefixes": {
            "customer": "c",
            "date_dim": "d",
            "item": "i",
        },
        "pad_width": 8,
    },
    "nulls": {
        "enabled": True,
        "profile": DEFAULT_NULL_PROFILE_NAME,
        "seed": 0,
        "selection_fraction_scope": "overall",
        "include_hot_path_columns": True,
    },
    "mcv": {
        "enabled": True,
        "profile": DEFAULT_MCV_PROFILE_NAME,
        "seed": 0,
        "selection_fraction_scope": "overall",
        "include_hot_path_columns": True,
    },
}

DEFAULT_NULL_PROFILE: Dict[str, Any] = {
    "column_selection_fraction": 0.24,
    "selection_fraction_scope": "overall",
    "null_marker": "",
    "buckets": [
        {"weight": 0.20, "min": 0.00, "max": 0.005},
        {"weight": 0.05, "min": 0.005, "max": 0.02},
        {"weight": 0.05, "min": 0.02, "max": 0.10},
        {"weight": 0.10, "min": 0.10, "max": 0.40},
        {"weight": 0.22, "min": 0.40, "max": 0.80},
        {"weight": 0.40, "min": 0.80, "max": 0.995},
    ],
}

DEFAULT_MCV_PROFILE: Dict[str, Any] = {
    "column_selection_fraction": 0.70,
    "selection_fraction_scope": "overall",
    "top20_buckets": [
        {"weight": 0.35, "min": 0.00, "max": 0.05},
        {"weight": 0.20, "min": 0.05, "max": 0.20},
        {"weight": 0.20, "min": 0.20, "max": 0.60},
        {"weight": 0.20, "min": 0.60, "max": 0.95},
        {"weight": 0.05, "min": 0.95, "max": 0.999},
    ],
    "r_buckets": [
        {"weight": 0.25, "min": 0.10, "max": 0.30},
        {"weight": 0.45, "min": 0.30, "max": 0.65},
        {"weight": 0.30, "min": 0.65, "max": 0.95},
    ],
}


def _merge_dict(base: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _merge_dict(base[key], value)
        else:
            base[key] = value
    return base


@lru_cache(maxsize=None)
def load_config() -> Dict[str, Any]:
    config = copy.deepcopy(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        with CONFIG_PATH.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
            if isinstance(parsed, dict):
                _merge_dict(config, parsed)
    return config


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        parsed = yaml.safe_load(handle) or {}
    return parsed if isinstance(parsed, dict) else {}


def _load_null_profiles() -> Dict[str, Dict[str, Any]]:
    cfg = _load_yaml(NULL_PROFILES_PATH)
    profiles = cfg.get("profiles") if isinstance(cfg, dict) else None
    if not isinstance(profiles, dict) or not profiles:
        return {DEFAULT_NULL_PROFILE_NAME: copy.deepcopy(DEFAULT_NULL_PROFILE)}
    normalized: Dict[str, Dict[str, Any]] = {}
    for name, profile in profiles.items():
        if isinstance(profile, dict):
            normalized[str(name)] = copy.deepcopy(profile)
    if DEFAULT_NULL_PROFILE_NAME not in normalized:
        normalized[DEFAULT_NULL_PROFILE_NAME] = copy.deepcopy(DEFAULT_NULL_PROFILE)
    return normalized


def _load_mcv_profiles() -> Dict[str, Dict[str, Any]]:
    cfg = _load_yaml(MCV_PROFILES_PATH)
    profiles = cfg.get("profiles") if isinstance(cfg, dict) else None
    if not isinstance(profiles, dict) or not profiles:
        return {DEFAULT_MCV_PROFILE_NAME: copy.deepcopy(DEFAULT_MCV_PROFILE)}
    normalized: Dict[str, Dict[str, Any]] = {}
    for name, profile in profiles.items():
        if isinstance(profile, dict):
            normalized[str(name)] = copy.deepcopy(profile)
    if DEFAULT_MCV_PROFILE_NAME not in normalized:
        normalized[DEFAULT_MCV_PROFILE_NAME] = copy.deepcopy(DEFAULT_MCV_PROFILE)
    return normalized


class _PrefixMap(dict):
    def __missing__(self, key: str) -> str:
        if not key:
            return ""
        value = key[0]
        self[key] = value
        return value


def stringify_rules() -> Dict[str, Any]:
    """
    Return the current stringify settings (prefix mapping + pad width).
    """
    cfg = load_config().get("stringify", {})
    pad_width = int(cfg.get("pad_width", DEFAULT_CONFIG["stringify"]["pad_width"]))
    enabled = bool(cfg.get("enabled", DEFAULT_CONFIG["stringify"]["enabled"]))
    prefixes = _PrefixMap()
    for table_name, prefix in (cfg.get("prefixes") or {}).items():
        if not table_name:
            continue
        prefixes[table_name] = str(prefix)
    return {
        "enabled": enabled,
        "pad_width": pad_width,
        "prefixes": prefixes,
    }


def null_skew_rules(
    overrides: Mapping[str, Any] | None = None, profile: str | None = None
) -> Dict[str, Any]:
    """
    Return the null-skew configuration merged from defaults, config.yml, and optional overrides.
    """
    cfg = copy.deepcopy(DEFAULT_CONFIG["nulls"])
    _merge_dict(cfg, load_config().get("nulls", {}))
    if overrides:
        _merge_dict(cfg, dict(overrides))

    raw_profile_name = profile or cfg.get("profile") or DEFAULT_NULL_PROFILE_NAME
    profile_name = NULL_TIER_ALIASES.get(str(raw_profile_name).lower(), raw_profile_name)
    tier_alias = raw_profile_name.lower() if raw_profile_name.lower() in NULL_TIER_ALIASES else None
    profiles = _load_null_profiles()
    profile_cfg = copy.deepcopy(
        profiles.get(profile_name) or profiles.get(DEFAULT_NULL_PROFILE_NAME) or DEFAULT_NULL_PROFILE
    )

    merged: Dict[str, Any] = copy.deepcopy(profile_cfg)
    merged["profile"] = profile_name
    if tier_alias is not None:
        merged["tier_alias"] = tier_alias
    merged["enabled"] = bool(cfg.get("enabled", DEFAULT_CONFIG["nulls"]["enabled"]))
    merged["seed"] = int(cfg.get("seed", DEFAULT_CONFIG["nulls"]["seed"]))

    if "null_marker" in cfg:
        merged["null_marker"] = cfg["null_marker"]
    if "selection_fraction_scope" in cfg:
        merged["selection_fraction_scope"] = cfg["selection_fraction_scope"]
    if "column_selection_fraction" in cfg:
        merged["column_selection_fraction"] = cfg["column_selection_fraction"]
    if "buckets" in cfg:
        merged["buckets"] = cfg["buckets"]
    if "column_probabilities" in cfg:
        merged["column_probabilities"] = cfg["column_probabilities"]
    if "include_hot_path_columns" in cfg:
        merged["include_hot_path_columns"] = bool(cfg["include_hot_path_columns"])
    if "min_ndv_for_injection" in cfg:
        merged["min_ndv_for_injection"] = cfg["min_ndv_for_injection"]
    if "ndv_reference_duckdb" in cfg:
        merged["ndv_reference_duckdb"] = cfg["ndv_reference_duckdb"]
    if "ndv_cache_dir" in cfg:
        merged["ndv_cache_dir"] = cfg["ndv_cache_dir"]
    if "scale_factor" in cfg:
        merged["scale_factor"] = cfg["scale_factor"]

    if "null_marker" not in merged:
        merged["null_marker"] = DEFAULT_NULL_PROFILE["null_marker"]
    if "selection_fraction_scope" not in merged:
        merged["selection_fraction_scope"] = DEFAULT_NULL_PROFILE.get("selection_fraction_scope", "overall")
    if "column_selection_fraction" not in merged:
        merged["column_selection_fraction"] = DEFAULT_NULL_PROFILE["column_selection_fraction"]
    if "buckets" not in merged:
        merged["buckets"] = copy.deepcopy(DEFAULT_NULL_PROFILE["buckets"])
    if "include_hot_path_columns" not in merged:
        merged["include_hot_path_columns"] = bool(
            DEFAULT_CONFIG["nulls"].get("include_hot_path_columns", True)
        )

    return merged


def mcv_skew_rules(
    overrides: Mapping[str, Any] | None = None, profile: str | None = None
) -> Dict[str, Any]:
    """
    Return the MCV-skew configuration merged from defaults, config.yml, and optional overrides.
    """
    cfg = copy.deepcopy(DEFAULT_CONFIG["mcv"])
    _merge_dict(cfg, load_config().get("mcv", {}))
    if overrides:
        _merge_dict(cfg, dict(overrides))

    raw_profile_name = profile or cfg.get("profile") or DEFAULT_MCV_PROFILE_NAME
    profile_name = MCV_TIER_ALIASES.get(str(raw_profile_name).lower(), raw_profile_name)
    tier_alias = raw_profile_name.lower() if raw_profile_name.lower() in MCV_TIER_ALIASES else None
    profiles = _load_mcv_profiles()
    profile_cfg = copy.deepcopy(
        profiles.get(profile_name) or profiles.get(DEFAULT_MCV_PROFILE_NAME) or DEFAULT_MCV_PROFILE
    )

    merged: Dict[str, Any] = copy.deepcopy(profile_cfg)
    merged["profile"] = profile_name
    if tier_alias is not None:
        merged["tier_alias"] = tier_alias
    merged["enabled"] = bool(cfg.get("enabled", DEFAULT_CONFIG["mcv"]["enabled"]))
    merged["seed"] = int(cfg.get("seed", DEFAULT_CONFIG["mcv"]["seed"]))

    if "column_selection_fraction" in cfg:
        merged["column_selection_fraction"] = cfg["column_selection_fraction"]
    if "selection_fraction_scope" in cfg:
        merged["selection_fraction_scope"] = cfg["selection_fraction_scope"]
    if "top20_buckets" in cfg:
        merged["top20_buckets"] = cfg["top20_buckets"]
    if "r_buckets" in cfg:
        merged["r_buckets"] = cfg["r_buckets"]
    if "column_top5_rules" in cfg:
        merged["column_top5_rules"] = cfg["column_top5_rules"]
    if "include_hot_path_columns" in cfg:
        merged["include_hot_path_columns"] = bool(cfg["include_hot_path_columns"])
    if "min_ndv_for_injection" in cfg:
        merged["min_ndv_for_injection"] = cfg["min_ndv_for_injection"]
    if "ndv_reference_duckdb" in cfg:
        merged["ndv_reference_duckdb"] = cfg["ndv_reference_duckdb"]
    if "ndv_cache_dir" in cfg:
        merged["ndv_cache_dir"] = cfg["ndv_cache_dir"]
    if "scale_factor" in cfg:
        merged["scale_factor"] = cfg["scale_factor"]

    if "column_selection_fraction" not in merged:
        merged["column_selection_fraction"] = DEFAULT_MCV_PROFILE["column_selection_fraction"]
    if "selection_fraction_scope" not in merged:
        merged["selection_fraction_scope"] = DEFAULT_MCV_PROFILE.get("selection_fraction_scope", "overall")
    if "top20_buckets" not in merged:
        merged["top20_buckets"] = copy.deepcopy(DEFAULT_MCV_PROFILE["top20_buckets"])
    if "r_buckets" not in merged:
        merged["r_buckets"] = copy.deepcopy(DEFAULT_MCV_PROFILE["r_buckets"])
    if "include_hot_path_columns" not in merged:
        merged["include_hot_path_columns"] = bool(
            DEFAULT_CONFIG["mcv"].get("include_hot_path_columns", True)
        )

    return merged

#!/usr/bin/env python3
"""
Helper utilities to post-process TPC-DS .tbl files by stringifying key columns and
optionally injecting NULL skew and Most Common Value (MCV) skew.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import csv
import subprocess
import tempfile
import time
import yaml
from dataclasses import dataclass, field
from datetime import date, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Set, Tuple

try:
    from tqdm.auto import tqdm  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    tqdm = None

from workload import stringification as stringification_cfg
from workload.dsdgen.config import mcv_skew_rules, null_skew_rules, stringify_rules

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "tpcds-kit" / "tools" / "tpcds.sql"

CREATE_TABLE_RE = re.compile(r"create\s+table\s+([a-zA-Z0-9_]+)", re.IGNORECASE)
COLUMN_DEF_RE = re.compile(
    r"^\s*([a-zA-Z0-9_]+)\s+([a-zA-Z]+(?:\([^)]+\))?)", re.IGNORECASE
)
KEY_SUFFIXES = ("_sk", "_id")
WORKER_ENV_VAR = "STRINGIFY_MAX_WORKERS"
BACKEND_ENV_VAR = "STRINGIFY_BACKEND"
DATA_EXTENSIONS = (".tbl", ".dat")
CPP_BINARY_CANDIDATES = ("stringify_cpp", "stringify_cpp.exe")

# Custom prefixes for select stringified attributes.
CUSTOM_PREFIXES: Mapping[str, Mapping[str, str]] = {
    "item": {
        "i_brand_id": "BRAND_",
        "i_class_id": "CLASS_",
        "i_category_id": "CAT_",
        "i_manufact_id": "MFG_",
        "i_manager_id": "MGR_",
    },
    "store": {
        "s_market_id": "MKT_",
        "s_division_id": "DIV_",
        "s_company_id": "CO_",
    },
    "call_center": {
        "cc_mkt_id": "MKT_",
        "cc_division": "DIV_",
        "cc_company": "CO_",
    },
    "web_site": {
        "web_mkt_id": "MKT_",
        "web_company_id": "CO_",
    },
    "store_sales": {
        "ss_ticket_number": "TKT_",
        "ss_sold_date_sk": "D_",
        "ss_sold_time_sk": "T_",
    },
    "store_returns": {
        "sr_ticket_number": "TKT_",
        "sr_returned_date_sk": "D_",
        "sr_return_time_sk": "T_",
    },
    "catalog_sales": {
        "cs_order_number": "ORD_",
        "cs_sold_date_sk": "D_",
        "cs_sold_time_sk": "T_",
        "cs_ship_date_sk": "D_",
    },
    "web_sales": {
        "ws_order_number": "ORD_",
        "ws_sold_date_sk": "D_",
        "ws_sold_time_sk": "T_",
        "ws_ship_date_sk": "D_",
    },
    "catalog_returns": {
        "cr_order_number": "ORD_",
        "cr_returned_date_sk": "D_",
        "cr_returned_time_sk": "T_",
    },
    "web_returns": {
        "wr_order_number": "ORD_",
        "wr_returned_date_sk": "D_",
        "wr_returned_time_sk": "T_",
    },
}

# Ensure PK/FK domains stringify to the same textual key space across tables.
# Without this, joins can collapse when table-local prefixes differ (e.g. c_customer_sk vs ss_customer_sk).
DOMAIN_SUFFIX_PREFIXES: Sequence[tuple[str, str]] = (
    ("income_band_sk", "i"),
    ("ship_mode_sk", "s"),
    ("call_center_sk", "c"),
    ("catalog_page_sk", "c"),
    ("web_page_sk", "w"),
    ("web_site_sk", "w"),
    ("warehouse_sk", "w"),
    ("customer_sk", "c"),
    ("item_sk", "i"),
    ("store_sk", "s"),
    ("reason_sk", "r"),
    ("promo_sk", "p"),
    ("addr_sk", "c"),
    ("cdemo_sk", "c"),
    ("hdemo_sk", "h"),
    ("date_sk", "D_"),
    ("time_sk", "T_"),
)

STATS_SAMPLE_SIZE = 5000
STATS_SEED = 0
MIN_NDV_FOR_INJECTION = 50
NDV_QUERY_TIMEOUT_SECONDS = 1800
DEFAULT_NDV_CACHE_DIR = (
    REPO_ROOT / "experiments" / "artifacts" / "hotpath_default_validation" / "ndv_cache"
)

# Type aliases to clarify the expected data structures.
SchemaInfo = Dict[str, Dict[str, Any]]
RowRules = Mapping[str, Any]
RulesMap = Mapping[str, Mapping[str, RowRules]]


@dataclass
class LengthStats:
    sample_size: int = STATS_SAMPLE_SIZE
    seed: int = STATS_SEED
    count: int = 0
    min_len: int | None = None
    max_len: int | None = None
    _samples: List[Tuple[float, int]] = field(default_factory=list)
    _lock: Lock = field(default_factory=Lock, repr=False)

    def observe(self, length: int, *key_parts: object) -> None:
        with self._lock:
            self.count += 1
            if self.min_len is None or length < self.min_len:
                self.min_len = length
            if self.max_len is None or length > self.max_len:
                self.max_len = length

            if self.sample_size <= 0:
                return
            token = _stable_unit_hash(self.seed, *key_parts)
            if len(self._samples) < self.sample_size:
                self._samples.append((token, length))
                return
            # Replace the largest token when the new token is smaller.
            max_idx = max(range(len(self._samples)), key=lambda idx: self._samples[idx][0])
            if token < self._samples[max_idx][0]:
                self._samples[max_idx] = (token, length)

    def summary(self) -> Mapping[str, int]:
        if self.count == 0 or self.min_len is None or self.max_len is None:
            return {}
        sample_lengths = [length for _, length in self._samples]
        if not sample_lengths:
            return {}
        sample_lengths.sort()
        median = sample_lengths[len(sample_lengths) // 2]
        p95_index = max(0, int(len(sample_lengths) * 0.95) - 1)
        p95 = sample_lengths[p95_index]
        return {
            "count": self.count,
            "min": self.min_len,
            "median": median,
            "p95": p95,
            "max": self.max_len,
            "sample_size": len(sample_lengths),
        }


def load_schema(path: str | Path, recast_types: Mapping[str, str] | None = None) -> SchemaInfo:
    """
    Parse the prodds.sql schema and identify key-like columns.

    Returns:
        {
            "table_name": {
                "columns": ["col_a", "col_b", ...],  # column order
                "varchar_keys": [
                    {"name": "col_x", "index": 0, "data_type": "varchar(32)"},
                    ...
                ],
                "key_like_columns": [
                    {"name": "col_x", "index": 0, "data_type": "varchar(32)"},
                    ...
                ],
                "not_null_columns": [
                    {"name": "col_y", "index": 2},
                    ...
                ],
            },
            ...
        }
    """
    schema: SchemaInfo = {}
    current_table: str | None = None
    current_columns: List[str] = []
    current_varchar_keys: List[Dict[str, Any]] = []
    current_key_like: List[Dict[str, Any]] = []
    current_not_null: List[Dict[str, Any]] = []
    current_column_types: Dict[str, str] = {}
    column_index = 0

    overrides = {k.lower(): v.lower() for k, v in (recast_types or {}).items()}

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("--"):
                continue

            if current_table is None:
                match = CREATE_TABLE_RE.match(line)
                if match:
                    current_table = match.group(1)
                    current_columns = []
                    current_varchar_keys = []
                    current_key_like = []
                    current_not_null = []
                    current_column_types = {}
                    column_index = 0
                continue

            if line.startswith(")"):
                schema[current_table] = {
                    "columns": list(current_columns),
                    "varchar_keys": list(current_varchar_keys),
                    "key_like_columns": list(current_key_like),
                    "not_null_columns": list(current_not_null),
                    "column_types": dict(current_column_types),
                }
                current_table = None
                continue

            lowered = line.lower()
            if lowered.startswith(("primary key", "unique", "constraint", "foreign key")):
                continue

            # Remove trailing comma to simplify parsing.
            clean_line = line.rstrip(",")
            column_match = COLUMN_DEF_RE.match(clean_line)
            if not column_match:
                continue

            col_name = column_match.group(1)
            data_type = column_match.group(2)
            override_key = f"{current_table.lower()}.{col_name.lower()}"
            if override_key in overrides:
                data_type = overrides[override_key]
            current_columns.append(col_name)
            current_column_types[col_name] = data_type

            if _is_varchar_key(col_name, data_type):
                current_varchar_keys.append(
                    {"name": col_name, "index": column_index, "data_type": data_type}
                )

            if col_name.lower().endswith(KEY_SUFFIXES):
                current_key_like.append(
                    {"name": col_name, "index": column_index, "data_type": data_type}
                )

            if "not null" in lowered:
                current_not_null.append({"name": col_name, "index": column_index})

            column_index += 1

    return schema


def _is_varchar_key(column_name: str, data_type: str) -> bool:
    lowered_type = data_type.lower()
    if "char" not in lowered_type:
        return False
    return column_name.lower().endswith(KEY_SUFFIXES)


def amplify_string(
    base_value: str,
    *,
    extra_pad: int = 0,
    separator: str = "_",
    marker: str = "X",
) -> str:
    if extra_pad <= 0:
        return base_value
    marker = marker if marker else "X"
    separator = separator if separator is not None else "_"
    return f"{base_value}{separator}{marker * int(extra_pad)}"


def stringify_value(
    value: str | int,
    prefix: str,
    pad_width: int,
    *,
    amplification_extra_pad: int = 0,
    amplification_separator: str = "_",
    amplification_marker: str = "X",
) -> str:
    """
    Convert an integer identifier into a prefixed/padded string.
    """
    if value in ("", "\\N", None):  # type: ignore[comparison-overlap]
        return value if isinstance(value, str) else ""

    try:
        numeric = int(value)
    except (TypeError, ValueError):
        # Some schemas already use string-based surrogate keys; leave them alone.
        return str(value)
    base = f"{prefix}{numeric:0{pad_width}d}"
    return amplify_string(
        base,
        extra_pad=amplification_extra_pad,
        separator=amplification_separator,
        marker=amplification_marker,
    )


def stringify_row(row: List[str], table_name: str, rules: RulesMap) -> List[str]:
    """
    Apply stringification to every configured column in-place.
    """
    table_rules = rules.get(table_name)
    if not table_rules:
        return row

    for column_name, config in table_rules.items():
        index = config.get("index")
        prefix = config.get("prefix", "")
        pad_width = int(config.get("pad_width", 0))
        amplification_extra_pad = int(config.get("amplification_extra_pad", 0))
        amplification_separator = str(config.get("amplification_separator", "_"))
        amplification_marker = str(config.get("amplification_marker", "X"))

        if index is None or index >= len(row):
            continue

        raw_value = row[index]
        if raw_value in ("", "\\N"):
            continue

        row[index] = stringify_value(
            raw_value,
            prefix,
            pad_width,
            amplification_extra_pad=amplification_extra_pad,
            amplification_separator=amplification_separator,
            amplification_marker=amplification_marker,
        )

    return row


def process_tbl(
    infile: str | Path,
    outfile: str | Path,
    table_name: str,
    rules: RulesMap,
    null_injector: NullInjector | None = None,
    mcv_injector: "MCVInjector | None" = None,
    partition_label: str | None = None,
    stats: Mapping[str, LengthStats] | None = None,
) -> int:
    """
    Stream/rewrite a .tbl file, converting key columns and optionally injecting NULL skew.

    Returns the number of rows processed.
    """
    table_rules = rules.get(table_name) if rules else {}
    null_rules_present = bool(null_injector and table_name in null_injector.rules)
    mcv_rules_present = bool(mcv_injector and table_name in mcv_injector.rules)
    rewrite_required = bool(table_rules) or null_rules_present or mcv_rules_present

    in_path = Path(infile)
    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    processed = 0
    with in_path.open("r", encoding="utf-8", errors="surrogateescape") as src, out_path.open(
        "w", encoding="utf-8", errors="surrogateescape"
    ) as dst:
        if not rewrite_required:
            for raw_line in src:
                dst.write(raw_line)
                processed += 1
            return processed

        for raw_line in src:
            stripped = raw_line.rstrip("\n")
            if not stripped:
                dst.write(raw_line)
                processed += 1
                continue

            row = stripped.split("|")
            if null_injector:
                null_injector.apply_to_row(table_name, row, processed, partition_label)
            stringify_row(row, table_name, rules)
            if mcv_injector:
                mcv_injector.apply_to_row(table_name, row, processed, partition_label)
            if stats and table_rules:
                for column_name, cfg in table_rules.items():
                    idx = cfg.get("index")
                    if idx is None or idx >= len(row):
                        continue
                    value = row[idx]
                    if value in ("", "\\N"):
                        continue
                    stat_key = f"{table_name}.{column_name}"
                    stat = stats.get(stat_key)
                    if stat:
                        stat.observe(len(str(value)), table_name, column_name, processed)
            dst.write("|".join(row) + "\n")
            processed += 1

    return processed


def _column_index(schema: SchemaInfo, table_name: str, column_name: str) -> int | None:
    columns = schema.get(table_name, {}).get("columns") or []
    try:
        return columns.index(column_name)
    except ValueError:
        return None


@lru_cache(maxsize=None)
def _schema_cache(recast_key: Tuple[Tuple[str, str], ...] = ()) -> SchemaInfo:
    recast_types = dict(recast_key)
    return load_schema(SCHEMA_PATH, recast_types=recast_types)


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _infer_scale_factor(
    cfg: Mapping[str, Any],
    *,
    source_data_dir: Path | None = None,
    reference_db_hint: Path | None = None,
) -> int | None:
    raw = cfg.get("scale_factor")
    if raw is not None:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value

    for candidate in (source_data_dir, reference_db_hint):
        if candidate is None:
            continue
        match = re.search(r"sf(\d+)", str(candidate).lower())
        if match:
            try:
                value = int(match.group(1))
            except ValueError:
                value = 0
            if value > 0:
                return value
    return None


def _resolve_ndv_reference_duckdb(
    cfg: Mapping[str, Any],
    *,
    source_data_dir: Path | None = None,
    scale_factor: int | None = None,
) -> Path | None:
    explicit = cfg.get("ndv_reference_duckdb")
    env_explicit = os.getenv("PRODDS_DUCKDB_REF")
    candidates: List[Path] = []
    if explicit:
        candidates.append(Path(str(explicit)).expanduser().resolve())
    if env_explicit:
        candidates.append(Path(env_explicit).expanduser().resolve())
    if scale_factor:
        candidates.append(
            REPO_ROOT / "data" / f"prodds_sf{scale_factor}_str10.duckdb"
        )
    if source_data_dir is not None:
        match = re.search(r"sf(\d+)", str(source_data_dir).lower())
        if match:
            try:
                inferred_sf = int(match.group(1))
            except ValueError:
                inferred_sf = 0
            if inferred_sf > 0:
                candidates.append(
                    REPO_ROOT / "data" / f"prodds_sf{inferred_sf}_str10.duckdb"
                )

    seen: Set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    return None


def _schema_signature(schema: SchemaInfo) -> str:
    hasher = hashlib.blake2b(digest_size=16)
    for table_name in sorted(schema):
        hasher.update(table_name.lower().encode("utf-8"))
        hasher.update(b"\x1e")
        columns = schema.get(table_name, {}).get("columns") or []
        for column_name in columns:
            hasher.update(str(column_name).lower().encode("utf-8"))
            hasher.update(b"\x1f")
        hasher.update(b"\x1d")
    return hasher.hexdigest()


def _ndv_cache_path(
    *,
    cache_dir: Path,
    reference_db: Path,
    schema: SchemaInfo,
    seed: int,
    min_ndv: int,
    scale_factor: int | None,
) -> Path:
    stat = reference_db.stat()
    token = "|".join(
        [
            str(reference_db.resolve()),
            str(stat.st_size),
            str(stat.st_mtime_ns),
            _schema_signature(schema),
            str(int(seed)),
            str(int(min_ndv)),
            str(int(scale_factor or 0)),
        ]
    )
    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=12).hexdigest()
    return cache_dir / f"ndv_cache_{digest}.json"


def _compute_ndv_map_from_duckdb(schema: SchemaInfo, reference_db: Path) -> Dict[str, int]:
    ndv_map: Dict[str, int] = {}
    duckdb_bin = os.getenv("DUCKDB_BIN", "duckdb")
    for table_name, meta in schema.items():
        columns = meta.get("columns") or []
        if not columns:
            continue
        select_expr = ", ".join(
            f"COUNT(DISTINCT {_quote_ident(column_name)}) AS {_quote_ident(column_name)}"
            for column_name in columns
        )
        sql = (
            f"COPY (SELECT {select_expr} FROM {_quote_ident(table_name)}) "
            "TO STDOUT (FORMAT CSV, HEADER);"
        )
        proc = subprocess.run(
            [duckdb_bin, str(reference_db), "-c", sql],
            check=False,
            capture_output=True,
            text=True,
            timeout=NDV_QUERY_TIMEOUT_SECONDS,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"NDV guard query failed for table '{table_name}' on {reference_db}: "
                f"{(proc.stderr or proc.stdout).strip()}"
            )
        reader = csv.DictReader((proc.stdout or "").splitlines())
        row = next(reader, None)
        if row is None:
            continue
        for column_name in columns:
            raw = row.get(column_name)
            if raw is None:
                continue
            try:
                ndv = int(float(raw))
            except (TypeError, ValueError):
                continue
            ndv_map[f"{table_name.lower()}.{column_name.lower()}"] = max(0, ndv)
    return ndv_map


def _load_ndv_map(
    schema: SchemaInfo,
    cfg: Mapping[str, Any],
    *,
    source_data_dir: Path | None = None,
) -> Tuple[Dict[str, int], int, Path, Path, int | None]:
    raw_min_ndv = cfg.get("min_ndv_for_injection", MIN_NDV_FOR_INJECTION)
    try:
        min_ndv = int(raw_min_ndv)
    except (TypeError, ValueError):
        min_ndv = MIN_NDV_FOR_INJECTION
    min_ndv = max(0, min_ndv)

    scale_factor = _infer_scale_factor(cfg, source_data_dir=source_data_dir)
    reference_db = _resolve_ndv_reference_duckdb(
        cfg,
        source_data_dir=source_data_dir,
        scale_factor=scale_factor,
    )
    if reference_db is None:
        if min_ndv <= 0:
            # NDV guard disabled -- skip cardinality check, inject into all eligible columns
            return {}, 0, Path("/dev/null"), Path("/dev/null"), scale_factor
        raise RuntimeError(
            "NDV guard requires a baseline DuckDB file. "
            "Set --ndv-reference-duckdb or PRODDS_NDV_DUCKDB, "
            "or set --min-ndv-for-injection 0 to skip the guard."
        )

    raw_cache_dir = cfg.get("ndv_cache_dir") or os.getenv("PRODDS_NDV_CACHE_DIR")
    cache_dir = (
        Path(str(raw_cache_dir)).expanduser().resolve()
        if raw_cache_dir
        else DEFAULT_NDV_CACHE_DIR.resolve()
    )
    cache_dir.mkdir(parents=True, exist_ok=True)

    seed = int(cfg.get("seed", 0))
    cache_path = _ndv_cache_path(
        cache_dir=cache_dir,
        reference_db=reference_db,
        schema=schema,
        seed=seed,
        min_ndv=min_ndv,
        scale_factor=scale_factor,
    )

    if cache_path.exists():
        try:
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            values = payload.get("ndv_values", {}) if isinstance(payload, dict) else {}
            if isinstance(values, dict):
                cached = {
                    str(k).lower(): int(v)
                    for k, v in values.items()
                    if isinstance(k, str) and isinstance(v, (int, float))
                }
                if cached:
                    return cached, min_ndv, reference_db, cache_path, scale_factor
        except (OSError, json.JSONDecodeError, ValueError, TypeError):
            pass

    ndv_values = _compute_ndv_map_from_duckdb(schema, reference_db)
    cache_payload = {
        "reference_db": str(reference_db),
        "seed": seed,
        "min_ndv_for_injection": min_ndv,
        "scale_factor": scale_factor,
        "generated_at_unix": time.time(),
        "ndv_values": ndv_values,
    }
    cache_path.write_text(json.dumps(cache_payload, indent=2) + "\n", encoding="utf-8")
    return ndv_values, min_ndv, reference_db, cache_path, scale_factor


def _prefix_for_column(
    table_name: str,
    column_name: str,
    *,
    custom_prefixes: Mapping[str, Mapping[str, str]],
    table_prefixes: Mapping[str, str],
) -> str:
    custom_prefix = custom_prefixes.get(table_name, {}).get(column_name)
    if custom_prefix is not None:
        return custom_prefix

    lowered = column_name.lower()
    for suffix, prefix in DOMAIN_SUFFIX_PREFIXES:
        if lowered.endswith(suffix):
            return prefix

    return table_prefixes[table_name]


def build_rules(
    config: stringification_cfg.StringificationConfig | None = None,
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Build a mapping of table -> column -> rewrite settings.
    """
    cfg = stringify_rules()
    prefixes = cfg["prefixes"]
    base_pad_width = int(cfg["pad_width"])

    if config is None:
        config = stringification_cfg.build_stringification_config(base_pad_width=base_pad_width)

    selected = set(config.schema_selected)
    if not selected:
        return {}

    recast_map = {col: config.schema_type_map[col] for col in config.schema_selected}
    recast_key = tuple(sorted((k.lower(), v) for k, v in recast_map.items()))
    schema = _schema_cache(recast_key)
    rules: Dict[str, Dict[str, Dict[str, Any]]] = {}
    pad_width = int(config.payload.pad_width)

    for full_name in config.schema_selected:
        table_name, _, column_name = full_name.partition(".")
        if not table_name or not column_name:
            continue
        index = _column_index(schema, table_name, column_name)
        if index is None:
            continue
        table_rules = rules.setdefault(table_name, {})
        prefix = _prefix_for_column(
            table_name,
            column_name,
            custom_prefixes=CUSTOM_PREFIXES,
            table_prefixes=prefixes,
        )
        table_rules[column_name] = {
            "index": index,
            "prefix": prefix,
            "pad_width": pad_width,
            "amplification_extra_pad": int(config.amplification_extra_pad),
            "amplification_separator": str(config.amplification_separator),
            "amplification_marker": str(config.amplification_marker),
        }

    return rules


def _stable_unit_hash(seed: int, *parts: object) -> float:
    hasher = hashlib.blake2b(digest_size=8)
    hasher.update(str(seed).encode("utf-8"))
    for part in parts:
        hasher.update(b"\x1f")
        hasher.update(str(part).encode("utf-8", errors="backslashreplace"))
    return int.from_bytes(hasher.digest(), "big") / float(2**64)


def _type_category(data_type: str | None) -> str:
    if not data_type:
        return "text"
    lowered = data_type.lower()
    if "date" in lowered and "time" not in lowered:
        return "date"
    if "time" in lowered:
        return "timestamp"
    if "int" in lowered:
        return "int"
    if "dec" in lowered or "num" in lowered:
        return "decimal"
    if "char" in lowered or "string" in lowered or "text" in lowered:
        return "text"
    return "text"


def _string_length_limit(data_type: str | None) -> int:
    if not data_type:
        return 32
    match = re.search(r"\((\d+)\)", data_type)
    if match:
        try:
            return max(1, int(match.group(1)))
        except ValueError:
            return 32
    return 32


def _decimal_scale(data_type: str | None) -> int:
    if not data_type:
        return 0
    match = re.search(r"decimal\s*\(\s*\d+\s*,\s*(\d+)\s*\)", data_type, re.IGNORECASE)
    if match:
        try:
            return max(0, int(match.group(1)))
        except ValueError:
            return 0
    return 0


def _generate_mcv_values(table: str, column: str, data_type: str | None, seed: int) -> List[str]:
    """
    Produce a deterministic pool of candidate MCV values by type without scanning data.
    """
    category = _type_category(data_type)
    base = int(_stable_unit_hash(seed, table, column, "mcv_pool") * 10_000)
    values: List[str] = []

    if category == "int":
        start = base % 1_000
        values = [str(start + i) for i in range(20)]
    elif category == "decimal":
        scale = _decimal_scale(data_type)
        factor = 10 ** scale
        start = (base % 1_000) / float(factor)
        values = [f"{start + (i / float(factor)):.{scale}f}" for i in range(20)]
    elif category == "date":
        anchor = date(2000, 1, 1) + timedelta(days=base % 1800)
        values = [(anchor + timedelta(days=7 * i)).isoformat() for i in range(20)]
    elif category == "timestamp":
        anchor = date(2000, 1, 1) + timedelta(days=base % 1800)
        values = [f"{(anchor + timedelta(days=i)).isoformat()} 00:00:00" for i in range(20)]
    else:
        limit = _string_length_limit(data_type)
        token = f"mcv_{table}_{column}_{base:04d}"
        values = [f"{token}_{i}"[:limit] for i in range(20)]

    return values if values else [""]


@dataclass(frozen=True)
class NullColumnRule:
    index: int
    probability: float
    name: str


class NullInjector:
    def __init__(
        self,
        schema: SchemaInfo,
        cfg: Mapping[str, Any],
        *,
        source_data_dir: Path | None = None,
    ) -> None:
        marker = cfg.get("null_marker")
        self.null_marker: str = "" if marker is None else str(marker)
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.seed: int = int(cfg.get("seed", 0))
        self.selection_scope: str = str(cfg.get("selection_fraction_scope", "overall")).lower()
        self.target_column_fraction: float = float(cfg.get("column_selection_fraction", 0.0))
        self.target_column_fraction = max(0.0, min(1.0, self.target_column_fraction))
        self.include_hot_path_columns: bool = bool(cfg.get("include_hot_path_columns", True))
        if self.include_hot_path_columns:
            self.exclude_tables = set()
            self.exclude_columns = set()
            self.exclude_qualified = set()
        else:
            self.exclude_tables: Set[str] = {str(t).lower() for t in cfg.get("exclude_tables", []) if t}
            self.exclude_columns: Set[str] = {str(c).lower() for c in cfg.get("exclude_columns", []) if c}
            self.exclude_qualified: Set[str] = {
                str(c).lower() for c in cfg.get("exclude_qualified_columns", []) if c
            }
        self.explicit_probabilities: Dict[str, float] = self._normalize_explicit_probabilities(
            cfg.get("column_probabilities") or {}
        )
        self._buckets = self._normalize_buckets(cfg.get("buckets") or [])
        self.ndv_values: Dict[str, int] = {}
        self.min_ndv_for_injection: int = MIN_NDV_FOR_INJECTION
        self.ndv_reference_duckdb: str | None = None
        self.ndv_cache_path: str | None = None
        self.scale_factor: int | None = None
        self.ndv_guard_excluded: List[Dict[str, Any]] = []
        if self.enabled:
            (
                self.ndv_values,
                self.min_ndv_for_injection,
                reference_db,
                cache_path,
                self.scale_factor,
            ) = _load_ndv_map(schema, cfg, source_data_dir=source_data_dir)
            self.ndv_reference_duckdb = str(reference_db)
            self.ndv_cache_path = str(cache_path)
        self.eligible_columns: Dict[str, List[str]] = {}
        self.rules: Dict[str, List[NullColumnRule]] = {}
        self.total_columns: int = 0
        self.eligible_fraction: float = 0.0
        self.selection_fraction: float = 0.0

        if self.enabled and self._buckets:
            eligible_map, eligible_count, total_columns = self._collect_eligible(schema)
            self.total_columns = total_columns
            self.eligible_columns = eligible_map
            self.eligible_fraction = (
                float(eligible_count) / float(total_columns) if total_columns > 0 else 0.0
            )
            self.selection_fraction = self._calibrate_selection_fraction(self.eligible_fraction)
            if self.selection_fraction > 0.0 and eligible_count > 0:
                self.rules = self._build_rules(schema, eligible_map)
        if self.enabled and self.explicit_probabilities:
            self._apply_explicit_rules(schema)

    @property
    def has_rules(self) -> bool:
        return bool(self.rules)

    def tables_with_rules(self) -> Set[str]:
        return set(self.rules)

    def eligible_count(self, table_name: str) -> int:
        return len(self.eligible_columns.get(table_name, []))

    def apply_to_row(self, table_name: str, row: List[str], row_index: int, partition: str | None = None) -> None:
        if not self.enabled:
            return
        table_rules = self.rules.get(table_name)
        if not table_rules:
            return

        token = partition or ""
        for rule in table_rules:
            if _stable_unit_hash(self.seed, table_name, rule.name, token, row_index) < rule.probability:
                row[rule.index] = self.null_marker

    def _normalize_buckets(self, buckets: Sequence[Mapping[str, Any]]) -> List[Dict[str, float]]:
        normalized: List[Dict[str, float]] = []
        total_weight = 0.0
        for bucket in buckets:
            if not isinstance(bucket, Mapping):
                continue
            try:
                weight = float(bucket.get("weight", 0.0))
                lower = float(bucket.get("min"))
                upper = float(bucket.get("max"))
            except Exception:
                continue
            if weight <= 0.0:
                continue
            low_bound, high_bound = (lower, upper) if lower <= upper else (upper, lower)
            normalized.append({"weight": weight, "min": low_bound, "max": high_bound})
            total_weight += weight

        if total_weight <= 0.0:
            return []

        for bucket in normalized:
            bucket["weight"] = bucket["weight"] / total_weight
        return normalized

    def _normalize_explicit_probabilities(self, raw: Mapping[str, Any]) -> Dict[str, float]:
        out: Dict[str, float] = {}
        if not isinstance(raw, Mapping):
            return out
        for key, value in raw.items():
            qualified = str(key).strip().lower()
            if "." not in qualified:
                continue
            try:
                prob = float(value)
            except Exception:
                continue
            out[qualified] = max(0.0, min(1.0, prob))
        return out

    def _choose_bucket(self, table: str, column: str) -> Mapping[str, float]:
        pick = _stable_unit_hash(self.seed, table, column, "bucket")
        cumulative = 0.0
        for bucket in self._buckets:
            cumulative += bucket["weight"]
            if pick <= cumulative:
                return bucket
        return self._buckets[-1]

    def _derive_probability(self, table: str, column: str) -> float:
        if self.selection_fraction <= 0.0 or not self._buckets:
            return 0.0
        if _stable_unit_hash(self.seed, table, column, "select") >= self.selection_fraction:
            return 0.0

        bucket = self._choose_bucket(table, column)
        span = max(0.0, bucket["max"] - bucket["min"])
        sample = _stable_unit_hash(self.seed, table, column, "prob")
        return max(0.0, min(1.0, bucket["min"] + sample * span))

    def _collect_eligible(self, schema: SchemaInfo) -> Tuple[Dict[str, List[str]], int, int]:
        eligible_map: Dict[str, List[str]] = {}
        eligible_count = 0
        total_columns = 0
        ndv_excluded: List[Dict[str, Any]] = []

        for table_name, meta in schema.items():
            if table_name.lower() in self.exclude_tables:
                continue
            columns = meta.get("columns") or []
            total_columns += len(columns)
            key_like = {c.get("name", "").lower() for c in meta.get("key_like_columns") or []}
            key_like.update({c.get("name", "").lower() for c in meta.get("varchar_keys") or []})
            not_nulls = {c.get("name", "").lower() for c in meta.get("not_null_columns") or []}

            eligible = []
            ndv_guard_active = self.min_ndv_for_injection > 0
            for column_name in columns:
                lowered = column_name.lower()
                if lowered in key_like or lowered in not_nulls:
                    continue
                qualified = f"{table_name.lower()}.{lowered}"
                if lowered in self.exclude_columns or qualified in self.exclude_qualified:
                    continue
                if ndv_guard_active:
                    ndv_value = self.ndv_values.get(qualified)
                    if ndv_value is None or ndv_value < self.min_ndv_for_injection:
                        ndv_excluded.append(
                            {
                                "table": table_name,
                                "column": column_name,
                                "qualified": qualified,
                                "ndv": ndv_value,
                            }
                        )
                        continue
                eligible.append(column_name)
            if eligible:
                eligible_map[table_name] = eligible
                eligible_count += len(eligible)

        self.ndv_guard_excluded = ndv_excluded
        return eligible_map, eligible_count, total_columns

    def _calibrate_selection_fraction(self, eligible_fraction: float) -> float:
        if eligible_fraction <= 0.0:
            return 0.0
        scope = "eligible" if self.selection_scope == "eligible" else "overall"
        if scope == "eligible":
            return max(0.0, min(1.0, self.target_column_fraction))
        target_overall = self.target_column_fraction
        adjusted = target_overall / eligible_fraction
        return max(0.0, min(1.0, adjusted))

    def _build_rules(
        self, schema: SchemaInfo, eligible_map: Mapping[str, List[str]]
    ) -> Dict[str, List[NullColumnRule]]:
        rules: Dict[str, List[NullColumnRule]] = {}

        for table_name, meta in schema.items():
            columns = meta.get("columns") or []
            table_rules: List[NullColumnRule] = []
            eligible = eligible_map.get(table_name, [])

            for idx, column_name in enumerate(columns):
                if column_name not in eligible:
                    continue
                probability = self._derive_probability(table_name, column_name)
                if probability <= 0.0:
                    continue
                table_rules.append(NullColumnRule(index=idx, probability=probability, name=column_name))

            if table_rules:
                rules[table_name] = table_rules

        return rules

    def _apply_explicit_rules(self, schema: SchemaInfo) -> None:
        for qualified, probability in self.explicit_probabilities.items():
            if probability <= 0.0:
                continue
            table_name, column_name = qualified.split(".", 1)
            meta = schema.get(table_name)
            if not meta:
                continue
            columns = meta.get("columns") or []
            if not columns:
                continue
            index = next((i for i, col in enumerate(columns) if col.lower() == column_name), None)
            if index is None:
                continue
            canonical_name = columns[index]
            existing = self.rules.get(table_name, [])
            by_name = {rule.name.lower(): rule for rule in existing}
            by_name[canonical_name.lower()] = NullColumnRule(
                index=index,
                probability=probability,
                name=canonical_name,
            )
            self.rules[table_name] = sorted(by_name.values(), key=lambda rule: rule.index)


@dataclass(frozen=True)
class MCVColumnRule:
    index: int
    name: str
    f20: float
    f1: float
    values: List[str]


class MCVInjector:
    def __init__(
        self,
        schema: SchemaInfo,
        cfg: Mapping[str, Any],
        null_marker: str = "\\N",
        *,
        source_data_dir: Path | None = None,
    ) -> None:
        self.null_marker = "" if null_marker is None else str(null_marker)
        self.enabled: bool = bool(cfg.get("enabled", True))
        self.seed: int = int(cfg.get("seed", 0))
        self.selection_scope: str = str(cfg.get("selection_fraction_scope", "overall")).lower()
        self.target_column_fraction: float = float(cfg.get("column_selection_fraction", 0.0))
        self.target_column_fraction = max(0.0, min(1.0, self.target_column_fraction))
        self.include_hot_path_columns: bool = bool(cfg.get("include_hot_path_columns", True))
        if self.include_hot_path_columns:
            self.exclude_tables = set()
            self.exclude_columns = set()
            self.exclude_qualified = set()
        else:
            self.exclude_tables: Set[str] = {str(t).lower() for t in cfg.get("exclude_tables", []) if t}
            self.exclude_columns: Set[str] = {str(c).lower() for c in cfg.get("exclude_columns", []) if c}
            self.exclude_qualified: Set[str] = {
                str(c).lower() for c in cfg.get("exclude_qualified_columns", []) if c
            }
        self.explicit_top5_rules: Dict[str, Dict[str, Any]] = self._normalize_explicit_top5_rules(
            cfg.get("column_top5_rules") or {}
        )
        self._top20_buckets = self._normalize_buckets(cfg.get("top20_buckets") or [])
        self._r_buckets = self._normalize_buckets(cfg.get("r_buckets") or [])
        self.ndv_values: Dict[str, int] = {}
        self.min_ndv_for_injection: int = MIN_NDV_FOR_INJECTION
        self.ndv_reference_duckdb: str | None = None
        self.ndv_cache_path: str | None = None
        self.scale_factor: int | None = None
        self.ndv_guard_excluded: List[Dict[str, Any]] = []
        if self.enabled:
            (
                self.ndv_values,
                self.min_ndv_for_injection,
                reference_db,
                cache_path,
                self.scale_factor,
            ) = _load_ndv_map(schema, cfg, source_data_dir=source_data_dir)
            self.ndv_reference_duckdb = str(reference_db)
            self.ndv_cache_path = str(cache_path)

        self.rules: Dict[str, List[MCVColumnRule]] = {}
        self.eligible_columns: Dict[str, List[str]] = {}
        self.total_columns: int = 0
        self.eligible_fraction: float = 0.0
        self.selection_fraction: float = 0.0
        if self.enabled and self._top20_buckets and self._r_buckets:
            eligible_map, eligible_count, total_columns = self._collect_eligible(schema)
            self.total_columns = total_columns
            self.eligible_columns = eligible_map
            self.eligible_fraction = float(eligible_count) / float(total_columns) if total_columns else 0.0
            self.selection_fraction = self._calibrate_selection_fraction(self.eligible_fraction)
            if self.selection_fraction > 0.0 and eligible_count > 0:
                self.rules = self._build_rules(schema, eligible_map)
        if self.enabled and self.explicit_top5_rules:
            self._apply_explicit_top5_rules(schema)

    @property
    def has_rules(self) -> bool:
        return bool(self.rules)

    def tables_with_rules(self) -> Set[str]:
        return set(self.rules)

    def apply_to_row(
        self, table_name: str, row: List[str], row_index: int, partition: str | None = None
    ) -> None:
        if not self.enabled:
            return
        table_rules = self.rules.get(table_name)
        if not table_rules:
            return

        token = partition or ""
        for rule in table_rules:
            if rule.f20 <= 0.0:
                continue
            if rule.index >= len(row):
                continue
            current = row[rule.index]
            if current in ("", self.null_marker, "\\N"):
                continue
            h = _stable_unit_hash(self.seed, table_name, rule.name, token, row_index)
            if h < rule.f1:
                row[rule.index] = rule.values[0]
            elif h < rule.f20 and len(rule.values) > 1:
                pick_hash = _stable_unit_hash(self.seed, table_name, rule.name, token, "mcv", row_index)
                choice = 1 + int(pick_hash * (len(rule.values) - 1))
                choice = min(max(choice, 1), len(rule.values) - 1)
                row[rule.index] = rule.values[choice]

    def _normalize_buckets(self, buckets: Sequence[Mapping[str, Any]]) -> List[Dict[str, float]]:
        normalized: List[Dict[str, float]] = []
        total_weight = 0.0
        for bucket in buckets:
            if not isinstance(bucket, Mapping):
                continue
            try:
                weight = float(bucket.get("weight", 0.0))
                lower = float(bucket.get("min"))
                upper = float(bucket.get("max"))
            except Exception:
                continue
            if weight <= 0.0:
                continue
            low_bound, high_bound = (lower, upper) if lower <= upper else (upper, lower)
            normalized.append({"weight": weight, "min": low_bound, "max": high_bound})
            total_weight += weight

        if total_weight <= 0.0:
            return []

        for bucket in normalized:
            bucket["weight"] = bucket["weight"] / total_weight
        return normalized

    def _normalize_explicit_top5_rules(self, raw: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        if not isinstance(raw, Mapping):
            return out
        for key, value in raw.items():
            qualified = str(key).strip().lower()
            if "." not in qualified or not isinstance(value, Mapping):
                continue
            try:
                share = float(value.get("share", 0.0))
            except Exception:
                continue
            share = max(0.0, min(1.0, share))
            values = value.get("values") or []
            normalized_values = [str(v) for v in values if v is not None]
            if not normalized_values:
                continue
            out[qualified] = {"share": share, "values": normalized_values}
        return out

    def _choose_bucket(self, table: str, column: str, buckets: Sequence[Mapping[str, float]], token: str) -> Mapping[str, float]:
        pick = _stable_unit_hash(self.seed, table, column, token)
        cumulative = 0.0
        for bucket in buckets:
            cumulative += bucket["weight"]
            if pick <= cumulative:
                return bucket
        return buckets[-1]

    def _derive_value(self, table: str, column: str, token: str, buckets: Sequence[Mapping[str, float]]) -> float:
        if not buckets:
            return 0.0
        bucket = self._choose_bucket(table, column, buckets, token)
        span = max(0.0, bucket["max"] - bucket["min"])
        sample = _stable_unit_hash(self.seed, table, column, token, "val")
        return max(0.0, min(1.0, bucket["min"] + sample * span))

    def _collect_eligible(self, schema: SchemaInfo) -> Tuple[Dict[str, List[str]], int, int]:
        eligible_map: Dict[str, List[str]] = {}
        eligible_count = 0
        total_columns = 0
        ndv_excluded: List[Dict[str, Any]] = []

        for table_name, meta in schema.items():
            if table_name.lower() in self.exclude_tables:
                continue
            columns = meta.get("columns") or []
            total_columns += len(columns)
            key_like = {c.get("name", "").lower() for c in meta.get("key_like_columns") or []}
            key_like.update({c.get("name", "").lower() for c in meta.get("varchar_keys") or []})
            not_nulls = {c.get("name", "").lower() for c in meta.get("not_null_columns") or []}

            eligible = []
            ndv_guard_active = self.min_ndv_for_injection > 0
            for column_name in columns:
                lowered = column_name.lower()
                if lowered in key_like or lowered in not_nulls:
                    continue
                qualified = f"{table_name.lower()}.{lowered}"
                if lowered in self.exclude_columns or qualified in self.exclude_qualified:
                    continue
                if ndv_guard_active:
                    ndv_value = self.ndv_values.get(qualified)
                    if ndv_value is None or ndv_value < self.min_ndv_for_injection:
                        ndv_excluded.append(
                            {
                                "table": table_name,
                                "column": column_name,
                                "qualified": qualified,
                                "ndv": ndv_value,
                            }
                        )
                        continue
                eligible.append(column_name)
            if eligible:
                eligible_map[table_name] = eligible
                eligible_count += len(eligible)

        self.ndv_guard_excluded = ndv_excluded
        return eligible_map, eligible_count, total_columns

    def _calibrate_selection_fraction(self, eligible_fraction: float) -> float:
        if eligible_fraction <= 0.0:
            return 0.0
        scope = "eligible" if self.selection_scope == "eligible" else "overall"
        if scope == "eligible":
            return max(0.0, min(1.0, self.target_column_fraction))
        adjusted = self.target_column_fraction / eligible_fraction
        return max(0.0, min(1.0, adjusted))

    def _build_rules(
        self, schema: SchemaInfo, eligible_map: Mapping[str, List[str]]
    ) -> Dict[str, List[MCVColumnRule]]:
        rules: Dict[str, List[MCVColumnRule]] = {}

        for table_name, meta in schema.items():
            columns = meta.get("columns") or []
            column_types = meta.get("column_types") or {}
            table_rules: List[MCVColumnRule] = []
            eligible = eligible_map.get(table_name, [])

            for idx, column_name in enumerate(columns):
                if column_name not in eligible:
                    continue
                if self.selection_fraction <= 0.0:
                    continue
                if _stable_unit_hash(self.seed, table_name, column_name, "select-mcv") >= self.selection_fraction:
                    continue

                f20 = self._derive_value(table_name, column_name, "f20", self._top20_buckets)
                if f20 <= 0.0:
                    continue
                r = self._derive_value(table_name, column_name, "r", self._r_buckets)
                f1 = max(0.0, min(f20, f20 * r))
                values = _generate_mcv_values(table_name, column_name, column_types.get(column_name), self.seed)
                table_rules.append(
                    MCVColumnRule(
                        index=idx,
                        name=column_name,
                        f20=f20,
                        f1=f1,
                        values=values,
                    )
                )

            if table_rules:
                rules[table_name] = table_rules

        return rules

    def _apply_explicit_top5_rules(self, schema: SchemaInfo) -> None:
        for qualified, cfg in self.explicit_top5_rules.items():
            share = float(cfg.get("share", 0.0))
            values = [str(v) for v in (cfg.get("values") or []) if v is not None]
            if share <= 0.0 or not values:
                continue
            table_name, column_name = qualified.split(".", 1)
            meta = schema.get(table_name)
            if not meta:
                continue
            columns = meta.get("columns") or []
            if not columns:
                continue
            index = next((i for i, col in enumerate(columns) if col.lower() == column_name), None)
            if index is None:
                continue
            canonical_name = columns[index]
            f1 = min(share, share / max(1, len(values)))
            explicit_rule = MCVColumnRule(
                index=index,
                name=canonical_name,
                f20=share,
                f1=f1,
                values=values,
            )
            existing = self.rules.get(table_name, [])
            by_name = {rule.name.lower(): rule for rule in existing}
            by_name[canonical_name.lower()] = explicit_rule
            self.rules[table_name] = sorted(by_name.values(), key=lambda rule: rule.index)

def _table_name_from_filename(filename: str) -> str | None:
    """
    Extract the TPC-DS table name from a .tbl filename (handling partition suffixes).
    """
    name = filename
    if "." in name:
        name = name.split(".", 1)[0]
    parts = name.split("_")
    while parts and parts[-1].isdigit():
        parts.pop()
    if not parts:
        return None
    return "_".join(parts).lower()


def _resolve_max_workers(requested: int | None, task_count: int) -> int:
    env_override: int | None = None
    if requested is None:
        env_value = os.getenv(WORKER_ENV_VAR)
        if env_value:
            try:
                env_override = int(env_value)
            except ValueError:
                env_override = None
    worker_target = requested if requested is not None else env_override
    if worker_target is None or worker_target <= 0:
        worker_target = os.cpu_count() or 1
    worker_target = min(worker_target, task_count) if task_count else worker_target
    return max(1, worker_target)


def _resolve_backend(requested: str | None = None) -> str:
    raw = (requested or os.getenv(BACKEND_ENV_VAR) or "auto").strip().lower()
    aliases = {"c++": "cpp", "cxx": "cpp"}
    raw = aliases.get(raw, raw)
    if raw not in {"auto", "cpp", "python"}:
        raise ValueError(
            f"Unsupported stringify backend '{raw}'. Use one of: auto, cpp, python."
        )
    return raw


def _resolve_cpp_binary(build_if_missing: bool = False) -> Path | None:
    cpp_dir = Path(__file__).resolve().parent
    source = cpp_dir / "stringify_cpp.cpp"
    makefile = cpp_dir / "Makefile"
    candidates = [cpp_dir / name for name in CPP_BINARY_CANDIDATES]

    def _pick_existing() -> Path | None:
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    binary = _pick_existing()
    if binary is not None:
        try:
            binary_mtime = binary.stat().st_mtime
            if source.exists() and source.stat().st_mtime > binary_mtime:
                build_if_missing = True
            if makefile.exists() and makefile.stat().st_mtime > binary_mtime:
                build_if_missing = True
        except OSError:
            build_if_missing = True
    if binary is None and not build_if_missing:
        return None
    if build_if_missing:
        try:
            print(f"[stringify] Building C++ backend binary...")
            subprocess.run(
                ["make", "stringify_cpp"],
                cwd=str(cpp_dir),
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError):
            return None
        binary = _pick_existing()
    return binary


def _run_cpp_rewrite(
    output_path: Path,
    *,
    max_workers: int | None,
    stringification_level: int | None,
    stringification_preset: str | None,
    allow_extended_levels: bool,
    str_plus_enabled: bool,
    str_plus_max_level: int,
    str_plus_pad_step: int,
    str_plus_separator: str,
    str_plus_marker: str,
    enable_stringify: bool | None,
    enable_nulls: bool | None,
    null_seed: int | None,
    null_marker: str | None,
    null_profile: str | None,
    include_hot_paths: bool | None,
    min_ndv_for_injection: int | None,
    ndv_reference_duckdb: str | None,
    ndv_cache_dir: str | None,
    scale_factor: int | None,
    null_overrides: Mapping[str, Any] | None,
    enable_mcv: bool | None,
    mcv_seed: int | None,
    mcv_profile: str | None,
    mcv_overrides: Mapping[str, Any] | None,
    backend_mode: str,
) -> tuple[int, int] | None:
    binary = _resolve_cpp_binary(build_if_missing=True)
    if binary is None:
        print(f"[stringify] C++ backend requested but binary could not be built/resolved. Falling back to Python backend.")
        if backend_mode == "cpp":
            raise RuntimeError("C++ stringify backend requested but binary could not be built/resolved.")
        return None

    with tempfile.TemporaryDirectory(prefix="stringify_cpp_") as tmpdir:
        tmp = Path(tmpdir)
        rules_path = tmp / "rules.yml"
        summary_path = tmp / "summary.json"
        export_rewrite_rules(
            rules_path,
            enable_stringify=enable_stringify,
            stringification_level=stringification_level,
            stringification_preset=stringification_preset,
            allow_extended_levels=allow_extended_levels,
            str_plus_enabled=str_plus_enabled,
            str_plus_max_level=str_plus_max_level,
            str_plus_pad_step=str_plus_pad_step,
            str_plus_separator=str_plus_separator,
            str_plus_marker=str_plus_marker,
            enable_nulls=enable_nulls,
            null_seed=null_seed,
            null_marker=null_marker,
            null_profile=null_profile,
            include_hot_paths=include_hot_paths,
            min_ndv_for_injection=min_ndv_for_injection,
            ndv_reference_duckdb=ndv_reference_duckdb,
            ndv_cache_dir=ndv_cache_dir,
            scale_factor=scale_factor,
            null_overrides=null_overrides,
            enable_mcv=enable_mcv,
            mcv_seed=mcv_seed,
            mcv_profile=mcv_profile,
            mcv_overrides=mcv_overrides,
            source_data_dir=output_path,
        )
        args = [
            str(binary),
            "--output-dir",
            str(output_path),
            "--rules-file",
            str(rules_path),
            "--summary-json",
            str(summary_path),
        ]
        if max_workers is not None:
            args.extend(["--max-workers", str(max_workers)])

        result = subprocess.run(args, check=False)
        if result.returncode != 0:
            if backend_mode == "cpp":
                raise RuntimeError(
                    f"C++ stringify backend failed with exit code {result.returncode}."
                )
            return None

        if not summary_path.exists():
            if backend_mode == "cpp":
                raise RuntimeError("C++ stringify backend did not produce summary output.")
            return None

        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            if backend_mode == "cpp":
                raise RuntimeError("Could not parse C++ stringify summary output.") from exc
            return None

        files = int(payload.get("files_rewritten", 0))
        rows = int(payload.get("rows_rewritten", 0))
        return files, rows


def _rewrite_single(
    tbl_file: Path,
    table_name: str,
    rules: RulesMap,
    null_injector: NullInjector | None,
    mcv_injector: "MCVInjector | None",
    stat_keys: List[str] | None,
) -> Tuple[Path, int, Dict[str, Mapping[str, int]]]:
    # Create process-local stats (no shared state, no Lock needed for pickling)
    local_stats: Dict[str, LengthStats] | None = None
    if stat_keys:
        local_stats = {
            key: LengthStats(sample_size=STATS_SAMPLE_SIZE, seed=STATS_SEED)
            for key in stat_keys
            if key.startswith(f"{table_name}.")
        }
    tmp_path = tbl_file.with_suffix(tbl_file.suffix + ".tmp")
    rows = process_tbl(
        tbl_file,
        tmp_path,
        table_name,
        rules,
        null_injector=null_injector,
        mcv_injector=mcv_injector,
        partition_label=tbl_file.name,
        stats=local_stats,
    )
    tmp_path.replace(tbl_file)
    summaries: Dict[str, Mapping[str, int]] = {}
    if local_stats:
        for key, stat in local_stats.items():
            s = stat.summary()
            if s:
                summaries[key] = s
    return tbl_file, rows, summaries


def _iter_with_progress(iterable: Iterable[Any], total: int) -> Iterable[Any]:
    if tqdm is None:
        completed = 0
        for item in iterable:
            completed += 1
            print(f"[stringify] processed {completed}/{total} files...")
            yield item
        return

    with tqdm(total=total, unit="file", desc="[stringify]", leave=False) as bar:
        for item in iterable:
            yield item
            bar.update(1)


def rewrite_tbl_directory(
    output_dir: Path,
    max_workers: int | None = None,
    backend: str | None = None,
    enable_stringify: bool | None = None,
    stringification_level: int | None = None,
    stringification_preset: str | None = None,
    allow_extended_levels: bool = True,
    str_plus_enabled: bool | None = None,
    str_plus_max_level: int = stringification_cfg.DEFAULT_STR_PLUS_MAX_LEVEL,
    str_plus_pad_step: int = 2,
    str_plus_separator: str = "_",
    str_plus_marker: str = "X",
    enable_nulls: bool | None = None,
    null_seed: int | None = None,
    null_marker: str | None = None,
    null_profile: str | None = None,
    include_hot_paths: bool | None = None,
    min_ndv_for_injection: int | None = None,
    ndv_reference_duckdb: str | None = None,
    ndv_cache_dir: str | None = None,
    scale_factor: int | None = None,
    null_overrides: Mapping[str, Any] | None = None,
    enable_mcv: bool | None = None,
    mcv_seed: int | None = None,
    mcv_profile: str | None = None,
    mcv_overrides: Mapping[str, Any] | None = None,
) -> Tuple[int, int]:
    """
    Rewrite every .tbl/.dat file in the directory using the configured stringify rules and
    optional NULL skew and MCV skew injection.

    Args:
        output_dir: Directory that contains .tbl files from dsdgen.
        max_workers: Optional cap for parallel rewrite workers. You can also set
            the STRINGIFY_MAX_WORKERS environment variable.
        backend: Rewrite backend (`auto`, `cpp`, `python`). Defaults to env
            `STRINGIFY_BACKEND` or `auto`.
        enable_stringify: Override for stringification toggle. Defaults to stringification level selection.
        stringification_level: Stringification level (1-15; default: 10). STR>10 extends string length.
        stringification_preset: Named stringification preset (vanilla, low, medium, high, production).
        allow_extended_levels: Allow levels beyond STR10 (default: True).
        str_plus_enabled: Enable STR+ amplification semantics for STR>10 (auto-detected from level if None).
        str_plus_max_level: Maximum accepted level when STR+ is enabled.
        str_plus_pad_step: Extra suffix characters added per level above STR10.
        str_plus_separator: Separator inserted before STR+ amplification suffix.
        str_plus_marker: Marker repeated in STR+ amplification suffix.
        enable_nulls: Override for NULL skew toggle. Defaults to config value.
        null_seed: Optional deterministic seed override for NULL injection decisions.
        null_marker: Override the marker used to represent NULLs in outputs.
        null_profile: Optional profile name to pull from config/null_profiles.yml.
        null_overrides: Additional override mapping merged into the null configuration.
        enable_mcv: Override for MCV skew toggle. Defaults to config value.
        mcv_seed: Optional deterministic seed override for MCV decisions.
        mcv_profile: Optional profile name from config/mcv_profiles.yml.
        mcv_overrides: Additional override mapping merged into the MCV configuration.

    Returns:
        (files_rewritten, total_rows_processed)
    """
    output_path = Path(output_dir).resolve()
    print(f"[stringify] Starting rewrite in directory: {output_path}")
    stringify_cfg = stringify_rules()
    base_pad_width = int(stringify_cfg["pad_width"])
    config = stringification_cfg.build_stringification_config(
        level=stringification_level,
        preset=stringification_preset,
        base_schema_path=SCHEMA_PATH,
        prod_schema_path=stringification_cfg.DEFAULT_PROD_SCHEMA,
        base_pad_width=base_pad_width,
        allow_extended_levels=allow_extended_levels,
        str_plus_enabled=str_plus_enabled,
        str_plus_max_level=str_plus_max_level,
        str_plus_pad_step=str_plus_pad_step,
        str_plus_separator=str_plus_separator,
        str_plus_marker=str_plus_marker,
    )
    stringify_enabled = (config.k_schema > 0) if enable_stringify is None else enable_stringify
    rules = build_rules(config) if stringify_enabled else {}
    recast_map = {col: config.schema_type_map[col] for col in config.schema_selected} if stringify_enabled else {}
    recast_key = tuple(sorted((k.lower(), v) for k, v in recast_map.items()))
    schema = _schema_cache(recast_key)

    null_cfg = null_skew_rules(overrides=null_overrides, profile=null_profile)
    if enable_nulls is not None:
        null_cfg["enabled"] = enable_nulls
    if null_seed is not None:
        null_cfg["seed"] = null_seed
    if null_marker is not None:
        null_cfg["null_marker"] = null_marker
    if include_hot_paths is not None:
        null_cfg["include_hot_path_columns"] = bool(include_hot_paths)
    if min_ndv_for_injection is not None:
        null_cfg["min_ndv_for_injection"] = int(min_ndv_for_injection)
    if ndv_reference_duckdb is not None:
        null_cfg["ndv_reference_duckdb"] = str(ndv_reference_duckdb)
    if ndv_cache_dir is not None:
        null_cfg["ndv_cache_dir"] = str(ndv_cache_dir)
    if scale_factor is not None:
        null_cfg["scale_factor"] = int(scale_factor)

    null_injector = NullInjector(schema, null_cfg, source_data_dir=output_path)
    has_null_rules = null_injector.enabled and null_injector.has_rules

    mcv_cfg = mcv_skew_rules(overrides=mcv_overrides, profile=mcv_profile)
    if enable_mcv is not None:
        mcv_cfg["enabled"] = enable_mcv
    if mcv_seed is not None:
        mcv_cfg["seed"] = mcv_seed
    if include_hot_paths is not None:
        mcv_cfg["include_hot_path_columns"] = bool(include_hot_paths)
    if min_ndv_for_injection is not None:
        mcv_cfg["min_ndv_for_injection"] = int(min_ndv_for_injection)
    if ndv_reference_duckdb is not None:
        mcv_cfg["ndv_reference_duckdb"] = str(ndv_reference_duckdb)
    if ndv_cache_dir is not None:
        mcv_cfg["ndv_cache_dir"] = str(ndv_cache_dir)
    if scale_factor is not None:
        mcv_cfg["scale_factor"] = int(scale_factor)
    mcv_injector = MCVInjector(
        schema,
        mcv_cfg,
        null_marker=null_cfg.get("null_marker", ""),
        source_data_dir=output_path,
    )
    has_mcv_rules = mcv_injector.enabled and mcv_injector.has_rules

    if null_injector.enabled:
        print(
            "[null-skew] pool="
            f"{'hot-path' if null_injector.include_hot_path_columns else 'conservative'} "
            f"min_ndv={null_injector.min_ndv_for_injection} "
            f"eligible={sum(len(v) for v in null_injector.eligible_columns.values())} "
            f"ndv_excluded={len(null_injector.ndv_guard_excluded)}"
        )
        for entry in null_injector.ndv_guard_excluded:
            print(
                f"[null-skew][ndv-guard] excluded {entry['qualified']} "
                f"(ndv={entry['ndv'] if entry['ndv'] is not None else 'missing'})"
            )
    if mcv_injector.enabled:
        print(
            "[mcv-skew] pool="
            f"{'hot-path' if mcv_injector.include_hot_path_columns else 'conservative'} "
            f"min_ndv={mcv_injector.min_ndv_for_injection} "
            f"eligible={sum(len(v) for v in mcv_injector.eligible_columns.values())} "
            f"ndv_excluded={len(mcv_injector.ndv_guard_excluded)}"
        )
        for entry in mcv_injector.ndv_guard_excluded:
            print(
                f"[mcv-skew][ndv-guard] excluded {entry['qualified']} "
                f"(ndv={entry['ndv'] if entry['ndv'] is not None else 'missing'})"
            )

    backend_mode = _resolve_backend(backend)
    print(f"[stringify] rewrite backend mode: {backend_mode}")
    stats: Dict[str, LengthStats] = {}
    if stringify_enabled and rules:
        for table_name, table_rules in rules.items():
            for column_name in table_rules:
                stats[f"{table_name}.{column_name}"] = LengthStats(
                    sample_size=STATS_SAMPLE_SIZE,
                    seed=STATS_SEED,
                )

    tasks: List[Tuple[Path, str]] = []
    data_files: List[Path] = []
    for extension in DATA_EXTENSIONS:
        data_files.extend(sorted(output_path.glob(f"*{extension}")))

    for data_file in data_files:
        table_name = _table_name_from_filename(data_file.name)
        if not table_name:
            continue
        table_has_stringify = table_name in rules
        table_has_nulls = has_null_rules and table_name in null_injector.rules
        table_has_mcv = has_mcv_rules and table_name in mcv_injector.rules
        if not table_has_stringify and not table_has_nulls and not table_has_mcv:
            continue
        tasks.append((data_file, table_name))

    task_count = len(tasks)
    if task_count == 0:
        print("[stringify] No eligible .tbl files found to rewrite.")
        manifest_path = output_path / stringification_cfg.DATA_MANIFEST_NAME
        stringification_cfg.write_json(
            manifest_path,
            {
                "stringification_level": config.level,
                "stringification_preset": config.preset,
                "intensity": config.intensity,
                "payload": {
                    "pad_width": config.payload.pad_width,
                    "min_pad_width": config.payload.min_pad_width,
                    "base_pad_width": config.payload.base_pad_width,
                    "regime": config.payload.regime,
                },
                "stringification_enabled": bool(stringify_enabled and rules),
                "str_plus_enabled": bool(config.str_plus_enabled),
                "amplification": {
                    "enabled": bool(config.str_plus_enabled),
                    "extra_pad": int(config.amplification_extra_pad),
                    "pad_step": int(config.amplification_pad_step),
                    "separator": str(config.amplification_separator),
                    "marker": str(config.amplification_marker),
                    "regime": (
                        f"str_plus(extra_pad={config.amplification_extra_pad})"
                        if config.str_plus_enabled
                        else "none"
                    ),
                },
                "recast_columns": list(config.schema_selected),
                "touched_columns": sorted(stats),
                "touched_columns_count": len(stats),
                "files_rewritten": 0,
                "rows_rewritten": 0,
                "length_summary": {},
                "null_profile": null_cfg.get("profile"),
                "null_tier_alias": null_cfg.get("tier_alias"),
                "null_seed": null_cfg.get("seed"),
                "include_hot_paths": bool(null_cfg.get("include_hot_path_columns", True)),
                "min_ndv_for_injection": int(
                    null_cfg.get("min_ndv_for_injection", MIN_NDV_FOR_INJECTION)
                ),
                "ndv_reference_duckdb": null_injector.ndv_reference_duckdb,
                "ndv_cache_path": null_injector.ndv_cache_path,
                "null_ndv_guard_excluded": null_injector.ndv_guard_excluded,
                "mcv_profile": mcv_cfg.get("profile"),
                "mcv_tier_alias": mcv_cfg.get("tier_alias"),
                "mcv_seed": mcv_cfg.get("seed"),
                "mcv_ndv_guard_excluded": mcv_injector.ndv_guard_excluded,
                "stats_sample_size": STATS_SAMPLE_SIZE,
                "stats_seed": STATS_SEED,
                "rewrite_backend": "none",
            },
        )
        return 0, 0

    worker_count = _resolve_max_workers(max_workers, task_count)
    feature_flags = []
    if stringify_enabled and rules:
        feature_flags.append("stringify")
    if has_null_rules:
        feature_flags.append("null-skew")
    if has_mcv_rules:
        feature_flags.append("mcv-skew")
    features = ", ".join(feature_flags) if feature_flags else "noop"
    print(
        f"[stringify] Starting rewrite of {task_count} .tbl files using {worker_count} workers "
        f"(configure via --max-workers or ${WORKER_ENV_VAR}); features: {features}."
    )

    start = time.perf_counter()
    files_rewritten = 0
    total_rows = 0
    used_backend = "python"
    if backend_mode in {"auto", "cpp"}:
        cpp_result = _run_cpp_rewrite(
            output_path,
            max_workers=max_workers,
            stringification_level=stringification_level,
            stringification_preset=stringification_preset,
            allow_extended_levels=allow_extended_levels,
            str_plus_enabled=str_plus_enabled,
            str_plus_max_level=str_plus_max_level,
            str_plus_pad_step=str_plus_pad_step,
            str_plus_separator=str_plus_separator,
            str_plus_marker=str_plus_marker,
            enable_stringify=enable_stringify,
            enable_nulls=enable_nulls,
            null_seed=null_seed,
            null_marker=null_marker,
            null_profile=null_profile,
            include_hot_paths=include_hot_paths,
            min_ndv_for_injection=min_ndv_for_injection,
            ndv_reference_duckdb=ndv_reference_duckdb,
            ndv_cache_dir=ndv_cache_dir,
            scale_factor=scale_factor,
            null_overrides=null_overrides,
            enable_mcv=enable_mcv,
            mcv_seed=mcv_seed,
            mcv_profile=mcv_profile,
            mcv_overrides=mcv_overrides,
            backend_mode=backend_mode,
        )
        if cpp_result is not None:
            files_rewritten, total_rows = cpp_result
            used_backend = "cpp"

    if used_backend == "python":
        print(f"[stringify] Using Python backend for rewrite with {worker_count} workers.")
        stat_keys = list(stats.keys()) if stats else None
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(
                    _rewrite_single,
                    tbl_file,
                    table_name,
                    rules,
                    null_injector if has_null_rules else None,
                    mcv_injector if has_mcv_rules else None,
                    stat_keys,
                )
                for tbl_file, table_name in tasks
            ]
            for future in _iter_with_progress(as_completed(futures), total=len(futures)):
                tbl_file, rows, summaries = future.result()
                files_rewritten += 1
                total_rows += rows
                for key, summary in summaries.items():
                    if key in stats:
                        # Merge: overwrite with per-process result (each file is
                        # processed by exactly one worker, so no conflicts)
                        existing = stats[key]
                        existing.count += summary.get("count", 0)
                        if existing.min_len is None or summary.get("min", existing.min_len) < existing.min_len:
                            existing.min_len = summary.get("min")
                        if existing.max_len is None or summary.get("max", existing.max_len) > existing.max_len:
                            existing.max_len = summary.get("max")
                if tqdm is None:
                    print(f"[stringify] finished {tbl_file.name} ({rows} rows).")

    duration = time.perf_counter() - start
    print(
        f"[stringify] Completed rewrite of {files_rewritten} files ({total_rows} rows) "
        f"in {duration:.2f}s (backend={used_backend})."
    )
    length_summary: Dict[str, Mapping[str, int]] = {}
    if used_backend == "python":
        for key, stat in stats.items():
            summary = stat.summary()
            if summary:
                length_summary[key] = summary
    manifest_path = output_path / stringification_cfg.DATA_MANIFEST_NAME
    stringification_cfg.write_json(
        manifest_path,
        {
            "stringification_level": config.level,
            "stringification_preset": config.preset,
            "intensity": config.intensity,
            "payload": {
                "pad_width": config.payload.pad_width,
                "min_pad_width": config.payload.min_pad_width,
                "base_pad_width": config.payload.base_pad_width,
                "regime": config.payload.regime,
            },
            "stringification_enabled": bool(stringify_enabled and rules),
            "str_plus_enabled": bool(config.str_plus_enabled),
            "amplification": {
                "enabled": bool(config.str_plus_enabled),
                "extra_pad": int(config.amplification_extra_pad),
                "pad_step": int(config.amplification_pad_step),
                "separator": str(config.amplification_separator),
                "marker": str(config.amplification_marker),
                "regime": (
                    f"str_plus(extra_pad={config.amplification_extra_pad})"
                    if config.str_plus_enabled
                    else "none"
                ),
            },
            "recast_columns": list(config.schema_selected),
            "touched_columns": sorted(stats),
            "touched_columns_count": len(stats),
            "files_rewritten": files_rewritten,
            "rows_rewritten": total_rows,
            "length_summary": length_summary,
            "null_profile": null_cfg.get("profile"),
            "null_tier_alias": null_cfg.get("tier_alias"),
            "null_seed": null_cfg.get("seed"),
            "include_hot_paths": bool(null_cfg.get("include_hot_path_columns", True)),
            "min_ndv_for_injection": int(
                null_cfg.get("min_ndv_for_injection", MIN_NDV_FOR_INJECTION)
            ),
            "ndv_reference_duckdb": null_injector.ndv_reference_duckdb,
            "ndv_cache_path": null_injector.ndv_cache_path,
            "null_ndv_guard_excluded": null_injector.ndv_guard_excluded,
            "mcv_profile": mcv_cfg.get("profile"),
            "mcv_tier_alias": mcv_cfg.get("tier_alias"),
            "mcv_seed": mcv_cfg.get("seed"),
            "mcv_ndv_guard_excluded": mcv_injector.ndv_guard_excluded,
            "stats_sample_size": STATS_SAMPLE_SIZE,
            "stats_seed": STATS_SEED,
            "rewrite_backend": used_backend,
        },
    )
    return files_rewritten, total_rows


def build_rewrite_rules(
    *,
    enable_stringify: bool | None = None,
    stringification_level: int | None = None,
    stringification_preset: str | None = None,
    allow_extended_levels: bool = True,
    str_plus_enabled: bool | None = None,
    str_plus_max_level: int = stringification_cfg.DEFAULT_STR_PLUS_MAX_LEVEL,
    str_plus_pad_step: int = 2,
    str_plus_separator: str = "_",
    str_plus_marker: str = "X",
    enable_nulls: bool | None = None,
    null_seed: int | None = None,
    null_marker: str | None = None,
    null_profile: str | None = None,
    include_hot_paths: bool | None = None,
    min_ndv_for_injection: int | None = None,
    ndv_reference_duckdb: str | None = None,
    ndv_cache_dir: str | None = None,
    scale_factor: int | None = None,
    null_overrides: Mapping[str, Any] | None = None,
    enable_mcv: bool | None = None,
    mcv_seed: int | None = None,
    mcv_profile: str | None = None,
    mcv_overrides: Mapping[str, Any] | None = None,
    source_data_dir: Path | None = None,
) -> Dict[str, Any]:
    cfg = stringify_rules()
    base_pad_width = int(cfg["pad_width"])
    config = stringification_cfg.build_stringification_config(
        level=stringification_level,
        preset=stringification_preset,
        base_pad_width=base_pad_width,
        allow_extended_levels=allow_extended_levels,
        str_plus_enabled=str_plus_enabled,
        str_plus_max_level=str_plus_max_level,
        str_plus_pad_step=str_plus_pad_step,
        str_plus_separator=str_plus_separator,
        str_plus_marker=str_plus_marker,
    )
    stringify_enabled = (config.k_schema > 0) if enable_stringify is None else bool(enable_stringify)

    recast_map = {col: config.schema_type_map[col] for col in config.schema_selected}
    recast_key = tuple(sorted((k.lower(), v) for k, v in recast_map.items()))
    schema = _schema_cache(recast_key)

    rules = build_rules(config) if stringify_enabled else {}

    null_cfg = null_skew_rules(overrides=null_overrides, profile=null_profile)
    if enable_nulls is not None:
        null_cfg["enabled"] = bool(enable_nulls)
    if null_seed is not None:
        null_cfg["seed"] = int(null_seed)
    if null_marker is not None:
        null_cfg["null_marker"] = null_marker
    if include_hot_paths is not None:
        null_cfg["include_hot_path_columns"] = bool(include_hot_paths)
    if min_ndv_for_injection is not None:
        null_cfg["min_ndv_for_injection"] = int(min_ndv_for_injection)
    if ndv_reference_duckdb is not None:
        null_cfg["ndv_reference_duckdb"] = str(ndv_reference_duckdb)
    if ndv_cache_dir is not None:
        null_cfg["ndv_cache_dir"] = str(ndv_cache_dir)
    if scale_factor is not None:
        null_cfg["scale_factor"] = int(scale_factor)

    mcv_cfg = mcv_skew_rules(overrides=mcv_overrides, profile=mcv_profile)
    if enable_mcv is not None:
        mcv_cfg["enabled"] = bool(enable_mcv)
    if mcv_seed is not None:
        mcv_cfg["seed"] = int(mcv_seed)
    if include_hot_paths is not None:
        mcv_cfg["include_hot_path_columns"] = bool(include_hot_paths)
    if min_ndv_for_injection is not None:
        mcv_cfg["min_ndv_for_injection"] = int(min_ndv_for_injection)
    if ndv_reference_duckdb is not None:
        mcv_cfg["ndv_reference_duckdb"] = str(ndv_reference_duckdb)
    if ndv_cache_dir is not None:
        mcv_cfg["ndv_cache_dir"] = str(ndv_cache_dir)
    if scale_factor is not None:
        mcv_cfg["scale_factor"] = int(scale_factor)

    null_injector = NullInjector(schema, null_cfg, source_data_dir=source_data_dir)
    mcv_injector = MCVInjector(
        schema,
        mcv_cfg,
        null_marker=null_cfg.get("null_marker", ""),
        source_data_dir=source_data_dir,
    )

    def _serialize_null_rules() -> Dict[str, List[Dict[str, Any]]]:
        serialized: Dict[str, List[Dict[str, Any]]] = {}
        for table_name, table_rules in (null_injector.rules or {}).items():
            serialized[table_name] = [
                {"index": rule.index, "probability": rule.probability, "name": rule.name}
                for rule in table_rules
            ]
        return serialized

    def _serialize_mcv_rules() -> Dict[str, List[Dict[str, Any]]]:
        serialized: Dict[str, List[Dict[str, Any]]] = {}
        for table_name, table_rules in (mcv_injector.rules or {}).items():
            serialized[table_name] = [
                {
                    "index": rule.index,
                    "name": rule.name,
                    "f20": rule.f20,
                    "f1": rule.f1,
                    "values": list(rule.values),
                }
                for rule in table_rules
            ]
        return serialized

    stringify_serialized: Dict[str, List[Dict[str, Any]]] = {}
    for table_name, table_rules in (rules or {}).items():
        stringify_serialized[table_name] = [
            {
                "index": cfg.get("index"),
                "prefix": cfg.get("prefix", ""),
                "pad_width": int(cfg.get("pad_width", 0)),
                "amplification_extra_pad": int(cfg.get("amplification_extra_pad", 0)),
                "amplification_separator": str(cfg.get("amplification_separator", "_")),
                "amplification_marker": str(cfg.get("amplification_marker", "X")),
                "name": column_name,
            }
            for column_name, cfg in table_rules.items()
        ]

    return {
        "meta": {
            "stringification_level": config.level,
            "str_plus_enabled": bool(config.str_plus_enabled),
            "amplification_extra_pad": int(config.amplification_extra_pad),
            "amplification_pad_step": int(config.amplification_pad_step),
            "amplification_separator": str(config.amplification_separator),
            "amplification_marker": str(config.amplification_marker),
        },
        "stringify": {
            "enabled": bool(stringify_enabled),
            "rules": stringify_serialized,
        },
        "nulls": {
            "enabled": bool(null_injector.enabled and null_injector.has_rules),
            "seed": int(null_cfg.get("seed", 0)),
            "null_marker": str(null_cfg.get("null_marker", "")),
            "include_hot_paths": bool(null_cfg.get("include_hot_path_columns", True)),
            "min_ndv_for_injection": int(
                null_cfg.get("min_ndv_for_injection", MIN_NDV_FOR_INJECTION)
            ),
            "ndv_reference_duckdb": null_injector.ndv_reference_duckdb,
            "ndv_cache_path": null_injector.ndv_cache_path,
            "ndv_guard_excluded": null_injector.ndv_guard_excluded,
            "rules": _serialize_null_rules(),
        },
        "mcv": {
            "enabled": bool(mcv_injector.enabled and mcv_injector.has_rules),
            "seed": int(mcv_cfg.get("seed", 0)),
            "null_marker": str(null_cfg.get("null_marker", "")),
            "include_hot_paths": bool(mcv_cfg.get("include_hot_path_columns", True)),
            "min_ndv_for_injection": int(
                mcv_cfg.get("min_ndv_for_injection", MIN_NDV_FOR_INJECTION)
            ),
            "ndv_reference_duckdb": mcv_injector.ndv_reference_duckdb,
            "ndv_cache_path": mcv_injector.ndv_cache_path,
            "ndv_guard_excluded": mcv_injector.ndv_guard_excluded,
            "rules": _serialize_mcv_rules(),
        },
    }


def export_rewrite_rules(
    output_path: Path,
    *,
    enable_stringify: bool | None = None,
    stringification_level: int | None = None,
    stringification_preset: str | None = None,
    allow_extended_levels: bool = True,
    str_plus_enabled: bool | None = None,
    str_plus_max_level: int = stringification_cfg.DEFAULT_STR_PLUS_MAX_LEVEL,
    str_plus_pad_step: int = 2,
    str_plus_separator: str = "_",
    str_plus_marker: str = "X",
    enable_nulls: bool | None = None,
    null_seed: int | None = None,
    null_marker: str | None = None,
    null_profile: str | None = None,
    include_hot_paths: bool | None = None,
    min_ndv_for_injection: int | None = None,
    ndv_reference_duckdb: str | None = None,
    ndv_cache_dir: str | None = None,
    scale_factor: int | None = None,
    null_overrides: Mapping[str, Any] | None = None,
    enable_mcv: bool | None = None,
    mcv_seed: int | None = None,
    mcv_profile: str | None = None,
    mcv_overrides: Mapping[str, Any] | None = None,
    source_data_dir: Path | None = None,
) -> Path:
    payload = build_rewrite_rules(
        enable_stringify=enable_stringify,
        stringification_level=stringification_level,
        stringification_preset=stringification_preset,
        allow_extended_levels=allow_extended_levels,
        str_plus_enabled=str_plus_enabled,
        str_plus_max_level=str_plus_max_level,
        str_plus_pad_step=str_plus_pad_step,
        str_plus_separator=str_plus_separator,
        str_plus_marker=str_plus_marker,
        enable_nulls=enable_nulls,
        null_seed=null_seed,
        null_marker=null_marker,
        null_profile=null_profile,
        include_hot_paths=include_hot_paths,
        min_ndv_for_injection=min_ndv_for_injection,
        ndv_reference_duckdb=ndv_reference_duckdb,
        ndv_cache_dir=ndv_cache_dir,
        scale_factor=scale_factor,
        null_overrides=null_overrides,
        enable_mcv=enable_mcv,
        mcv_seed=mcv_seed,
        mcv_profile=mcv_profile,
        mcv_overrides=mcv_overrides,
        source_data_dir=source_data_dir,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)
    return output_path

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import re
import sys
from typing import Any

import yaml


def _base_augmented_join_count(b_eff: int, k: int) -> int:
    """
    Paper-model base block with LOD replicas:
      J0(k) = b + k*(b+1)
    """
    return b_eff + k * (b_eff + 1)


def _effective_join_count(b_eff: int, k: int, m: int) -> int:
    """
    Paper-model effective joins with segment replicas:
      J(k,m) = (m+1)*J0(k) + m
             = (m+1)*(b + k*(b+1)) + m
    """
    return (m + 1) * _base_augmented_join_count(b_eff, k) + m


def solve_km_for_target_prefer_k(b: int, J_target: int, Kmax: int, Mmax: int):
    """
    Deterministic best-fit solver for the paper model.

    We search all (k,m) in bounds and minimize absolute error to target.
    Tie-breakers keep query text smaller and deterministic.
    """
    best: tuple[int, int, int, int] | None = None  # (abs_diff, expansion_units, m, -k)
    best_k = 0
    best_m = 0
    best_j = _effective_join_count(b, 0, 0)

    for k in range(Kmax + 1):
        for m in range(Mmax + 1):
            j = _effective_join_count(b, k, m)
            abs_diff = abs(j - J_target)
            # (m+1)*(k+1) is a proxy for generated SQL replication size.
            expansion_units = (m + 1) * (k + 1)
            score = (abs_diff, expansion_units, m, -k)
            if best is None or score < best:
                best = score
                best_k = k
                best_m = m
                best_j = j
                if abs_diff == 0 and m == 0:
                    # Perfect and compact: nothing can beat this for our scoring.
                    break

    strategy = "paper_exact" if best_j == J_target else ("paper_floor" if J_target <= b else "paper_nearest")
    return best_k, best_m, best_j, {
        "strategy": strategy,
        "b_eff": b,
        "max_reachable": _effective_join_count(b, Kmax, Mmax),
    }


def _resolve_target_override(
    overrides: Any, target: int, Kmax: int, Mmax: int, b: int
) -> tuple[int, int, int, dict[str, Any]] | None:
    """
    Resolve optional per-target (k,m) override from config.
    Expected shape:
      target_overrides:
        "16": {k: 1, m: 0}
    """
    if not isinstance(overrides, dict):
        return None
    raw = overrides.get(str(target), overrides.get(int(target)))
    if not isinstance(raw, dict):
        return None
    if "k" not in raw or "m" not in raw:
        raise ValueError(f"target_overrides[{target}] must define both k and m")
    k = int(raw["k"])
    m = int(raw["m"])
    if k < 0 or k > Kmax:
        raise ValueError(f"target_overrides[{target}] has invalid k={k}; expected 0..{Kmax}")
    if m < 0 or m > Mmax:
        raise ValueError(f"target_overrides[{target}] has invalid m={m}; expected 0..{Mmax}")
    J = _effective_join_count(b, k, m)
    return k, m, J, {
        "strategy": "target_override",
        "b_eff": b,
        "max_reachable": _effective_join_count(b, Kmax, Mmax),
    }


def lod_cte(j: int, key: str, agg: str, measure: str) -> str:
    """
    Legacy helper kept for tests/docs; generator now inlines base blocks directly.
    """
    if "," in key:
        raise ValueError(f"Composite keys not supported in v1 (got: '{key}')")
    alias = f"lod_{j:02d}"
    return f"""-- LOD #{j}: {key} with {agg}({measure}) AS m_{j:02d}
{alias} AS (
  SELECT {key} AS g_{j:02d},
         {agg}({measure}) AS m_{j:02d}
  FROM base
  GROUP BY {key}
)"""


def lod_join(j: int, key: str) -> str:
    """
    Legacy helper kept for tests/docs; generator now inlines base blocks directly.
    """
    if "," in key:
        raise ValueError(f"Composite keys not supported in v1 (got: '{key}')")
    alias = f"lod_{j:02d}"
    return f"LEFT JOIN {alias} ON {alias}.g_{j:02d} = b.{key}"


def _indent(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line if line else "" for line in text.splitlines())


def _find_matching_paren(text: str, open_idx: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = open_idx

    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                i += 2
                continue
            in_single = not in_single
            i += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            i += 1
            continue

        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return i
        i += 1

    raise ValueError("Could not find matching ')' for WITH base AS (...) block")


def _extract_base_block(template_sql: str) -> str:
    m = re.search(r"\bwith\s+base\s+as\s*\(", template_sql, flags=re.I)
    if not m:
        raise ValueError("Template must contain `WITH base AS (` block")
    open_idx = m.end() - 1
    close_idx = _find_matching_paren(template_sql, open_idx)
    return template_sql[open_idx + 1 : close_idx].strip()


def _dim_fp_col(copy_tag: int) -> str:
    return f"dim_fp_{copy_tag}"


def _build_dim_fingerprint_expr(dim_fingerprint_columns: list[str], base_alias: str) -> str:
    terms: list[str] = []
    for idx, col in enumerate(dim_fingerprint_columns, start=1):
        # Cast to VARCHAR for type-stability across mixed numeric/string columns.
        terms.append(f"COALESCE(LENGTH(CAST({base_alias}.{col} AS VARCHAR)), 0) * {idx}")
    return " + ".join(terms) if terms else "0"


def _build_decorated_base_instance(base_sql: str, copy_tag: int, dim_fingerprint_columns: list[str]) -> str:
    if copy_tag <= 0 and not dim_fingerprint_columns:
        return base_sql

    lines = [
        "SELECT",
        "  base_src.*",
    ]
    if dim_fingerprint_columns:
        fp_expr = _build_dim_fingerprint_expr(dim_fingerprint_columns, "base_src")
        lines[-1] += ","
        lines.append(f"  {fp_expr} AS {_dim_fp_col(copy_tag)}")
    lines.extend(
        [
            "FROM (",
            _indent(base_sql, 2),
            ") base_src",
        ]
    )
    return "\n".join(lines)


def _base_and_lods_fp_columns(k: int, copy_seed: int) -> list[str]:
    cols = [_dim_fp_col(copy_seed * 1000 + 1)]
    cols.extend(f"fp_lod_{j:02d}" for j in range(1, k + 1))
    return cols


def _build_base_and_lods_select(
    base_sql: str,
    keys: list[str],
    agg_cycle: list[str],
    measure: str,
    k: int,
    *,
    copy_seed: int,
    dim_fingerprint_columns: list[str],
) -> str:
    main_copy_tag = copy_seed * 1000 + 1
    main_base = _build_decorated_base_instance(
        base_sql,
        main_copy_tag,
        dim_fingerprint_columns,
    )
    lines: list[str] = []
    lines.append("SELECT")
    lines.append("  b.*")
    for j in range(1, k + 1):
        lines.append(f"  , lod_{j:02d}.m_{j:02d} AS m_{j:02d}")
        if dim_fingerprint_columns:
            lines.append(f"  , lod_{j:02d}.fp_lod_{j:02d} AS fp_lod_{j:02d}")
    lines.append("FROM (")
    lines.append(_indent(main_base, 2))
    lines.append(") b")

    for j in range(1, k + 1):
        key = keys[j - 1]
        if "," in key:
            raise ValueError(f"Composite keys not supported in v1 (got: '{key}')")
        agg = agg_cycle[(j - 1) % len(agg_cycle)]
        lod_copy_tag = copy_seed * 1000 + 100 + j
        lod_base = _build_decorated_base_instance(
            base_sql,
            lod_copy_tag,
            dim_fingerprint_columns,
        )
        lines.append("LEFT JOIN (")
        lines.append(f"  SELECT {key} AS g_{j:02d},")
        lines.append(f"         {agg}({measure}) AS m_{j:02d}")
        if dim_fingerprint_columns:
            lines.append(f"       , SUM(COALESCE(lod_base_{j:02d}.{_dim_fp_col(lod_copy_tag)}, 0)) AS fp_lod_{j:02d}")
        lines.append("  FROM (")
        lines.append(_indent(lod_base, 4))
        lines.append(f"  ) lod_base_{j:02d}")
        lines.append(f"  GROUP BY {key}")
        lines.append(f") lod_{j:02d} ON lod_{j:02d}.g_{j:02d} = b.{key}")
    return "\n".join(lines)


def _build_sql(
    *,
    target: int,
    b_eff: int,
    k: int,
    m: int,
    strategy: str,
    max_reachable: int,
    keys_used: list[str],
    join_keys: list[str],
    cohort_predicates: list[str],
    include_segment_flags: bool,
    base_sql: str,
    keys: list[str],
    agg_cycle: list[str],
    measure: str,
    dim_fingerprint_columns: list[str],
) -> str:
    expected = _effective_join_count(b_eff, k, m)
    header = (
        f"-- TARGET_JOINS={target}  b_eff={b_eff}  Kmax={len(keys_used)}  max_reachable={max_reachable}\n"
        f"-- STRATEGY={strategy}  chosen k={k} m={m}  EXPECTED_EFFECTIVE_JOINS={expected}\n"
        f"-- FORMULA=J(k,m)=(m+1)*(b+k*(b+1))+m  with b={b_eff}\n"
        f"-- LOD_KEYS_USED: {', '.join(keys_used[:k])}\n"
    )

    using_cols = ", ".join(join_keys)
    seg_predicates = cohort_predicates or ["1=1"]
    select_lines = ["b.*"]
    join_lines: list[str] = []
    main_base_and_lods = _build_base_and_lods_select(
        base_sql,
        keys,
        agg_cycle,
        measure,
        k,
        copy_seed=1,
        dim_fingerprint_columns=dim_fingerprint_columns,
    )

    for i in range(1, m + 1):
        alias = f"seg_{i:03d}"
        flag_col = f"seg_flag_{i:03d}"
        seg_fp_col = f"seg_fp_{i:03d}"
        predicate = seg_predicates[(i - 1) % len(seg_predicates)]
        seg_base_and_lods = _build_base_and_lods_select(
            base_sql,
            keys,
            agg_cycle,
            measure,
            k,
            copy_seed=10_000 + i,
            dim_fingerprint_columns=dim_fingerprint_columns,
        )
        seg_projection = [using_cols, f"1 AS {flag_col}"]
        if dim_fingerprint_columns:
            fp_inputs = _base_and_lods_fp_columns(k, copy_seed=10_000 + i)
            fp_expr = " + ".join(f"COALESCE(base_and_aggregates.{col}, 0)" for col in fp_inputs)
            seg_projection.append(f"({fp_expr}) AS {seg_fp_col}")
        if include_segment_flags:
            select_lines.append(f"COALESCE({alias}.{flag_col}, 0) AS {flag_col}")
        if dim_fingerprint_columns:
            select_lines.append(f"COALESCE({alias}.{seg_fp_col}, 0) AS {seg_fp_col}")
        join_lines.extend(
            [
                "LEFT JOIN (",
                "  SELECT " + ",\n         ".join(seg_projection),
                "  FROM (",
                _indent(seg_base_and_lods, 4),
                "  ) base_and_aggregates",
                f"  WHERE {predicate}",
                f") {alias} USING ({using_cols})",
            ]
        )

    sql_lines = [
        "-- AUTO-GENERATED JOIN SCALING (INLINE BASE BLOCKS)",
        header.rstrip(),
        "SELECT",
        "  " + ",\n  ".join(select_lines),
        "FROM (",
        _indent(main_base_and_lods, 2),
        ") b",
    ]
    if join_lines:
        sql_lines.extend(join_lines)
    sql_lines.append(";")
    return "\n".join(sql_lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate join-heavy query blocks from YAML config and a base .tpl read from stdin."
    )
    ap.add_argument("--cfg", required=True, help="Path to YAML config (returns.yml or sales.yml)")
    ap.add_argument("--target", type=int, default=300, help="Target join count (paper inline model)")
    ap.add_argument("--k", type=int, default=None, help="Explicit LOD count override (bypass solver).")
    ap.add_argument("--m", type=int, default=None, help="Explicit segment-copy count override (bypass solver).")
    args = ap.parse_args()
    if (args.k is None) ^ (args.m is None):
        raise ValueError("--k and --m must be provided together")

    with open(args.cfg, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    b = int(cfg.get("b", 10))
    capJ = int(cfg.get("cap_joins", 10000))
    Mmax = int(cfg.get("max_filts", 200))
    J_target = min(args.target, capJ)

    measure = cfg["measure"]
    keys = cfg["group_keys"]
    agg_cycle = cfg["agg_cycle"]
    join_keys = cfg["join_keys"]
    cohort_predicates = cfg.get("cohort_predicates", [])
    include_segment_flags = bool(cfg.get("include_segment_flags", True))
    dim_fingerprint_columns = cfg.get("dim_fingerprint_columns", [])
    if dim_fingerprint_columns is None:
        dim_fingerprint_columns = []

    if not isinstance(keys, list) or len(keys) == 0:
        raise ValueError("Config group_keys must be a non-empty list.")
    if not isinstance(agg_cycle, list) or len(agg_cycle) == 0:
        raise ValueError("Config agg_cycle must be a non-empty list.")
    if not isinstance(join_keys, list) or len(join_keys) == 0:
        raise ValueError("Config join_keys must be a non-empty list.")
    if not isinstance(cohort_predicates, list):
        raise ValueError("Config cohort_predicates must be a list when present.")
    if not isinstance(dim_fingerprint_columns, list):
        raise ValueError("Config dim_fingerprint_columns must be a list when present.")
    if any(not isinstance(col, str) or not col.strip() for col in dim_fingerprint_columns):
        raise ValueError("Config dim_fingerprint_columns entries must be non-empty strings.")

    Kmax = len(keys)
    if args.k is not None and args.m is not None:
        if args.k < 0 or args.k > Kmax:
            raise ValueError(f"--k must be in 0..{Kmax}")
        if args.m < 0 or args.m > Mmax:
            raise ValueError(f"--m must be in 0..{Mmax}")
        k = int(args.k)
        m = int(args.m)
        aux = {"strategy": "manual_km", "b_eff": b, "max_reachable": _effective_join_count(b, Kmax, Mmax)}
    else:
        override = _resolve_target_override(cfg.get("target_overrides"), J_target, Kmax, Mmax, b)
        if override is not None:
            k, m, _, aux = override
        else:
            k, m, _, aux = solve_km_for_target_prefer_k(b, J_target, Kmax, Mmax)

    template_sql = sys.stdin.read()
    base_sql = _extract_base_block(template_sql)
    sql = _build_sql(
        target=J_target,
        b_eff=b,
        k=k,
        m=m,
        strategy=str(aux.get("strategy", "")),
        max_reachable=int(aux.get("max_reachable", _effective_join_count(b, Kmax, Mmax))),
        keys_used=keys,
        join_keys=join_keys,
        cohort_predicates=cohort_predicates,
        include_segment_flags=include_segment_flags,
        base_sql=base_sql,
        keys=keys,
        agg_cycle=agg_cycle,
        measure=measure,
        dim_fingerprint_columns=dim_fingerprint_columns,
    )
    sys.stdout.write(sql)


if __name__ == "__main__":
    main()

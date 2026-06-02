#!/usr/bin/env python3
"""
Unified wrapper for dsqgen.

- Uses *_ext.tpl by default (auto-generates templates_ext.lst).
- Runs dsqgen from tpcds-kit/tools/ so tpcds.idx is found.
- Splits query_0.sql and deletes it by default (for template lists).
- Post-processes LIMITs per template to avoid dsqgen's global _LIMIT behavior.
- Generates UNION ALL fan-in queries by default.
- Optional join-heavy template generation via workload/dsqgen/generate_join_query.py.
- Optional pure-data mode disables level-dependent query-layer rewrites.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from workload import stringification as stringification_cfg
from workload.dsdgen import config as dsdgen_config
from workload.dsdgen import stringify as dsdgen_stringify

REPO_ROOT = Path(__file__).resolve().parent
TPCDS_KIT_DIR = REPO_ROOT / "tpcds-kit"
TOOLS_DIR = TPCDS_KIT_DIR / "tools"
DSQGEN_CANDIDATES = ("dsqgen", "dsqgen.bin", "dsqgen.exe")
UNION_FANIN_TARGETS = [2, 5, 10, 20, 200]
DEFAULT_JOIN_TARGETS = [50, 100, 200]
CANONICAL_JOIN_SCALING_LEVELS = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048}
DEFAULT_SEED_OVERRIDES_DIR = REPO_ROOT / "configs"
JOIN_META_RE = re.compile(
    r"--\s*STRATEGY=(?P<strategy>\S+)\s+chosen k=(?P<k>\d+)\s+m=(?P<m>\d+)\s+EXPECTED_EFFECTIVE_JOINS=(?P<j>\d+)",
    flags=re.I,
)


def _positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid int value: {raw}") from exc
    if value < 1:
        raise argparse.ArgumentTypeError("value must be >= 1")
    return value


def _resolve_path(root: Path, raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else (root / path)


def _resolve_dsqgen_binary() -> Path:
    for name in DSQGEN_CANDIDATES:
        candidate = TOOLS_DIR / name
        if candidate.exists():
            return candidate
    raise SystemExit(
        "Could not find tpcds-kit/tools/dsqgen*. Run ./install.sh first to fetch and build the TPC-DS toolkit."
    )


def _resolve_templates(
    template_input: Path,
    template_dir: Path,
    template_list: Path,
    *,
    stringification_level: int | None,
    stringification_preset: str | None,
    str_plus_max_level: int = stringification_cfg.DEFAULT_STR_PLUS_MAX_LEVEL,
    str_plus_pad_step: int = 2,
    str_plus_separator: str = "_",
    str_plus_marker: str = "X",
) -> stringification_cfg.QuerySelection:
    from workload.dsqgen.template_resolver import read_templates, resolve_templates

    names = read_templates(template_input)
    config = stringification_cfg.build_stringification_config(
        level=stringification_level,
        preset=stringification_preset,
        template_names=names,
        template_dir=template_dir,
        str_plus_max_level=str_plus_max_level,
        str_plus_pad_step=str_plus_pad_step,
        str_plus_separator=str_plus_separator,
        str_plus_marker=str_plus_marker,
    )
    selection = stringification_cfg.QuerySelection(
        candidates=config.query_candidates,
        selected=config.query_selected,
        k_query=config.k_query,
        K_query_max=config.K_query_max,
    )
    resolved = resolve_templates(
        names,
        template_dir,
        use_extensions=True,
        allowed_ext=set(selection.enabled_ext_templates),
    )
    template_list.write_text("\n".join(resolved) + "\n", encoding="utf-8")
    return selection


def _is_safe_output_dir(path: Path) -> bool:
    if path == Path("/"):
        return False
    home = Path.home()
    if path == home:
        return False
    if path == REPO_ROOT or path == REPO_ROOT.parent:
        return False
    return True


def _clear_output_dir(path: Path) -> None:
    if not _is_safe_output_dir(path):
        raise SystemExit(f"Refusing to clear unsafe output directory: {path}")
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return
    for entry in path.iterdir():
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()


def _generate_join_template(
    *,
    cfg: Path,
    target: int,
    base_template: Path,
    output_template: Path,
) -> None:
    cmd = [
        sys.executable,
        str(REPO_ROOT / "workload" / "dsqgen" / "generate_join_query.py"),
        "--cfg",
        str(cfg),
        "--target",
        str(target),
    ]
    with base_template.open("r", encoding="utf-8") as src, output_template.open(
        "w", encoding="utf-8"
    ) as dst:
        subprocess.run(cmd, stdin=src, stdout=dst, check=True)

    if target in CANONICAL_JOIN_SCALING_LEVELS:
        sql = output_template.read_text(encoding="utf-8")
        m = JOIN_META_RE.search(sql)
        if not m:
            raise SystemExit(
                f"Join template {output_template} has no calibration header for target {target}. "
                "Regenerate with workload/dsqgen/generate_join_query.py and calibrated returns.yml target_overrides."
            )
        if m.group("strategy") != "target_override":
            raise SystemExit(
                f"Join target {target} generated with STRATEGY={m.group('strategy')} (expected target_override). "
                "Check workload/config/returns.yml target_overrides."
            )


def _run_dsqgen(cmd: list[str], *, quiet: bool = False) -> None:
    if quiet:
        proc = subprocess.run(cmd, cwd=str(TOOLS_DIR), capture_output=True, text=True)
        if proc.returncode != 0:
            if proc.stdout:
                print(proc.stdout, file=sys.stderr, end="")
            if proc.stderr:
                print(proc.stderr, file=sys.stderr, end="")
            raise subprocess.CalledProcessError(
                proc.returncode, proc.args, output=proc.stdout, stderr=proc.stderr
            )
        return
    subprocess.run(cmd, cwd=str(TOOLS_DIR), check=True)


def _split_queries(output_dir: Path, *, keep_combined: bool) -> None:
    from workload.dsqgen.split_queries import split_queries

    combined = output_dir / "query_0.sql"
    if not combined.exists():
        return
    written = split_queries(combined, output_dir)
    if not keep_combined and written > 1:
        combined.unlink()


def _postprocess_limits(output_dir: Path, template_dir: Path, scale: int = 1) -> None:
    postprocess = REPO_ROOT / "workload" / "dsqgen" / "limit_postprocess.py"
    if not postprocess.exists():
        return
    cmd = [
        sys.executable,
        str(postprocess),
        str(output_dir),
        "--template-dir",
        str(template_dir),
        "--scale",
        str(scale),
    ]
    subprocess.run(cmd, check=True)


def _truncate_query_to_first_statement(sql: str) -> str:
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""
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
        if ch == ";" and not in_single and not in_double:
            return sql[:i].strip()
        i += 1
    return sql.strip()


def _sanitize_query_files(output_dir: Path) -> None:
    for path in output_dir.glob("*.sql"):
        raw = path.read_text(encoding="utf-8")
        # Drop start/end markers and truncate to the first statement.
        lines = [line for line in raw.splitlines() if not line.strip().startswith("-- start query") and not line.strip().startswith("-- end query")]
        cleaned = _truncate_query_to_first_statement("\n".join(lines))
        if cleaned:
            path.write_text(cleaned + "\n", encoding="utf-8")


def _sanitize_generated_sql(sql: str) -> str:
    lines = [
        line
        for line in sql.splitlines()
        if not line.strip().startswith("-- start query")
        and not line.strip().startswith("-- end query")
    ]
    return _truncate_query_to_first_statement("\n".join(lines))


def _rewrite_postgres_intervals(sql: str) -> str:
    pattern = re.compile(r"([+-])\s+(\d+)\s+days", re.IGNORECASE)
    return pattern.sub(lambda m: f"{m.group(1)} interval '{m.group(2)} days'", sql)


def _find_paren_depth_at(sql: str, index: int) -> int:
    depth = 0
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    while i < index:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < index else ""
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
        i += 1
    return depth


def _find_matching_close(sql: str, start_index: int, target_depth: int) -> int | None:
    depth = target_depth
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = start_index
    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""
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
                if depth == target_depth - 1:
                    return i
        i += 1
    return None


def _rewrite_keep_limit_top(sql: str) -> str:
    pattern = re.compile(r"select\s+/\*KEEP_LIMIT\*/\s+top\s+(\d+)\s+", re.IGNORECASE)
    matches = list(pattern.finditer(sql))
    if not matches:
        return sql
    updated = sql
    for match in reversed(matches):
        limit = match.group(1)
        start = match.start()
        end = match.end()
        depth = _find_paren_depth_at(updated, start)
        close_idx = _find_matching_close(updated, end, depth)
        if close_idx is None:
            continue
        updated = (
            updated[:start]
            + "select /*KEEP_LIMIT*/ "
            + updated[end:close_idx]
            + f" limit {limit}"
            + updated[close_idx:]
        )
    return updated


def _rewrite_postgres_sql(filename: str, sql: str) -> str:
    sql = _rewrite_postgres_intervals(sql)
    sql = _rewrite_keep_limit_top(sql)
    sql = _rewrite_list_separators(sql)
    sql = _rewrite_lochierarchy_alias(sql)
    sql = _rewrite_scalar_subquery_star(sql)
    sql = _rewrite_scalar_distinct_month_seq(sql)
    sql = _rewrite_materialized_year_total(sql)
    sql = _rewrite_year_total_year_filter(sql)
    sql = _rewrite_year_total_pivot(sql)
    sql = _rewrite_postgres_query_fixes(filename, sql)
    return sql


def _postprocess_postgres(output_dir: Path) -> None:
    for path in output_dir.glob("*.sql"):
        sql = path.read_text(encoding="utf-8")
        path.write_text(_rewrite_postgres_sql(path.name, sql), encoding="utf-8")


def _rewrite_duckdb_query_fixes(filename: str, sql: str) -> str:
    key = filename.lower()
    if key == "query_25.sql":
        sql = re.sub(
            r"(d1\.d_moy\s*=\s*)\d+\b",
            r"\g<1>3",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"(d2\.d_moy\s+between\s+)\d+(\s+and\s+)\d+\b",
            r"\g<1>3\g<2>9",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        sql = re.sub(
            r"(d3\.d_moy\s+between\s+)\d+(\s+and\s+)\d+\b",
            r"\g<1>3\g<2>9",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
    elif key == "query_37.sql":
        sql = re.sub(
            r"\n\s*and\s+i_manufact_id\s+in\s*\([^)]*\)",
            "",
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
    return sql


def _rewrite_duckdb_sql(filename: str, sql: str) -> str:
    return _rewrite_duckdb_query_fixes(filename, _rewrite_postgres_sql(filename, sql))


def _postprocess_duckdb(output_dir: Path) -> None:
    for path in output_dir.glob("*.sql"):
        sql = path.read_text(encoding="utf-8")
        path.write_text(_rewrite_duckdb_sql(path.name, sql), encoding="utf-8")


def _rewrite_lochierarchy_alias(sql: str) -> str:
    match = re.search(
        r"grouping\(([^)]+)\)\s*\+\s*grouping\(([^)]+)\)\s+as\s+lochierarchy",
        sql,
        re.IGNORECASE,
    )
    if not match:
        return sql
    expr = f"grouping({match.group(1)})+grouping({match.group(2)})"
    prefix = sql[:match.end()]
    suffix = sql[match.end():]
    suffix = re.sub(r"\blochierarchy\b", expr, suffix)
    return prefix + suffix


def _rewrite_select_top_distinct(sql: str) -> str:
    """Fix 'select top N distinct(col)' → 'select distinct top N col'.

    dsqgen generates invalid syntax where DISTINCT looks like a function call.
    """
    return re.sub(
        r"\bselect\s+top\s+(\d+)\s+distinct\((\w+)\)",
        r"select distinct top \1 \2",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_scalar_subquery_star(sql: str) -> str:
    pattern = re.compile(r"\(select\s+\*\s+from\s+max_store_sales\)", re.IGNORECASE)
    return pattern.sub("(select tpcds_cmax from max_store_sales)", sql)


def _rewrite_scalar_distinct_month_seq(sql: str) -> str:
    pattern = re.compile(r"select\s+distinct\s*\(d_month_seq\)", re.IGNORECASE)
    return pattern.sub("select min(d_month_seq)", sql)


def _rewrite_materialized_year_total(sql: str) -> str:
    pattern = re.compile(r"with\s+year_total\s+as\s*\(", re.IGNORECASE)
    return pattern.sub("with year_total as materialized (", sql)


def _rewrite_year_total_year_filter(sql: str) -> str:
    if "with year_total" not in sql.lower():
        return sql
    # Detect base year from query filters (e.g., 1998 or 1999) and restrict CTE.
    base_year = None
    match = re.search(r"t_s_firstyear\.(d?year)\s*=\s*(\d{4})", sql, re.IGNORECASE)
    if match:
        base_year = int(match.group(2))
    if base_year is None:
        return sql
    years = f"{base_year},{base_year + 1}"

    def inject(pattern: str, text: str) -> str:
        guarded = rf"({pattern})(?!\s*and\s+d_year\s+in\s*\({years}\))"
        return re.sub(
            guarded,
            lambda m: f"{m.group(1)}\n   and d_year in ({years})",
            text,
            flags=re.IGNORECASE,
        )

    sql = inject(r"and\s+ss_sold_date_sk\s*=\s*d_date_sk", sql)
    sql = inject(r"and\s+cs_sold_date_sk\s*=\s*d_date_sk", sql)
    sql = inject(r"and\s+ws_sold_date_sk\s*=\s*d_date_sk", sql)
    sql = re.sub(
        rf"(\n\s*and\s+d_year\s+in\s*\({years}\))(?:\s*\1)+",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_year_total_pivot(sql: str) -> str:
    lowered = sql.lower()
    if "with year_total" not in lowered:
        return sql
    if "t_s_firstyear" not in lowered or "t_s_secyear" not in lowered:
        return sql
    if "t_w_firstyear" not in lowered or "t_w_secyear" not in lowered:
        return sql
    base_match = re.search(r"t_s_firstyear\.(d?year)\s*=\s*(\d{4})", sql, re.IGNORECASE)
    if not base_match:
        return sql
    year_alias = base_match.group(1)
    base_year = int(base_match.group(2))
    next_year = base_year + 1
    cte_match = re.search(r"with\s+year_total\s+as\s+materialized\s*\(", sql, re.IGNORECASE)
    if not cte_match:
        return sql
    open_idx = cte_match.end() - 1
    depth = _find_paren_depth_at(sql, open_idx + 1)
    close_idx = _find_matching_close(sql, open_idx + 1, depth)
    if close_idx is None:
        return sql
    cte = sql[: close_idx + 1]
    tail = sql[close_idx + 1 :]
    select_match = re.search(
        r"select\s+(.*?)\s+from\s+year_total\s+t_s_firstyear",
        tail,
        re.IGNORECASE | re.DOTALL,
    )
    if not select_match:
        return sql
    select_body = select_match.group(1).strip()
    select_cols_raw = [col.strip() for col in select_body.split(",") if col.strip()]
    select_cols = [
        re.sub(r"\bt_s_secyear\.", "", col, flags=re.IGNORECASE) for col in select_cols_raw
    ]
    order_match = re.search(
        r"order\s+by\s+(.*?)\s+limit\s+(\d+)",
        tail,
        re.IGNORECASE | re.DOTALL,
    )
    if not order_match:
        return sql
    order_body = order_match.group(1).strip()
    limit = order_match.group(2)
    order_cols_raw = [col.strip() for col in order_body.split(",") if col.strip()]
    order_cols = [
        re.sub(r"\bt_s_secyear\.", "", col, flags=re.IGNORECASE) for col in order_cols_raw
    ]
    has_catalog = "t_c_firstyear" in lowered or "t_c_secyear" in lowered

    def expr(sale_type: str, year: int) -> str:
        return (
            "max(case when sale_type = '{sale_type}' and {alias} = {year} "
            "then year_total end)".format(sale_type=sale_type, alias=year_alias, year=year)
        )

    base_s = expr("s", base_year)
    next_s = expr("s", next_year)
    base_w = expr("w", base_year)
    next_w = expr("w", next_year)
    conditions = [
        f"{base_s} > 0",
        f"{base_w} > 0",
    ]
    if has_catalog:
        base_c = expr("c", base_year)
        next_c = expr("c", next_year)
        conditions.append(f"{base_c} > 0")
        conditions.append(f"({next_c} / nullif({base_c}, 0)) > ({next_s} / nullif({base_s}, 0))")
        conditions.append(f"({next_c} / nullif({base_c}, 0)) > ({next_w} / nullif({base_w}, 0))")
    else:
        conditions.append(f"({next_w} / nullif({base_w}, 0)) > ({next_s} / nullif({base_s}, 0))")
    having_clause = "\n   and ".join(conditions)
    select_list = ",\n       ".join(select_cols)
    group_list = ",\n       ".join(select_cols)
    order_list = ",\n       ".join(order_cols)
    rewritten = (
        f"{cte}\n select {select_list}\n"
        " from year_total\n"
        f" group by {group_list}\n"
        f" having {having_clause}\n"
        f" order by {order_list}\n"
        f" limit {limit}\n"
    )
    return rewritten


def _to_postgres_numeric_key(expr: str) -> str:
    return f"cast(regexp_replace(cast({expr} as text), '[^0-9-]', '', 'g') as bigint)"


def _rewrite_stringified_date_sk_arithmetic(sql: str) -> str:
    # Convert stringified date-key arithmetic (e.g. ws_ship_date_sk - ws_sold_date_sk <= 'D_00000030')
    # to explicit numeric comparisons that work for both plain integers and prefixed keys.
    pattern = re.compile(
        r"(?P<lhs>\b[a-z_][\w.]*date_sk\b)\s*-\s*"
        r"(?P<rhs>\b[a-z_][\w.]*date_sk\b)\s*"
        r"(?P<op><=|>=|<>|!=|=|<|>)\s*"
        r"(?P<threshold>'[^']*'|-?\d+)",
        re.IGNORECASE,
    )

    def _replace(match: re.Match) -> str:
        lhs = match.group("lhs")
        rhs = match.group("rhs")
        op = match.group("op")
        threshold = match.group("threshold")
        return (
            f"({_to_postgres_numeric_key(lhs)} - {_to_postgres_numeric_key(rhs)} "
            f"{op} {_to_postgres_numeric_key(threshold)})"
        )

    return pattern.sub(_replace, sql)


def _rewrite_query_9(sql: str) -> str:
    sql = re.sub(
        r"\n\s*,any_value\(r_reason_desc\)\s+as\s+any_reason_desc",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,count\(distinct\s+r_reason_id\)\s+as\s+distinct_reason_id_count",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_1(sql: str) -> str:
    sql = re.sub(
        r"with\s+customer_total_return\s+as\s*\(",
        "with customer_total_return as materialized (",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"from customer_total_return ctr1\s*,store\s*,customer",
        "from customer_total_return ctr1\n"
        ",(select ctr_store_sk, avg(ctr_total_return)*1.2 as avg_total_return\n"
        "  from customer_total_return\n"
        "  group by ctr_store_sk) ctr_avg\n"
        ",store\n"
        ",customer",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"where ctr1\.ctr_total_return >\s*\(select avg\(ctr_total_return\)\*1\.2\s*"
        r"from customer_total_return ctr2\s*"
        r"where ctr1\.ctr_store_sk = ctr2\.ctr_store_sk\)",
        "where ctr1.ctr_store_sk = ctr_avg.ctr_store_sk\n"
        "and ctr1.ctr_total_return > ctr_avg.avg_total_return",
        sql,
        flags=re.IGNORECASE,
    )
    if re.search(r"\blimit\b", sql, re.IGNORECASE) is None:
        sql = sql.rstrip() + "\n limit 100"
    return sql


def _rewrite_query_11(sql: str) -> str:
    sql = re.sub(
        r"\n\s*,any_value\(t_s_secyear\.customer_birth_country\)\s+as\s+any_birth_country",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,any_value\(t_s_secyear\.customer_email_address\)\s+as\s+any_email_address",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,count\(distinct\s+t_s_secyear\.customer_login\)\s+as\s+distinct_login_count",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,max\(t_s_secyear\.customer_login\)\s+over\s*\(\)\s+as\s+max_login",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,min\(t_s_secyear\.customer_birth_country\)\s+over\s*\(\)\s+as\s+min_birth_country",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,any_value\(t_s_secyear\.customer_preferred_cust_flag\)\s+over\s*\(\)\s+as\s+any_pref_flag",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_24(sql: str) -> str:
    return re.sub(
        r"any_value\(c_birth_country\)\s+as\s+any_birth_country",
        "any_value(ca_state) as any_birth_country",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_query_6(sql: str) -> str:
    return re.sub(
        r"(c\.c_birth_country\s+in\s*)\(\s*'United States'\s*,\s*'Canada'\s*,\s*'Mexico'\s*\)",
        r"\1('UNITED STATES','CANADA','MEXICO')",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_query_25(sql: str) -> str:
    sql = re.sub(
        r"(d1\.d_moy\s*=\s*)\d+\b",
        r"\g<1>2",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(d2\.d_moy\s+between\s+)\d+(\s+and\s+)\d+\b",
        r"\g<1>2\g<2>8",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(d3\.d_moy\s+between\s+)\d+(\s+and\s+)\d+\b",
        r"\g<1>2\g<2>8",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(d1\.d_year\s*=\s*)\d+\b",
        r"\g<1>1998",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(d2\.d_year\s*=\s*)\d+\b",
        r"\g<1>1998",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(d3\.d_year\s*=\s*)\d+\b",
        r"\g<1>1998",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_30(sql: str) -> str:
    return re.sub(
        r"(ca_state\s*=\s*)'AR'",
        r"\1'MO'",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


def _rewrite_query_37(sql: str) -> str:
    pattern = re.compile(r"i_manufact_id\s+in\s*\(([^)]*)\)", flags=re.IGNORECASE)
    match = pattern.search(sql)
    if not match:
        return sql
    values = match.group(1)
    # Keep STR1 numeric predicates untouched; only rewrite stringified id domains.
    if "'" not in values:
        return sql
    return pattern.sub("i_manufact_id in ('MFG_00000445')", sql, count=1)


def _rewrite_query_41(sql: str) -> str:
    return re.sub(
        r"order\s+by\s+i_manufact_id\s+desc",
        "order by i_product_name",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_query_44(sql: str) -> str:
    sql = re.sub(
        r"\n\s*,any_value\(i1\.i_brand\)\s+as\s+any_best_brand",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,any_value\(i2\.i_brand\)\s+as\s+any_worst_brand",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,any_value\(i1\.i_category\)\s+as\s+any_best_category",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,any_value\(i2\.i_category\)\s+as\s+any_worst_category",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,max\(i1\.i_class\)\s+as\s+max_best_class",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,max\(i2\.i_class\)\s+as\s+max_worst_class",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_58(sql: str) -> str:
    sql = re.sub(
        r"any_value\(\(select i_category from item where i_item_id = ss_items\.item_id\)\)\s+as\s+any_item_category",
        "(select max(i_category) from item where i_item_id = ss_items.item_id) as any_item_category",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"max\(\(select i_brand from item where i_item_id = ss_items\.item_id\)\)\s+as\s+max_item_brand",
        "(select max(i_brand) from item where i_item_id = ss_items.item_id) as max_item_brand",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"count\(distinct\s+\(select i_product_name from item where i_item_id = ss_items\.item_id\)\)\s+as\s+distinct_product_name_count",
        "(select count(distinct i_product_name) from item where i_item_id = ss_items.item_id) as distinct_product_name_count",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\(select i_category from item where i_item_id = ss_items\.item_id\)\s+in",
        "(select max(i_category) from item where i_item_id = ss_items.item_id) in",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\(select i_brand from item where i_item_id = ss_items\.item_id\)\s+is not null",
        "(select max(i_brand) from item where i_item_id = ss_items.item_id) is not null",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(\n\s+and\s+\(select i_brand from item where i_item_id = ss_items\.item_id\)\s+is not null)\n(\s*order by)",
        r"\1\n group by ss_items.item_id\n        ,ss_item_rev\n\2",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(\n\s+and\s+\(select max\(i_brand\) from item where i_item_id = ss_items\.item_id\)\s+is not null)\n(\s*order by)",
        r"\1\n group by ss_items.item_id\n        ,ss_item_rev\n\2",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"order\s+by\s+item_id",
        "order by ss_items.item_id",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_60(sql: str) -> str:
    sql = re.sub(
        r"any_value\(\(select i_category from item where i_item_id = tmp1\.i_item_id\)\)\s+as\s+any_item_category",
        "(select max(i_category) from item where i_item_id = tmp1.i_item_id) as any_item_category",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"count\(distinct\s+\(select i_product_name from item where i_item_id = tmp1\.i_item_id\)\)\s+as\s+distinct_product_name_count",
        "(select count(distinct i_product_name) from item where i_item_id = tmp1.i_item_id) as distinct_product_name_count",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_66(sql: str) -> str:
    first_block = (
        "     group by \n"
        "        w_warehouse_name\n"
        " \t,w_warehouse_sq_ft\n"
        " \t,w_city\n"
        " \t,w_county\n"
        " \t,w_state\n"
        " \t,w_country\n"
        "       ,d_year\n"
        " union all"
    )
    first_fixed = (
        "     group by \n"
        "        w_warehouse_name\n"
        " \t,w_warehouse_id\n"
        " \t,w_warehouse_sk\n"
        " \t,w_warehouse_sq_ft\n"
        " \t,w_city\n"
        " \t,w_county\n"
        " \t,w_state\n"
        " \t,w_country\n"
        " \t,w_zip\n"
        "       ,d_year\n"
        " union all"
    )
    second_block = (
        " union all\n"
        "     select \n"
        " \tw_warehouse_name\n"
        " \t,w_warehouse_sq_ft\n"
        " \t,w_city\n"
        " \t,w_county\n"
        " \t,w_state\n"
        " \t,w_country\n"
        " \t,'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers\n"
        "       ,d_year as year\n"
    )
    second_fixed = (
        " union all\n"
        "     select \n"
        " \tw_warehouse_name\n"
        " \t,w_warehouse_id\n"
        " \t,w_warehouse_sk\n"
        " \t,w_warehouse_sq_ft\n"
        " \t,w_city\n"
        " \t,w_county\n"
        " \t,w_state\n"
        " \t,w_country\n"
        " \t,w_zip\n"
        " \t,'ORIENTAL' || ',' || 'BOXBUNDLES' as ship_carriers\n"
        "       ,d_year as year\n"
    )
    catalog_measure_pattern = re.compile(
        r"(?P<indent>\t,sum\(case when d_moy = 1\s*\n\t\tthen\s+)"
        r"(?P<sales_expr>[a-z_]+)\* cs_quantity else 0 end\) as jan_sales\s*\n"
        r"(?P<from_indent>\s*from)",
        re.IGNORECASE,
    )
    missing_groupby_pattern = re.compile(
        r"group by\s*\n\s*w_warehouse_name\s*\n\s*,w_warehouse_sq_ft\s*\n\s*,w_city\s*\n\s*,w_county\s*\n\s*,w_state\s*\n\s*,w_country\s*\n\s*,d_year",
        re.IGNORECASE,
    )
    missing_select_pattern = re.compile(
        r"select\s*\n\s*w_warehouse_name\s*\n\s*,w_warehouse_sq_ft\s*\n\s*,w_city\s*\n\s*,w_county\s*\n\s*,w_state\s*\n\s*,w_country\s*\n\s*,(?P<ship>[^\n]+as ship_carriers)",
        re.IGNORECASE,
    )
    missing_select_replacement = (
        "select \n"
        " \tw_warehouse_name\n"
        " \t,w_warehouse_id\n"
        " \t,w_warehouse_sk\n"
        " \t,w_warehouse_sq_ft\n"
        " \t,w_city\n"
        " \t,w_county\n"
        " \t,w_state\n"
        " \t,w_country\n"
        " \t,w_zip\n"
        " \t,\\g<ship>"
    )
    missing_groupby_replacement = (
        "group by \n"
        "        w_warehouse_name\n"
        " \t,w_warehouse_id\n"
        " \t,w_warehouse_sk\n"
        " \t,w_warehouse_sq_ft\n"
        " \t,w_city\n"
        " \t,w_county\n"
        " \t,w_state\n"
        " \t,w_country\n"
        " \t,w_zip\n"
        "       ,d_year"
    )
    if first_block in sql:
        sql = sql.replace(first_block, first_fixed, 1)
    if second_block in sql:
        sql = sql.replace(second_block, second_fixed, 1)
    def _expand_catalog_branch_measures(match: re.Match[str]) -> str:
        sales_expr = match.group("sales_expr")
        indent = match.group("indent")
        from_indent = match.group("from_indent")
        lines = [f"{indent}{sales_expr}* cs_quantity else 0 end) as jan_sales"]
        for month_num, month_name in enumerate(
            ["feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
            start=2,
        ):
            lines.append(
                "\t,sum(case when d_moy = "
                f"{month_num} \n\t\tthen {sales_expr}* cs_quantity else 0 end) as {month_name}_sales"
            )
        lines.append(
            "\t,sum(case when d_moy = 1 \n\t\tthen cs_net_profit * cs_quantity else 0 end) as jan_net"
        )
        return "\n".join(lines) + f"\n{from_indent}"

    sql = catalog_measure_pattern.sub(_expand_catalog_branch_measures, sql, count=1)
    sql = missing_select_pattern.sub(missing_select_replacement, sql)
    sql = missing_groupby_pattern.sub(missing_groupby_replacement, sql)
    return sql


def _rewrite_query_68(sql: str) -> str:
    sql = re.sub(
        r"\n\s*,any_value\(store\.s_store_name\)\s+as\s+any_store_name",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,any_value\(store\.s_market_desc\)\s+as\s+any_market_desc",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,count\(distinct current_addr\.ca_state\)\s+as\s+distinct_current_state_count",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,min\(d_date\)\s+as\s+min_sold_date",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*,max\(cast\(d_date as timestamp\)\)\s+as\s+max_sold_ts",
        "",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"order\s+by\s+extended_price\s+desc\s*,\s*max_sold_ts\s+desc\s*,\s*min_sold_date\s+desc",
        "order by extended_price desc",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_72(sql: str) -> str:
    # DuckDB can treat unqualified d_week_seq as ambiguous in this query because d1/d2/d3 all expose it.
    sql = re.sub(
        r"(order\s+by\s+[^;\n]*?)(?<!\.)\bd_week_seq\b",
        r"\1d1.d_week_seq",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_77(sql: str) -> str:
    return re.sub(r"\breturns\b", "returns_amt", sql, flags=re.IGNORECASE)


def _rewrite_query_83(sql: str) -> str:
    sql = re.sub(
        r"any_value\(\(select i_category from item where i_item_id = sr_items\.item_id\)\)\s+as\s+any_item_category",
        "(select max(i_category) from item where i_item_id = sr_items.item_id) as any_item_category",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"max\(\(select i_brand from item where i_item_id = sr_items\.item_id\)\)\s+as\s+max_item_brand",
        "(select max(i_brand) from item where i_item_id = sr_items.item_id) as max_item_brand",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"count\(distinct\s+\(select i_product_name from item where i_item_id = sr_items\.item_id\)\)\s+as\s+distinct_product_name_count",
        "(select count(distinct i_product_name) from item where i_item_id = sr_items.item_id) as distinct_product_name_count",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"(\n\s+and sr_item_qty > 0)\n(\s*order by)",
        r"\1\n group by sr_items.item_id\n        ,sr_item_qty\n\2",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_49(sql: str) -> str:
    pattern = re.compile(
        r"/\s*cast\(sum\(coalesce\((?P<expr>[^,]+),0\)\)\s+as\s+decimal\(15,4\)\s*\)",
        re.IGNORECASE,
    )
    return pattern.sub(
        r"/ nullif(cast(sum(coalesce(\g<expr>,0)) as decimal(15,4)), 0)",
        sql,
    )


def _rewrite_query_54(sql: str) -> str:
    sql = re.sub(
        r"(c_birth_country\s+in\s*)\(\s*'United States'\s*,\s*'Canada'\s*\)",
        r"\1('UNITED STATES','CANADA')",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*and\s+ca_county\s*=\s*s_county",
        "",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"\n\s*and\s+ca_city\s+in\s*\([^)]*\)",
        "",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        (
            r"and\s+d_month_seq\s+between\s*"
            r"\(\s*select\s+distinct\s+d_month_seq\+1\s+from\s+date_dim\s+"
            r"where\s+d_year\s*=\s*1999\s+and\s+d_moy\s*=\s*1\s*\)\s*"
            r"and\s*"
            r"\(\s*select\s+distinct\s+d_month_seq\+3\s+from\s+date_dim\s+"
            r"where\s+d_year\s*=\s*1999\s+and\s+d_moy\s*=\s*1\s*\)"
        ),
        "and d_year = 1999",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )
    return sql


def _rewrite_query_85(sql: str) -> str:
    return re.sub(
        r"\n\s*and\s+wp_type\s+is\s+not\s+null",
        "",
        sql,
        count=1,
        flags=re.IGNORECASE,
    )


def _rewrite_query_90(sql: str) -> str:
    sql = re.sub(
        r"cast\(amc as decimal\(15,4\)\)/cast\(pmc as decimal\(15,4\)\)",
        "cast(amc as decimal(15,4))/nullif(cast(pmc as decimal(15,4)), 0)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(r"\)\s+at\s*,", ") am_tbl,", sql, count=1, flags=re.IGNORECASE)
    return sql


def _rewrite_query_93(sql: str) -> str:
    return re.sub(
        r"(r_reason_desc\s*=\s*)'reason\s+\d+'",
        r"\1'Did not like the warranty'",
        sql,
        flags=re.IGNORECASE,
    )


def _rewrite_postgres_query_fixes(filename: str, sql: str) -> str:
    sql = _rewrite_stringified_date_sk_arithmetic(sql)
    rewrites = {
        "query_1.sql": _rewrite_query_1,
        "query_6.sql": _rewrite_query_6,
        "query_9.sql": _rewrite_query_9,
        "query_11.sql": _rewrite_query_11,
        "query_24.sql": _rewrite_query_24,
        "query_25.sql": _rewrite_query_25,
        "query_30.sql": _rewrite_query_30,
        "query_37.sql": _rewrite_query_37,
        "query_41.sql": _rewrite_query_41,
        "query_44.sql": _rewrite_query_44,
        "query_49.sql": _rewrite_query_49,
        "query_54.sql": _rewrite_query_54,
        "query_58.sql": _rewrite_query_58,
        "query_60.sql": _rewrite_query_60,
        "query_66.sql": _rewrite_query_66,
        "query_68.sql": _rewrite_query_68,
        "query_72.sql": _rewrite_query_72,
        "query_77.sql": _rewrite_query_77,
        "query_83.sql": _rewrite_query_83,
        "query_85.sql": _rewrite_query_85,
        "query_90.sql": _rewrite_query_90,
        "query_93.sql": _rewrite_query_93,
    }
    key = filename.lower()
    rewrite = rewrites.get(key)
    if rewrite is not None:
        sql = rewrite(sql)
    return sql


def _sql_code_segments(sql: str) -> list[tuple[int, int]]:
    segments: list[tuple[int, int]] = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    seg_start = 0
    i = 0
    length = len(sql)

    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                seg_start = i + 1
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                seg_start = i
                continue
            i += 1
            continue

        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                if seg_start < i:
                    segments.append((seg_start, i))
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                if seg_start < i:
                    segments.append((seg_start, i))
                in_block_comment = True
                i += 2
                continue

        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                i += 2
                continue
            if in_single:
                in_single = False
                i += 1
                seg_start = i
                continue
            if seg_start < i:
                segments.append((seg_start, i))
            in_single = True
            i += 1
            continue

        if ch == '"' and not in_single:
            if in_double:
                in_double = False
                i += 1
                seg_start = i
                continue
            if seg_start < i:
                segments.append((seg_start, i))
            in_double = True
            i += 1
            continue

        i += 1

    if not in_single and not in_double and not in_line_comment and not in_block_comment:
        if seg_start < length:
            segments.append((seg_start, length))

    return segments


def _next_word(sql: str, start: int) -> tuple[str | None, int]:
    i = start
    length = len(sql)
    while i < length and sql[i].isspace():
        i += 1
    if i >= length:
        return None, i
    if not (sql[i].isalpha() or sql[i] == "_"):
        return None, i
    j = i + 1
    while j < length and (sql[j].isalnum() or sql[j] == "_"):
        j += 1
    return sql[i:j], j


def _rewrite_list_separators(sql: str) -> str:
    segments = _sql_code_segments(sql)
    if not segments:
        return sql

    start_keywords = {"with", "select", "insert", "update", "delete"}

    def _process_segment(segment: str) -> str:
        out: list[str] = []
        depth = 0
        ctx_by_depth: dict[int, str] = {}
        in_list_depths: list[int] = []
        pending_in = False
        pending_order = False
        i = 0
        length = len(segment)

        while i < length:
            ch = segment[i]
            if ch == "(":
                depth += 1
                if pending_in:
                    in_list_depths.append(depth)
                    pending_in = False
                out.append(ch)
                i += 1
                continue
            if ch == ")":
                if in_list_depths and depth == in_list_depths[-1]:
                    in_list_depths.pop()
                if depth in ctx_by_depth and ctx_by_depth[depth] == "order_by":
                    ctx_by_depth.pop(depth, None)
                depth = max(0, depth - 1)
                out.append(ch)
                i += 1
                continue

            if ch.isalpha() or ch == "_":
                word, next_idx = _next_word(segment, i)
                if word is None:
                    out.append(ch)
                    i += 1
                    continue
                lowered = word.lower()
                if pending_order and lowered == "by":
                    ctx_by_depth[depth] = "order_by"
                    pending_order = False
                else:
                    pending_order = lowered == "order"

                if lowered == "select":
                    ctx_by_depth[depth] = "select"
                elif lowered == "from" and ctx_by_depth.get(depth) == "select":
                    ctx_by_depth.pop(depth, None)
                elif lowered in ("limit", "fetch", "offset") and ctx_by_depth.get(depth) == "order_by":
                    ctx_by_depth.pop(depth, None)
                elif lowered == "in":
                    pending_in = True

                out.append(segment[i:next_idx])
                i = next_idx
                continue

            if ch == ";":
                next_word, _ = _next_word(segment, i + 1)
                if depth == 0 and (next_word is None or next_word.lower() in start_keywords):
                    out.append(ch)
                elif in_list_depths or ctx_by_depth.get(depth) in ("select", "order_by"):
                    out.append(",")
                else:
                    out.append(ch)
                i += 1
                continue

            if pending_in and not ch.isspace():
                pending_in = False

            out.append(ch)
            i += 1

        return "".join(out)

    parts: list[str] = []
    last = 0
    for start, end in segments:
        parts.append(sql[last:start])
        parts.append(_process_segment(sql[start:end]))
        last = end
    parts.append(sql[last:])
    return "".join(parts)


StringifyLiteralRule = tuple[str, int, int, str, str]


def _stringify_literal(
    value: str,
    prefix: str,
    pad_width: int,
    amplification_extra_pad: int = 0,
    amplification_separator: str = "~",
    amplification_marker: str = "X",
) -> str:
    return (
        "'"
        + dsdgen_stringify.stringify_value(
            value,
            prefix,
            pad_width,
            amplification_extra_pad=amplification_extra_pad,
            amplification_separator=amplification_separator,
            amplification_marker=amplification_marker,
        )
        + "'"
    )


def _build_stringify_lookup(
    config: stringification_cfg.StringificationConfig,
) -> tuple[dict[str, StringifyLiteralRule], dict[str, StringifyLiteralRule]]:
    rules = dsdgen_stringify.build_rules(config)
    if not rules:
        return {}, {}

    qualified: dict[str, StringifyLiteralRule] = {}
    column_rules: dict[str, set[StringifyLiteralRule]] = {}

    for table, cols in rules.items():
        for column, cfg in cols.items():
            prefix = str(cfg.get("prefix", ""))
            pad_width = int(cfg.get("pad_width", 0))
            amplification_extra_pad = int(cfg.get("amplification_extra_pad", 0))
            amplification_separator = str(cfg.get("amplification_separator", "~"))
            amplification_marker = str(cfg.get("amplification_marker", "X"))
            key = f"{table.lower()}.{column.lower()}"
            rule: StringifyLiteralRule = (
                prefix,
                pad_width,
                amplification_extra_pad,
                amplification_separator,
                amplification_marker,
            )
            qualified[key] = rule
            column_rules.setdefault(column.lower(), set()).add(rule)

    unique = {
        col: next(iter(rule_set))
        for col, rule_set in column_rules.items()
        if len(rule_set) == 1
    }
    return unique, qualified


def _compile_stringify_patterns(unique_cols: list[str], qualified_cols: list[str]) -> dict[str, re.Pattern]:
    patterns: dict[str, re.Pattern] = {}
    if unique_cols:
        col_group = "|".join(re.escape(col) for col in unique_cols)
        col_pattern = rf"\b(?:\w+\.)?(?:{col_group})\b"
        patterns["unique_eq"] = re.compile(
            rf"(?P<col>{col_pattern})\s*(?P<op>=|<>|!=|<=|>=|<|>)\s*(?P<num>-?\d+)\b",
            re.IGNORECASE,
        )
        patterns["unique_eq_rev"] = re.compile(
            rf"(?P<num>-?\d+)\s*(?P<op>=|<>|!=|<=|>=|<|>)\s*(?P<col>{col_pattern})",
            re.IGNORECASE,
        )
        patterns["unique_between"] = re.compile(
            rf"(?P<col>{col_pattern})\s+between\s+(?P<n1>-?\d+)\s+and\s+(?P<n2>-?\d+)",
            re.IGNORECASE,
        )
        patterns["unique_between_expr"] = re.compile(
            rf"(?P<col>{col_pattern})\s+between\s+(?P<n1>-?\d+)\s+and\s+(?P<n2>-?\d+)\s*(?P<offset>[+-])\s*(?P<delta>\d+)",
            re.IGNORECASE,
        )
        patterns["unique_in"] = re.compile(
            rf"(?P<col>{col_pattern})\s+in\s*\((?P<list>[^)]*)\)",
            re.IGNORECASE,
        )

    if qualified_cols:
        qualified_group = "|".join(
            rf"(?:{re.escape(table)}\s*\.\s*{re.escape(column)})"
            for table, column in (col.split(".", 1) for col in qualified_cols)
        )
        col_pattern = rf"\b(?:{qualified_group})\b"
        patterns["qualified_eq"] = re.compile(
            rf"(?P<col>{col_pattern})\s*(?P<op>=|<>|!=|<=|>=|<|>)\s*(?P<num>-?\d+)\b",
            re.IGNORECASE,
        )
        patterns["qualified_eq_rev"] = re.compile(
            rf"(?P<num>-?\d+)\s*(?P<op>=|<>|!=|<=|>=|<|>)\s*(?P<col>{col_pattern})",
            re.IGNORECASE,
        )
        patterns["qualified_between"] = re.compile(
            rf"(?P<col>{col_pattern})\s+between\s+(?P<n1>-?\d+)\s+and\s+(?P<n2>-?\d+)",
            re.IGNORECASE,
        )
        patterns["qualified_between_expr"] = re.compile(
            rf"(?P<col>{col_pattern})\s+between\s+(?P<n1>-?\d+)\s+and\s+(?P<n2>-?\d+)\s*(?P<offset>[+-])\s*(?P<delta>\d+)",
            re.IGNORECASE,
        )
        patterns["qualified_in"] = re.compile(
            rf"(?P<col>{col_pattern})\s+in\s*\((?P<list>[^)]*)\)",
            re.IGNORECASE,
        )

    return patterns


def _normalize_qualified_col(raw: str) -> str:
    return re.sub(r"\s+", "", raw).lower()


def _rewrite_in_list(
    raw_list: str,
    *,
    rule: StringifyLiteralRule,
) -> str:
    if re.search(r"\bselect\b", raw_list, re.IGNORECASE):
        return raw_list
    fixed = raw_list.replace(";", ",")
    prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker = rule

    def _replace_token(match: re.Match) -> str:
        return _stringify_literal(
            match.group(0),
            prefix,
            pad_width,
            amplification_extra_pad,
            amplification_separator,
            amplification_marker,
        )

    return re.sub(r"\b-?\d+\b", _replace_token, fixed)


def _apply_stringify_patterns(
    segment: str,
    *,
    patterns: dict[str, re.Pattern],
    unique_rules: dict[str, StringifyLiteralRule],
    qualified_rules: dict[str, StringifyLiteralRule],
) -> str:
    def _lookup(col_text: str, *, qualified: bool) -> StringifyLiteralRule | None:
        if qualified:
            key = _normalize_qualified_col(col_text)
            return qualified_rules.get(key)
        col_name = col_text.split(".")[-1].lower()
        return unique_rules.get(col_name)

    def _replace_eq(match: re.Match, *, qualified: bool) -> str:
        col = match.group("col")
        rule = _lookup(col, qualified=qualified)
        if rule is None:
            return match.group(0)
        prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker = rule
        num = match.group("num")
        op = match.group("op")
        return (
            f"{col} {op} "
            f"{_stringify_literal(num, prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker)}"
        )

    def _replace_eq_rev(match: re.Match, *, qualified: bool) -> str:
        col = match.group("col")
        rule = _lookup(col, qualified=qualified)
        if rule is None:
            return match.group(0)
        prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker = rule
        num = match.group("num")
        op = match.group("op")
        return (
            f"{_stringify_literal(num, prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker)}"
            f" {op} {col}"
        )

    def _replace_between(match: re.Match, *, qualified: bool) -> str:
        col = match.group("col")
        rule = _lookup(col, qualified=qualified)
        if rule is None:
            return match.group(0)
        prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker = rule
        n1 = match.group("n1")
        n2 = match.group("n2")
        return (
            f"{col} between {_stringify_literal(n1, prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker)}"
            f" and {_stringify_literal(n2, prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker)}"
        )

    def _replace_between_expr(match: re.Match, *, qualified: bool) -> str:
        col = match.group("col")
        rule = _lookup(col, qualified=qualified)
        if rule is None:
            return match.group(0)
        prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker = rule
        n1 = int(match.group("n1"))
        n2 = int(match.group("n2"))
        delta = int(match.group("delta"))
        if match.group("offset") == "-":
            delta = -delta
        n2_resolved = n2 + delta
        return (
            f"{col} between {_stringify_literal(n1, prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker)}"
            f" and {_stringify_literal(n2_resolved, prefix, pad_width, amplification_extra_pad, amplification_separator, amplification_marker)}"
        )

    def _replace_in(match: re.Match, *, qualified: bool) -> str:
        col = match.group("col")
        rule = _lookup(col, qualified=qualified)
        if rule is None:
            return match.group(0)
        list_body = match.group("list")
        updated = _rewrite_in_list(list_body, rule=rule)
        return f"{col} in ({updated})"

    if "qualified_eq" in patterns:
        segment = patterns["qualified_eq"].sub(lambda m: _replace_eq(m, qualified=True), segment)
        segment = patterns["qualified_eq_rev"].sub(
            lambda m: _replace_eq_rev(m, qualified=True), segment
        )
        segment = patterns["qualified_between_expr"].sub(
            lambda m: _replace_between_expr(m, qualified=True), segment
        )
        segment = patterns["qualified_between"].sub(
            lambda m: _replace_between(m, qualified=True), segment
        )
        segment = patterns["qualified_in"].sub(lambda m: _replace_in(m, qualified=True), segment)

    if "unique_eq" in patterns:
        segment = patterns["unique_eq"].sub(lambda m: _replace_eq(m, qualified=False), segment)
        segment = patterns["unique_eq_rev"].sub(
            lambda m: _replace_eq_rev(m, qualified=False), segment
        )
        segment = patterns["unique_between_expr"].sub(
            lambda m: _replace_between_expr(m, qualified=False), segment
        )
        segment = patterns["unique_between"].sub(
            lambda m: _replace_between(m, qualified=False), segment
        )
        segment = patterns["unique_in"].sub(lambda m: _replace_in(m, qualified=False), segment)

    return segment


def _postprocess_stringified_literals(
    output_dir: Path, config: stringification_cfg.StringificationConfig
) -> None:
    for path in output_dir.glob("*.sql"):
        sql = path.read_text(encoding="utf-8")
        rewritten = _rewrite_stringified_literals_sql(sql, config=config)
        if rewritten != sql:
            path.write_text(rewritten, encoding="utf-8")


def _rewrite_stringified_literals_sql(
    sql: str, *, config: stringification_cfg.StringificationConfig
) -> str:
    unique_rules, qualified_rules = _build_stringify_lookup(config)
    if not unique_rules and not qualified_rules:
        return sql

    patterns = _compile_stringify_patterns(list(unique_rules.keys()), list(qualified_rules.keys()))
    if not patterns:
        return sql

    segments = _sql_code_segments(sql)
    if not segments:
        return sql

    parts: list[str] = []
    last = 0
    for start, end in segments:
        parts.append(sql[last:start])
        parts.append(
            _apply_stringify_patterns(
                sql[start:end],
                patterns=patterns,
                unique_rules=unique_rules,
                qualified_rules=qualified_rules,
            )
        )
        last = end
    parts.append(sql[last:])
    return "".join(parts)


def _yaml_available() -> bool:
    try:
        import yaml  # type: ignore
    except Exception:
        return False
    return True


def _resolve_union_targets(max_inputs: int | None) -> list[int]:
    targets: list[int] = []
    for t in UNION_FANIN_TARGETS:
        if max_inputs is not None:
            t = min(t, max_inputs)
        if t < 2:
            continue
        if t not in targets:
            targets.append(t)
    return targets


def _generate_union_template(*, inputs: int, output_template: Path) -> None:
    script = REPO_ROOT / "workload" / "dsqgen" / "generate_union_query.py"
    cmd = [sys.executable, str(script), "--inputs", str(inputs)]
    output_template.parent.mkdir(parents=True, exist_ok=True)
    with output_template.open("w", encoding="utf-8") as dst:
        subprocess.run(cmd, stdout=dst, check=True)


def _run_join_query(
    *,
    dsqgen_bin: Path,
    join_template: Path,
    output_dir: Path,
    dialect: str,
    scale: str,
    template_dir: Path,
) -> None:
    # dsqgen stores option strings in fixed 80-byte buffers; long paths can corrupt heap.
    # Use a short repo-root temp dir to keep -DIRECTORY/-OUTPUT_DIR safely under the limit.
    tmp_root = REPO_ROOT / ".join_tmp"
    tmp_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="join_", dir=str(tmp_root)))

    dialect_tpl = template_dir / f"{dialect}.tpl"
    if not dialect_tpl.exists():
        raise SystemExit(f"Dialect template not found: {dialect_tpl}")
    tmp_dialect = tmp_dir / dialect_tpl.name
    tmp_dialect.write_text(dialect_tpl.read_text(encoding="utf-8"), encoding="utf-8")

    tmp_join_template = tmp_dir / join_template.name
    tmp_join_template.write_text(join_template.read_text(encoding="utf-8"), encoding="utf-8")

    cmd = [
        str(dsqgen_bin),
        "-DIRECTORY",
        str(tmp_dir),
        "-TEMPLATE",
        tmp_join_template.name,
        "-DIALECT",
        dialect,
        "-SCALE",
        scale,
        "-OUTPUT_DIR",
        str(tmp_dir),
    ]
    _run_dsqgen(cmd, quiet=True)

    combined = tmp_dir / "query_0.sql"
    if combined.exists():
        target = output_dir / f"{join_template.stem}.sql"
        target.parent.mkdir(parents=True, exist_ok=True)
        combined.replace(target)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _cleanup_join_template(path: Path) -> None:
    if path.exists():
        path.unlink()


def _maybe_remove_empty_dir(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        return
    try:
        next(path.iterdir())
    except StopIteration:
        path.rmdir()


def _parse_join_targets(raw: list[str] | None, fallback: Sequence[int]) -> list[int]:
    if not raw:
        targets: list[int] = []
        for value in fallback:
            target = int(value)
            if target <= 0:
                raise SystemExit(f"Invalid join target '{target}'. Must be > 0.")
            if target not in targets:
                targets.append(target)
        return targets
    targets: list[int] = []
    for chunk in raw:
        if chunk is None:
            continue
        for part in chunk.split(","):
            value = part.strip()
            if not value:
                continue
            try:
                target = int(value)
            except ValueError as exc:
                raise SystemExit(f"Invalid join target '{value}'. Must be an integer.") from exc
            if target <= 0:
                raise SystemExit(f"Invalid join target '{value}'. Must be > 0.")
            if target not in targets:
                targets.append(target)
    if not targets:
        return [fallback]
    return targets


def _parse_scale_as_float(raw: str) -> float | None:
    try:
        return float(raw)
    except Exception:
        return None


def _scale_tags_equal(left: str, right: str) -> bool:
    left_value = _parse_scale_as_float(left)
    right_value = _parse_scale_as_float(right)
    if left_value is not None and right_value is not None:
        return abs(left_value - right_value) <= 1e-9
    return left.strip().lower() == right.strip().lower()


def _scale_to_tag(raw: str) -> str:
    value = _parse_scale_as_float(raw)
    if value is not None:
        rounded = round(value)
        if abs(value - rounded) <= 1e-9:
            return str(int(rounded))
        text = f"{value:.10f}".rstrip("0").rstrip(".")
    else:
        text = raw.strip()
    text = text.replace(".", "p")
    text = re.sub(r"[^a-zA-Z0-9_-]", "_", text)
    return text or "unknown"


def _resolve_seed_overrides_path(*, scale: str, stringification_level: int) -> Path:
    return (
        DEFAULT_SEED_OVERRIDES_DIR
        / f"seed_overrides_sf{_scale_to_tag(scale)}_str{int(stringification_level)}.yml"
    )


def _resolve_seed_overrides_paths(
    *, scale: str, stringification_level: int, dialect: str
) -> list[Path]:
    generic = _resolve_seed_overrides_path(
        scale=scale,
        stringification_level=stringification_level,
    )
    suffix = re.sub(r"[^a-z0-9_]+", "", dialect.strip().lower())
    if not suffix:
        return [generic]
    dialect_specific = (
        DEFAULT_SEED_OVERRIDES_DIR
        / f"seed_overrides_sf{_scale_to_tag(scale)}_str{int(stringification_level)}_{suffix}.yml"
    )
    if dialect_specific == generic:
        return [generic]
    return [generic, dialect_specific]


def _normalize_query_filename(raw: str) -> str | None:
    value = raw.strip()
    if not value:
        return None
    if value.isdigit():
        return f"query_{int(value)}.sql"
    lowered = value.lower()
    if lowered.startswith("query_") and lowered.endswith(".sql"):
        num = lowered[len("query_") : -len(".sql")]
        if num.isdigit():
            return f"query_{int(num)}.sql"
        return None
    if lowered.startswith("query_"):
        suffix = lowered[len("query_") :]
        if suffix.isdigit():
            return f"query_{int(suffix)}.sql"
    return None


def _load_seed_overrides(
    path: Path,
    *,
    expected_scale: str | None = None,
    expected_stringification_level: int | None = None,
) -> dict[str, dict[str, object]]:
    if not path.exists():
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        print(
            f"[seed-overrides] PyYAML unavailable; ignoring overrides file {path}.",
            file=sys.stderr,
        )
        return {}

    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        return {}
    meta = payload.get("meta")
    if isinstance(meta, dict):
        if expected_stringification_level is not None and "stringification_level" in meta:
            try:
                meta_level = int(meta["stringification_level"])
            except Exception:
                meta_level = None
            if (
                meta_level is not None
                and int(expected_stringification_level) != meta_level
            ):
                print(
                    (
                        "[seed-overrides] ignored overrides file {} "
                        "(meta stringification_level={} expected={})."
                    ).format(path, meta_level, expected_stringification_level),
                    file=sys.stderr,
                )
                return {}
        if expected_scale is not None and "scale" in meta:
            meta_scale = str(meta["scale"])
            if not _scale_tags_equal(meta_scale, expected_scale):
                print(
                    (
                        "[seed-overrides] ignored overrides file {} "
                        "(meta scale={} expected={})."
                    ).format(path, meta_scale, expected_scale),
                    file=sys.stderr,
                )
                return {}

    raw_mapping = payload.get("queries", payload)
    if not isinstance(raw_mapping, dict):
        return {}

    normalized: dict[str, dict[str, object]] = {}
    for raw_key, raw_value in raw_mapping.items():
        key = _normalize_query_filename(str(raw_key))
        if key is None:
            continue
        seed: int | None = None
        template_name: str | None = None
        if isinstance(raw_value, int):
            seed = int(raw_value)
        elif isinstance(raw_value, dict):
            if "seed" in raw_value:
                try:
                    seed = int(raw_value["seed"])
                except Exception:
                    seed = None
            if "template" in raw_value and raw_value["template"] is not None:
                template_name = str(raw_value["template"]).strip() or None
        if seed is None:
            continue
        if seed < 0:
            continue
        entry: dict[str, object] = {"seed": seed}
        if template_name:
            entry["template"] = template_name
        normalized[key] = entry
    return normalized


def _parse_streams_permutation(combined_path: Path) -> dict[int, int]:
    """Parse query_0.sql to build {template_num: query_position} from STREAMS output.

    Each query block starts with a line like:
      -- start query 4 in stream 0 using template query44_ext.tpl
    This means position 4 → template 44.  We return {44: 4, ...}.
    """
    if not combined_path.exists():
        return {}
    text = combined_path.read_text(encoding="utf-8")
    mapping: dict[int, int] = {}
    for line in text.splitlines():
        if not line.lower().startswith("-- start query"):
            continue
        parts = line.strip("- \n").split()
        try:
            qpos = int(parts[2])
            tpl_name = parts[-1]  # e.g. "query44_ext.tpl"
            m = re.match(r"query(\d+)", tpl_name)
            if m:
                mapping[int(m.group(1))] = qpos
        except (IndexError, ValueError):
            continue
    return mapping


def _remap_seed_overrides_for_streams(
    overrides: dict[str, dict[str, object]],
    perm: dict[int, int],
) -> dict[str, dict[str, object]]:
    """Remap seed override keys from QUALIFY numbering to STREAMS numbering.

    Seed override files assume query_N.sql corresponds to template N (QUALIFY mode).
    In STREAMS mode, template N appears at a different position. This function
    remaps the keys so the override is applied to the correct query file.
    """
    if not perm:
        return overrides
    remapped: dict[str, dict[str, object]] = {}
    for filename, entry in overrides.items():
        tpl_name = str(entry.get("template", ""))
        m = re.match(r"query(\d+)", tpl_name)
        if m:
            tpl_num = int(m.group(1))
            if tpl_num in perm:
                new_filename = f"query_{perm[tpl_num]}.sql"
                remapped[new_filename] = entry
                continue
        # Fallback: keep original key
        remapped[filename] = entry
    return remapped


def _resolve_query_template_name(
    *,
    qnum: int,
    template_dir: Path,
    enabled_ext_templates: set[str],
    explicit_template: str | None = None,
) -> str:
    if explicit_template:
        return explicit_template
    ext_name = f"query{qnum}_ext.tpl"
    ext_path = template_dir / ext_name
    if ext_path.exists() and ext_name in enabled_ext_templates:
        return ext_name
    return f"query{qnum}.tpl"


def _generate_single_query_sql(
    *,
    dsqgen_bin: Path,
    template_dir: Path,
    template_name: str,
    dialect: str,
    scale: str,
    rng_seed: int,
) -> str:
    cmd = [
        str(dsqgen_bin),
        "-DIRECTORY",
        str(template_dir),
        "-TEMPLATE",
        template_name,
        "-DIALECT",
        dialect,
        "-SCALE",
        scale,
        "-RNGSEED",
        str(rng_seed),
        "-FILTER",
        "Y",
    ]
    proc = subprocess.run(cmd, cwd=str(dsqgen_bin.parent), text=True, capture_output=True)
    if proc.returncode != 0:
        msg = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(msg or f"dsqgen failed for {template_name}")
    sql = _sanitize_generated_sql(proc.stdout)
    if not sql:
        raise RuntimeError(f"Generated SQL is empty for template {template_name}")
    return sql


def _apply_seed_overrides(
    *,
    output_dir: Path,
    template_dir: Path,
    dsqgen_bin: Path,
    dialect: str,
    scale: str,
    compat_rewrite: bool,
    schema_config: stringification_cfg.StringificationConfig,
    stringification_level: int,
    enabled_ext_templates: set[str],
    overrides_path: Path,
    streams_permutation: dict[int, int] | None = None,
) -> list[str]:
    overrides = _load_seed_overrides(
        overrides_path,
        expected_scale=scale,
        expected_stringification_level=stringification_level,
    )
    if not overrides:
        return []
    if streams_permutation:
        overrides = _remap_seed_overrides_for_streams(overrides, streams_permutation)

    applied: list[str] = []
    for query_filename in sorted(overrides.keys()):
        query_path = output_dir / query_filename
        if not query_path.exists():
            continue
        qmatch = re.match(r"query_(\d+)\.sql$", query_filename, flags=re.IGNORECASE)
        if not qmatch:
            continue
        qnum = int(qmatch.group(1))
        override = overrides[query_filename]
        seed = int(override["seed"])
        explicit_template = None
        if "template" in override and override["template"] is not None:
            explicit_template = str(override["template"])
        template_name = _resolve_query_template_name(
            qnum=qnum,
            template_dir=template_dir,
            enabled_ext_templates=enabled_ext_templates,
            explicit_template=explicit_template,
        )
        sql = _generate_single_query_sql(
            dsqgen_bin=dsqgen_bin,
            template_dir=template_dir,
            template_name=template_name,
            dialect=dialect,
            scale=scale,
            rng_seed=seed,
        )
        if compat_rewrite:
            if dialect.lower() == "duckdb":
                sql = _rewrite_duckdb_sql(query_filename, sql)
            else:
                sql = _rewrite_postgres_sql(query_filename, sql)
        if schema_config.schema_selected:
            sql = _rewrite_stringified_literals_sql(sql, config=schema_config)
        query_path.write_text(sql.rstrip() + "\n", encoding="utf-8")
        print(
            f"[seed-overrides] applied {query_filename} seed={seed} template={template_name}",
            file=sys.stderr,
        )
        applied.append(query_filename)
    return applied


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Run dsqgen with extensions by default, optionally generate join-heavy templates."
    )
    ap.add_argument(
        "--output-dir",
        default=None,
        help="Directory for generated SQL output (required unless --default is used).",
    )
    ap.add_argument("--dialect", default="ansi", help="Dsqgen dialect (default: ansi).")
    ap.add_argument(
        "--postgres-compat",
        action="store_true",
        help="Apply Postgres post-processing even when dialect is not postgres.",
    )
    ap.add_argument("--scale", default="1", help="Scale factor (default: 1).")
    ap.add_argument(
        "--no-clear-output",
        action="store_true",
        help="Do not clear the output directory before generation.",
    )
    ap.add_argument(
        "--qualify",
        dest="qualify",
        action="store_true",
        help="Generate queries in qualification (ascending template) order.",
    )
    ap.add_argument(
        "--no-qualify",
        dest="qualify",
        action="store_false",
        help="Do not force qualification order (use TPC-DS stream permutation, default).",
    )

    ap.add_argument(
        "--no-extensions",
        action="store_true",
        help="Use base templates.lst (skip *_ext.tpl resolution).",
    )
    ap.add_argument(
        "--template-dir",
        default="query_templates",
        help="Template directory (default: query_templates).",
    )
    ap.add_argument(
        "--template-input",
        default="query_templates/templates.lst",
        help="Base template list (default: query_templates/templates.lst).",
    )
    ap.add_argument(
        "--template-list",
        default="query_templates/templates_ext.lst",
        help="Resolved template list (default: query_templates/templates_ext.lst).",
    )
    ap.add_argument(
        "--stringification-level",
        "--stringify-level",
        dest="stringification_level",
        type=_positive_int,
        help="Stringification level (1-15; default: 10). STR=1 is vanilla TPC-DS, STR=10 recasts all 131 columns, STR=11-15 extends string length.",
    )
    ap.add_argument(
        "--stringification-preset",
        dest="stringification_preset",
        type=str,
        choices=sorted(stringification_cfg.PRESET_LEVELS.keys()),
        help="Stringification preset controlling template extensions.",
    )
    ap.add_argument(
        "--pure-data-mode",
        action="store_true",
        help=(
            "Disable level-dependent query-layer stringification (no *_ext.tpl activation, "
            "no literal postprocess, no seed overrides)."
        ),
    )
    ap.add_argument(
        "--str-plus-max-level",
        type=_positive_int,
        default=stringification_cfg.DEFAULT_STR_PLUS_MAX_LEVEL,
        help="Maximum accepted level in STR+ mode (default: 20).",
    )
    ap.add_argument(
        "--str-plus-pad-step",
        type=_positive_int,
        default=2,
        help="Extra suffix growth per level above STR10 (default: 2).",
    )
    ap.add_argument(
        "--str-plus-separator",
        type=str,
        default="~",
        help="Suffix separator for STR+ literals/data (default: ~).",
    )
    ap.add_argument(
        "--str-plus-marker",
        type=str,
        default="X",
        help="Suffix marker for STR+ literals/data (default: X).",
    )

    ap.add_argument(
        "--split",
        dest="split",
        action="store_true",
        help="Split query_0.sql into individual query files (template list only).",
    )
    ap.add_argument(
        "--no-split",
        dest="split",
        action="store_false",
        help="Do not split query_0.sql (template list only).",
    )
    ap.set_defaults(split=None)
    ap.add_argument(
        "--keep-combined",
        action="store_true",
        help="Keep query_0.sql after splitting.",
    )

    ap.add_argument(
        "--join-only",
        action="store_true",
        help="Generate and run only the join-heavy query (skip the full template list).",
    )
    ap.add_argument(
        "--join",
        dest="include_join",
        action="store_true",
        help="Include a join-heavy query in addition to the template list.",
    )
    ap.add_argument(
        "--no-join",
        dest="include_join",
        action="store_false",
        help="Do not include a join-heavy query.",
    )
    ap.add_argument(
        "--union",
        dest="include_union",
        action="store_true",
        help="Include UNION ALL fan-in queries (default: enabled).",
    )
    ap.add_argument(
        "--no-union",
        dest="include_union",
        action="store_false",
        help="Do not include UNION ALL fan-in queries.",
    )
    ap.add_argument(
        "--join-config",
        default="workload/config/returns.yml",
        help="Join generator config (default: workload/config/returns.yml).",
    )
    ap.add_argument(
        "--join-base-template",
        default="workload/templates/base_returns.tpl",
        help="Base template for join generator (default: workload/templates/base_returns.tpl).",
    )
    ap.add_argument(
        "--join-target",
        type=int,
        default=None,
        help="Target join count (used when --join-targets is not provided).",
    )
    ap.add_argument(
        "--join-targets",
        action="append",
        default=None,
        help="Comma-separated list of join targets (e.g. 200,500,1500). Overrides --join-target.",
    )
    ap.add_argument(
        "--join-template-out",
        default=None,
        help="Output path for generated join template (default: <output-dir>/query_join_J{target}.tpl).",
    )
    ap.add_argument(
        "--union-max-inputs",
        type=int,
        default=None,
        help="Cap the maximum UNION ALL fan-in inputs (default: no cap).",
    )
    ap.add_argument(
        "--default",
        action="store_true",
        help="Use recommended defaults: STR=10, dialect=duckdb, output=./queries.",
    )

    ap.set_defaults(include_join=True)
    ap.set_defaults(include_union=True)
    ap.set_defaults(qualify=False)
    args = ap.parse_args(argv)

    # --no-extensions implies no join/union micro-suite queries
    if args.no_extensions:
        args.include_join = False
        args.include_union = False

    # --default: STR=10, dialect=duckdb, output=./queries
    if args.default:
        if args.stringification_level is None and args.stringification_preset is None:
            args.stringification_level = 10
        if args.dialect == "ansi":
            args.dialect = "duckdb"
        if args.output_dir is None:
            args.output_dir = "./queries"
        print("[default] Using recommended defaults: STR=10, dialect=duckdb, output=./queries")

    if args.output_dir is None:
        raise SystemExit("--output-dir is required (or use --default).")

    if args.stringification_level is not None and args.stringification_preset is not None:
        raise SystemExit("--stringification-level and --stringification-preset are mutually exclusive.")
    resolved_level, resolved_preset = stringification_cfg.resolve_level(
        args.stringification_level,
        args.stringification_preset,
        allow_extended=True,
        max_level=int(args.str_plus_max_level),
    )
    stringify_cfg = dsdgen_config.stringify_rules()
    base_pad_width = int(stringify_cfg.get("pad_width", 8))
    schema_config = stringification_cfg.build_stringification_config(
        level=resolved_level,
        preset=resolved_preset,
        base_pad_width=base_pad_width,
        str_plus_max_level=int(args.str_plus_max_level),
        str_plus_pad_step=int(args.str_plus_pad_step),
        str_plus_separator=str(args.str_plus_separator),
        str_plus_marker=str(args.str_plus_marker),
    )
    pure_data_mode = bool(args.pure_data_mode)

    dsqgen_bin = _resolve_dsqgen_binary()
    output_dir = _resolve_path(REPO_ROOT, args.output_dir)
    if args.no_clear_output:
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        _clear_output_dir(output_dir)

    fallback_targets = [args.join_target] if args.join_target is not None else DEFAULT_JOIN_TARGETS
    join_targets = _parse_join_targets(args.join_targets, fallback_targets)
    if (args.join_only or args.include_join) and not _yaml_available():
        if args.join_only:
            raise SystemExit(
                "PyYAML is required for join-only generation. Install PyYAML or run without --join-only."
            )
        print(
            "Warning: PyYAML not installed; skipping join-heavy query generation. "
            "Install PyYAML to enable it.",
            file=sys.stderr,
        )
        args.include_join = False
    if args.join_template_out and len(join_targets) > 1:
        raise SystemExit("--join-template-out can only be used with a single join target.")

    if args.join_only:
        cfg = _resolve_path(REPO_ROOT, args.join_config)
        base_template = _resolve_path(REPO_ROOT, args.join_base_template)
        template_dir = REPO_ROOT / "query_templates"
        join_tpl_root = output_dir / ".join_templates"
        join_tpl_root.mkdir(parents=True, exist_ok=True)
        for target in join_targets:
            cleanup_join_template = False
            if args.join_template_out:
                join_template = _resolve_path(REPO_ROOT, args.join_template_out)
            else:
                join_template = join_tpl_root / f"query_join_J{target}.tpl"
                cleanup_join_template = True
            join_tpl_root.mkdir(parents=True, exist_ok=True)

            _generate_join_template(
                cfg=cfg,
                target=target,
                base_template=base_template,
                output_template=join_template,
            )

            _run_join_query(
                dsqgen_bin=dsqgen_bin,
                join_template=join_template,
                output_dir=output_dir,
                dialect=args.dialect,
                scale=str(args.scale),
                template_dir=template_dir,
            )
            if cleanup_join_template:
                _cleanup_join_template(join_template)
        _maybe_remove_empty_dir(join_tpl_root)

        split_default = False
        selection = stringification_cfg.QuerySelection(candidates=(), selected=(), k_query=0, K_query_max=0)
    else:
        template_dir = _resolve_path(REPO_ROOT, args.template_dir)
        template_input = _resolve_path(REPO_ROOT, args.template_input)
        template_list = _resolve_path(REPO_ROOT, args.template_list)

        if not args.no_extensions and not pure_data_mode:
            selection = _resolve_templates(
                template_input,
                template_dir,
                template_list,
                stringification_level=resolved_level,
                stringification_preset=resolved_preset,
                str_plus_max_level=int(args.str_plus_max_level),
                str_plus_pad_step=int(args.str_plus_pad_step),
                str_plus_separator=str(args.str_plus_separator),
                str_plus_marker=str(args.str_plus_marker),
            )
            template_source = template_list
        else:
            template_source = template_input
            selection = stringification_cfg.QuerySelection(candidates=(), selected=(), k_query=0, K_query_max=0)

        cmd = [
            str(dsqgen_bin),
            "-DIRECTORY",
            str(template_dir),
            "-INPUT",
            str(template_source),
            "-DIALECT",
            args.dialect,
            "-SCALE",
            str(args.scale),
            "-OUTPUT_DIR",
            str(output_dir),
        ]
        if args.qualify:
            cmd.extend(["-QUALIFY", "Y"])
        else:
            cmd.extend(["-STREAMS", "1"])
        _run_dsqgen(cmd)

        # Extract STREAMS permutation before splitting removes the combined file.
        streams_perm: dict[int, int] = {}
        if not args.qualify:
            streams_perm = _parse_streams_permutation(output_dir / "query_0.sql")
            if streams_perm:
                # Write permutation map so downstream tools (reproduce.sh) can
                # correctly map dialect variant overlays by template number.
                perm_path = output_dir / "_permutation.json"
                # Format: {template_num: query_position}
                perm_path.write_text(
                    json.dumps(streams_perm, sort_keys=True) + "\n",
                    encoding="utf-8",
                )

        split_default = True

        if args.include_join:
            cfg = _resolve_path(REPO_ROOT, args.join_config)
            base_template = _resolve_path(REPO_ROOT, args.join_base_template)
            template_dir = REPO_ROOT / "query_templates"
            join_tpl_root = output_dir / ".join_templates"
            join_tpl_root.mkdir(parents=True, exist_ok=True)
            for target in join_targets:
                cleanup_join_template = False
                if args.join_template_out:
                    join_template = _resolve_path(REPO_ROOT, args.join_template_out)
                else:
                    join_template = join_tpl_root / f"query_join_J{target}.tpl"
                    cleanup_join_template = True
                join_tpl_root.mkdir(parents=True, exist_ok=True)

                _generate_join_template(
                    cfg=cfg,
                    target=target,
                    base_template=base_template,
                    output_template=join_template,
                )
                _run_join_query(
                    dsqgen_bin=dsqgen_bin,
                    join_template=join_template,
                    output_dir=output_dir,
                    dialect=args.dialect,
                    scale=str(args.scale),
                    template_dir=template_dir,
                )
                if cleanup_join_template:
                    _cleanup_join_template(join_template)
            _maybe_remove_empty_dir(join_tpl_root)

    split_queries = args.split if args.split is not None else split_default
    if split_queries:
        _split_queries(output_dir, keep_combined=args.keep_combined)
        _sanitize_query_files(output_dir)

    if not args.join_only:
        _postprocess_limits(output_dir, template_dir, scale=args.scale)
        # Dialect-independent SQL fixups (dsqgen template bugs)
        for path in output_dir.glob("*.sql"):
            sql = path.read_text(encoding="utf-8")
            fixed = _rewrite_select_top_distinct(sql)
            if fixed != sql:
                path.write_text(fixed, encoding="utf-8")
        postgres_compat = args.dialect.lower() == "postgres" or args.postgres_compat
        duckdb_compat = args.dialect.lower() == "duckdb"
        compat_rewrite = postgres_compat or duckdb_compat
        if compat_rewrite:
            if args.dialect.lower() == "duckdb":
                _postprocess_duckdb(output_dir)
            else:
                _postprocess_postgres(output_dir)
        if schema_config.schema_selected and not pure_data_mode:
            _postprocess_stringified_literals(output_dir, schema_config)

        if args.include_union:
            union_targets = _resolve_union_targets(args.union_max_inputs)
            if union_targets:
                union_tpl_root = output_dir / ".union_templates"
                union_tpl_root.mkdir(parents=True, exist_ok=True)
                for inputs in union_targets:
                    union_template = union_tpl_root / f"query_union_U{inputs}.tpl"
                    _generate_union_template(inputs=inputs, output_template=union_template)
                    _run_join_query(
                        dsqgen_bin=dsqgen_bin,
                        join_template=union_template,
                        output_dir=output_dir,
                        dialect=args.dialect,
                        scale=str(args.scale),
                        template_dir=template_dir,
                    )
                    _cleanup_join_template(union_template)
                _maybe_remove_empty_dir(union_tpl_root)

        if not pure_data_mode:
            overrides_paths = _resolve_seed_overrides_paths(
                scale=str(args.scale),
                stringification_level=resolved_level,
                dialect=args.dialect,
            )
            for overrides_path in overrides_paths:
                if not overrides_path.exists():
                    continue
                enabled_ext_templates = {edit.ext_template for edit in selection.selected}
                _apply_seed_overrides(
                    output_dir=output_dir,
                    template_dir=template_dir,
                    dsqgen_bin=dsqgen_bin,
                    dialect=args.dialect,
                    scale=str(args.scale),
                    compat_rewrite=compat_rewrite,
                    schema_config=schema_config,
                    stringification_level=resolved_level,
                    enabled_ext_templates=enabled_ext_templates,
                    overrides_path=overrides_path,
                    streams_permutation=streams_perm if streams_perm else None,
                )

    manifest_path = output_dir / stringification_cfg.QUERY_MANIFEST_NAME
    stringification_cfg.write_json(
        manifest_path,
        {
            "stringification_level": resolved_level,
            "stringification_preset": resolved_preset,
            "intensity": stringification_cfg.intensity_from_level(resolved_level),
            "str_plus_enabled": bool(schema_config.str_plus_enabled),
            "amplification": {
                "enabled": bool(schema_config.str_plus_enabled),
                "extra_pad": int(schema_config.amplification_extra_pad),
                "pad_step": int(schema_config.amplification_pad_step),
                "separator": str(schema_config.amplification_separator),
                "marker": str(schema_config.amplification_marker),
            },
            "pure_data_mode": pure_data_mode,
            "k_query": selection.k_query,
            "K_query_max": selection.K_query_max,
            "enabled_edits": [
                {
                    "query_id": edit.query_id,
                    "edit_id": edit.edit_id,
                    "template": edit.ext_template,
                }
                for edit in selection.selected
            ],
            "queries_with_edits": [edit.query_id for edit in selection.selected],
        },
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

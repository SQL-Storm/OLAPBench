#!/usr/bin/env python3
"""
Find deterministic dsqgen seeds that yield non-empty results for selected queries.

The script generates one query at a time using dsqgen + wrapper post-processing,
probes Postgres with a cheap EXISTS check, and writes the first passing seed for
each query to a YAML overrides file.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import wrap_dsqgen
from workload import stringification as stringification_cfg
from workload.dsqgen.template_resolver import read_templates

DEFAULT_TARGET_QUERIES = [8, 24, 25, 37, 41, 54, 58]


def _is_word_boundary(ch: str) -> bool:
    return not ch or (not ch.isalnum() and ch != "_")


def _strip_outer_order_limit(sql: str) -> tuple[str, bool]:
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    depth = 0
    order_pos: int | None = None
    limit_pos: int | None = None

    lower_sql = sql.lower()
    index = 0
    while index < len(sql):
        ch = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""
        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
            index += 1
            continue
        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue
        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                index += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                index += 2
                continue
        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                index += 2
                continue
            in_single = not in_single
            index += 1
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            index += 1
            continue
        if in_single or in_double:
            index += 1
            continue
        if ch == "(":
            depth += 1
            index += 1
            continue
        if ch == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        if depth == 0 and lower_sql.startswith("order", index):
            before = lower_sql[index - 1] if index > 0 else ""
            after_order = index + 5
            if _is_word_boundary(before) and _is_word_boundary(
                lower_sql[after_order] if after_order < len(sql) else ""
            ):
                j = after_order
                while j < len(sql) and lower_sql[j].isspace():
                    j += 1
                if lower_sql.startswith("by", j):
                    after_by = j + 2
                    if _is_word_boundary(lower_sql[after_by] if after_by < len(sql) else ""):
                        order_pos = index
                        index += 5
                        continue
        if depth == 0 and lower_sql.startswith("limit", index):
            before = lower_sql[index - 1] if index > 0 else ""
            after = index + 5
            if _is_word_boundary(before) and _is_word_boundary(lower_sql[after] if after < len(sql) else ""):
                limit_pos = index
                index += 5
                continue
        index += 1

    cut_pos: int | None = None
    if order_pos is not None:
        cut_pos = order_pos
    elif limit_pos is not None:
        cut_pos = limit_pos
    if cut_pos is None:
        return sql, False
    stripped = sql[:cut_pos].rstrip()
    if not stripped:
        return sql, False
    return stripped, True


def _parse_queries(raw: str) -> list[int]:
    values: list[int] = []
    for chunk in raw.split(","):
        token = chunk.strip()
        if not token:
            continue
        lowered = token.lower()
        if lowered.startswith("query_") and lowered.endswith(".sql"):
            lowered = lowered[len("query_") : -len(".sql")]
        elif lowered.startswith("query_"):
            lowered = lowered[len("query_") :]
        qnum = int(lowered)
        if qnum <= 0:
            raise ValueError(f"Invalid query number: {qnum}")
        values.append(qnum)
    return values


def _parse_zero_queries_from_audit(path: Path) -> list[int]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON array in audit results: {path}")
    found: list[int] = []
    seen: set[int] = set()
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status")) != "success":
            continue
        try:
            row_count = int(entry.get("row_count", -1))
        except Exception:
            continue
        if row_count != 0:
            continue
        query_name = str(entry.get("query", "")).strip().lower()
        match = re.fullmatch(r"query_(\d+)\.sql", query_name)
        if not match:
            continue
        qnum = int(match.group(1))
        if qnum not in seen:
            seen.add(qnum)
            found.append(qnum)
    return sorted(found)


def _resolve_enabled_ext_templates(
    *,
    template_input: Path,
    template_dir: Path,
    stringification_level: int | None,
    stringification_preset: str | None,
) -> set[str]:
    names = read_templates(template_input)
    cfg = stringification_cfg.build_stringification_config(
        level=stringification_level,
        preset=stringification_preset,
        template_names=names,
        template_dir=template_dir,
    )
    return {edit.ext_template for edit in cfg.query_selected}


def _probe_nonempty(
    *,
    db: str,
    pg_user: str,
    sql: str,
    timeout_ms: int,
    session_settings: list[str],
) -> tuple[bool, bool, str]:
    def _normalize_setting_sql(setting: str) -> str:
        trimmed = setting.strip().rstrip(";")
        if not trimmed:
            raise ValueError("empty session setting")
        if trimmed.lower().startswith("set "):
            return f"{trimmed};"
        if "=" not in trimmed:
            raise ValueError(f"invalid session setting (expected key=value or SET ...): {setting}")
        key, value = trimmed.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ValueError(f"invalid session setting (empty key/value): {setting}")
        if not (
            (value.startswith("'") and value.endswith("'"))
            or (value.startswith('"') and value.endswith('"'))
        ):
            if re.search(r"[A-Za-z]", value):
                value = f"'{value}'"
        return f"SET {key}={value};"

    setting_parts = [f"SET statement_timeout='{timeout_ms}ms';"]
    for setting in session_settings:
        setting_parts.append(_normalize_setting_sql(setting))
    setting_parts.append(f"SELECT EXISTS (SELECT 1 FROM ({sql}) AS _q LIMIT 1);")
    wrapped_sql = " ".join(setting_parts)

    cmd = [
        "psql",
        "-At",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        pg_user,
        "-d",
        db,
        "-c",
        wrapped_sql,
    ]
    env = dict(os.environ)
    env.setdefault("PGHOST", "/var/run/postgresql")
    proc = subprocess.run(cmd, text=True, capture_output=True, env=env)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        if not err:
            err = "unknown_error"
        last = err.splitlines()[-1]
        return False, False, last
    lines = [
        line.strip()
        for line in proc.stdout.splitlines()
        if line.strip() and line.strip().upper() != "SET"
    ]
    exists = bool(lines) and lines[-1].lower() in {"t", "true", "1"}
    return True, exists, ""


def _probe_nonempty_duckdb(
    *,
    db_path: Path,
    duckdb_bin: str,
    sql: str,
    timeout_ms: int,
    session_settings: list[str],
) -> tuple[bool, bool, str]:
    def _normalize_setting_sql(setting: str) -> str:
        trimmed = setting.strip().rstrip(";")
        if not trimmed:
            raise ValueError("empty session setting")
        lowered = trimmed.lower()
        if lowered.startswith("set ") or lowered.startswith("pragma "):
            return f"{trimmed};"
        if "=" not in trimmed:
            raise ValueError(
                f"invalid session setting (expected key=value or SET/PRAGMA ...): {setting}"
            )
        key, value = trimmed.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ValueError(f"invalid session setting (empty key/value): {setting}")
        if re.search(r"\b(memory_limit|threads|worker_threads|max_memory)\b", key, re.IGNORECASE):
            return f"PRAGMA {key}={value};"
        return f"SET {key}={value};"

    parts = ["PRAGMA disable_progress_bar;"]
    for setting in session_settings:
        parts.append(_normalize_setting_sql(setting))
    parts.append(f"SELECT EXISTS (SELECT 1 FROM ({sql}) AS _q LIMIT 1);")
    wrapped_sql = " ".join(parts)
    cmd = [
        duckdb_bin,
        "-noheader",
        "-csv",
        str(db_path),
        "-c",
        wrapped_sql,
    ]
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            capture_output=True,
            timeout=max(1, timeout_ms // 1000),
        )
    except subprocess.TimeoutExpired:
        return False, False, f"process timeout ({timeout_ms}ms)"

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        if not err:
            err = "unknown_error"
        last = err.splitlines()[-1]
        return False, False, last

    lines = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    exists = bool(lines) and lines[-1].lower() in {"1", "t", "true", "yes", "y"}
    return True, exists, ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Search dsqgen seeds that make selected queries non-empty on Postgres."
    )
    ap.add_argument(
        "--queries",
        default=None,
        help="Comma-separated query numbers to search (e.g. 8,24 or query_8.sql,query_24.sql).",
    )
    ap.add_argument(
        "--audit-json",
        default=None,
        help="Optional audit results JSON; if provided (and --queries omitted), use zero-row query list.",
    )
    ap.add_argument("--seed-start", type=int, default=19620718)
    ap.add_argument("--max-attempts", type=int, default=500)
    ap.add_argument("--timeout-ms", type=int, default=60000)
    ap.add_argument(
        "--max-timeout-attempts",
        type=int,
        default=6,
        help="Stop searching a query/template early after this many probe timeouts/errors.",
    )
    ap.add_argument(
        "--max-empty-attempts",
        type=int,
        default=25,
        help="Stop searching a query/template early after this many successful-but-empty probes.",
    )
    ap.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help="Print per-template progress every N attempts.",
    )
    ap.add_argument("--db", default="prodds")
    ap.add_argument("--pg-user", default=os.environ.get("USER", "postgres"))
    ap.add_argument(
        "--engine",
        choices=("postgres", "duckdb"),
        default="postgres",
        help="Probe backend to test non-empty results.",
    )
    ap.add_argument(
        "--duckdb-path",
        default=None,
        help="DuckDB database path (required when --engine=duckdb).",
    )
    ap.add_argument(
        "--duckdb-bin",
        default="duckdb",
        help="DuckDB CLI binary when --engine=duckdb.",
    )
    ap.add_argument(
        "--session-setting",
        action="append",
        default=[],
        help="Optional repeated session setting (key=value or full SET ...).",
    )
    ap.add_argument("--dialect", default="postgres")
    ap.add_argument("--scale", default="1")
    ap.add_argument(
        "--stringification-level",
        type=int,
        choices=range(1, 11),
        default=1,
    )
    ap.add_argument(
        "--stringification-preset",
        choices=sorted(stringification_cfg.PRESET_LEVELS.keys()),
        default=None,
    )
    ap.add_argument("--template-dir", default="query_templates")
    ap.add_argument("--template-input", default="query_templates/templates.lst")
    ap.add_argument(
        "--allow-base-fallback",
        action="store_true",
        default=True,
        help="If ext template search fails, try base queryN.tpl with same seed range.",
    )
    ap.add_argument(
        "--no-base-fallback",
        action="store_false",
        dest="allow_base_fallback",
        help="Disable base-template fallback.",
    )
    ap.add_argument(
        "--overrides-file",
        default=None,
        help="Output YAML path for overrides. Default: configs/seed_overrides_sf<scale>_str<level>.yml",
    )
    ap.add_argument(
        "--strip-outer-order-limit",
        action="store_true",
        help="Strip top-level ORDER BY/LIMIT before probing EXISTS.",
    )
    args = ap.parse_args()

    if args.seed_start < 0:
        raise SystemExit("--seed-start must be >= 0")
    if args.max_attempts <= 0:
        raise SystemExit("--max-attempts must be > 0")
    if args.timeout_ms <= 0:
        raise SystemExit("--timeout-ms must be > 0")
    if args.max_timeout_attempts <= 0:
        raise SystemExit("--max-timeout-attempts must be > 0")
    if args.max_empty_attempts <= 0:
        raise SystemExit("--max-empty-attempts must be > 0")
    if args.progress_every <= 0:
        raise SystemExit("--progress-every must be > 0")
    if args.stringification_level is not None and args.stringification_preset is not None:
        raise SystemExit("--stringification-level and --stringification-preset are mutually exclusive")
    if args.engine == "duckdb" and not args.duckdb_path:
        raise SystemExit("--duckdb-path is required when --engine=duckdb")

    template_dir = Path(args.template_dir)
    if not template_dir.is_absolute():
        template_dir = (REPO_ROOT / template_dir).resolve()
    template_input = Path(args.template_input)
    if not template_input.is_absolute():
        template_input = (REPO_ROOT / template_input).resolve()
    if not template_dir.exists():
        raise SystemExit(f"Template directory not found: {template_dir}")
    if not template_input.exists():
        raise SystemExit(f"Template input list not found: {template_input}")
    duckdb_path: Path | None = None
    if args.engine == "duckdb":
        duckdb_path = Path(args.duckdb_path).resolve()  # type: ignore[arg-type]
        if not duckdb_path.exists():
            raise SystemExit(f"DuckDB database not found: {duckdb_path}")

    dsqgen_bin = wrap_dsqgen._resolve_dsqgen_binary()

    resolved_level, resolved_preset = stringification_cfg.resolve_level(
        args.stringification_level, args.stringification_preset
    )

    if args.queries:
        query_numbers = _parse_queries(args.queries)
    elif args.audit_json:
        audit_path = Path(args.audit_json)
        if not audit_path.is_absolute():
            audit_path = (REPO_ROOT / audit_path).resolve()
        if not audit_path.exists():
            raise SystemExit(f"Audit JSON not found: {audit_path}")
        query_numbers = _parse_zero_queries_from_audit(audit_path)
        print(
            f"[seed-search] loaded {len(query_numbers)} zero-row queries from {audit_path}",
            flush=True,
        )
    else:
        query_numbers = list(DEFAULT_TARGET_QUERIES)

    if not query_numbers:
        raise SystemExit("No query numbers provided")

    if args.overrides_file:
        overrides_path = Path(args.overrides_file)
        if not overrides_path.is_absolute():
            overrides_path = (REPO_ROOT / overrides_path).resolve()
    else:
        overrides_path = wrap_dsqgen._resolve_seed_overrides_path(
            scale=str(args.scale),
            stringification_level=resolved_level,
        ).resolve()
    stringify_cfg = wrap_dsqgen.dsdgen_config.stringify_rules()
    base_pad_width = int(stringify_cfg.get("pad_width", 8))
    schema_config = stringification_cfg.build_stringification_config(
        level=resolved_level,
        preset=resolved_preset,
        base_pad_width=base_pad_width,
    )

    enabled_ext_templates = _resolve_enabled_ext_templates(
        template_input=template_input,
        template_dir=template_dir,
        stringification_level=resolved_level,
        stringification_preset=resolved_preset,
    )

    results: dict[str, dict[str, int | str]] = {}
    unresolved: dict[str, str] = {}

    for qnum in query_numbers:
        query_filename = f"query_{qnum}.sql"
        template_name = wrap_dsqgen._resolve_query_template_name(
            qnum=qnum,
            template_dir=template_dir,
            enabled_ext_templates=enabled_ext_templates,
        )
        template_candidates = [template_name]
        base_template = f"query{qnum}.tpl"
        if (
            args.allow_base_fallback
            and template_name != base_template
            and (template_dir / base_template).exists()
        ):
            template_candidates.append(base_template)
        print(
            f"[seed-search] {query_filename}: templates={','.join(template_candidates)}",
            flush=True,
        )
        found_seed: int | None = None
        found_template: str | None = None
        last_error = ""
        for candidate_template in template_candidates:
            timeout_failures = 0
            empty_failures = 0
            for attempt in range(args.max_attempts):
                seed = args.seed_start + attempt
                try:
                    sql = wrap_dsqgen._generate_single_query_sql(
                        dsqgen_bin=dsqgen_bin,
                        template_dir=template_dir,
                        template_name=candidate_template,
                        dialect=args.dialect,
                        scale=str(args.scale),
                        rng_seed=seed,
                    )
                    if args.dialect.lower() == "duckdb":
                        sql = wrap_dsqgen._rewrite_duckdb_sql(query_filename, sql)
                    else:
                        sql = wrap_dsqgen._rewrite_postgres_sql(query_filename, sql)
                    if schema_config.schema_selected:
                        sql = wrap_dsqgen._rewrite_stringified_literals_sql(sql, config=schema_config)
                    if args.strip_outer_order_limit:
                        sql, _ = _strip_outer_order_limit(sql)
                except Exception as exc:
                    last_error = str(exc)
                    timeout_failures += 1
                    if timeout_failures >= args.max_timeout_attempts:
                        break
                    continue

                if args.engine == "duckdb":
                    ok, nonempty, err = _probe_nonempty_duckdb(
                        db_path=duckdb_path,  # type: ignore[arg-type]
                        duckdb_bin=args.duckdb_bin,
                        sql=sql,
                        timeout_ms=args.timeout_ms,
                        session_settings=args.session_setting,
                    )
                else:
                    ok, nonempty, err = _probe_nonempty(
                        db=args.db,
                        pg_user=args.pg_user,
                        sql=sql,
                        timeout_ms=args.timeout_ms,
                        session_settings=args.session_setting,
                    )
                if not ok:
                    last_error = err
                    err_lower = err.lower()
                    if "timeout" in err_lower:
                        timeout_failures += 1
                        if timeout_failures >= args.max_timeout_attempts:
                            break
                    continue
                timeout_failures = 0
                if nonempty:
                    found_seed = seed
                    found_template = candidate_template
                    print(
                        (
                            f"[seed-search] {query_filename}: "
                            f"seed={seed} template={candidate_template} -> non-empty"
                        ),
                        flush=True,
                    )
                    break
                empty_failures += 1
                if empty_failures >= args.max_empty_attempts:
                    break
                if (attempt + 1) % args.progress_every == 0:
                    print(
                        (
                            f"[seed-search] {query_filename}: "
                            f"attempt={attempt + 1} template={candidate_template} "
                            f"empty={empty_failures} timeouts={timeout_failures}"
                        ),
                        flush=True,
                    )
            if found_seed is not None:
                break

        if found_seed is None:
            reason = last_error or "no_nonempty_seed_within_budget"
            unresolved[query_filename] = reason
            print(f"[seed-search] {query_filename}: unresolved ({reason})", flush=True)
            continue

        results[query_filename] = {
            "seed": found_seed,
            "template": found_template or template_name,
        }

    payload = {
        "meta": {
            "scale": str(args.scale),
            "stringification_level": int(resolved_level),
            "dialect": str(args.dialect),
            "seed_start": int(args.seed_start),
            "max_attempts": int(args.max_attempts),
            "timeout_ms": int(args.timeout_ms),
        },
        "queries": results,
    }
    if unresolved:
        payload["unresolved"] = unresolved

    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    overrides_path.write_text(yaml.safe_dump(payload, sort_keys=True), encoding="utf-8")
    print(f"[seed-search] wrote {overrides_path}")
    print(
        f"[seed-search] resolved={len(results)} unresolved={len(unresolved)} "
        f"queries={','.join(f'query_{q}.sql' for q in query_numbers)}"
    )

    return 0 if not unresolved else 2


if __name__ == "__main__":
    raise SystemExit(main())

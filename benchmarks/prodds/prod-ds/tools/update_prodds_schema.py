#!/usr/bin/env python3
"""
Normalize tools/prodds.sql so it keeps all base columns and only recasts key-like
columns (suffix _sk/_id) to varchar. Non-key columns revert to the base schema type.

This keeps column names intact while ensuring stringification only targets keys.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE = REPO_ROOT / "tools" / "tpcds.sql"
DEFAULT_PROD = REPO_ROOT / "tools" / "prodds.sql"

CREATE_TABLE_RE = re.compile(r"\s*create\s+table\s+([A-Za-z0-9_]+)", re.IGNORECASE)
COLUMN_DEF_RE = re.compile(r"(\s*)([A-Za-z0-9_]+)(\s+)([A-Za-z0-9_]+(?:\([^)]*\))?)(.*)")
NUMERIC_PREFIXES = ("int", "integer", "bigint", "smallint", "decimal", "number", "numeric")
CHAR_PREFIXES = ("char", "varchar", "text")
KEY_SUFFIXES = ("_sk", "_id")


def _is_numeric_type(token: str) -> bool:
    lowered = token.strip().lower()
    return lowered.startswith(NUMERIC_PREFIXES)


def _is_char_type(token: str) -> bool:
    lowered = token.strip().lower()
    return lowered.startswith(CHAR_PREFIXES)


def _parse_schema(path: Path) -> Dict[str, Dict[str, str]]:
    tables: Dict[str, Dict[str, str]] = {}
    current_table: str | None = None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("--"):
            continue

        match = CREATE_TABLE_RE.match(line)
        if match:
            current_table = match.group(1).lower()
            tables[current_table] = {}
            continue

        if current_table and line.startswith(")"):
            current_table = None
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
        data_type = col_match.group(4).strip()
        tables[current_table][col_name] = data_type

    return tables


def _desired_type(col_name: str, base_type: str, key_varchar_len: int) -> str:
    lowered_col = col_name.lower()
    if lowered_col.endswith(KEY_SUFFIXES):
        if _is_char_type(base_type):
            return base_type
        if _is_numeric_type(base_type):
            return f"varchar({key_varchar_len})"
    return base_type


def _rewrite_schema(
    base_lines: List[str],
    base_types: Dict[str, Dict[str, str]],
    *,
    key_varchar_len: int,
) -> Tuple[List[str], List[str], List[Tuple[str, str, str, str]], List[str]]:
    output: List[str] = []
    current_table: str | None = None
    changed: List[Tuple[str, str, str, str]] = []
    extra_cols: List[str] = []

    for line in base_lines:
        create_match = CREATE_TABLE_RE.match(line)
        if create_match:
            current_table = create_match.group(1).lower()
            output.append(line)
            continue

        if current_table and line.lstrip().startswith(")"):
            current_table = None
            output.append(line)
            continue

        if not current_table:
            output.append(line)
            continue

        lowered = line.strip().lower()
        if lowered.startswith(("primary key", "unique", "constraint", "foreign key")):
            output.append(line)
            continue

        col_match = COLUMN_DEF_RE.match(line)
        if not col_match:
            output.append(line)
            continue

        indent, col_name, spacing, data_type, rest = col_match.groups()
        line_end = "\n" if line.endswith("\n") else ""
        base_table = base_types.get(current_table, {})
        base_type = base_table.get(col_name.lower())
        if base_type is None:
            extra_cols.append(f"{current_table}.{col_name}")
            output.append(line)
            continue

        target_type = _desired_type(col_name, base_type, key_varchar_len)
        if data_type != target_type:
            changed.append((current_table, col_name, data_type, target_type))
            output.append(f"{indent}{col_name}{spacing}{target_type}{rest}{line_end}")
        else:
            output.append(line)

    missing_cols: List[str] = []
    for table, cols in base_types.items():
        for col in cols:
            if table not in base_types:
                continue
            # We only know prod lines when rewritten; rely on base/prod table presence.
            # Missing columns are detected by absence in prod tables, which we check elsewhere.
            pass

    return output, missing_cols, changed, extra_cols


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Normalize prodds.sql key types.")
    parser.add_argument("--base-schema", type=Path, default=DEFAULT_BASE)
    parser.add_argument("--prod-schema", type=Path, default=DEFAULT_PROD)
    parser.add_argument("--key-varchar-len", type=int, default=32)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    base_schema = args.base_schema.expanduser().resolve()
    prod_schema = args.prod_schema.expanduser().resolve()

    if not base_schema.exists():
        print(f"Base schema not found: {base_schema}", file=sys.stderr)
        return 2
    if not prod_schema.exists():
        print(f"Prod schema not found: {prod_schema}", file=sys.stderr)
        return 2

    base_types = _parse_schema(base_schema)
    base_lines = base_schema.read_text(encoding="utf-8").splitlines(keepends=True)
    rewritten, _, changed, extra_cols = _rewrite_schema(
        base_lines,
        base_types,
        key_varchar_len=args.key_varchar_len,
    )

    print(f"[prodds] Planned changes: {len(changed)} columns")
    if extra_cols:
        print(f"[prodds] Extra columns not in base schema: {len(extra_cols)}")

    if args.dry_run:
        for table, col, old, new in changed[:50]:
            print(f"  {table}.{col}: {old} -> {new}")
        if len(changed) > 50:
            print(f"  ... {len(changed) - 50} more")
        return 0

    prod_schema.write_text("".join(rewritten), encoding="utf-8")
    for table, col, old, new in changed[:50]:
        print(f"  {table}.{col}: {old} -> {new}")
    if len(changed) > 50:
        print(f"  ... {len(changed) - 50} more")
    print(f"[prodds] Updated {prod_schema}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

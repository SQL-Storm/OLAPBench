#!/usr/bin/env python3
"""
Derive a Prod-DS dbschema.json from the canonical TPC-DS dbschema by applying
stringification recasts at a chosen STR level.

Must run inside the prod-ds Python venv (uses prod-ds workload.stringification).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BASE = REPO_ROOT / "benchmarks" / "tpcds" / "tpcds.dbschema.json"
PROD_DS_DIR = REPO_ROOT / "benchmarks" / "prodds" / "prod-ds"

NOT_NULL_RE = re.compile(r"\bnot\s+null\b", re.IGNORECASE)


def _ensure_prod_ds_importable() -> None:
    if str(PROD_DS_DIR) not in sys.path:
        sys.path.insert(0, str(PROD_DS_DIR))


def _rewrite_column(column: dict, new_base_type: str) -> None:
    original = column["type"]
    if NOT_NULL_RE.search(original):
        column["type"] = f"{new_base_type} not null"
    else:
        column["type"] = new_base_type


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--level", type=int, required=True, help="Stringification level (1-15+)")
    parser.add_argument("--out", type=Path, required=True, help="Output dbschema.json path")
    parser.add_argument("--base", type=Path, default=DEFAULT_BASE, help="Base TPC-DS dbschema.json")
    parser.add_argument("--str-plus-max-level", type=int, default=20)
    args = parser.parse_args()

    _ensure_prod_ds_importable()
    from workload import stringification  # type: ignore

    cfg = stringification.build_stringification_config(
        level=args.level,
        preset=None,
        prod_schema_path=stringification.DEFAULT_PROD_SCHEMA,
        str_plus_max_level=args.str_plus_max_level,
    )

    schema = json.loads(args.base.read_text(encoding="utf-8"))

    selected = set(cfg.schema_selected)
    rewritten = 0
    for table in schema["tables"]:
        table_name = table["name"].lower()
        for column in table["columns"]:
            key = f"{table_name}.{column['name'].lower()}"
            if key in selected:
                _rewrite_column(column, cfg.schema_type_map[key])
                rewritten += 1

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(schema, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.out} (STR={args.level}, recast {rewritten} columns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

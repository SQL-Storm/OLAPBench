#!/usr/bin/env python3
"""CLI entrypoint for ProdDS-Kit utilities."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from workload import stringification as stringification_cfg
from workload.dsqgen import template_resolver

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_TEMPLATE_DIR = REPO_ROOT / "query_templates"
DEFAULT_TEMPLATE_LIST = DEFAULT_TEMPLATE_DIR / "templates.lst"


def _load_manifest(path: Path) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def explain_stringification(args: argparse.Namespace) -> int:
    if args.stringification_level is not None and args.stringification_preset is not None:
        raise SystemExit("--stringification-level and --stringification-preset are mutually exclusive.")
    resolved_level, resolved_preset = stringification_cfg.resolve_level(
        args.stringification_level, args.stringification_preset
    )
    intensity = stringification_cfg.intensity_from_level(resolved_level)

    manifest_dir = Path(args.manifest_dir).resolve()
    schema_manifest = _load_manifest(manifest_dir / stringification_cfg.SCHEMA_MANIFEST_NAME)
    data_manifest = _load_manifest(manifest_dir / stringification_cfg.DATA_MANIFEST_NAME)
    query_manifest = _load_manifest(manifest_dir / stringification_cfg.QUERY_MANIFEST_NAME)

    if schema_manifest:
        recast_columns = list(schema_manifest.get("recast_columns", []))
        schema_count = len(recast_columns)
        schema_total = int(schema_manifest.get("K_schema_max", schema_count))
    else:
        config = stringification_cfg.build_stringification_config(level=resolved_level, preset=resolved_preset)
        recast_columns = list(config.schema_selected)
        schema_count = len(recast_columns)
        schema_total = config.K_schema_max

    if query_manifest:
        query_count = int(query_manifest.get("k_query", 0))
        query_total = int(query_manifest.get("K_query_max", query_count))
        enabled_edits = query_manifest.get("enabled_edits", [])
    else:
        template_dir = Path(args.template_dir)
        template_list = Path(args.template_input)
        names = template_resolver.read_templates(template_list)
        selection = stringification_cfg.select_query_edits(
            names,
            template_dir,
            level=resolved_level,
            preset=resolved_preset,
        )
        query_count = selection.k_query
        query_total = selection.K_query_max
        enabled_edits = [
            {
                "query_id": edit.query_id,
                "edit_id": edit.edit_id,
                "template": edit.ext_template,
            }
            for edit in selection.selected
        ]

    if data_manifest:
        payload = data_manifest.get("payload", {})
        touched_columns = data_manifest.get("touched_columns", [])
        rows_rewritten = data_manifest.get("rows_rewritten", 0)
        length_summary = data_manifest.get("length_summary", {})
    else:
        payload = {}
        touched_columns = []
        rows_rewritten = 0
        length_summary = {}

    preset_display = resolved_preset or "custom"
    print(f"Stringification level {resolved_level} (preset={preset_display})")
    print(f"Intensity p={intensity:.3f}")

    print(f"Schema: {schema_count}/{schema_total} recast columns")
    if recast_columns:
        print("  " + ", ".join(recast_columns))
    else:
        print("  (none)")

    regime = payload.get("regime") if isinstance(payload, Mapping) else None
    print("Data:")
    if regime:
        print(f"  payload: {regime}")
    print(f"  touched columns: {len(touched_columns)}")
    if touched_columns:
        print("  " + ", ".join(touched_columns))
    if rows_rewritten:
        print(f"  rows rewritten: {rows_rewritten}")
    if length_summary:
        print("  length summary:")
        for key in sorted(length_summary):
            stats = length_summary[key]
            if not isinstance(stats, Mapping):
                continue
            print(
                "    "
                + f"{key}: min={stats.get('min')} median={stats.get('median')} "
                + f"p95={stats.get('p95')} max={stats.get('max')}"
            )

    print(f"Query: {query_count}/{query_total} templates modified")
    if enabled_edits:
        for edit in enabled_edits:
            query_id = edit.get("query_id")
            edit_id = edit.get("edit_id")
            template = edit.get("template")
            print(f"  {query_id}: {edit_id} ({template})")
    else:
        print("  (none)")

    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="ProdDS-Kit utilities.")
    subparsers = ap.add_subparsers(dest="command", required=True)

    explain = subparsers.add_parser("explain-stringification", help="Explain stringification settings")
    explain.add_argument(
        "--stringification-level",
        "--level",
        dest="stringification_level",
        type=int,
        choices=range(1, 11),
        help="Stringification level (1-10).",
    )
    explain.add_argument(
        "--stringification-preset",
        "--preset",
        dest="stringification_preset",
        type=str,
        choices=sorted(stringification_cfg.PRESET_LEVELS.keys()),
        help="Stringification preset.",
    )
    explain.add_argument(
        "--manifest-dir",
        default=".",
        help="Directory containing stringification manifest files (default: .).",
    )
    explain.add_argument(
        "--template-dir",
        default=str(DEFAULT_TEMPLATE_DIR),
        help="Template directory for query selection (default: query_templates).",
    )
    explain.add_argument(
        "--template-input",
        default=str(DEFAULT_TEMPLATE_LIST),
        help="Template list file (default: query_templates/templates.lst).",
    )
    explain.set_defaults(func=explain_stringification)

    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

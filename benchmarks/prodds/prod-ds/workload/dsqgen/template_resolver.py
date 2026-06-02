#!/usr/bin/env python3
"""
Resolve dsqgen template names, optionally swapping in _ext.tpl variants when available.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List


DEFAULT_TEMPLATE_DIR = Path("query_templates")
DEFAULT_INPUT = DEFAULT_TEMPLATE_DIR / "templates.lst"


def read_templates(list_path: Path) -> List[str]:
    lines = []
    for raw in list_path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(stripped)
    return lines


def _with_ext_name(name: str) -> str:
    if name.endswith(".tpl"):
        return f"{name[:-4]}_ext.tpl"
    return f"{name}_ext.tpl"


def resolve_templates(
    template_names: Iterable[str],
    template_dir: Path,
    use_extensions: bool,
    *,
    allowed_ext: set[str] | None = None,
) -> List[str]:
    resolved: List[str] = []
    for name in template_names:
        candidate = _with_ext_name(name)
        if use_extensions and (template_dir / candidate).exists():
            if allowed_ext is not None and candidate not in allowed_ext:
                resolved.append(name)
                continue
            resolved.append(candidate)
        else:
            resolved.append(name)
    return resolved


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Resolve dsqgen template list, optionally preferring *_ext.tpl variants when present."
    )
    ap.add_argument(
        "--input",
        default=str(DEFAULT_INPUT),
        help="Path to templates.lst (default: query_templates/templates.lst)",
    )
    ap.add_argument(
        "--directory",
        default=str(DEFAULT_TEMPLATE_DIR),
        help="Template directory dsqgen reads from (default: query_templates)",
    )
    ap.add_argument(
        "--output",
        default=None,
        help="Where to write the resolved list (default: templates_ext.lst when extensions are enabled, "
        "otherwise templates_resolved.lst next to the input list).",
    )
    ap.add_argument(
        "--use-extended-queries",
        "--enable-string-agg-extensions",
        dest="use_extensions",
        action="store_true",
        help="Prefer *_ext.tpl when available; fall back to the base templates otherwise.",
    )
    args = ap.parse_args(argv)

    template_dir = Path(args.directory)
    input_path = Path(args.input)
    if args.output:
        output_path = Path(args.output)
    else:
        default_name = "templates_ext.lst" if args.use_extensions else "templates_resolved.lst"
        output_path = input_path.with_name(default_name)

    names = read_templates(input_path)
    resolved = resolve_templates(names, template_dir, args.use_extensions)
    output_path.write_text("\n".join(resolved) + "\n", encoding="utf-8")

    print(f"Wrote {len(resolved)} entries to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

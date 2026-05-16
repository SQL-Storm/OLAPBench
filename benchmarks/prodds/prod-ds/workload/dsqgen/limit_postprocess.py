#!/usr/bin/env python3
"""
Post-process dsqgen SQL to fix LIMIT values per template without modifying dsqgen.

Uses the template name embedded in the query header:
  -- start query N in stream 0 using template queryXX_ext.tpl
and rewrites TOP/LIMIT/FETCH FIRST/ROWNUM to match the _LIMIT defined in that template.
If the template does not use LIMIT macros, any LIMIT syntax is removed.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


TEMPLATE_HEADER_RE = re.compile(r"using template\s+(\S+)", re.IGNORECASE)
LIMIT_DEF_RE = re.compile(r"^\s*define\s+_LIMIT\s*=\s*([0-9]+)\s*;", re.MULTILINE | re.IGNORECASE)
LIMIT_MACRO_RE = re.compile(r"\[_LIMIT[ABC]\]", re.IGNORECASE)

TOP_RE = re.compile(r"\btop\s+\d+\b", re.IGNORECASE)
SELECT_TOP_RE = re.compile(r"\bselect\s+top\s+\d+\s+", re.IGNORECASE)
LIMIT_RE = re.compile(r"\blimit\s+\d+\b", re.IGNORECASE)
FETCH_RE = re.compile(r"\bfetch\s+first\s+\d+\s+rows\s+only\b", re.IGNORECASE)
ROWNUM_RE = re.compile(r"\brownum\s*<=\s*\d+\b", re.IGNORECASE)
KEEP_LIMIT_TOKEN = "KEEP_LIMIT"


def _has_keep_marker(text: str, start: int) -> bool:
    window = text[max(0, start - 40) : start]
    return KEEP_LIMIT_TOKEN in window


def _replace_pattern(text: str, pattern: re.Pattern[str], repl: str) -> tuple[str, int]:
    out = []
    last = 0
    replaced = 0
    for m in pattern.finditer(text):
        out.append(text[last : m.start()])
        if _has_keep_marker(text, m.start()):
            out.append(m.group(0))
        else:
            out.append(repl)
            replaced += 1
        last = m.end()
    out.append(text[last:])
    return "".join(out), replaced


def _load_template_limits(template_dir: Path) -> dict[str, tuple[bool, int | None]]:
    mapping: dict[str, tuple[bool, int | None]] = {}
    for tpl in template_dir.glob("query*_ext.tpl"):
        text = tpl.read_text(encoding="utf-8")
        has_macro = bool(LIMIT_MACRO_RE.search(text))
        m = LIMIT_DEF_RE.search(text)
        limit_val = int(m.group(1)) if m else None
        mapping[tpl.name] = (has_macro, limit_val)
    return mapping


def _extract_template_name(text: str) -> str | None:
    for line in text.splitlines()[:5]:
        m = TEMPLATE_HEADER_RE.search(line)
        if m:
            return m.group(1)
    return None


def _rewrite_limit(text: str, desired: int | None) -> tuple[str, int]:
    replaced = 0
    if desired is None:
        # Remove limits unless explicitly marked to keep.
        new_text, n = _replace_pattern(text, SELECT_TOP_RE, "select ")
        replaced += n
        text = new_text
        new_text, n = _replace_pattern(text, LIMIT_RE, "")
        replaced += n
        text = new_text
        new_text, n = _replace_pattern(text, FETCH_RE, "")
        replaced += n
        text = new_text
        new_text, n = _replace_pattern(text, ROWNUM_RE, "1=1")
        replaced += n
        text = new_text
        return text, replaced

    for pattern, repl in (
        (TOP_RE, f"top {desired}"),
        (LIMIT_RE, f"limit {desired}"),
        (FETCH_RE, f"fetch first {desired} rows only"),
        (ROWNUM_RE, f"rownum <= {desired}"),
    ):
        new_text, n = _replace_pattern(text, pattern, repl)
        replaced += n
        text = new_text

    # If TOP is used as "select top N", ensure replacement happened
    if replaced == 0:
        new_text, n = _replace_pattern(text, SELECT_TOP_RE, f"select top {desired} ")
        replaced += n
        text = new_text

    return text, replaced


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Post-process dsqgen SQL to fix LIMIT values per template.")
    ap.add_argument("output_dir", help="Directory with query_*.sql files.")
    ap.add_argument(
        "--template-dir",
        default="query_templates",
        help="Template directory (default: query_templates).",
    )
    ap.add_argument(
        "--scale",
        type=int,
        default=1,
        help="Scale factor to multiply limits by (default: 1).",
    )
    args = ap.parse_args(argv)

    output_dir = Path(args.output_dir)
    template_dir = Path(args.template_dir)
    mapping = _load_template_limits(template_dir)

    touched = 0
    missing_template = 0
    missing_rewrite = 0

    for sql_path in sorted(output_dir.glob("query_*.sql")):
        text = sql_path.read_text(encoding="utf-8")
        tpl = _extract_template_name(text)
        if not tpl:
            continue
        entry = mapping.get(tpl)
        if entry is None:
            missing_template += 1
            continue
        has_macro, limit_val = entry
        desired = limit_val if has_macro else None
        if desired is not None:
             desired *= args.scale

        new_text, replaced = _rewrite_limit(text, desired)
        if replaced == 0 and desired is not None:
            missing_rewrite += 1
        if new_text != text:
            sql_path.write_text(new_text, encoding="utf-8")
            touched += 1

    msg = f"post-processing: Updated {touched} limits."
    if missing_template or missing_rewrite:
        msg += f" (unknown templates: {missing_template}, no limit syntax: {missing_rewrite})"
    print(msg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

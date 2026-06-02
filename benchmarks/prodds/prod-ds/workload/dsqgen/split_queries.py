#!/usr/bin/env python3
"""
Utility to split a dsqgen output file (with multiple queries separated by start/end markers)
into individual query_N.sql files. This is helpful for downstream tools that expect one query
per file.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional, Tuple


def _parse_start(line: str) -> Optional[Tuple[int, str]]:
    """
    Parse a start marker line of the form:
      -- start query 3 in stream 0 using template query75_ext.tpl
    Returns (query_number, template_name) or None if not a start marker.
    """
    prefix = "-- start query"
    if not line.lower().startswith(prefix):
        return None
    parts = line.strip("- \n").split()
    # expected: ["start","query","<n>","in","stream",...,"template","<tpl>"]
    try:
        qnum = int(parts[2])
    except (IndexError, ValueError):
        return None
    template_name = parts[-1] if parts else ""
    return qnum, template_name


def _is_end(line: str) -> bool:
    return line.lower().startswith("-- end query")


def split_queries(input_path: Path, output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    lines = input_path.read_text(encoding="utf-8").splitlines()

    current: list[str] = []
    current_qnum: Optional[int] = None
    written = 0

    for line in lines:
        start = _parse_start(line)
        if start:
            # write any dangling block (should not happen if markers are well formed)
            if current and current_qnum is not None:
                outfile = output_dir / f"query_{current_qnum}.sql"
                outfile.write_text("\n".join(current) + "\n", encoding="utf-8")
                written += 1
            current = [line]
            current_qnum, _ = start
            continue

        if _is_end(line):
            if current_qnum is None:
                continue
            current.append(line)
            outfile = output_dir / f"query_{current_qnum}.sql"
            outfile.write_text("\n".join(current) + "\n", encoding="utf-8")
            written += 1
            current = []
            current_qnum = None
            continue

        if current_qnum is not None:
            current.append(line)

    return written


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Split a dsqgen output file containing multiple queries into individual files."
    )
    ap.add_argument("input", help="Path to dsqgen output SQL file (e.g., query_0.sql)")
    ap.add_argument(
        "-o",
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Directory to write individual query files (default: alongside input in a 'split' subdir).",
    )
    args = ap.parse_args(argv)

    input_path = Path(args.input)
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = input_path.parent / "split"

    written = split_queries(input_path, output_dir)
    print(f"Wrote {written} queries to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Run Postgres queries from a directory and log results in blocks.

Example:
  python3 experiments/scripts/query_check_postgres.py \
    --query-dir /path/to/sql \
    --db prodds \
    --block-size 3 \
    --timeout 5min \
    --notes-file /path/to/notes.md
"""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from typing import Iterable, List


def iter_sql_files(query_dir: Path) -> List[Path]:
    files = sorted(query_dir.glob("*.sql"))
    return files


def strip_comments(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def truncate_first_statement(sql: str) -> str:
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


def run_query(db: str, sql: str, timeout: str) -> tuple[bool, str]:
    cmd = [
        "psql",
        "-At",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        db,
        "-c",
        f"SET statement_timeout='{timeout}'; SELECT COUNT(*) FROM ({sql}) AS _q;",
    ]
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode == 0:
        output = result.stdout.strip().splitlines()
        return True, (output[-1] if output else "")
    msg = (result.stderr or result.stdout or "").strip().splitlines()
    return False, (msg[-1] if msg else "unknown_error")


def write_block_header(notes: Path, title: str) -> None:
    with notes.open("a", encoding="utf-8") as handle:
        handle.write(f"\n### {title}\n\n")


def write_block_line(notes: Path, line: str) -> None:
    with notes.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description="Run Postgres queries in blocks with progress output.")
    ap.add_argument("--query-dir", required=True, help="Directory containing query_*.sql files")
    ap.add_argument("--db", default="prodds", help="Database name")
    ap.add_argument("--block-size", type=int, default=3)
    ap.add_argument("--timeout", default="5min", help="Statement timeout (psql format)")
    ap.add_argument("--notes-file", required=True, help="Markdown notes file to append")
    ap.add_argument(
        "--start-after",
        default="",
        help="Resume after a specific filename (e.g., query_11.sql).",
    )
    ap.add_argument(
        "--max-blocks",
        type=int,
        default=0,
        help="Stop after N blocks (0 means no limit).",
    )
    args = ap.parse_args()

    query_dir = Path(args.query_dir)
    notes_path = Path(args.notes_file)
    files = iter_sql_files(query_dir)
    if not files:
        print(f"[query-check] No .sql files found in {query_dir}")
        return 1

    total = len(files)
    if args.start_after:
        try:
            start_idx = next(i for i, path in enumerate(files) if path.name == args.start_after) + 1
            files = files[start_idx:]
        except StopIteration:
            print(f"[query-check] start-after file not found: {args.start_after}")
            return 1
        if not files:
            print("[query-check] No queries left after start-after.")
            return 0
    block_size = max(1, args.block_size)
    max_blocks = args.max_blocks if args.max_blocks and args.max_blocks > 0 else None
    blocks_run = 0
    total = len(files)
    for block_start in range(0, total, block_size):
        block = files[block_start:block_start + block_size]
        title = f"Block {block_start // block_size + 1} ({block[0].name} .. {block[-1].name})"
        print(f"[query-check] {title}")
        write_block_header(notes_path, title)

        for path in block:
            raw = path.read_text(encoding="utf-8")
            sql = truncate_first_statement(strip_comments(raw))
            if not sql:
                msg = f"- {path.name}: skipped (empty)"
                print(f"[query-check] {msg}")
                write_block_line(notes_path, msg)
                continue
            ok, out = run_query(args.db, sql, args.timeout)
            if ok:
                msg = f"- {path.name}: success, rows={out}"
            else:
                msg = f"- {path.name}: ERROR, {out}"
            print(f"[query-check] {msg}")
            write_block_line(notes_path, msg)
        blocks_run += 1
        if max_blocks is not None and blocks_run >= max_blocks:
            print("[query-check] Reached max blocks; stopping early.")
            break

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

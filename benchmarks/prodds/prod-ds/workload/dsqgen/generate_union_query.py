#!/usr/bin/env python3
"""
Generate a UNION ALL fan-in template with a configurable number of inputs.
"""

from __future__ import annotations

import argparse


def _branch(month: int, idx: int) -> str:
    return (
        "  SELECT ss_item_sk,\n"
        "         ss_ext_sales_price,\n"
        "         d_year,\n"
        "         d_moy,\n"
        "         s_state,\n"
        f"         {idx} AS union_branch\n"
        "  FROM base\n"
        f"  WHERE d_moy = {month}\n"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Generate a UNION ALL fan-in query template.")
    ap.add_argument("--inputs", type=int, required=True, help="Number of UNION ALL inputs.")
    args = ap.parse_args(argv)

    inputs = max(2, int(args.inputs))
    branches = []
    for i in range(1, inputs + 1):
        month = ((i - 1) % 12) + 1
        branches.append(f"u{i:04d} AS (\n{_branch(month, i)}  )")

    union_selects = []
    for i in range(1, inputs + 1):
        union_selects.append(f"SELECT * FROM u{i:04d}")

    sql = (
        f"-- AUTO-GENERATED UNION ALL fan-in template (inputs={inputs})\n"
        "define YEAR = random(1998,2002,uniform);\n"
        "WITH base AS (\n"
        "  SELECT ss_item_sk,\n"
        "         ss_ext_sales_price,\n"
        "         d_year,\n"
        "         d_moy,\n"
        "         s_state\n"
        "  FROM store_sales, date_dim, store\n"
        "  WHERE ss_sold_date_sk = d_date_sk\n"
        "    AND ss_store_sk = s_store_sk\n"
        "    AND d_year = [YEAR]\n"
        ")\n"
        ",\n"
        + ",\n".join(branches)
        + "\n"
        + "\n"
        + "\nUNION ALL\n".join(union_selects)
        + ";\n"
    )

    print(sql)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

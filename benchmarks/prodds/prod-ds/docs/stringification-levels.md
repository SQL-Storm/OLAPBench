# Appendix B: Stringification Level Details

This appendix describes the stringification level parameter `STR` and its effect
on schema recasts, data post-processing, and query-template extensions.

## Overview

The stringification level `STR` is a single integer in `[1, 15]` that jointly
controls three subsystems:

1. **Schema recasts** -- the number of columns converted from numeric types to
   `varchar`.
2. **Data post-processing** -- the zero-padding width applied to converted values.
3. **Query-template extensions** -- the number of base query templates replaced by
   string-extended variants (`_ext.tpl`).

`STR = 1` corresponds to unmodified TPC-DS types. `STR = 10` is the default
configuration used in the paper evaluation.

## Intensity Formula

All three subsystems scale with a common intensity value:

```
i = (STR - 1) / 9,  clamped to [0, 1]
```

At each level:

- Columns recast to `varchar`: `ceil(i * 131)` out of 131 candidates across 24
  tables.
- Zero-padding width: `ceil(4 + 4 * i)` digits.
- Query templates activated: `ceil(i * 92)` out of 92 eligible templates.

## STR+ Mode (STR > 10)

For `STR > 10`, the column set and query templates remain at their `STR = 10`
values (all 131 columns recast, all 92 templates active). Only the per-value
string length increases: `2 * (STR - 10)` filler characters are appended after a
separator. This isolates the effect of string payload size from type coverage.

The range is capped at 15 to keep the configuration space practical while covering
both type recasting (levels 1--10) and payload scaling (levels 11--15).

## Level Breakdown Table

The table below shows the concrete schema, data, and query changes at each level.
**Cols** is the cumulative count of columns recast (out of 131 candidates).
**Pad** is the zero-padding width; "+n" denotes STR+ filler appended after a
separator. **Sample value** shows the stringified representation for an original
integer value of 42. **Query edits** is the number of base templates replaced by
string-extended variants (out of 92 eligible).

| STR | Cols (/131) | Pad     | Sample value              | Query edits (/92) |
|----:|------------:|--------:|---------------------------|------------------:|
|   1 |           0 | --      | *(unchanged)*             |                 0 |
|   2 |          15 | 4       | `c0042`                   |                10 |
|   3 |          29 | 5       | `c00042`                  |                20 |
|   4 |          44 | 5       | `c00042`                  |                31 |
|   5 |          58 | 6       | `c000042`                 |                41 |
|   6 |          73 | 6       | `c000042`                 |                51 |
|   7 |          87 | 7       | `c0000042`                |                61 |
|   8 |         102 | 7       | `c0000042`                |                72 |
|   9 |         116 | 8       | `c00000042`               |                82 |
|  10 |         131 | 8       | `c00000042`               |                92 |
|  11 |         131 | 8+2     | `..42_XX`                 |                92 |
|  12 |         131 | 8+4     | `..42_XXXX`               |                92 |
|  13 |         131 | 8+6     | `..42_XXXXXX`             |                92 |
|  14 |         131 | 8+8     | `..42_XXXXXXXX`           |                92 |
|  15 |         131 | 8+10    | `..42_XXXXXXXXXX`         |                92 |

## Implementation Reference

The stringification logic is implemented in:

- **`workload/dsdgen/stringify.py`** -- the main post-processing module that
  applies column recasts, zero-padding, and (at STR+ levels) filler-character
  appending to generated `.tbl`/`.dat` files.

The module reads the column selection order from a deterministic, curated list.
Custom prefixes (e.g., `BRAND_`, `TKT_`, `ORD_`) are applied to specific columns
to produce realistic identifier strings. Domain-suffix prefixes ensure that
PK/FK columns sharing the same domain stringify to the same textual key space
across tables, preserving join semantics.

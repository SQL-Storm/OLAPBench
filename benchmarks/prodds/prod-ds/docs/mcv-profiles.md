# Appendix E: MCV Skew Profiles

This appendix details the Most-Common-Value (MCV) injection mechanism used by
Prod-DS to model value-skew patterns observed in production datasets.

## Overview

Production datasets often exhibit strong MCV dominance: for many categorical
attributes, a single value accounts for the majority of rows. In production
workloads, the top 20% of columns by MCV share exceed 60% single-value
concentration, and the top 15% approach full dominance. TPC-DS underrepresents
this effect. Prod-DS injects configurable skew via MCV profiles to reproduce
the heavy-tailed MCV behavior observed in real schemas.

## Profile Tuple

An MCV profile is defined as a tuple:

```
M = (f, B_20, B_r, E)
```

where:

- `f` -- the column selection fraction.
- `B_20` -- weighted buckets for the top-20 replacement probability `f_20`
  (the probability that a non-null cell is replaced with one of 20 synthetic
  MCV candidates).
- `B_r` -- weighted buckets for the dominance ratio `r` in `(0, 1)`, which
  controls how much of the MCV mass is concentrated on the single dominant value.
- `E` -- the exclusion set (shared with the NULL profile).

## Eligibility

Eligibility rules are identical to NULL injection:

1. Not a primary key or foreign key.
2. NDV >= 50.
3. Not in the exclusion set `E`.

## Per-Column Assignment

For each eligible column `c`, the following steps are applied:

### Step 1: Column selection

Hash-based selection identical to NULL injection but with a separate token
(`"select-mcv"`) to ensure independent selection from the NULL profile.

### Step 2: f_20 assignment

Draw a bucket from `B_20` and interpolate to obtain the probability `f_20` that a
non-null cell is replaced with one of the 20 synthetic MCV candidates.

### Step 3: Dominance ratio (r) assignment

Draw a bucket from `B_r` (constant across tiers) and interpolate. The top-1
probability is computed as:

```
f_1 = f_20 * r
```

This concentrates a fraction `r` of all MCV replacements on a single dominant
synthetic value.

### Step 4: Per-cell replacement

For each non-null cell with hash value `h`:

- If `h < f_1`, replace with the dominant synthetic value (rank 1).
- Else if `h < f_20`, replace with one of the remaining 19 candidates.
- Else, keep the original value.

### Synthetic value generation

The 20 synthetic values per column are generated deterministically from the seed
(not sampled from original data) and are type-aware: integers, decimals, dates,
and strings each receive appropriately typed synthetic values.

## Execution Order

The three data transformations are applied in a fixed order:

```
NULL injection  -->  Stringification  -->  MCV injection
```

Cells already nullified by NULL injection are skipped during MCV replacement, so
the two injections do not interfere with each other.

## Dominance Ratio Buckets

The dominance ratio buckets `B_r` are shared across all three skew tiers:

| Range [lo, hi) | Weight |
|----------------|-------:|
| [0.10, 0.30)   |   0.25 |
| [0.30, 0.65)   |   0.45 |
| [0.65, 0.95)   |   0.30 |

## Skew Tiers

Prod-DS ships three MCV profile configurations. All share the same dominance-ratio
buckets `B_r` and exclusion set; they differ in column selection fraction `f` and
`f_20` bucket weights. The **medium** tier is derived from production fleet
telemetry.

| Range `f_20`    | Intent      | Low  | Med  | High |
|-----------------|-------------|-----:|-----:|-----:|
| [0.00, 0.05)    | Near-zero   | 0.50 | 0.35 | 0.03 |
| [0.05, 0.20)    | Mild        | 0.25 | 0.20 | 0.05 |
| [0.20, 0.40)    | Moderate    | 0.15 | 0.20 | 0.07 |
| [0.40, 0.60)    | Noticeable  | 0.08 | --   | 0.30 |
| [0.60, 0.80)    | Strong      | 0.02 | --   | --   |
| [0.60, 0.95)    | Strong      | --   | 0.20 | 0.40 |
| [0.95, 0.999)   | Extreme     | --   | 0.05 | --   |
| [0.95, 0.99)    | Very strong | --   | --   | 0.15 |

| Parameter              | Low     | Med      | High    |
|------------------------|--------:|---------:|--------:|
| Column fraction `f`    |    0.35 |     0.70 |    0.90 |
| Dominant MCV share     |  <20%   |  5--95%  | 40--95% |

Note: The low tier has more granular buckets at the upper end while the medium and
high tiers use wider ranges with heavier weights at the high end to model the
extreme skew observed in production.

## Configuration Reference

The MCV profile tiers are defined in:

- **`config/mcv_profiles.yml`** -- contains the three shipped tiers:
  - `mcv_low` (low skew)
  - `mcv_fleet_default` (medium/default, derived from production telemetry)
  - `mcv_high` (high skew)

Each profile entry specifies `column_selection_fraction`, `top20_buckets`
(for `f_20` assignment), `r_buckets` (for dominance ratio assignment),
excluded tables, and excluded qualified columns.

## Implementation Reference

- **`workload/dsdgen/stringify.py`** -- contains the MCV injection logic,
  including synthetic value generation, BLAKE2b-based column selection and
  bucket assignment, and per-cell replacement.

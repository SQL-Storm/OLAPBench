# Appendix D: NULL Sparsity Profiles

This appendix details the NULL injection mechanism used by Prod-DS to model
column-wise sparsity patterns observed in production datasets.

## Overview

In TPC-DS, most columns are dense. In production schemas, a long tail of sparse
attributes exists: above the 80th column percentile, NULL fractions rise sharply,
and the top 10% of columns are 60--100% NULL. Prod-DS injects NULLs according to
configurable NULL profiles to reproduce this qualitative shape.

## Profile Tuple

A NULL profile is defined as a tuple:

```
P = (f, B, E)
```

where:

- `f` is in `(0, 1]` -- the column selection fraction (fraction of eligible
  columns that receive a non-zero NULL probability).
- `B = {(w_i, lo_i, hi_i)}` -- a set of weighted buckets with `sum(w_i) = 1`.
  Each bucket defines a NULL probability range `[lo_i, hi_i)`.
- `E` -- an exclusion set of columns that must remain dense (never receive NULLs).

## Eligibility Criteria

A column is eligible for NULL injection if all of the following hold:

1. It is **not** a primary key or foreign key.
2. It has **NDV >= 50** (low-cardinality columns are excluded because NULL
   injection would disproportionately distort their value distributions).
3. It is **not** in the exclusion set `E`.

The exclusion set includes:

- Entire tables: `date_dim`, `time_dim`.
- Specific columns used as critical filter or join predicates (e.g.,
  `item.i_category`, `store.s_state`, `customer_address.ca_state`,
  `customer_demographics.cd_gender`).
- Measure columns that must remain dense for aggregation correctness (e.g.,
  `store_sales.ss_ext_sales_price`).

The full exclusion list is defined in `config/null_profiles.yml`.

## Per-Column Assignment Algorithm

For each eligible column `c` in table `t`, with global seed `s`, the following
four-step deterministic procedure is applied:

### Step 1: Column selection

Compute `h_sel = BLAKE2b(s, t, c, "select")`, mapped to `[0, 1)`.
If `h_sel >= f`, the column is **skipped** (receives no NULLs).

### Step 2: Bucket assignment

Compute `h_bkt = BLAKE2b(s, t, c, "bucket")`, mapped to `[0, 1)`.
Select bucket `b_i` via cumulative weights.

### Step 3: Probability interpolation

Compute `h_prob = BLAKE2b(s, t, c, "prob")`, mapped to `[0, 1)`.
The column NULL probability is:

```
p_c = lo_i + h_prob * (hi_i - lo_i)
```

### Step 4: Cell injection

For each row `r`, compute `h_cell = BLAKE2b(s, t, c, r)`, mapped to `[0, 1)`.
If `h_cell < p_c`, the cell is set to NULL.

### Hash construction

All hashes use an 8-byte BLAKE2b digest converted to a uniform float via
`int(digest) / 2^64`. The procedure is fully deterministic: identical
`(s, P, schema, data)` inputs produce byte-identical outputs.

## Sparsity Tiers

Prod-DS ships three NULL profile configurations. All share the same six bucket
boundaries and exclusion set; they differ in column selection fraction `f` and
bucket weights.

The **medium** tier is derived from production fleet telemetry and serves as the
default. The **low** and **high** tiers redistribute bucket weights while
preserving the same boundary structure.

| Range           | Intent      | Low  | Med  | High |
|-----------------|-------------|-----:|-----:|-----:|
| [0.000, 0.005)  | Near-zero   | 0.45 | 0.20 | 0.05 |
| [0.005, 0.020)  | Very low    | 0.20 | 0.05 | 0.03 |
| [0.020, 0.100)  | Low         | 0.15 | 0.05 | 0.02 |
| [0.100, 0.400)  | Moderate    | 0.12 | 0.10 | 0.10 |
| [0.400, 0.800)  | High        | 0.06 | 0.22 | 0.30 |
| [0.800, 0.995)  | Very high   | 0.02 | 0.40 | 0.50 |

| Parameter              | Low    | Med     | High   |
|------------------------|-------:|--------:|-------:|
| Column fraction `f`    |   0.12 |    0.24 |   0.40 |
| Expected cell nullity  | 5--15% | 30--50% | 60--80%|

## Configuration Reference

The NULL profile tiers are defined in:

- **`config/null_profiles.yml`** -- contains the three shipped tiers:
  - `null_low` (low sparsity)
  - `fleet_realworld_final` (medium/default, derived from production telemetry)
  - `null_high` (high sparsity)

Legacy profiles (`fleet_default`, `fleet_v2_default`, `fleet_v3_default`) are
preserved in the configuration file for historical reference but are not used in
the paper evaluation.

## Implementation Reference

- **`workload/dsdgen/stringify.py`** -- contains the NULL injection logic,
  including the BLAKE2b hashing procedure, eligibility checks, bucket assignment,
  and per-cell injection. The `MIN_NDV_FOR_INJECTION` constant (set to 50)
  enforces the NDV eligibility threshold.

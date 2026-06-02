# Quickstart: Prod-DS Kit on SF1 with DuckDB

This guide walks through generating TPC-DS and Prod-DS data at scale factor 1,
generating queries, loading them into DuckDB, and comparing execution times.

## Prerequisites

- `./install.sh` completed successfully (from the repository root)
- DuckDB CLI installed (`brew install duckdb` on macOS, or see https://duckdb.org/docs/installation/)
- Virtual environment activated: `source .venv/bin/activate`

All commands below are run from the **repository root**.

## Step 1: Generate SF1 data at STR=1 (vanilla TPC-DS baseline)

```bash
python3 wrap_dsdgen.py --stringification-level 1 -DIR ./data/sf1_str1 -SCALE 1
```

This produces standard TPC-DS `.dat` files with pipe-delimited fields and no
stringification applied (integer keys remain integers).

## Step 2: Generate SF1 data at STR=10 (full Prod-DS)

```bash
python3 wrap_dsdgen.py --stringification-level 10 \
    --null-profile medium --mcv-profile medium \
    -DIR ./data/sf1_str10 -SCALE 1
```

This applies all 131 column recasts, NULL sparsity injection, and MCV skew
injection to produce production-realistic data.

## Step 3: Generate queries

```bash
# Vanilla TPC-DS queries (no extensions)
python3 wrap_dsqgen.py --output-dir ./queries/sf1_str1 --no-extensions

# Full Prod-DS queries (extended templates, joins, unions)
python3 wrap_dsqgen.py --output-dir ./queries/sf1_str10 \
    --stringification-level 10
```

## Step 4: Load into DuckDB and run

```bash
bash examples/quickstart/run.sh
```

Or manually:

```bash
# Create and load STR=1 database
duckdb ./data/sf1_str1.duckdb < <(python3 tools/generate_tpcds_schema.py --level 1)
# Load data files...

# Create and load STR=10 database
duckdb ./data/sf1_str10.duckdb < <(python3 tools/generate_tpcds_schema.py --level 10)
# Load data files...
```

## Step 5: Compare timing

The `run.sh` script prints a side-by-side comparison of execution times for
a subset of queries under STR=1 (vanilla) and STR=10 (Prod-DS) configurations.

## Expected observations

- STR=10 queries that touch recast columns will show measurable slowdowns due
  to string comparison and hashing overhead replacing integer operations.
- Join-amplified queries scale super-linearly with the join target parameter.
- NULL and MCV injection affect selectivity estimates and plan choices.

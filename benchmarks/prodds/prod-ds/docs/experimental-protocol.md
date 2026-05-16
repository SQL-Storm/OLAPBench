# Experimental Protocol

> **Note.** This document is the frozen protocol used for the paper evaluation.
> All paths use environment variables (`$DATA_ROOT`, `$QUERY_DIR`, `$REPO_ROOT`).
> See `install.sh` for setup.

## Mode

Mechanism-first, reproducible, one-engine-at-a-time execution.

---

## Host Environment

| Property      | Value                                  |
|---------------|----------------------------------------|
| OS            | Ubuntu 24.04.4 LTS                     |
| Kernel        | 6.14.0-37-generic                      |
| CPU           | 2 x AMD EPYC 7453, 28 cores/socket    |
| SMT           | 2 (hyperthreading)                     |
| Logical CPUs  | 112                                    |
| RAM           | ~1.0 TiB                               |

## Engine Versions

| Engine     | Version                                     |
|------------|---------------------------------------------|
| DuckDB     | v1.4.4 (Andium) `6ddac802ff`                |
| CedarDB    | v2026-02-03 (2026-01-29, format 0)          |
| MonetDB    | 11.55.1 (Dec2025)                           |

---

## Data Policy

Authoritative data roots:

- `$DATA_ROOT/tpcds/sf100`
- `$DATA_ROOT/prodds/sf100/str1`
- `$DATA_ROOT/prodds/sf100/str10`

No data is copied into the repository.

## Query Policy

- Canonical query root namespace: `$QUERY_DIR/...`
- Engine-specific variant root: `experiments/queries/<engine>/<suite>/...`
- Mapping file: `experiments/queries/query_mapping.yaml`
- `query_0.sql` is always excluded from execution.

Active scaling levels:

- Join: `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]`
- Union: `[2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]` (level 1
  deprecated)

---

## Execution Invariants

| Invariant                 | Value                                       |
|---------------------------|---------------------------------------------|
| Node count                | 1 (single node)                             |
| Query concurrency         | Serial (no concurrent query workers)        |
| Engine concurrency        | Exactly 1 engine active at a time           |
| Warmup passes             | 1                                           |
| Timed repetitions         | 10                                          |
| Per-query timeout          | 1800 seconds (30 min)                       |

## Threading Policy

- **Baseline** (default): physical cores only, 56 threads/workers.
- **Optional sensitivity mode**: logical CPUs, 112 threads/workers.

| Engine     | Baseline command                                           |
|------------|------------------------------------------------------------|
| DuckDB     | `PRAGMA threads=56;`                                       |
| MonetDB    | `monetdb set nthreads=56 <db>`                             |
| CedarDB    | `parallel="56"` (or environment equivalent)                |

## Statistics Collection

Before any timed workload execution, a full statistics refresh is mandatory on all
tables for optimizer fairness and cross-engine comparability.

| Engine     | Command                              | Notes                           |
|------------|--------------------------------------|---------------------------------|
| DuckDB     | `ANALYZE;` or `PRAGMA analyze;`      | Explicit after bulk load        |
| MonetDB    | `ANALYZE;` or `CALL sys.analyze();`  | Explicit before timing          |
| CedarDB    | `ANALYZE;`                           | Protocol symmetry; adaptive internally |

Statistics collection is not timed. Completion must be logged.

## Timeout Policy

- Per-query timeout: **1800 seconds** (30 minutes).
- Near-timeout exclusion rule: if any audited engine completes a query within 60
  seconds of the timeout, that query is excluded from the common subset.

---

## Experiments

### E1 -- Audit and Timeout Freeze

Run audit with provisional timeout. Freeze timeout `T` using rule:
`T = max_successful_calibration_runtime + 60s`. Compute and publish common subset.

Runner: `python -m experiments run --experiment workload_compare`

### E2 -- End-to-End Timing

- Suites: `tpcds`, `prodds` (STR10).
- Report per-query medians and workload median totals.

Runner: `python -m experiments run --experiment workload_compare`

### E3 -- Join Scaling Micro-Suite

- Join levels: `[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]`
- Generator model: `J(k,m) = (m+1)*(b + k*(b+1)) + m` with base width `b = 10`.
- Calibration pinned in `workload/config/returns.yml::target_overrides`.
- Planning collection: DuckDB deep-dive with `EXPLAIN (ANALYZE, FORMAT JSON)`;
  CedarDB/MonetDB best-effort via `EXPLAIN` wall-clock.

Runner: `python -m experiments run --experiment join_scaling`

### E5 -- Stringification Sweep (DuckDB, STR 1..15)

- Engine: DuckDB only.
- Workload: Prod-DS SF100, stringification levels STR 1 through STR 15.
- Preflight: full query-set feasibility check with non-empty enforcement per level.
- Timing: selected stringification-sensitive queries; 1 warmup, timed repetitions,
  median reporting.

Runner: `python -m experiments run --experiment string_sweep`

### E6 -- Result Verification

- DuckDB-only post-E5 consistency audit.
- Deterministic wrapper: `SELECT * FROM (<query>) q ORDER BY 1, 2, ..., N`.
- SHA256 fingerprinting per `(query, STR)` pair.
- Classification: `FORMAT_ONLY`, `ORDERING_ONLY`, `TRUE_SEMANTIC_CHANGE`.

---

## Error Taxonomy

| Status    | Description                                  |
|-----------|----------------------------------------------|
| `SUCCESS` | Query completed within timeout               |
| `TIMEOUT` | Query exceeded per-query timeout             |
| `ERROR`   | Query failed (see subclasses below)          |

Error subclasses:

| Subclass  | Description                                  |
|-----------|----------------------------------------------|
| `OOM`     | Out-of-memory error                          |
| `FAILURE` | Engine-reported execution failure             |
| `DIALECT` | SQL dialect incompatibility                  |
| `PARSE`   | SQL parse error                              |
| `UNKNOWN` | Fallback if classification fails             |

## Artifact Contract (Per Run)

Every run must produce:

- `raw_times.csv`
- `failures.csv`
- `audit.json`
- `run_manifest.json`
- `environment_snapshot.md`
- `effective_config.json`

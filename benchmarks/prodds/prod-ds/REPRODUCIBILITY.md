# Reproducibility Guide

**Paper:** "Scaling Analytical Benchmarks to Production Complexity: A Data- and Query-Centric Extension to TPC-DS"
**Venue:** VLDB 2027
**Repository:** https://github.com/szlangini/prod-ds-kit

## Overview

Prod-DS Kit is a data- and query-centric extension to TPC-DS. It adds
stringification, join-graph amplification, NULL sparsity, MCV skew, and
92 extended query templates to the standard TPC-DS benchmark. This
reproducibility package allows reviewers to regenerate all data and queries,
re-run the five experiments reported in the paper, and reproduce the
corresponding figures and tables.

## Prerequisites

- **OS:** Ubuntu 22.04+ (tested on Ubuntu 24.04.4 LTS)
- **Python:** >= 3.9
- **System packages:** `curl`, `unzip`, `git`, `build-essential` (for TPC-DS toolkit compilation)
- **Disk:**
  - ~50 GB for SF=10
  - ~500 GB for SF=100
- **RAM:**
  - 16 GB minimum (SF=1)
  - 64 GB recommended (SF=10)
  - 1 TiB for full reproduction (SF=100)

## Quick Start (SF=1, ~30 minutes)

A minimal run to verify the pipeline works end-to-end on a single engine.

```bash
./reproduce.sh --init --sf 1 --engines duckdb
./reproduce.sh --experiment E1 --sf 1 --engines duckdb --reps 1
```

This generates SF=1 data, loads it into DuckDB, and runs Experiment E1 with
a single repetition. Results appear in `.reproduce/results/`.

## Recommended Review Run (SF=10, ~4-8 hours)

A practical configuration for reviewers with a 64 GB machine.

```bash
./reproduce.sh --init --sf 10 --engines duckdb
./reproduce.sh --all --sf 10 --engines duckdb --reps 3
```

This runs all five experiments at SF=10 with three repetitions each.

## Full Reproduction (SF=100, matching paper)

Reproduces the paper results exactly. Requires ~1 TiB RAM, ~500 GB disk,
and all three engines installed.

```bash
./reproduce.sh --init --sf 100 --engines all --reps 10
./reproduce.sh --all --sf 100 --engines all --reps 10
```

## Experiments

| ID | Paper Section | Description                              | Engines              | Figures/Tables |
|----|---------------|------------------------------------------|----------------------|----------------|
| E1 | 6.5          | End-to-end TPC-DS vs Prod-DS             | DuckDB, CedarDB, MonetDB | Fig. 8, 9, 10 |
| E2 | 6.6          | Join-scaling micro-suite (J=16..2048)    | All                  | Fig. 11        |
| E3 | 6.7          | UNION ALL fan-in scaling (U=2..2048)     | All                  | Fig. 12        |
| E4 | 6.8          | Stringification sweep (STR=1..15)        | DuckDB               | Fig. 13        |
| E5 | 6.9          | Sparsity and skew sensitivity            | DuckDB, CedarDB      | Table 3        |

Individual experiments can be run selectively:

```bash
./reproduce.sh --experiment E1 --sf 10 --engines duckdb --reps 3
./reproduce.sh --experiment E4 --sf 10 --engines duckdb --reps 3
```

## Command Reference

```
./reproduce.sh [FLAGS]
```

| Flag              | Description                                          | Default      |
|-------------------|------------------------------------------------------|--------------|
| `--init`          | Install dependencies, build TPC-DS toolkit, generate data, and load databases | (required for first run) |
| `--experiment ID` | Run a single experiment (E1, E2, E3, E4, or E5)     | --           |
| `--all`           | Run all experiments (E1 through E5)                  | --           |
| `--sf N`          | Scale factor (1, 10, 100)                            | 1            |
| `--engines LIST`  | Comma-separated engine list or `all`                 | duckdb       |
| `--reps N`        | Number of timed repetitions per query                | 3            |
| `--warmup N`      | Number of warmup passes before timed runs            | 1            |
| `--timeout N`     | Per-query timeout in seconds                         | 1800         |
| `--threads N`     | Number of threads (auto-detected if omitted)         | auto         |
| `--plots`         | Generate plots from existing results without re-running experiments | -- |
| `--help`          | Print usage information and exit                     | --           |

## Engine Versions (Paper)

The paper results were obtained with the following engine versions:

- **DuckDB** v1.4.4 (Andium)
- **CedarDB** v2026-02-03
- **MonetDB** v11.55.1 (Dec2025)

The `--init` step downloads DuckDB automatically. CedarDB and MonetDB must
be installed separately if you wish to use them (see Troubleshooting below).

## Execution Protocol (Paper Section 6.2)

The paper follows this protocol for all timed measurements:

- **Isolation:** One engine at a time (no concurrent engine processes).
- **Warmup:** 1 warmup pass (queries executed but not recorded).
- **Repetitions:** 10 timed repetitions per query.
- **Timeout:** 1800 seconds per query.
- **Reporting:** Median over repetitions.
- **Hardware reference:** 2x AMD EPYC 7453 (56 physical cores), ~1 TiB RAM, Ubuntu 24.04.4 LTS.

For quicker validation, use `--reps 1` or `--reps 3`. Trends are visible
even at lower repetition counts; absolute numbers may differ from the paper
on different hardware.

## Output Structure

All artifacts are written to the `.reproduce/` directory:

```
.reproduce/
  data/              # Generated TPC-DS + Prod-DS data files
  queries/           # Generated SQL queries (TPC-DS and extended)
  databases/         # Engine-specific database files (e.g., DuckDB .db)
  configs/           # Auto-generated experiment configurations
  results/
    E1/
      raw.jsonl      # Per-query, per-repetition timing records
      summary.csv    # Aggregated results (median, p5, p95)
    E2/
      raw.jsonl
      summary.csv
    ...
    plots/           # Generated figures (PDF and PNG)
```

**Result format:**
- `raw.jsonl`: One JSON object per query execution, including query ID,
  engine, repetition number, wall-clock time, and status.
- `summary.csv`: One row per (query, engine) pair with median, 5th, and 95th
  percentile runtimes.

To regenerate plots from existing results:

```bash
./reproduce.sh --plots
```

## Troubleshooting

**TPC-DS toolkit build fails**
Install build dependencies:
```bash
sudo apt-get install build-essential
```

**DuckDB download fails**
Check network connectivity. The init step downloads DuckDB v1.4.4 from
GitHub releases. If the download is blocked, manually download the binary
and place it on your PATH.

**CedarDB or MonetDB not available**
These engines require separate installation. If you cannot install them,
skip them and use DuckDB only:
```bash
./reproduce.sh --all --sf 10 --engines duckdb --reps 3
```
DuckDB alone covers experiments E1 through E5 and is sufficient to validate
the main claims.

**Long runtimes**
Use a smaller scale factor or fewer repetitions for initial validation:
```bash
./reproduce.sh --all --sf 1 --engines duckdb --reps 1
```

**Disk space errors during data generation**
SF=100 requires approximately 500 GB. Check available space with `df -h`
before starting. SF=10 requires approximately 50 GB.

**Python dependency errors**
The init step creates a virtual environment at `.venv/`. If it fails, ensure
Python >= 3.9 is installed and try:
```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

## License

This software is provided for research reproducibility. The TPC-DS toolkit
is subject to the TPC End User License Agreement. See `NOTICE.md` for
details and third-party license information.

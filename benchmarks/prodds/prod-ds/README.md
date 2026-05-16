# Prod-DS Kit

Prod-DS Kit is a data- and query-centric extension to TPC-DS that adds
production-realistic string processing, join-graph amplification, NULL
sparsity, most-common-value (MCV) skew, and configurable query complexity
(LIMIT, GROUP BY, UNION ALL fan-in) to the standard benchmark. All extensions
operate at the logical level, enabling engine-agnostic evaluation across
analytical systems.

## Paper Appendices (Technical Documentation)

The full formal specification of every extension, including algorithms,
formulas, and worked examples from the paper appendices:

| Appendix | Document | Contents |
|----------|----------|----------|
| A | [docs/join-amplification.md](docs/join-amplification.md) | Join-graph growth model J(k,m), SQL patterns (LOD/segment CTE), LEFT JOIN rejoin, calibration tables |
| B | [docs/stringification-levels.md](docs/stringification-levels.md) | Intensity formula i=(STR-1)/9, full STR=1..15 table, column counts, padding widths, STR+ mode |
| C | [docs/column-recast.md](docs/column-recast.md) | 131 recast candidates across 24 tables, semantic categories, selection ordering, usage statistics |
| D | [docs/null-profiles.md](docs/null-profiles.md) | Profile tuple P=(f,B,E), 4-step BLAKE2b assignment, three sparsity tiers with bucket definitions |
| E | [docs/mcv-profiles.md](docs/mcv-profiles.md) | Profile tuple M=(f,B20,Br,E), dominance ratios, three skew tiers, injection ordering |
| -- | [docs/experimental-protocol.md](docs/experimental-protocol.md) | Frozen evaluation protocol (E1-E6), engine versions, host spec, timeout policy, error taxonomy |
| -- | [docs/dialect-adaptations.md](docs/dialect-adaptations.md) | Per-engine SQL rewrites for DuckDB, CedarDB, MonetDB; adding a new dialect |

## Quick Start

```bash
git clone https://github.com/szlangini/prod-ds-kit.git
cd prod-ds-kit
./install.sh
source .venv/bin/activate
```

### Default Commands

The `--default` flag uses recommended settings so you can get started with a single flag.

```bash
# Generate data (STR=10, NULL=medium, MCV=medium, SF=10, output=./output)
python3 wrap_dsdgen.py --default

# Generate queries (STR=10, dialect=duckdb, output=./queries)
python3 wrap_dsqgen.py --default
```

You can override individual defaults:

```bash
# Default settings but at SF=1 (smaller, faster)
python3 wrap_dsdgen.py --default -SCALE 1

# Default settings but at SF=100
python3 wrap_dsdgen.py --default -SCALE 100
```

## For AI Agents

Read [`AGENTS.md`](AGENTS.md) for a structured project overview.

### Agent Prompt (Copy-Paste)

Give this prompt to an AI coding agent (e.g. Claude Code, Cursor, Copilot) to
set up and run Prod-DS Kit end-to-end with DuckDB:

> Clone the Prod-DS Kit repository from https://github.com/szlangini/prod-ds-kit.git
> and run `./install.sh` to build the TPC-DS toolkit. Activate the virtual
> environment with `source .venv/bin/activate`. Verify that `dsdgen` and `dsqgen`
> binaries exist in `tpcds-kit/tools/`. Check that `python3 -c "from workload
> import stringification"` succeeds. Then generate data using
> `python3 wrap_dsdgen.py --default -SCALE 1` (uses STR=10, NULL=medium,
> MCV=medium). Generate queries with `python3 wrap_dsqgen.py --default`.
> Verify that `./output/` contains `.dat` files and `./queries/` contains `.sql`
> files. Report any errors encountered.

## Extension Parameters

### Data generation (`wrap_dsdgen.py`)

| Flag | Values | Default | Effect |
|------|--------|---------|--------|
| `--stringification-level` | 1-15 | 10 | Columns recast to strings (1-10) and per-value padding width; STR>10 extends string length only (column set frozen at STR=10) |
| `--stringification-preset` | `vanilla` (STR 1), `low` (STR 3), `medium` (STR 5), `high` (STR 7), `production` (STR 10) | none | Named shortcut for stringification level |
| `--null-profile` | `low`, `medium`, `high` | none (no NULLs) | Fleet-derived NULL sparsity tier injected into eligible columns |
| `--mcv-profile` | `low`, `medium`, `high` | none (no MCV) | Fleet-derived MCV skew tier injected into eligible columns |
| `-SCALE` | integer | 1 | TPC-DS scale factor (1, 10, 100, ...) |
| `-DIR` | path | required | Output directory for `.dat` files |

### Query generation (`wrap_dsqgen.py`)

| Flag | Values | Default | Effect |
|------|--------|---------|--------|
| `--default` | flag | off | Use recommended defaults: STR=10, dialect=duckdb, output=./queries |
| `--output-dir` | path | required | Output directory for generated SQL files (optional with `--default`) |
| `--stringification-level` | 1-15 | none | Activates extended templates and literal post-processing for the given STR level |
| `--no-extensions` | flag | off | Use base TPC-DS templates only (skip `*_ext.tpl`) |
| `--dialect` | `ansi`, `duckdb` | `ansi` | SQL dialect for dsqgen output |
| `--join` / `--no-join` | flag | on | Include/exclude join-amplified queries |
| `--join-targets` | comma-separated ints | `50,100,200` | Target effective join counts for generated join queries |
| `--union` / `--no-union` | flag | on | Include/exclude UNION ALL fan-in queries |
| `--union-max-inputs` | integer | no cap | Cap maximum UNION ALL fan-in branches |
| `--pure-data-mode` | flag | off | Disable query-layer rewrites (for data-only stringification evaluation) |
| `--scale` | integer | `1` | Scale factor passed to dsqgen |

### Seed

The experiment queries use the default dsqgen seed (`19620718`) and have been
validated at SF=1, SF=10, and SF=100. If you encounter issues with other seeds, please
open an issue so we can investigate.

### Profile tiers

| Dimension | Low | Medium (default) | High | Config file |
|-----------|-----|-------------------|------|-------------|
| NULL sparsity | ~5% columns, light rates | ~30% columns, fleet-derived rates | ~60% columns, heavy rates | `config/null_profiles.yml` |
| MCV skew | ~20% columns, mild dominance | ~70% columns, fleet-derived dominance | ~90% columns, strong dominance | `config/mcv_profiles.yml` |
| Stringification | vanilla (STR 1): 0 columns recast | medium (STR 5): 65 columns | production (STR 10): 131 columns | `config/string_profiles.yml` |

### Benchmark runner (`python -m experiments run`)

| Flag | Values | Default | Effect |
|------|--------|---------|--------|
| `--config` | path | required | YAML config file (see `experiments/config.example.yaml`) |
| `--experiment` | `workload_compare`, `join_scaling`, `string_sweep` | required | Which experiment to execute |
| `--system` | `duckdb`, `cedardb`, `monetdb` | required | Target engine |

See `experiments/config.example.yaml` for the full configuration schema
including threading, memory limits, timeouts, and repetition counts.

## What Prod-DS Adds to TPC-DS

### Stringification (STR=1..15)

Recasts up to 131 integer columns (surrogate keys, demographic keys, time
keys, codes) to variable-length strings. Forces hash joins, disables integer
fast-paths, and inflates working-set sizes. STR=1 is vanilla TPC-DS; STR=10
covers all 131 columns. STR+ (11-15) extends per-value string length without
adding new columns.

### Join-Graph Amplification

Generates CTE-based queries with tuneable join counts (1 to 2048+). Uses a
two-level design: base blocks replicated across LOD and segment branches, with
a LEFT JOIN rejoin pattern. The join count follows the formula
J(k,m) = (m+1) * J0(k) + m, where J0(k) = b + k*(b+1).

### UNION ALL Fan-In Scaling

Generates standalone UNION ALL queries with 2, 5, 10, 20, or 200 branches
over a shared base CTE (store_sales/date_dim/store), each branch filtering
on a different month. Stresses materialisation buffers, hash-table sizing,
and scan concurrency at scale.

### NULL Sparsity Injection

Injects NULL values into eligible non-key columns using fleet-derived
probability distributions. Three tiers (low, medium, high) control the
fraction of affected columns and per-cell NULL rates. Assignment is
deterministic via BLAKE2b hashing.

### MCV Skew Injection

Replaces values in eligible columns with synthetic most-common values to
create realistic frequency skew. Three tiers (low, medium, high) control
the fraction of affected columns and dominance ratios. Execution order:
NULL injection, then stringification, then MCV injection.

### Extended Query Templates

92 extended templates (*_ext.tpl) that rewrite GROUP BY, LIMIT, and filter
predicates to reference stringified columns where appropriate. Each template
is automatically selected based on the active stringification level.

## Reproducing Paper Experiments

A single script reproduces all experiments (E1–E5) from the paper:

```bash
# Quick validation (SF=1, ~15 min, DuckDB only)
./reproduce.sh --init --all --sf 1

# Full reproduction (SF=10, all engines)
./reproduce.sh --init --all --sf 10 --engines all --reps 10
```

This generates data, queries, dialect-specific fixes, runs all experiments,
and produces the paper figures in `.reproduce/results/plots/`.

See [REPRODUCIBILITY.md](REPRODUCIBILITY.md) for the full reviewer guide.

## Supported Engines

DuckDB, CedarDB, MonetDB.

Engine adapters are in `experiments/adapters/`. Adding a new engine requires
implementing the adapter interface in `experiments/adapters/base.py`.

## Running Tests

```bash
source .venv/bin/activate
pytest tests/ -v
```

Tests that require the TPC-DS toolkit (built via `install.sh`) are marked
with `@pytest.mark.needs_tpcds_tools` and will be skipped automatically
if the toolkit is not present. Tests requiring a C++ compiler are marked
with `@pytest.mark.needs_cpp`.

## License

Prod-DS Kit is released under an academic non-commercial license.
See [LICENSE](LICENSE) for the full terms.

The underlying TPC-DS toolkit is subject to the
[TPC End User License Agreement](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_eula_v2.2.0.pdf)
and is fetched separately during installation. See [NOTICE.md](NOTICE.md)
for third-party attributions.

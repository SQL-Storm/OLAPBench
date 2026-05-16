# Benchmark Harness

This harness runs three benchmark experiments against multiple analytical SQL engines using a single execution mode (native or docker).

## Quick Start

1) Create a config file:

```
cp experiments/config.example.yaml experiments/config.yaml
```

2) Edit `experiments/config.yaml` to point to your SQL directories, generator commands, and engine settings.

3) Run an experiment:

```
python -m experiments run --config experiments/config.yaml --experiment workload_compare --system duckdb
```

## Experiments

### Experiment 1: Workload comparison
Runs TPC-DS and Prod-DS query suites.

```
python -m experiments run --config experiments/config.yaml --experiment workload_compare --system all
```

Required config:
- `experiments.workload_compare.tpcds_dir`
- `experiments.workload_compare.prodds_dir`

### Experiment 2: Join scaling
Generates join-heavy queries for each join count and seed, then executes them.

```
python -m experiments run --config experiments/config.yaml --experiment join_scaling --system duckdb
```

Required config:
- `experiments.join_scaling.generator_command` (supports `{J}`, `{SEED}`, `{OUTDIR}`, `{OUTFILE}` placeholders)

Optional:
- `experiments.join_scaling.generator_output_file` if the generator writes a file instead of stdout.

### Experiment 3: Stringification sweep
Generates Prod-DS queries for each stringification level and executes them.

```
python -m experiments run --config experiments/config.yaml --experiment string_sweep --system duckdb
```

Required config:
- `experiments.string_sweep.generator_command` (supports `{S}`, `{OUTDIR}`, `{SUBSET_FILE}` placeholders)

Optional:
- `experiments.string_sweep.query_subset_file`

## Results Layout

Results are written to:

```
results/<timestamp>/<system>/<experiment>/raw.jsonl
results/<timestamp>/<system>/<experiment>/summary.csv
results/<timestamp>/manifest.json
```

Each JSONL record includes timestamps, per-query outcomes, timing, memory (best-effort), and git hashes. Summaries report median runtime and failure rates per query.

## Resource Limits

- **Native mode**: applies CPU affinity via `taskset` when provided, and memory limits via cgroup v2 when available. If cgroup v2 is unavailable or not writable, the harness falls back to `ulimit -v` (virtual memory).
- **Docker mode**: uses `--cpuset-cpus` and `--memory`.

## Scripts

The install/setup scripts you provided are preserved under:
- `experiments/scripts/install_engines.sh`
- `experiments/scripts/setup_eval_env.sh`

## Notes

- `planning_enabled` runs a fast `EXPLAIN` phase before each query to separate planning time; set `planning_enabled: false` to skip.
- `load_command` is required per engine and is run once before each experiment.
- If an engine crashes mid-run, the harness will restart it and continue.

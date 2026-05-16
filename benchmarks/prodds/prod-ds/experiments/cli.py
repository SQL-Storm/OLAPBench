from __future__ import annotations

import sys

import click

from .config import ConfigError, load_config
from .runner import BenchmarkRunner


@click.group()
def main() -> None:
    """Benchmark harness for analytical SQL engines."""


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True))
@click.option(
    "--experiment",
    required=True,
    type=click.Choice(["workload_compare", "join_scaling", "union_scaling", "string_sweep"], case_sensitive=False),
)
@click.option(
    "--system",
    required=True,
    type=click.Choice(["duckdb", "postgres", "cedardb", "monetdb", "all"], case_sensitive=False),
)
def run(config_path: str, experiment: str, system: str) -> None:
    """Run a benchmark experiment."""
    try:
        cfg = load_config(config_path)
    except ConfigError as exc:
        click.echo(f"Config error: {exc}", err=True)
        sys.exit(2)

    runner = BenchmarkRunner(cfg, experiment, system)
    runner.run()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from benchmarks.benchmark import benchmarks
from util import logger


workdir = os.getcwd()


def generate(args):
    benchmark_descriptions = benchmarks()

    if args.env is not None:
        load_dotenv(dotenv_path=args.env, verbose=True)

    logger.set_verbose(args.verbose)
    logger.set_very_verbose(args.very_verbose)

    data_dir = os.path.join(workdir, args.data)
    os.makedirs(data_dir, exist_ok=True)

    benchmark = benchmark_descriptions[args.benchmark].instantiate(data_dir, vars(args))
    logger.log_driver(f"Generating data for {benchmark.fullname}")
    benchmark.dbgen()


def main():
    logger.log_header("OLAPBench dbgen")

    if not os.getenv("VIRTUAL_ENV"):
        logger.log_warn(f"Activate the venv first:\n   source {os.path.dirname(os.path.realpath(__file__))}/.venv/bin/activate")

    parser = argparse.ArgumentParser(description="Generate data for a benchmark")
    parser.add_argument("-v", "--verbose", dest="verbose", default=False, action="store_true", help="verbose output")
    parser.add_argument("-vv", "--very-verbose", dest="very_verbose", default=False, action="store_true", help="very verbose output")
    parser.add_argument("--data", dest="data", type=str, default="data", help="directory where to store the data (default: ./data)")
    parser.add_argument("--env", dest="env", type=str, default=None, help="file containing environment variables")

    benchmark_map = benchmarks()
    benchmark_parsers = parser.add_subparsers(dest="benchmark", required=True)
    for name, benchmark in benchmark_map.items():
        benchmark_parser = benchmark_parsers.add_parser(name, help=benchmark.get_description())
        benchmark.add_arguments(benchmark_parser)

    args = parser.parse_args()

    generate(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.log_error(e)
        raise e

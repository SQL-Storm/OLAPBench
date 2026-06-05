#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

from dotenv import load_dotenv

from benchmarks.benchmark import benchmarks
from dbms.dbms import database_systems
from util.log import log


workdir = os.getcwd()


def _parse_kv(values):
    result = {}
    for item in values or []:
        if "=" not in item:
            raise ValueError(f"expected key=value, got: {item}")
        key, value = item.split("=", 1)
        if value.lower() in ("true", "false"):
            value = value.lower() == "true"
        else:
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
        result[key] = value
    return result


def generate(args):
    benchmark_descriptions = benchmarks()

    if args.env is not None:
        load_dotenv(dotenv_path=args.env, verbose=True)

    log.set_verbose(args.verbose)
    log.set_very_verbose(args.very_verbose)

    data_dir = os.path.join(workdir, args.data)
    os.makedirs(data_dir, exist_ok=True)

    benchmark = benchmark_descriptions[args.benchmark].instantiate(data_dir, vars(args))
    log.driver(f"Generating data for {benchmark.fullname}")
    benchmark.dbgen()

    if args.umbradev:
        db_dir = os.path.join(workdir, args.db)
        os.makedirs(db_dir, exist_ok=True)

        params = _parse_kv(args.parameter)
        settings = _parse_kv(args.setting)

        dbms_descriptions = database_systems()
        log.driver(f"Loading {benchmark.fullname} into umbradev (params: {params}, settings: {settings})")
        with dbms_descriptions["umbradev"].instantiate(benchmark, db_dir, data_dir, params, settings) as dbms:
            dbms.load_database()


def main():
    log.header("OLAPBench dbgen")

    if not os.getenv("VIRTUAL_ENV"):
        log.warn(f"Activate the venv first:\n   source {os.path.dirname(os.path.realpath(__file__))}/.venv/bin/activate")

    parser = argparse.ArgumentParser(description="Generate data for a benchmark")
    parser.add_argument("-v", "--verbose", dest="verbose", default=False, action="store_true", help="verbose output")
    parser.add_argument("-vv", "--very-verbose", dest="very_verbose", default=False, action="store_true", help="very verbose output")
    parser.add_argument("--data", dest="data", type=str, default="data", help="directory where to store the data (default: ./data)")
    parser.add_argument("--db", dest="db", type=str, default="db", help="directory where to store the databases (default: ./db), only used with --umbradev")
    parser.add_argument("--env", dest="env", type=str, default=None, help="file containing environment variables")
    parser.add_argument("--umbradev", dest="umbradev", default=False, action="store_true", help="after generating, load the database into umbradev")
    parser.add_argument("--parameter", "-p", dest="parameter", action="append", default=[], metavar="KEY=VALUE", help="umbradev parameter (repeatable)")
    parser.add_argument("--setting", "-s", dest="setting", action="append", default=[], metavar="KEY=VALUE", help="umbradev setting (repeatable)")

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
        log.error(e)
        raise e

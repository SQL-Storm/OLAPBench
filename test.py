#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from types import SimpleNamespace

from benchmark import run_benchmarks
from benchmarks.benchmark import benchmarks
from util import logger, schemajson

workdir = os.getcwd()
csv.field_size_limit(sys.maxsize)


def main():
    parser = argparse.ArgumentParser(description="Test OLAPBench.")
    parser.add_argument("filenames", nargs="*", default=[], help="Optional list of configuration files")
    args = parser.parse_args()

    yaml_files = [
        f for f in os.listdir(os.path.join(workdir, "test"))
        if f.endswith('.yaml') and (not args.filenames or f.split('.')[0] in args.filenames)
    ]
    for file in yaml_files:
        logger.log_header(file)

        args = SimpleNamespace(
            json=os.path.join(workdir, "test", file),
            verbose=False,
            very_verbose=False,
            db="db",
            data="data",
            env=None,
            clear=True,
            launch=False,
            benchmark="default",
        )

        run_benchmarks(args)

        definition = schemajson.load(os.path.join(workdir, args.json), "benchmark.schema.json")
        output_dir = os.path.join(workdir, definition["output"])

        results = {}
        versions = []
        dbms = None
        for system in definition["systems"]:
            if dbms is None:
                dbms = system["dbms"]
            elif dbms != system["dbms"]:
                raise Exception(f"System name '{system['dbms']}' in definition does not match the first system.")

            if isinstance(system["parameter"]["version"], list):
                versions.extend(system["parameter"]["version"])
            else:
                versions.append(system["parameter"]["version"])

        benchmark_descriptions = benchmarks()
        for b in definition["benchmarks"]:
            benchmark = benchmark_descriptions[b["name"]].instantiate("data", b)

            with open(os.path.join(output_dir, benchmark.result_name + ".csv"), "r") as csvfile:
                reader = csv.DictReader(csvfile)

                for row in reader:
                    dbms = row["dbms"]
                    version = row["version"]
                    state = row["state"]
                    query = row["query"]

                    if dbms != dbms:
                        raise Exception(f"System name '{dbms}' in output does not match the definition.")

                    if version not in versions:
                        raise Exception(f"Version '{version}' in output does not match the definition.")

                    if state not in ["success", "timeout", "global_timeout"]:
                        raise Exception(f"Unexpected state '{state}' in output.")

                    if version not in results:
                        results[version] = []
                    results[row["version"]].append(query)

        query_list, _ = benchmark.queries(dbms)
        queries = [name for name, _ in query_list]
        for version in results:
            if set(queries) != set(results[version]):
                raise Exception(f"Mismatch between expected queries and results for version '{version}'.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.log_error(e)
        raise e

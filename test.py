#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from types import SimpleNamespace

from benchmark import run_benchmarks
from benchmarks.benchmark import benchmarks
from compare import load_expected, compare_input
from util import schemajson
from util.log import log

workdir = os.getcwd()
csv.field_size_limit(sys.maxsize)


def main():
    parser = argparse.ArgumentParser(description="Test OLAPBench.")
    parser.add_argument("filenames", nargs="*", default=[], help="Optional list of configuration files")
    args = parser.parse_args()

    requested = {os.path.basename(f).split('.')[0] for f in args.filenames}
    yaml_files = [
        f for f in os.listdir(os.path.join(workdir, "test"))
        if f.endswith('.yaml') and (not requested or f.split('.')[0] in requested)
    ]
    if args.filenames and not yaml_files:
        log.error(f"No test files matched: {', '.join(args.filenames)}")
        return False

    succeeded = []
    failed = []
    for file in yaml_files:
        log.header(file)

        try:
            run_test(file)
            succeeded.append(file)
        except Exception as e:
            log.error(f"Test '{file}' failed: {e}")
            failed.append((file, e))

    log.header("Test summary")
    log.info(f"{len(succeeded)}/{len(yaml_files)} tests succeeded")
    for file in succeeded:
        log.info(f"  PASS {file}")
    for file, e in failed:
        log.error(f"  FAIL {file}: {e}")

    return len(failed) == 0


def run_test(file):
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
    expected_dbms = None
    for system in definition["systems"]:
        if expected_dbms is None:
            expected_dbms = system["dbms"]
        elif expected_dbms != system["dbms"]:
            raise Exception(f"System name '{system['dbms']}' in definition does not match the first system.")

        if isinstance(system["parameter"]["version"], list):
            versions.extend(system["parameter"]["version"])
        else:
            versions.append(system["parameter"]["version"])

    benchmark_descriptions = benchmarks()
    total_valid = {}
    total_invalid = {}
    all_mismatches = []
    for b in definition["benchmarks"]:
        benchmark = benchmark_descriptions[b["name"]].instantiate("data", b)
        result_csv = os.path.join(output_dir, benchmark.result_name + ".csv")

        with open(result_csv, "r") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                dbms = row["dbms"]
                version = row["version"]
                state = row["state"]
                query = row["query"]

                if dbms != expected_dbms:
                    raise Exception(f"System name '{dbms}' in output does not match the definition.")

                if version not in versions:
                    raise Exception(f"Version '{version}' in output does not match the definition.")

                if state not in ["success", "timeout", "global_timeout"]:
                    raise Exception(f"Unexpected state '{state}' in output.")

                if version not in results:
                    results[version] = []
                results[row["version"]].append(query)

        # Compare the results against the pre-computed expected results
        expected_dir = os.path.join(workdir, "test", "expected", benchmark.result_name)
        if not os.path.isdir(expected_dir):
            raise Exception(f"No expected results found for '{benchmark.result_name}' (expected at {expected_dir}).")

        expected, queries = load_expected(expected_dir)
        _, _, per_system_valid, per_system_invalid, mismatches = compare_input(result_csv, expected, queries)

        for title, count in per_system_valid.items():
            total_valid[title] = total_valid.get(title, 0) + count
        for title, count in per_system_invalid.items():
            total_invalid[title] = total_invalid.get(title, 0) + count
        all_mismatches.extend(mismatches)

    # Report, per version, how many queries match and mismatch the expectation.
    # Use the `log` console (which owns the comparison progress bar) so the
    # transient progress display is cleared correctly before the header is drawn.
    log.header("Result comparison")
    for title in sorted(total_valid.keys() | total_invalid.keys()):
        match = total_valid.get(title, 0)
        mismatch = total_invalid.get(title, 0)
        log.info(f"{title}: {match} match, {mismatch} mismatch")

    if all_mismatches:
        details = ", ".join(f"{title}: {query}" for title, query in all_mismatches)
        raise Exception(f"{len(all_mismatches)} queries with results differing from the expectation: {details}")

    query_names = benchmark.query_names()

    for version in results:
        if set(query_names) != set(results[version]):
            raise Exception(f"Mismatch between expected queries and results for version '{version}'.")


if __name__ == "__main__":
    try:
        all_passed = main()
    except Exception as e:
        log.error(e)
        raise e
    sys.exit(0 if all_passed else 1)

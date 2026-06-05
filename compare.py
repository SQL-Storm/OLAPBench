#!/usr/bin/env python3
import argparse
import csv
import os
import sys

import simplejson as json

from util.log import log
from util.compare_results import locate_difference, compare_results, smart_open
from validate import Result, validate_queries

csv.field_size_limit(sys.maxsize)


def load_expected(expected_dir):
    """
    Load the pre-computed expectation files from expected_dir.

    Args:
        expected_dir (str): Directory containing valid_queries.csv, invalid_queries.csv and results.csv.gz.

    Returns:
        tuple[dict, set]: A mapping of query to (Result, systems) and the set of all known queries.
    """
    valid_csv = os.path.join(expected_dir, "valid_queries.csv")
    if not os.path.exists(valid_csv):
        raise Exception(f"Valid queries file {valid_csv} does not exist.")

    invalid_csv = os.path.join(expected_dir, "invalid_queries.csv")
    if not os.path.exists(invalid_csv):
        raise Exception(f"Invalid queries file {invalid_csv} does not exist.")

    results_csv = os.path.join(expected_dir, "results.csv.gz")
    if not os.path.exists(results_csv):
        raise Exception(f"Results file {results_csv} does not exist.")

    queries = set()
    valid = []

    log.info(f"Loading the valid queries ...")
    with log.progress(f"Loading the valid queries", total=len(queries)) as progress:
        with smart_open(valid_csv, encoding='utf-8') as f1:
            with smart_open(results_csv, encoding='utf-8') as f2:
                reader1 = csv.DictReader(f1)
                reader2 = csv.DictReader(f2)

                for row1, row2 in zip(reader1, reader2):
                    if row1["query"] != row2["query"]:
                        raise Exception(f"Queries do not match: {row1['query']} != {row2['query']}")
                    progress.description(row1["query"])

                    query = row1["query"]
                    systems = json.loads(row1["systems"])
                    dbms = row2["system"]
                    result = row2["result"]

                    valid.append((Result(dbms, query, result, False, ""), systems))
                    queries.add(query)

                    progress.advance()

    with smart_open(invalid_csv, encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            queries.add(row["query"])

    expected = {}
    for r, systems in valid:
        expected[r.query] = (r, systems)

    return expected, queries


def load_expected_from_results(result_csv):
    """
    Compute the expectation on the fly from a result CSV (validate.py's logic).

    Args:
        result_csv (str): Result file to compute the expected results from.

    Returns:
        tuple[dict, set]: A mapping of query to (Result, systems) and the set of all known queries.
    """
    log.info(f"Loading the expected results from {result_csv} ...")
    valid, invalid = validate_queries(result_csv)

    queries = set()
    for r, _ in valid:
        queries.add(r.query)
    for query, _ in invalid:
        queries.add(query)

    expected = {}
    for r, systems in valid:
        expected[r.query] = (r, systems)

    return expected, queries


def compare_input(input_csv, expected, queries, ignore_decimal_points=False, ignore_microseconds=False):
    """
    Compare the results in input_csv against the expected results.

    Args:
        input_csv (str): Result file to validate.
        expected (dict): Mapping of query to (Result, systems) as returned by load_expected.
        queries (set): The set of all known queries.

    Returns:
        tuple: (valid_count, invalid_count, per_system_valid, per_system_invalid, mismatches).
    """
    valid_count = 0
    invalid_count = 0
    per_system_valid = {}
    per_system_invalid = {}
    mismatches = []

    log.newline()
    log.info(f"Loading the data from {input_csv} ...")
    with log.progress(f"Loading the data", total=len(queries)) as progress:
        with smart_open(input_csv, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                query = row['query']
                progress.description(query)

                if query not in queries:
                    log.warn(f"Query {query} is not in the expected results.", row["title"])
                    progress.advance()
                    continue

                dbms = row['dbms'].strip()
                state = row['state'].strip()
                result = row['result']

                if state == "success":
                    if query not in expected:
                        log.info_verbose(f"Query {query} has no expected results.", row["title"])
                        progress.advance()
                        continue

                    r, systems = expected[query]
                    if not compare_results(r.result, result, ignore_decimal_points=ignore_decimal_points, ignore_microseconds=ignore_microseconds):
                        log.warn(f"Query {query} has different results (computed by {systems[0]}{", " + str(systems[1:]) + " also had a different result" if len(systems) > 1 else ""}).", row["title"])
                        log.warn(locate_difference(r.result, result, r.dbms, dbms, ignore_decimal_points=ignore_decimal_points, ignore_microseconds=ignore_microseconds, columns=row["columns"]), row["title"])
                        invalid_count += 1
                        per_system_invalid[row["title"]] = per_system_invalid.get(row["title"], 0) + 1
                        mismatches.append((row["title"], query))
                    else:
                        log.info_verbose(f"Query {query} has the same results.", row["title"])
                        valid_count += 1
                        per_system_valid[row["title"]] = per_system_valid.get(row["title"], 0) + 1

                progress.advance()

    return valid_count, invalid_count, per_system_valid, per_system_invalid, mismatches


def main():
    log.header("Compare Results")

    """
    Main function to validate query results.
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-b", "--benchmark", default=None, help="The benchmark for which to load the results")
    argparser.add_argument("-d", "--expected-dir", default=None, help="Directory with pre-computed expectation files (overrides --benchmark)")
    argparser.add_argument("-e", "--expected", default=None, help="System to compare")
    argparser.add_argument("--ignore-decimal-points", default=False, action="store_true", help="Ignore decimal point differences")
    argparser.add_argument("--ignore-microseconds", default=False, action="store_true", help="Ignore microsecond differences")
    argparser.add_argument("input", help="Input file")
    args = argparser.parse_args()

    if args.benchmark is None and args.expected_dir is None and args.expected is None:
        raise Exception("Please provide a benchmark, an expectation directory or a file to compute the expected results.")

    input_csv = args.input

    if args.expected_dir is not None:
        expected, queries = load_expected(args.expected_dir)
    elif args.benchmark is not None:
        expected, queries = load_expected(os.path.join("benchmarks", args.benchmark))
    else:
        expected, queries = load_expected_from_results(args.expected)

    if args.ignore_decimal_points:
        log.info("Ignoring decimal point differences.")
    if args.ignore_microseconds:
        log.info("Ignoring microsecond differences.")

    valid_count, invalid_count, per_system_valid, per_system_invalid, _ = compare_input(
        input_csv, expected, queries,
        ignore_decimal_points=args.ignore_decimal_points,
        ignore_microseconds=args.ignore_microseconds,
    )

    log.newline()
    log.info(f"Queries with equal result: {valid_count}")
    log.info(f"Queries with different result: {invalid_count}")

    all_titles = sorted(per_system_valid.keys() | per_system_invalid.keys())
    if all_titles:
        log.newline()
        log.info("Per-system summary:")
        for title in all_titles:
            equal = per_system_valid.get(title, 0)
            different = per_system_invalid.get(title, 0)
            log.info(f"  {title}: {equal} equal, {different} different")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        raise e

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


def main():
    log.header("Compare Results")

    """
    Main function to validate query results.
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-b", "--benchmark", default=None, help="The benchmark for which to load the results")
    argparser.add_argument("-e", "--expected", default=None, help="System to compare")
    argparser.add_argument("--ignore-decimal-points", default=False, action="store_true", help="Ignore decimal point differences")
    argparser.add_argument("--ignore-microseconds", default=False, action="store_true", help="Ignore microsecond differences")
    argparser.add_argument("input", help="Input file")
    args = argparser.parse_args()

    if args.benchmark is None and args.expected is None:
        raise Exception("Please provide a benchmark or a file to compute the expected results.")

    input_csv = args.input

    queries = set()
    if args.benchmark:
        valid_csv = os.path.join("benchmarks", args.benchmark, "valid_queries.csv")
        if not os.path.exists(valid_csv):
            raise Exception(f"Valid queries file {valid_csv} does not exist.")

        invalid_csv = os.path.join("benchmarks", args.benchmark, "invalid_queries.csv")
        if not os.path.exists(invalid_csv):
            raise Exception(f"Invalid queries file {invalid_csv} does not exist.")

        results_csv = os.path.join("benchmarks", args.benchmark, "results.csv.gz")
        if not os.path.exists(results_csv):
            raise Exception(f"Results file {results_csv} does not exist.")

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

    else:
        log.info(f"Loading the expected results from {args.expected} ...")
        valid, invalid = validate_queries(args.expected)

        for r, _ in valid:
            queries.add(r.query)
        for query, _ in invalid:
            queries.add(query)

    expected = {}
    for r, systems in valid:
        expected[r.query] = (r, systems)

    ignore_decimal_points = args.ignore_decimal_points
    ignore_microseconds = args.ignore_microseconds
    if ignore_decimal_points:
        log.info("Ignoring decimal point differences.")
    if ignore_microseconds:
        log.info("Ignoring microsecond differences.")

    # Open and read the CSV file
    log.newline()
    log.info(f"Loading the data from {input_csv} ...")
    valid_count = 0
    invalid_count = 0
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
                    else:
                        log.info_verbose(f"Query {query} has the same results.", row["title"])
                        valid_count += 1

                progress.advance()

    log.newline()
    log.info(f"Queries with equal result: {valid_count}")
    log.info(f"Queries with different result: {invalid_count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        raise e

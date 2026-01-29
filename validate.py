#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from dataclasses import dataclass

import simplejson as json

from util.log import log
from util.compare_results import smart_open, sort_query_list, compare_results

csv.field_size_limit(sys.maxsize)


@dataclass(frozen=True, eq=True)
class Result:
    """
    A structure to represent a result.

    Attributes:
        dbms (str): The database management system.
        query (str): The query.
        result (str): The result of the query.
        error (bool): A flag indicating whether an error occurred.
        message (str): A message providing additional information about the result.
    """
    dbms: str
    query: str
    result: str
    error: bool
    message: str

    def __hash__(self):
        return hash((self.dbms, self.query))


def validate_queries(csv_path):
    """
    Compare results from a CSV file and write the comparison to an output file.

    Args:
        csv_path (str): Path to the input CSV file.
        writer (csv.DictWriter): CSV writer for the output file.

    Returns:
        tuple[list, list]: List of equal queries and all queries.
    """
    # Dictionary to track results for each query
    queries = {}

    # Track the systems
    systems = set()

    # Open and read the CSV file
    log.info("Loading the data...")
    num_rows = 0
    with log.progress("Loading the data", total=0) as progress:
        with smart_open(csv_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                num_rows += 1

                query = row['query']
                progress.description(query)

                if query not in queries:
                    queries[query] = {}

                dbms = row['dbms'].strip()
                state = row['state'].strip()
                result = row['result']
                message = row['message'].strip()

                systems.add(dbms)

                if state == "success":
                    try:
                        r1 = Result(dbms, query, result, False, "")
                    except Exception as e:
                        log.error(f"Error parsing result: {e}\n```\n{result}\n```")
                        raise e
                elif state == "error":
                    r1 = Result(dbms, query, "", True, message)
                else:
                    progress.advance()
                    continue

                if dbms not in queries[query]:
                    queries[query][dbms] = r1
                else:
                    r2 = queries[query][dbms]
                    if not (r1.error == r2.error and compare_results(r1.result, r2.result)):
                        queries[query][dbms] = None

                progress.advance()

    num_systems = len(systems)
    majority = int(num_systems / 2) + 1
    log.info(f"Loaded {num_rows} rows, found {len(queries)} queries and {num_systems} systems, majority of {majority} systems required for equal results.")

    log.info("Comparing the results...")
    valid_queries = []
    invalid_queries = []
    with log.progress("Comparing the results", total=len(queries)) as progress:
        for query in sort_query_list(list(queries.keys())):
            progress.description(query)

            classes = {}
            for d1, r1 in queries[query].items():
                found = False

                for r2 in classes.keys():
                    if r1 is None or r2 is None:
                        # Both queries are not deterministic
                        if r1 is None and r2 is None:
                            found = True
                            classes[r2].append(d1)
                            break

                    # Check if result is equal
                    elif r1.error == r2.error and compare_results(r1.result, r2.result):
                        found = True
                        classes[r2].append(d1)
                        break

                if not found:
                    assert r1 not in classes
                    classes[r1] = [d1]

            # The majority of systems produces the same result
            if len(classes.keys()) == 1:
                r = list(classes.keys())[0]
                systems = classes[r]
                if r is None:
                    systems = [[s] for s in systems]
                    log.info_verbose(f"Query {query} has non-deterministic results for all systems: {systems}")
                    invalid_queries.append((query, systems))
                elif len(classes[r]) >= (num_systems - 1):
                    if r.error:
                        log.info_verbose(f"Query {query} has error for all systems: {systems}")
                        invalid_queries.append((query, [systems]))
                    else:
                        log.info_verbose(f"Query {query} has equal results for all systems: {systems}")
                        valid_queries.append((r, [systems]))
                else:
                    log.info_verbose(f"Query {query} has not enough successful system: {systems}")
                    invalid_queries.append((query, [systems]))

            elif any(len(systems) >= 2 for _, systems in classes.items()):
                correct_result = [r for r, systems in classes.items() if len(systems) >= 2][0]
                correct_systems = list(sorted(classes[correct_result]))
                incorrect_systems = list(sorted([list(sorted(systems)) for r, systems in classes.items() if len(systems) < 2]))
                if correct_result is None:
                    systems = [[s] for s in correct_systems] + incorrect_systems
                    log.info_verbose(f"Query {query} has non-deterministic results: {systems}")
                    invalid_queries.append((query, systems))
                elif correct_result.error:
                    log.warn(f"Query {query} has error for the majority of systems: {correct_systems} vs. {incorrect_systems}")
                    invalid_queries.append((query, [correct_systems] + incorrect_systems))
                else:
                    log.warn(f"Query {query} has equal results for the majority of systems: {correct_systems} vs. {incorrect_systems}")
                    valid_queries.append((correct_result, [correct_systems] + incorrect_systems))

            else:
                systems = list(sorted([list(sorted(systems)) for r, systems in classes.items()]))
                log.info_verbose(f"Query {query} has different results: {systems}")
                invalid_queries.append((query, systems))

            progress.advance()

    log.info(f"Found {len(valid_queries)} valid queries and {len(invalid_queries)} invalid queries.")

    return valid_queries, invalid_queries


def main():
    """
    Main function to validate query results.
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument("benchmark", help="Dataset of the benchmark")
    argparser.add_argument("result", help="Result file")

    args = argparser.parse_args()
    result_csv = args.result

    log.header("Validate Query Results")

    valid, invalid = validate_queries(result_csv)

    valid_csv = os.path.join("benchmarks", args.benchmark, "valid_queries.csv")
    invalid_csv = os.path.join("benchmarks", args.benchmark, "invalid_queries.csv")
    results_csv = os.path.join("benchmarks", args.benchmark, "results.csv.gz")

    log.newline()
    log.info(f"Writing valid queries to {valid_csv}")
    with log.progress("Writing valid queries", total=len(valid)) as progress:
        with smart_open(valid_csv, 'wt', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["query", "systems"])
            writer.writeheader()

            for r, systems in valid:
                progress.description(r.query)
                writer.writerow({
                    "query": r.query,
                    "systems": json.dumps(systems)
                })
                progress.advance()

    log.info(f"Writing results to {results_csv}")
    with log.progress("Writing results", total=len(valid)) as progress:
        with smart_open(results_csv, 'wt', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["query", "system", "result"])
            writer.writeheader()

            for r, _ in valid:
                progress.description(r.query)
                writer.writerow({
                    "query": r.query,
                    "system": r.dbms,
                    "result": r.result
                })
                progress.advance()

    log.info(f"Writing invalid queries to {invalid_csv}")
    with log.progress("Writing invalid queries", total=len(invalid)) as progress:
        with smart_open(invalid_csv, 'wt', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["query", "systems"])
            writer.writeheader()

            for query, systems in invalid:
                progress.description(query)
                writer.writerow({
                    "query": query,
                    "systems": json.dumps(systems)
                })
                progress.advance()

    log.newline()
    log.info(f"Queries with equal results: {len(valid)} / {len(valid) + len(invalid)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        raise e

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import math
import os
import random
import sys
from dataclasses import dataclass, field
from statistics import median, geometric_mean
from typing import Dict, List

import simplejson as json
from dotenv import load_dotenv

from benchmarks.benchmark import benchmark_arguments, benchmarks, Benchmark
from dbms.dbms import Result, database_systems
from util import formatter, schemajson
from util.log import log
from util.resultcsv import ResultCSV
from util.template import Template, unfold

workdir = os.getcwd()
csv.field_size_limit(sys.maxsize)


@dataclass
class System:
    title: str
    dbms: str
    params: dict
    settings: dict


@dataclass
class Runtime:
    title: str

    queries: int = 0
    success: int = 0
    error: int = 0
    fatal: int = 0
    oom: int = 0
    timeout: int = 0
    global_timeout: int = 0

    global_time: float = 0
    times: List[float] = field(default_factory=lambda: [])


def _enabled(value) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "on", "target"}
    return bool(value)


def _statistics_name(name) -> str:
    name = str(name)
    if (name.startswith('"') and name.endswith('"')) or (name.startswith('`') and name.endswith('`')):
        return name[1:-1]
    return name


def _expected_statistics_tables(benchmark: Benchmark) -> set[str]:
    schema = benchmark.get_schema(primary_key=False, foreign_keys=False)
    return {
        _statistics_name(table["name"])
        for table in schema["tables"]
        if table.get("_eval", True) and not table.get("initially empty", False)
    }


def _collected_statistics_tables(statistics: str) -> set[str]:
    try:
        payload = json.loads(statistics)
    except Exception as e:
        log.warn(f"Could not parse Umbra statistics string as JSON: {e}")
        return set()

    tables = payload.get("tables", [])
    if not isinstance(tables, list):
        log.warn("Umbra statistics string does not contain a table list")
        return set()

    return {
        _statistics_name(table["table"])
        for table in tables
        if isinstance(table, dict) and "table" in table
    }


def _warn_missing_statistics_tables(benchmark: Benchmark, statistics: str):
    expected_tables = _expected_statistics_tables(benchmark)
    collected_tables = _collected_statistics_tables(statistics)
    missing_tables = sorted(expected_tables - collected_tables)
    if missing_tables:
        preview = ", ".join(missing_tables[:10])
        suffix = "" if len(missing_tables) <= 10 else f", ... ({len(missing_tables)} total)"
        log.warn(f"Umbra statistics are missing benchmark tables: {preview}{suffix}")


def _plan_queries_with_umbra(benchmark: Benchmark, system: System, query_names: list[str], dbms_descriptions: dict,
                             db_dir: str, data_dir: str, target_dbms=None) -> list[tuple[str, str]]:
    use_target_statistics = _enabled(system.params.get("umbra_planner_statistics", False))
    if use_target_statistics and target_dbms is None:
        raise Exception("Umbra planner target statistics require a loaded target DBMS")

    log.driver("Using Umbra planner" + (" with target statistics" if use_target_statistics else ""))
    umbra_planner_params = dict(system.params.get("umbra_planner_parameter", {}))
    umbra_planner_settings = dict(system.params.get("umbra_planner_settings", {}))
    statistics = None
    umbra_planned_queries = []

    if use_target_statistics:
        umbra_planner_params["umbra_schema_only"] = True

    with dbms_descriptions["umbradev"].instantiate(benchmark, db_dir, data_dir, umbra_planner_params, umbra_planner_settings) as umbra:
        if use_target_statistics:
            umbra.load_schema()

            stats_query = umbra.statsql_query(system.dbms)
            log.sql_verbose(stats_query)
            log.driver(f"Collecting {system.dbms} statistics for Umbra planner")
            stats_timeout = system.params.get("umbra_planner_statistics_timeout", 0) or 0
            statistics = str(target_dbms.execute_scalar(stats_query, timeout=stats_timeout))
            _warn_missing_statistics_tables(benchmark, statistics)
        else:
            umbra.load_database()

        with log.progress("Planning queries...", len(query_names)) as progress:
            for name in query_names:
                progress.next(f'Planning {name}...')
                query = benchmark.read_query(name, "umbra")
                umbra_query = umbra.plan_query(query, system.dbms, statistics=statistics)
                if umbra_query is not None:
                    umbra_planned_queries.append((name, umbra_query))
                else:
                    log.warn_verbose(f"Query {name} not supported by Umbra")
                progress.finish()

    return umbra_planned_queries


def run_benchmark(benchmark: Benchmark, systems: List[System], definition: dict, result_dir: str, db_dir: str, data_dir: str):
    log.driver(f"Preparing {benchmark.fullname}")
    dbms_descriptions = database_systems()

    timeout = definition.get("timeout", 0)
    global_timeout = definition.get("global_timeout", 0) * 1000
    fetch_result = definition.get("fetch_result", True)
    fetch_result_limit = definition.get("fetch_result_limit", 0)
    query_seed = definition.get("query_seed", None)

    benchmark.dbgen()

    result_name = os.path.join(result_dir, benchmark.result_name)
    result_csv = result_name + ".csv"
    executed_queries = {}
    failed_query = (None, None)
    benchmark_type = definition.get("type", "queries")

    if definition.get("clear", False):
        clear(benchmark, result_dir)

    runtimes: Dict[Runtime] = {}
    for system in systems:
        runtimes[system.title] = Runtime(title=system.title)
        executed_queries[system.title] = set()

    if os.path.exists(result_csv) and benchmark_type == "queries":
        log.driver(f"Found results in {result_csv}, skipping already executed queries")
        with open(result_csv, 'r') as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                title = row["title"]
                query = row["query"]
                state = row["state"]
                times = [float(x) for x in json.loads(row["client_total"], allow_nan=True)]

                if title not in runtimes:
                    continue

                executed_queries[title].add(query)

                runtimes[title].queries += 1
                if state not in [Result.FATAL, Result.GLOBAL_TIMEOUT]:
                    assert len(times) > 0
                    runtimes[title].global_time += median(times)
                    runtimes[title].times.append(median(times))

                match state:
                    case Result.SUCCESS:
                        runtimes[title].success += 1
                    case Result.ERROR:
                        runtimes[title].error += 1
                    case Result.FATAL:
                        runtimes[title].fatal += 1
                    case Result.OOM:
                        runtimes[title].oom += 1
                    case Result.TIMEOUT:
                        runtimes[title].timeout += 1
                    case Result.GLOBAL_TIMEOUT:
                        runtimes[title].global_timeout += 1

    if os.path.exists(result_csv + "_current") and benchmark_type == "queries":
        with open(result_csv + "_current", 'r') as file:
            title, query = file.read().strip().split("\n")
            failed_query = (title, query)
            log.driver(f"Last execution of {query} failed in {title}")

    with ResultCSV(result_csv, append=True) as result_csv_file:
        for system in systems:
            log.header(system.title)
            log.driver(f"Running {system.title} on {benchmark.result_name} (dbms: {system.dbms}, params: {system.params}, settings: {system.settings})")

            # Prepare the benchmark
            match benchmark_type:
                case "queries":
                    umbra_planner = _enabled(system.params.get("umbra_planner", False))
                    dbms_name = "umbra" if umbra_planner else system.dbms
                    use_target_statistics = umbra_planner and _enabled(system.params.get("umbra_planner_statistics", False))
                    umbra_planned_queries = []

                    log.driver(f"Loading query names...")
                    query_names = benchmark.query_names()
                    log.driver(f"Found {len(query_names)} queries")

                    # Shuffle the queries
                    if query_seed is not None:
                        random.seed(query_seed)
                        random.shuffle(query_names)

                    # Filter out executed queries
                    if system.title in executed_queries:
                        query_names = [name for name in query_names if name not in executed_queries[system.title]]

                    # Plan the queries with Umbra
                    # When planning is enabled, all planned queries are loaded into a list in memory for now
                    if umbra_planner and len(query_names) != 0 and not use_target_statistics:
                        umbra_planned_queries = _plan_queries_with_umbra(benchmark, system, query_names, dbms_descriptions, db_dir, data_dir)

                    if len(query_names) == 0:
                        runtime = runtimes[system.title]
                        rsum = formatter.format_time(sum(runtime.times))
                        rgeomean = formatter.format_time(math.nan if len(runtime.times) == 0 else geometric_mean(runtime.times))
                        rmedian = formatter.format_time(math.nan if len(runtime.times) == 0 else median(runtime.times))

                        log.driver(
                            f"total runtime {rsum} (geomean: {rgeomean}, median: {rmedian}) of {runtime.queries} queries (success: {runtime.success}, error: {runtime.error}, fatal: {runtime.fatal}, oom: {runtime.oom}, timeout: {runtime.timeout}, global timeout: {runtime.global_timeout})")
                        continue

            with dbms_descriptions[system.dbms].instantiate(benchmark, db_dir, data_dir, system.params, system.settings) as dbms:
                dbms.load_database()

                if benchmark_type == "queries":
                    if umbra_planner and len(query_names) != 0 and use_target_statistics:
                        umbra_planned_queries = _plan_queries_with_umbra(benchmark, system, query_names, dbms_descriptions, db_dir, data_dir, target_dbms=dbms)

                    log.driver("Benchmarking queries")

                    repetitions = definition["repetitions"]
                    warmup = definition["warmup"]

                    use_lazy = not umbra_planner
                    query_list = query_names if use_lazy else umbra_planned_queries
                    num_queries = len(query_list)

                    with log.progress("Running queries...", num_queries * (repetitions + warmup), base=repetitions + warmup) as progress:
                        for item in query_list:
                            if use_lazy:
                                name = item
                                query = benchmark.read_query(name, dbms_name)
                            else:
                                name, query = item
                            result = Result()

                            if system.title == failed_query[0] and name == failed_query[1]:
                                # Fatal error in the last execution of the query
                                result.state = Result.FATAL
                                result.message = "olapbench: system crash!"
                            elif runtimes[system.title].global_time > global_timeout and global_timeout > 0:
                                # Global timeout reached
                                result.state = Result.GLOBAL_TIMEOUT
                                result.message = "olapbench: global timeout!"

                            result_csv_file.start_olap(system.title, name)

                            progress.next(f'Running {name}...')
                            if result.state == Result.SUCCESS:
                                for i in range(warmup):
                                    dbms._execute(query, fetch_result, timeout=timeout, fetch_result_limit=fetch_result_limit)
                                    progress.finish()

                                for i in range(repetitions):
                                    result.merge(dbms._execute(query, fetch_result, timeout=timeout, fetch_result_limit=fetch_result_limit))
                                    progress.finish()

                            med = median(result.client_total) if len(result.client_total) > 0 else math.nan
                            if not math.isnan(med):
                                runtimes[system.title].global_time += med

                            if runtimes[system.title].global_time > global_timeout and global_timeout > 0:
                                result = Result()
                                result.state = Result.GLOBAL_TIMEOUT
                                med = math.nan

                            query_plan = definition.get("query_plan", {})
                            retrieve_query_plan = query_plan.get("retrieve", False)
                            if retrieve_query_plan and result.state == Result.SUCCESS:
                                system_representation = query_plan.get("system_representation", False)
                                result.plan = dbms.retrieve_query_plan(query, include_system_representation=system_representation, timeout=timeout)

                            result.round(3)
                            result_csv_file.olap(system.title, system.dbms, dbms.version, name, result)

                            lname = name.ljust(10)
                            lmessage = ""
                            match result.state:
                                case Result.SUCCESS:
                                    lmessage = "success (" + str(result.rows) + " rows)"
                                    runtimes[system.title].success += 1
                                case Result.ERROR:
                                    lmessage = "error (" + result.message.replace("\n", " ")[:40] + ")"
                                    runtimes[system.title].error += 1
                                case Result.FATAL:
                                    lmessage = "fatal error"
                                    runtimes[system.title].fatal += 1
                                case Result.OOM:
                                    lmessage = "out of memory"
                                    runtimes[system.title].oom += 1
                                case Result.TIMEOUT:
                                    lmessage = "query timeout"
                                    runtimes[system.title].timeout += 1
                                case Result.GLOBAL_TIMEOUT:
                                    lmessage = "global timeout"
                                    runtimes[system.title].global_timeout += 1

                            runtimes[system.title].queries += 1
                            if result.state not in [Result.ERROR, Result.FATAL, Result.GLOBAL_TIMEOUT]:
                                assert not math.isnan(med)
                                runtimes[system.title].times.append(med)

                            log.dbms_verbose(f'{lname} {formatter.format_time(med)} {lmessage}', dbms)

                    runtime = runtimes[system.title]
                    rsum = formatter.format_time(sum(runtime.times))
                    rgeomean = formatter.format_time(math.nan if len(runtime.times) == 0 else geometric_mean(runtime.times))
                    rmedian = formatter.format_time(math.nan if len(runtime.times) == 0 else median(runtime.times))

                    log.driver(
                        f"total runtime {rsum} (geomean: {rgeomean}, median: {rmedian}) of {runtime.queries} queries (success: {runtime.success}, error: {runtime.error}, fatal: {runtime.fatal}, oom: {runtime.oom}, timeout: {runtime.timeout}, global timeout: {runtime.global_timeout})")

                elif benchmark_type == "launch":
                    log.dbms(f"Connect to {system.title} using `{dbms.connection_string()}`", dbms)
                    input("Press Enter to continue...")

                else:
                    raise ValueError("benchmark type not supported")


def clear(benchmark: Benchmark, result_dir: str):
    """
    Deletes result files associated with the given benchmark.

    Args:
        benchmark (Benchmark): The benchmark object containing the unique name used to identify the result files.
    """

    def delete_file(file_path):
        try:
            os.remove(file_path)
        except FileNotFoundError:
            pass
        except Exception as e:
            log.warn_verbose(f"Failed to delete {file_path}: {e}")

    result_name = os.path.join(result_dir, benchmark.result_name)
    log.driver(f"Clearing results for {result_name}")

    files_to_delete = [result_name + ext for ext in [".csv", ".csv_current"]]
    for file_path in files_to_delete:
        delete_file(file_path)


def run_benchmarks(args):
    benchmark_descriptions = benchmarks()

    if args.env is not None:
        load_dotenv(dotenv_path=args.env, verbose=True)

    log.set_verbose(args.verbose)
    log.set_very_verbose(args.very_verbose)

    definition = schemajson.load(os.path.join(workdir, args.json), "benchmark.schema.json")

    result_dir = os.path.join(workdir, definition["output"])
    db_dir = os.path.join(workdir, args.db)
    data_dir = os.path.join(workdir, args.data)

    os.makedirs(result_dir, exist_ok=True)
    os.makedirs(db_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    systems: List[System] = []
    for system in definition["systems"]:
        if "disabled" in system and system["disabled"]:
            continue

        params: dict = {}
        if "parameter" in system.keys():
            params = system["parameter"]

        if "parameter" in definition.keys():
            for key in definition["parameter"].keys():
                if key not in params.keys():
                    params[key] = definition["parameter"][key]

        settings: dict = {}
        if "settings" in system.keys():
            settings = system["settings"]

        if "settings" in definition.keys():
            for key in definition["settings"].keys():
                if key not in settings.keys():
                    settings[key] = definition["settings"][key]

        for params in unfold(params):
            for settings in unfold(settings):
                # fill the title
                template = Template(system["title"])
                title = template.substitute(**settings, **params)

                systems.append(System(title, system["dbms"], params, settings))

    # Resolve sample_base (a system title) into the base system's params dict, so
    # umbradev can copy the base's .sample files without needing the full systems list.
    for s in systems:
        base_title = s.params.get("sample_base")
        if not isinstance(base_title, str):
            continue
        if base_title == s.title:
            del s.params["sample_base"]
            continue
        base = next((x for x in systems if x.title == base_title), None)
        if base is None:
            log.warn(f"sample_base: '{base_title}' not found in systems list")
            del s.params["sample_base"]
        elif base.dbms != "umbradev":
            log.warn(f"sample_base: '{base_title}' is not a umbradev system")
            del s.params["sample_base"]
        else:
            s.params["sample_base"] = base.params

    definition["type"] = "launch" if args.launch else "queries"
    definition["clear"] = args.clear

    if args.benchmark == "default":
        for bs in definition["benchmarks"]:
            queries = None if "queries" not in bs else bs["queries"]
            excluded_queries = None if "excluded_queries" not in bs else bs["excluded_queries"]
            bs["queries"] = None
            for b in unfold(bs):
                if "disabled" in b and b["disabled"]:
                    continue

                benchmark = benchmark_descriptions[b["name"]].instantiate(data_dir, b, included_queries=queries, excluded_queries=excluded_queries)
                run_benchmark(benchmark, systems, definition, result_dir, db_dir, data_dir)
    else:
        benchmark = benchmark_descriptions[args.benchmark].instantiate(data_dir, vars(args))
        run_benchmark(benchmark, systems, definition, result_dir, db_dir, data_dir)


def main():
    log.header("OLAPBench")

    if not os.getenv("VIRTUAL_ENV"):
        log.warn(f"Activate the venv first:\n   source {os.path.dirname(os.path.realpath(__file__))}/.venv/bin/activate")

    parser = argparse.ArgumentParser(description="Run a benchmark")
    parser.add_argument("-j", "--json", dest="json", required=True, type=str, help="path to the benchmark's json definition")
    parser.add_argument("-v", "--verbose", dest="verbose", default=False, action="store_true", help="verbose output")
    parser.add_argument("-vv", "--very-verbose", dest="very_verbose", default=False, action="store_true", help="very verbose output")
    parser.add_argument("--db", dest="db", type=str, default="db", help="directory where to store the databases (default: ./db)")
    parser.add_argument("--data", dest="data", type=str, default="data", help="directory where to store the data (default: ./data)")
    parser.add_argument("--env", dest="env", type=str, default=None, help="file containing environment variables")
    parser.add_argument("--clear", dest="clear", default=False, action="store_true", help="Clear the results")
    parser.add_argument("--launch", default=False, action="store_true", help="Only launch the database without running any queries")
    benchmark_arguments(parser)

    args = parser.parse_args()

    run_benchmarks(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(e)
        raise e

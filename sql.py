#!/usr/bin/env python3
import argparse
import ast
import atexit
import math
import os
from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.table import Table

from benchmarks.benchmark import benchmark_arguments, benchmarks
from dbms.dbms import DBMS, Result, database_systems


@dataclass
class ReplConfig:
    repeats: int
    fetch_result: bool
    timeout: int
    fetch_result_limit: int


console = Console()
HISTORY_FILE = os.path.expanduser("~/.olapbench_sql_history")


def _setup_history():
    try:
        import readline

        readline.set_history_length(5000)
        if os.path.exists(HISTORY_FILE):
            readline.read_history_file(HISTORY_FILE)
        atexit.register(readline.write_history_file, HISTORY_FILE)
    except Exception:
        # History is optional; continue without it if readline is unavailable.
        pass


def _parse_key_value(items: list[str] | None) -> dict[str, Any]:
    if not items:
        return {}

    parsed: dict[str, Any] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Invalid key=value argument: {item}")
        key, raw_value = item.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        parsed[key] = _parse_value(raw_value)
    return parsed


def _parse_value(raw_value: str) -> Any:
    lower = raw_value.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False

    try:
        return int(raw_value)
    except ValueError:
        pass

    try:
        return float(raw_value)
    except ValueError:
        pass

    try:
        return ast.literal_eval(raw_value)
    except Exception:
        return raw_value


def _normalize_special_params(dbms_name: str, params: dict[str, Any]) -> dict[str, Any]:
    if "index" in params and isinstance(params["index"], str):
        params["index"] = DBMS.Index.from_string(params["index"])

    if dbms_name in {"umbra", "umbradev"}:
        from dbms.umbra import Umbra

        if "relation" in params and isinstance(params["relation"], str):
            params["relation"] = Umbra.Relation.from_string(params["relation"])
        if "backend" in params and isinstance(params["backend"], str):
            params["backend"] = Umbra.Backend.from_string(params["backend"])
        if "indexMethod" in params and isinstance(params["indexMethod"], str):
            params["indexMethod"] = Umbra.IndexMethod.from_string(params["indexMethod"])

    return params


def _format_ms(value: float) -> str:
    if math.isnan(value):
        return "nan"
    return f"{value:.2f} ms"


def _get_benchmark_query(queries: list[tuple[str, str]], identifier: str) -> tuple[str, str]:
    index: int | None = None
    try:
        index = int(identifier)
    except ValueError:
        pass

    if index is not None:
        if index < 1 or index > len(queries):
            raise ValueError(f"Benchmark query index out of range: {identifier}")
        return queries[index - 1]

    for name, query in queries:
        if name == identifier:
            return name, query

    raise ValueError(f"Unknown benchmark query: {identifier}")


def _print_result_table(columns: list[Any], rows: list[list[Any]]):
    if not rows:
        print("Result: <empty>")
        return

    headers = [str(c) for c in columns] if columns else [f"col{i + 1}" for i in range(len(rows[0]))]
    width_count = max(len(headers), max((len(r) for r in rows), default=0))
    if len(headers) < width_count:
        headers.extend([f"col{i + 1}" for i in range(len(headers), width_count)])

    table = Table(show_header=True, header_style="bold")
    for header in headers:
        table.add_column(header)

    for row in rows:
        rendered = ["NULL" if v is None else str(v) for v in row]
        if len(rendered) < width_count:
            rendered.extend([""] * (width_count - len(rendered)))
        table.add_row(*rendered)

    console.print(table)


def _execute_query(dbms, query_name: str, query_text: str, config: ReplConfig):
    result = Result()

    for _ in range(config.repeats):
        run_result = dbms._execute(
            query_text,
            fetch_result=config.fetch_result,
            timeout=config.timeout,
            fetch_result_limit=config.fetch_result_limit,
        )
        result.merge(run_result)

    total_runtime = sum(result.client_total) if result.client_total else math.nan

    print()

    if result.state != Result.SUCCESS and result.message:
        print(f"Error: {result.message}")

    if config.fetch_result and result.state == Result.SUCCESS:
        _print_result_table(result.columns, result.result)

    runs_text = f"runs={config.repeats}"
    runtime_text = f"total={_format_ms(total_runtime)}"
    state_text = f"state={result.state}"
    query_text_summary = f"query={query_name}"
    rows_value = result.rows if result.rows is not None else (len(result.result) if result.result else 0)
    rows_text = f"rows={rows_value}" if config.fetch_result and result.state == Result.SUCCESS else "rows=n/a"
    per_run_text = f"per_run=[{', '.join(_format_ms(t) for t in result.client_total)}]" if result.client_total else "per_run=[]"

    print(f"{query_text_summary} | {state_text} | {runtime_text} | {runs_text} | {rows_text} | {per_run_text}")


def _format_cardinality(value: Any) -> str:
    return "?" if value is None else str(value)


def _format_operator_details(operator: Any) -> str:
    details: list[str] = []
    for key, value in vars(operator).items():
        if key in {"operator_type", "operator_id"} or value is None:
            continue
        details.append(f"{key}={value}")
    return ", ".join(details)


def _print_plan_tree(node: Any, prefix: str = "", is_last: bool = True):
    connector = "└─>" if is_last else "├─>"

    operator = node.operator
    op_type = operator.operator_type.name
    op_id = operator.operator_id
    est = _format_cardinality(node.estimated_cardinality)
    exact = _format_cardinality(node.exact_cardinality)
    details = _format_operator_details(operator)
    details_text = f", {details}" if details else ""

    print(f"{prefix}{connector} {op_type} [id={op_id}, exact={exact}, est={est}{details_text}]")

    children = getattr(node, "children", [])
    for i, child in enumerate(children):
        child_is_last = i == len(children) - 1
        child_prefix = prefix + ("   " if is_last else "│  ")
        _print_plan_tree(child, child_prefix, child_is_last)


def _explain_query(dbms, query_name: str, query_text: str, timeout: int):
    print(f"\nExplain: {query_name}")
    query_plan = dbms.retrieve_query_plan(query_text, include_system_representation=True, timeout=timeout)

    if query_plan is None or getattr(query_plan, "plan", None) is None:
        print("No plan available for this system/query.")
        return

    print("Plan:")
    _print_plan_tree(query_plan.plan)


def _resolve_explain_input(value: str, queries: list[tuple[str, str]]) -> tuple[str, str]:
    value = value.strip()
    if value.startswith("\\run "):
        identifier = value[len("\\run "):].strip()
        return _get_benchmark_query(queries, identifier)
    return "ad_hoc", value


def _print_help():
    print("""
Commands:
  \\help                      Show this help
  \\list                      List benchmark queries
  \\run <index|name>          Run benchmark query by 1-based index or filename
  \\explain <sql|\\run ...>    Explain a SQL query or benchmark query
  \\repeat <n>                Set repeats
  \\fetch <on|off>            Enable or disable result extraction
  \\timeout <seconds>         Set query timeout in seconds (0 = disabled)
  \\limit <n>                 Set fetch result row limit (0 = unlimited)
  \\config                    Show current config
  \\quit                      Exit

Any non-command input is treated as SQL and executed.
Multi-line SQL is supported (input ends when a line ends with ';').
""".strip())


def _print_config(config: ReplConfig):
    print(
        f"Config: repeats={config.repeats}, fetch_result={config.fetch_result}, "
        f"timeout={config.timeout}s, fetch_result_limit={config.fetch_result_limit}"
    )


def main():
    parser = argparse.ArgumentParser(description="Interactive SQL REPL for OLAPBench systems")
    parser.add_argument("--dbms", required=True, choices=sorted(database_systems().keys()), help="Database system")
    parser.add_argument("--db", dest="db_dir", type=str, default="db", help="Database directory (default: ./db)")
    parser.add_argument("--data", dest="data_dir", type=str, default="data", help="Data directory (default: ./data)")
    parser.add_argument("--param", action="append", default=[], help="DBMS parameter in key=value format (repeatable)")
    parser.add_argument("--setting", action="append", default=[], help="DBMS setting in key=value format (repeatable)")
    parser.add_argument("--repeats", type=int, default=1, help="Number of repetitions per query (default: 1)")
    parser.add_argument("--timeout", type=int, default=0, help="Query timeout in seconds (default: 0 = disabled)")
    parser.add_argument("--fetch-result", action=argparse.BooleanOptionalAction, default=True, help="Fetch and print query result")
    parser.add_argument("--fetch-result-limit", type=int, default=0, help="Limit fetched rows (default: 0 = unlimited)")
    parser.add_argument("--query", type=str, default=None, help="Run this ad-hoc SQL query once before entering REPL")
    parser.add_argument("--benchmark-query", type=str, default=None, help="Run benchmark query by index or name before entering REPL")
    parser.add_argument("--no-repl", action="store_true", help="Exit after optional --query/--benchmark-query")

    benchmark_arguments(parser)
    args = parser.parse_args()

    _setup_history()

    if args.repeats < 1:
        raise ValueError("--repeats must be >= 1")
    if args.timeout < 0:
        raise ValueError("--timeout must be >= 0")
    if args.fetch_result_limit < 0:
        raise ValueError("--fetch-result-limit must be >= 0")

    params = _parse_key_value(args.param)
    settings = _parse_key_value(args.setting)
    params = _normalize_special_params(args.dbms, params)

    workdir = os.getcwd()
    db_dir = os.path.join(workdir, args.db_dir)
    data_dir = os.path.join(workdir, args.data_dir)

    benchmark_map = benchmarks()
    benchmark_description = benchmark_map[args.benchmark]
    benchmark = benchmark_description.instantiate(data_dir, vars(args))

    dbms_descriptions = database_systems()
    dbms_description = dbms_descriptions[args.dbms]

    queries, _ = benchmark.queries(args.dbms)

    print(f"Preparing benchmark data for {benchmark.fullname}...")
    benchmark.dbgen()

    config = ReplConfig(
        repeats=args.repeats,
        fetch_result=args.fetch_result,
        timeout=args.timeout,
        fetch_result_limit=args.fetch_result_limit,
    )

    os.makedirs(db_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    with dbms_description.instantiate(benchmark, db_dir, data_dir, params, settings) as dbms:
        print("Loading database...")
        dbms.load_database()

        print("Connected.")
        print(f"Benchmark: {benchmark.description}")
        print(f"DBMS: {args.dbms}")
        print(f"Loaded benchmark queries: {len(queries)}")

        if args.benchmark_query is not None:
            name, query = _get_benchmark_query(queries, args.benchmark_query)
            _execute_query(dbms, name, query, config)

        if args.query is not None:
            _execute_query(dbms, "ad_hoc", args.query, config)

        if args.no_repl:
            return

        _print_help()
        _print_config(config)

        while True:
            line = input("sql> ").strip()
            if not line:
                continue

            if line.startswith("\\"):
                parts = line.split(maxsplit=1)
                command = parts[0]
                value = parts[1] if len(parts) > 1 else None

                if command in {"\\quit", "\\exit"}:
                    break
                if command == "\\help":
                    _print_help()
                    continue
                if command == "\\list":
                    for i, (name, _) in enumerate(queries, start=1):
                        print(f"{i:>3}: {name}")
                    continue
                if command == "\\run":
                    if value is None:
                        print("Usage: \\run <index|name>")
                        continue
                    try:
                        name, query = _get_benchmark_query(queries, value)
                        _execute_query(dbms, name, query, config)
                    except Exception as e:
                        print(f"Error: {e}")
                    continue
                if command == "\\explain":
                    if value is None:
                        print("Usage: \\explain <sql|\\run <index|name>>")
                        continue
                    try:
                        name, query = _resolve_explain_input(value, queries)
                        _explain_query(dbms, name, query, config.timeout)
                    except Exception as e:
                        print(f"Error: {e}")
                    continue
                if command == "\\repeat":
                    if value is None:
                        print("Usage: \\repeat <n>")
                        continue
                    config.repeats = max(1, int(value))
                    _print_config(config)
                    continue
                if command == "\\fetch":
                    if value not in {"on", "off"}:
                        print("Usage: \\fetch <on|off>")
                        continue
                    config.fetch_result = value == "on"
                    _print_config(config)
                    continue
                if command == "\\timeout":
                    if value is None:
                        print("Usage: \\timeout <seconds>")
                        continue
                    config.timeout = max(0, int(value))
                    _print_config(config)
                    continue
                if command == "\\limit":
                    if value is None:
                        print("Usage: \\limit <n>")
                        continue
                    config.fetch_result_limit = max(0, int(value))
                    _print_config(config)
                    continue
                if command == "\\config":
                    _print_config(config)
                    continue

                print(f"Unknown command: {command}. Try \\help")
                continue

            sql_lines = [line]
            while not sql_lines[-1].rstrip().endswith(";"):
                sql_lines.append(input("... "))
            query_text = "\n".join(sql_lines)

            try:
                _execute_query(dbms, "ad_hoc", query_text, config)
            except Exception as e:
                print(f"Error: {e}")


if __name__ == "__main__":
    main()

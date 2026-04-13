#!/usr/bin/env python3
"""
HTTP server for interactive OLAP benchmark querying.

Starts one or more DBMS instances and exposes HTTP endpoints to execute
queries against them with optional query plan retrieval.
"""
from __future__ import annotations

import argparse
import atexit
import os
import sys
import threading
from typing import Dict, Optional

from dotenv import load_dotenv
from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS

from benchmarks.benchmark import benchmark_arguments, benchmarks, Benchmark
from dbms.dbms import Result, database_systems, DBMS
from queryplan.queryplan import encode_query_plan
from util import logger, schemajson, sql
from util.template import Template, unfold

workdir = os.getcwd()
load_dotenv()

FRONTEND_BUILD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'frontend', 'build')

app = Flask(__name__, static_folder=FRONTEND_BUILD_DIR, static_url_path='')
CORS(app)  # Enable CORS for all routes

# Global state
active_dbms: Dict[tuple[str, str], DBMS] = {}  # (dataset_name, title) -> DBMS
dbms_locks: Dict[tuple[str, str], threading.Lock] = {}  # (dataset_name, title) -> Lock
benchmark_instances: Dict[str, Benchmark] = {}  # dataset_name -> Benchmark
optimizer_dbms_name: Dict[str, Optional[str]] = {}  # dataset_name -> optimizer title
dbms_lock = threading.Lock()  # Lock for modifying the above dicts
dbms_restart_configs: Dict[tuple[str, str], dict] = {}  # (dataset_name, title) -> restart config


def cleanup_dbms():
    """Clean up all active DBMS instances on shutdown."""
    with dbms_lock:
        for (dataset_name, title), dbms in active_dbms.items():
            try:
                logger.log_driver(f"Shutting down {dataset_name}/{title}...")
                dbms.__exit__(None, None, None)
            except Exception as e:
                logger.log_error(f"Error shutting down {dataset_name}/{title}: {e}")
        active_dbms.clear()
        dbms_locks.clear()


def restart_dbms(dataset_name: str, title: str) -> Optional[str]:
    """
    Restart a DBMS instance. Returns an error message on failure, or None on success.
    Acquires the per-DBMS query_lock to block concurrent queries during the restart.
    """
    key = (dataset_name, title)
    query_lock = dbms_locks.get(key, threading.Lock())

    with query_lock:
        with dbms_lock:
            config = dbms_restart_configs.get(key)
            if config is None:
                return f'No restart config for {dataset_name}/{title}'
            old_dbms = active_dbms.get(key)

        # Shut down outside dbms_lock — can be slow
        if old_dbms is not None:
            try:
                old_dbms.__exit__(None, None, None)
            except Exception as e:
                logger.log_error(f"Error shutting down {dataset_name}/{title} during restart: {e}")

        # Re-instantiate
        dbms_descriptions = database_systems()
        dbms_name = config['dbms_name']
        try:
            logger.log_driver(f"Restarting {dataset_name}/{title}...")
            benchmark = benchmark_instances[dataset_name]
            dbms = dbms_descriptions[dbms_name].instantiate(
                benchmark, config['db_dir'], config['data_dir'], config['params'], config['settings']
            )
            dbms.__enter__()
            logger.log_driver(f"Loading database for {title}...")
            dbms.load_database()
            with dbms_lock:
                active_dbms[key] = dbms
            logger.log_driver(f"✓ {title} restarted successfully")
            return None
        except Exception as e:
            logger.log_error(f"Failed to restart {dataset_name}/{title}: {e}")
            return str(e)


def error(message: str, status_code: int = 404):
    """Helper to return an error response."""
    return jsonify({
        'status': 'error',
        'error': message
    }), status_code


def resolve_dataset(dataset_name: Optional[str]):
    """
    Resolve a dataset name to a benchmark instance.
    If dataset_name is None and only one dataset is loaded, returns that dataset's name.
    Returns (name, error_response) — error_response is None on success.
    """
    if dataset_name:
        if dataset_name not in benchmark_instances:
            return None, error(
                f'Dataset "{dataset_name}" not found. Available: {list(benchmark_instances.keys())}', 404
            )
        return dataset_name, None

    if len(benchmark_instances) == 1:
        return next(iter(benchmark_instances)), None

    if len(benchmark_instances) == 0:
        return None, error('No datasets loaded', 404)

    return None, error(
        f'Multiple datasets loaded, "dataset" field is required. Available: {list(benchmark_instances.keys())}', 400
    )


@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_frontend(path: str):
    """Serve the React frontend. Falls back to index.html for client-side routing."""
    if path and os.path.exists(os.path.join(FRONTEND_BUILD_DIR, path)):
        return send_from_directory(FRONTEND_BUILD_DIR, path)
    index = os.path.join(FRONTEND_BUILD_DIR, 'index.html')
    if os.path.exists(index):
        return send_file(index)
    return jsonify({'error': 'Frontend not built. Run: cd frontend && npm install && npm run build'}), 404


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    with dbms_lock:
        benchmarks = []
        for dataset_name, bench in benchmark_instances.items():
            benchmarks.append({
                'name': bench.name,
                'fullname': bench.fullname,
                'systems': [{'title': title, 'name': dbms.name} for (d, title), dbms in active_dbms.items() if d == dataset_name],
                'optimizer': optimizer_dbms_name.get(dataset_name),
            })

        return jsonify({
            'status': 'ok',
            'benchmarks': benchmarks,
            'endpoints': {
                'health': 'GET /health',
                'dataset': 'POST /dataset',
                'query': 'POST /query',
                'plan': 'POST /plan',
                'optimize': 'POST /optimize'
            }
        })


@app.route('/query', methods=['POST'])
def execute_query():
    """
    Execute a query on a specified DBMS.

    Request JSON:
    {
        "dataset": "tpch",           # Optional if only one dataset is loaded
        "dbms": "duckdb",            # Required: name of DBMS to execute on
        "query": "SELECT ...",        # Required: SQL query to execute
        "timeout": 5,               # Optional: query timeout in seconds (default: 5)
        "fetch_result": true,         # Optional: fetch result rows (default: true)
        "fetch_result_limit": 1000    # Optional: limit result rows (default: 1000)
    }

    Response JSON:
    {
        "status": "success" | "error" | "timeout" | "fatal" | "oom",
        "runtime_ms": 123.45,         # Client-side total time in milliseconds
        "server_time_ms": 120.5,      # Server-side execution time (if available)
        "rows": 42,                   # Number of rows (if fetch_result=true)
        "columns": ["col1", "col2"],  # Column names (if fetch_result=true)
        "result": [[...], ...],       # Result rows (if fetch_result=true)
        "error": "error message"      # Error message (if status != success)
    }
    """
    data = request.get_json()

    if not data:
        return error('Request body must be JSON', 400)

    dbms_name = data.get('dbms')
    query = data.get('query')

    if not dbms_name:
        return error('Missing required field: dbms', 400)
    if not query:
        return error('Missing required field: query', 400)

    dataset_name, err = resolve_dataset(data.get('dataset'))
    if err:
        return err

    timeout = data.get('timeout', 5)
    fetch_result = data.get('fetch_result', True)
    fetch_result_limit = data.get('fetch_result_limit', 1000)

    # Get DBMS instance and its lock
    with dbms_lock:
        key = (dataset_name, dbms_name)
        if key not in active_dbms:
            available = [t for (d, t) in active_dbms if d == dataset_name]
            return error(f'DBMS "{dbms_name}" is not active for dataset "{dataset_name}". Available: {available}', 404)

        dbms = active_dbms[key]
        query_lock = dbms_locks[key]

    # Serialize queries to the same DBMS; fail fast if a restart is in progress
    if not query_lock.acquire(blocking=False):
        return error(f'DBMS "{dbms_name}" is restarting, please try again shortly', 503)
    try:
        result = dbms._execute(query, fetch_result=fetch_result, timeout=timeout, fetch_result_limit=fetch_result_limit)

        response = {}
        response['status'] = result.state
        response['runtime_ms'] = result.client_total[0] if result.client_total else None
        response['server_time_ms'] = result.total[0] if result.total else None

        if result.state == Result.SUCCESS:
            if fetch_result:
                response['rows'] = result.rows
                response['columns'] = result.columns
                response['result'] = result.result
        else:
            response['error'] = result.message

        return jsonify(response)

    except Exception as e:
        logger.log_error(f"Unexpected error executing query on {dataset_name}/{dbms_name}: {e}")
        threading.Thread(target=restart_dbms, args=(dataset_name, dbms_name), daemon=True).start()
        return jsonify({'status': Result.FATAL, 'error': str(e), 'runtime_ms': None, 'server_time_ms': None})

    finally:
        query_lock.release()


@app.route('/plan', methods=['POST'])
def get_query_plan():
    """
    Retrieve query plan for a query on a specified DBMS.

    Request JSON:
    {
        "dataset": "tpch",           # Optional if only one dataset is loaded
        "dbms": "duckdb",            # Required: name of DBMS to get plan from
        "query": "SELECT ...",        # Required: SQL query to analyze
        "timeout": 5                # Optional: timeout in seconds (default: 5)
    }

    Response JSON:
    {
        "status": "success" | "error",
        "query_plan": {...},          # Query plan object (if status=success and supported)
        "error": "error message"      # Error message (if status=error or not supported)
    }
    """
    data = request.get_json()

    if not data:
        return error('Request body must be JSON', 400)

    dbms_name = data.get('dbms')
    query = data.get('query')
    timeout = data.get('timeout', 5)

    if not dbms_name:
        return error('Missing required field: dbms', 400)
    if not query:
        return error('Missing required field: query', 400)

    dataset_name, err = resolve_dataset(data.get('dataset'))
    if err:
        return err

    # Get DBMS instance and its lock
    with dbms_lock:
        key = (dataset_name, dbms_name)
        if key not in active_dbms:
            available = [t for (d, t) in active_dbms if d == dataset_name]
            return error(f'DBMS "{dbms_name}" is not active for dataset "{dataset_name}". Available: {available}', 404)

        dbms = active_dbms[key]
        query_lock = dbms_locks[key]

    # Serialize queries to the same DBMS; fail fast if a restart is in progress
    if not query_lock.acquire(blocking=False):
        return error(f'DBMS "{dbms_name}" is restarting, please try again shortly', 503)
    try:
        plan = dbms.retrieve_query_plan(query, include_system_representation=False, timeout=timeout)
        if plan:
            return jsonify({'status': 'success', 'query_plan': encode_query_plan(plan, format="json")})
        return jsonify({'status': 'error', 'error': 'Query plan retrieval not supported for this DBMS'})

    except Exception as e:
        logger.log_error(f"Error retrieving query plan on {dataset_name}/{dbms_name}: {e}")
        return jsonify({'status': 'error', 'error': str(e)})

    finally:
        query_lock.release()


@app.route('/optimize', methods=['POST'])
def optimize():
    """
    Optimize a query using Umbra's query planner.

    Request JSON:
    {
        "dataset": "tpch",           # Optional if only one dataset is loaded
        "query": "SELECT ...",        # Required: SQL query to optimize
        "dbms": "duckdb"              # Required: the dbms to optimize for
    }

    Response JSON:
    {
        "status": "success" | "error",
        "optimized_query": "SELECT ...",  # Optimized query (if status=success)
        "error": "error message"           # Error message (if status=error)
    }
    """
    data = request.get_json()

    if not data:
        return error('Request body must be JSON', 400)

    query = data.get('query')
    dbms = data.get('dbms')

    if not query:
        return error('Missing required field: query', 400)
    if not dbms:
        return error('Missing required field: dbms', 400)

    dataset_name, err = resolve_dataset(data.get('dataset'))
    if err:
        return err

    # Get optimizer DBMS instance
    with dbms_lock:
        opt_name = optimizer_dbms_name.get(dataset_name)
        if opt_name is None:
            return error(f'No Umbra/UmbraDev instance configured for query optimization on dataset "{dataset_name}"', 404)

        opt_key = (dataset_name, opt_name)
        if opt_key not in active_dbms:
            return error(f'Optimizer DBMS "{opt_name}" is not active', 404)

        optimizer = active_dbms[opt_key]
        optimizer_lock = dbms_locks[opt_key]

    # Check if optimizer supports plan_query
    if not hasattr(optimizer, 'plan_query'):
        return error(f'DBMS "{opt_name}" does not support query optimization', 400)

    # Optimize the query
    with optimizer_lock:
        try:
            optimized = optimizer.plan_query(query, dbms)

            if optimized is None:
                raise 'Query optimization failed'

            return jsonify({
                'status': 'success',
                'optimized_query': optimized
            })

        except Exception as e:
            logger.log_error(f"Error optimizing query: {e}")
            return error(str(e), 500)


@app.route('/dataset', methods=['POST'])
def get_dataset():
    """
    Get information about a loaded dataset.

    Request JSON:
    {
        "dataset": "tpch"    # Optional if only one dataset is loaded
    }

    Response JSON:
    {
        "status": "success",
        "benchmark": "tpch",
        "schema": "CREATE TABLE ...",
        "queries": [
            {
                "name": "1.sql",
                "sql": "SELECT ...",
                "clickhouse": "SELECT ...",
                "duckdb": "SELECT ..."
            },
            ...
        ]
    }
    """
    data = request.get_json() or {}
    dataset_name, err = resolve_dataset(data.get('dataset'))
    if err:
        return err

    bench = benchmark_instances[dataset_name]

    try:
        schema = bench.get_schema(primary_key=True, foreign_keys=False)
        schema_sql = '\n\n'.join(sql.create_table_statements(schema, alter_table=False))

        queries_list, query_overrides = bench.queries('')

        queries = []
        for name, query_sql in queries_list:
            entry = {'name': name, 'sql': query_sql}
            for dbms_name, overrides in query_overrides.items():
                if name in overrides:
                    entry[dbms_name] = overrides[name]
            queries.append(entry)

        return jsonify({
            'status': 'success',
            'benchmark': bench.nice_name,
            'description': bench.description,
            'schema': schema_sql,
            'queries': queries
        })

    except Exception as e:
        logger.log_error(f"Error retrieving dataset info for {dataset_name}: {e}")
        return error(str(e), 500)


def setup_dbms(benchmark: Benchmark, systems: list[dict], db_dir: str, data_dir: str, base_port: int = 54320, optimizer_name: Optional[str] = None):
    """
    Initialize and load all specified DBMS instances for a single benchmark/dataset.

    Args:
        benchmark: The benchmark instance
        systems: List of system configurations
        db_dir: Database directory
        data_dir: Data directory
        base_port: Starting port for DBMS allocation
        optimizer_name: Name of Umbra/UmbraDev instance for optimization (optional)
    """
    dataset_name = benchmark.name
    dbms_descriptions = database_systems()

    logger.log_driver(f"Preparing {benchmark.description}")
    benchmark.dbgen()

    port_offset = len(active_dbms)

    with dbms_lock:
        for system_config in systems:
            title = system_config['title']
            dbms_name = system_config['dbms']
            params = dict(system_config.get('params', {}))
            settings = system_config.get('settings', {})

            host_port = base_port + port_offset
            port_offset += 1
            params['host_port'] = host_port

            logger.log_header(title)
            logger.log_driver(f"Starting {title} (dataset: {dataset_name}, dbms: {dbms_name}, params: {params}, settings: {settings})")

            if dbms_name not in dbms_descriptions:
                logger.log_error(f"Unknown DBMS: {dbms_name}")
                continue

            try:
                dbms = dbms_descriptions[dbms_name].instantiate(benchmark, db_dir, data_dir, params, settings)
                dbms.__enter__()

                logger.log_driver(f"Loading database for {title}...")
                dbms.load_database()

                active_dbms[(dataset_name, title)] = dbms
                dbms_locks[(dataset_name, title)] = threading.Lock()
                dbms_restart_configs[(dataset_name, title)] = {
                    'dbms_name': dbms_name,
                    'params': params,
                    'settings': settings,
                    'db_dir': db_dir,
                    'data_dir': data_dir,
                }
                logger.log_driver(f"✓ {title} is ready (port: {host_port})")

                conn_str = dbms.connection_string()
                if conn_str:
                    logger.log_driver(f"  Connection: {conn_str}")

            except Exception as e:
                logger.log_error(f"Failed to start {title}: {e}")
                raise

        # Determine optimizer DBMS for this dataset
        if optimizer_name:
            if (dataset_name, optimizer_name) not in active_dbms:
                logger.log_error(f"Specified optimizer '{optimizer_name}' not found in active DBMS for dataset '{dataset_name}'")
            else:
                optimizer_dbms_name[dataset_name] = optimizer_name
                logger.log_driver(f"Using {optimizer_name} for query optimization on dataset '{dataset_name}'")
        else:
            for (d, title), dbms in active_dbms.items():
                if d == dataset_name and hasattr(dbms, 'plan_query'):
                    optimizer_dbms_name[dataset_name] = title
                    logger.log_driver(f"Using {title} for query optimization on dataset '{dataset_name}' (auto-detected)")
                    break

            if dataset_name not in optimizer_dbms_name:
                optimizer_dbms_name[dataset_name] = None
                logger.log_driver(f"No Umbra/UmbraDev instance found for query optimization on dataset '{dataset_name}'")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='HTTP server for interactive OLAP benchmark querying',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  ./server.py -j server_config.yaml --port 5000

server_config.yaml format (single dataset):
  benchmark:
    name: tpch
    scale: 1

  systems:
    - title: DuckDB
      dbms: duckdb
      params:
        version: latest
      settings:
        max_memory: 8GB

server_config.yaml format (multiple datasets, shared systems):
  datasets:
    - name: tpch
      scale: 1
    - name: job

  systems:
    - title: DuckDB
      dbms: duckdb
    - title: ClickHouse
      dbms: clickhouse

Endpoints:
  GET  /health          - Server health and status
  POST /dataset         - Schema and queries (add "dataset" field for multiple datasets)
  POST /query           - Execute query on specified DBMS (add "dataset" field for multiple datasets)
  POST /plan            - Get query plan for a query
  POST /optimize        - Optimize query using Umbra (if configured)
"""
    )

    parser.add_argument('-j', '--json', required=True, help='YAML configuration file')
    parser.add_argument('--db-dir', default=os.path.join(workdir, 'db'), help='Database directory (default: ./db)')
    parser.add_argument('--data-dir', default=os.path.join(workdir, 'data'), help='Data directory (default: ./data)')
    parser.add_argument('--base-port', type=int, default=55000, help='Starting port for DBMS allocation (default: 54320)')
    parser.add_argument('--port', type=int, default=5000, help='HTTP server port (default: 5000)')
    parser.add_argument('--host', default='0.0.0.0', help='HTTP server host (default: 0.0.0.0)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    parser.add_argument('-vv', '--very-verbose', action='store_true', help='Enable very verbose logging')
    benchmark_arguments(parser, required=False)

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    """Load and validate the YAML configuration file against the schema."""
    return schemajson.load(config_path, "server.schema.json")


def _instantiate_benchmark(benchmark_config: dict) -> Benchmark:
    """Instantiate a Benchmark from a config dict."""
    benchmark_name = benchmark_config.get('name')
    if not benchmark_name:
        raise ValueError("benchmark config must specify 'name'")

    benchmark_map = benchmarks()
    if benchmark_name not in benchmark_map:
        raise ValueError(f"Unknown benchmark: {benchmark_name}. Available: {list(benchmark_map.keys())}")

    return benchmark_map[benchmark_name].instantiate('./', benchmark_config)


def _parse_systems(systems_config: list) -> list[dict]:
    """Expand system configs (unfold parameter/settings templates)."""
    systems = []
    for system_config in systems_config:
        if system_config.get('disabled', False):
            continue

        params = system_config.get('parameter', {})
        settings = system_config.get('settings', {})

        for params in unfold(params):
            for settings in unfold(settings):
                template = Template(system_config['title'])
                title = template.substitute(**settings, **params)
                systems.append({
                    'title': title,
                    'dbms': system_config['dbms'],
                    'params': params,
                    'settings': settings
                })
    return systems


def build_frontend():
    """Build the React frontend if source is present."""
    import subprocess
    frontend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'frontend')
    if not os.path.exists(os.path.join(frontend_dir, 'package.json')):
        return
    logger.log_driver("Building frontend...")
    subprocess.run(['npm', 'install', '--legacy-peer-deps'], cwd=frontend_dir, check=True)
    subprocess.run(['npm', 'run', 'build'], cwd=frontend_dir, check=True)
    logger.log_driver("Frontend built.")


def main():
    """Main entry point."""
    args = parse_args()

    logger.set_very_verbose(args.very_verbose)
    logger.set_verbose(args.verbose)

    try:
        build_frontend()
    except Exception as e:
        logger.log_error(f"Failed to build frontend: {e}")
        sys.exit(1)

    try:
        config = load_config(args.json)
    except Exception as e:
        logger.log_error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Shared systems list — same for all datasets
    systems_config = config.get('systems', [])
    optimizer_name = config.get('optimizer', None)

    systems = _parse_systems(systems_config)
    if not systems:
        logger.log_error("No systems configured")
        sys.exit(1)

    # Collect benchmark configs. Priority:
    #   1. CLI benchmark argument (if not "default" or None)
    #   2. "datasets" list in config
    #   3. "benchmark" dict in config (single-dataset legacy)
    cli_benchmark = getattr(args, 'benchmark', None)
    if cli_benchmark and cli_benchmark != 'default':
        cli_args = vars(args)
        benchmark_configs = [{**cli_args, 'name': cli_benchmark}]
    elif 'datasets' in config:
        benchmark_configs = config['datasets']
    else:
        benchmark_config = config.get('benchmark', {})
        if not benchmark_config:
            logger.log_error("Configuration must specify 'benchmark', 'datasets', or a CLI benchmark argument")
            sys.exit(1)
        benchmark_configs = [benchmark_config]

    atexit.register(cleanup_dbms)

    for benchmark_config in benchmark_configs:
        try:
            bench = _instantiate_benchmark(benchmark_config)
        except Exception as e:
            logger.log_error(f"Failed to instantiate benchmark: {e}")
            cleanup_dbms()
            sys.exit(1)

        benchmark_instances[bench.name] = bench

        try:
            setup_dbms(bench, systems, args.db_dir, args.data_dir, args.base_port, optimizer_name)
        except Exception as e:
            logger.log_error(f"Failed to set up DBMS instances for dataset '{bench.name}': {e}")
            cleanup_dbms()
            sys.exit(1)

    _start_server(args)


def _start_server(args):
    """Log startup info and start the Flask server."""
    logger.log_header("Server Ready")
    logger.log_driver(f"HTTP server starting on {args.host}:{args.port}")
    for dataset_name in benchmark_instances:
        systems = [title for (d, title) in active_dbms if d == dataset_name]
        opt = optimizer_dbms_name.get(dataset_name)
        logger.log_driver(f"  [{dataset_name}] systems: {systems}" + (f", optimizer: {opt}" if opt else ""))
    logger.log_driver("")
    logger.log_driver("Endpoints:")
    logger.log_driver("  GET  /health   - Health check")
    logger.log_driver("  POST /dataset  - Schema and queries")
    logger.log_driver("  POST /query    - Execute query")
    logger.log_driver("  POST /plan     - Get query plan")
    logger.log_driver("  POST /optimize - Optimize query using Umbra")
    logger.log_driver("")

    try:
        app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        logger.log_driver("Shutting down...")
    finally:
        cleanup_dbms()


if __name__ == '__main__':
    main()

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

import simplejson as json
from benchmark import System
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

from benchmarks.benchmark import benchmark_arguments, benchmarks, Benchmark
from dbms.dbms import Result, database_systems, DBMS
from queryplan.queryplan import encode_query_plan
from util import logger, schemajson, sql
from util.template import Template, unfold

workdir = os.getcwd()
load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Global state
active_dbms: Dict[str, DBMS] = {}
dbms_locks: Dict[str, threading.Lock] = {}  # Per-DBMS locks for query serialization
benchmark_instance: Optional[Benchmark] = None
optimizer_dbms_name: Optional[str] = None  # Name of Umbra/UmbraDev instance for optimization
dbms_lock = threading.Lock()  # Lock for modifying active_dbms/dbms_locks dictionaries


def cleanup_dbms():
    """Clean up all active DBMS instances on shutdown."""
    with dbms_lock:
        for name, dbms in active_dbms.items():
            try:
                logger.log_driver(f"Shutting down {name}...")
                dbms.__exit__(None, None, None)
            except Exception as e:
                logger.log_error(f"Error shutting down {name}: {e}")
        active_dbms.clear()
        dbms_locks.clear()


def error(message: str, status_code: int = 404):
    """Helper to return an error response."""
    return jsonify({
        'status': 'error',
        'error': message
    }), status_code


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    with dbms_lock:
        return jsonify({
            'status': 'ok',
            'active_dbms': [{'title': title, 'name': dbms.name} for title, dbms in active_dbms.items()],
            'benchmark': benchmark_instance.fullname if benchmark_instance else None,
            'optimizer': optimizer_dbms_name,
            'endpoints': {
                'health': 'GET /health',
                'dataset': 'GET /dataset',
                'query': 'POST /query',
                'plan': 'POST /plan',
                'optimize': 'POST /optimize' if optimizer_dbms_name else None
            }
        })


@app.route('/query', methods=['POST'])
def execute_query():
    """
    Execute a query on a specified DBMS.

    Request JSON:
    {
        "dbms": "duckdb",           # Required: name of DBMS to execute on
        "query": "SELECT ...",       # Required: SQL query to execute
        "timeout": 5,              # Optional: query timeout in seconds (default: 5)
        "fetch_result": true,        # Optional: fetch result rows (default: true)
        "fetch_result_limit": 1000   # Optional: limit result rows (default: 1000)
    }

    Response JSON:
    {
        "status": "success" | "error" | "timeout" | "fatal" | "oom",
        "runtime_ms": 123.45,        # Client-side total time in milliseconds
        "server_time_ms": 120.5,     # Server-side execution time (if available)
        "rows": 42,                  # Number of rows (if fetch_result=true)
        "columns": ["col1", "col2"], # Column names (if fetch_result=true)
        "result": [[...], ...],      # Result rows (if fetch_result=true)
        "error": "error message"     # Error message (if status != success)
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

    timeout = data.get('timeout', 5)
    fetch_result = data.get('fetch_result', True)
    fetch_result_limit = data.get('fetch_result_limit', 1000)

    # Get DBMS instance and its lock
    with dbms_lock:
        if dbms_name not in active_dbms:
            return error(f'DBMS "{dbms_name}" is not active. Available: {list(active_dbms.keys())}', 404)

        dbms = active_dbms[dbms_name]
        query_lock = dbms_locks[dbms_name]

    # Serialize queries to the same DBMS
    response = {}
    with query_lock:
        try:
            # Execute the query
            result = dbms._execute(query, fetch_result=fetch_result, timeout=timeout, fetch_result_limit=fetch_result_limit)

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
            logger.log_error(f"Unexpected error executing query on {dbms_name}: {e}")
            response['status'] = Result.FATAL
            response['error'] = str(e)
            response['runtime_ms'] = None
            response['server_time_ms'] = None

    return jsonify(response)


@app.route('/plan', methods=['POST'])
def get_query_plan():
    """
    Retrieve query plan for a query on a specified DBMS.

    Request JSON:
    {
        "dbms": "duckdb",           # Required: name of DBMS to get plan from
        "query": "SELECT ...",       # Required: SQL query to analyze
        "timeout": 5               # Optional: timeout in seconds (default: 5)
    }

    Response JSON:
    {
        "status": "success" | "error",
        "query_plan": {...},         # Query plan object (if status=success and supported)
        "error": "error message"     # Error message (if status=error or not supported)
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

    # Get DBMS instance and its lock
    with dbms_lock:
        if dbms_name not in active_dbms:
            return error(f'DBMS "{dbms_name}" is not active. Available: {list(active_dbms.keys())}', 404)

        dbms = active_dbms[dbms_name]
        query_lock = dbms_locks[dbms_name]

    # Serialize queries to the same DBMS
    response = {}
    with query_lock:
        try:
            plan = dbms.retrieve_query_plan(query, include_system_representation=False, timeout=timeout)
            if plan:
                response['status'] = 'success'
                response['query_plan'] = encode_query_plan(plan, format="json")
            else:
                response['status'] = 'error'
                response['error'] = 'Query plan retrieval not supported for this DBMS'

            return jsonify(response)

        except Exception as e:
            logger.log_error(f"Error retrieving query plan on {dbms_name}: {e}")
            response['status'] = 'error'
            response['error'] = str(e)

    return jsonify(response)


@app.route('/optimize', methods=['POST'])
def optimize():
    """
    Optimize a query using Umbra's query planner.

    Request JSON:
    {
        "query": "SELECT ...",       # Required: SQL query to optimize
        "dbms": "duckdb"             # Required: the dbms to optimize for
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

    # Get optimizer DBMS instance
    with dbms_lock:
        if optimizer_dbms_name is None:
            return error('No Umbra/UmbraDev instance configured for query optimization', 404)

        if optimizer_dbms_name not in active_dbms:
            return error(f'Optimizer DBMS "{optimizer_dbms_name}" is not active', 404)

        optimizer = active_dbms[optimizer_dbms_name]
        optimizer_lock = dbms_locks[optimizer_dbms_name]

    # Check if optimizer supports plan_query
    if not hasattr(optimizer, 'plan_query'):
        return error(f'DBMS "{optimizer_dbms_name}" does not support query optimization', 400)

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


@app.route('/dataset', methods=['GET'])
def get_dataset():
    """
    Get information about the loaded dataset.

    Response JSON:
    {
        "status": "success",
        "benchmark": "tpch",
        "schema": {
            "tables": [...],
            "delimiter": ",",
            ...
        },
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
    if benchmark_instance is None:
        return error('No benchmark loaded', 404)

    try:
        # Get schema (with primary keys, without foreign keys for Umbra compatibility)
        schema = benchmark_instance.get_schema(primary_key=True, foreign_keys=False)

        # Convert schema dict to SQL CREATE TABLE statements
        schema_sql = '\n\n'.join(sql.create_table_statements(schema, alter_table=False))

        # Get queries and any DB-specific overrides
        queries_list, query_overrides = benchmark_instance.queries('')

        # Format queries as list of dicts with per-DBMS overrides embedded
        queries = []
        for name, query_sql in queries_list:
            entry = {'name': name, 'sql': query_sql}
            for dbms_name, overrides in query_overrides.items():
                if name in overrides:
                    entry[dbms_name] = overrides[name]
            queries.append(entry)

        return jsonify({
            'status': 'success',
            'benchmark': benchmark_instance.nice_name,
            'description': benchmark_instance.description,
            'schema': schema_sql,
            'queries': queries
        })

    except Exception as e:
        logger.log_error(f"Error retrieving dataset info: {e}")
        return error(str(e), 500)


def setup_dbms(benchmark: Benchmark, systems: list[dict], db_dir: str, data_dir: str, base_port: int = 54320, optimizer_name: Optional[str] = None):
    """
    Initialize and load all specified DBMS instances.

    Args:
        benchmark: The benchmark instance
        systems: List of system configurations
        db_dir: Database directory
        data_dir: Data directory
        base_port: Starting port for DBMS allocation (default: 54320)
        optimizer_name: Name of Umbra/UmbraDev instance for optimization (optional)
    """
    global optimizer_dbms_name
    dbms_descriptions = database_systems()

    # Generate data if needed
    logger.log_driver(f"Preparing {benchmark.description}")
    benchmark.dbgen()

    port_offset = 0
    with dbms_lock:
        for system_config in systems:
            title = system_config['title']
            dbms_name = system_config['dbms']
            params = system_config.get('params', {})
            settings = system_config.get('settings', {})

            # Allocate unique port for this DBMS
            host_port = base_port + port_offset
            port_offset += 1
            params['host_port'] = host_port

            logger.log_header(title)
            logger.log_driver(f"Starting {title} (dbms: {dbms_name}, params: {params}, settings: {settings})")

            if dbms_name not in dbms_descriptions:
                logger.log_error(f"Unknown DBMS: {dbms_name}")
                continue

            try:
                dbms = dbms_descriptions[dbms_name].instantiate(benchmark, db_dir, data_dir, params, settings)
                dbms.__enter__()

                # Load the database
                logger.log_driver(f"Loading database for {title}...")
                dbms.load_database()

                active_dbms[title] = dbms
                dbms_locks[title] = threading.Lock()
                logger.log_driver(f"✓ {title} is ready (port: {host_port})")

                # Log connection string if available
                conn_str = dbms.connection_string()
                if conn_str:
                    logger.log_driver(f"  Connection: {conn_str}")

            except Exception as e:
                logger.log_error(f"Failed to start {title}: {e}")
                raise

        # Determine optimizer DBMS
        if optimizer_name:
            if optimizer_name not in active_dbms:
                logger.log_error(f"Specified optimizer '{optimizer_name}' not found in active DBMS")
            else:
                optimizer_dbms_name = optimizer_name
                logger.log_driver(f"Using {optimizer_dbms_name} for query optimization")
        else:
            # Find first umbra or umbradev instance
            for title, dbms in active_dbms.items():
                if hasattr(dbms, 'plan_query'):
                    optimizer_dbms_name = title
                    logger.log_driver(f"Using {optimizer_dbms_name} for query optimization (auto-detected)")
                    break

            if optimizer_dbms_name is None:
                logger.log_driver("No Umbra/UmbraDev instance found for query optimization")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='HTTP server for interactive OLAP benchmark querying',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  ./server.py -j server_config.yaml --port 5000

server_config.yaml format:
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
    
    - title: ClickHouse
      dbms: clickhouse
      params:
        version: latest

Endpoints:
  GET  /health       - Server health and status
  GET  /dataset      - Schema and available queries
  POST /query        - Execute query on specified DBMS
  POST /plan         - Get query plan for a query
  POST /optimize     - Optimize query using Umbra (if configured)
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
    benchmark_arguments(parser)

    return parser.parse_args()


def load_config(config_path: str) -> dict:
    """Load and validate the YAML configuration file against the schema."""
    return schemajson.load(config_path, "server.schema.json")


def main():
    """Main entry point."""
    global benchmark_instance

    args = parse_args()

    # Set up logging
    logger.set_very_verbose(args.very_verbose)
    logger.set_verbose(args.verbose)

    # Load configuration
    try:
        config = load_config(args.json)
    except Exception as e:
        logger.log_error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Parse benchmark configuration
    # Command-line benchmark overrides config file
    if args.benchmark != "default":
        # Use command-line specified benchmark
        benchmark_descriptions = benchmarks()
        if args.benchmark not in benchmark_descriptions:
            logger.log_error(f"Unknown benchmark: {args.benchmark}. Available: {list(benchmark_descriptions.keys())}")
            sys.exit(1)

        benchmark_instance = benchmark_descriptions[args.benchmark].instantiate('./', vars(args))
    else:
        # Use benchmark from config file
        benchmark_config = config.get('benchmark', {})
        benchmark_name = benchmark_config.get('name')

        if not benchmark_name:
            logger.log_error("Configuration must specify benchmark.name or use command-line benchmark argument")
            sys.exit(1)

        # Get benchmark arguments
        bench_args = benchmark_arguments()
        if benchmark_name not in bench_args:
            logger.log_error(f"Unknown benchmark: {benchmark_name}. Available: {list(bench_args.keys())}")
            sys.exit(1)

        # Parse benchmark parameters from config
        benchmark_params = {}
        for key, value in benchmark_config.items():
            if key != 'name':
                benchmark_params[key] = value

        # Instantiate benchmark
        benchmark_instance = benchmarks()[benchmark_name]('./', benchmark_params)

    # Parse systems configuration
    systems = []
    for system_config in config.get('systems', []):
        if system_config.get('disabled', False):
            continue

        params = system_config.get('parameter', {})
        settings = system_config.get('settings', {})

        for params in unfold(params):
            for settings in unfold(settings):
                # fill the title
                template = Template(system_config['title'])
                title = template.substitute(**settings, **params)

                systems.append({
                    'title': title,
                    'dbms': system_config['dbms'],
                    'params': params,
                    'settings': settings
                })

    if not systems:
        logger.log_error("No systems configured")
        sys.exit(1)

    # Register cleanup
    atexit.register(cleanup_dbms)

    # Set up DBMS instances
    try:
        optimizer_name = config.get('optimizer', None)
        setup_dbms(benchmark_instance, systems, args.db_dir, args.data_dir, args.base_port, optimizer_name)
    except Exception as e:
        logger.log_error(f"Failed to set up DBMS instances: {e}")
        cleanup_dbms()
        sys.exit(1)

    logger.log_header("Server Ready")
    logger.log_driver(f"HTTP server starting on {args.host}:{args.port}")
    logger.log_driver(f"Active DBMS: {list(active_dbms.keys())}")
    logger.log_driver(f"Benchmark: {benchmark_instance.name}")
    if optimizer_dbms_name:
        logger.log_driver(f"Query Optimizer: {optimizer_dbms_name}")
    logger.log_driver("")
    logger.log_driver("Endpoints:")
    logger.log_driver("  GET  /health - Health check")
    logger.log_driver("  GET  /dataset - Get schema and queries")
    logger.log_driver("  POST /query  - Execute query")
    logger.log_driver("  POST /plan   - Get query plan for a query")
    if optimizer_dbms_name:
        logger.log_driver("  POST /optimize - Optimize query using Umbra")
    logger.log_driver("")

    # Start Flask server
    try:
        app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        logger.log_driver("Shutting down...")
    finally:
        cleanup_dbms()


if __name__ == '__main__':
    main()

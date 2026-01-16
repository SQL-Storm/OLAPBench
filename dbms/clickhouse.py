import os
import random
import tempfile
import time

import clickhouse_connect
import simplejson as json

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from queryplan.parsers.clickhouseparser import ClickHouseParser
from queryplan.queryplan import encode_query_plan
from util import formatter, logger, process, sql


class ClickHouse(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

        self._client = None

    @property
    def name(self) -> str:
        return "clickhouse"

    @property
    def docker_image_name(self) -> str:
        return f'clickhouse:{self._version}'

    def connection_string(self) -> str:
        # HTTP endpoint exposed to the host
        port = getattr(self, '_host_port', 54325)
        return f"http://localhost:{port}/?database=clickhouse&password=clickhouse"

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)
        self.temp_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        # Append a short random hex suffix to avoid name collisions
        self.container_name = "docker_clickhouse_" + format(random.getrandbits(32), '08x')
        clickhouse_environment = {
            "CLICKHOUSE_DB": "clickhouse",
            "CLICKHOUSE_PASSWORD": "clickhouse"
        }
        docker_params = {
            "name": self.container_name,
            "shm_size": "%d" % self._buffer_size,
            "stdin_open": True,
        }
        # Expose HTTP port (8123 in container) to host port (from params or default 54325)
        self._host_port = self._host_port if self._host_port is not None else 54325
        self._start_container(clickhouse_environment, 8123, self._host_port, self.host_dir.name, "/var/lib/clickhouse/", docker_params=docker_params)

        logger.log_verbose_dbms(f"Connecting to {self.name}...", self)
        time.sleep(5)

        start_time = time.time()
        timeout = 120  # 2 minutes
        self._client = None
        while time.time() - start_time < timeout:
            try:
                if self._container_status() != "running":
                    time.sleep(1)
                    continue

                self._client = clickhouse_connect.get_client(
                    host='localhost',
                    port=self._host_port,
                    username=os.environ.get('CLICKHOUSE_USER', 'default'),
                    password=os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse'),
                    database='clickhouse'
                )
                # Simple readiness probe
                self._client.query('select 1')
                break
            except Exception as e:
                time.sleep(1)  # 1 second

        if self._client is None:
            raise Exception(f"Unable to connect to {self.name}")

        logger.log_verbose_dbms(f"Established connection to {self.name}", self)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
        self._close_container()
        self.temp_dir.cleanup()
        self.host_dir.cleanup()

    def _transform_schema(self, schema: dict) -> dict:
        schema = sql.transform_schema(schema, escape='"', lowercase=self._umbra_planner)
        for table in schema['tables']:
            for column in table['columns']:
                column['type'] = column['type'].replace('timestamp', 'datetime64(6)')
        return schema

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema, extra_text="engine=MergeTree")

    def _copy_statements(self, schema: dict) -> list[str]:
        non_empty_tables = [table for table in schema['tables'] if not table.get("initially empty", False) and not self._benchmark.empty()]

        settings = {'input_format_allow_errors_num': 0}
        settings.update(self._settings if self._settings else {})
        if schema['delimiter'] != '\t':
            settings['format_csv_delimiter'] = schema['delimiter']

        with logger.LogProgress("Loading tables...", len(schema["tables"])) as progress:
            for table in schema["tables"]:
                progress.next(f'Loading {table["name"]}...')
                begin = time.time()
                if table in non_empty_tables:
                    try:
                        query_path = os.path.join(self.temp_dir.name, "query.sql")
                        with open(query_path, 'w') as query_sql:
                            query_sql.write("set allow_experimental_join_condition=1;\n")
                            query_sql.write("set allow_experimental_analyzer=1;\n")
                            for key, value in self._settings.items():
                                if isinstance(value, str):
                                    query_sql.write(f"set {key}='{value}';\n")
                                else:
                                    query_sql.write(f"set {key}={value};\n")

                            if schema['delimiter'] == '\t':
                                query_sql.write(f"insert into {table['name']} from infile '/data/{table['file']}' format TSV;")
                            else:
                                query_sql.write(f"set format_csv_delimiter='{schema['delimiter']}';")
                                query_sql.write(f"insert into {table['name']} from infile '/data/{table['file']}' format CSV;")

                        process.Process(f"docker cp {query_path} {self.container_name}:/tmp/query.sql").run()

                        self._execute_in_container(f'bash -c "clickhouse-client --time --format="Null" -d clickhouse --password clickhouse --queries-file=/tmp/query.sql"')
                    except Exception as e:
                        logger.log_error(f'Error while loading table: {str(e)}')
                        raise Exception(f'Error while loading table: {str(e)}')
                total = time.time() - begin

                logger.log_verbose_dbms(f'Loaded {table["name"]} in {formatter.format_time(total)}', self)
                progress.finish()

        return []

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()
        # Prepare per-query settings
        settings = {}
        settings['allow_experimental_join_condition'] = 1
        settings['allow_experimental_analyzer'] = 1
        settings['aggregate_functions_null_for_empty'] = 1
        settings['union_default_mode'] = 'DISTINCT'
        settings['data_type_default_nullable'] = 1
        if timeout and timeout > 0:
            settings['max_execution_time'] = timeout
        settings.update(self._settings if self._settings else {})

        begin = time.time()
        try:
            summary = None
            if fetch_result:
                qres = self._client.query(query.strip(), settings=settings)
                # Convert tuples to lists for consistency with existing interface
                client_total = (time.time() - begin) * 1000.0

                # Extract column names
                result.columns = qres.column_names

                for row in qres.result_rows:
                    result.result.append(list(row))
                result.rows = len(result.result)
                if fetch_result_limit > 0 and len(result.result) > fetch_result_limit:
                    result.result = result.result[:fetch_result_limit]

                # Obtain server-side elapsed if available
                summary = qres.summary
            else:
                cres = self._client.command(query.strip(), settings=settings)
                client_total = (time.time() - begin) * 1000.0

                summary = cres.summary

            result.client_total.append(client_total)
            elapsed_ms = int(summary.get('elapsed_ns', None)) * 1e-6 if summary else None
            if elapsed_ms is not None:
                result.total.append(elapsed_ms)
            return result
        except Exception as e:
            client_total = (time.time() - begin) * 1000.0
            if self._container_status() != "running":
                # Container died; propagate exception
                raise e

            result.message = str(e)
            msg_lower = result.message.lower()
            is_timeout = 'timeout' in msg_lower or 'time out' in msg_lower or 'deadline' in msg_lower
            result.state = Result.TIMEOUT if is_timeout else Result.ERROR
            result.client_total.append(timeout * 1000 if result.state == Result.TIMEOUT and timeout else client_total)
            return result

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False, timeout: int = 0):
        result = self._execute(query="explain json=1, actions=1 " + query.strip(), fetch_result=True, timeout=timeout).result
        if not result or not result[0]:
            logger.log_warn("Could not retrieve query plan")
        text_plan = result[0][0]
        json_plan = json.loads(text_plan, allow_nan=True)[0]
        plan_parser = ClickHouseParser(include_system_representation=include_system_representation)
        return plan_parser.parse_json_plan(query, json_plan)


class ClickHouseDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'clickhouse'

    @staticmethod
    def get_description() -> str:
        return 'ClickHouse'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        return ClickHouse(benchmark, db_dir, data_dir, params, settings)

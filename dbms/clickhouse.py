import os
import tempfile
import threading
import time

import simplejson as json

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from util import logger, sql, process


class ClickHouse(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return "clickhouse"

    @property
    def docker_image_name(self) -> str:
        return f'clickhouse:{self._version}'

    def connection_string(self) -> str:
        return "docker exec -it docker_clickhouse clickhouse-client -d clickhouse"

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)
        self.temp_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        self.container_name = "docker_clickhouse"
        clickhouse_environment = {
            "CLICKHOUSE_DB": "clickhouse"
        }
        docker_params = {
            "name": self.container_name,
            "shm_size": "%d" % self._buffer_size,
            "stdin_open": True,
        }
        self._start_container(clickhouse_environment, 9005, 54325, self.host_dir.name, "/var/lib/clickhouse/", docker_params=docker_params)

        logger.log_verbose_dbms("Starting ClickHouse docker image ...", self)

        start_time = time.time()
        timeout = 120  # 2 minutes
        while time.time() - start_time < timeout and self.container.exec_run('clickhouse-client -d clickhouse --query "select 1"').exit_code != 0:
            time.sleep(1)  # 1 second

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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
        stmts = []
        for i, table in enumerate(schema["tables"]):
            if schema['delimiter'] == '\t':
                stmts.append(f"insert into {table['name']} from infile '/data/{table['file']}' format TSV;")
            else:
                stmts.append(f"set format_csv_delimiter='{schema['delimiter']}';" +
                             f"insert into {table['name']} from infile '/data/{table['file']}' format CSV;")
        return stmts

    def _container_status(self) -> str:
        try:
            return self.client.containers.get(self.container.id).status
        except Exception:
            return "removed"

    def _execute_in_container(self, command: str, timeout: int = 0):
        timer = None
        if timeout > 0:
            timer = threading.Timer(timeout, self._kill_container)
            timer.start()

        logger.log_verbose_process(command)
        result = self.container.exec_run(command)

        if timer is not None:
            timer.cancel()
            timer.join()

        if result.exit_code != 0:
            logger.log_verbose_process_stderr(result.output.decode('utf-8'))
            raise Exception(result.output.decode('utf-8'))
        else:
            if result.output:
                logger.log_verbose_process(result.output.decode('utf-8').strip())

        return result

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        query_path = os.path.join(self.temp_dir.name, "query.sql")
        with open(query_path, 'w') as query_sql:
            query_sql.write("set allow_experimental_join_condition=1;\n")
            query_sql.write("set allow_experimental_analyzer=1;\n")
            if timeout > 0:
                query_sql.write(f"set max_execution_time={timeout};\n")

            query_sql.write(query)
            query_sql.write("\n")

        process.Process(f"docker cp {query_path} {self.container_name}:/tmp/query.sql").run()

        begin = time.time()
        try:
            return_value = self._execute_in_container(
                f'bash -c "clickhouse-client --time --format={"Null" if not fetch_result else "JSONCompactEachRowWithNamesAndTypes"} -d clickhouse --queries-file=/tmp/query.sql > /tmp/result.json"', timeout=timeout * 10)
        except Exception as e:
            client_total = time.time() - begin
            if self._container_status() != "running":
                raise e

            logger.log_error_verbose(str(e))
            result.message = str(e)
            result.state = Result.TIMEOUT if "Timeout exceeded" in result.message else Result.ERROR
            result.client_total.append(timeout * 1000 if result.state == Result.TIMEOUT else client_total * 1000)
            return result

        if fetch_result:
            result_path = os.path.join(self.temp_dir.name, "result.json")
            process.Process(f'docker cp {self.container_name}:/tmp/result.json {result_path}').run()
            with open(result_path, 'r') as result_file:
                lines = result_file.readlines()
                types = json.loads(lines[1].strip())

                for line in lines[2:]:
                    if line.strip() == "":
                        continue

                    row = []
                    for i, value in enumerate(json.loads(line.strip(), use_decimal=True)):
                        if (types[i] == 'UInt64' or types[i] == 'Int64') and isinstance(value, str):
                            value = int(value)
                        row.append(value)
                    result.result.append(row)

            result.rows = len(result.result)
            if len(result.result) > fetch_result_limit and fetch_result_limit > 0:
                result.result = result.result[:fetch_result_limit]

        client_total = (time.time() - begin) * 1000
        output = return_value.output.decode('utf-8').strip()
        total_time = float(output.split('\n')[-1]) * 1000
        result.client_total.append(client_total)
        result.total.append(total_time)
        return result


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

import os
import tempfile
import threading
import time

import requests
import simplejson as json

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from queryplan.parsers.duckdbparser import DuckDBParser
from queryplan.queryplan import QueryPlan
from util import logger, sql

duck = None


class DuckDB(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return "duckdb"

    def connection_string(self) -> str:
        return self.connection

    def _connect(self, port: int):
        self.connection = None
        url = f"http://localhost:{port}/query"

        start_time = time.time()
        check_timeout = 120  # 2 minutes
        while time.time() - start_time < check_timeout:
            try:
                response = requests.post(url, json={"query": "SELECT 1"})
                if response.status_code == 200:
                    self.connection = url
                    break

            except requests.exceptions.RequestException as e:
                pass

            time.sleep(1)  # 1 second

        if self.connection is None:
            self._kill_container()
            raise Exception(f"Unable to connect to {self.name}")

        logger.log_verbose_dbms(f"Established connection to {self.name}", self)

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        docker_params = {}
        self._start_container({}, 5432, 54323, self.host_dir.name, "/db", docker_params=docker_params)
        self._connect(54323)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_container()
        if self.host_dir:
            self.host_dir.cleanup()

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema, alter_table=False)

    def _copy_statements(self, schema: dict) -> list[str]:
        if self._benchmark.name == "clickbench":
            schema["null"] = "\\c"
            schema["quote"] = "\\b"
        if self._benchmark.unique_name.startswith("stackoverflow_math") or (self._version in ["0.8.0", "0.8.1", "0.9.0", "0.9.1", "0.9.2", "0.10.2"] and schema["format"] == "csv"):
            return sql.copy_statements_duckdb_csv_singlethreaded(schema, "/data")
        return sql.copy_statements_postgres(schema, "/data", supports_text=False)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        output = Result()

        timer_kill = None
        if timeout > 0:
            timer_kill = threading.Timer(timeout * 10, self._kill_container)
            timer_kill.start()

        payload = {"query": query.strip(), "timeout": timeout, "fetch": fetch_result, "limit": fetch_result_limit}
        response = requests.post(self.connection, json=payload)

        if timer_kill is not None:
            timer_kill.cancel()
            timer_kill.join()

        if response.status_code != 200:
            raise Exception(f"Error {response.status_code}: {response.text}")

        payload = response.json()
        if payload.get("error"):
            logger.log_error_verbose(payload.get("error"))
            output.message = payload.get("error")
            output.state = Result.TIMEOUT if "INTERRUPT Error: Interrupted!" in output.message or "canceled Context" in output.message else Result.ERROR
            output.state = Result.OOM if "Cannot allocate" in output.message else output.state

        output.rows = payload.get("rows")
        output.client_total.append(timeout * 1000 if output.state == Result.TIMEOUT else payload.get("client_total"))
        if output.state == Result.SUCCESS:
            if payload.get("total") is not None:
                output.total.append(payload.get("total"))
            if payload.get("execution") is not None:
                output.execution.append(payload.get("execution"))
            if payload.get("compilation") is not None:
                output.compilation.append(payload.get("compilation"))

        if fetch_result:
            try:
                with open(os.path.join(self.host_dir.name, "results.json"), 'r') as result_file:
                    for row in json.loads(result_file.read(), use_decimal=True):
                        output.result.append(row)
            except Exception:
                pass

        return output

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False) -> QueryPlan:
        result = self._execute(query="explain (format json, analyze) " + query.strip(), fetch_result=True).result
        if not result or not result[0]:
            return None
        json_plan = json.loads(result[0][1])
        plan_parser = DuckDBParser(include_system_representation=include_system_representation)
        query_plan = plan_parser.parse_json_plan(query, json_plan)
        return query_plan


class DuckDBDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'duckdb'

    @staticmethod
    def get_description() -> str:
        return 'DuckDB'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        return DuckDB(benchmark, db_dir, data_dir, params, settings)

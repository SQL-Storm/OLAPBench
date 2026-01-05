import os
import tempfile
import threading
import time

import psycopg2
from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from queryplan.parsers.postgresparser import PostgresParser
from queryplan.queryplan import QueryPlan
from util import sql, logger


class Postgres(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return "postgres"

    @property
    def docker_image_name(self) -> str:
        return f'postgres:{self._version}'

    def connection_string(self) -> str:
        return self._connection_string

    def _connect(self, database: str, user: str, password: str, port: int):
        self.connection = None
        start_time = time.time()
        check_timeout = 120  # 2 minutes
        while time.time() - start_time < check_timeout:
            try:
                self.connection = psycopg2.connect(database=database, user=user, password=password, host="localhost", port=port)
                break
            except psycopg2.OperationalError:
                time.sleep(1)  # 1 second

        if self.connection is None:
            raise Exception(f"Unable to connect to {self.name}")

        self._connection_string = f"PGPASSWORD='{password}' psql -h localhost -p {port} -U {user} -d {database}"

        self.connection.set_session(autocommit=True)
        self.connection.set_client_encoding('utf-8')
        self.cursor = self.connection.cursor()

        logger.log_verbose_dbms(f"Established connection to {self.name}", self)

    def _write_config_file(self, file):
        def config(param, value):
            file.write("%s = '%s'\n" % (param, value))

        # we need this so that we can actually connect to the server
        config("listen_addresses", "*")

        # configure memory usage
        config("shared_buffers", "%dB" % self._buffer_size)
        config("work_mem", "%dB" % (self._buffer_size / self._worker_threads))  # emulate Umbra behavior here
        config("autovacuum_work_mem", "%dB" % (self._buffer_size / self._worker_threads))  # emulate Umbra behavior here
        config("maintenance_work_mem", "%dB" % (self._buffer_size / self._worker_threads))  # emulate Umbra behavior here

        # configure WAL behavior
        config("wal_level", "replica")
        config("max_wal_senders", "0")
        config("wal_compression", "ON")
        config("full_page_writes", "ON")
        config("fsync", "ON")
        config("checkpoint_timeout", "1h")
        config("checkpoint_completion_target", "0.9")
        config("min_wal_size", "1GB")
        config("max_wal_size", "%dB" % self._buffer_size)  # emulate Umbra behavior here

        # configure parallelization
        config("max_worker_processes", "%d" % self._worker_threads)
        config("max_parallel_workers_per_gather", "%d" % self._worker_threads)
        config("max_parallel_maintenance_workers", "%d" % self._worker_threads)
        config("max_parallel_workers", "%d" % self._worker_threads)
        config("parallel_setup_cost", "0")
        config("parallel_tuple_cost", "0")

        # misc configuration
        config("default_transaction_isolation", "repeatable read")
        config("random_page_cost", "1")
        config("seq_page_cost", "1")
        config("enable_nestloop", "0")

        # user settings
        for key, value in self._settings.items():
            config(key, value)

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # write config file
        with open(os.path.join(self.host_dir.name, "postgres.conf"), "w") as file:
            self._write_config_file(file)

        # start Docker container
        postgres_environment = {
            'POSTGRES_PASSWORD': 'postgres',
            'PGDATA': '/db/postgres',
            'LC_COLLATE': 'C',
            'LC_CTYPE': 'C',
        }
        docker_params = {
            "shm_size": "%d" % self._buffer_size,
            "command": "postgres -c config_file=/db/postgres.conf",
        }
        self._host_port = self._host_port if self._host_port is not None else 54321
        self._start_container(postgres_environment, 5432, self._host_port, self.host_dir.name, "/db", docker_params=docker_params)
        self._connect("postgres", "postgres", "postgres", self._host_port)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self._close_container()
        if self.host_dir:
            self.host_dir.cleanup()

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> list[str]:
        return sql.copy_statements_postgres(schema, "/data")

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        timer = None
        timer_kill = None
        if timeout > 0:
            timer_kill = threading.Timer(timeout * 10, self._kill_container)
            timer_kill.start()
            timer = threading.Timer(timeout, self.connection.cancel)
            timer.start()

        begin = time.time()
        try:
            self.cursor.execute(query)

            result.rows = self.cursor.rowcount
            if fetch_result:
                if fetch_result_limit > 0:
                    result.result = self.cursor.fetchmany(fetch_result_limit)
                else:
                    result.result = self.cursor.fetchall()

            client_total = time.time() - begin
            result.client_total.append(client_total * 1000)

        except Exception as e:
            client_total = time.time() - begin
            if timer_kill is not None:
                timer_kill.cancel()
                timer_kill.join()
            if timer is not None:
                timer.cancel()
                timer.join()

            if self.connection.closed:
                raise e

            logger.log_error_verbose(str(e))
            result.message = str(e)
            result.state = Result.ERROR
            result.state = Result.TIMEOUT if "canceled" in result.message or "canceling statement due to user request" in result.message else result.state
            result.state = Result.OOM if "Cannot allocate " in result.message or "unable to allocate " in str(e) else result.state
            result.client_total.append(timeout * 1000 if result.state == Result.TIMEOUT else client_total * 1000)
            return result

        if timer_kill is not None:
            timer_kill.cancel()
            timer_kill.join()
        if timer is not None:
            timer.cancel()
            timer.join()

        return result

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False) -> QueryPlan:
        result = self._execute(query="explain (format json, analyze) " + query.strip(), fetch_result=True).result
        json_plan = result[0][0][0]
        plan_parser = PostgresParser(include_system_representation=include_system_representation)
        query_plan = plan_parser.parse_json_plan(query, json_plan)
        return query_plan


class PostgresDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'postgres'

    @staticmethod
    def get_description() -> str:
        return 'PostgreSQL'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return Postgres(benchmark, db_dir, data_dir, params, settings)

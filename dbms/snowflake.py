import os
import tempfile
import threading
import time

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from util import sql, logger
import snowflake.connector


class Snowflake(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)
        self._size = params.get("size", "XSMALL")
        self.db_name = SnowflakeDescription.get_database_name(benchmark, params)

    @property
    def name(self) -> str:
        return "snowflake"

    def connection_string(self) -> str:
        return "not available for Snowflake"

    def __enter__(self):
        self.conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            role='ACCOUNTADMIN'
        )
        self.cs = self.conn.cursor()
        logger.log_verbose_dbms(f"Established connection to Snowflake", self)

        self.warehouse_name = f"wh_{self.db_name}"
        self.database_name = f"db_{self.db_name}"
        self.schema_name = f"sc_{self.db_name}"

        self.cs.execute(f"CREATE WAREHOUSE IF NOT EXISTS {self.warehouse_name} \
                      WAREHOUSE_SIZE = '{self._size}' \
                      AUTO_SUSPEND = 300 \
                      AUTO_RESUME = TRUE")
        self.cs.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
        self.cs.execute(f"CREATE SCHEMA IF NOT EXISTS {self.database_name}.{self.schema_name}")

        self.cs.execute(f"USE WAREHOUSE {self.warehouse_name}")
        self.cs.execute(f"USE DATABASE {self.database_name}")
        self.cs.execute(f"USE SCHEMA {self.schema_name}")
        logger.log_verbose_dbms(f"Created warehouse {self.warehouse_name} (size: {self._size}) with database {self.database_name}", self)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cs.execute(f"DROP DATABASE IF EXISTS {self.database_name}")
        self.cs.execute(f"DROP WAREHOUSE IF EXISTS {self.warehouse_name}")
        logger.log_verbose_dbms(f"Dropped warehouse {self.warehouse_name} with database {self.database_name}", self)

        self.cs.close()
        self.conn.close()

    def _transform_schema(self, schema: dict) -> dict:
        schema = sql.transform_schema(schema, escape='', lowercase=False)
        for table in schema['tables']:
            for column in table['columns']:
                # rename bool to boolean
                t = column['type'].replace('bool', 'boolean')
                # update type
                column['type'] = t
        return schema

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> list[str]:
        return sql.copy_statements_snowflake(schema, self._data_dir)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        timer_kill = None
        if timeout > 0:
            def kill_warehouse():
                logger.log_dbms(f"Killing Snowflake warehouse {self.warehouse_name} due to timeout", self)
                logger.log_dbms(f"Killed Snowflake warehouse {self.warehouse_name} due to timeout", self)

            timer_kill = threading.Timer(timeout * 10, kill_warehouse)
            timer_kill.start()

        begin = time.time()
        try:
            self.cs.execute(query, timeout=timeout if timeout > 0 else None)

            # Get Snowflake Query ID
            sfqid = getattr(self.cs, 'sfqid', None)

            result.rows = self.cs.rowcount
            if fetch_result:
                if fetch_result_limit > 0:
                    result.result = self.cs.fetchmany(fetch_result_limit)
                else:
                    result.result = self.cs.fetchall()
            client_total = (time.time() - begin) * 1000

            if timer_kill is not None:
                timer_kill.cancel()
                timer_kill.join()

            # Get real warehouse execution time and additional metrics from query history
            if sfqid:
                try:
                    self.cs.execute(f"""
                        SELECT total_elapsed_time, execution_time, compilation_time, rows_produced, bytes_scanned
                        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                        WHERE QUERY_ID = '{sfqid}'
                    """)
                    row = self.cs.fetchone()
                    if row:
                        (total_elapsed_time, execution_time, compilation_time, rows_produced, bytes_scanned) = row
                        client_total = float(total_elapsed_time) if total_elapsed_time is not None else client_total
                        execution = float(execution_time) if execution_time is not None else None
                        compilation = float(compilation_time) if compilation_time is not None else None
                        total = execution_time + compilation_time if execution is not None and compilation is not None else None

                        extra = {}
                        extra["rows_produced"] = int(rows_produced) if rows_produced is not None else None
                        extra["bytes_scanned"] = int(bytes_scanned) if bytes_scanned is not None else None

                        if execution is not None:
                            result.execution.append(execution)
                        if compilation is not None:
                            result.compilation.append(compilation)
                        if total is not None:
                            result.total.append(total)
                        result.extra = extra
                except Exception as e:
                    logger.log_warn_verbose(f"Failed to fetch Snowflake execution metrics: {e}")

            result.client_total.append(client_total)

        except Exception as e:
            client_total = time.time() - begin
            if timer_kill is not None:
                timer_kill.cancel()
                timer_kill.join()

            if self.conn.is_closed():
                raise e

            logger.log_error_verbose(str(e))
            result.message = str(e)
            result.state = Result.ERROR
            result.state = Result.TIMEOUT if isinstance(e, snowflake.connector.ProgrammingError) and e.errno == 604 else result.state
            result.client_total.append(timeout * 1000 if result.state == Result.TIMEOUT else client_total * 1000)
            return result

        return result


class SnowflakeDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'snowflake'

    @staticmethod
    def get_description() -> str:
        return 'Snowflake'

    @staticmethod
    def get_database_name(benchmark: Benchmark, params: dict) -> str:
        size = params.get("size", "XSMALL").lower()
        return (DBMSDescription.get_database_name(benchmark, params) + "_" + size).replace(".", "_")

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return Snowflake(benchmark, db_dir, data_dir, params, settings)

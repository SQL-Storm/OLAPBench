import re
import tempfile
import threading
import time

import pyodbc

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, DBMSDescription, Result
from util import sql, logger


class SQLServer(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return 'sqlserver'

    @property
    def docker_image_name(self) -> str:
        return 'mcr.microsoft.com/mssql/server:2022-latest'

    def connection_string(self) -> str:
        return self._connection_string

    def _connect(self, connection: str):
        self.connection = None
        start_time = time.time()
        check_timeout = 120  # 2 minutes
        while time.time() - start_time < check_timeout:
            try:
                self.connection = pyodbc.connect(connection, ansi=True)
                self.connection.autocommit = True
                self.connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf8')
                self.connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf8')

                break
            except pyodbc.OperationalError as e:
                time.sleep(1)  # 1 second
            except pyodbc.InterfaceError as e:
                time.sleep(1)  # 1 second

        if self.connection is None:
            raise Exception("could not connect to sqlserver")

        self._connection_string = f"iusql \"{connection}\" -v"
        self.cursor = self.connection.cursor()

        logger.log_verbose_dbms(f"Established connection to {self.name}", self)

    def __enter__(self):
        # prepare database directories
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        sqlserver_environment = {
            "ACCEPT_EULA": "Y",
            "SA_PASSWORD": "yourStsrong(!)Password"
        }
        docker_params = {
            "shm_size": "%d" % self._buffer_size,
        }
        self._start_container(sqlserver_environment, 1433, 14331, self.host_dir.name, "/var/opt/mssql", docker_params=docker_params)
        self._connect("DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,14331;UID=SA;TrustServerCertificate=yes;PWD=yourStsrong(!)Password")

        # configure SQL server
        self.cursor.execute("EXEC sp_configure 'show advanced options', '1'")
        self.cursor.execute("RECONFIGURE WITH OVERRIDE")
        self.cursor.execute("EXEC sp_configure 'max server memory', %d" % (self._buffer_size // 1024))
        self.cursor.execute("EXEC sp_configure 'max degree of parallelism', '%d'" % self._worker_threads)
        self.cursor.execute("EXEC sp_configure 'default trace enabled', 0")
        self.cursor.execute("RECONFIGURE WITH OVERRIDE")
        self.cursor.execute("SET STATISTICS TIME ON;")

        logger.log_verbose_dbms(f"Prepared sqlserver database", self)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self._close_container()
        self.host_dir.cleanup()

    def _transform_schema(self, schema):
        schema = sql.transform_schema(schema, escape='"', lowercase=False)
        for table in schema['tables']:
            for column in table['columns']:
                notnull = "not null" in column['type']
                # remove not null from type
                intermediate = column['type'].replace('not null', '').strip()
                # bool is not supported in SQLServer
                intermediate = intermediate.replace('bool', 'tinyint')
                # varchar is called nvarchar in sqlserver
                if 'varchar' in intermediate:
                    intermediate = intermediate + ' COLLATE Latin1_General_100_CI_AS_SC_UTF8'
                # text is deprecated and will be replaced by varchar(max) in SQLServer
                intermediate = intermediate.replace('text', 'varchar(max) COLLATE Latin1_General_100_CI_AS_SC_UTF8')
                # the type timestamp is called datetime in sqlserver
                intermediate = intermediate.replace('timestamp', 'datetime2')
                # update type
                column['type'] = intermediate + (' not null' if notnull else '')
        return schema

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> list[str]:
        return sql.copy_statements_sqlserver(schema)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        timer = None
        timer_kill = None
        if timeout > 0:
            timer_kill = threading.Timer(timeout * 10, self._kill_container)
            timer_kill.start()
            timer = threading.Timer(timeout, self.cursor.cancel)
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
                    result.rows = len(result.result)

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

            # Server is gone raise exception for restart
            if "Lost connection to server during query" in str(e) or "Server has gone away" in str(e):
                raise e

            logger.log_error_verbose(str(e))
            result.message = str(e)
            result.state = Result.TIMEOUT if "Query execution was interrupted" in result.message or "Operation canceled" in result.message else Result.ERROR
            result.client_total.append(timeout * 1000 if result.state == Result.TIMEOUT else client_total * 1000)
            return result

        result.result = [list(row) for row in result.result]

        if timer_kill is not None:
            timer_kill.cancel()
            timer_kill.join()
        if timer is not None:
            timer.cancel()
            timer.join()

        for _, m in self.cursor.messages:
            if "Error" in m:
                result.message = m
                result.state = Result.TIMEOUT if "Query execution was interrupted" in result.message or "Operation canceled" in result.message else Result.ERROR
                result.client_total = []
                break
            elif "parse and compile time" in m:
                elapsed_time_match = re.search(r"elapsed time = (\d+) ms", m)
                t = int(elapsed_time_match.group(1))
                if t == 0:
                    continue
                result.compilation.append(t)
            elif "Execution Times" in m:
                elapsed_time_match = re.search(r"elapsed time = (\d+) ms", m)
                t = int(elapsed_time_match.group(1))
                if t == 0:
                    continue
                result.compilation.append(t)

        if len(result.compilation) == 1 and len(result.execution) == 1:
            result.total.append(result.compilation[0] + result.execution[0])

        return result

    def load_database(self):
        super().load_database()
        self.cursor.execute("DBCC CHECKDB")


class SQLServerDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'sqlserver'

    @staticmethod
    def get_description() -> str:
        return 'SQLServer'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return SQLServer(benchmark, db_dir, data_dir, params, settings)

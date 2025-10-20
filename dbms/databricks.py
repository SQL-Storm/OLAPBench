import os
import threading
import time
import base64

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from util import sql, logger

from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog


class Databricks(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)
        self._size = params.get("size", "2X-Small")
        self.db_name = DatabricksDescription.get_database_name(benchmark, params)
        self.warehouse = None
        self.uploaded_files = []

    @property
    def name(self) -> str:
        return "databricks"

    def connection_string(self) -> str:
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")
        return f"{host} ({token})" if host and token else "databricks"

    def __enter__(self):
        self.warehouse_name = f"wh_{self.db_name}"
        self.database_name = f"db_{self.db_name}"

        try:
            self.w = WorkspaceClient()
            self.warehouse = self.w.warehouses.create_and_wait(
                name=f"{self.warehouse_name}",
                cluster_size=self._size,
                max_num_clusters=1,
                auto_stop_mins=10,
                enable_serverless_compute=True,
                enable_photon=True,
            )

            self.w.warehouses.start_and_wait(self.warehouse.id)

            # establish connection
            self.connection = dbsql.connect(server_hostname=os.getenv("DATABRICKS_HOST"), http_path=self.warehouse.odbc_params.path, access_token=os.getenv("DATABRICKS_TOKEN"))
            self.cursor = self.connection.cursor()
            logger.log_verbose_dbms(f"Established connection to Databricks ({os.getenv('DATABRICKS_HOST')})", self)

            # create and switch to database
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            self.cursor.execute(f"USE {self.database_name}")
            logger.log_verbose_dbms(f"Using Databricks database {self.database_name}", self)

            logger.log_verbose_dbms(f"Created Databricks warehouse {self.warehouse.name} (size: {self._size}) with database {self.database_name}", self)

            return self
        except Exception:
            if self.warehouse is not None:
                self.w.warehouses.delete(self.warehouse.id)
                logger.log_verbose_dbms(f"Dropped Databricks warehouse {self.warehouse.name}", self)
                self.warehouse = None
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.cursor.close()
            self.connection.close()
        except Exception:
            pass

        for uploaded_file in self.uploaded_files:
            try:
                self.w.dbfs.delete(uploaded_file, recursive=True)
            except Exception as e:
                logger.log_error_verbose(f"Could not delete uploaded file {uploaded_file}: {e}")
                pass

        try:
            self.connection = dbsql.connect(server_hostname=os.getenv("DATABRICKS_HOST"), http_path=self.warehouse.odbc_params.path, access_token=os.getenv("DATABRICKS_TOKEN"))
            self.connection.cursor().execute(f"DROP DATABASE IF EXISTS {self.database_name} CASCADE")
            self.connection.close()
            logger.log_verbose_dbms(f"Dropped Databricks database {self.database_name}", self)
        except Exception:
            logger.log_error_verbose(f"Could not drop Databricks database {self.database_name}")
            pass

        if self.warehouse is not None:
            self.w.warehouses.delete(self.warehouse.id)
            logger.log_verbose_dbms(f"Dropped Databricks warehouse {self.warehouse.name}", self)

    def _transform_schema(self, schema: dict) -> dict:
        return schema

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> list[str]:
        with logger.LogProgress("Uploading files...", len(schema["tables"])) as progress:
            self.w.volumes.create(catalog_name="workspace", schema_name=self.database_name, name="files", volume_type=catalog.VolumeType.MANAGED)
            for table in schema["tables"]:
                progress.next(f'{os.path.basename(table["file"])}...')

                local_path = os.path.join(self._data_dir, table["file"])
                dbfs_path = f"/Volumes/workspace/{self.database_name}/files/{table['file']}"
                with open(local_path, "rb") as f:
                    raw = f.read()
                logger.log_verbose_dbms(f"Upload {local_path} to {dbfs_path}", self)
                self.w.files.upload(file_path=dbfs_path, contents=raw, overwrite=True)
                self.uploaded_files.append(dbfs_path)
                progress.finish()

        return sql.copy_statements_databricks(schema, f"/Volumes/workspace/{self.database_name}/files")

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        def kill_warehouse():
            self.w.warehouses.delete(self.warehouse.id)
            logger.log_verbose_dbms(f"Dropped Databricks warehouse {self.warehouse.name}", self)
            self.warehouse = None

        timer = None
        timer_kill = None
        if timeout > 0:
            timer_kill = threading.Timer(timeout * 10, kill_warehouse)
            timer_kill.start()
            timer = threading.Timer(timeout, self.cursor.cancel)
            timer.start()

        begin = time.time()
        try:
            self.cursor.execute(query)

            try:
                result.rows = self.cursor.rowcount
            except Exception:
                result.rows = None

            if fetch_result:
                if fetch_result_limit > 0:
                    result.result = self.cursor.fetchmany(fetch_result_limit)
                else:
                    result.result = self.cursor.fetchall()

            client_total = (time.time() - begin) * 1000
            result.client_total.append(client_total)

        except Exception as e:
            client_total = (time.time() - begin) * 1000
            if timer_kill is not None:
                timer_kill.cancel()
                timer_kill.join()
            if timer is not None:
                timer.cancel()
                timer.join()

            logger.log_error_verbose(str(e))
            result.message = str(e)
            result.state = Result.ERROR
            result.client_total.append(client_total)
            return result

        if timer_kill is not None:
            timer_kill.cancel()
            timer_kill.join()
        if timer is not None:
            timer.cancel()
            timer.join()

        return result


class DatabricksDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'databricks'

    @staticmethod
    def get_description() -> str:
        return 'Databricks SQL'

    @staticmethod
    def get_database_name(benchmark: Benchmark, params: dict) -> str:
        size = params.get("size", "2X-Small").lower().replace("-", "_")
        return (DBMSDescription.get_database_name(benchmark, params) + "_" + size).replace(".", "_")

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return Databricks(benchmark, db_dir, data_dir, params, settings)

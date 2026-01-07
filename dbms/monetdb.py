import os
import tempfile
import time

import docker
import docker.types
import pymonetdb

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result
from dbms.dbms import DBMSDescription
from util import sql, logger


class MonetDB(DBMS):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return "monetdb"

    @property
    def docker_image(self) -> str:
        return 'gitlab.db.in.tum.de:5005/schmidt/olapbench/monetdb:latest'

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        monet_environment = {
            'HOST_UID': os.getuid(),
            'HOST_GID': os.getgid(),
        }

        client = docker.from_env()
        client.images.pull(self.docker_image)

        self._host_port = self._host_port if self._host_port is not None else 50001

        self.container = client.containers.run(
            image=self.docker_image,
            auto_remove=True,
            detach=True,
            privileged=True,
            tty=True,
            environment=monet_environment,
            cpuset_cpus=self._cpuset_cpus,
            cpuset_mems=self._cpuset_mems,
            ports={
                "50000/tcp": self._host_port
            },
            volumes={
                self.host_dir.name: {"bind": "/var/monetdb5", "mode": "rw"},
                self._data_dir: {"bind": "/data", "mode": "ro"},
            },
        )
        self.container.start()
        time.sleep(2)
        self.connection = None

        # connect to MonetDB
        start_time = time.time()
        check_timeout = 120  # 2 minutes
        while time.time() - start_time < check_timeout:
            try:
                self.connection = pymonetdb.connect(database="main", user="monetdb", password="monetdb", host="localhost", port=self._host_port, autocommit=True)
                break
            except Exception as e:
                time.sleep(1)

        if self.connection is None:
            raise Exception("unable to connect to MonetDB")

        self.cursor = self.connection.cursor()

        # configure the session
        self.cursor.execute("call sys.setmemorylimit(%d)" % (self._buffer_size // (1024 * 1024)))
        self.cursor.execute("call sys.setworkerlimit(%d)" % self._worker_threads)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self.container.stop()
        self.host_dir.cleanup()

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> list[str]:
        return sql.copy_statements_monet(schema)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        self.cursor.execute(f"call sys.setquerytimeout({timeout})")

        begin = time.time()
        try:
            self.cursor.execute(query)
        except Exception as e:
            logger.log_error_verbose(str(e))
            result.message = str(e)
            result.state = Result.TIMEOUT if "HYT00!Query aborted due to timeout" in result.message else Result.ERROR
            result.client_total.append(timeout * 1000 if result.state == Result.TIMEOUT else client_total * 1000)
            return result

        result.rows = self.cursor.rowcount
        if fetch_result:
            # Extract column names from cursor description
            if self.cursor.description:
                result.columns = [desc[0] for desc in self.cursor.description]

            if fetch_result_limit > 0:
                result.result = self.cursor.fetchmany(fetch_result_limit)
            else:
                result.result = self.cursor.fetchall()

        client_total = time.time() - begin
        result.client_total.append(client_total * 1000)
        return result

    def load_database(self):
        super().load_database()
        self.cursor.execute("call sys.analyze()")


class MonetDBDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'monetdb'

    @staticmethod
    def get_description() -> str:
        return 'MonetDB'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return MonetDB(benchmark, db_dir, data_dir, params, settings)

import argparse
import csv
import math
import os
import re
import tempfile
import time

import simplejson as json
from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result
from dbms.umbra import UmbraDescription, Umbra
from queryplan.parsers.umbraparser import UmbraParser
from queryplan.queryplan import QueryPlan
from util import logger, sql
from util.process import Process


class UmbraDev(Umbra):
    prebuilt_versions = ["2025-01-23", "2024-02-04"]

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)
        self._version = params.get("version", "HEAD")

        self._umbra_db = os.path.join(self._db_dir, params["umbra_db"] if "umbra_db" in params else ".")
        self._umbra_cache = os.path.join(self._db_dir, "umbra_cache")

        self._cwd = os.getcwd()
        self._umbra_src = os.path.join(self._cwd, params["umbra_src"] if "umbra_src" in params else ".")
        self._bin_dir = os.path.join(self._cwd, params["bin"] if "bin" in params else "bin")

        self._database_name = UmbraDevDescription.get_database_name(benchmark, params)

        self.process = None

    @property
    def version(self) -> str:
        if self._version == "HEAD":
            return Process(f'git rev-parse {self._version}', cwd=self._umbra_src).run().split('\n')[0]
        return self._version

    def __enter__(self):
        # Prepare database directory
        os.makedirs(self._umbra_db, exist_ok=True)

        self.result_dir = tempfile.TemporaryDirectory(dir=self._umbra_db)
        self._umbra_db_client = self._umbra_db
        self._data_dir_client = self._data_dir

        # Prepare the sql binary
        if self._version == "HEAD":
            logger.log_verbose_dbms(f"Using umbra sql binary from {self._bin_dir}", self)
            self.sql = os.path.join(self._bin_dir, "sql")

        elif self._version == "latest" or re.match(r"\d{2}\.\d{2}(\.\d+)?", self._version):
            # Start Umbra in the docker container
            logger.log_verbose_dbms(f"Using umbra sql binary from docker container {self.docker_image_name}", self)
            env = " ".join([f"-e {key}={value}" for key, value in self.umbra_env().items()])
            self.sql = f"docker run --rm -i -v {self._umbra_db}:/var/db:rw -v {self._data_dir}:/data:ro --user {os.getuid()}:{os.getgid()} {env} {self.docker_image_name} umbra-sql"
            self._umbra_db_client = "/var/db"
            self._data_dir_client = "/data"

            # Pull the docker image
            self._pull_image()

        else:
            os.makedirs(self._umbra_cache, exist_ok=True)
            self._bin_dir = os.path.join(self._umbra_cache, self._version, "bin")
            self.sql = os.path.join(self._bin_dir, "sql")

            if os.path.isdir(self._bin_dir):
                # Use the cached binary
                logger.log_verbose_dbms(f"Using cached Umbra binary from {self._bin_dir}", self)

            elif self._version in self.prebuilt_versions:
                # Download a prebuilt version
                logger.log_verbose_dbms(f"Downloading prebuilt Umbra binary {self._version}", self)

                with tempfile.TemporaryDirectory(dir=self._db_dir) as build_dir:
                    Process(f"curl -O https://db.in.tum.de/~schmidt/umbra-{self._version}.tar.xz", cwd=build_dir).run()
                    Process(f"tar -xf umbra-{self._version}.tar.xz", cwd=build_dir).run()
                    Process(f'mv {os.path.join(build_dir, "umbra")} {os.path.join(self._umbra_cache, self._version)}').run()

            else:
                # Compile a git commit
                self._version = Process(f'git rev-parse {self._version}', cwd=self._umbra_src).run().split('\n')[0]
                logger.log_verbose_dbms(f"Compiling Umbra from source directory {self._umbra_src} at commit {self._version}", self)

                with tempfile.TemporaryDirectory(dir=self._db_dir) as build_dir:
                    Process(f'git clone -q {self._umbra_src} {build_dir}').run()
                    Process(f'git checkout -q {self._version}', cwd=build_dir).run()

                    Process("cppmake NODEBINFO=1 bin/sql", cwd=build_dir).run()
                    os.makedirs(self._bin_dir)
                    Process(f'cp {os.path.join(build_dir, "bin", "sql")} {self._bin_dir}').run()

        # The result directory might be different for the docker binaries
        self.result_dir_host = os.path.join(self._umbra_db, os.path.basename(self.result_dir.name))
        self.result_dir_client = os.path.join(self._umbra_db_client, os.path.basename(self.result_dir.name))

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.process is not None:
            self.process.stop()
        self.result_dir.cleanup()

    def _copy_statements(self, schema: dict) -> list[str]:
        return sql.copy_statements_postgres(schema, self._data_dir_client)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()
        result_file = os.path.join(self.result_dir_host, "result.csv")
        record_file = os.path.join(self.result_dir_host, "record.csv")

        if os.path.exists(result_file):
            os.remove(result_file)
        if os.path.exists(record_file):
            os.remove(record_file)

        with open(os.path.join(self.result_dir_host, "query.sql"), "w") as query_file:
            query_file.write(f'\\record {os.path.join(self.result_dir_client, "record.csv")}\n')
            query_file.write(f'\\o {os.path.join(self.result_dir_client, "result.csv") if fetch_result else "-"}\n')
            if timeout != 0:
                query_file.write(f'\\set debug.timeout {int(timeout * 1000)}\n')
            query_file.write(str(query))
            query_file.flush()

        begin = time.time()
        self.process.write(f'\\i {os.path.join(self.result_dir_client, "query.sql")}')

        output: str = None
        client_total = math.nan
        while output is None:
            output = self.process.readline_stderr()
            client_total = (time.time() - begin) * 1000

            if "execution:" in output and "compilation:" in output:
                break
            elif output.startswith("ERROR:"):
                logger.log_error_verbose(output)
                result.client_total.append(client_total)
                result.message = output
                result.state = Result.OOM if "unable to allocate memory" in output else Result.ERROR
                return result
            elif output.startswith("WARNING: query") and "timed out after" in output:
                logger.log_error_verbose(output)
                result.client_total.append(timeout * 1000)
                result.message = output
                result.state = Result.TIMEOUT
                return result
            elif output:
                logger.log_warn_verbose(output)

            output = None

        execution = math.nan
        compilation = math.nan
        extra = {}

        try:
            [execution, compilation] = re.findall(r'([0-9.]*) avg', output)
            execution = float(execution) * 1000
            compilation = float(compilation) * 1000
        except Exception:
            pass

        try:
            with open(record_file, "r") as file:
                reader = csv.DictReader(file)
                for row in reader:
                    execution = float(row["execution_time"]) * 1000
                    compilation = float(row["compilation_time"]) * 1000
                    perf_counters = json.loads(row["perf_counters"], allow_nan=True)
                    for k in perf_counters.keys():
                        extra[k] = perf_counters[k]
                    extra["scale"] = float(row["scale"])
                    extra["ipc"] = float(row["ipc"])
                    extra["cpus"] = float(row["cpus"])

                    querycounters = row if self._version == "25.01" else json.loads(row["querycounters"], allow_nan=True)
                    for k in querycounters.keys():
                        if "." in k:
                            extra[k] = float(querycounters[k])
                    break
        except Exception:
            pass

        result.execution.append(execution)
        result.compilation.append(compilation)
        result.total.append(execution + compilation)
        result.client_total.append(client_total)
        result.extra = extra

        if fetch_result:
            with open(result_file, "r") as file:
                result.result = file.readlines()
                result.rows = len(result.result) - 1
                if len(result.result) > fetch_result_limit and fetch_result_limit > 0:
                    result.result = result.result[:fetch_result_limit]
        else:
            result.rows = -1

        return result

    def load_database(self):
        self.db = os.path.join(self._umbra_db, self._database_name + ".db")
        self.db_client = os.path.join(self._umbra_db_client, self._database_name + ".db")

        if os.path.isfile(self.db):
            command = f'{self.sql} {self.db_client}'
            logger.log_verbose_dbms(f"Starting umbra with an existing database `{command}`", self)
            self.db_exists = True
        else:
            command = f'{self.sql} --createdb {self.db_client}'
            logger.log_verbose_dbms(f"Starting umbra with a new database `{command}`", self)
            self.db_exists = False

        self._connection_string = f'{self.sql} {self.db_client}'

        env = self.umbra_env()
        self.process = Process(command, env=env)
        self.process.start()
        time.sleep(1)

        super().load_database()
        self.process.write(f'set profiling = on;')
        time.sleep(1)
        self.process.read_and_discard()

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False) -> QueryPlan:
        result = self._execute(query="explain (format json, analyze) " + query.strip(), fetch_result=True).result
        text_plan = "".join(result)
        json_plan = json.loads(text_plan, allow_nan=True)
        plan_parser = UmbraParser(include_system_representation=include_system_representation)
        query_plan = plan_parser.parse_json_plan(query, json_plan)
        return query_plan


class UmbraDevDescription(UmbraDescription):

    @staticmethod
    def get_name() -> str:
        return 'umbradev'

    @staticmethod
    def get_description() -> str:
        return 'Umbra Dev'

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        UmbraDescription.add_arguments(parser, ".")

        parser.add_argument("--umbra-src", dest="umbra_src", type=str, default=".", help="the umbra database to use (relative to the current work dir)")
        parser.add_argument("--bin", dest="bin", type=str, default="bin", help="the binary directory ")

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return UmbraDev(benchmark, db_dir, data_dir, params, settings)

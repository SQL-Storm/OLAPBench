import argparse
import os
import re
from abc import ABC, abstractmethod
from enum import Enum
from statistics import median
from typing import Optional, List, Dict

import docker
from benchmarks.benchmark import Benchmark
from queryplan.queryplan import QueryPlan
from util import numa, logger, formatter, sql


class Result:
    SUCCESS = "success"
    ERROR = "error"
    FATAL = "fatal"
    OOM = "oom"
    TIMEOUT = "timeout"
    GLOBAL_TIMEOUT = "global_timeout"

    def __init__(self):
        self.state: Result.State = Result.SUCCESS
        self.client_total: List[float] = []
        self.total: List[float] = []
        self.execution: List[float] = []
        self.compilation: List[float] = []
        self.rows: Optional[int] = None
        self.extra: Dict[str, float] = {}
        self.result: List[List[any]] = []
        self.message: str = ""
        self.plan: Optional[QueryPlan] = None

    def merge(self, other: 'Result'):
        """
        Merge the results of two runs of the same query.

        Args:
            other (Result): The other result to merge with.
        """

        # Update the state if the other result is in a worse state
        self.state = other.state if other.state != Result.SUCCESS else self.state

        # Add runtimes to the lists
        self.client_total.extend(other.client_total)
        self.total.extend(other.total)
        self.execution.extend(other.execution)
        self.compilation.extend(other.compilation)

        # Update the number of rows
        self.rows = other.rows if other.rows is not None else self.rows

        # Update the additional information
        self.extra = other.extra if not self.extra else self.extra
        self.result = other.result if not self.result else self.result
        self.message = other.message or self.message
        self.plan = other.plan or self.plan

    def round(self, decimals: int):
        """
        Round all float values in the object's attributes to the specified number of decimal places.

        Args:
            decimals (int): The number of decimal places to round to.
        """
        self.client_total = [round(x, decimals) for x in self.client_total]
        self.total = [round(x, decimals) for x in self.total]
        self.execution = [round(x, decimals) for x in self.execution]
        self.compilation = [round(x, decimals) for x in self.compilation]
        self.extra = {k: round(v, decimals) for k, v in self.extra.items()}


def _parse_bytes(input: str) -> int:
    """
    Convert a string representing a memory size with units into an integer number of bytes.

    Args:
        input (str): A string representing the memory size, e.g., "10K", "512M", "1G".
                     The string must consist of a number followed by a unit (B, K, M, G, T).

    Returns:
        int: The memory size in bytes.

    Raises:
        ValueError: If the input string is not in the correct format.
    """
    units = {"B": 1, "K": 2 ** 10, "M": 2 ** 20, "G": 2 ** 30, "T": 2 ** 40}
    match = re.fullmatch(r"(\d+)([BKMGT])", input)
    if match:
        value, unit = match.groups()
        return int(value) * units[unit]
    raise ValueError(f"malformed memory specification: {input}")


class DBMS(ABC):
    class Index(Enum):
        NONE = "none"
        PRIMARY = "primary"
        FOREIGN = "foreign"

        @staticmethod
        def from_string(s: str) -> 'DBMS.Index':
            try:
                return DBMS.Index[s.upper()]
            except KeyError:
                raise ValueError(f"Invalid index type: {s}")

        def __str__(self) -> str:
            return self.value

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        self._benchmark = benchmark
        self._db_dir = db_dir
        self._data_dir = data_dir

        self._numa_node = params["numa_node"] if "numa_node" in params else None
        self._cpuset_cpus = numa.get_cpus(self._numa_node)
        self._cpuset_mems = numa.get_mems(self._numa_node)
        self._buffer_size = params["buffer_size"] if "buffer_size" in params and params["buffer_size"] is not None else numa.get_memory_size(self._numa_node) / 2
        self._worker_threads = params["worker_threads"] if "worker_threads" in params and params["worker_threads"] is not None else numa.get_thread_count(self._numa_node)
        self._index = DBMS.Index.from_string(params.get("index", "primary"))
        self._version = params.get("version", "latest")
        self._umbra_planner = params.get("umbra_planner", False)
        self._docker = docker.from_env()

        self._settings = settings

        self.container = None

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    def version(self) -> str:
        return self._version

    @property
    def docker_image_name(self) -> str:
        pass

    def _pull_image(self):
        # Pull the docker image
        logger.log_dbms(f"Pulling {self.docker_image_name} docker image", self)
        try:
            return self._docker.images.pull(self.docker_image_name)
        except Exception as e:
            logger.log_dbms(f"Could not pull {self.docker_image_name} docker image: {e}", self)

    def _start_container(self, environment: dict, source_port: int, dest_port: int, source_db_dir: str, dest_db_dir: str, docker_params: dict = {}):
        # Pull the docker image
        image = self._pull_image()

        # Start the container
        try:
            self.container = self._docker.containers.run(
                image=image,
                auto_remove=False,
                detach=True,
                privileged=True,
                tty=True,
                user=f"{os.getuid()}:{os.getgid()}",
                environment=environment,
                cpuset_cpus=self._cpuset_cpus,
                cpuset_mems=self._cpuset_mems,
                ports={f"{source_port}/tcp": dest_port},
                volumes={
                    source_db_dir: {"bind": dest_db_dir, "mode": "rw"},
                    self._data_dir: {"bind": "/data", "mode": "ro"},
                },
                **docker_params
            )
            logger.log_dbms(f"Started {self.name} docker container", self)
        except Exception as e:
            logger.log_dbms(f"Could not start {self.name} docker container: {e}", self)
            raise Exception(f"Could not start {self.name} docker container")

    def _container_status(self) -> str:
        if self.container is None:
            return "not started"

        try:
            return self.client.containers.get(self.container.id).status
        except Exception:
            return "removed"

    def _kill_container(self):
        if self.container is not None:
            logger.log_dbms(f"Killing {self.name} docker container", self)
            self.container.kill()
            self.container.wait(timeout=None, condition="removed")
            logger.log_dbms(f"Killed {self.name} docker container", self)

    def _close_container(self):
        if self.container is not None:
            self.container.stop(timeout=300)
            logger.log_dbms(f"Stopped {self.name} docker container", self)

    def _transform_schema(self, schema: dict) -> dict:
        return sql.transform_schema(schema, escape='"', lowercase=False)

    @abstractmethod
    def _create_table_statements(self, schema: dict) -> list[str]:
        pass

    @abstractmethod
    def _copy_statements(self, schema: dict) -> list[str]:
        pass

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        raise NotImplementedError()

    def load_database(self):
        primary_key = self._index in [DBMS.Index.PRIMARY, DBMS.Index.FOREIGN]
        foreign_keys = self._index == DBMS.Index.FOREIGN
        schema = self._benchmark.get_schema(primary_key=primary_key, foreign_keys=foreign_keys)
        schema = self._transform_schema(schema)

        create_stmts = self._create_table_statements(schema)
        for create_statement in create_stmts:
            logger.log_verbose_sql(create_statement)
            output = self._execute(create_statement, False)
            if output.state != Result.SUCCESS:
                logger.log_error(f'Error while creating table: {output.message}')
                raise Exception(f'Error while creating table: {output.message}')

        statements = self._copy_statements(schema)
        non_empty_tables = [table for table in schema['tables'] if not table.get("initially empty", False) and not self._benchmark.empty()]

        with logger.LogProgress("Loading tables...", len(statements)) as progress:
            j = 0
            for table in schema['tables']:
                progress.next(f'Loading {table["name"]}...')
                time = 0.0
                if table in non_empty_tables:
                    for _ in range(int(len(statements) / len(non_empty_tables))):
                        logger.log_verbose_sql(statements[j])
                        output = self._execute(statements[j], False)
                        if output.state != Result.SUCCESS:
                            logger.log_error(f'Error while loading table: {output.message}')
                            raise Exception(f'Error while loading table: {output.message}')
                        time += output.client_total[0]
                        progress.finish()
                        j += 1
                if "additional_sql_insert" in schema:
                    table_insert_statements = [sql["query"] for sql in schema["additional_sql_insert"] if "tags" in sql and table["name"] in sql.get("tags")]
                    for stmt in table_insert_statements:
                        logger.log_verbose_sql(stmt)
                        output = self._execute(stmt, False)
                        if output.state != Result.SUCCESS:
                            logger.log_error(f'Error while executing additional insert: {output.message}')
                            raise Exception(f'Error while executing additional insert: {output.message}')
                        time += output.client_total[0]

                logger.log_verbose_dbms(f'Loaded {table["name"]} in {formatter.format_time(time)}', self)

        table_names = {table["name"] for table in schema['tables']}
        if "additional_sql_insert" in schema:
            with logger.LogProgress("Executing additional queries...", len(schema["additional_sql_insert"])) as progress:
                for statement in schema["additional_sql_insert"]:
                    if "tags" in statement and (set(statement.get("tags")) <= table_names):
                        continue
                    progress.next('Executing additional query...')
                    logger.log_verbose_sql(statement["query"])
                    output = self._execute(statement["query"], False)
                    if output.state != Result.SUCCESS:
                        logger.log_error(f'Error while executing additional query: {output.message}')
                        raise Exception(f'Error while executing additional query: {output.message}')
                    time = output.client_total[0]
                    progress.finish()
                    logger.log_verbose_dbms(f'Executed additional query in {formatter.format_time(time)}', self)

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False) -> QueryPlan:
        return None

    def connection_string(self) -> str:
        return None


class DBMSDescription:
    """
    Abstract class that provides a description of a DBMS and methods to interact with it.
    """

    @staticmethod
    def get_name() -> str:
        """
        Returns the name of the DBMS.
        """
        raise NotImplementedError()

    @staticmethod
    def get_description() -> str:
        """
        Returns a description of the DBMS.
        """
        raise NotImplementedError()

    @staticmethod
    def get_database_name(benchmark: Benchmark, params: dict) -> str:
        """
        Returns the name of the database for the given benchmark and parameters.

        :param benchmark: The benchmark instance.
        :param params: A dictionary of parameters.
        :return: The unique name of the database.
        """
        index = params.get("index", DBMS.Index.PRIMARY)
        index = "_foreignkeys" if index == DBMS.Index.FOREIGN else ("_nokeys" if index == DBMS.Index.NONE else "")

        return benchmark.unique_name + index

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        """
        Adds command-line arguments for the DBMS.

        :param parser: The argument parser instance.
        """
        parser.add_argument('--buffer-size', type=_parse_bytes, default=None, help="The desired buffer pool size.")
        parser.add_argument('--worker-threads', type=int, default=None, help="The desired number of worker threads.")
        parser.add_argument('--numa-node', type=int, default=None, help="Bind execution to a specific NUMA node.")
        parser.add_argument("--index", dest="index", type=DBMS.Index.from_string, choices=list(DBMS.Index), default=DBMS.Index.PRIMARY, help="Which indexes to build (default: primary).")

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        """
        Instantiates a DBMS instance.

        :param benchmark: The benchmark instance.
        :param db_dir: The directory for the database.
        :param data_dir: The directory for the data.
        :param params: A dictionary of parameters.
        :param settings: A dictionary of settings.
        :return: An instance of the DBMS.
        """
        raise NotImplementedError()


def database_systems() -> Dict[str, DBMSDescription]:
    """
    Returns a dictionary of all database descriptions.

    Returns:
        Dict[str, DBMSDescription]: A dictionary mapping DBMS names to their description classes.
    """
    from dbms import apollo, cedardb, clickhouse, databricks, duckdb, hyper, monetdb, postgres, singlestore, snowflake, sqlserver, umbra, umbradev

    dbms_list = [
        apollo.ApolloDescription,
        cedardb.CedarDBDescription,
        clickhouse.ClickHouseDescription,
        databricks.DatabricksDescription,
        duckdb.DuckDBDescription,
        hyper.HyperDescription,
        monetdb.MonetDBDescription,
        postgres.PostgresDescription,
        singlestore.SingleStoreDescription,
        snowflake.SnowflakeDescription,
        sqlserver.SQLServerDescription,
        umbra.UmbraDescription,
        umbradev.UmbraDevDescription
    ]
    return {dbms.get_name(): dbms for dbms in dbms_list}

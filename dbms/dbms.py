import argparse
import os
import re
from abc import ABC, abstractmethod
from enum import Enum
from statistics import median
import threading
from typing import Any, Optional, List, Dict

import docker
from benchmarks.benchmark import Benchmark
from queryplan.queryplan import QueryPlan
from util import numa, formatter, sql
from util.log import log


class Result:
    SUCCESS = "success"
    ERROR = "error"
    FATAL = "fatal"
    OOM = "oom"
    TIMEOUT = "timeout"
    GLOBAL_TIMEOUT = "global_timeout"

    def __init__(self):
        self.state: str = Result.SUCCESS
        self.client_total: List[float] = []
        self.total: List[float] = []
        self.execution: List[float] = []
        self.compilation: List[float] = []
        self.rows: Optional[int] = None
        self.extra: Dict[str, float] = {}
        self.result: List[List[Any]] = []
        self.columns: List[str] = []
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
        self.columns = other.columns if not self.columns else self.columns
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


def _parse_cpuset(cpuset: str) -> list[int]:
    """Parse a Docker cpuset_cpus string (e.g. "0-7,16-23") into a list of CPU ids."""
    cpus: list[int] = []
    for part in cpuset.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, high = part.split("-")
            cpus.extend(range(int(low), int(high) + 1))
        else:
            cpus.append(int(part))
    return cpus


# Allocator that hands out CPU sets to containers. We restrict how many CPUs a container gets
# (so it sees the configured core count) but spread the choice across the machine so multiple
# concurrently running containers don't all land on the same cores. Usage is reference-counted
# so CPUs freed by a destructed DBMS become preferred for the next allocation.
_cpu_alloc_lock = threading.Lock()
_cpu_usage: dict[int, int] = {}  # cpu id -> number of live containers currently assigned to it


def _allocate_cpuset(cpuset: str, count: int) -> tuple[str, list[int]]:
    """
    Pick `count` CPUs out of the available pool, preferring the least-used ones.

    `cpuset` is the pool of CPUs the container may use ("" means all host CPUs). Returns the
    chosen cpuset string and the list of CPU ids reserved (so they can be released later). The
    reservation is reference-counted; pass the returned list to `_release_cpus` on teardown.
    """
    available = _parse_cpuset(cpuset) if cpuset else list(range(numa.get_thread_count(None)))
    if not available or count >= len(available):
        # Nothing to restrict — let the container use the whole pool (nothing to reserve/release).
        return cpuset, []

    with _cpu_alloc_lock:
        # Prefer the least-used CPUs; tie-break by id for determinism.
        chosen = sorted(available, key=lambda c: (_cpu_usage.get(c, 0), c))[:count]
        for c in chosen:
            _cpu_usage[c] = _cpu_usage.get(c, 0) + 1

    return ",".join(str(c) for c in chosen), chosen


def _release_cpus(cpus: list[int]):
    """Return previously reserved CPUs (from `_allocate_cpuset`) to the pool."""
    if not cpus:
        return
    with _cpu_alloc_lock:
        for c in cpus:
            remaining = _cpu_usage.get(c, 0) - 1
            if remaining > 0:
                _cpu_usage[c] = remaining
            else:
                _cpu_usage.pop(c, None)


class DBMS(ABC):
    # Whether this system runs in a managed Docker container (via _start_container) and can
    # therefore have the `cpus`/`memory` restrictions enforced. Subclasses that run outside
    # such a container (e.g. a local binary) override this to False.
    enforces_resource_limits = True

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

        # `cpus` and `memory` define the resource budget. They are enforced at the container
        # level (cpuset_cpus + mem_limit) AND handed to each DBMS so it sizes its own buffer pool
        # and parallelism to match. The latter matters because most systems read the *host's*
        # resources rather than the cgroup, and would otherwise ignore the cap (and get
        # OOM-killed). `memory` may be given as a string with a unit suffix (e.g. "32G").
        memory = params.get("memory")
        if isinstance(memory, str):
            memory = _parse_bytes(memory)
        self._memory_limit = memory  # only set when explicitly configured; drives the Docker mem_limit

        cpus = params.get("cpus")
        self._cpus_limit = cpus  # only set when explicitly configured; drives the Docker cpuset

        # Values passed to the DBMS configuration: the configured budget if set, else the host's.
        self._memory = int(memory) if memory is not None else numa.get_memory_size(self._numa_node) / 2
        self._cpus = int(cpus) if cpus is not None else numa.get_thread_count(self._numa_node)

        # Restrict the container to `cpus` cores. The specific CPUs are picked by an allocator
        # that spreads concurrent containers across the machine instead of all sharing the same
        # cores; the reserved CPUs are released again when this DBMS is destructed (see __del__).
        self._allocated_cpus: list[int] = []
        if self._cpus_limit is not None:
            self._cpuset_cpus, self._allocated_cpus = _allocate_cpuset(self._cpuset_cpus, int(self._cpus_limit))

        # Warn if limits were requested but this system can't have them enforced via Docker.
        if (self._cpus_limit is not None or self._memory_limit is not None) and not self.enforces_resource_limits:
            log.warn(f"{self.name} does not run in a managed Docker container; cannot enforce cpus/memory restrictions")
        self._index = DBMS.Index.from_string(params.get("index", "primary"))
        self._version = params.get("version", "latest")
        self._umbra_planner = params.get("umbra_planner", False)
        self._docker = docker.from_env()
        self._host_port = params.get('host_port', None)

        self._settings = settings

        self.container = None

    def __del__(self):
        # Release any CPUs this instance reserved so they can be reused by the next DBMS. Guarded
        # because __del__ may run during interpreter shutdown when globals are already torn down.
        try:
            cpus = self.__dict__.get("_allocated_cpus")
            if cpus:
                _release_cpus(cpus)
                self._allocated_cpus = []
        except Exception:
            pass

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
        log.dbms(f"Pulling {self.docker_image_name} docker image", self)
        try:
            return self._docker.images.pull(self.docker_image_name)
        except Exception as e:
            log.dbms(f"Could not pull {self.docker_image_name} docker image: {e}", self)

    def _start_container(self, environment: dict, source_port: int, dest_port: int, source_db_dir: str, dest_db_dir: str, docker_params: Optional[dict] = None):
        # Pull the docker image
        image = self._pull_image()

        # Merge any extra bind mounts requested by the DBMS with the defaults
        docker_params = dict(docker_params or {})
        volumes = {
            source_db_dir: {"bind": dest_db_dir, "mode": "rw"},
            self._data_dir: {"bind": "/data", "mode": "ro"},
        }
        volumes.update(docker_params.pop("volumes", {}))

        # Constrain the container to the configured budget so the system inside sees a smaller
        # machine. mem_limit caps the memory cgroup (the kernel OOM-kills the container if it is
        # exceeded); cpuset_cpus (set in __init__) restricts it to the requested number of cores,
        # so the reported core count matches. Only applied when `memory`/`cpus` were configured.
        if self._memory_limit is not None:
            docker_params.setdefault("mem_limit", int(self._memory_limit))

        # Start the container
        try:
            self.container = self._docker.containers.run(
                image=image,
                auto_remove=True,
                detach=True,
                privileged=True,
                tty=True,
                user=f"{os.getuid()}:{os.getgid()}",
                environment=environment,
                cpuset_cpus=self._cpuset_cpus,
                cpuset_mems=self._cpuset_mems,
                ports={f"{source_port}/tcp": dest_port},
                volumes=volumes,
                **docker_params
            )
            log.dbms(f"Started {self.name} docker container", self)
        except Exception as e:
            log.dbms(f"Could not start {self.name} docker container: {e}", self)
            raise Exception(f"Could not start {self.name} docker container")

    def _container_status(self) -> str:
        if self.container is None:
            return "not started"
        try:
            return self._docker.containers.get(self.container.id).status
        except Exception:
            return "removed"

    def _execute_in_container(self, command: str, timeout: int = 0):
        timer = None
        if timeout > 0:
            timer = threading.Timer(timeout, self._kill_container)
            timer.start()

        log.process_verbose(command)
        result = self.container.exec_run(command)

        if timer is not None:
            timer.cancel()
            timer.join()

        if result.exit_code != 0:
            log.process_verbose(result.output.decode('utf-8'))
            raise Exception(result.output.decode('utf-8'))
        else:
            if result.output:
                log.process_verbose(result.output.decode('utf-8').strip())

        return result

    def _kill_container(self):
        if self.container is not None:
            log.dbms(f"Killing {self.name} docker container", self)
            self.container.kill()
            self.container.wait(timeout=None, condition="removed")
            log.dbms(f"Killed {self.name} docker container", self)

    def _close_container(self):
        if self.container is not None:
            self.container.stop(timeout=300)
            log.dbms(f"Stopped {self.name} docker container", self)

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
            log.sql_verbose(create_statement)
            output = self._execute(create_statement, False)
            if output.state != Result.SUCCESS:
                log.error(f'Error while creating table: {output.message}')
                raise Exception(f'Error while creating table: {output.message}')

        statements = self._copy_statements(schema)
        non_empty_tables = [table for table in schema['tables'] if not table.get("initially empty", False) and not self._benchmark.empty()]

        if len(statements) == 0:
            return

        with log.progress("Loading tables...", len(statements)) as progress:
            j = 0
            total_time = 0.0
            for table in schema['tables']:
                progress.next(f'Loading {table["name"]}...')
                time = 0.0
                if table in non_empty_tables:
                    for _ in range(int(len(statements) / len(non_empty_tables))):
                        log.sql_verbose(statements[j])
                        output = self._execute(statements[j], False)
                        if output.state != Result.SUCCESS:
                            log.error(f'Error while loading table: {output.message}')
                            raise Exception(f'Error while loading table: {output.message}')
                        time += output.client_total[0]
                        progress.finish()
                        j += 1

                log.dbms_verbose(f'Loaded {table["name"]} in {formatter.format_time(time)}', self)
                total_time += time

            log.dbms(f'Loaded database in {formatter.format_time(total_time)}', self)

    def benchmark_query(self, queries: list[tuple[str, str]], repetitions: int, warmup: int, timeout: int = 0, fetch_result: bool = True) -> dict[str, Result]:
        results: dict[str, Result] = {}

        with log.progress("Running queries...", len(queries) * (repetitions + warmup), base=repetitions + warmup) as progress:
            for (name, query) in queries:
                result = Result()

                progress.next(f'Running {name}...')
                for i in range(warmup):
                    self._execute(query, fetch_result, timeout=timeout)
                    progress.finish()

                for i in range(repetitions):
                    result.merge(self._execute(query, fetch_result, timeout=timeout))
                    progress.finish()

                results[name] = result

                med = median(result.client_total) if len(result.client_total) > 0 else float('nan')
                log.dbms_verbose(f'{name} {formatter.format_time(med)} ({result.rows} row)', self)

        return results

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False, timeout: int = 0) -> Optional[QueryPlan]:
        return None

    def connection_string(self) -> Optional[str]:
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
        parser.add_argument('--cpus', type=int, default=None, help="Restrict the container to this many CPUs (enforced via Docker only).")
        parser.add_argument('--memory', type=_parse_bytes, default=None, help="Restrict the container to this much memory (enforced via Docker only).")
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
    from dbms import apollo, cedardb, clickhouse, duckdb, hyper, monetdb, postgres, singlestore, sqlserver, umbra, umbradev

    dbms_list = [
        apollo.ApolloDescription, cedardb.CedarDBDescription, clickhouse.ClickHouseDescription,
        duckdb.DuckDBDescription, hyper.HyperDescription, monetdb.MonetDBDescription,
        postgres.PostgresDescription, singlestore.SingleStoreDescription, sqlserver.SQLServerDescription,
        umbra.UmbraDescription, umbradev.UmbraDevDescription
    ]
    return {dbms.get_name(): dbms for dbms in dbms_list}

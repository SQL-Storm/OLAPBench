import argparse
import os
import tempfile
from enum import Enum

import docker
import docker.types
import simplejson as json
from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS
from dbms.dbms import DBMSDescription
from dbms.postgres import Postgres
from queryplan.parsers.umbraparser import UmbraParser
from queryplan.queryplan import QueryPlan
from util import sql, logger


class Umbra(Postgres):
    class Relation(str, Enum):
        DEFAULT = "default"
        MAPPED = "mapped"
        PAGED = "paged"
        BLOCK = "block"
        COLUMN = "columnar"

        @staticmethod
        def from_string(s: str):
            try:
                return Umbra.Relation[s.upper()]
            except KeyError:
                raise ValueError()

        def __str__(self):
            return self.value

    class IndexMethod(str, Enum):
        BTREE = "btree"
        UNCHECKED = "unchecked"

        @staticmethod
        def from_string(s: str):
            try:
                return Umbra.IndexMethod[s.upper()]
            except KeyError:
                raise ValueError()

        def __str__(self):
            return self.value

    class Backend(str, Enum):
        BUFFER = "buffer"
        FILESYSTEM = "filesystem"
        CLOUD = "cloud"

        @staticmethod
        def from_string(s: str):
            try:
                return Umbra.Backend[s.upper()]
            except KeyError:
                raise ValueError()

        def __str__(self):
            return self.value

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

        self._relation = params["relation"] if "relation" in params else Umbra.Relation.DEFAULT
        self._backend = params["backend"] if "backend" in params else Umbra.Backend.FILESYSTEM
        self._blockshift = params["blockshift"] if "blockshift" in params else 18
        self._s3_bucket = params["s3_bucket"] if "s3_bucket" in params else None
        self._access_key_id = params["access_key_id"] if "access_key_id" in params else None
        self._access_key = params["access_key"] if "access_key" in params else None
        self._indexMethod = params["indexMethod"] if "indexMethod" in params else Umbra.IndexMethod.BTREE
        self._fk = params["foreignkeys"] if "foreignkeys" in params else False

        self._client_threads = params["client-threads"] if "client-threads" in params else 10
        self._pipeline_depth = params["pipeline-depth"] if "pipeline-depth" in params else 64

        self._umbra_db = params["umbra_db"] if "umbra_db" in params else None

    @property
    def name(self) -> str:
        return "umbra"

    @property
    def docker_image(self) -> str:
        return f'gitlab.db.in.tum.de:5005/schmidt/olapbench/umbra:{self._version}'

    def umbra_env(self):
        env = {
            "OPTIMIZER_JSON_TYPES": "1",
            "OPTIMIZER_JSON_IUS": "1",
            "INDEX_METHOD": str(self._indexMethod).upper(),
            "BUFFERSIZE": "%dB" % self._buffer_size,
            "PARALLEL": "%d" % self._worker_threads,
        }

        for k in self._settings.keys():
            value = self._settings[k]
            k = k.upper().replace(".", "_")
            if isinstance(value, str):
                env[k] = value
            elif isinstance(value, bool):
                env[k] = "1" if value else "0"
            else:
                env[k] = str(value)

        logger.log_verbose_dbms(f"Umbra environment variables: {env}", self)

        return env

    def __enter__(self):
        # prepare database directory
        if self._umbra_db is None:
            self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)
            self.umbra_db_dir = self.host_dir.name
            logger.log_verbose_dbms(f"Using a temporary umbra database in {self.umbra_db_dir}", self)
        else:
            self.host_dir = None
            self.umbra_db_dir = os.path.join(self._db_dir, self._umbra_db, self.database_name)
            os.makedirs(self.umbra_db_dir, exist_ok=True)
            logger.log_verbose_dbms(f"Using a persistent umbra database in {self.umbra_db_dir}", self)

        self.db = os.path.join(self.umbra_db_dir, "umbra.db")
        self.db_exists = os.path.isfile(self.db)

        # start Docker container
        environment = self.umbra_env()
        docker_params = {
            "ulimits": [docker.types.Ulimit(name="memlock", soft=2 ** 30, hard=2 ** 30)],
        }
        self._start_container(environment, 5432, 54322, self.umbra_db_dir, "/var/db", docker_params=docker_params)
        self._connect("postgres", "postgres", "postgres", 54322)

        return self

    def _storage_params(self) -> [str]:
        if self._relation == Umbra.Relation.DEFAULT:
            return []
        elif self._relation == Umbra.Relation.COLUMN:
            return [f"storage={self._relation}", f"backend={self._backend}", f"blocksize={2 ** self._blockshift}"]
        else:
            return [f"storage={self._relation}"]

    def _create_table_statements(self, schema: dict) -> [str]:
        statements = sql.create_table_statements(schema, storage_parameters=self._storage_params())
        if self._backend == Umbra.Backend.CLOUD:
            statements.insert(0, f"create remote storage s3 using '{self._s3_bucket}' with secret '{self._access_key_id}' '{self._access_key}';")

        return statements

    def load_database(self):
        if not self.db_exists:
            logger.log_verbose_dbms("Loading umbra database " + self.db, self)
            super().load_database()
        else:
            logger.log_verbose_dbms("Using existing umbra database " + self.db, self)

    def plan_query(self, query: str, dialect: str) -> str:
        dialects = {
            "sqlserver": "sqlserver",
            "apollo": "sqlserver",
            "clickhouse": "clickhouse",
            "singlestore": "mysql",
        }
        dialect = "postgresql" if dialect not in dialects else dialects[dialect]
        res = self._execute(f"explain (sql, dialect {dialect}) {query}", True)
        if res.error:
            return None
        return "".join(res.result)

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False) -> QueryPlan:
        result = self._execute(query="explain (format json, analyze) " + query.strip(), fetch_result=True).result
        if not result or not result[0]:
            return None
        text_plan = result[0][0]
        json_plan = json.loads(text_plan, allow_nan=True)
        plan_parser = UmbraParser(include_system_representation=include_system_representation)
        query_plan = plan_parser.parse_json_plan(query, json_plan)
        return query_plan


class UmbraDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return "umbra"

    @staticmethod
    def get_description() -> str:
        return "Umbra"

    @staticmethod
    def get_database_name(benchmark: Benchmark, params: dict) -> str:
        relation = params.get("relation", Umbra.Relation.COLUMN)
        backend = params.get("backend", Umbra.Backend.FILESYSTEM)
        blockshift = params.get("blockshift", 18)
        indexMethod = params.get("indexMethod", Umbra.IndexMethod.BTREE)

        relation = "_" + relation + (f"_{backend}{blockshift}" if relation == Umbra.Relation.COLUMN else "")
        indexMethod = "_" + indexMethod if indexMethod != Umbra.IndexMethod.BTREE else ""
        return DBMSDescription.get_database_name(benchmark, params) + relation + indexMethod

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser, default_umbra_db: str = None):
        DBMSDescription.add_arguments(parser)

        parser.add_argument("-r", "--relation", dest="relation", type=Umbra.Relation.from_string, choices=list(Umbra.Relation), default=Umbra.Relation.COLUMN,
                            help="which relation to use (default: mapped)")
        parser.add_argument("-b", "--backend", dest="backend", type=Umbra.Backend.from_string, choices=list(Umbra.Backend), default=Umbra.Backend.FILESYSTEM,
                            help="the storage backend to use (default: filesystem)")
        parser.add_argument("-s", "--blockshift", dest="blockshift", type=int, default=18, help="the block size (default: 18)")

        parser.add_argument("--s3_bucket", dest="s3_bucket", type=str, default=None, help="")
        parser.add_argument("--access_key_id", dest="access_key_id", type=str, default=None, help="")
        parser.add_argument("--access_key", dest="access_key", type=str, default=None, help="")

        parser.add_argument("--indexMethod", dest="indexMethod", type=Umbra.IndexMethod.from_string, choices=list(Umbra.IndexMethod), default=Umbra.IndexMethod.BTREE,
                            help="which index method to use (default: btree)")
        parser.add_argument("--version", dest="version", type=str, default="HEAD", help="version of the umbra (default: HEAD)")

        parser.add_argument("--client-threads", dest="client-threads", type=int, default=10, help="the number of client threads to start, only used in oltp benchmarks (default: 10)")
        parser.add_argument("--pipeline-depth", dest="pipeline-depth", type=int, default=64, help="the pipeline depth, only used in oltp benchmarks (default: 64)")

        parser.add_argument("--umbra-db", dest="umbra_db", type=str, default=default_umbra_db, help="the umbra database to use")

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return Umbra(benchmark, db_dir, data_dir, params, settings)

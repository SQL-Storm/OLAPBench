import json
import tempfile

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, DBMSDescription
from dbms.duckdb import DuckDB
from queryplan.parsers.hyperparser import HyperParser
from queryplan.queryplan import QueryPlan
from util import sql


class Hyper(DuckDB):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return "hyper"

    def __enter__(self):
        # prepare database directory
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        docker_params = {}
        self._start_container({}, 5432, 54326, self.host_dir.name, "/db", docker_params=docker_params)
        self._connect(54326)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_container()
        if self.host_dir:
            self.host_dir.cleanup()

    def _create_table_statements(self, schema: dict) -> list[str]:
        statements = sql.create_table_statements(schema)
        statements = [s.replace("primary key", "assumed primary key") for s in statements]
        return statements

    def _copy_statements(self, schema: dict) -> list[str]:
        return sql.copy_statements_postgres(schema, "/data")

    def retrieve_query_plan(self, query: str, include_system_representation: bool = False) -> QueryPlan:
        result = self._execute(query="explain (format json, analyze) " + query.strip(), fetch_result=True).result
        json_plan = json.loads(result[0][0])["input"]
        plan_parser = HyperParser(include_system_representation=include_system_representation)
        query_plan = plan_parser.parse_json_plan(query, json_plan)
        return query_plan


class HyperDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'hyper'

    @staticmethod
    def get_description() -> str:
        return 'Tableau Hyper'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        return Hyper(benchmark, db_dir, data_dir, params, settings)

import tempfile

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, DBMSDescription
from dbms.sqlserver import SQLServer
from util import sql


class SingleStore(SQLServer):

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

    @property
    def name(self) -> str:
        return 'singlestore'

    @property
    def docker_image_name(self) -> str:
        return "singlestore/cluster-in-a-box:latest"

    def __enter__(self):
        # prepare database directories
        self.host_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        # start Docker container
        singlestore_environment = {
            "ROOT_PASSWORD": "SingleStore",
            "START_AFTER_INIT": "Y",
            "LICENSE_KEY": "BGFlODdhMGI4MTkyZDQzMjk5MjI2ZDEzYzAyMmEzY2IzjlxuZwAAAAAAAAAAAAAAAAkwNAIYLfSh1I1PbuEfRtEPWxLwdyKwQMZIGJUlAhgSLR+GTxtuGUSCuGxUab43dWJsHnmTMn4AAA=="
        }
        docker_params = {
            "command": ["/bin/sh", "-c", "ls -al /etc/memsql/memsqlctl.hcl && sed -i 's/user = \"memsql\"/user = \"local\"/' /etc/memsql/memsqlctl.hcl && /startup"]
        }
        self._start_container(singlestore_environment, 3306, 33061, self.host_dir.name, "/var/lib/memsql", docker_params=docker_params)
        self._connect("DRIVER={MariaDB};SERVER=127.0.0.1;PORT=33061;TrustServerCertificate=yes;UID=root;PWD=SingleStore;OPTION=" + str(67108864 + 1048576))

        self.cursor.execute("CREATE DATABASE benchy;")
        self.cursor.close()

        self._connect("DRIVER={MariaDB};SERVER=127.0.0.1;PORT=33061;DATABASE=benchy;TrustServerCertificate=yes;UID=root;PWD=SingleStore;OPTION=" + str(67108864 + 1048576))
        self.cursor.execute("SET sql_mode = 'ANSI_QUOTES';")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self._close_container()
        self.host_dir.cleanup()

    def _transform_schema(self, schema: dict) -> dict:
        schema = sql.transform_schema(schema, escape='"', lowercase=self._umbra_planner)
        for table in schema['tables']:
            for column in table['columns']:
                # text is limited to 65KB replace with longtext
                column['type'] = column['type'].replace('text', 'longtext')
        return schema

    def _create_table_statements(self, schema: dict) -> [str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> [str]:
        return sql.copy_statements_singlestore(schema)

    def load_database(self):
        DBMS.load_database(self)

    def connection_string(self) -> str:
        return 'iusql "DRIVER={MariaDB};Server=127.0.0.1;Port=33061;DATABASE=benchy;TrustServerCertificate=yes;UID=root;PWD=SingleStore;OPTION=68157440" -v'


class SingleStoreDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'singlestore'

    @staticmethod
    def get_description() -> str:
        return 'SingleStore'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir, data_dir, params: dict, settings: dict) -> DBMS:
        return SingleStore(benchmark, db_dir, data_dir, params, settings)

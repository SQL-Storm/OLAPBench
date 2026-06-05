import os
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

    def _image_file(self, path: str) -> str:
        """Read a file from the image so we can extend it for the host user."""
        output = self._docker.containers.run(self.docker_image_name, entrypoint="cat", command=[path], remove=True)
        return output.decode()

    def _write_memsql_config(self) -> dict:
        """memsqlctl refuses to run unless the process user matches its config.

        The harness runs the container under the host UID/GID, which does not
        exist inside the image, so memsqlctl aborts. Provide a config without the
        "user" restriction plus passwd/group entries that map the host UID/GID to
        a name, and bind-mount them over the image's versions.
        """
        uid, gid = os.getuid(), os.getgid()
        self.config_dir = tempfile.TemporaryDirectory(dir=self._db_dir)

        hcl_path = os.path.join(self.config_dir.name, "memsqlctl.hcl")
        with open(hcl_path, "w") as f:
            f.write('version = 1\n')
            f.write('nodeMetadataFile = "/var/lib/memsql/nodes.hcl"\n')
            f.write('defaultInstallDir = "/var/lib/memsql"\n')

        passwd_path = os.path.join(self.config_dir.name, "passwd")
        with open(passwd_path, "w") as f:
            f.write(self._image_file("/etc/passwd"))
            f.write(f"local:x:{uid}:{gid}::/var/lib/memsql:/bin/sh\n")

        group_path = os.path.join(self.config_dir.name, "group")
        with open(group_path, "w") as f:
            f.write(self._image_file("/etc/group"))
            f.write(f"local:x:{gid}:\n")

        return {
            hcl_path: {"bind": "/etc/memsql/memsqlctl.hcl", "mode": "ro"},
            passwd_path: {"bind": "/etc/passwd", "mode": "ro"},
            group_path: {"bind": "/etc/group", "mode": "ro"},
        }

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
            "volumes": self._write_memsql_config()
        }
        self._host_port = self._host_port if self._host_port is not None else 33061
        self._start_container(singlestore_environment, 3306, self._host_port, self.host_dir.name, "/var/lib/memsql", docker_params=docker_params)
        self._connect(f"DRIVER={{MariaDB}};SERVER=127.0.0.1;PORT={self._host_port};TrustServerCertificate=yes;UID=root;PWD=SingleStore;OPTION=" + str(67108864 + 1048576))

        cursor = self.connection.cursor()
        cursor.execute("CREATE DATABASE benchy;")
        cursor.close()

        self._connect(f"DRIVER={{MariaDB}};SERVER=127.0.0.1;PORT={self._host_port};DATABASE=benchy;TrustServerCertificate=yes;UID=root;PWD=SingleStore;OPTION=" + str(67108864 + 1048576))
        cursor = self.connection.cursor()
        cursor.execute("SET sql_mode = 'ANSI_QUOTES';")
        cursor.close()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        self._close_container()
        self.host_dir.cleanup()
        self.config_dir.cleanup()

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
        port = getattr(self, '_host_port', 33061)
        return f'iusql "DRIVER={{MariaDB}};Server=127.0.0.1;Port={port};DATABASE=benchy;TrustServerCertificate=yes;UID=root;PWD=SingleStore;OPTION=68157440" -v'


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

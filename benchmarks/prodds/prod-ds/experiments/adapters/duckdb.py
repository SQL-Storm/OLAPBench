from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .base import EngineAdapter


class DuckDBAdapter(EngineAdapter):
    name = "duckdb"

    def __init__(self, config: Dict[str, Any], global_cfg, resource, logger):
        super().__init__(config, global_cfg, resource, logger)
        self.native_cfg = config.get("native") or {}
        self.docker_cfg = config.get("docker") or {}
        self.database_path = (
            config.get("database_path")
            or self.native_cfg.get("database_path")
            or self.docker_cfg.get("database_path")
        )

    def build_sql(self, sql: str, stage: str) -> str:
        statements = [f"PRAGMA threads={self.global_cfg.threads}"]
        if stage == "planning":
            statements.append(f"EXPLAIN {sql}")
        else:
            statements.append(sql)
        return ";\n".join(statements) + ";"

    def build_command(self, sql: str, stage: str) -> List[str]:
        if self.execution_mode == "native":
            cli = self.native_cfg.get("cli_path", "duckdb")
            db_path = self.database_path or ":memory:"
            return [cli, db_path, "-c", sql]

    def build_command_file(self, sql_file: str, stage: str) -> List[str]:
        if self.execution_mode == "native":
            cli = self.native_cfg.get("cli_path", "duckdb")
            db_path = self.database_path or ":memory:"
            return ["bash", "-c", f"{cli} {db_path} < {sql_file}"]

        image = self.docker_cfg.get("image")
        if not image:
            raise RuntimeError("DuckDB docker image not configured.")

        docker_args = ["docker", "run", "--rm"]
        docker_args += self._docker_resource_args()

        volumes = list(self.docker_cfg.get("volumes") or [])
        container_dir = self.docker_cfg.get("container_dir", "/data")
        db_path = self.database_path
        container_db = ":memory:"
        if db_path:
            host_db = Path(db_path)
            if host_db.exists():
                volumes.append(f"{host_db.parent}:{container_dir}")
                container_db = f"{container_dir}/{host_db.name}"
            else:
                container_db = db_path

        for vol in volumes:
            docker_args += ["-v", vol]

        cli_path = self.docker_cfg.get("cli_path", "duckdb")
        docker_args += [image, cli_path, container_db, "-c", sql]
        return docker_args

    def version(self) -> str:
        if self.execution_mode == "native":
            return self._version_native()
        return self._version_docker()

    def _version_native(self) -> str:
        from ..process import run_command

        result = run_command([self.native_cfg.get("cli_path", "duckdb"), "--version"], env=self.env)
        return (result.stdout or result.stderr).strip() or "unknown"

    def _version_docker(self) -> str:
        from ..process import run_command

        image = self.docker_cfg.get("image")
        if not image:
            return "unknown"
        result = run_command(["docker", "run", "--rm", image, "duckdb", "--version"], env=self.env)
        return (result.stdout or result.stderr).strip() or "unknown"

    def _docker_resource_args(self) -> List[str]:
        args: List[str] = ["--memory", str(self.global_cfg.memory_limit_bytes)]
        if self.global_cfg.cpu_affinity:
            cpus = ",".join(str(c) for c in self.global_cfg.cpu_affinity)
            args += ["--cpuset-cpus", cpus]
        return args

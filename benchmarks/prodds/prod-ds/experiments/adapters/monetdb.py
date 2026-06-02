from __future__ import annotations

import shlex
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import EngineAdapter
from ..process import run_command, run_shell_command, CommandResult
from ..utils import truncate_error


class MonetDBAdapter(EngineAdapter):
    name = "monetdb"

    def __init__(self, config: Dict[str, Any], global_cfg, resource, logger):
        super().__init__(config, global_cfg, resource, logger)
        self.native_cfg = config.get("native") or {}
        self.host = self.native_cfg.get("host", "localhost")
        self.port = int(self.native_cfg.get("port", 50000))
        self.user = self.native_cfg.get("user", "monetdb")
        self.password = self.native_cfg.get("password", "monetdb")
        self.dbname = self.native_cfg.get("dbname", "tpcds_sf10")
        self.mclient_path = self.native_cfg.get("mclient_path", "mclient")
        self.monetdb_path = self.native_cfg.get("monetdb_path", "monetdb")
        self.monetdbd_path = self.native_cfg.get("monetdbd_path", "monetdbd")
        self.farm_path = self.native_cfg.get("farm_path", "/var/lib/monetdb")
        self.start_command = self.native_cfg.get("start_command")
        self.stop_command = self.native_cfg.get("stop_command")

        # Set up authentication file for mclient
        auth_file = Path(tempfile.gettempdir()) / ".monetdb_experiments"
        auth_file.write_text(f"user={self.user}\npassword={self.password}\n")
        self.env["DOTMONETDBFILE"] = str(auth_file)

    def start(self) -> None:
        if self.start_command:
            result = run_shell_command(
                self.start_command,
                env=self.env,
                resource=self.resource,
                use_memory_limit=False,
            )
            if result.returncode != 0:
                msg = truncate_error(result.stderr or result.stdout)
                raise RuntimeError(f"MonetDB start failed: {msg}")
        self._wait_for_ready()

    def stop(self) -> None:
        if self.stop_command:
            run_shell_command(
                self.stop_command,
                env=self.env,
                resource=self.resource,
                use_memory_limit=False,
            )
        super().stop()

    def _wait_for_ready(self) -> None:
        retries = int(self.native_cfg.get("startup_retries", 15))
        delay = float(self.native_cfg.get("startup_delay_seconds", 2.0))
        for _ in range(retries):
            result = run_command(
                self._mclient_command("SELECT 1;"),
                env=self.env,
            )
            if result.returncode == 0:
                return
            import time
            time.sleep(delay)
        raise RuntimeError("MonetDB did not become ready in time.")

    def supports_planning(self) -> bool:
        return False

    def build_sql(self, sql: str, stage: str) -> str:
        # mclient -s requires a trailing semicolon; normalize_sql strips it
        if stage == "planning":
            return f"EXPLAIN {sql};"
        return f"{sql};"

    def build_command(self, sql: str, stage: str) -> List[str]:
        return self._mclient_command(sql)

    def build_command_file(self, sql_file: str, stage: str) -> List[str]:
        cmd = [self.mclient_path, "-l", "sql", "-d", self.dbname]
        if self.host:
            cmd += ["-h", str(self.host)]
        cmd += ["-p", str(self.port)]
        # Read SQL from file via shell redirection
        return ["bash", "-c", " ".join(cmd) + f" < {sql_file}"]

    def _mclient_command(self, sql: str) -> List[str]:
        cmd = [self.mclient_path, "-l", "sql", "-d", self.dbname]
        if self.host:
            cmd += ["-h", str(self.host)]
        cmd += ["-p", str(self.port)]
        cmd += ["-s", sql]
        return cmd

    def use_shell(self) -> bool:
        return False

    def check_alive(self) -> bool:
        result = run_command(
            self._mclient_command("SELECT 1;"),
            env=self.env,
            timeout_s=10,
        )
        return result.returncode == 0
    
    def version(self) -> str:
        # Try querying server version
        result = run_command(
            self._mclient_command("SELECT sys.environment() WHERE name = 'monet_version';"),
            env=self.env,
            timeout_s=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            for line in result.stdout.splitlines():
                line = line.strip()
                if line and not line.startswith('%') and not line.startswith('#'):
                    return f"MonetDB {line}"
        # Fallback to mserver5
        result = run_command(["mserver5", "--version"], env=self.env)
        out = (result.stdout or result.stderr or "").strip()
        for line in out.splitlines():
            if "MonetDB" in line:
                return line.strip()
        return out.splitlines()[0] if out else "unknown"

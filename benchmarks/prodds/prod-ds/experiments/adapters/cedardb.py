from __future__ import annotations

from typing import Any, Dict, List

from .postgres import PostgresAdapter
from ..process import run_command
from ..utils import truncate_error


class CedarDBAdapter(PostgresAdapter):
    name = "cedardb"

    def start(self) -> None:
        super().start()
        self._apply_session_settings()

    def _session_settings(self) -> List[str]:
        # Apply settings once in _apply_session_settings, not per query.
        return []

    def _apply_session_settings(self) -> None:
        settings = []
        threads = self.global_cfg.threads
        if threads <= 1:
            settings = [
                "SET max_parallel_workers_per_gather = 0",
                "SET max_parallel_workers = 0",
            ]
        else:
            settings = [
                f"SET max_parallel_workers_per_gather = {threads}",
                f"SET max_parallel_workers = {threads}",
            ]

        sql = ";\n".join(settings) + ";"
        cmd = self._psql_command(sql)
        # Do not fail if CedarDB does not support these settings.
        result = run_command(cmd, env=self.env)
        if result.returncode != 0:
            msg = truncate_error(result.stderr or result.stdout)
            self.logger.warning("CedarDB SET options failed (continuing): %s", msg)

    def __init__(self, config: Dict[str, Any], global_cfg, resource, logger):
        super().__init__(config, global_cfg, resource, logger)
        # Force password for CedarDB audit
        self.env["PGPASSWORD"] = "postgres"

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..process import CommandResult, run_command, run_shell_command
from ..utils import normalize_sql, truncate_error

# Queries larger than this are written to a temp file to avoid ARG_MAX limits
_MAX_INLINE_SQL_BYTES = 128 * 1024  # 128 KB


@dataclass
class RunResult:
    status: str
    return_code: Optional[int]
    error_type: Optional[str]
    error_message: Optional[str]
    wall_time_ms_total: Optional[int]
    wall_time_ms_planning: Optional[int]
    wall_time_ms_execution: Optional[int]
    peak_rss_bytes: Optional[int]


class EngineAdapter:
    name = "base"

    def __init__(self, config: Dict[str, Any], global_cfg, resource, logger):
        self.config = config
        self.global_cfg = global_cfg
        self.resource = resource
        self.logger = logger
        self.execution_mode = global_cfg.execution_mode
        self.env = self._build_env()
        self.server_process = None
        self.server_cgroup = None

    def _build_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env_cfg = self.config.get("env") or {}
        for key, value in env_cfg.items():
            env[str(key)] = str(value)
        return env

    def start(self) -> None:
        return None

    def stop(self) -> None:
        if self.server_process:
            self.server_process.terminate()
            self.server_process.wait(timeout=10)
            self.server_process = None
        if self.server_cgroup:
            self.server_cgroup.cleanup()
            self.server_cgroup = None

    def restart(self) -> None:
        self.stop()
        self.start()

    def load_data(self) -> None:
        load_command = self.config.get("load_command")
        if not load_command:
            return
        self.logger.info("Loading data for %s...", self.name)
        result = run_shell_command(
            load_command,
            env=self.env,
            resource=self.resource,
            use_memory_limit=False,
        )
        if result.returncode != 0:
            msg = truncate_error(result.stderr or result.stdout)
            raise RuntimeError(f"Load command failed for {self.name}: {msg}")

    def version(self) -> str:
        return "unknown"

    def supports_planning(self) -> bool:
        return True

    def build_sql(self, sql: str, stage: str) -> str:
        return sql

    def build_command(self, sql: str, stage: str) -> List[str]:
        raise NotImplementedError

    def use_shell(self) -> bool:
        return False

    def monitor_pids(self) -> List[int]:
        return []

    def is_connection_error(self, stderr: str, stdout: str) -> bool:
        msg = f"{stderr}\n{stdout}".lower()
        needles = [
            "connection refused",
            "connection reset",
            "server closed the connection",
            "could not connect",
            "connection to server was lost",
            "connection terminated",
            "broken pipe",
        ]
        return any(n in msg for n in needles)

    def is_oom(self, result: CommandResult) -> bool:
        if result.returncode in {137, 143, -9}:
            return True
        msg = f"{result.stderr}\n{result.stdout}".lower()
        return "out of memory" in msg or "oom" in msg

    def check_alive(self) -> bool:
        if self.server_process:
            return self.server_process.poll() is None
        return True

    def _execute(self, sql: str, timeout_s: int, stage: str) -> CommandResult:
        sql_bytes = len(sql.encode("utf-8"))
        monitor_pids = self.monitor_pids()

        # For large SQL that would exceed ARG_MAX, write to a temp file
        # and let the adapter use stdin redirection instead.
        if sql_bytes > _MAX_INLINE_SQL_BYTES:
            return self._execute_via_file(sql, timeout_s, stage, monitor_pids)

        cmd = self.build_command(sql, stage)
        if self.use_shell():
            return run_shell_command(
                " ".join(cmd),
                timeout_s=timeout_s,
                env=self.env,
                resource=self.resource,
                monitor_pids=monitor_pids,
            )
        return run_command(
            cmd,
            timeout_s=timeout_s,
            env=self.env,
            resource=self.resource,
            monitor_pids=monitor_pids,
        )

    def _execute_via_file(self, sql: str, timeout_s: int, stage: str,
                          monitor_pids: Optional[List[int]] = None) -> CommandResult:
        """Execute large SQL via temp file + stdin redirection."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", delete=False, encoding="utf-8"
        ) as f:
            f.write(sql)
            tmp_path = f.name
        try:
            cmd = self.build_command_file(tmp_path, stage)
            return run_command(
                cmd,
                timeout_s=timeout_s,
                env=self.env,
                resource=self.resource,
                monitor_pids=monitor_pids,
            )
        finally:
            os.unlink(tmp_path)

    def build_command_file(self, sql_file: str, stage: str) -> List[str]:
        """Build command that reads SQL from a file instead of inline.

        Override in subclasses if the engine supports file-based execution.
        Default: falls back to shell redirection via 'sh -c'.
        """
        # Fallback: build the inline command but replace SQL with stdin redirect
        # This won't work for all engines, so subclasses should override.
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support file-based SQL execution. "
            f"Override build_command_file() for queries >128KB."
        )

    def run_sql(self, sql: str) -> RunResult:
        planning_timeout = self.global_cfg.timeout_seconds_planning
        execution_timeout = self.global_cfg.timeout_seconds_execution

        sql_clean = normalize_sql(sql)
        planning_ms = None
        execution_ms = None
        peak_rss = None
        return_code = None
        error_message = None
        status = "success"
        error_type = None

        if self.global_cfg.planning_enabled and self.supports_planning():
            plan_sql = self.build_sql(sql_clean, stage="planning")
            plan_res = self._execute(plan_sql, planning_timeout, stage="planning")
            planning_ms = plan_res.wall_time_ms
            peak_rss = max([x for x in [peak_rss, plan_res.peak_rss_bytes] if x is not None], default=None)
            if plan_res.timed_out:
                status = "timeout_planning"
                error_type = status
                error_message = truncate_error(plan_res.stderr or plan_res.stdout)
                return_code = plan_res.returncode
                return RunResult(
                    status=status,
                    return_code=return_code,
                    error_type=error_type,
                    error_message=error_message,
                    wall_time_ms_total=planning_ms,
                    wall_time_ms_planning=planning_ms,
                    wall_time_ms_execution=None,
                    peak_rss_bytes=peak_rss,
                )
            if plan_res.returncode != 0:
                status = "error"
                if self.is_oom(plan_res):
                    status = "oom"
                error_type = status
                error_message = truncate_error(plan_res.stderr or plan_res.stdout)
                return_code = plan_res.returncode
                if status == "error" and (self.is_connection_error(plan_res.stderr, plan_res.stdout) or not self.check_alive()):
                    status = "engine_crash"
                    error_type = status
                return RunResult(
                    status=status,
                    return_code=return_code,
                    error_type=error_type,
                    error_message=error_message,
                    wall_time_ms_total=planning_ms,
                    wall_time_ms_planning=planning_ms,
                    wall_time_ms_execution=None,
                    peak_rss_bytes=peak_rss,
                )

        exec_sql = self.build_sql(sql_clean, stage="execution")
        exec_res = self._execute(exec_sql, execution_timeout, stage="execution")
        execution_ms = exec_res.wall_time_ms
        peak_rss = max([x for x in [peak_rss, exec_res.peak_rss_bytes] if x is not None], default=None)
        return_code = exec_res.returncode

        if exec_res.timed_out:
            status = "timeout_execution"
        elif exec_res.returncode != 0:
            status = "error"
            if self.is_oom(exec_res):
                status = "oom"

        if status != "success":
            error_type = status
            error_message = truncate_error(exec_res.stderr or exec_res.stdout)
            if status == "error" and (self.is_connection_error(exec_res.stderr, exec_res.stdout) or not self.check_alive()):
                status = "engine_crash"
                error_type = status

        total_ms = execution_ms
        if planning_ms is not None and execution_ms is not None:
            total_ms = planning_ms + execution_ms

        return RunResult(
            status=status,
            return_code=return_code,
            error_type=error_type,
            error_message=error_message,
            wall_time_ms_total=total_ms,
            wall_time_ms_planning=planning_ms,
            wall_time_ms_execution=execution_ms,
            peak_rss_bytes=peak_rss,
        )

    def run_sql_raw(self, sql: str, *, timeout_s: Optional[int] = None) -> CommandResult:
        exec_sql = self.build_sql(normalize_sql(sql), stage="execution")
        return self._execute(
            exec_sql,
            timeout_s if timeout_s is not None else self.global_cfg.timeout_seconds_execution,
            stage="execution",
        )

from __future__ import annotations

import os
import shutil
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple


@dataclass
class CgroupHandle:
    path: Path

    def cleanup(self) -> None:
        try:
            self.path.rmdir()
        except OSError:
            return


class ResourceController:
    def __init__(self, memory_limit_bytes: int, cpu_affinity: Optional[List[int]], logger):
        self.memory_limit_bytes = memory_limit_bytes
        self.cpu_affinity = cpu_affinity
        self.logger = logger
        self._taskset_path = shutil.which("taskset")
        self._cgroup_root = Path("/sys/fs/cgroup")
        self._cgroup_available = self._detect_cgroup_v2()
        self._cgroup_writable = self._cgroup_available and os.access(self._cgroup_root, os.W_OK)

        if self.cpu_affinity and not self._taskset_path:
            self.logger.warning("cpu_affinity provided but taskset not found; ignoring CPU affinity.")
            self.cpu_affinity = None

        if self._cgroup_available and not self._cgroup_writable:
            self.logger.warning("cgroup v2 detected but not writable; falling back to ulimit for memory limits.")

    def _detect_cgroup_v2(self) -> bool:
        return (self._cgroup_root / "cgroup.controllers").exists()

    def create_cgroup(self, name: Optional[str] = None) -> Optional[CgroupHandle]:
        if not (self._cgroup_available and self._cgroup_writable):
            return None

        group_name = name or f"experiments_{uuid.uuid4().hex}"
        path = self._cgroup_root / group_name
        try:
            path.mkdir(exist_ok=False)
            (path / "memory.max").write_text(str(self.memory_limit_bytes))
            # Best effort: avoid swap if supported
            swap_file = path / "memory.swap.max"
            if swap_file.exists():
                try:
                    swap_file.write_text("0")
                except OSError:
                    pass
            return CgroupHandle(path=path)
        except OSError as exc:
            self.logger.warning("Failed to create cgroup (%s); falling back to ulimit.", exc)
            self._cgroup_writable = False
            return None

    def attach_self_to_cgroup(self, handle: CgroupHandle) -> None:
        (handle.path / "cgroup.procs").write_text(str(os.getpid()))

    def attach_pid(self, handle: CgroupHandle, pid: int) -> None:
        (handle.path / "cgroup.procs").write_text(str(pid))

    def wrap_command(
        self,
        cmd: List[str],
        *,
        use_memory_limit: bool = True,
        cgroup_handle: Optional[CgroupHandle] = None,
    ) -> Tuple[List[str], Optional[Callable[[], None]]]:
        preexec_fn: Optional[Callable[[], None]] = None

        if self.cpu_affinity:
            cpus = ",".join(str(c) for c in self.cpu_affinity)
            cmd = [self._taskset_path, "-c", cpus] + cmd

        if use_memory_limit and cgroup_handle:
            def _attach() -> None:
                self.attach_self_to_cgroup(cgroup_handle)
            preexec_fn = _attach
        elif use_memory_limit and not cgroup_handle and not self._cgroup_writable:
            # ulimit -v expects KB
            limit_kb = max(1, int(self.memory_limit_bytes // 1024))
            cmd_str = "ulimit -v {limit}; exec {cmd}".format(
                limit=limit_kb,
                cmd=shlex_join(cmd),
            )
            cmd = ["bash", "-lc", cmd_str]

        return cmd, preexec_fn


def shlex_join(cmd: List[str]) -> str:
    # Minimal replacement for shlex.join (Python 3.11 has it, but keep explicit).
    import shlex

    return " ".join(shlex.quote(part) for part in cmd)

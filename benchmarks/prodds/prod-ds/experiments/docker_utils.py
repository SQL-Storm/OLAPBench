from __future__ import annotations

import shlex
from typing import Dict, List, Optional, Sequence

from .process import run_command


def _docker_names(args: List[str]) -> List[str]:
    result = run_command(["docker"] + args)
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def container_exists(name: str) -> bool:
    names = _docker_names(["ps", "-a", "--filter", f"name=^{name}$", "--format", "{{.Names}}"])
    return name in names


def container_running(name: str) -> bool:
    names = _docker_names(["ps", "--filter", f"name=^{name}$", "--format", "{{.Names}}"])
    return name in names


def run_container(
    *,
    name: str,
    image: str,
    ports: Optional[Sequence[str]] = None,
    env: Optional[Dict[str, str]] = None,
    volumes: Optional[Sequence[str]] = None,
    extra_args: Optional[Sequence[str]] = None,
    command: Optional[Sequence[str] | str] = None,
    resource_args: Optional[Sequence[str]] = None,
) -> None:
    cmd: List[str] = ["docker", "run", "-d", "--name", name]

    if resource_args:
        cmd += list(resource_args)

    for port in ports or []:
        cmd += ["-p", port]
    for vol in volumes or []:
        cmd += ["-v", vol]
    for key, value in (env or {}).items():
        cmd += ["-e", f"{key}={value}"]
    cmd += list(extra_args or [])
    cmd.append(image)

    if command:
        if isinstance(command, str):
            cmd += shlex.split(command)
        else:
            cmd += list(command)

    result = run_command(cmd)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)


def start_container(name: str) -> None:
    run_command(["docker", "start", name])


def stop_container(name: str) -> None:
    run_command(["docker", "stop", name])


def restart_container(name: str) -> None:
    run_command(["docker", "restart", name])

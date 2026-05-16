from __future__ import annotations

import datetime as dt
import json
import os
import platform
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import psutil


def iso_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def normalize_sql(sql: str) -> str:
    sql = sql.strip()
    lines = sql.splitlines()
    # Strip leading empty lines and SQL comments
    # (dsqgen emits "-- start query ..." headers that confuse mclient -s)
    while lines:
        first = lines[0].strip()
        if not first or first.startswith("--"):
            lines.pop(0)
        else:
            break
    # Strip trailing empty lines, SQL comments, and bare semicolons
    # (dsqgen emits "-- end query ..." footers that confuse mclient -s)
    while lines:
        last = lines[-1].strip()
        if not last or last.startswith("--") or last == ";":
            lines.pop()
        else:
            break
    sql = "\n".join(lines)
    # Remove trailing semicolons for safe prefixing
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()
    return sql


def truncate_error(msg: str, limit: int = 2000) -> str:
    msg = msg.strip()
    if len(msg) <= limit:
        return msg
    return msg[: limit - 3] + "..."


def read_sql_files(directory: str) -> List[Path]:
    path = Path(directory)
    if not path.exists():
        raise FileNotFoundError(f"SQL directory not found: {directory}")
    files = sorted(path.glob("*.sql"), key=_sql_sort_key)
    return files


def _sql_sort_key(path: Path) -> Any:
    stem = path.stem
    match = re.search(r"(\d+)", stem)
    if match:
        return (int(match.group(1)), stem)
    return (999999, stem)


def load_sql_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def get_host_info() -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "os": platform.system(),
        "os_release": platform.release(),
        "os_version": platform.version(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "memory_total_bytes": psutil.virtual_memory().total,
    }

    cpu_model = _cpu_model()
    if cpu_model:
        info["cpu_model"] = cpu_model

    return info


def _cpu_model() -> Optional[str]:
    if platform.system() == "Linux":
        cpuinfo = Path("/proc/cpuinfo")
        if cpuinfo.exists():
            for line in cpuinfo.read_text(encoding="utf-8").splitlines():
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[-1].strip()
    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True)
            return out.strip()
        except Exception:
            return None
    return None


def find_git_hash(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        target = Path(path)
        if target.is_file():
            target = target.parent
        result = subprocess.check_output(
            ["git", "-C", str(target), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return result.strip()
    except Exception:
        return None


def find_git_root(path: Optional[str]) -> Optional[str]:
    if not path:
        return None
    try:
        target = Path(path)
        if target.is_file():
            target = target.parent
        result = subprocess.check_output(
            ["git", "-C", str(target), "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return result.strip()
    except Exception:
        return None


def dump_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def first_n(iterable: Iterable[Any], n: int) -> List[Any]:
    out: List[Any] = []
    for item in iterable:
        out.append(item)
        if len(out) >= n:
            break
    return out

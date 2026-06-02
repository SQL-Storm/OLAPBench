from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


class ConfigError(RuntimeError):
    pass


@dataclass
class GlobalConfig:
    execution_mode: str
    threads: int
    memory_limit_bytes: int
    timeout_seconds_planning: int = 300
    timeout_seconds_execution: int = 1800
    cpu_affinity: Optional[List[int]] = None
    warmup_queries: int = 5
    repetitions: int = 3
    results_dir: str = "results"
    planning_enabled: bool = True
    log_level: str = "INFO"


@dataclass
class BenchConfig:
    path: Path
    root_dir: Path
    raw: Dict[str, Any]
    global_cfg: GlobalConfig
    engines: Dict[str, Dict[str, Any]]
    experiments: Dict[str, Any]


def _require_key(obj: Dict[str, Any], key: str, ctx: str) -> Any:
    if key not in obj:
        raise ConfigError(f"Missing required key '{key}' in {ctx}.")
    return obj[key]


def _ensure_int(value: Any, key: str, ctx: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"Expected integer for '{key}' in {ctx} (got {value!r}).")


def _resolve_path(base: Path, value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    p = Path(value)
    if not p.is_absolute():
        p = (base / p).resolve()
    return str(p)


def load_config(path: str) -> BenchConfig:
    cfg_path = Path(path).expanduser().resolve()
    if not cfg_path.exists():
        raise ConfigError(f"Config file not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    if not isinstance(data, dict):
        raise ConfigError("Config must be a YAML mapping at the top level.")

    base_dir = cfg_path.parent

    global_raw = _require_key(data, "global", "top-level")
    if not isinstance(global_raw, dict):
        raise ConfigError("global must be a mapping.")

    execution_mode = str(_require_key(global_raw, "execution_mode", "global")).lower()
    if execution_mode not in {"native", "docker"}:
        raise ConfigError("global.execution_mode must be 'native' or 'docker'.")

    threads = _ensure_int(_require_key(global_raw, "threads", "global"), "threads", "global")
    memory_limit_bytes = _ensure_int(
        _require_key(global_raw, "memory_limit_bytes", "global"),
        "memory_limit_bytes",
        "global",
    )

    cpu_affinity = global_raw.get("cpu_affinity")
    if cpu_affinity is not None:
        if not isinstance(cpu_affinity, list) or not all(isinstance(x, int) for x in cpu_affinity):
            raise ConfigError("global.cpu_affinity must be a list of integers if provided.")

    global_cfg = GlobalConfig(
        execution_mode=execution_mode,
        threads=threads,
        memory_limit_bytes=memory_limit_bytes,
        timeout_seconds_planning=int(global_raw.get("timeout_seconds_planning", 300)),
        timeout_seconds_execution=int(global_raw.get("timeout_seconds_execution", 1800)),
        cpu_affinity=cpu_affinity,
        warmup_queries=int(global_raw.get("warmup_queries", 5)),
        repetitions=int(global_raw.get("repetitions", 3)),
        results_dir=str(global_raw.get("results_dir", "results")),
        planning_enabled=bool(global_raw.get("planning_enabled", True)),
        log_level=str(global_raw.get("log_level", "INFO")).upper(),
    )

    engines_raw = _require_key(data, "engines", "top-level")
    if not isinstance(engines_raw, dict) or not engines_raw:
        raise ConfigError("engines must be a non-empty mapping.")

    engines: Dict[str, Dict[str, Any]] = {}
    for name, cfg in engines_raw.items():
        if not isinstance(cfg, dict):
            raise ConfigError(f"engines.{name} must be a mapping.")

        if "execution_mode" in cfg or "mode" in cfg:
            raise ConfigError(
                f"engines.{name} attempts to override execution_mode. Remove per-engine mode overrides."
            )

        enabled = bool(cfg.get("enabled", True))
        load_command = cfg.get("load_command")
        if enabled and not load_command:
            raise ConfigError(f"engines.{name}.load_command is required for enabled engines.")

        native_cfg = cfg.get("native") or {}
        docker_cfg = cfg.get("docker") or {}
        if enabled:
            if execution_mode == "native" and not native_cfg:
                raise ConfigError(f"engines.{name}.native must be provided for native execution_mode.")
            if execution_mode == "docker" and not docker_cfg:
                raise ConfigError(f"engines.{name}.docker must be provided for docker execution_mode.")

        # Resolve a couple of common paths if present
        if "database_path" in native_cfg:
            native_cfg["database_path"] = _resolve_path(base_dir, native_cfg["database_path"])
        if "database_path" in cfg:
            cfg["database_path"] = _resolve_path(base_dir, cfg["database_path"])
        if "data_dir" in native_cfg:
            native_cfg["data_dir"] = _resolve_path(base_dir, native_cfg["data_dir"])
        if "db_dir" in native_cfg:
            native_cfg["db_dir"] = _resolve_path(base_dir, native_cfg["db_dir"])
        if "pid_file" in native_cfg:
            native_cfg["pid_file"] = _resolve_path(base_dir, native_cfg["pid_file"])

        engines[name] = cfg

    experiments = data.get("experiments") or {}
    if not isinstance(experiments, dict):
        raise ConfigError("experiments must be a mapping.")

    workload_cfg = experiments.get("workload_compare")
    if workload_cfg:
        if not isinstance(workload_cfg, dict):
            raise ConfigError("experiments.workload_compare must be a mapping.")
        workload_cfg["tpcds_dir"] = _resolve_path(base_dir, workload_cfg.get("tpcds_dir"))
        workload_cfg["prodds_dir"] = _resolve_path(base_dir, workload_cfg.get("prodds_dir"))

    join_cfg = experiments.get("join_scaling")
    if join_cfg:
        if not isinstance(join_cfg, dict):
            raise ConfigError("experiments.join_scaling must be a mapping.")
        join_cfg["output_dir"] = _resolve_path(base_dir, join_cfg.get("output_dir"))
        join_cfg["generator_output_file"] = _resolve_path(base_dir, join_cfg.get("generator_output_file"))

    union_cfg = experiments.get("union_scaling")
    if union_cfg:
        if not isinstance(union_cfg, dict):
            raise ConfigError("experiments.union_scaling must be a mapping.")
        union_cfg["output_dir"] = _resolve_path(base_dir, union_cfg.get("output_dir"))

    string_cfg = experiments.get("string_sweep")
    if string_cfg:
        if not isinstance(string_cfg, dict):
            raise ConfigError("experiments.string_sweep must be a mapping.")
        string_cfg["output_root"] = _resolve_path(base_dir, string_cfg.get("output_root"))
        string_cfg["query_subset_file"] = _resolve_path(base_dir, string_cfg.get("query_subset_file"))

    return BenchConfig(
        path=cfg_path,
        root_dir=base_dir,
        raw=data,
        global_cfg=global_cfg,
        engines=engines,
        experiments=experiments,
    )

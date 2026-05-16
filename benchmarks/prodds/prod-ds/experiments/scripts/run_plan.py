#!/usr/bin/env python3
"""
Plan runner for experiments.

Usage:
  python3 experiments/scripts/run_plan.py --plan experiments/plans/example.small.yaml

The plan YAML supports:
- plan_name: optional name for the plan
- base_config: path to a base experiments config (YAML)
- engines: list of engines to enable (duckdb, clickhouse, postgres, cedardb)
- system_mode: all | per_engine (default: all)
- matrix:
    threads: [..]
    scale_factors: [..]
    stringification_levels: [..]
- experiments: mapping of experiment name to config overrides
    workload_compare:
      enabled: true
    join_scaling:
      enabled: true
      join_counts: [...]
      seeds: [...]
      queries_per_join: ...
    string_sweep:
      enabled: true
      levels: [...]
- paths: templates for outputs
    root, data_dir, tpcds_dir, prodds_dir, join_dir, string_dir, config_dir,
    results_dir, engine_root, query_root
- generation: optional pre-generation commands
    data, tpcds_queries, prodds_queries (or any key), each with:
      enabled: true|false
      command: "..."
      output_dir: "{DATA_DIR}"
      manifest: ".gen_manifest.json"
      fingerprint: ["wrap_dsdgen.py", "workload/dsdgen"]
      reuse: true|false
      on_stale: regenerate|skip|fail
      requires_experiments: ["workload_compare"]

Placeholders available in templates:
  {PLAN_NAME} {RUN_ID} {RUN_TS} {SF} {STR} {THREADS}
  {ROOT} {DATA_DIR} {TPCDS_DIR} {PRODDS_DIR} {JOIN_DIR} {STRING_DIR}
  {CONFIG_DIR} {RESULTS_DIR} {ENGINE_ROOT} {QUERY_ROOT}
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime
from itertools import product
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]


class PlanError(RuntimeError):
    pass


def _require_yaml() -> Any:
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise PlanError("PyYAML is required. Install it via experiments/scripts/setup_eval_env.sh") from exc
    return yaml


def read_yaml(path: Path) -> Dict[str, Any]:
    yaml = _require_yaml()
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise PlanError(f"Expected YAML mapping at top level: {path}")
    return data


def write_yaml(path: Path, data: Dict[str, Any]) -> None:
    yaml = _require_yaml()
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False)


def listify(value: Any, default: List[Any]) -> List[Any]:
    if value is None:
        return list(default)
    if isinstance(value, list):
        return value
    return [value]


def resolve_path(base: Path, value: str | None) -> Path | None:
    if value is None:
        return None
    raw = Path(value)
    if raw.is_absolute():
        return raw
    return (base / raw).resolve()


def deep_merge(dst: Dict[str, Any], src: Mapping[str, Any]) -> Dict[str, Any]:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            deep_merge(dst[key], value)
        else:
            dst[key] = copy.deepcopy(value)
    return dst


class _SafeDict(dict):
    def __missing__(self, key: str) -> str:  # pragma: no cover - trivial
        return "{" + key + "}"


def format_string(template: str, ctx: Mapping[str, Any]) -> str:
    try:
        return template.format_map(_SafeDict(ctx))
    except Exception:
        return template


def render_templates(obj: Any, ctx: Mapping[str, Any]) -> Any:
    if isinstance(obj, str):
        return format_string(obj, ctx)
    if isinstance(obj, list):
        return [render_templates(v, ctx) for v in obj]
    if isinstance(obj, dict):
        return {k: render_templates(v, ctx) for k, v in obj.items()}
    return obj


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def hash_dir(path: Path) -> str:
    hasher = hashlib.sha256()
    for file in sorted(p for p in path.rglob("*") if p.is_file()):
        rel = str(file.relative_to(path)).encode("utf-8")
        hasher.update(rel)
        hasher.update(b"\0")
        hasher.update(file.read_bytes())
        hasher.update(b"\0")
    return hasher.hexdigest()


def hash_path(path: Path) -> str:
    if path.is_dir():
        return hash_dir(path)
    if path.is_file():
        return hash_file(path)
    return "missing"


def _resolve_fingerprint_path(base_dir: Path, alt_dir: Path, rendered: str) -> Path:
    candidate = Path(rendered)
    if candidate.is_absolute():
        return candidate
    first = (base_dir / candidate).resolve()
    if first.exists():
        return first
    return (alt_dir / candidate).resolve()


def compute_fingerprint(
    paths: Iterable[str], base_dir: Path, alt_dir: Path, ctx: Mapping[str, Any]
) -> Dict[str, str]:
    fingerprint: Dict[str, str] = {}
    for raw in paths:
        rendered = format_string(raw, ctx)
        resolved = _resolve_fingerprint_path(base_dir, alt_dir, rendered)
        fingerprint[str(rendered)] = hash_path(resolved)
    return fingerprint


def load_manifest(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_manifest(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def git_info() -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    try:
        rev = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=str(REPO_ROOT)).decode().strip()
        status = subprocess.check_output(["git", "status", "--porcelain"], cwd=str(REPO_ROOT)).decode().strip()
        info["commit"] = rev
        info["dirty"] = bool(status)
    except Exception:
        return info
    return info


def manifest_matches(manifest: Dict[str, Any], *, command: str, params: Dict[str, Any], fingerprint: Dict[str, str]) -> bool:
    if not manifest:
        return False
    if manifest.get("command") != command:
        return False
    if manifest.get("params") != params:
        return False
    if manifest.get("fingerprint") != fingerprint:
        return False
    return True


def run_shell(command: str, *, dry_run: bool) -> None:
    if dry_run:
        print(f"[dry-run] {command}")
        return
    subprocess.run(command, shell=True, check=True)


def collect_enabled_experiments(plan_experiments: Mapping[str, Any] | None, base_experiments: Mapping[str, Any]) -> Tuple[List[str], Dict[str, Any]]:
    if not plan_experiments:
        return list(base_experiments.keys()), {}

    enabled: List[str] = []
    overrides: Dict[str, Any] = {}
    for name, cfg in plan_experiments.items():
        cfg_dict: Dict[str, Any] = {}
        enabled_flag = True
        if isinstance(cfg, dict):
            cfg_dict = dict(cfg)
            enabled_flag = bool(cfg_dict.pop("enabled", True))
        elif cfg is None:
            enabled_flag = True
        else:
            enabled_flag = bool(cfg)

        overrides[name] = cfg_dict
        if enabled_flag:
            enabled.append(name)
    return enabled, overrides


def build_context(
    *,
    plan_name: str,
    run_ts: str,
    threads: int,
    sf: int,
    str_level: int,
    paths_cfg: Mapping[str, Any],
) -> Dict[str, Any]:
    root_raw = paths_cfg.get("root", "experiments/plan_runs")
    root = resolve_path(REPO_ROOT, format_string(str(root_raw), {"PLAN_NAME": plan_name, "RUN_TS": run_ts}))
    root = root or REPO_ROOT / "experiments" / "plan_runs"

    ctx: Dict[str, Any] = {
        "PLAN_NAME": plan_name,
        "RUN_TS": run_ts,
        "SF": sf,
        "STR": str_level,
        "THREADS": threads,
        "ROOT": str(root),
    }

    run_id_template = paths_cfg.get("run_id", "sf{SF}_str{STR}_thr{THREADS}")
    run_id = format_string(str(run_id_template), ctx)
    ctx["RUN_ID"] = run_id

    ctx["DATA_DIR"] = format_string(paths_cfg.get("data_dir", "{ROOT}/data/sf{SF}/str{STR}"), ctx)
    ctx["QUERY_ROOT"] = format_string(paths_cfg.get("query_root", "{ROOT}/queries"), ctx)
    ctx["TPCDS_DIR"] = format_string(
        paths_cfg.get("tpcds_dir", "{ROOT}/queries/tpcds/sf{SF}"), ctx
    )
    ctx["PRODDS_DIR"] = format_string(
        paths_cfg.get("prodds_dir", "{ROOT}/queries/prodds/sf{SF}/str{STR}"), ctx
    )
    ctx["JOIN_DIR"] = format_string(
        paths_cfg.get("join_dir", "{ROOT}/queries/join_scaling/sf{SF}"), ctx
    )
    ctx["STRING_DIR"] = format_string(
        paths_cfg.get("string_dir", "{ROOT}/queries/string_sweep"), ctx
    )
    ctx["CONFIG_DIR"] = format_string(paths_cfg.get("config_dir", "{ROOT}/configs"), ctx)
    ctx["RESULTS_DIR"] = format_string(paths_cfg.get("results_dir", "{ROOT}/results/{RUN_ID}"), ctx)
    ctx["ENGINE_ROOT"] = format_string(paths_cfg.get("engine_root", "{ROOT}/engines"), ctx)
    return ctx


def maybe_generate(
    name: str,
    cfg: Mapping[str, Any],
    *,
    ctx: Mapping[str, Any],
    plan_dir: Path,
    dry_run: bool,
    no_generate: bool,
    force: bool,
    enabled_experiments: Iterable[str],
) -> None:
    if not cfg:
        return
    if not bool(cfg.get("enabled", True)):
        return

    requires = cfg.get("requires_experiments")
    if requires:
        enabled_set = set(enabled_experiments)
        if not any(r in enabled_set for r in requires):
            return

    command_raw = cfg.get("command")
    if not command_raw:
        return

    output_dir_raw = cfg.get("output_dir")
    output_dir = None
    if output_dir_raw:
        output_dir = Path(format_string(str(output_dir_raw), ctx))
        ensure_dir(output_dir)

    manifest_name = cfg.get("manifest", ".gen_manifest.json")
    manifest_path = None
    if output_dir:
        manifest_path = output_dir / manifest_name

    reuse = bool(cfg.get("reuse", True))
    on_stale = str(cfg.get("on_stale", "regenerate")).lower()

    command = format_string(str(command_raw), ctx)
    fingerprint_paths = cfg.get("fingerprint") or []
    fingerprint = compute_fingerprint(fingerprint_paths, plan_dir, REPO_ROOT, ctx)
    params = {
        "SF": ctx.get("SF"),
        "STR": ctx.get("STR"),
        "THREADS": ctx.get("THREADS"),
    }

    if manifest_path and reuse and not force:
        manifest = load_manifest(manifest_path)
        if manifest_matches(manifest, command=command, params=params, fingerprint=fingerprint):
            print(f"[skip] {name}: up-to-date")
            return
        if manifest and on_stale == "skip":
            print(f"[skip] {name}: stale but on_stale=skip")
            return
        if manifest and on_stale == "fail":
            raise PlanError(f"{name}: stale manifest and on_stale=fail")

    if no_generate:
        print(f"[skip] {name}: generation disabled via --no-generate")
        return

    print(f"[gen] {name}: {command}")
    run_shell(command, dry_run=dry_run)

    if manifest_path:
        payload = {
            "name": name,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "command": command,
            "params": params,
            "fingerprint": fingerprint,
            "git": git_info(),
        }
        if not dry_run:
            write_manifest(manifest_path, payload)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run experiments from a plan YAML.")
    parser.add_argument("--plan", required=True, help="Path to plan YAML")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing commands")
    parser.add_argument("--no-generate", action="store_true", help="Skip data/query generation steps")
    parser.add_argument("--no-run", action="store_true", help="Skip running experiments")
    parser.add_argument("--force", action="store_true", help="Force regeneration even if manifests match")
    parser.add_argument(
        "--only-experiment",
        action="append",
        choices=["workload_compare", "join_scaling", "string_sweep"],
        help="Run only specific experiment(s). Can be repeated.",
    )
    args = parser.parse_args()

    plan_path = Path(args.plan).expanduser().resolve()
    plan_dir = plan_path.parent
    plan = read_yaml(plan_path)

    plan_name = str(plan.get("plan_name") or plan_path.stem)
    base_config_path = resolve_path(plan_dir, plan.get("base_config"))
    if not base_config_path or not base_config_path.exists():
        raise PlanError("base_config is required and must exist")

    base_config = read_yaml(base_config_path)
    matrix = plan.get("matrix") or {}

    default_threads = base_config.get("global", {}).get("threads", 16)
    threads_list = listify(matrix.get("threads"), [default_threads])
    sf_list = listify(matrix.get("scale_factors"), [1])
    str_list = listify(matrix.get("stringification_levels"), [1])

    plan_experiments = plan.get("experiments")
    enabled_experiments, experiment_overrides = collect_enabled_experiments(
        plan_experiments, base_config.get("experiments", {})
    )

    if args.only_experiment:
        enabled_experiments = [e for e in enabled_experiments if e in args.only_experiment]

    engines_list = plan.get("engines")
    system_mode = str(plan.get("system_mode", "all")).lower()
    if system_mode not in {"all", "per_engine"}:
        raise PlanError("system_mode must be 'all' or 'per_engine'")

    generation_cfg = plan.get("generation") or {}
    paths_cfg = plan.get("paths") or {}
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for threads, sf, str_level in product(threads_list, sf_list, str_list):
        ctx = build_context(
            plan_name=plan_name,
            run_ts=run_ts,
            threads=int(threads),
            sf=int(sf),
            str_level=int(str_level),
            paths_cfg=paths_cfg,
        )

        config_dir = Path(ctx["CONFIG_DIR"])
        ensure_dir(config_dir)

        print(f"\n[run] plan={plan_name} run_id={ctx['RUN_ID']}")

        for gen_name, gen_cfg in generation_cfg.items():
            maybe_generate(
                gen_name,
                gen_cfg or {},
                ctx=ctx,
                plan_dir=plan_dir,
                dry_run=args.dry_run,
                no_generate=args.no_generate,
                force=args.force,
                enabled_experiments=enabled_experiments,
            )

        config = copy.deepcopy(base_config)
        if "global" in config:
            config["global"]["threads"] = int(threads)

        if experiment_overrides:
            config.setdefault("experiments", {})
            deep_merge(config["experiments"], experiment_overrides)

        if engines_list:
            config.setdefault("engines", {})
            for name in list(config["engines"].keys()):
                cfg = config["engines"].setdefault(name, {})
                cfg["enabled"] = name in engines_list

        config = render_templates(config, ctx)

        config_path = config_dir / f"{plan_name}_{ctx['RUN_ID']}.yaml"
        if not args.dry_run:
            write_yaml(config_path, config)
        print(f"[config] {config_path}")

        if args.no_run or not enabled_experiments:
            continue

        systems: List[str] = []
        if system_mode == "all":
            systems = ["all"]
        else:
            if engines_list:
                systems = list(engines_list)
            else:
                systems = [name for name, cfg in (config.get("engines") or {}).items() if cfg.get("enabled", True)]

        for experiment in enabled_experiments:
            for system in systems:
                cmd = (
                    f"python -m experiments run --config {config_path} "
                    f"--experiment {experiment} --system {system}"
                )
                print(f"[run] {cmd}")
                run_shell(cmd, dry_run=args.dry_run)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PlanError as exc:
        print(f"Plan error: {exc}", file=sys.stderr)
        raise SystemExit(2)

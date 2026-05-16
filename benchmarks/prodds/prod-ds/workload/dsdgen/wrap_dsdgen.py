#!/usr/bin/env python3
"""
Lightweight wrapper for tpcds-kit/tools/dsdgen with optional stringification, NULL skew, and MCV skew injection.
"""

import argparse
import multiprocessing
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

from workload import stringification as stringification_cfg


REPO_ROOT = Path(__file__).resolve().parents[2]
TPCDS_KIT_DIR = REPO_ROOT / "tpcds-kit"
TOOLS_DIR = TPCDS_KIT_DIR / "tools"
BIN_CANDIDATES = ("dsdgen", "dsdgen.bin", "dsdgen.exe")


def _parse_positive_int(raw: str) -> int:
    try:
        val = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid int value: {raw}") from exc
    if val < 1:
        raise argparse.ArgumentTypeError("value must be >= 1")
    return val


def _parse_wrapper_args(argv: Sequence[str]) -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--stringification-level",
        "--stringify-level",
        dest="stringification_level",
        type=_parse_positive_int,
        help="Stringification level (1-15; default: 10). STR=1 is vanilla TPC-DS, STR=10 recasts all 131 columns, STR=11-15 extends string length.",
    )
    parser.add_argument(
        "--stringification-preset",
        dest="stringification_preset",
        type=str,
        choices=["vanilla", "low", "medium", "high", "production"],
        help="Stringification preset (vanilla, low, medium, high, production).",
    )
    parser.add_argument(
        "--stringify",
        nargs="?",
        const=10,
        type=_parse_positive_int,
        help="Deprecated alias for --stringification-level (defaults to production when no value is given).",
    )
    parser.add_argument(
        "--str-plus-max-level",
        type=_parse_positive_int,
        default=stringification_cfg.DEFAULT_STR_PLUS_MAX_LEVEL,
        help="Maximum accepted level in STR+ mode (default: 20).",
    )
    parser.add_argument(
        "--str-plus-pad-step",
        type=_parse_positive_int,
        default=2,
        help="Extra suffix growth per level above STR10 (default: 2).",
    )
    parser.add_argument(
        "--str-plus-separator",
        type=str,
        default="_",
        help="Suffix separator for STR+ amplification (default: _).",
    )
    parser.add_argument(
        "--str-plus-marker",
        type=str,
        default="X",
        help="Suffix marker repeated in STR+ amplification (default: X).",
    )
    parser.add_argument(
        "--disable-null-skew",
        action="store_true",
        help="Skip NULL skew post-processing (enabled by default).",
    )
    parser.add_argument("--null-marker", type=str, help="Override NULL marker (e.g., '\\N').")
    parser.add_argument("--null-seed", type=int, help="Deterministic seed for NULL skew.")
    parser.add_argument(
        "--null-profile",
        type=str,
        help="Null profile name or tier alias (low/medium/high). Default: medium (fleet_realworld_final).",
    )
    parser.add_argument(
        "--exclude-hot-paths",
        action="store_true",
        help="Use conservative column pool (legacy behavior). Default includes hot-path columns.",
    )
    parser.add_argument(
        "--min-ndv-for-injection",
        type=int,
        default=None,
        help="Minimum NDV required for NULL/MCV injection eligibility (default: 50).",
    )
    parser.add_argument(
        "--ndv-reference-duckdb",
        type=str,
        default=None,
        help="Optional DuckDB path used for NDV guard COUNT(DISTINCT) checks.",
    )
    parser.add_argument(
        "--ndv-cache-dir",
        type=str,
        default=None,
        help="Optional cache directory for NDV guard results.",
    )
    parser.add_argument(
        "--scale-factor",
        type=int,
        default=None,
        help="Optional SF value used for NDV cache keying and DB-path inference.",
    )
    parser.add_argument(
        "--disable-mcv-skew",
        action="store_true",
        help="Skip MCV skew post-processing (enabled by default).",
    )
    parser.add_argument("--mcv-seed", type=int, help="Deterministic seed for MCV skew.")
    parser.add_argument(
        "--mcv-profile",
        type=str,
        help="MCV profile name or tier alias (low/medium/high). Default: medium (mcv_fleet_default).",
    )
    parser.add_argument(
        "--rewrite-max-workers", type=int, help="Cap worker processes used during rewrite stage."
    )
    parser.add_argument(
        "--parallel",
        type=_parse_positive_int,
        default=None,
        help="Number of parallel dsdgen child processes (default: cpu_count - 1). Uses dsdgen's native -PARALLEL/-CHILD mechanism.",
    )
    parser.add_argument(
        "--default",
        action="store_true",
        help="Use recommended defaults: STR=10, NULL medium, MCV medium, SF=10, output ./output.",
    )

    parsed, passthrough = parser.parse_known_args(argv)
    return parsed, passthrough


def _resolve_dsdgen_binary() -> Path:
    for name in BIN_CANDIDATES:
        candidate = TOOLS_DIR / name
        if candidate.exists():
            return candidate
    raise SystemExit(
        "Could not find tpcds-kit/tools/dsdgen*. Run ./install.sh first to fetch and build the TPC-DS toolkit."
    )


def _normalize_dir_args(
    args: List[str], require_dir: bool, default_dir: Optional[Path] = None
) -> Tuple[Optional[Path], List[str]]:
    dir_path: Optional[Path] = None
    normalized: List[str] = []
    i = 0
    while i < len(args):
        token = args[i]
        lower = token.lower()
        if lower in ("-dir", "--dir", "-directory", "--directory"):
            if i + 1 >= len(args):
                raise SystemExit("Missing value for -DIR/-DIRECTORY.")
            raw_value = args[i + 1]
            abs_path = Path(raw_value).expanduser().resolve()
            normalized.extend([token, str(abs_path)])
            if dir_path is None:
                dir_path = abs_path
            i += 2
            continue
        if "=" in token:
            prefix, value = token.split("=", 1)
            lower_prefix = prefix.lower()
            if lower_prefix in ("-dir", "--dir", "-directory", "--directory"):
                abs_path = Path(value).expanduser().resolve()
                normalized.append(f"{prefix}={abs_path}")
                if dir_path is None:
                    dir_path = abs_path
                i += 1
                continue
        normalized.append(token)
        i += 1

    if require_dir and dir_path is None:
        if default_dir is not None:
            dir_path = default_dir
        else:
            raise SystemExit(
                "Post-processing requires specifying the dsdgen output directory via -DIR."
            )
    return dir_path, normalized


def _run_dsdgen(binary: Path, args: Sequence[str]) -> int:
    proc = subprocess.run([str(binary), *args], check=False, cwd=str(TOOLS_DIR))
    return proc.returncode


def _run_dsdgen_parallel(binary: Path, args: Sequence[str], num_children: int) -> int:
    """Run dsdgen with -PARALLEL N -CHILD i for each child, wait for all."""
    # Remove any existing -PARALLEL/-CHILD from args to avoid conflicts
    filtered: List[str] = []
    skip_next = False
    for i, token in enumerate(args):
        if skip_next:
            skip_next = False
            continue
        upper = token.upper()
        if upper in ("-PARALLEL", "--PARALLEL", "-CHILD", "--CHILD"):
            skip_next = True
            continue
        if upper.startswith(("-PARALLEL=", "--PARALLEL=", "-CHILD=", "--CHILD=")):
            continue
        filtered.append(token)

    print(f"[parallel] Spawning {num_children} dsdgen child processes...")
    procs: List[subprocess.Popen] = []
    for child_id in range(1, num_children + 1):
        child_args = [str(binary)] + filtered + ["-PARALLEL", str(num_children), "-CHILD", str(child_id)]
        proc = subprocess.Popen(child_args, cwd=str(TOOLS_DIR))
        procs.append(proc)

    # Wait for all children and collect return codes
    failed = 0
    for i, proc in enumerate(procs, 1):
        rc = proc.wait()
        if rc != 0:
            print(f"[parallel] Child {i}/{num_children} exited with code {rc}", file=sys.stderr)
            failed += 1

    if failed:
        print(f"[parallel] {failed}/{num_children} child processes failed.", file=sys.stderr)
        return 1
    print(f"[parallel] All {num_children} child processes completed successfully.")
    return 0


def _run_rewrite(
    output_dir: Path,
    *,
    stringification_level: int | None,
    stringification_preset: str | None,
    str_plus_enabled: bool,
    str_plus_max_level: int,
    str_plus_pad_step: int,
    str_plus_separator: str,
    str_plus_marker: str,
    null_enabled: bool,
    mcv_enabled: bool,
    null_marker: Optional[str] = None,
    null_seed: Optional[int] = None,
    null_profile: Optional[str] = None,
    mcv_seed: Optional[int] = None,
    mcv_profile: Optional[str] = None,
    include_hot_paths: Optional[bool] = None,
    min_ndv_for_injection: Optional[int] = None,
    ndv_reference_duckdb: Optional[str] = None,
    ndv_cache_dir: Optional[str] = None,
    scale_factor: Optional[int] = None,
    max_workers: Optional[int] = None,
) -> None:
    print(f"[rewrite] Rewriting in directory: {output_dir}")
    try:
        from workload.dsdgen import stringify  # type: ignore
    except ImportError as exc:  # pragma: no cover - executed once module exists
        raise SystemExit(
            "Post-processing requires workload/dsdgen/stringify.py. Implement/install it before retrying."
        ) from exc

    resolved_dir = output_dir.resolve()
    if not resolved_dir.exists():
        raise SystemExit(f"dsdgen output directory not found: {resolved_dir}")

    files, rows = stringify.rewrite_tbl_directory(
        resolved_dir,
        max_workers=max_workers,
        backend=(os.getenv("STRINGIFY_BACKEND") or "auto"),
        enable_stringify=None,
        stringification_level=stringification_level,
        stringification_preset=stringification_preset,
        str_plus_enabled=str_plus_enabled,
        str_plus_max_level=str_plus_max_level,
        str_plus_pad_step=str_plus_pad_step,
        str_plus_separator=str_plus_separator,
        str_plus_marker=str_plus_marker,
        enable_nulls=null_enabled,
        null_seed=null_seed,
        null_marker=null_marker,
        null_profile=null_profile,
        include_hot_paths=include_hot_paths,
        min_ndv_for_injection=min_ndv_for_injection,
        ndv_reference_duckdb=ndv_reference_duckdb,
        ndv_cache_dir=ndv_cache_dir,
        scale_factor=scale_factor,
        enable_mcv=mcv_enabled,
        mcv_seed=mcv_seed,
        mcv_profile=mcv_profile,
    )
    print(f"[rewrite] Rewrote {files} data files ({rows} rows).")


def main(argv: Sequence[str]) -> int:
    parsed, passthrough = _parse_wrapper_args(argv)

    # --default: STR=10, NULL medium, MCV medium, SF=10, output ./output
    if parsed.default:
        if parsed.stringification_level is None and parsed.stringification_preset is None and parsed.stringify is None:
            parsed.stringification_level = 10
        if parsed.null_profile is None:
            parsed.null_profile = "medium"
        if parsed.mcv_profile is None:
            parsed.mcv_profile = "medium"
        # Skip NDV guard for quick-start (no pre-built DuckDB reference needed)
        if parsed.min_ndv_for_injection is None:
            parsed.min_ndv_for_injection = 0
        # Inject -SCALE 10 and -DIR ./output if not already specified
        has_scale = any(t.upper() in ("-SCALE", "--SCALE") or t.upper().startswith("-SCALE=") for t in passthrough)
        if not has_scale:
            passthrough = passthrough + ["-SCALE", "10"]
        has_dir = any(t.lower() in ("-dir", "--dir", "-directory", "--directory") or t.lower().startswith(("-dir=", "--dir=")) for t in passthrough)
        if not has_dir:
            default_out = Path("./output").resolve()
            default_out.mkdir(parents=True, exist_ok=True)
            passthrough = passthrough + ["-DIR", str(default_out)]
        # Add FORCE so re-runs don't fail
        has_force = any(t.upper() in ("-FORCE", "--FORCE") for t in passthrough)
        if not has_force:
            passthrough = passthrough + ["-FORCE"]
        print("[default] Using recommended defaults: STR=10, NULL=medium, MCV=medium, SF=10, DIR=./output")

    stringify_level = parsed.stringification_level
    stringify_preset = parsed.stringification_preset
    if stringify_level is not None and stringify_preset is not None:
        raise SystemExit("--stringification-level and --stringification-preset are mutually exclusive.")
    if parsed.stringify is not None:
        if stringify_level is not None or stringify_preset is not None:
            raise SystemExit("--stringify cannot be combined with --stringification-level/--stringification-preset.")
        stringify_level = parsed.stringify
    resolved_level, resolved_preset = stringification_cfg.resolve_level(
        stringify_level,
        stringify_preset,
        allow_extended=True,
        max_level=int(parsed.str_plus_max_level),
    )
    stringify_requested = resolved_level > 1
    null_skew_enabled = not bool(parsed.disable_null_skew)
    mcv_skew_enabled = not bool(parsed.disable_mcv_skew)

    dir_path, normalized_args = _normalize_dir_args(
        list(passthrough),
        stringify_requested or null_skew_enabled or mcv_skew_enabled,
        default_dir=TOOLS_DIR,
    )
    passthrough = normalized_args

    binary = _resolve_dsdgen_binary()
    num_parallel = parsed.parallel
    if num_parallel is None:
        # Default: single process (backward compatible)
        rc = _run_dsdgen(binary, passthrough)
    elif num_parallel == 1:
        rc = _run_dsdgen(binary, passthrough)
    else:
        rc = _run_dsdgen_parallel(binary, passthrough, num_parallel)
    if rc != 0:
        return rc

    if stringify_requested or null_skew_enabled or mcv_skew_enabled:
        print(
            f"[stringify] Stringification level {resolved_level} (preset={resolved_preset or 'custom'})."
        )

    if (stringify_requested or null_skew_enabled or mcv_skew_enabled) and dir_path is not None:
        print(f"[rewrite] Starting post-processing rewrite with stringification={stringify_requested}, null_skew={null_skew_enabled}, mcv_skew={mcv_skew_enabled}...")
        _run_rewrite(
            dir_path,
            stringification_level=resolved_level,
            stringification_preset=resolved_preset,
            str_plus_enabled=resolved_level > stringification_cfg.BASE_MAX_LEVEL,
            str_plus_max_level=int(parsed.str_plus_max_level),
            str_plus_pad_step=int(parsed.str_plus_pad_step),
            str_plus_separator=str(parsed.str_plus_separator),
            str_plus_marker=str(parsed.str_plus_marker),
            null_enabled=null_skew_enabled,
            mcv_enabled=mcv_skew_enabled,
            null_marker=parsed.null_marker,
            null_seed=parsed.null_seed,
            null_profile=parsed.null_profile,
            include_hot_paths=not bool(parsed.exclude_hot_paths),
            min_ndv_for_injection=parsed.min_ndv_for_injection,
            ndv_reference_duckdb=parsed.ndv_reference_duckdb,
            ndv_cache_dir=parsed.ndv_cache_dir,
            scale_factor=parsed.scale_factor,
            mcv_seed=parsed.mcv_seed,
            mcv_profile=parsed.mcv_profile,
            max_workers=parsed.rewrite_max_workers,
        )

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

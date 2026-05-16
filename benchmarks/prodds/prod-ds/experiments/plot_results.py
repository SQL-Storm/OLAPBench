#!/usr/bin/env python3
"""
plot_results.py -- Generate paper figures from Prod-DS Kit experiment results.

Reads JSONL / CSV output from reproduce.sh and produces PNG plots
matching the figures in Section 6 of the paper.

Usage:
    python3 experiments/plot_results.py \
        --results-dir .reproduce/results \
        --output-dir  .reproduce/results/plots
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ── Colour palette (engine branding) ────────────────────────────
ENGINE_COLORS: Dict[str, str] = {
    "duckdb":  "#FF6B00",
    "cedardb": "#2196F3",
    "monetdb": "#4CAF50",
}
ENGINE_ORDER = ["duckdb", "cedardb", "monetdb"]
ENGINE_LABELS: Dict[str, str] = {
    "duckdb":  "DuckDB",
    "cedardb": "CedarDB",
    "monetdb": "MonetDB",
}

SUITE_COLORS = {
    "tpcds":  "#9E9E9E",
    "prodds": "#E65100",
}
SUITE_LABELS = {
    "tpcds":  "TPC-DS",
    "prodds": "Prod-DS",
}

# Error categories shown in Fig 10
ERROR_CATEGORIES = ["success", "error", "timeout_planning", "timeout_execution",
                    "oom", "engine_crash"]
ERROR_COLORS = {
    "success":           "#4CAF50",
    "error":             "#F44336",
    "timeout_planning":  "#FF9800",
    "timeout_execution": "#FFC107",
    "oom":               "#9C27B0",
    "engine_crash":      "#795548",
}
ERROR_LABELS = {
    "success":           "Success",
    "error":             "Error",
    "timeout_planning":  "Plan Timeout",
    "timeout_execution": "Exec Timeout",
    "oom":               "OOM",
    "engine_crash":      "Crash",
}


# ── IO helpers ──────────────────────────────────────────────────
def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_csv(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def safe_float(val: Any) -> Optional[float]:
    if val is None or val == "" or val == "None":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _summarize_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Inline summary: group by query_id+suite, compute median/min/max."""
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        key = f"{rec.get('query_id', '')}|{rec.get('suite', '')}"
        grouped[key].append(rec)

    summaries: List[Dict[str, Any]] = []
    for _, items in grouped.items():
        sample = items[0]
        total = len(items)
        successes = [i for i in items if i.get("status") == "success"]
        success_times = [
            i["wall_time_ms_total"] for i in successes
            if i.get("wall_time_ms_total") is not None
        ]
        planning_times = [
            i["wall_time_ms_planning"] for i in successes
            if i.get("wall_time_ms_planning") is not None
        ]
        execution_times = [
            i["wall_time_ms_execution"] for i in successes
            if i.get("wall_time_ms_execution") is not None
        ]

        summaries.append({
            "query_id": sample.get("query_id"),
            "suite": sample.get("suite"),
            "runs_total": total,
            "runs_success": len(successes),
            "runs_failed": total - len(successes),
            "failure_rate": (total - len(successes)) / total if total else 0,
            "median_ms": float(np.median(success_times)) if success_times else None,
            "min_ms": min(success_times) if success_times else None,
            "max_ms": max(success_times) if success_times else None,
            "median_planning_ms": float(np.median(planning_times)) if planning_times else None,
            "median_execution_ms": float(np.median(execution_times)) if execution_times else None,
        })
    return summaries


def engine_color(name: str) -> str:
    return ENGINE_COLORS.get(name.lower(), "#888888")


def engine_label(name: str) -> str:
    return ENGINE_LABELS.get(name.lower(), name)


# ── Style setup ─────────────────────────────────────────────────
def apply_style() -> None:
    plt.rcParams.update({
        "figure.facecolor":   "white",
        "axes.facecolor":     "white",
        "axes.edgecolor":     "#333333",
        "axes.labelcolor":    "#333333",
        "axes.labelsize":     11,
        "axes.titlesize":     13,
        "xtick.color":        "#333333",
        "ytick.color":        "#333333",
        "xtick.labelsize":    9,
        "ytick.labelsize":    9,
        "legend.fontsize":    9,
        "legend.framealpha":  0.9,
        "legend.edgecolor":   "#cccccc",
        "grid.alpha":         0.3,
        "grid.color":         "#cccccc",
        "grid.linestyle":     "--",
        "font.family":        "sans-serif",
        "figure.dpi":         150,
        "savefig.dpi":        200,
        "savefig.bbox":       "tight",
        "savefig.pad_inches": 0.15,
    })


def save_fig(fig: plt.Figure, output_dir: Path, name: str) -> None:
    path = output_dir / name
    fig.savefig(str(path))
    plt.close(fig)
    print(f"  saved {path}")


# ── Data aggregation helpers ────────────────────────────────────
def _detect_engine(dirname: str) -> str:
    """Extract engine name from a directory name like 'duckdb_tpcds'."""
    low = dirname.lower()
    for eng in ENGINE_ORDER:
        if eng in low:
            return eng
    return low


def _detect_suite(dirname: str) -> str:
    """Extract suite name from a directory name like 'duckdb_prodds'."""
    low = dirname.lower()
    if "prodds" in low:
        return "prodds"
    if "tpcds" in low:
        return "tpcds"
    return low


def _find_latest_file(base_dir: Path, filename: str) -> Optional[Path]:
    """Find *filename* inside *base_dir*, searching recursively.

    The results hierarchy is <base_dir>/<timestamp>/<engine>/workload_compare/<file>.
    When multiple timestamp dirs exist, use the latest (sorted descending).
    Also handles flat layout where *filename* sits directly in *base_dir*.
    """
    direct = base_dir / filename
    if direct.is_file():
        return direct
    # Search recursively — pick the latest timestamp directory's file
    candidates = sorted(base_dir.rglob(filename), reverse=True)
    return candidates[0] if candidates else None


def _collect_e1_summaries(e1_dir: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Return {engine: {suite: [summary_rows]}} from E1 subdirectories."""
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not e1_dir.is_dir():
        return result

    for sub in sorted(e1_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        suite = _detect_suite(sub.name)

        summary_path = _find_latest_file(sub, "summary.csv")
        raw_path = _find_latest_file(sub, "raw.jsonl")

        rows: List[Dict[str, Any]] = []
        if summary_path:
            rows = load_csv(summary_path)
        elif raw_path:
            records = load_jsonl(raw_path)
            rows = _summarize_records(records)

        if rows:
            result.setdefault(engine, {}).setdefault(suite, []).extend(rows)

    return result


def _collect_e1_raw(e1_dir: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Return {engine: {suite: [raw_records]}} from E1 subdirectories."""
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not e1_dir.is_dir():
        return result

    for sub in sorted(e1_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        suite = _detect_suite(sub.name)

        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            records = load_jsonl(raw_path)
            result.setdefault(engine, {}).setdefault(suite, []).extend(records)
    return result


# ── E1 Plots (Figs 8, 9, 10) ───────────────────────────────────

def plot_fig8_bar(summaries: Dict[str, Dict[str, List[Dict[str, Any]]]],
                  output_dir: Path, agg: str, fname: str, title: str) -> None:
    """Bar chart comparing runtime aggregates per engine, TPC-DS vs Prod-DS."""
    engines = [e for e in ENGINE_ORDER if e in summaries]
    if not engines:
        return

    suites = ["tpcds", "prodds"]
    bar_width = 0.35
    x = np.arange(len(engines))

    fig, ax = plt.subplots(figsize=(5, 4))

    for i, suite in enumerate(suites):
        vals = []
        for eng in engines:
            rows = summaries.get(eng, {}).get(suite, [])
            times = [safe_float(r.get("median_ms")) for r in rows]
            times = [t for t in times if t is not None and t > 0]
            if not times:
                vals.append(0)
                continue
            if agg == "median":
                vals.append(float(np.median(times)))
            elif agg == "mean":
                vals.append(float(np.mean(times)))
            elif agg == "sum":
                vals.append(float(np.sum(times)))
            else:
                vals.append(0)

        offset = (i - 0.5) * bar_width
        bars = ax.bar(x + offset, vals, bar_width, label=SUITE_LABELS.get(suite, suite),
                      color=SUITE_COLORS.get(suite, "#888"), edgecolor="white", linewidth=0.5)

    ax.set_yscale("log")
    ax.set_ylabel("Runtime (ms)")
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels([engine_label(e) for e in engines])
    ax.legend()
    ax.yaxis.set_minor_locator(ticker.LogLocator(subs="auto", numticks=20))
    ax.yaxis.set_minor_formatter(ticker.NullFormatter())
    fig.tight_layout()
    save_fig(fig, output_dir, fname)


def plot_fig9_cdf(summaries: Dict[str, Dict[str, List[Dict[str, Any]]]],
                  output_dir: Path) -> None:
    """CDF of per-query median runtime on Prod-DS, one line per engine."""
    fig, ax = plt.subplots(figsize=(6, 4))
    plotted = False

    for eng in ENGINE_ORDER:
        rows = summaries.get(eng, {}).get("prodds", [])
        times = sorted(t for r in rows if (t := safe_float(r.get("median_ms"))) is not None and t > 0)
        if not times:
            continue
        cdf_y = np.arange(1, len(times) + 1) / len(times)
        ax.step(times, cdf_y, where="post", label=engine_label(eng),
                color=engine_color(eng), linewidth=1.8)
        plotted = True

    if not plotted:
        plt.close(fig)
        return

    ax.set_xscale("log")
    ax.set_xlabel("Median query runtime (ms)")
    ax.set_ylabel("CDF")
    ax.set_title("Per-query runtime CDF on Prod-DS")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right")
    fig.tight_layout()
    save_fig(fig, output_dir, "fig9_runtime_cdf.png")


def plot_fig10_errors(raw: Dict[str, Dict[str, List[Dict[str, Any]]]],
                      output_dir: Path) -> None:
    """Stacked bar: error breakdown per engine on Prod-DS."""
    engines = [e for e in ENGINE_ORDER if e in raw and "prodds" in raw[e]]
    if not engines:
        return

    # Count unique (query_id, status) keeping last repetition
    engine_counts: Dict[str, Dict[str, int]] = {}
    for eng in engines:
        records = raw[eng].get("prodds", [])
        # Take status per query_id from the last record for that query
        per_query: Dict[str, str] = {}
        for rec in records:
            qid = rec.get("query_id", "")
            per_query[qid] = rec.get("status", "error")

        counts: Dict[str, int] = defaultdict(int)
        for status in per_query.values():
            cat = status if status in ERROR_COLORS else "error"
            counts[cat] += 1
        engine_counts[eng] = dict(counts)

    # Determine which categories actually appear
    cats_present = [c for c in ERROR_CATEGORIES
                    if any(engine_counts[e].get(c, 0) > 0 for e in engines)]
    if not cats_present:
        return

    fig, ax = plt.subplots(figsize=(5, 4))
    x = np.arange(len(engines))
    bar_width = 0.55
    bottoms = np.zeros(len(engines))

    for cat in cats_present:
        vals = np.array([engine_counts[e].get(cat, 0) for e in engines], dtype=float)
        ax.bar(x, vals, bar_width, bottom=bottoms,
               label=ERROR_LABELS.get(cat, cat),
               color=ERROR_COLORS.get(cat, "#888"),
               edgecolor="white", linewidth=0.5)
        bottoms += vals

    ax.set_ylabel("Number of queries")
    ax.set_title("Query outcome breakdown (Prod-DS)")
    ax.set_xticks(x)
    ax.set_xticklabels([engine_label(e) for e in engines])
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()
    save_fig(fig, output_dir, "fig10_error_breakdown.png")


# ── E2 Plots (Fig 11) ──────────────────────────────────────────

def _collect_e2(e2_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    """Return {engine: [records]} from E2 subdirectories."""
    result: Dict[str, List[Dict[str, Any]]] = {}
    if not e2_dir.is_dir():
        return result
    for sub in sorted(e2_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            result.setdefault(engine, []).extend(load_jsonl(raw_path))
    return result


def _extract_join_level(rec: Dict[str, Any]) -> Optional[int]:
    """Get join count from either the field or the query_id."""
    jc = rec.get("join_count")
    if jc is not None:
        try:
            return int(jc)
        except (ValueError, TypeError):
            pass
    qid = rec.get("query_id", "")
    m = re.search(r"J(\d+)", qid)
    if m:
        return int(m.group(1))
    return None


def _aggregate_by_level(records: List[Dict[str, Any]],
                        level_fn, time_field: str
                        ) -> Tuple[List[int], List[float]]:
    """Group records by level, compute median of the given time field."""
    groups: Dict[int, List[float]] = defaultdict(list)
    for rec in records:
        if rec.get("status") != "success":
            continue
        lvl = level_fn(rec)
        t = safe_float(rec.get(time_field))
        if lvl is not None and t is not None and t > 0:
            groups[lvl].append(t)

    levels = sorted(groups.keys())
    medians = [float(np.median(groups[lvl])) for lvl in levels]
    return levels, medians


def plot_fig11(e2_data: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> None:
    """Log-log: median execution and planning time vs join level."""
    engines = [e for e in ENGINE_ORDER if e in e2_data]
    if not engines:
        return

    for time_field, suffix, ylabel, title in [
        ("wall_time_ms_execution", "a_join_execution",
         "Median execution time (ms)", "Join scaling: execution time"),
        ("wall_time_ms_planning", "b_join_planning",
         "Median planning time (ms)", "Join scaling: planning time"),
    ]:
        fig, ax = plt.subplots(figsize=(6, 4))
        plotted = False
        for eng in engines:
            levels, meds = _aggregate_by_level(
                e2_data[eng], _extract_join_level, time_field)
            if not levels:
                continue
            ax.plot(levels, meds, "o-", label=engine_label(eng),
                    color=engine_color(eng), linewidth=1.8, markersize=5)
            plotted = True

        if not plotted:
            plt.close(fig)
            continue

        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("Join level")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
        ax.legend()
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ax.xaxis.set_minor_formatter(ticker.NullFormatter())
        fig.tight_layout()
        save_fig(fig, output_dir, f"fig11{suffix}.png")


# ── E3 Plot (Fig 12) ───────────────────────────────────────────

def _collect_e3(e3_dir: Path) -> Dict[str, List[Dict[str, Any]]]:
    result: Dict[str, List[Dict[str, Any]]] = {}
    if not e3_dir.is_dir():
        return result
    for sub in sorted(e3_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            result.setdefault(engine, []).extend(load_jsonl(raw_path))
    return result


def _extract_union_level(rec: Dict[str, Any]) -> Optional[int]:
    uc = rec.get("union_count")
    if uc is not None:
        try:
            return int(uc)
        except (ValueError, TypeError):
            pass
    qid = rec.get("query_id", "")
    m = re.search(r"U(\d+)", qid)
    if m:
        return int(m.group(1))
    return None


def plot_fig12(e3_data: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> None:
    """Log-log: end-to-end runtime vs UNION fan-in level."""
    engines = [e for e in ENGINE_ORDER if e in e3_data]
    if not engines:
        return

    fig, ax = plt.subplots(figsize=(6, 4))
    plotted = False

    for eng in engines:
        levels, meds = _aggregate_by_level(
            e3_data[eng], _extract_union_level, "wall_time_ms_total")
        if not levels:
            continue
        ax.plot(levels, meds, "o-", label=engine_label(eng),
                color=engine_color(eng), linewidth=1.8, markersize=5)
        plotted = True

    if not plotted:
        plt.close(fig)
        return

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xlabel("UNION ALL fan-in level")
    ax.set_ylabel("Median end-to-end runtime (ms)")
    ax.set_title("UNION ALL scaling")
    ax.legend()
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.xaxis.set_minor_formatter(ticker.NullFormatter())
    fig.tight_layout()
    save_fig(fig, output_dir, "fig12_union_runtime.png")


# ── E4 Plot (Fig 13) ───────────────────────────────────────────

def _collect_e4(e4_dir: Path) -> List[Dict[str, Any]]:
    """Collect all raw records from E4 str1..str15 subdirectories."""
    records: List[Dict[str, Any]] = []
    if not e4_dir.is_dir():
        return records
    for sub in sorted(e4_dir.iterdir()):
        if not sub.is_dir():
            continue
        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            for rec in load_jsonl(raw_path):
                if rec.get("string_level") is None:
                    m = re.search(r"str(\d+)", sub.name)
                    if m:
                        rec["string_level"] = int(m.group(1))
                records.append(rec)
    return records


def plot_fig13(e4_records: List[Dict[str, Any]], output_dir: Path) -> None:
    """Quantile fan: normalized median runtime vs STR level."""
    # Group by (query_id, string_level)
    groups: Dict[Tuple[str, int], List[float]] = defaultdict(list)
    for rec in e4_records:
        if rec.get("status") != "success":
            continue
        qid = rec.get("query_id", "")
        sl = rec.get("string_level")
        t = safe_float(rec.get("wall_time_ms_total"))
        if sl is not None and t is not None and t > 0:
            groups[(qid, int(sl))].append(t)

    if not groups:
        return

    # Compute per-query median at each level
    query_level_median: Dict[str, Dict[int, float]] = defaultdict(dict)
    for (qid, sl), times in groups.items():
        query_level_median[qid][sl] = float(np.median(times))

    # Normalize each query by its value at the lowest STR level present
    all_levels = sorted({sl for _, sl in groups.keys()})
    if len(all_levels) < 2:
        return

    base_level = all_levels[0]
    normalized: Dict[int, List[float]] = defaultdict(list)
    for qid, level_map in query_level_median.items():
        base_val = level_map.get(base_level)
        if base_val is None or base_val <= 0:
            continue
        for sl in all_levels:
            val = level_map.get(sl)
            if val is not None:
                normalized[sl].append(val / base_val)

    if not normalized:
        return

    levels = sorted(normalized.keys())
    p10, p25, p50, p75, p90 = [], [], [], [], []
    for sl in levels:
        vals = sorted(normalized[sl])
        n = len(vals)
        if n < 2:
            p10.append(vals[0]); p25.append(vals[0]); p50.append(vals[0])
            p75.append(vals[0]); p90.append(vals[0])
            continue
        p10.append(float(np.percentile(vals, 10)))
        p25.append(float(np.percentile(vals, 25)))
        p50.append(float(np.percentile(vals, 50)))
        p75.append(float(np.percentile(vals, 75)))
        p90.append(float(np.percentile(vals, 90)))

    fig, ax = plt.subplots(figsize=(7, 4.5))
    levels_arr = np.array(levels)

    ax.fill_between(levels_arr, p10, p90, alpha=0.15, color="#FF6B00", label="P10\u2013P90")
    ax.fill_between(levels_arr, p25, p75, alpha=0.30, color="#FF6B00", label="P25\u2013P75")
    ax.plot(levels_arr, p50, "o-", color="#FF6B00", linewidth=2, markersize=5, label="Median")

    ax.axhline(1.0, color="#999999", linestyle="--", linewidth=0.8)
    ax.set_xlabel("Stringification level (STR)")
    ax.set_ylabel(f"Normalized runtime (vs STR={base_level})")
    ax.set_title("Stringification sweep: runtime by STR level")
    ax.set_xticks(levels)
    ax.legend(loc="upper left")
    fig.tight_layout()
    save_fig(fig, output_dir, "fig13_str_quantile.png")


# ── E5 Plot (Table 3) ──────────────────────────────────────────

def _collect_e5(e5_dir: Path) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """Return {variant: {engine: [records]}} from E5 subdirectories.

    Directory names follow the pattern: <variant>_<engine>
    e.g. baseline_duckdb, sparsity_only_duckdb, combined_duckdb
    """
    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    if not e5_dir.is_dir():
        return result

    for sub in sorted(e5_dir.iterdir()):
        if not sub.is_dir():
            continue
        engine = _detect_engine(sub.name)
        # Strip engine name to get variant
        variant = sub.name
        for eng in ENGINE_ORDER:
            variant = variant.replace(f"_{eng}", "").replace(eng, "")
        variant = variant.strip("_") or sub.name

        raw_path = _find_latest_file(sub, "raw.jsonl")
        if raw_path:
            result.setdefault(variant, {}).setdefault(engine, []).extend(
                load_jsonl(raw_path))
    return result


def plot_table3(e5_data: Dict[str, Dict[str, List[Dict[str, Any]]]],
                output_dir: Path) -> None:
    """Bar chart showing delta-% vs baseline for each variant and engine."""
    if "baseline" not in e5_data:
        return

    # Compute per-engine total successful runtime for each variant
    variants = [v for v in ["sparsity_only", "skew_only", "combined"]
                if v in e5_data]
    if not variants:
        return

    all_engines = set()
    for vdata in e5_data.values():
        all_engines.update(vdata.keys())
    engines = [e for e in ENGINE_ORDER if e in all_engines]
    if not engines:
        return

    # Total runtime per (variant, engine)
    def _total_runtime(records: List[Dict[str, Any]]) -> Optional[float]:
        times = [safe_float(r.get("wall_time_ms_total"))
                 for r in records if r.get("status") == "success"]
        times = [t for t in times if t is not None and t > 0]
        return sum(times) if times else None

    baseline_totals: Dict[str, Optional[float]] = {}
    for eng in engines:
        baseline_totals[eng] = _total_runtime(e5_data["baseline"].get(eng, []))

    fig, ax = plt.subplots(figsize=(6, 4))
    n_variants = len(variants)
    n_engines = len(engines)
    bar_width = 0.8 / max(n_engines, 1)
    x = np.arange(n_variants)

    for j, eng in enumerate(engines):
        deltas = []
        for var in variants:
            var_total = _total_runtime(e5_data.get(var, {}).get(eng, []))
            base_total = baseline_totals.get(eng)
            if var_total is not None and base_total is not None and base_total > 0:
                delta_pct = (var_total - base_total) / base_total * 100
                deltas.append(delta_pct)
            else:
                deltas.append(0)

        offset = (j - (n_engines - 1) / 2) * bar_width
        ax.bar(x + offset, deltas, bar_width, label=engine_label(eng),
               color=engine_color(eng), edgecolor="white", linewidth=0.5)

    ax.axhline(0, color="#333333", linewidth=0.8)
    ax.set_ylabel(r"$\Delta$% total runtime vs baseline")
    ax.set_title("Sparsity & skew sensitivity")
    ax.set_xticks(x)
    variant_labels = {
        "sparsity_only": "Sparsity only",
        "skew_only":     "Skew only",
        "combined":      "Combined",
    }
    ax.set_xticklabels([variant_labels.get(v, v) for v in variants])
    ax.legend()
    fig.tight_layout()
    save_fig(fig, output_dir, "table3_sparsity_skew.png")


# ── Main ────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate paper figures from experiment results.")
    parser.add_argument("--results-dir", required=True, type=Path,
                        help="Base results directory (e.g. .reproduce/results)")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Directory to save PNG plots")
    args = parser.parse_args()

    results_dir: Path = args.results_dir
    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    apply_style()

    if not results_dir.is_dir():
        print(f"Results directory does not exist: {results_dir}", file=sys.stderr)
        sys.exit(1)

    e1_dir = results_dir / "E1"
    e2_dir = results_dir / "E2"
    e3_dir = results_dir / "E3"
    e4_dir = results_dir / "E4"
    e5_dir = results_dir / "E5"

    generated = 0

    # ── E1 ──────────────────────────────────────────────────────
    if e1_dir.is_dir():
        print("E1: generating Figs 8, 9, 10 ...")
        summaries = _collect_e1_summaries(e1_dir)
        raw = _collect_e1_raw(e1_dir)

        if summaries:
            plot_fig8_bar(summaries, output_dir, "median",
                          "fig8a_median_runtime.png",
                          "Median query runtime: TPC-DS vs Prod-DS")
            plot_fig8_bar(summaries, output_dir, "mean",
                          "fig8b_average_runtime.png",
                          "Average query runtime: TPC-DS vs Prod-DS")
            plot_fig8_bar(summaries, output_dir, "sum",
                          "fig8c_total_runtime.png",
                          "Total workload runtime: TPC-DS vs Prod-DS")
            plot_fig9_cdf(summaries, output_dir)
            generated += 4

        if raw:
            plot_fig10_errors(raw, output_dir)
            generated += 1
    else:
        print("E1: skipped (directory not found)")

    # ── E2 ──────────────────────────────────────────────────────
    if e2_dir.is_dir():
        print("E2: generating Fig 11 ...")
        e2_data = _collect_e2(e2_dir)
        if e2_data:
            plot_fig11(e2_data, output_dir)
            generated += 2
    else:
        print("E2: skipped (directory not found)")

    # ── E3 ──────────────────────────────────────────────────────
    if e3_dir.is_dir():
        print("E3: generating Fig 12 ...")
        e3_data = _collect_e3(e3_dir)
        if e3_data:
            plot_fig12(e3_data, output_dir)
            generated += 1
    else:
        print("E3: skipped (directory not found)")

    # ── E4 ──────────────────────────────────────────────────────
    if e4_dir.is_dir():
        print("E4: generating Fig 13 ...")
        e4_records = _collect_e4(e4_dir)
        if e4_records:
            plot_fig13(e4_records, output_dir)
            generated += 1
    else:
        print("E4: skipped (directory not found)")

    # ── E5 ──────────────────────────────────────────────────────
    if e5_dir.is_dir():
        print("E5: generating Table 3 plot ...")
        e5_data = _collect_e5(e5_dir)
        if e5_data:
            plot_table3(e5_data, output_dir)
            generated += 1
    else:
        print("E5: skipped (directory not found)")

    print(f"\nDone. {generated} plot(s) generated in {output_dir}")


if __name__ == "__main__":
    main()

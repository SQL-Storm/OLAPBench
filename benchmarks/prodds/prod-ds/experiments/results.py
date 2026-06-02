from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional


class ResultWriter:
    def __init__(self, raw_path: Path):
        self.raw_path = raw_path
        self.raw_path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.raw_path.open("a", encoding="utf-8")

    def write(self, record: Dict[str, Any]) -> None:
        self._fh.write(json.dumps(record) + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


def summarize(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        key = _summary_key(record)
        grouped[key].append(record)

    summaries: List[Dict[str, Any]] = []
    for _, items in grouped.items():
        summaries.append(_summarize_group(items))

    return summaries


def _summary_key(record: Dict[str, Any]) -> str:
    parts = [
        record.get("query_id", ""),
        str(record.get("suite", "")),
        str(record.get("experiment_name", "")),
        str(record.get("join_count", "")),
        str(record.get("union_count", "")),
        str(record.get("string_level", "")),
        str(record.get("seed", "")),
    ]
    return "|".join(parts)


def _summarize_group(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    sample = items[0]
    total = len(items)
    successes = [i for i in items if i.get("status") == "success"]
    success_times = [i.get("wall_time_ms_total") for i in successes if i.get("wall_time_ms_total") is not None]
    planning_times = [i.get("wall_time_ms_planning") for i in successes if i.get("wall_time_ms_planning") is not None]
    execution_times = [i.get("wall_time_ms_execution") for i in successes if i.get("wall_time_ms_execution") is not None]

    summary = {
        "query_id": sample.get("query_id"),
        "suite": sample.get("suite"),
        "experiment_name": sample.get("experiment_name"),
        "join_count": sample.get("join_count"),
        "string_level": sample.get("string_level"),
        "seed": sample.get("seed"),
        "runs_total": total,
        "runs_success": len(successes),
        "runs_failed": total - len(successes),
        "failure_rate": (total - len(successes)) / total if total else 0,
        "median_ms": median(success_times) if success_times else None,
        "min_ms": min(success_times) if success_times else None,
        "max_ms": max(success_times) if success_times else None,
        "median_planning_ms": median(planning_times) if planning_times else None,
        "median_execution_ms": median(execution_times) if execution_times else None,
    }
    return summary


def write_summary(path: Path, summaries: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not summaries:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(summaries[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in summaries:
            writer.writerow(row)

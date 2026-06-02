from __future__ import annotations

import csv
import datetime as dt
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from .adapters import ADAPTERS
from .config import BenchConfig
from .logging_utils import setup_logging
from .process import run_shell_command
from .resources import ResourceController
from .results import ResultWriter, summarize, write_summary
from .utils import (
    dump_json,
    ensure_dir,
    find_git_hash,
    first_n,
    get_host_info,
    iso_timestamp,
    load_sql_file,
    normalize_sql,
    read_sql_files,
    truncate_error,
)


@dataclass
class QuerySpec:
    query_id: str
    sql: Optional[str]
    suite: str
    join_count: Optional[int] = None
    union_count: Optional[int] = None
    string_level: Optional[int] = None
    seed: Optional[int] = None
    path: Optional[str] = None
    generation_error: Optional[str] = None


class BenchmarkRunner:
    def __init__(self, cfg: BenchConfig, experiment: str, system: str):
        self.cfg = cfg
        self.experiment = experiment
        self.system = system.lower()
        self.timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.run_root = Path(self.cfg.global_cfg.results_dir) / self.timestamp
        ensure_dir(self.run_root)

        log_path = self.run_root / "run.log"
        self.logger = setup_logging(self.cfg.global_cfg.log_level, log_path)

        self.resource = ResourceController(
            memory_limit_bytes=self.cfg.global_cfg.memory_limit_bytes,
            cpu_affinity=self.cfg.global_cfg.cpu_affinity,
            logger=self.logger,
        )

        self.git_hashes = self._collect_git_hashes()

    def run(self) -> None:
        host_info = get_host_info()
        manifest: Dict[str, Any] = {
            "timestamp": self.timestamp,
            "config": self.cfg.raw,
            "host": host_info,
            "git_hashes": self.git_hashes,
            "systems": {},
        }

        systems = self._select_systems()
        for system_name in systems:
            adapter = self._build_adapter(system_name)
            system_dir = self.run_root / system_name / self.experiment
            ensure_dir(system_dir)

            raw_path = system_dir / "raw.jsonl"
            summary_path = system_dir / "summary.csv"

            writer = ResultWriter(raw_path)
            records: List[Dict[str, Any]] = []

            try:
                adapter.start()
                adapter.load_data()
                manifest["systems"][system_name] = adapter.version()

                if self.experiment == "workload_compare":
                    records = self._run_workload_compare(adapter, writer, system_dir)
                elif self.experiment == "join_scaling":
                    records = self._run_join_scaling(adapter, writer, system_dir)
                elif self.experiment == "union_scaling":
                    records = self._run_union_scaling(adapter, writer, system_dir)
                elif self.experiment == "string_sweep":
                    records = self._run_string_sweep(adapter, writer, system_dir)
                else:
                    raise RuntimeError(f"Unknown experiment: {self.experiment}")

                summaries = summarize(records)
                write_summary(summary_path, summaries)
                if self.experiment == "workload_compare":
                    for suite_name in {"tpcds", "prodds"}:
                        suite_records = [r for r in records if r.get("suite") == suite_name]
                        if suite_records:
                            suite_summary = summarize(suite_records)
                            write_summary(system_dir / f"summary_{suite_name}.csv", suite_summary)
            finally:
                writer.close()
                adapter.stop()

        dump_json(self.run_root / "manifest.json", manifest)
        self.logger.info("Results written to %s", self.run_root)

    def _select_systems(self) -> List[str]:
        if self.system == "all":
            return [name for name, cfg in self.cfg.engines.items() if cfg.get("enabled", True)]
        if self.system not in self.cfg.engines:
            raise RuntimeError(f"System '{self.system}' not defined in config.")
        return [self.system]

    def _build_adapter(self, system_name: str):
        adapter_cls = ADAPTERS.get(system_name)
        if not adapter_cls:
            raise RuntimeError(f"Unsupported system: {system_name}")
        cfg = self.cfg.engines[system_name]
        return adapter_cls(cfg, self.cfg.global_cfg, self.resource, self.logger)

    def _collect_git_hashes(self) -> Dict[str, Optional[str]]:
        hashes: Dict[str, Optional[str]] = {
            "harness": find_git_hash(str(self.cfg.root_dir)),
            "join_generator": None,
            "string_generator": None,
        }

        join_cfg = self.cfg.experiments.get("join_scaling") or {}
        join_cmd = join_cfg.get("generator_command")
        hashes["join_generator"] = self._git_hash_from_command(join_cmd)

        string_cfg = self.cfg.experiments.get("string_sweep") or {}
        string_cmd = string_cfg.get("generator_command")
        hashes["string_generator"] = self._git_hash_from_command(string_cmd)

        return hashes

    def _git_hash_from_command(self, command: Optional[str]) -> Optional[str]:
        if not command:
            return None
        try:
            parts = shlex.split(command)
        except ValueError:
            return None
        if not parts:
            return None

        candidate = parts[0]
        if candidate in {"python", "python3", "bash", "sh"} and len(parts) > 1:
            candidate = parts[1]

        path = Path(candidate)
        if not path.is_absolute():
            path = (self.cfg.root_dir / path).resolve()

        if not path.exists():
            return None

        return find_git_hash(str(path))

    def _run_workload_compare(self, adapter, writer: ResultWriter, system_dir: Path) -> List[Dict[str, Any]]:
        cfg = self.cfg.experiments.get("workload_compare")
        if not cfg:
            raise RuntimeError("experiments.workload_compare missing in config.")

        tpcds_dir = cfg.get("tpcds_dir")
        prodds_dir = cfg.get("prodds_dir")
        if not tpcds_dir or not prodds_dir:
            raise RuntimeError("workload_compare requires tpcds_dir and prodds_dir.")

        records: List[Dict[str, Any]] = []
        run_index = 0

        for suite_name, suite_dir in [("tpcds", tpcds_dir), ("prodds", prodds_dir)]:
            files = read_sql_files(suite_dir)
            queries = [
                QuerySpec(
                    query_id=path.stem,
                    sql=load_sql_file(path),
                    suite=suite_name,
                    path=str(path),
                )
                for path in files
            ]

            validation = self._validate_queries(adapter, queries, system_dir, report_tag=suite_name)
            self._warmup(adapter, queries, suite_name)
            run_index, new_records = self._run_queries(
                adapter,
                queries,
                writer,
                run_index,
                validation_map=validation,
            )
            records.extend(new_records)

        return records

    def _run_join_scaling(self, adapter, writer: ResultWriter, system_dir: Path) -> List[Dict[str, Any]]:
        cfg = self.cfg.experiments.get("join_scaling")
        if not cfg:
            raise RuntimeError("experiments.join_scaling missing in config.")

        join_counts_raw = cfg.get("join_counts") or [100, 200, 400, 800, 1600]
        join_counts = [int(x) for x in join_counts_raw]
        seeds = cfg.get("seeds")
        queries_per_join = int(cfg.get("queries_per_join", 5))
        if seeds is None:
            seeds = list(range(1, queries_per_join + 1))
        else:
            seeds = [int(x) for x in seeds]
            if queries_per_join:
                seeds = seeds[:queries_per_join]

        output_dir = cfg.get("output_dir") or (self.run_root / "generated" / "join_scaling")
        output_dir = Path(output_dir)
        ensure_dir(output_dir)

        queries: List[QuerySpec] = []
        for join_count in join_counts:
            for seed in seeds:
                query_id = f"join_J{join_count}_seed{seed}"
                sql, err = self._generate_join_query(cfg, join_count, seed, output_dir)
                queries.append(
                    QuerySpec(
                        query_id=query_id,
                        sql=sql,
                        suite="join_scaling",
                        join_count=join_count,
                        seed=seed,
                        path=None,
                        generation_error=err,
                    )
                )

        validation = self._validate_queries(adapter, queries, system_dir)
        self._warmup(adapter, queries, "join_scaling")
        _, records = self._run_queries(adapter, queries, writer, 0, validation_map=validation)
        return records

    def _run_union_scaling(self, adapter, writer: ResultWriter, system_dir: Path) -> List[Dict[str, Any]]:
        cfg = self.cfg.experiments.get("union_scaling")
        if not cfg:
            raise RuntimeError("experiments.union_scaling missing in config.")

        union_counts_raw = cfg.get("union_counts") or [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
        union_counts = [int(x) for x in union_counts_raw]

        output_dir = cfg.get("output_dir") or (self.run_root / "generated" / "union_scaling")
        output_dir = Path(output_dir)
        ensure_dir(output_dir)

        queries: List[QuerySpec] = []
        for union_count in union_counts:
            query_id = f"union_U{union_count}"
            sql, err = self._generate_union_query(cfg, union_count, output_dir)
            queries.append(
                QuerySpec(
                    query_id=query_id,
                    sql=sql,
                    suite="union_scaling",
                    union_count=union_count,
                    path=None,
                    generation_error=err,
                )
            )

        validation = self._validate_queries(adapter, queries, system_dir)
        self._warmup(adapter, queries, "union_scaling")
        _, records = self._run_queries(adapter, queries, writer, 0, validation_map=validation)
        return records

    def _generate_union_query(self, cfg: Dict[str, Any], union_count: int, out_dir: Path) -> tuple[Optional[str], Optional[str]]:
        command_tmpl = cfg.get("generator_command")
        if not command_tmpl:
            return None, "union_scaling.generator_command not configured"

        output_file = out_dir / f"union_U{union_count}.sql"
        cmd = command_tmpl.format(
            U=union_count,
            OUTDIR=str(out_dir),
            OUTFILE=str(output_file),
        )
        timeout_s = int(cfg.get("generator_timeout_seconds", 300))
        result = run_shell_command(cmd, timeout_s=timeout_s)
        if result.returncode != 0:
            err = truncate_error(result.stderr or result.stdout)
            return None, err

        if output_file.exists():
            try:
                sql = output_file.read_text(encoding="utf-8")
            except Exception as exc:
                return None, f"Failed to read generated SQL: {exc}"
            return sql, None

        if not result.stdout.strip():
            return None, "Union generator produced empty SQL"
        return result.stdout, None

    def _run_string_sweep(self, adapter, writer: ResultWriter, system_dir: Path) -> List[Dict[str, Any]]:
        cfg = self.cfg.experiments.get("string_sweep")
        if not cfg:
            raise RuntimeError("experiments.string_sweep missing in config.")

        levels_raw = cfg.get("levels") or [1, 3, 5, 7, 10]
        levels = [int(x) for x in levels_raw]
        output_root = cfg.get("output_root") or (self.run_root / "generated" / "string_sweep")
        output_root = Path(output_root)
        ensure_dir(output_root)

        subset_ids = None
        subset_file = cfg.get("query_subset_file")
        if subset_file:
            subset_ids = {
                line.strip() for line in Path(subset_file).read_text(encoding="utf-8").splitlines() if line.strip()
            }

        queries: List[QuerySpec] = []
        for level in levels:
            out_dir = output_root / f"S{level}"
            ensure_dir(out_dir)
            if not self._generate_string_queries(cfg, level, out_dir):
                self.logger.error("Stringification generator failed for level %s; skipping.", level)
                continue

            files = read_sql_files(str(out_dir))
            for path in files:
                if subset_ids and path.stem not in subset_ids:
                    continue
                queries.append(
                    QuerySpec(
                        query_id=path.stem,
                        sql=load_sql_file(path),
                        suite="prodds",
                        string_level=level,
                        path=str(path),
                    )
                )

        validation = self._validate_queries(adapter, queries, system_dir)
        self._warmup(adapter, queries, "string_sweep")
        _, records = self._run_queries(adapter, queries, writer, 0, validation_map=validation)
        return records

    def _warmup(self, adapter, queries: List[QuerySpec], suite: str) -> None:
        warmup_n = self.cfg.global_cfg.warmup_queries
        if warmup_n <= 0:
            return
        warmup_queries = first_n([q for q in queries if q.sql], warmup_n)
        if not warmup_queries:
            return
        self.logger.info("Warmup: %s queries for %s", len(warmup_queries), suite)
        for query in warmup_queries:
            result = adapter.run_sql(query.sql or "")
            if result.status == "engine_crash":
                adapter.restart()

    def _run_queries(
        self,
        adapter,
        queries: List[QuerySpec],
        writer: ResultWriter,
        run_index_start: int,
        *,
        validation_map: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> tuple[int, List[Dict[str, Any]]]:
        records: List[Dict[str, Any]] = []
        run_index = run_index_start
        repetitions = self.cfg.global_cfg.repetitions
        validation_map = validation_map or {}
        validation_cfg = self.cfg.raw.get("validation") or {}
        fail_on_empty = bool(validation_cfg.get("fail_on_empty", True))
        fail_on_too_large = bool(validation_cfg.get("fail_on_too_large", False))
        fail_on_large_sql = bool(validation_cfg.get("fail_on_large_sql", False))

        for query in queries:
            validation_key = self._validation_key(query)
            validation = validation_map.get(validation_key, {})
            for rep in range(1, repetitions + 1):
                run_index += 1
                record = {
                    "timestamp": iso_timestamp(),
                    "experiment_name": self.experiment,
                    "suite": query.suite,
                    "system": adapter.name,
                    "query_id": query.query_id,
                    "query_path": query.path,
                    "join_count": query.join_count,
                    "union_count": query.union_count,
                    "string_level": query.string_level,
                    "seed": query.seed,
                    "run_index": run_index,
                    "repetition_index": rep,
                    "threads": self.cfg.global_cfg.threads,
                    "memory_limit_bytes": self.cfg.global_cfg.memory_limit_bytes,
                    "execution_mode": self.cfg.global_cfg.execution_mode,
                    "git_hash_harness": self.git_hashes.get("harness"),
                    "git_hash_join_generator": self.git_hashes.get("join_generator"),
                    "git_hash_string_generator": self.git_hashes.get("string_generator"),
                }

                if validation:
                    record.update(validation)

                if query.generation_error:
                    record.update(
                        {
                            "status": "error",
                            "error_type": "error",
                            "error_message": query.generation_error,
                            "return_code": None,
                            "wall_time_ms_total": None,
                            "wall_time_ms_planning": None,
                            "wall_time_ms_execution": None,
                            "peak_rss_bytes": None,
                        }
                    )
                    writer.write(record)
                    records.append(record)
                    continue

                result = adapter.run_sql(query.sql or "")
                record.update(
                    {
                        "status": result.status,
                        "error_type": result.error_type,
                        "error_message": result.error_message,
                        "return_code": result.return_code,
                        "wall_time_ms_total": result.wall_time_ms_total,
                        "wall_time_ms_planning": result.wall_time_ms_planning,
                        "wall_time_ms_execution": result.wall_time_ms_execution,
                        "peak_rss_bytes": result.peak_rss_bytes,
                    }
                )

                if record.get("status") == "success":
                    if validation.get("validation_empty") and fail_on_empty:
                        record["status"] = "validation_empty"
                    elif validation.get("validation_too_large_rows") and fail_on_too_large:
                        record["status"] = "validation_too_large_rows"
                    elif validation.get("validation_too_large_sql") and fail_on_large_sql:
                        record["status"] = "validation_too_large_sql"

                writer.write(record)
                records.append(record)

                if result.status == "engine_crash":
                    self.logger.warning("Engine crash detected. Restarting %s.", adapter.name)
                    adapter.restart()

        return run_index, records

    def _validation_key(self, query: QuerySpec) -> str:
        parts = [
            str(query.suite),
            str(query.query_id),
            str(query.join_count),
            str(query.union_count),
            str(query.string_level),
            str(query.seed),
        ]
        return "|".join(parts)

    def _parse_scalar_int(self, output: str) -> Optional[int]:
        pattern = re.compile(r"^[\\s\\|\\+\\-]*\\d+[\\s\\|\\+\\-]*$")
        for line in output.splitlines():
            if not line.strip():
                continue
            if pattern.match(line):
                match = re.search(r"\\d+", line)
                if match:
                    return int(match.group(0))
        return None

    def _parse_scalar_bool(self, output: str) -> Optional[bool]:
        for line in output.splitlines():
            raw = line.strip().strip("|").strip()
            if not raw:
                continue
            lowered = raw.lower()
            if lowered in {"t", "true", "1", "yes"}:
                return True
            if lowered in {"f", "false", "0", "no"}:
                return False
        return None

    def _validate_queries(
        self,
        adapter,
        queries: List[QuerySpec],
        system_dir: Path,
        *,
        report_tag: Optional[str] = None,
    ) -> Dict[str, Dict[str, Any]]:
        validation_cfg = self.cfg.raw.get("validation") or {}
        if not validation_cfg.get("enabled", False):
            return {}

        check_empty = bool(validation_cfg.get("check_empty", True))
        max_rows = validation_cfg.get("max_rows")
        max_query_bytes = validation_cfg.get("max_query_bytes")
        timeout_s = int(validation_cfg.get("timeout_seconds", self.cfg.global_cfg.timeout_seconds_execution))

        report_path = validation_cfg.get("report_path")
        if report_path:
            report_path = Path(str(report_path))
            if not report_path.is_absolute():
                report_path = system_dir / report_path
        else:
            suffix = f"_{report_tag}" if report_tag else ""
            report_path = system_dir / f"validation_report{suffix}.csv"

        rows: List[Dict[str, Any]] = []
        results: Dict[str, Dict[str, Any]] = {}

        for query in queries:
            key = self._validation_key(query)
            sql = normalize_sql(query.sql) if query.sql else None
            sql_bytes = len(sql.encode("utf-8")) if sql else None
            too_large_sql = False
            if max_query_bytes is not None and sql_bytes is not None:
                try:
                    too_large_sql = int(sql_bytes) > int(max_query_bytes)
                except Exception:
                    too_large_sql = False

            row_count: Optional[int] = None
            empty_result: Optional[bool] = None
            error: Optional[str] = None

            if not sql:
                error = query.generation_error or "missing_sql"
            else:
                try:
                    if max_rows is not None:
                        count_sql = f"SELECT COUNT(*) FROM ({sql}) AS _q"
                        count_res = adapter.run_sql_raw(count_sql, timeout_s=timeout_s)
                        if count_res.timed_out:
                            error = "timeout"
                        elif count_res.returncode not in (0, None):
                            error = truncate_error(count_res.stderr or count_res.stdout)
                        else:
                            row_count = self._parse_scalar_int(count_res.stdout)
                            if row_count is None:
                                error = "could_not_parse_row_count"
                            else:
                                empty_result = row_count == 0
                    elif check_empty:
                        exists_sql = f"SELECT 1 FROM ({sql}) AS _q LIMIT 1"
                        exists_res = adapter.run_sql_raw(exists_sql, timeout_s=timeout_s)
                        if exists_res.timed_out:
                            error = "timeout"
                        elif exists_res.returncode not in (0, None):
                            error = truncate_error(exists_res.stderr or exists_res.stdout)
                        else:
                            parsed = self._parse_scalar_bool(exists_res.stdout)
                            if parsed is None:
                                parsed_int = self._parse_scalar_int(exists_res.stdout)
                                if parsed_int is not None:
                                    parsed = parsed_int > 0
                            if parsed is None:
                                error = "could_not_parse_exists"
                            else:
                                empty_result = not parsed
                except Exception as exc:
                    error = str(exc)

            too_large_rows = False
            if row_count is not None and max_rows is not None:
                try:
                    too_large_rows = int(row_count) > int(max_rows)
                except Exception:
                    too_large_rows = False

            flags = []
            if empty_result:
                flags.append("empty")
            if too_large_rows:
                flags.append("rows_gt_max")
            if too_large_sql:
                flags.append("sql_bytes_gt_max")
            if error:
                flags.append("error")

            row = {
                "query_id": query.query_id,
                "suite": query.suite,
                "join_count": query.join_count,
                "string_level": query.string_level,
                "seed": query.seed,
                "sql_bytes": sql_bytes,
                "row_count": row_count,
                "empty": empty_result,
                "too_large_rows": too_large_rows,
                "too_large_sql": too_large_sql,
                "flags": ",".join(flags) if flags else "",
                "error": error,
            }
            rows.append(row)
            results[key] = {
                "validation_empty": empty_result,
                "validation_row_count": row_count,
                "validation_too_large_rows": too_large_rows,
                "validation_too_large_sql": too_large_sql,
                "validation_error": error,
                "validation_flags": flags,
            }

        if report_path:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            fieldnames = [
                "query_id",
                "suite",
                "join_count",
                "string_level",
                "seed",
                "sql_bytes",
                "row_count",
                "empty",
                "too_large_rows",
                "too_large_sql",
                "flags",
                "error",
            ]
            with report_path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                for row in rows:
                    writer.writerow(row)

        return results

    def _generate_join_query(self, cfg: Dict[str, Any], join_count: int, seed: int, out_dir: Path) -> tuple[Optional[str], Optional[str]]:
        command_tmpl = cfg.get("generator_command")
        if not command_tmpl:
            return None, "join_scaling.generator_command not configured"

        output_file_tmpl = cfg.get("generator_output_file")
        output_file = None
        if output_file_tmpl:
            output_file = str(output_file_tmpl).format(J=join_count, SEED=seed, OUTDIR=str(out_dir))

        cmd = command_tmpl.format(
            J=join_count,
            SEED=seed,
            OUTDIR=str(out_dir),
            OUTFILE=str(output_file) if output_file else "",
        )
        timeout_s = int(cfg.get("generator_timeout_seconds", 300))
        result = run_shell_command(cmd, timeout_s=timeout_s)
        if result.returncode != 0:
            err = truncate_error(result.stderr or result.stdout)
            return None, err

        if output_file:
            try:
                sql = Path(output_file).read_text(encoding="utf-8")
            except Exception as exc:
                return None, f"Failed to read generated SQL: {exc}"
            return sql, None

        if not result.stdout.strip():
            return None, "Join generator produced empty SQL"
        return result.stdout, None

    def _generate_string_queries(self, cfg: Dict[str, Any], level: int, out_dir: Path) -> bool:
        command_tmpl = cfg.get("generator_command")
        if not command_tmpl:
            raise RuntimeError("string_sweep.generator_command not configured")

        subset_file = cfg.get("query_subset_file") or ""
        cmd = command_tmpl.format(S=level, OUTDIR=str(out_dir), SUBSET_FILE=subset_file)
        timeout_s = int(cfg.get("generator_timeout_seconds", 600))
        result = run_shell_command(cmd, timeout_s=timeout_s)
        if result.returncode != 0:
            self.logger.error("String sweep generator failed: %s", result.stderr or result.stdout)
            return False
        return True

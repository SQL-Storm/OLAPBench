#!/usr/bin/env python3
from __future__ import annotations

import csv
import datetime as dt
import difflib
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
GEN_SCRIPT = REPO / "workload" / "dsqgen" / "generate_join_query.py"
CFG_PATH = REPO / "workload" / "config" / "returns.yml"
BASE_TEMPLATE = REPO / "workload" / "templates" / "base_returns.tpl"
JOIN_DIR = Path(os.environ.get("PRODDS_JOIN_DIR", REPO / "queries" / "prodds" / "generators" / "join"))
DUCKDB_DB = Path(os.environ.get("PRODDS_DUCKDB_DB", REPO / "data" / "duckdb" / "prodds_sf10_str10.duckdb"))
JOIN_LEVELS = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]
LOW_LEVELS = [1, 2, 4, 8, 16]
MAX_M_SEARCH = 24

ART = REPO / "experiments" / "artifacts" / "E7_preflight_join_generator"
DIFF_DIR = ART / "raw_sql_diffs"
COMMANDS_LOG = ART / "commands.log"
PROGRESS_LOG = ART / "progress.log"
CALIB_CSV = ART / "calibration_results.csv"
CHOSEN_JSON = ART / "chosen_params.json"
SANITY_CSV = ART / "join_sql_sanity.csv"
OVERVIEW_MD = ART / "overview.md"


def ts() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg: str) -> None:
    line = f"{ts()} {msg}"
    print(line, flush=True)
    with PROGRESS_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_cmd(cmd: list[str], stdin_preview: str | None = None) -> None:
    shown = " ".join(cmd)
    if stdin_preview:
        shown += " # stdin=" + " ".join(stdin_preview.strip().split())[:220]
    with COMMANDS_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{ts()} cmd={shown}\n")


def run_cmd(
    cmd: list[str],
    *,
    timeout_s: int = 120,
    stdin_text: str | None = None,
) -> tuple[int, str, str, float, bool]:
    log_cmd(cmd, stdin_preview=stdin_text)
    t0 = time.perf_counter()
    try:
        p = subprocess.run(cmd, input=stdin_text, text=True, capture_output=True, timeout=timeout_s)
        elapsed = (time.perf_counter() - t0) * 1000.0
        return p.returncode, p.stdout or "", p.stderr or "", elapsed, False
    except subprocess.TimeoutExpired as e:
        elapsed = (time.perf_counter() - t0) * 1000.0
        return (
            124,
            (e.stdout or "") if isinstance(e.stdout, str) else "",
            (e.stderr or "") if isinstance(e.stderr, str) else "",
            elapsed,
            True,
        )


def stop_other_engines() -> None:
    cmds = [
        ["bash", "-lc", "pkill -x cedardb >/dev/null 2>&1 || true"],
        ["bash", "-lc", "monetdb stop prodds_sf10_str10 >/dev/null 2>&1 || true"],
        ["bash", "-lc", "monetdb stop tpcds_sf10 >/dev/null 2>&1 || true"],
        ["bash", "-lc", f"monetdbd stop {os.environ.get('MONETDB_FARM', 'monetdb/experiment0_patch_farm')} >/dev/null 2>&1 || true"],
        ["bash", "-lc", "sudo systemctl stop postgresql >/dev/null 2>&1 || true"],
    ]
    for cmd in cmds:
        run_cmd(cmd, timeout_s=30)


def preflight_one_engine() -> tuple[bool, str]:
    rc, out, err, _, _ = run_cmd([str(REPO / "tools" / "preflight_one_engine.sh")], timeout_s=20)
    return rc == 0, (out + "\n" + err).strip()


def normalize_sql(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
    sql = re.sub(r"\s+", " ", sql).strip().lower()
    return sql


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_operator_counts(explain_text: str) -> tuple[int, int]:
    join_ops = 0
    scan_ops = 0
    for line in explain_text.splitlines():
        if "│" not in line:
            continue
        parts = [p.strip() for p in line.split("│") if p.strip()]
        for token in parts:
            if re.fullmatch(r"[A-Z_]+", token) is None:
                continue
            if token in {
                "HASH_JOIN",
                "NESTED_LOOP_JOIN",
                "PIECEWISE_MERGE_JOIN",
                "MERGE_JOIN",
                "ASOF_JOIN",
                "BLOCKWISE_NL_JOIN",
            }:
                join_ops += 1
            if token in {
                "SEQ_SCAN",
                "TABLE_SCAN",
                "COLUMN_DATA_SCAN",
                "INDEX_SCAN",
                "DELIM_SCAN",
                "DUMMY_SCAN",
            }:
                scan_ops += 1
    return join_ops, scan_ops


def join_keyword_count(sql: str) -> int:
    return len(re.findall(r"\bjoin\b", sql, flags=re.I))


def load_cfg() -> dict[str, Any]:
    import yaml

    cfg = yaml.safe_load(CFG_PATH.read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise RuntimeError("returns.yml is not a dict")
    return cfg


def generate_sql(*, target: int, k: int, m: int, template_text: str) -> str:
    rc, out, err, _, to = run_cmd(
        [
            "python3",
            str(GEN_SCRIPT),
            "--cfg",
            str(CFG_PATH),
            "--target",
            str(target),
            "--k",
            str(k),
            "--m",
            str(m),
        ],
        timeout_s=60,
        stdin_text=template_text,
    )
    if rc != 0 or to:
        msg = (err or out or "generator failed").strip().replace("\n", " ")[:300]
        raise RuntimeError(f"generator failed k={k} m={m}: {msg}")
    return out


def explain_join_ops(sql: str) -> tuple[int, int, float]:
    rc, out, err, elapsed_ms, to = run_cmd(
        ["duckdb", str(DUCKDB_DB), "-c", f"PRAGMA threads=56; EXPLAIN {sql}"],
        timeout_s=300,
    )
    if rc != 0 or to:
        msg = (err or out or "explain failed").strip().replace("\n", " ")[:300]
        raise RuntimeError(msg)
    join_ops, scan_ops = extract_operator_counts(out)
    return join_ops, scan_ops, elapsed_ms


def compute_sanity_rows(levels: list[int]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for j in levels:
        path = JOIN_DIR / f"join_{j}.sql"
        if not path.exists():
            rows.append(
                {
                    "J": str(j),
                    "path": str(path),
                    "exists": "false",
                    "sha256": "",
                    "size_bytes": "",
                    "normalized_sha256": "",
                    "join_keyword_count": "",
                    "from_keyword_count": "",
                    "from_clause_comma_count": "",
                    "adjacent_suspicion": "",
                }
            )
            continue
        sql = path.read_text(encoding="utf-8")
        norm = normalize_sql(sql)
        from_count = len(re.findall(r"\bfrom\b", sql, flags=re.I))
        m = re.search(
            r"\bfrom\b(.*?)(\bwhere\b|\bgroup\b|\border\b|\bhaving\b|\blimit\b|;|$)",
            sql,
            flags=re.I | re.S,
        )
        comma_count = m.group(1).count(",") if m else 0
        rows.append(
            {
                "J": str(j),
                "path": str(path.resolve()),
                "exists": "true",
                "sha256": sha256_file(path),
                "size_bytes": str(path.stat().st_size),
                "normalized_sha256": sha256_text(norm),
                "join_keyword_count": str(join_keyword_count(sql)),
                "from_keyword_count": str(from_count),
                "from_clause_comma_count": str(comma_count),
                "adjacent_suspicion": "",
            }
        )

    by_j = {int(r["J"]): r for r in rows if r["exists"] == "true"}
    for a, b in zip(levels, levels[1:]):
        if a not in by_j or b not in by_j:
            continue
        ra = by_j[a]
        rb = by_j[b]
        reasons: list[str] = []
        if ra["sha256"] == rb["sha256"]:
            reasons.append("same_sha256")
        if ra["size_bytes"] == rb["size_bytes"]:
            reasons.append("same_size")
        if ra["normalized_sha256"] == rb["normalized_sha256"]:
            reasons.append("same_normalized_sha256")
        if reasons:
            tag = f"{a}_vs_{b}:" + "|".join(reasons)
            ra["adjacent_suspicion"] = (ra["adjacent_suspicion"] + ";" if ra["adjacent_suspicion"] else "") + tag
            rb["adjacent_suspicion"] = (rb["adjacent_suspicion"] + ";" if rb["adjacent_suspicion"] else "") + tag

    return rows


def write_sanity_csv(rows: list[dict[str, str]]) -> None:
    with SANITY_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "J",
                "path",
                "exists",
                "sha256",
                "size_bytes",
                "normalized_sha256",
                "join_keyword_count",
                "from_keyword_count",
                "from_clause_comma_count",
                "adjacent_suspicion",
            ],
        )
        w.writeheader()
        by_j = {int(r["J"]): r for r in rows}
        for j in JOIN_LEVELS:
            w.writerow(by_j[j])


def choose_params(
    levels: list[int],
    candidates: dict[tuple[int, int], dict[str, Any]],
) -> tuple[dict[int, dict[str, Any]], bool, dict[str, Any]]:
    valid = [c for c in candidates.values() if int(c["join_ops"]) >= 0]
    by_ops: dict[int, list[dict[str, Any]]] = {}
    for c in valid:
        by_ops.setdefault(int(c["join_ops"]), []).append(c)

    for ops in by_ops:
        by_ops[ops].sort(
            key=lambda c: (
                int(c["k"]) + int(c["m"]),
                abs(int(c["join_keyword_count"]) - ops),
                int(c["k"]),
                int(c["m"]),
            )
        )

    ops_values = sorted(by_ops.keys())
    n = len(levels)
    strict_monotone = True
    selected_ops: list[int] = []

    # Prefer strict monotone assignment via DP over reachable join-op values.
    if len(ops_values) >= n:
        m = len(ops_values)
        INF = 10**18
        dp = [[INF] * m for _ in range(n)]
        prev = [[-1] * m for _ in range(n)]

        for j, ops in enumerate(ops_values):
            dp[0][j] = abs(ops - levels[0])

        for i in range(1, n):
            best_cost = INF
            best_idx = -1
            for j, ops in enumerate(ops_values):
                if j > 0 and dp[i - 1][j - 1] < best_cost:
                    best_cost = dp[i - 1][j - 1]
                    best_idx = j - 1
                if best_cost < INF:
                    dp[i][j] = best_cost + abs(ops - levels[i])
                    prev[i][j] = best_idx

        end = min(range(m), key=lambda j: dp[n - 1][j])
        idxs = [end]
        for i in range(n - 1, 0, -1):
            idxs.append(prev[i][idxs[-1]])
        idxs.reverse()
        selected_ops = [ops_values[idx] for idx in idxs]
    else:
        # Fallback if candidate diversity is too small.
        strict_monotone = False
        selected_ops = sorted(ops_values)[:n]

    selected: dict[int, dict[str, Any]] = {}
    used_sha: set[str] = set()

    for level, ops in zip(levels, selected_ops):
        chosen = None
        for c in by_ops[ops]:
            if c["normalized_sha"] not in used_sha:
                chosen = c
                break
        if chosen is None:
            strict_monotone = False
            chosen = by_ops[ops][0]
        selected[level] = chosen
        used_sha.add(chosen["normalized_sha"])

    meta = {
        "min_join_ops": min(ops_values) if ops_values else None,
        "max_join_ops": max(ops_values) if ops_values else None,
        "selected_join_ops": selected_ops,
    }
    return selected, strict_monotone, meta


def atomic_write(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def main() -> int:
    if ART.exists():
        shutil.rmtree(ART)
    ART.mkdir(parents=True, exist_ok=True)
    DIFF_DIR.mkdir(parents=True, exist_ok=True)

    log("E7 join-generator calibration start")
    log("determinism: fixed candidate grid order, no randomness")

    stop_other_engines()
    ok, pre = preflight_one_engine()
    log(f"preflight ok={ok}")
    if not ok:
        raise RuntimeError(f"preflight failed: {pre}")

    cfg = load_cfg()
    template_text = BASE_TEMPLATE.read_text(encoding="utf-8")
    Kmax = len(cfg["group_keys"])
    Mmax = int(cfg.get("max_filts", 200))
    m_cap = min(MAX_M_SEARCH, Mmax)
    log(f"search grid: k=0..{Kmax}, m=0..{m_cap}")

    # capture old sanity for diff snippets
    old_rows = compute_sanity_rows(JOIN_LEVELS)
    old_by_j = {int(r["J"]): r for r in old_rows if r["exists"] == "true"}
    previously_identical_pairs: list[tuple[int, int]] = []
    for a, b in zip(JOIN_LEVELS, JOIN_LEVELS[1:]):
        if a in old_by_j and b in old_by_j and old_by_j[a]["normalized_sha256"] == old_by_j[b]["normalized_sha256"]:
            previously_identical_pairs.append((a, b))

    # evaluate candidate grid once
    candidates: dict[tuple[int, int], dict[str, Any]] = {}
    for k in range(0, Kmax + 1):
        for m in range(0, m_cap + 1):
            sql = generate_sql(target=0, k=k, m=m, template_text=template_text)
            norm = normalize_sql(sql)
            nsha = sha256_text(norm)
            try:
                join_ops, scan_ops, explain_ms = explain_join_ops(sql)
                note = ""
            except Exception as e:  # pragma: no cover - runtime guard
                join_ops, scan_ops, explain_ms = -1, -1, 0.0
                note = str(e)
            candidates[(k, m)] = {
                "k": k,
                "m": m,
                "join_ops": join_ops,
                "scan_ops": scan_ops,
                "normalized_sha": nsha,
                "size_bytes": len(sql.encode("utf-8")),
                "join_keyword_count": join_keyword_count(sql),
                "explain_ms": explain_ms,
                "notes": note,
                "sql": sql,
            }

    log(f"candidate grid evaluated count={len(candidates)}")

    chosen, strict_monotone, select_meta = choose_params(JOIN_LEVELS, candidates)

    # write calibration table (row per target x candidate)
    with CALIB_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "J",
                "m",
                "k",
                "join_ops",
                "normalized_sha",
                "size_bytes",
                "join_keyword_count",
                "notes",
            ],
        )
        w.writeheader()
        for j in JOIN_LEVELS:
            for c in sorted(
                candidates.values(),
                key=lambda x: (
                    abs(int(x["join_ops"]) - j),
                    abs(int(x["join_keyword_count"]) - j),
                    int(x["m"]) + int(x["k"]),
                    int(x["k"]),
                    int(x["m"]),
                ),
            ):
                notes = c["notes"]
                if chosen[j]["k"] == c["k"] and chosen[j]["m"] == c["m"]:
                    notes = (notes + ";" if notes else "") + "selected"
                w.writerow(
                    {
                        "J": j,
                        "m": c["m"],
                        "k": c["k"],
                        "join_ops": c["join_ops"],
                        "normalized_sha": c["normalized_sha"],
                        "size_bytes": c["size_bytes"],
                        "join_keyword_count": c["join_keyword_count"],
                        "notes": notes,
                    }
                )

    # regenerate final files atomically with selected params
    final_manifest: dict[str, dict[str, Any]] = {}
    for j in JOIN_LEVELS:
        c = chosen[j]
        sql = generate_sql(target=j, k=int(c["k"]), m=int(c["m"]), template_text=template_text)
        out = JOIN_DIR / f"join_{j}.sql"
        atomic_write(out, sql)
        final_manifest[str(j)] = {
            "k": int(c["k"]),
            "m": int(c["m"]),
            "join_ops": int(c["join_ops"]),
            "normalized_sha": c["normalized_sha"],
            "sha256": sha256_file(out),
            "path": str(out),
        }
    CHOSEN_JSON.write_text(json.dumps(final_manifest, indent=2), encoding="utf-8")

    # final sanity
    final_rows = compute_sanity_rows(JOIN_LEVELS)
    write_sanity_csv(final_rows)
    final_by_j = {int(r["J"]): r for r in final_rows if r["exists"] == "true"}

    # diff snippets for previously identical adjacent levels
    for a, b in previously_identical_pairs:
        pa = JOIN_DIR / f"join_{a}.sql"
        pb = JOIN_DIR / f"join_{b}.sql"
        da = pa.read_text(encoding="utf-8").splitlines(keepends=True)
        db = pb.read_text(encoding="utf-8").splitlines(keepends=True)
        diff = "".join(difflib.unified_diff(da, db, fromfile=f"join_{a}.sql", tofile=f"join_{b}.sql"))
        if not diff:
            diff = "# Files are still byte-identical\n"
        (DIFF_DIR / f"J{a}_vs_J{b}.diff").write_text(diff, encoding="utf-8")

    # stop condition evaluation
    low_norm = [final_by_j[j]["normalized_sha256"] for j in LOW_LEVELS if j in final_by_j]
    low_distinct = len(set(low_norm)) == len(LOW_LEVELS)
    chosen_ops = [int(final_manifest[str(j)]["join_ops"]) for j in JOIN_LEVELS]
    strict_monotone_final = all(chosen_ops[i] < chosen_ops[i + 1] for i in range(len(chosen_ops) - 1))
    monotone = strict_monotone_final or all(chosen_ops[i] <= chosen_ops[i + 1] for i in range(len(chosen_ops) - 1))

    stop_recommend_exclude_low = not low_distinct

    lines: list[str] = []
    lines.append("# E7 Join Generator Calibration")
    lines.append("")
    lines.append(f"- Timestamp: `{ts()}`")
    lines.append(f"- Generator: `{GEN_SCRIPT}`")
    lines.append(f"- Config: `{CFG_PATH}`")
    lines.append(f"- Base template: `{BASE_TEMPLATE}`")
    lines.append(f"- Levels: `{JOIN_LEVELS}`")
    lines.append(f"- Candidate grid: `k=0..{Kmax}, m=0..{m_cap}`")
    lines.append("")
    lines.append("## Floor Diagnosis")
    lines.append("- Floor mechanism source: `solve_km_for_target_prefer_k` uses model `J0(k)=k*(b+1)+b`, so for small targets with `b=12` it collapses to `k=0,m=0`.")
    lines.append("- Minimal patch implemented: generator now supports explicit `--k/--m` and optional `target_overrides` to bypass solver floor deterministically.")
    lines.append("")
    lines.append("## Selection Outcome")
    lines.append(f"- Strict monotone selection algorithm succeeded: `{strict_monotone}`")
    lines.append(f"- Final low-level normalized distinctness (J=1..16): `{low_distinct}`")
    lines.append(f"- Final chosen join_ops by level: `{dict((j, final_manifest[str(j)]['join_ops']) for j in JOIN_LEVELS)}`")
    lines.append(
        f"- Reachable join_ops envelope in searched space: `[{select_meta.get('min_join_ops')}, {select_meta.get('max_join_ops')}]`"
    )
    lines.append(f"- Selected join_ops sequence: `{select_meta.get('selected_join_ops')}`")
    lines.append(f"- Final monotone (non-decreasing) join_ops over all levels: `{monotone}`")
    lines.append(f"- Final strict monotone join_ops over all levels: `{strict_monotone_final}`")
    lines.append("")
    lines.append("## Files")
    lines.append(f"- `calibration_results.csv`: `{CALIB_CSV}`")
    lines.append(f"- `chosen_params.json`: `{CHOSEN_JSON}`")
    lines.append(f"- `join_sql_sanity.csv`: `{SANITY_CSV}`")
    lines.append(f"- `raw_sql_diffs/`: `{DIFF_DIR}`")
    lines.append("")

    if stop_recommend_exclude_low:
        lines.append("## Stop Condition")
        lines.append("- Unable to produce distinct low levels after minimal patch.")
        lines.append("- Recommendation: exclude low J and start active levels at 32.")
    else:
        lines.append("## Conclusion")
        lines.append("- Generator now emits structurally distinct SQL for low levels and calibrated complexity mapping across all active levels.")

    OVERVIEW_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    log("E7 join-generator calibration done")
    print("tail -f experiments/artifacts/E7_preflight_join_generator/progress.log")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

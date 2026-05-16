#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

JOIN_HEADER_RE = re.compile(
    r"--\s*STRATEGY=(?P<strategy>\S+)\s+chosen k=(?P<k>\d+)\s+m=(?P<m>\d+)\s+EXPECTED_EFFECTIVE_JOINS=(?P<j>\d+)",
    flags=re.I,
)
JOIN_FILE_RE = re.compile(r"join_(\d+)\.sql$", flags=re.I)

JOIN_OPS = {
    "HASH_JOIN",
    "NESTED_LOOP_JOIN",
    "PIECEWISE_MERGE_JOIN",
    "MERGE_JOIN",
    "ASOF_JOIN",
    "BLOCKWISE_NL_JOIN",
    "CROSS_PRODUCT",
}


def parse_levels(raw: str) -> set[int]:
    vals: set[int] = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        vals.add(int(p))
    return vals


def list_join_files(join_dir: Path) -> list[tuple[int, Path]]:
    out: list[tuple[int, Path]] = []
    for path in join_dir.glob("join_*.sql"):
        m = JOIN_FILE_RE.search(path.name)
        if not m:
            continue
        out.append((int(m.group(1)), path))
    out.sort(key=lambda x: x[0])
    return out


def parse_header(sql: str) -> tuple[str, int, int, int] | None:
    m = JOIN_HEADER_RE.search(sql)
    if not m:
        return None
    return (
        m.group("strategy"),
        int(m.group("k")),
        int(m.group("m")),
        int(m.group("j")),
    )


def sql_join_keyword_count(sql: str) -> int:
    return len(re.findall(r"\bjoin\b", sql, flags=re.I))


def count_plan_joins(plan_text: str) -> int:
    cnt = 0
    for line in plan_text.splitlines():
        if "│" not in line:
            continue
        toks = [t.strip() for t in line.split("│") if t.strip()]
        for tok in toks:
            if re.fullmatch(r"[A-Z_]+", tok) and tok in JOIN_OPS:
                cnt += 1
    if cnt == 0:
        cnt = len(
            re.findall(
                r"\b(?:HASH_JOIN|NESTED_LOOP_JOIN|PIECEWISE_MERGE_JOIN|MERGE_JOIN|ASOF_JOIN|BLOCKWISE_NL_JOIN|CROSS_PRODUCT)\b",
                plan_text,
            )
        )
    return cnt


def count_cedardb_plan_joins(plan_text: str) -> int:
    # CedarDB EXPLAIN typically prints tree lines like: "⨝ JOIN (rightouter, hashjoin)"
    cnt = 0
    for line in plan_text.splitlines():
        if "JOIN" not in line.upper():
            continue
        if ("⨝" in line) or ("JOIN (" in line.upper()):
            cnt += 1
    if cnt == 0:
        cnt = len(re.findall(r"\bJOIN\b", plan_text, flags=re.I))
    return cnt


def explain_with_duckdb(sql: str, db: Path, threads: int, timeout_s: int) -> tuple[int, str, str, bool]:
    cmd = ["duckdb", str(db)]
    query = f"PRAGMA threads={threads}; EXPLAIN {sql}"
    try:
        p = subprocess.run(
            cmd,
            input=query,
            text=True,
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
        return p.returncode, p.stdout or "", p.stderr or "", False
    except subprocess.TimeoutExpired as e:
        return 124, (e.stdout or "") if isinstance(e.stdout, str) else "", (
            (e.stderr or "") if isinstance(e.stderr, str) else ""
        ), True


def explain_with_cedardb(
    sql: str,
    host: str,
    port: str,
    user: str,
    database: str,
    timeout_s: int,
    password_env: str,
) -> tuple[int, str, str, bool]:
    cmd = [
        "psql",
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-h",
        host,
        "-p",
        port,
        "-U",
        user,
        "-d",
        database,
        "-f",
        "-",
    ]
    body = f"SET statement_timeout='{timeout_s}s';\nEXPLAIN {sql}\n"
    env = os.environ.copy()
    if password_env:
        env_val = os.environ.get(password_env)
        if env_val:
            env["PGPASSWORD"] = env_val
    try:
        p = subprocess.run(
            cmd,
            input=body,
            text=True,
            capture_output=True,
            timeout=timeout_s + 30,
            check=False,
            env=env,
        )
        return p.returncode, p.stdout or "", p.stderr or "", False
    except subprocess.TimeoutExpired as e:
        return 124, (e.stdout or "") if isinstance(e.stdout, str) else "", (
            (e.stderr or "") if isinstance(e.stderr, str) else ""
        ), True


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate effective join counts with EXPLAIN (DuckDB/CedarDB).")
    ap.add_argument("--join-dir", default=None)
    ap.add_argument("--duckdb-db", default=None)
    ap.add_argument("--engines", default="duckdb", help="Comma list: duckdb,cedardb")
    ap.add_argument("--levels", default="", help="Optional comma list filter for join levels, e.g. 16,64,256,1024")
    ap.add_argument("--cedar-host", default="127.0.0.1")
    ap.add_argument("--cedar-port", default="5433")
    ap.add_argument("--cedar-user", default="postgres")
    ap.add_argument("--cedar-db", default="prodds_sf10_str10")
    ap.add_argument(
        "--cedar-password-env",
        default="PGPASSWORD",
        help="Environment variable name holding CedarDB password. Empty to skip.",
    )
    ap.add_argument("--out-csv", default="join_validation.csv")
    ap.add_argument("--tolerance", type=float, default=0.20, help="Relative tolerance against target J.")
    ap.add_argument("--threads", type=int, default=56)
    ap.add_argument("--timeout-s", type=int, default=600)
    ap.add_argument("--fail-on-violation", action="store_true")
    args = ap.parse_args()

    join_dir_raw = args.join_dir or os.environ.get(
        "PRODDS_JOIN_DIR", str(REPO_ROOT / "queries" / "prodds" / "generators" / "join")
    )
    duckdb_db_raw = args.duckdb_db or os.environ.get(
        "PRODDS_DUCKDB_DB", str(REPO_ROOT / "data" / "duckdb" / "prodds_sf10_str10.duckdb")
    )
    join_dir = Path(join_dir_raw).resolve()
    duckdb_db = Path(duckdb_db_raw).resolve()
    out_csv = Path(args.out_csv).resolve()
    engines = [e.strip().lower() for e in str(args.engines).split(",") if e.strip()]
    allowed = {"duckdb", "cedardb"}
    unknown = [e for e in engines if e not in allowed]
    if unknown:
        raise SystemExit(f"unsupported engine(s): {unknown}; allowed={sorted(allowed)}")
    if not engines:
        raise SystemExit("no engines selected")

    if not join_dir.exists():
        raise SystemExit(f"missing join dir: {join_dir}")
    if "duckdb" in engines and not duckdb_db.exists():
        raise SystemExit(f"missing duckdb db: {duckdb_db}")

    files = list_join_files(join_dir)
    if not files:
        raise SystemExit(f"no join_*.sql files in {join_dir}")
    level_filter = parse_levels(args.levels) if args.levels.strip() else set()
    if level_filter:
        files = [x for x in files if x[0] in level_filter]
        if not files:
            raise SystemExit(f"no join files matched --levels={args.levels}")

    rows: list[dict[str, str]] = []
    violations = 0

    for engine in engines:
        for target_j, path in files:
            sql = path.read_text(encoding="utf-8")
            header = parse_header(sql)
            strategy = ""
            k = ""
            m = ""
            expected_effective = ""
            if header is not None:
                strategy = header[0]
                k = str(header[1])
                m = str(header[2])
                expected_effective = str(header[3])

            sql_join_cnt = sql_join_keyword_count(sql)
            if engine == "duckdb":
                rc, out, err, timed_out = explain_with_duckdb(sql, duckdb_db, args.threads, args.timeout_s)
            else:
                rc, out, err, timed_out = explain_with_cedardb(
                    sql,
                    host=str(args.cedar_host),
                    port=str(args.cedar_port),
                    user=str(args.cedar_user),
                    database=str(args.cedar_db),
                    timeout_s=args.timeout_s,
                    password_env=str(args.cedar_password_env),
                )
            explain_ok = (rc == 0) and (not timed_out)
            plan_join_cnt = ""
            delta_target_vs_plan = ""
            delta_expected_vs_plan = ""
            pass_fail = "FAIL"
            note = ""

            if explain_ok:
                pj = count_plan_joins(out) if engine == "duckdb" else count_cedardb_plan_joins(out)
                plan_join_cnt = str(pj)
                d_target = target_j - pj
                delta_target_vs_plan = str(d_target)
                if expected_effective:
                    d_expected = int(expected_effective) - pj
                    delta_expected_vs_plan = str(d_expected)

                tol_abs = max(1.0, args.tolerance * float(target_j))
                ok = abs(float(d_target)) <= tol_abs
                pass_fail = "PASS" if ok else "FAIL"
                if not ok:
                    violations += 1
                    note = f"outside tolerance ±{tol_abs:.2f} around target"
            else:
                violations += 1
                if timed_out:
                    note = "EXPLAIN timeout"
                else:
                    msg = (err or out).strip().replace("\n", " ")
                    note = f"EXPLAIN failed: {msg[:240]}"

            rows.append(
                {
                    "engine": engine,
                    "query": path.name,
                    "target_j": str(target_j),
                    "strategy": strategy,
                    "k": k,
                    "m": m,
                    "expected_effective_joins": expected_effective,
                    "sql_join_keyword_count": str(sql_join_cnt),
                    "plan_join_count": plan_join_cnt,
                    "delta_target_vs_plan": delta_target_vs_plan,
                    "delta_expected_vs_plan": delta_expected_vs_plan,
                    "explain_ok": "true" if explain_ok else "false",
                    "pass_fail": pass_fail,
                    "notes": note,
                    "sql_path": str(path),
                }
            )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"wrote {len(rows)} rows to {out_csv}")
    if violations:
        print(f"violations={violations}")
    return 1 if (args.fail_on_violation and violations > 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())

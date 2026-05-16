#!/usr/bin/env bash
set -euo pipefail

print_block() {
    local title="$1"
    local body="$2"
    echo "  - ${title}:"
    if [[ -n "${body}" ]]; then
        while IFS= read -r line; do
            [[ -n "${line}" ]] && echo "      ${line}"
        done <<< "${body}"
    else
        echo "      (none)"
    fi
}

pg_lines() {
    local pattern="$1"
    pgrep -fa -- "${pattern}" || true
}

pg_lines_filtered() {
    local pattern="$1"
    local keep_regex="$2"
    pg_lines "${pattern}" | grep -E -- "${keep_regex}" || true
}

port_lines() {
    local port="$1"
    ss -ltnpH 2>/dev/null | grep -E "[:.]${port}[[:space:]]" || true
}

postgres_pgrep="$(
    pg_lines_filtered 'postgres' '/postgres([[:space:]]|$)|postgres:[[:space:]]'
)"
cedardb_pgrep="$(
    pg_lines_filtered 'cedardb' '/cedardb([[:space:]]|$)|[[:space:]]cedardb([[:space:]]|$)'
)"
monetdb_pgrep="$(
    pg_lines_filtered 'monetdbd|mserver5' '/monetdbd([[:space:]]|$)|/mserver5([-.0-9]+)?([[:space:]]|$)'
)"
duckdb_pgrep="$(
    pg_lines_filtered 'duckdb' '/duckdb([[:space:]]|$)|[[:space:]]duckdb([[:space:]]|$)'
)"

postgres_ss="$(port_lines 5432)"
cedardb_ss="$(port_lines 5433)"
monetdb_ss="$(port_lines 50000)"

active_engines=()
if [[ -n "${postgres_pgrep}" || -n "${postgres_ss}" ]]; then
    active_engines+=("postgres")
fi
if [[ -n "${cedardb_pgrep}" || -n "${cedardb_ss}" ]]; then
    active_engines+=("cedardb")
fi
if [[ -n "${monetdb_pgrep}" || -n "${monetdb_ss}" ]]; then
    active_engines+=("monetdb")
fi

echo "active_engines=[${active_engines[*]}]"
echo "evidence:"
print_block "postgres pgrep" "${postgres_pgrep}"
print_block "postgres ss(:5432)" "${postgres_ss}"
print_block "cedardb pgrep" "${cedardb_pgrep}"
print_block "cedardb ss(:5433)" "${cedardb_ss}"
print_block "monetdb pgrep" "${monetdb_pgrep}"
print_block "monetdb ss(:50000)" "${monetdb_ss}"
print_block "duckdb pgrep (non-blocking)" "${duckdb_pgrep}"

if [[ -n "${duckdb_pgrep}" ]]; then
    echo "WARNING: duckdb CLI/process detected; non-blocking but review before timed runs." >&2
fi

if (( ${#active_engines[@]} > 1 )); then
    echo "ERROR: preflight_one_engine failed: multiple benchmark engines are active: ${active_engines[*]}" >&2
    exit 1
fi

echo "OK: preflight_one_engine passed."

#!/usr/bin/env bash
# examples/quickstart/run.sh — End-to-end Prod-DS Kit quickstart on SF1 with DuckDB.
#
# Generates data and queries for both STR=1 (vanilla TPC-DS) and STR=10
# (full Prod-DS), loads into DuckDB, runs a subset of queries, and prints
# a timing comparison.
#
# Prerequisites:
#   - ./install.sh completed
#   - DuckDB CLI on PATH
#   - Virtual environment active (source .venv/bin/activate)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

DATA_STR1="$REPO_ROOT/data/quickstart_sf1_str1"
DATA_STR10="$REPO_ROOT/data/quickstart_sf1_str10"
QUERIES_STR1="$REPO_ROOT/queries/quickstart_sf1_str1"
QUERIES_STR10="$REPO_ROOT/queries/quickstart_sf1_str10"
DB_STR1="$REPO_ROOT/data/quickstart_sf1_str1.duckdb"
DB_STR10="$REPO_ROOT/data/quickstart_sf1_str10.duckdb"
SCHEMA_GEN="$REPO_ROOT/tools/generate_tpcds_schema.py"

# Subset of queries to run (fast, representative)
QUERY_SUBSET=(3 7 19 25 42 52 55 73 79 96)

info()  { printf '\033[1;34m[quickstart]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[quickstart]\033[0m %s\n' "$*"; }

# Check prerequisites
command -v duckdb >/dev/null 2>&1 || { echo "DuckDB CLI not found. Install it first." >&2; exit 1; }
[ -f "$REPO_ROOT/tpcds-kit/tools/dsdgen" ] || { echo "TPC-DS toolkit not built. Run ./install.sh first." >&2; exit 1; }

# ---------- Step 1: Generate STR=1 data ----------
if [ ! -d "$DATA_STR1" ]; then
    info "Generating SF1 data at STR=1 (vanilla TPC-DS)..."
    python3 wrap_dsdgen.py --stringification-level 1 -DIR "$DATA_STR1" -SCALE 1
    ok "STR=1 data generated."
else
    info "STR=1 data already exists, skipping."
fi

# ---------- Step 2: Generate STR=10 data ----------
if [ ! -d "$DATA_STR10" ]; then
    info "Generating SF1 data at STR=10 (full Prod-DS)..."
    python3 wrap_dsdgen.py --stringification-level 10 \
        --null-profile medium --mcv-profile medium \
        -DIR "$DATA_STR10" -SCALE 1
    ok "STR=10 data generated."
else
    info "STR=10 data already exists, skipping."
fi

# ---------- Step 3: Generate queries ----------
if [ ! -d "$QUERIES_STR1" ]; then
    info "Generating vanilla TPC-DS queries..."
    python3 wrap_dsqgen.py --output-dir "$QUERIES_STR1" --no-extensions
    ok "STR=1 queries generated."
else
    info "STR=1 queries already exist, skipping."
fi

if [ ! -d "$QUERIES_STR10" ]; then
    info "Generating Prod-DS queries (STR=10)..."
    python3 wrap_dsqgen.py --output-dir "$QUERIES_STR10" --stringification-level 10
    ok "STR=10 queries generated."
else
    info "STR=10 queries already exist, skipping."
fi

# ---------- Step 4: Load into DuckDB ----------
load_duckdb() {
    local db_path="$1" data_dir="$2" str_level="$3"
    if [ -f "$db_path" ]; then
        info "Database $db_path already exists, skipping load."
        return
    fi
    info "Loading data into $db_path (STR=$str_level)..."

    # Generate schema
    local schema_sql
    schema_sql=$(python3 "$SCHEMA_GEN" --level "$str_level")
    duckdb "$db_path" -c "$schema_sql"

    # Load each data file
    for file in "$data_dir"/*.dat; do
        [ -f "$file" ] || continue
        local table
        table="$(basename "${file%.dat}")"
        iconv -f ISO-8859-1 -t UTF-8 "$file" | \
            duckdb "$db_path" -c "COPY $table FROM '/dev/stdin' (DELIMITER '|', HEADER false, NULL '', AUTO_DETECT false);"
    done
    duckdb "$db_path" -c "ANALYZE;"
    ok "Loaded $db_path"
}

load_duckdb "$DB_STR1"  "$DATA_STR1"  1
load_duckdb "$DB_STR10" "$DATA_STR10" 10

# ---------- Step 5: Run queries and compare ----------
info "Running ${#QUERY_SUBSET[@]} queries on each configuration..."

printf '\n%-10s %12s %12s %12s\n' "Query" "STR=1 (ms)" "STR=10 (ms)" "Slowdown"
printf '%-10s %12s %12s %12s\n' "-----" "----------" "-----------" "--------"

for q in "${QUERY_SUBSET[@]}"; do
    qfile_str1="$QUERIES_STR1/query_${q}.sql"
    qfile_str10="$QUERIES_STR10/query_${q}.sql"

    if [ ! -f "$qfile_str1" ] || [ ! -f "$qfile_str10" ]; then
        printf '%-10s %12s %12s %12s\n' "Q${q}" "MISSING" "MISSING" "-"
        continue
    fi

    # Run STR=1
    t1=$(duckdb "$DB_STR1" -csv -c ".timer on" < "$qfile_str1" 2>&1 | grep -i "^Run Time" | tail -1 | sed 's/[^0-9.]//g' || echo "0")
    # Run STR=10
    t10=$(duckdb "$DB_STR10" -csv -c ".timer on" < "$qfile_str10" 2>&1 | grep -i "^Run Time" | tail -1 | sed 's/[^0-9.]//g' || echo "0")

    # Compute slowdown
    if [ -n "$t1" ] && [ -n "$t10" ] && [ "$t1" != "0" ]; then
        slowdown=$(python3 -c "print(f'{float(\"${t10}\")/float(\"${t1}\"):.2f}x')" 2>/dev/null || echo "-")
    else
        slowdown="-"
    fi

    # Convert to ms
    t1_ms=$(python3 -c "print(f'{float(\"${t1}\")*1000:.1f}')" 2>/dev/null || echo "$t1")
    t10_ms=$(python3 -c "print(f'{float(\"${t10}\")*1000:.1f}')" 2>/dev/null || echo "$t10")

    printf '%-10s %12s %12s %12s\n' "Q${q}" "$t1_ms" "$t10_ms" "$slowdown"
done

echo ""
ok "Quickstart complete. See examples/quickstart/README.md for details."

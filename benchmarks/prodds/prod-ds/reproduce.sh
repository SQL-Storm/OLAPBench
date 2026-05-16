#!/usr/bin/env bash
# reproduce.sh — VLDB 2027 Reproducibility Script for Prod-DS Kit
#
# Reproduces experiments E1–E5 from the paper:
#   "Scaling Analytical Benchmarks to Production Complexity"
#
# Usage:
#   ./reproduce.sh --init [--sf N] [--engines LIST]
#   ./reproduce.sh --experiment E1 [--sf N] [--engines LIST]
#   ./reproduce.sh --all [--sf N] [--engines LIST]
#   ./reproduce.sh --plots
#   ./reproduce.sh --help
#
# Experiments:
#   E1  End-to-end TPC-DS vs Prod-DS (Section 6.5)
#   E2  Join-scaling micro-suite    (Section 6.6)
#   E3  UNION ALL fan-in scaling    (Section 6.7)
#   E4  Stringification sweep       (Section 6.8, DuckDB only)
#   E5  Sparsity & skew sensitivity (Section 6.9, DuckDB + CedarDB)
#
# Examples:
#   ./reproduce.sh --init --sf 1                          # Quick test
#   ./reproduce.sh --init --sf 10 --engines duckdb        # Reviewer run
#   ./reproduce.sh --experiment E1 --sf 10 --engines all  # Full E1
#   ./reproduce.sh --all --sf 100 --reps 10               # Full reproduction
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$ROOT_DIR/.reproduce"
VENV_DIR="$ROOT_DIR/.venv"

SF=1
ENGINES="duckdb"
EXPERIMENTS=""
INIT=false
PLOTS_ONLY=false
REPS=3
WARMUP=1
TIMEOUT=1800
THREADS=""  # auto-detect
E4_LEVELS="${E4_LEVELS:-$(seq 1 15)}"  # STR levels for E4 (override to e.g. "1 5 10 15")

# Engine versions (matching paper Section 6.1–6.2)
DUCKDB_VERSION="${DUCKDB_VERSION:-1.4.4}"

# ──────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────
info()  { printf '\033[1;34m[reproduce]\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m[reproduce]\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33m[reproduce]\033[0m %s\n' "$*"; }
fail()  { printf '\033[1;31m[reproduce]\033[0m %s\n' "$*" >&2; exit 1; }
step()  { printf '\n\033[1;36m═══ %s ═══\033[0m\n' "$*"; }

# ──────────────────────────────────────────────────────────────
# CLI Parsing
# ──────────────────────────────────────────────────────────────
usage() {
    sed -n '2,/^set -euo/p' "$0" | grep '^#' | sed 's/^# \?//'
    exit 0
}

parse_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --init)       INIT=true ;;
            --experiment) shift; EXPERIMENTS="$EXPERIMENTS $1" ;;
            --all)        EXPERIMENTS="E1 E2 E3 E4 E5" ;;
            --sf)         shift; SF="$1" ;;
            --engines)    shift; ENGINES="$1" ;;
            --reps)       shift; REPS="$1" ;;
            --warmup)     shift; WARMUP="$1" ;;
            --timeout)    shift; TIMEOUT="$1" ;;
            --threads)    shift; THREADS="$1" ;;
            --plots)      PLOTS_ONLY=true ;;
            --help|-h)    usage ;;
            *) fail "Unknown argument: $1" ;;
        esac
        shift
    done

    if [ "$ENGINES" = "all" ]; then
        ENGINES="duckdb,cedardb,monetdb"
    fi

    if [ -z "$THREADS" ]; then
        THREADS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
    fi

    # Validate experiment names
    for e in $EXPERIMENTS; do
        case "$e" in
            E1|E2|E3|E4|E5) ;;
            *) fail "Unknown experiment: $e (valid: E1 E2 E3 E4 E5)" ;;
        esac
    done

    if ! $INIT && [ -z "$EXPERIMENTS" ] && ! $PLOTS_ONLY; then
        echo "No action specified. Use --init, --experiment, --all, or --plots."
        echo "Run with --help for usage."
        exit 1
    fi
}

# ──────────────────────────────────────────────────────────────
# Path helpers
# ──────────────────────────────────────────────────────────────
data_dir()    { echo "$WORK_DIR/data/$1"; }
queries_dir() { echo "$WORK_DIR/queries/$1"; }
db_dir()      { echo "$WORK_DIR/databases"; }
engines_dir() { echo "$WORK_DIR/engines"; }
configs_dir() { echo "$WORK_DIR/configs"; }
results_dir() { echo "$WORK_DIR/results"; }

activate_venv() {
    if [ -f "$VENV_DIR/bin/activate" ]; then
        # shellcheck disable=SC1091
        source "$VENV_DIR/bin/activate"
    else
        fail "Virtual environment not found. Run --init first."
    fi
}

duckdb_bin() {
    local engines
    engines="$(engines_dir)"
    if [ -x "$engines/duckdb/duckdb" ]; then
        echo "$engines/duckdb/duckdb"
    elif command -v duckdb >/dev/null 2>&1; then
        command -v duckdb
    else
        fail "DuckDB not found. Run --init first."
    fi
}

cedardb_bin() {
    local engines
    engines="$(engines_dir)"
    if [ -x "$engines/cedardb/cedardb" ]; then
        echo "$engines/cedardb/cedardb"
    elif command -v cedardb >/dev/null 2>&1; then
        command -v cedardb
    else
        fail "CedarDB not found. Run --init first."
    fi
}

engine_dialect() {
    case "$1" in
        duckdb)  echo "duckdb" ;;
        cedardb) echo "postgres" ;;
        monetdb) echo "postgres" ;;
        *)       echo "ansi" ;;
    esac
}

engine_available() {
    case "$1" in
        duckdb)  duckdb_bin >/dev/null 2>&1 ;;
        cedardb) cedardb_bin >/dev/null 2>&1 ;;
        monetdb) command -v mclient >/dev/null 2>&1 ;;
        *)       return 1 ;;
    esac
}

# ──────────────────────────────────────────────────────────────
# Prerequisites
# ──────────────────────────────────────────────────────────────
check_prerequisites() {
    step "Checking prerequisites"

    # Python >= 3.9
    local python=""
    for candidate in python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            local ver
            ver=$("$candidate" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
            local major=${ver%%.*}
            local minor=${ver#*.}
            if [ "$major" -ge 3 ] && [ "$minor" -ge 9 ]; then
                python="$candidate"
                break
            fi
        fi
    done
    [ -n "$python" ] || fail "Python >= 3.9 required."
    ok "Python: $python ($ver)"

    # curl
    command -v curl >/dev/null 2>&1 || fail "curl is required."
    ok "curl: $(command -v curl)"

    # unzip
    command -v unzip >/dev/null 2>&1 || fail "unzip is required."
    ok "unzip: $(command -v unzip)"

    # git
    command -v git >/dev/null 2>&1 || fail "git is required."
    ok "git: $(command -v git)"
}

# ──────────────────────────────────────────────────────────────
# Install Toolkit (TPC-DS + Prod-DS Python package)
# ──────────────────────────────────────────────────────────────
install_toolkit() {
    step "Installing Prod-DS toolkit"
    cd "$ROOT_DIR"
    bash install.sh
    ok "Toolkit installed."
}

# ──────────────────────────────────────────────────────────────
# Install Engines
# ──────────────────────────────────────────────────────────────
install_duckdb() {
    local dest
    dest="$(engines_dir)/duckdb"
    if [ -x "$dest/duckdb" ]; then
        ok "DuckDB already installed at $dest/duckdb"
        return
    fi

    info "Installing DuckDB v${DUCKDB_VERSION}..."
    mkdir -p "$dest"

    local arch
    arch="$(uname -m)"
    case "$arch" in
        x86_64)          arch="amd64" ;;
        aarch64|arm64)   arch="aarch64" ;;
        *) fail "Unsupported architecture: $arch" ;;
    esac

    local url="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${arch}.zip"
    curl -fSL -o "$dest/duckdb.zip" "$url"
    unzip -o "$dest/duckdb.zip" -d "$dest"
    chmod +x "$dest/duckdb"
    rm -f "$dest/duckdb.zip"
    ok "DuckDB v${DUCKDB_VERSION} installed at $dest/duckdb"
}

install_cedardb() {
    local dest
    dest="$(engines_dir)/cedardb"
    if [ -x "$dest/cedardb" ]; then
        ok "CedarDB already installed at $dest/cedardb"
        return
    fi

    info "Installing CedarDB..."
    mkdir -p "$dest"
    cd "$dest"
    if curl -sSL https://get.cedardb.com | bash; then
        chmod +x cedardb 2>/dev/null || true
        ok "CedarDB installed at $dest/cedardb"
    else
        warn "CedarDB installation failed. CedarDB experiments will be skipped."
        warn "Install manually: https://cedardb.com/docs/getting-started/"
    fi
    cd "$ROOT_DIR"
}

install_monetdb() {
    if command -v mserver5 >/dev/null 2>&1; then
        ok "MonetDB already available: $(command -v mserver5)"
        return
    fi

    info "Installing MonetDB..."
    if command -v apt-get >/dev/null 2>&1; then
        info "Installing MonetDB from packages (requires sudo)..."
        sudo apt-get update -qq
        sudo apt-get install -y -qq monetdb5-sql monetdb-client 2>/dev/null || {
            warn "MonetDB package installation failed."
            warn "Install manually: https://www.monetdb.org/easy-setup/"
            warn "MonetDB experiments will be skipped."
            return
        }
        ok "MonetDB installed from packages."
    else
        warn "Non-Debian system detected. Install MonetDB manually."
        warn "See: https://www.monetdb.org/easy-setup/"
    fi
}

install_engines() {
    step "Installing database engines"
    mkdir -p "$(engines_dir)"

    IFS=',' read -ra engine_list <<< "$ENGINES"
    for engine in "${engine_list[@]}"; do
        case "$engine" in
            duckdb)  install_duckdb ;;
            cedardb) install_cedardb ;;
            monetdb) install_monetdb ;;
            *) warn "Unknown engine: $engine" ;;
        esac
    done
}

# ──────────────────────────────────────────────────────────────
# Data Generation
# ──────────────────────────────────────────────────────────────
generate_data_variant() {
    # Usage: generate_data_variant <output_dir> <str_level> [--disable-null-skew] [--disable-mcv-skew]
    local out_dir="$1"
    local str_level="$2"
    shift 2
    local extra_flags=("$@")

    if [ -d "$out_dir" ] && ls "$out_dir"/*.dat 1>/dev/null 2>&1; then
        info "Data already exists at $out_dir — skipping."
        return
    fi

    mkdir -p "$out_dir"
    info "Generating data: STR=$str_level SF=$SF → $out_dir"

    local cmd=(python3 "$ROOT_DIR/wrap_dsdgen.py"
        --stringification-level "$str_level"
        --min-ndv-for-injection 0
        -SCALE "$SF"
        -DIR "$out_dir"
    )
    cmd+=("${extra_flags[@]}")

    "${cmd[@]}"
    ok "Data generated at $out_dir"
}

generate_all_data() {
    step "Generating data (SF=$SF)"
    activate_venv

    # TPC-DS vanilla (used by E1)
    generate_data_variant "$(data_dir tpcds_sf${SF})" 1 --disable-null-skew --disable-mcv-skew

    # Prod-DS default STR=10 (used by E1, E2, E3)
    generate_data_variant "$(data_dir prodds_sf${SF}_str10)" 10

    # E4: Stringification sweep
    # Always generate during --init so that a later --all run has data ready.
    if $INIT || echo "$EXPERIMENTS" | grep -qw "E4"; then
        for str in $E4_LEVELS; do
            generate_data_variant "$(data_dir str_sweep/str${str})" "$str"
        done
    fi

    # E5: Sparsity & skew variants
    if $INIT || echo "$EXPERIMENTS" | grep -qw "E5"; then
        generate_data_variant "$(data_dir sparsity/baseline)" 10 --disable-null-skew --disable-mcv-skew
        generate_data_variant "$(data_dir sparsity/sparsity_only)" 10 --disable-mcv-skew
        generate_data_variant "$(data_dir sparsity/skew_only)" 10 --disable-null-skew
        # Combined is the same as default Prod-DS
        if [ ! -d "$(data_dir sparsity/combined)" ]; then
            ln -sfn "$(data_dir prodds_sf${SF}_str10)" "$(data_dir sparsity/combined)"
        fi
    fi
}

# ──────────────────────────────────────────────────────────────
# Query Generation
# ──────────────────────────────────────────────────────────────
generate_queries_variant() {
    # Usage: generate_queries_variant <output_dir> <str_level> [extra_flags...]
    local out_dir="$1"
    local str_level="$2"
    shift 2
    local extra_flags=("$@")

    if [ -d "$out_dir" ] && ls "$out_dir"/query_*.sql 1>/dev/null 2>&1; then
        info "Queries already exist at $out_dir — skipping."
        return
    fi

    mkdir -p "$out_dir"
    info "Generating queries: STR=$str_level → $out_dir"

    local cmd=(python3 "$ROOT_DIR/wrap_dsqgen.py"
        --output-dir "$out_dir"
        --stringification-level "$str_level"
        --scale "$SF"
    )
    cmd+=("${extra_flags[@]}")

    "${cmd[@]}"
    ok "Queries generated at $out_dir"
}

generate_all_queries() {
    step "Generating queries (SF=$SF)"
    activate_venv

    # Per-engine query generation with appropriate dialect
    IFS=',' read -ra engine_list <<< "$ENGINES"
    for engine in "${engine_list[@]}"; do
        local dialect
        dialect=$(engine_dialect "$engine")
        info "Generating queries for $engine (dialect=$dialect)..."

        # TPC-DS vanilla queries (no extensions)
        generate_queries_variant "$(queries_dir ${engine}/tpcds)" 1 --no-extensions --dialect "$dialect"

        # Prod-DS extended queries (STR=10)
        generate_queries_variant "$(queries_dir ${engine}/prodds)" 10 --dialect "$dialect"
    done

    # Join-scaling micro-suite (E2) — via wrap_dsqgen.py --join-only
    if echo "$EXPERIMENTS" | grep -qw "E2"; then
        local join_dir
        join_dir="$(queries_dir join_scaling)"
        if [ ! -d "$join_dir" ] || [ -z "$(ls -A "$join_dir" 2>/dev/null)" ]; then
            mkdir -p "$join_dir"
            info "Generating join-scaling queries (J=16..2048)..."
            python3 "$ROOT_DIR/wrap_dsqgen.py" \
                --output-dir "$join_dir" \
                --join-only \
                --join-targets "16,32,64,128,256,512,1024,2048" \
                --scale "$SF" \
                --stringification-level 10
            ok "Join-scaling queries generated."
        fi
    fi

    # Union-scaling micro-suite (E3) — via wrap_dsqgen.py --union
    if echo "$EXPERIMENTS" | grep -qw "E3"; then
        local union_dir
        union_dir="$(queries_dir union_scaling)"
        if [ ! -d "$union_dir" ] || [ -z "$(ls -A "$union_dir" 2>/dev/null)" ]; then
            mkdir -p "$union_dir"
            info "Generating union-scaling queries (U=2..2048)..."
            python3 "$ROOT_DIR/wrap_dsqgen.py" \
                --output-dir "$union_dir" \
                --union \
                --union-max-inputs 2048 \
                --no-join \
                --scale "$SF" \
                --stringification-level 10 \
                --dialect duckdb
            # Remove base Prod-DS queries — E3 should only contain union micro-suite
            local before_count after_count
            before_count=$(ls "$union_dir"/query_*.sql 2>/dev/null | wc -l)
            find "$union_dir" -name "query_*.sql" ! -name "query_union_*.sql" -delete
            after_count=$(ls "$union_dir"/query_*.sql 2>/dev/null | wc -l)
            info "Union-scaling: kept $after_count union queries (removed $((before_count - after_count)) base queries)"
            ok "Union-scaling queries generated."
        fi
        # MonetDB needs CTE-inlined union queries (optimizer bug with CTE+JOIN+UNION ALL)
        if echo "$ENGINES" | grep -q "monetdb"; then
            local monetdb_union_dir
            monetdb_union_dir="$(queries_dir monetdb/union_scaling)"
            if [ ! -d "$monetdb_union_dir" ] || [ -z "$(ls -A "$monetdb_union_dir" 2>/dev/null)" ]; then
                info "Generating MonetDB union-scaling variants (CTE inlined)..."
                monetdb_inline_union_ctes "$union_dir" "$monetdb_union_dir"
                ok "MonetDB union-scaling variants generated."
            fi
        fi
    fi

    # E4: Stringification sweep queries (DuckDB only per paper)
    if echo "$EXPERIMENTS" | grep -qw "E4"; then
        for str in $E4_LEVELS; do
            generate_queries_variant "$(queries_dir duckdb/str_sweep/str${str})" "$str" --dialect duckdb
        done
    fi

    # Apply dialect fixes to all generated queries
    apply_all_dialect_fixes
}

# ──────────────────────────────────────────────────────────────
# Dialect Fixes (post-processing + structural overlays)
# ──────────────────────────────────────────────────────────────

# Rewrite union-scaling queries for MonetDB by inlining the base CTE join
# into each UNION branch. MonetDB Dec2025-SP1 has an optimizer bug where
# CTE + JOIN + UNION ALL triggers "unexpected end of file".
monetdb_inline_union_ctes() {
    local src_dir="$1"
    local dst_dir="$2"
    mkdir -p "$dst_dir"
    python3 -c "
import re, sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
for f in sorted(src.glob('query_union_*.sql')):
    text = f.read_text()
    # Extract base CTE: columns, tables, where clause
    m = re.search(
        r'WITH\s+base\s+AS\s*\(\s*SELECT\s+(.*?)\s+FROM\s+(.*?)\s+WHERE\s+(.*?)\s*\)',
        text, re.DOTALL | re.IGNORECASE)
    if not m:
        # Not a union CTE query, copy as-is
        (dst / f.name).write_text(text)
        continue
    cols, tables, base_where = m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
    # Extract branches: union_branch number + filter
    branches = re.findall(
        r'u\d+\s+AS\s*\(\s*SELECT\s+.*?,\s*(\d+)\s+AS\s+union_branch\s+FROM\s+base\s+WHERE\s+(.*?)\s*\)',
        text, re.DOTALL | re.IGNORECASE)
    parts = []
    for branch_num, filt in branches:
        parts.append(
            f'SELECT {cols}, {branch_num} AS union_branch\n'
            f'FROM {tables}\n'
            f'WHERE {base_where} AND {filt.strip()}')
    sql = '\nUNION ALL\n'.join(parts) + ';\n'
    (dst / f.name).write_text(sql)
    print(f'  {f.name}: {len(branches)} branches inlined')
# Only copy union queries (E3 micro-suite should not include base queries)

" "$src_dir" "$dst_dir"
}

# Apply regex-based dialect fixes to generated queries.
# Handles common SQL incompatibilities per engine without changing
# parameter values. For structural rewrites that cannot be automated,
# static overlay files are used from experiments/queries/dialect_variants/.
apply_dialect_fixes() {
    local engine="$1"
    local query_dir="$2"
    local suite="$3"  # tpcds or prodds

    [ -d "$query_dir" ] || return 0

    local fix_count=0

    # ── Phase 1: Apply variant overlays FIRST ──
    # Structural rewrites (reserved words, ambiguous
    # column refs, LIMIT placement, etc.) that cannot be handled by regex.
    # Variant files are named by TEMPLATE number (query_58.sql = fix for
    # template 58). With STREAMS ordering, template 58 may appear at a
    # different query position (e.g., query_19.sql). The _permutation.json
    # file maps template_num → query_position for correct overlay.
    local overlay_count=0
    local variant_dir="$ROOT_DIR/experiments/queries/dialect_variants/$engine/$suite"
    if [ -d "$variant_dir" ]; then
        local perm_file="$query_dir/_permutation.json"
        for variant in "$variant_dir"/query_*.sql; do
            [ -f "$variant" ] || continue

            # Safety: skip TPC-DS variants that contain Prod-DS artifacts.
            # These are broken files that were accidentally generated from
            # stringified Prod-DS queries instead of vanilla TPC-DS queries.
            # Patterns: stringified IDs ('MFG_...', 'i00000...'), Prod-DS columns,
            # regexp_replace wrappers around surrogate keys.
            if [ "$suite" = "tpcds" ]; then
                if grep -qiE "'(MFG|MGR|MKT|CLR|SIZ|BRD|CAT|CLS|DIV)_[0-9]|'[a-z][0-9]{4,}" "$variant" 2>/dev/null || \
                   grep -q "c_last_review_date[^_]" "$variant" 2>/dev/null || \
                   grep -q "regexp_replace.*'g'" "$variant" 2>/dev/null; then
                    warn "Skipping contaminated TPC-DS variant: $(basename "$variant")"
                    continue
                fi
            fi

            local vname
            vname=$(basename "$variant")
            # Extract template number from variant filename (e.g., query_58.sql → 58)
            local tpl_num
            tpl_num=$(echo "$vname" | sed 's/query_\([0-9]*\)\.sql/\1/')

            # Map template number to query position via permutation.
            # For non-standard names (query_union_U2.sql, query_join_J50.sql),
            # tpl_num won't be numeric — skip permutation and use filename as-is.
            local target_name="$vname"
            if [ -f "$perm_file" ] && [[ "$tpl_num" =~ ^[0-9]+$ ]]; then
                local pos
                pos=$(python3 -c "import json; p=json.load(open('$perm_file')); print(p.get('$tpl_num', p.get(int('$tpl_num'), '')))" 2>/dev/null || true)
                if [ -n "$pos" ] && [ "$pos" != "" ]; then
                    target_name="query_${pos}.sql"
                fi
            fi

            if [ -f "$query_dir/$target_name" ]; then
                cp "$variant" "$query_dir/$target_name"
                overlay_count=$((overlay_count + 1))
            fi
        done
    fi

    # ── Phase 2: Regex fixes AFTER overlays ──
    # These run on all query files including overlaid ones, ensuring
    # overlay files that contain any_value(), materialized hints, or
    # PostgreSQL-style intervals are also fixed.

    # Ensure all queries end with a semicolon (required by MonetDB mclient,
    # harmless for other engines)
    for f in "$query_dir"/query_*.sql; do
        [ -f "$f" ] || continue
        if ! tail -c 20 "$f" | grep -q ';[[:space:]]*$'; then
            printf '\n;\n' >> "$f"
            fix_count=$((fix_count + 1))
        fi
    done

    case "$engine" in
        monetdb)
            # MonetDB does not support CTE materialization hints
            for f in "$query_dir"/query_*.sql; do
                [ -f "$f" ] || continue
                if grep -qi 'as materialized' "$f" 2>/dev/null; then
                    sed -i 's/[Aa][Ss] [Mm][Aa][Tt][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Ee][Dd]/as/gI' "$f"
                    fix_count=$((fix_count + 1))
                fi
            done

            # MonetDB does not support any_value() — use max() instead
            for f in "$query_dir"/query_*.sql; do
                [ -f "$f" ] || continue
                if grep -q 'any_value(' "$f" 2>/dev/null; then
                    sed -i 's/any_value(/max(/g' "$f"
                    fix_count=$((fix_count + 1))
                fi
            done

            # MonetDB requires ISO interval syntax: interval '30' day (not '30 days')
            for f in "$query_dir"/query_*.sql; do
                [ -f "$f" ] || continue
                if grep -qE "interval '[0-9]+ days?" "$f" 2>/dev/null; then
                    sed -i -E "s/interval '([0-9]+) days?'/interval '\1' day/g" "$f"
                    fix_count=$((fix_count + 1))
                fi
            done
            ;;
        cedardb)
            # CedarDB does not support CTE materialization hints
            for f in "$query_dir"/query_*.sql; do
                [ -f "$f" ] || continue
                if grep -qi 'as materialized' "$f" 2>/dev/null; then
                    sed -i 's/[Aa][Ss] [Mm][Aa][Tt][Ee][Rr][Ii][Aa][Ll][Ii][Zz][Ee][Dd]/as/gI' "$f"
                    fix_count=$((fix_count + 1))
                fi
            done
            ;;
    esac

    if [ "$fix_count" -gt 0 ] || [ "$overlay_count" -gt 0 ]; then
        info "Dialect fixes for $engine/$suite: $fix_count regex, $overlay_count structural overlays"
    fi
}

apply_all_dialect_fixes() {
    step "Applying dialect fixes"

    IFS=',' read -ra engine_list <<< "$ENGINES"
    for engine in "${engine_list[@]}"; do
        apply_dialect_fixes "$engine" "$(queries_dir ${engine}/tpcds)" "tpcds"
        apply_dialect_fixes "$engine" "$(queries_dir ${engine}/prodds)" "prodds"
    done

    ok "Dialect fixes applied."
}

# ──────────────────────────────────────────────────────────────
# Database Loading (per engine)
# ──────────────────────────────────────────────────────────────
load_duckdb_database() {
    local db_path="$1"
    local data_dir="$2"
    local str_level="${3:-10}"

    if [ -f "$db_path" ]; then
        info "DuckDB database already exists: $db_path — skipping load."
        return
    fi

    mkdir -p "$(dirname "$db_path")"
    info "Loading data into DuckDB: $db_path"

    DUCKDB_BIN="$(duckdb_bin)" \
    DUCKDB_PATH="$db_path" \
    DATA_DIR="$data_dir" \
    STR="$str_level" \
    DUCKDB_ALLOW_OVERWRITE=1 \
        bash "$ROOT_DIR/experiments/scripts/load_duckdb.sh"

    ok "DuckDB loaded: $db_path"
}

# CedarDB: state tracking
CEDARDB_PID=""
CEDARDB_DB_DIR=""

start_cedardb() {
    local db_dir="$1"
    CEDARDB_DB_DIR="$db_dir"
    mkdir -p "$db_dir"

    if [ -n "$CEDARDB_PID" ] && kill -0 "$CEDARDB_PID" 2>/dev/null; then
        info "CedarDB already running (PID=$CEDARDB_PID)."
        return
    fi

    info "Starting CedarDB..."
    local cedar_bin
    cedar_bin="$(cedardb_bin)"
    "$cedar_bin" --createdb "$db_dir" --address=127.0.0.1 --port 5433 &
    CEDARDB_PID=$!
    # Wait for ready
    for _ in $(seq 1 30); do
        if PGPASSWORD=postgres psql -X -q -h /tmp -p 5433 -U postgres -c "SELECT 1;" >/dev/null 2>&1; then
            ok "CedarDB started (PID=$CEDARDB_PID)."
            return
        fi
        sleep 1
    done
    warn "CedarDB did not become ready in 30s."
}

stop_cedardb() {
    if [ -n "$CEDARDB_PID" ] && kill -0 "$CEDARDB_PID" 2>/dev/null; then
        info "Stopping CedarDB (PID=$CEDARDB_PID)..."
        kill "$CEDARDB_PID" 2>/dev/null || true
        wait "$CEDARDB_PID" 2>/dev/null || true
        CEDARDB_PID=""
        ok "CedarDB stopped."
    fi
}

load_cedardb_database() {
    local data_dir="$1"
    local str_level="${2:-10}"
    local dbname="${3:-prodds}"

    info "Loading data into CedarDB ($dbname)..."

    DATA_DIR="$data_dir" \
    STR="$str_level" \
    CEDAR_HOST="/tmp" \
    CEDAR_PORT="5433" \
    CEDAR_DB="$dbname" \
    CEDAR_RECREATE_DB="1" \
        bash "$ROOT_DIR/experiments/scripts/load_cedardb.sh"

    ok "CedarDB loaded: $dbname"
}

# MonetDB: state tracking
MONETDB_FARM=""

start_monetdb() {
    local farm_path="$1"
    MONETDB_FARM="$farm_path"
    mkdir -p "$farm_path"

    # Check if already running via mclient probe (monetdbd status does not exist in all versions)
    if mclient -l sql -d demo -s "SELECT 1;" >/dev/null 2>&1; then
        info "MonetDB farm already running at $farm_path."
        return
    fi

    info "Starting MonetDB farm..."
    if [ ! -f "$farm_path/.merovingian_properties" ]; then
        monetdbd create "$farm_path"
    fi
    monetdbd start "$farm_path" 2>/dev/null || {
        # Might already be running
        if mclient -l sql -d demo -s "SELECT 1;" >/dev/null 2>&1; then
            info "MonetDB farm was already running."
            return
        fi
        warn "monetdbd start returned error."
    }
    # Wait for farm to be ready
    local retries=10
    while [ $retries -gt 0 ]; do
        if mclient -l sql -d demo -s "SELECT 1;" >/dev/null 2>&1; then
            break
        fi
        sleep 1
        retries=$((retries - 1))
    done
    ok "MonetDB farm started."
}

stop_monetdb() {
    if [ -n "$MONETDB_FARM" ]; then
        info "Stopping MonetDB farm..."
        monetdbd stop "$MONETDB_FARM" 2>/dev/null || true
        ok "MonetDB farm stopped."
    fi
}

load_monetdb_database() {
    local data_dir="$1"
    local str_level="${2:-10}"
    local dbname="${3:-prodds}"

    info "Loading data into MonetDB ($dbname)..."

    DATA_DIR="$data_dir" \
    STR="$str_level" \
    DBNAME="$dbname" \
    MONETDB_FARM="$MONETDB_FARM" \
        bash "$ROOT_DIR/experiments/scripts/load_monetdb.sh"

    ok "MonetDB loaded: $dbname"
}

# Unified loader
load_engine_data() {
    local engine="$1"
    local data_dir="$2"
    local str_level="$3"
    local db_label="$4"  # e.g., tpcds_sf1, prodds_sf1_str10

    case "$engine" in
        duckdb)
            local db_path
            db_path="$(db_dir)/duckdb/${db_label}.duckdb"
            load_duckdb_database "$db_path" "$data_dir" "$str_level"
            ;;
        cedardb)
            local db_dir_cedar
            db_dir_cedar="$(db_dir)/cedardb/${db_label}"
            start_cedardb "$db_dir_cedar"
            load_cedardb_database "$data_dir" "$str_level" "$db_label"
            ;;
        monetdb)
            local farm_path
            farm_path="$(db_dir)/monetdb/farm"
            start_monetdb "$farm_path"
            load_monetdb_database "$data_dir" "$str_level" "$db_label"
            ;;
    esac
}

# Cleanup on exit
cleanup_engines() {
    stop_cedardb 2>/dev/null || true
    stop_monetdb 2>/dev/null || true
}
trap cleanup_engines EXIT

# ──────────────────────────────────────────────────────────────
# Config Generation (engine-aware)
# ──────────────────────────────────────────────────────────────
write_experiment_config() {
    local config_path="$1"
    local engine="$2"
    local tpcds_qdir="$3"
    local prodds_qdir="$4"
    local exp_results_dir="$5"
    # Additional engine-specific params via positional args
    local db_label="${6:-}"

    mkdir -p "$(dirname "$config_path")"

    local mem_bytes
    mem_bytes=$(awk '/MemTotal/ {printf "%d", $2 * 1024 * 0.8}' /proc/meminfo 2>/dev/null || echo 8589934592)

    # Write global + experiments section
    cat > "$config_path" <<YAML
global:
  execution_mode: native
  threads: ${THREADS}
  memory_limit_bytes: ${mem_bytes}
  timeout_seconds_planning: 300
  timeout_seconds_execution: ${TIMEOUT}
  warmup_queries: ${WARMUP}
  repetitions: ${REPS}
  results_dir: "${exp_results_dir}"
  planning_enabled: true
  log_level: INFO

validation:
  enabled: false

experiments:
  workload_compare:
    tpcds_dir: "${tpcds_qdir}"
    prodds_dir: "${prodds_qdir}"

engines:
YAML

    # Append engine-specific config
    case "$engine" in
        duckdb)
            local db_path
            db_path="$(db_dir)/duckdb/${db_label}.duckdb"
            cat >> "$config_path" <<YAML
  duckdb:
    enabled: true
    load_command: "echo 'Data pre-loaded.'"
    database_path: "${db_path}"
    native:
      cli_path: "$(duckdb_bin)"
YAML
            ;;
        cedardb)
            cat >> "$config_path" <<YAML
  cedardb:
    enabled: true
    load_command: "echo 'Data pre-loaded.'"
    native:
      host: /tmp
      port: 5433
      user: postgres
      dbname: "${db_label}"
      password: postgres
YAML
            ;;
        monetdb)
            cat >> "$config_path" <<YAML
  monetdb:
    enabled: true
    load_command: "echo 'Data pre-loaded.'"
    native:
      host: localhost
      port: 50000
      user: monetdb
      password: monetdb
      dbname: "${db_label}"
      mclient_path: mclient
YAML
            ;;
    esac
}

# ──────────────────────────────────────────────────────────────
# Experiment Runners
# ──────────────────────────────────────────────────────────────

# Helper: run a single workload on an engine
run_workload_on_engine() {
    local engine="$1"
    local tpcds_qdir="$2"
    local prodds_qdir="$3"
    local exp_results_dir="$4"
    local db_label="$5"

    local cfg
    cfg="$(configs_dir)/${db_label}_${engine}.yaml"
    write_experiment_config "$cfg" "$engine" "$tpcds_qdir" "$prodds_qdir" \
        "$exp_results_dir" "$db_label"

    python3 -m experiments run \
        --config "$cfg" \
        --experiment workload_compare \
        --system "$engine" || warn "$engine run had errors."
}

run_e1() {
    step "E1: End-to-end TPC-DS vs Prod-DS (Section 6.5)"
    activate_venv

    local exp_results
    exp_results="$(results_dir)/E1"
    local empty_dir="$(configs_dir)/empty_queries"
    mkdir -p "$exp_results" "$empty_dir"

    IFS=',' read -ra engine_list <<< "$ENGINES"
    for engine in "${engine_list[@]}"; do
        if ! engine_available "$engine"; then
            warn "E1: $engine not available, skipping."
            continue
        fi
        info "Running E1 on $engine..."

        # Load TPC-DS data
        load_engine_data "$engine" "$(data_dir tpcds_sf${SF})" 1 "tpcds_sf${SF}"
        # Load Prod-DS data
        load_engine_data "$engine" "$(data_dir prodds_sf${SF}_str10)" 10 "prodds_sf${SF}_str10"

        # Run TPC-DS suite
        info "E1/$engine: Running TPC-DS workload..."
        run_workload_on_engine "$engine" \
            "$(queries_dir ${engine}/tpcds)" "$empty_dir" \
            "$exp_results/${engine}_tpcds" "tpcds_sf${SF}"

        # Run Prod-DS suite
        info "E1/$engine: Running Prod-DS workload..."
        run_workload_on_engine "$engine" \
            "$empty_dir" "$(queries_dir ${engine}/prodds)" \
            "$exp_results/${engine}_prodds" "prodds_sf${SF}_str10"

        # Stop server engines between engines (isolation)
        case "$engine" in
            cedardb) stop_cedardb ;;
            monetdb) stop_monetdb ;;
        esac
    done

    ok "E1 complete. Results in $exp_results"
}

run_e2() {
    step "E2: Join-scaling micro-suite (Section 6.6)"
    activate_venv

    local exp_results
    exp_results="$(results_dir)/E2"
    local empty_dir="$(configs_dir)/empty_queries"
    mkdir -p "$exp_results" "$empty_dir"

    IFS=',' read -ra engine_list <<< "$ENGINES"
    for engine in "${engine_list[@]}"; do
        if ! engine_available "$engine"; then
            warn "E2: $engine not available, skipping."
            continue
        fi
        info "Running E2 on $engine..."

        load_engine_data "$engine" "$(data_dir prodds_sf${SF}_str10)" 10 "prodds_sf${SF}_str10"

        run_workload_on_engine "$engine" \
            "$empty_dir" "$(queries_dir join_scaling)" \
            "$exp_results/$engine" "prodds_sf${SF}_str10"

        case "$engine" in
            cedardb) stop_cedardb ;;
            monetdb) stop_monetdb ;;
        esac
    done

    ok "E2 complete. Results in $exp_results"
}

run_e3() {
    step "E3: UNION ALL fan-in scaling (Section 6.7)"
    activate_venv

    local exp_results
    exp_results="$(results_dir)/E3"
    local empty_dir="$(configs_dir)/empty_queries"
    mkdir -p "$exp_results" "$empty_dir"

    IFS=',' read -ra engine_list <<< "$ENGINES"
    for engine in "${engine_list[@]}"; do
        if ! engine_available "$engine"; then
            warn "E3: $engine not available, skipping."
            continue
        fi
        info "Running E3 on $engine..."

        load_engine_data "$engine" "$(data_dir prodds_sf${SF}_str10)" 10 "prodds_sf${SF}_str10"

        # MonetDB uses CTE-inlined variants (optimizer bug workaround)
        local union_dir="$(queries_dir union_scaling)"
        if [ "$engine" = "monetdb" ] && [ -d "$(queries_dir monetdb/union_scaling)" ]; then
            union_dir="$(queries_dir monetdb/union_scaling)"
        fi

        run_workload_on_engine "$engine" \
            "$empty_dir" "$union_dir" \
            "$exp_results/$engine" "prodds_sf${SF}_str10"

        case "$engine" in
            cedardb) stop_cedardb ;;
            monetdb) stop_monetdb ;;
        esac
    done

    ok "E3 complete. Results in $exp_results"
}

run_e4() {
    step "E4: Stringification sweep STR=1..15 (Section 6.8, DuckDB only)"
    activate_venv

    local exp_results
    exp_results="$(results_dir)/E4"
    local empty_dir="$(configs_dir)/empty_queries"
    mkdir -p "$exp_results" "$empty_dir"

    for str in $E4_LEVELS; do
        info "E4: STR=$str (levels: $E4_LEVELS)"

        load_engine_data "duckdb" "$(data_dir str_sweep/str${str})" "$str" "str_sweep_sf${SF}_str${str}"

        run_workload_on_engine "duckdb" \
            "$empty_dir" "$(queries_dir duckdb/str_sweep/str${str})" \
            "$exp_results/str${str}" "str_sweep_sf${SF}_str${str}"
    done

    ok "E4 complete. Results in $exp_results"
}

run_e5() {
    step "E5: Sparsity & skew sensitivity (Section 6.9)"
    activate_venv

    local exp_results
    exp_results="$(results_dir)/E5"
    local empty_dir="$(configs_dir)/empty_queries"
    mkdir -p "$exp_results" "$empty_dir"

    local variants=("baseline" "sparsity_only" "skew_only" "combined")

    # E5 runs on DuckDB + CedarDB (per paper)
    local e5_engines="duckdb"
    if echo "$ENGINES" | grep -q "cedardb"; then
        e5_engines="duckdb,cedardb"
    fi

    for variant in "${variants[@]}"; do
        info "E5: variant=$variant"

        IFS=',' read -ra e5_list <<< "$e5_engines"
        for engine in "${e5_list[@]}"; do
            if ! engine_available "$engine"; then
                warn "E5/$variant: $engine not available, skipping."
                continue
            fi

            local db_label="sparsity_sf${SF}_${variant}"
            load_engine_data "$engine" "$(data_dir sparsity/${variant})" 10 "$db_label"

            run_workload_on_engine "$engine" \
                "$empty_dir" "$(queries_dir ${engine}/prodds)" \
                "$exp_results/${variant}_${engine}" "$db_label"

            case "$engine" in
                cedardb) stop_cedardb ;;
            esac
        done
    done

    ok "E5 complete. Results in $exp_results"
}

# ──────────────────────────────────────────────────────────────
# Plotting
# ──────────────────────────────────────────────────────────────
generate_plots() {
    step "Generating plots"
    activate_venv

    local plot_dir
    plot_dir="$(results_dir)/plots"
    mkdir -p "$plot_dir"

    if [ -f "$ROOT_DIR/experiments/plot_results.py" ]; then
        python3 "$ROOT_DIR/experiments/plot_results.py" \
            --results-dir "$(results_dir)" \
            --output-dir "$plot_dir" || warn "Plot generation had errors."
        ok "Plots saved to $plot_dir"
    else
        warn "Plot script not found. Skipping plot generation."
    fi
}

# ──────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────
main() {
    parse_args "$@"

    echo ""
    echo "  Prod-DS Kit — VLDB 2027 Reproducibility"
    echo "  ──────────────────────────────────────────"
    echo "  Scale factor:  SF=$SF"
    echo "  Engines:       $ENGINES"
    echo "  Repetitions:   $REPS (warmup: $WARMUP)"
    echo "  Timeout:       ${TIMEOUT}s"
    echo "  Threads:       $THREADS"
    if [ -n "$EXPERIMENTS" ]; then
        echo "  Experiments:   $EXPERIMENTS"
    fi
    echo ""

    if $PLOTS_ONLY; then
        generate_plots
        exit 0
    fi

    if $INIT; then
        check_prerequisites
        install_toolkit
        install_engines
        generate_all_data
        generate_all_queries
        ok "Initialization complete."
    fi

    # Ensure queries exist for requested experiments (even without --init)
    if [ -n "$EXPERIMENTS" ] && ! $INIT; then
        activate_venv
        # Check if base queries exist; generate if missing
        IFS=',' read -ra engine_list <<< "$ENGINES"
        for engine in "${engine_list[@]}"; do
            local dialect
            dialect=$(engine_dialect "$engine")
            if [ ! -d "$(queries_dir ${engine}/tpcds)" ]; then
                generate_queries_variant "$(queries_dir ${engine}/tpcds)" 1 --no-extensions --dialect "$dialect"
            fi
            if [ ! -d "$(queries_dir ${engine}/prodds)" ]; then
                generate_queries_variant "$(queries_dir ${engine}/prodds)" 10 --dialect "$dialect"
            fi
            # Always apply dialect fixes (idempotent) — even if queries existed
            # from a previous partial run that may not have completed the fix step
            apply_dialect_fixes "$engine" "$(queries_dir ${engine}/tpcds)" "tpcds"
            apply_dialect_fixes "$engine" "$(queries_dir ${engine}/prodds)" "prodds"
        done
        # E2/E3 micro-suites
        if echo "$EXPERIMENTS" | grep -qw "E2"; then
            local join_dir
            join_dir="$(queries_dir join_scaling)"
            if [ ! -d "$join_dir" ] || [ -z "$(ls -A "$join_dir" 2>/dev/null)" ]; then
                generate_all_queries
            fi
        fi
        if echo "$EXPERIMENTS" | grep -qw "E3"; then
            local union_dir
            union_dir="$(queries_dir union_scaling)"
            if [ ! -d "$union_dir" ] || [ -z "$(ls -A "$union_dir" 2>/dev/null)" ]; then
                generate_all_queries
            fi
            # MonetDB needs CTE-inlined union queries (generated separately)
            if echo "$ENGINES" | grep -q "monetdb"; then
                local monetdb_union_dir
                monetdb_union_dir="$(queries_dir monetdb/union_scaling)"
                if [ ! -d "$monetdb_union_dir" ] || [ -z "$(ls -A "$monetdb_union_dir" 2>/dev/null)" ]; then
                    monetdb_inline_union_ctes "$union_dir" "$monetdb_union_dir"
                fi
            fi
        fi
    fi

    for exp in $EXPERIMENTS; do
        case "$exp" in
            E1) run_e1 ;;
            E2) run_e2 ;;
            E3) run_e3 ;;
            E4) run_e4 ;;
            E5) run_e5 ;;
        esac
    done

    if [ -n "$EXPERIMENTS" ]; then
        generate_plots
        echo ""
        echo "  ═══════════════════════════════════════"
        echo "  All requested experiments complete."
        echo "  Results: $(results_dir)"
        echo "  ═══════════════════════════════════════"
    fi
}

main "$@"

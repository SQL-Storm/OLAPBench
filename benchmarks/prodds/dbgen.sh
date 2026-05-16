#!/usr/bin/env bash
set -euo pipefail

SF=${1:-1}
STR=${2:-10}
NULLP=${3:-medium}
MCVP=${4:-medium}

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PROD_DS_DIR="$REPO_ROOT/benchmarks/prodds/prod-ds"
OUT_REL="prodds/sf${SF}_str${STR}_null${NULLP}_mcv${MCVP}"
OUT_ABS="$REPO_ROOT/data/$OUT_REL"

echo "Generating Prod-DS dataset: SF=$SF STR=$STR NULL=$NULLP MCV=$MCVP -> $OUT_REL"

if [ ! -f "$PROD_DS_DIR/.install_complete" ]; then
  echo "Bootstrapping prod-ds (one-time setup)..."
  (cd "$PROD_DS_DIR" && bash install.sh)
fi

# shellcheck disable=SC1091
source "$PROD_DS_DIR/.venv/bin/activate"

mkdir -p "$OUT_ABS"

export STRINGIFY_BACKEND=cpp

# Run dsdgen single-process so output is one .dat per table (matches OLAPBench TPC-DS layout).
# --parallel N would emit sharded files like <table>_<child>_<N>.dat which the schema loader cannot consume.
NULL_ARGS=(--null-profile "$NULLP")
if [ "$NULLP" = "none" ]; then
  NULL_ARGS=(--disable-null-skew)
fi
MCV_ARGS=(--mcv-profile "$MCVP")
if [ "$MCVP" = "none" ]; then
  MCV_ARGS=(--disable-mcv-skew)
fi
echo "$PROD_DS_DIR/wrap_dsdgen.py --stringification-level $STR ${NULL_ARGS[*]} ${MCV_ARGS[*]} --min-ndv-for-injection 0 -SCALE $SF -DIR $OUT_ABS -FORCE"
python3 "$PROD_DS_DIR/wrap_dsdgen.py" \
  --stringification-level "$STR" \
  "${NULL_ARGS[@]}" \
  "${MCV_ARGS[@]}" \
  --min-ndv-for-injection 0 \
  -SCALE "$SF" -DIR "$OUT_ABS" -FORCE

# dsdgen emits trailing pipes; strip them in place to match existing OLAPBench TPC-DS layout
for f in "$OUT_ABS"/*.dat; do
  [ -f "$f" ] || continue
  sed -i 's/|$//' "$f"
done

python3 "$REPO_ROOT/benchmarks/prodds/gen_schema.py" \
  --level "$STR" \
  --out "$OUT_ABS/schema.json"

# Generate queries for all three dialects at this STR level. Each invocation is
# fast (~5s) and lets the user switch --dialect without re-running dbgen.
for d in ansi duckdb postgres; do
  case $d in
    ansi) qdir="$OUT_ABS/queries" ;;
    *)    qdir="$OUT_ABS/queries_$d" ;;
  esac
  python3 "$PROD_DS_DIR/wrap_dsqgen.py" \
    --output-dir "$qdir" \
    --dialect "$d" \
    --stringification-level "$STR" \
    --no-join --no-union >/dev/null
  rm -f "$qdir/_permutation.json"
done

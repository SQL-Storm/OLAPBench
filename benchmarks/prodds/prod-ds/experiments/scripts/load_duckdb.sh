#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-}"
SF="${SF:-}"
STR="${STR:-10}"

DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
DUCKDB_PATH="${DUCKDB_PATH:-${DATABASE_PATH:-}}"
DUCKDB_ALLOW_OVERWRITE="${DUCKDB_ALLOW_OVERWRITE:-0}"
SCHEMA_FILE="${SCHEMA_FILE:-}"

if [[ -z "${DATA_DIR}" ]]; then
  echo "DATA_DIR is required (directory with *.dat/*.tbl files)." >&2
  exit 1
fi
if [[ ! -d "${DATA_DIR}" ]]; then
  echo "DATA_DIR not found: ${DATA_DIR}" >&2
  exit 1
fi

if [[ -z "${DUCKDB_PATH}" ]]; then
  echo "DUCKDB_PATH (or DATABASE_PATH) is required." >&2
  exit 1
fi

if [[ -z "${SCHEMA_FILE}" ]]; then
  if [[ "${STR}" -le 1 ]]; then
    SCHEMA_FILE="tools/tpcds.sql"
  else
    SCHEMA_FILE="tools/prodds.sql"
  fi
fi
if [[ ! -f "${SCHEMA_FILE}" ]]; then
  echo "Schema file not found: ${SCHEMA_FILE}" >&2
  exit 1
fi

if [[ -e "${DUCKDB_PATH}" && "${DUCKDB_ALLOW_OVERWRITE}" != "1" ]]; then
  echo "DuckDB file exists and overwrite disabled: ${DUCKDB_PATH}" >&2
  echo "Set DUCKDB_ALLOW_OVERWRITE=1 to rebuild it." >&2
  exit 1
fi

if [[ -e "${DUCKDB_PATH}" && "${DUCKDB_ALLOW_OVERWRITE}" == "1" ]]; then
  rm -f "${DUCKDB_PATH}"
fi
mkdir -p "$(dirname "${DUCKDB_PATH}")"

echo "[load] DuckDB settings:"
echo "  db=${DUCKDB_PATH}"
echo "  DATA_DIR=${DATA_DIR}"
echo "  SF=${SF:-} STR=${STR}"
echo "  SCHEMA_FILE=${SCHEMA_FILE}"
echo "  overwrite=${DUCKDB_ALLOW_OVERWRITE}"

"${DUCKDB_BIN}" "${DUCKDB_PATH}" -c ".read ${SCHEMA_FILE}"

shopt -s nullglob
files=( "${DATA_DIR}"/*.dat "${DATA_DIR}"/*.tbl )
if [[ "${#files[@]}" -eq 0 ]]; then
  echo "No data files found in ${DATA_DIR} (expected *.dat or *.tbl)." >&2
  exit 1
fi

for file in "${files[@]}"; do
  base="$(basename "${file}")"
  table="${base%.*}"
  echo "[load] ${table} <- ${file}"
  iconv -f ISO-8859-1 -t UTF-8 "${file}" | \
    "${DUCKDB_BIN}" "${DUCKDB_PATH}" -c "COPY ${table} FROM '/dev/stdin' (DELIMITER '|', HEADER false, NULL '', AUTO_DETECT false);"
done

echo "[load] ANALYZE"
"${DUCKDB_BIN}" "${DUCKDB_PATH}" -c "ANALYZE;"
echo "[load] DuckDB load complete."

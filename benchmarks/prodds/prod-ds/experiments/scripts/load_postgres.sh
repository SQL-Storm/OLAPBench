#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-}"
SF="${SF:-}"
STR="${STR:-}"

PGHOST="${PGHOST:-/var/run/postgresql}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-${USER}}"
PGDATABASE="${PGDATABASE:-prodds}"

if [[ -z "${DATA_DIR}" ]]; then
  echo "DATA_DIR is required (directory with *.dat/*.tbl files)." >&2
  exit 1
fi

if [[ ! -d "${DATA_DIR}" ]]; then
  echo "DATA_DIR not found: ${DATA_DIR}" >&2
  exit 1
fi

if [[ -z "${STR}" ]]; then
  STR="10"
fi

SCHEMA_FILE="${SCHEMA_FILE:-}"
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

PSQL_BASE=(psql -X -q -v ON_ERROR_STOP=1 -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}")

echo "[load] Postgres settings:"
echo "  host=${PGHOST} port=${PGPORT} user=${PGUSER} db=${PGDATABASE}"
echo "  DATA_DIR=${DATA_DIR}"
echo "  SF=${SF:-} STR=${STR}"
echo "  SCHEMA_FILE=${SCHEMA_FILE}"

echo "[load] Dropping and recreating database ${PGDATABASE}..."
"${PSQL_BASE[@]}" -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='${PGDATABASE}' AND pid <> pg_backend_pid();"
"${PSQL_BASE[@]}" -d postgres -c "DROP DATABASE IF EXISTS \"${PGDATABASE}\";"
"${PSQL_BASE[@]}" -d postgres -c "CREATE DATABASE \"${PGDATABASE}\";"

echo "[load] Loading schema..."
"${PSQL_BASE[@]}" -d "${PGDATABASE}" -f "${SCHEMA_FILE}"

echo "[load] Ensuring any_value aggregate exists..."
"${PSQL_BASE[@]}" -d "${PGDATABASE}" <<'SQL'
DO $do$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_aggregate a
    JOIN pg_proc p ON p.oid = a.aggfnoid
    WHERE p.proname = 'any_value'
  ) THEN
    CREATE OR REPLACE FUNCTION any_value_agg(state anyelement, value anyelement)
    RETURNS anyelement
    LANGUAGE SQL
    IMMUTABLE
    AS $fn$ SELECT COALESCE(state, value) $fn$;
    CREATE AGGREGATE any_value(anyelement) (
      SFUNC = any_value_agg,
      STYPE = anyelement
    );
  END IF;
END $do$;
SQL

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
  sed 's/|$//' "${file}" | "${PSQL_BASE[@]}" -d "${PGDATABASE}" -c "\\copy ${table} FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL '', ENCODING 'LATIN1')"
done

echo "[load] Postgres load complete."

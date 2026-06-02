#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${DATA_DIR:-}"
SF="${SF:-}"
STR="${STR:-}"

CEDAR_HOST="${CEDAR_HOST:-/tmp}"
CEDAR_PORT="${CEDAR_PORT:-5433}"
CEDAR_USER="${CEDAR_USER:-postgres}"
CEDAR_DB="${CEDAR_DB:-prodds}"
CEDAR_ADMIN_DB="${CEDAR_ADMIN_DB:-postgres}"
CEDAR_RECREATE_DB="${CEDAR_RECREATE_DB:-1}"

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

PSQL_BASE=(psql -X -q -v ON_ERROR_STOP=1 -h "${CEDAR_HOST}" -p "${CEDAR_PORT}" -U "${CEDAR_USER}")

echo "[load] CedarDB settings:"
echo "  host=${CEDAR_HOST} port=${CEDAR_PORT} user=${CEDAR_USER} db=${CEDAR_DB}"
echo "  DATA_DIR=${DATA_DIR}"
echo "  SF=${SF:-} STR=${STR}"
echo "  SCHEMA_FILE=${SCHEMA_FILE}"
echo "  recreate_db=${CEDAR_RECREATE_DB}"

if [[ "${CEDAR_RECREATE_DB}" == "1" ]]; then
  echo "[load] Dropping and recreating database ${CEDAR_DB}..."
  if "${PSQL_BASE[@]}" -d "${CEDAR_ADMIN_DB}" -c "DROP DATABASE IF EXISTS \"${CEDAR_DB}\";" \
    && "${PSQL_BASE[@]}" -d "${CEDAR_ADMIN_DB}" -c "CREATE DATABASE \"${CEDAR_DB}\";"; then
    :
  else
    echo "[load] DROP/CREATE DATABASE unsupported; dropping all tables then recreating public schema."
    # Drop every user table in public schema (handles tables not matching data files)
    "${PSQL_BASE[@]}" -d "${CEDAR_DB}" -t -A -c \
      "SELECT tablename FROM pg_tables WHERE schemaname = 'public';" | \
    while IFS= read -r t; do
      [[ -z "${t}" ]] && continue
      "${PSQL_BASE[@]}" -d "${CEDAR_DB}" -c "DROP TABLE IF EXISTS \"${t}\" CASCADE;" 2>/dev/null || \
      "${PSQL_BASE[@]}" -d "${CEDAR_DB}" -c "DROP TABLE IF EXISTS \"${t}\";" || true
    done
    # Attempt schema recreate (best-effort; tables are already gone)
    "${PSQL_BASE[@]}" -d "${CEDAR_DB}" -c "DROP SCHEMA IF EXISTS public;" 2>/dev/null || true
    "${PSQL_BASE[@]}" -d "${CEDAR_DB}" -c "CREATE SCHEMA IF NOT EXISTS public;" 2>/dev/null || true
  fi
fi

echo "[load] Loading schema..."
"${PSQL_BASE[@]}" -d "${CEDAR_DB}" -f "${SCHEMA_FILE}"

echo "[load] Ensuring any_value aggregate exists..."
if ! "${PSQL_BASE[@]}" -d "${CEDAR_DB}" <<'SQL'
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
then
  echo "[load] any_value setup skipped (unsupported on this CedarDB build)." >&2
fi

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
  sed 's/|$//' "${file}" | iconv -f LATIN1 -t UTF-8 | \
    "${PSQL_BASE[@]}" -d "${CEDAR_DB}" -c "\\copy ${table} FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL '')"
done

echo "[load] CedarDB load complete."

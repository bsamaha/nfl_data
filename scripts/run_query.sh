#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 -f queries/<file.sql> [flags] [--] [duckdb_args...]" >&2
  echo "  -s, --season N           Set SELECT N AS season (for per-season queries)" >&2
  echo "  -w, --thru-week N        Set SELECT N AS thru_week (for through-week queries)" >&2
  echo "  -t, --season-type STR    Set 'REG'|'POST'|'PRE' AS season_type" >&2
  echo "  -S, --season-start N     Set SELECT N AS season_start (for YoY range queries)" >&2
  echo "  -E, --season-end N       Set SELECT N AS season_end (for YoY range queries)" >&2
}

FILE=""
SEASON=""
THRU_WEEK=""
SEASON_TYPE=""
SEASON_START=""
SEASON_END=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--file) FILE="$2"; shift 2;;
    -s|--season) SEASON="$2"; shift 2;;
    -w|--thru-week) THRU_WEEK="$2"; shift 2;;
    -t|--season-type) SEASON_TYPE="$2"; shift 2;;
    -S|--season-start) SEASON_START="$2"; shift 2;;
    -E|--season-end) SEASON_END="$2"; shift 2;;
    --) shift; break;;
    -h|--help) usage; exit 0;;
    *) break;;
  esac
done

if [[ -z "${FILE}" ]]; then
  usage; exit 1
fi

if [[ ! -f "${FILE}" ]]; then
  echo "File not found: ${FILE}" >&2
  exit 1
fi

TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT
cp "${FILE}" "$TMP"

# If season not provided, detect latest active season from data/silver
if [[ -z "${SEASON}" ]]; then
  # Prefer schedules partition; fallback to pbp year; otherwise current year
  if compgen -G "data/silver/schedules/season=*/*.parquet" > /dev/null; then
    SEASON=$(ls -d data/silver/schedules/season=* 2>/dev/null | sed -E 's#.*/season=([0-9]{4}).*#\1#' | sort -nr | head -1 || true)
  fi
  if [[ -z "${SEASON}" ]] && compgen -G "data/silver/pbp/year=*/**/*.parquet" > /dev/null; then
    SEASON=$(ls -d data/silver/pbp/year=* 2>/dev/null | sed -E 's#.*/year=([0-9]{4}).*#\1#' | sort -nr | head -1 || true)
  fi
  if [[ -z "${SEASON}" ]]; then
    SEASON=$(date +%Y)
  fi
fi

# Replace default params if flags provided
if [[ -n "${SEASON}" ]]; then
  sed -E -i "s/(SELECT )[0-9]{4}( AS season)/\1${SEASON}\2/" "$TMP"
fi
if [[ -n "${THRU_WEEK}" ]]; then
  sed -E -i "s/(SELECT )[0-9]{1,2}( AS thru_week)/\1${THRU_WEEK}\2/" "$TMP"
fi
if [[ -n "${SEASON_TYPE}" ]]; then
  sed -E -i "s/'(REG|POST|PRE)'( AS season_type)/'${SEASON_TYPE}'\2/" "$TMP"
fi
if [[ -n "${SEASON_START}" ]]; then
  sed -E -i "s/(SELECT )[0-9]{4}( AS season_start)/\1${SEASON_START}\2/" "$TMP"
fi
if [[ -n "${SEASON_END}" ]]; then
  sed -E -i "s/(SELECT )[0-9]{4}( AS season_end)/\1${SEASON_END}\2/" "$TMP"
fi

# Run with DuckDB, pass through any remaining args
duckdb "$@" < "$TMP"



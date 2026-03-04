#!/usr/bin/env bash

# Usage: ./load_test_ha_compare.sh [1k|10k|100k|1m]
#
# Runs each test file with noHA, HA(H2), and optionally HA(PG) and compares the results.
# PostgreSQL runs require Docker; they are skipped with a warning if Docker is unavailable.
# Defaults to "1k" if no argument is given.

set -euo pipefail

JAR="target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Fat JAR not found at $JAR"
  echo "Run: mvn -pl drools-ansible-rulebook-integration-main -am package"
  exit 1
fi

# 1) Pick up the size argument, default to "1k"
size="${1:-1k}"

# 2) Validate it
case "$size" in
  1k|10k|100k|1m) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [1k|10k|100k|1m]"
    exit 1
    ;;
esac

# 3) Prepare filenames
match_file="24kb_${size}_events.json"
unmatch_file="24kb_${size}_events_unmatch.json"

out="result_ha_compare_${size}.txt"

# 4) Docker PostgreSQL setup
PG_ENABLED=false
PG_CONTAINER=""
PG_PARAMS=""

setup_postgres() {
  if ! docker info >/dev/null 2>&1; then
    echo "WARNING: Docker is not available. Skipping PostgreSQL (HA-PG) runs."
    return
  fi

  # Pick a random available port (podman doesn't support -p 0:5432)
  local pg_port
  pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

  echo "Starting PostgreSQL container on port $pg_port..."
  PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_USER=loadtest \
    -e POSTGRES_PASSWORD=loadtest \
    -e POSTGRES_DB=loadtest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  # Wait for PostgreSQL to be ready (timeout 30s)
  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U loadtest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "WARNING: PostgreSQL failed to start within timeout. Skipping PG runs."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      PG_CONTAINER=""
      return
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"loadtest\",\"user\":\"loadtest\",\"password\":\"loadtest\",\"sslmode\":\"disable\"}"
  PG_ENABLED=true
  echo "PostgreSQL ready on port $pg_port"
}

cleanup_postgres() {
  if [ -n "$PG_CONTAINER" ]; then
    echo "Stopping PostgreSQL container..."
    docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
    PG_CONTAINER=""
  fi
}

trap cleanup_postgres EXIT

setup_postgres

# 5) Header
header=$(printf "=== HA Performance Comparison (size=%s) ===\n" "$size")
table_header=$(printf "\n%-36s %-10s %14s %9s\n" "File" "Mode" "Memory(bytes)" "Time(ms)")
separator=$(printf "%s" "$(head -c 71 < /dev/zero | tr '\0' '-')")

{
  echo "$header"
  echo "$table_header"
  echo "$separator"
} | tee "$out"

# Helper: extract metrics from stderr. The metrics line starts with the filename.
# Usage: parse_metrics "$stderr_output" "$file"
# Sets: _mem and _time variables
parse_metrics() {
  local stderr_output="$1"
  local filename="$2"
  local metrics_line
  metrics_line=$(echo "$stderr_output" | grep "^${filename}" | tail -1)
  if [ -z "$metrics_line" ]; then
    _mem="FAILED"
    _time="FAILED"
  else
    _mem=$(echo "$metrics_line" | cut -d',' -f2 | tr -d ' ')
    _time=$(echo "$metrics_line" | cut -d',' -f3 | tr -d ' ')
  fi
}

# 6) Debug log — all Java stdout+stderr is appended here
LOG="out.log"
> "$LOG"

# Helper: run java, append all output to out.log, capture stderr for metrics parsing
# Usage: run_java <label> <args...>
# Sets: _run_stderr
run_java() {
  local label="$1"; shift
  local tmpstderr
  tmpstderr=$(mktemp)
  echo "=== $label ===" >> "$LOG"
  java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar "$JAR" "$@" >> "$LOG" 2>"$tmpstderr" || true
  cat "$tmpstderr" >> "$LOG"
  _run_stderr=$(cat "$tmpstderr")
  rm -f "$tmpstderr"
  echo "" >> "$LOG"
}

# Run comparison for each file
for file in "$match_file" "$unmatch_file"; do

  # --- noHA run ---
  echo "Running $file (noHA)..."
  run_java "$file (noHA)" "$file"
  parse_metrics "$_run_stderr" "$file"
  noha_mem="$_mem"; noha_time="$_time"

  # --- HA(H2) run ---
  echo "Running $file (HA-H2)..."
  run_java "$file (HA-H2)" "$file" --ha
  parse_metrics "$_run_stderr" "$file"
  ha_mem="$_mem"; ha_time="$_time"

  # --- HA(PG) run (if Docker available) ---
  pg_mem=""
  pg_time=""
  if [ "$PG_ENABLED" = true ]; then
    echo "Running $file (HA-PG)..."
    run_java "$file (HA-PG)" "$file" --ha-db-params "$PG_PARAMS"
    parse_metrics "$_run_stderr" "$file"
    pg_mem="$_mem"; pg_time="$_time"
  fi

  # --- Calculate overheads ---
  h2_overhead="N/A"
  pg_overhead="N/A"
  if [ "$noha_time" -gt 0 ] 2>/dev/null; then
    h2_overhead=$(awk "BEGIN { printf \"%.1f\", ($ha_time - $noha_time) / $noha_time * 100 }")
    if [ -n "$pg_time" ] && [ "$pg_time" -gt 0 ] 2>/dev/null; then
      pg_overhead=$(awk "BEGIN { printf \"%.1f\", ($pg_time - $noha_time) / $noha_time * 100 }")
    fi
  fi

  # --- Print results ---
  {
    printf "%-36s %-10s %14s %9s\n" "$file" "noHA" "$noha_mem" "$noha_time"
    printf "%-36s %-10s %14s %9s\n" "$file" "HA(H2)" "$ha_mem" "$ha_time"
    if [ "$PG_ENABLED" = true ]; then
      printf "%-36s %-10s %14s %9s\n" "$file" "HA(PG)" "$pg_mem" "$pg_time"
    fi
    printf "%-36s %-10s %14s %+8s%%\n" "" "H2 overhead" "" "$h2_overhead"
    if [ "$PG_ENABLED" = true ]; then
      printf "%-36s %-10s %14s %+8s%%\n" "" "PG overhead" "" "$pg_overhead"
    fi
    echo ""
  } | tee -a "$out"

done

echo "Results written to $out"

#!/usr/bin/env bash

# Usage: ./load_test_encryption.sh [100|500|1k]
#
# Measures HA PostgreSQL encryption overhead on the same temporal once_within
# workload used by load_test_temporal.sh.
# Runs the workload twice:
#   1. HA encryption OFF
#   2. HA encryption ON
# Reports load time, memory, final DB row counts, and stored payload bytes.
#
# Requires Docker for PostgreSQL.

set -euo pipefail

JAR="target/drools-ansible-rulebook-integration-main-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Fat JAR not found at $JAR"
  echo "Run: mvn -pl drools-ansible-rulebook-integration-main -am package"
  exit 1
fi

size="${1:-100}"

case "$size" in
  100|500|1k) ;;
  *)
    echo "Invalid size: $size"
    echo "Usage: $0 [100|500|1k]"
    exit 1
    ;;
esac

test_file="once_within_${size}_events.json"

out="result_encryption_${size}.txt"
LOG="out_encryption.log"
> "$LOG"

PG_CONTAINER=""
PG_PARAMS=""
ENCRYPTION_KEY=""

setup_postgres() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not available. PostgreSQL is required for encryption test."
    exit 1
  fi

  local pg_port
  pg_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

  echo "Starting PostgreSQL container on port $pg_port..."
  PG_CONTAINER=$(docker run -d --rm \
    -e POSTGRES_USER=encryptiontest \
    -e POSTGRES_PASSWORD=encryptiontest \
    -e POSTGRES_DB=encryptiontest \
    -p "${pg_port}:5432" \
    postgres:15-alpine)

  echo "Waiting for PostgreSQL to be ready on port $pg_port..."
  local retries=30
  while ! docker exec "$PG_CONTAINER" pg_isready -U encryptiontest -q 2>/dev/null; do
    retries=$((retries - 1))
    if [ "$retries" -le 0 ]; then
      echo "ERROR: PostgreSQL failed to start within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  local conn_retries=10
  while ! docker exec "$PG_CONTAINER" psql -U encryptiontest -d encryptiontest -c "SELECT 1" >/dev/null 2>&1; do
    conn_retries=$((conn_retries - 1))
    if [ "$conn_retries" -le 0 ]; then
      echo "ERROR: PostgreSQL authentication not ready within timeout."
      docker stop "$PG_CONTAINER" >/dev/null 2>&1 || true
      exit 1
    fi
    sleep 1
  done

  PG_PARAMS="{\"db_type\":\"postgres\",\"host\":\"localhost\",\"port\":${pg_port},\"database\":\"encryptiontest\",\"user\":\"encryptiontest\",\"password\":\"encryptiontest\",\"sslmode\":\"disable\"}"
  ENCRYPTION_KEY=$(python3 -c 'import os, base64; print(base64.b64encode(os.urandom(32)).decode())')
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

run_java() {
  local label="$1"; shift
  local tmpstderr
  tmpstderr=$(mktemp)
  echo "=== $label ===" >> "$LOG"
  java -Xmx1g -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar "$JAR" "$@" >> "$LOG" 2>"$tmpstderr" || true
  cat "$tmpstderr" >> "$LOG"
  _run_stderr=$(cat "$tmpstderr")
  rm -f "$tmpstderr"
  echo "" >> "$LOG"
}

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

pg_scalar() {
  local sql="$1"
  docker exec "$PG_CONTAINER" psql -U encryptiontest -d encryptiontest -tAc "$sql" 2>/dev/null || echo "ERR"
}

pg_count() {
  local table="$1"
  pg_scalar "SELECT COUNT(*) FROM $table"
}

pg_truncate() {
  docker exec "$PG_CONTAINER" psql -U encryptiontest -d encryptiontest -c \
    "TRUNCATE drools_ansible_session_state, drools_ansible_matching_event, drools_ansible_action_info, drools_ansible_ha_stats" >/dev/null 2>&1 || true
}

plain_config='{"write_after":1}'
encrypted_config="{\"write_after\":1,\"encryption_key_primary\":\"${ENCRYPTION_KEY}\"}"

header=$(printf "=== HA Encryption Load Test (once_within, size=%s) ===" "$size")
table_header=$(printf "\n%-14s %14s %9s %10s %10s %10s %16s" "Mode" "Memory(bytes)" "Time(ms)" "SESSION" "MATCHING" "ACTION" "PayloadBytes")
separator=$(printf "%s" "$(head -c 101 < /dev/zero | tr '\0' '-')")

{
  echo "$header"
  echo "$table_header"
  echo "$separator"
} | tee "$out"

declare -A MEM_RESULTS
declare -A TIME_RESULTS
declare -A PAYLOAD_RESULTS

for mode in plain encrypted; do
  pg_truncate

  if [ "$mode" = "plain" ]; then
    mode_label="HA-PG"
    ha_config="$plain_config"
  else
    mode_label="HA-PG-ENC"
    ha_config="$encrypted_config"
  fi

  echo ""
  echo "Loading events into PostgreSQL ($test_file, $mode_label)..."
  run_java "$test_file ($mode_label)" "$test_file" \
    --ha-db-params "$PG_PARAMS" \
    --ha-config "$ha_config"
  parse_metrics "$_run_stderr" "$test_file"

  MEM_RESULTS["$mode"]="$_mem"
  TIME_RESULTS["$mode"]="$_time"

  session_rows=$(pg_count "drools_ansible_session_state")
  matching_rows=$(pg_count "drools_ansible_matching_event")
  action_rows=$(pg_count "drools_ansible_action_info")
  payload_bytes=$(pg_scalar "SELECT COALESCE((SELECT SUM(pg_column_size(partial_matching_events)) FROM drools_ansible_session_state), 0) + COALESCE((SELECT SUM(pg_column_size(event_data)) FROM drools_ansible_matching_event), 0) + COALESCE((SELECT SUM(pg_column_size(action_data)) FROM drools_ansible_action_info), 0)")
  PAYLOAD_RESULTS["$mode"]="$payload_bytes"

  printf "%-14s %14s %9s %10s %10s %10s %16s\n" \
    "$mode_label" "${MEM_RESULTS[$mode]}" "${TIME_RESULTS[$mode]}" "$session_rows" "$matching_rows" "$action_rows" "$payload_bytes" | tee -a "$out"
done

plain_time="${TIME_RESULTS[plain]}"
enc_time="${TIME_RESULTS[encrypted]}"
plain_payload="${PAYLOAD_RESULTS[plain]}"
enc_payload="${PAYLOAD_RESULTS[encrypted]}"

time_overhead="N/A"
payload_overhead="N/A"

if [ "$plain_time" != "FAILED" ] && [ "$enc_time" != "FAILED" ] && [ "$plain_time" -gt 0 ] 2>/dev/null; then
  time_overhead=$(awk "BEGIN { printf \"%.1f\", ($enc_time - $plain_time) / $plain_time * 100 }")
fi

if [ "$plain_payload" != "ERR" ] && [ "$enc_payload" != "ERR" ] && [ "$plain_payload" -gt 0 ] 2>/dev/null; then
  payload_overhead=$(awk "BEGIN { printf \"%.1f\", ($enc_payload - $plain_payload) / $plain_payload * 100 }")
fi

{
  echo ""
  echo "=== Encryption Overhead Summary ==="
  printf "%-22s %16s\n" "Time overhead" "${time_overhead}%"
  printf "%-22s %16s\n" "Payload size overhead" "${payload_overhead}%"
  echo ""
} | tee -a "$out"

echo "Results written to $out"
echo "Full logs in $LOG"

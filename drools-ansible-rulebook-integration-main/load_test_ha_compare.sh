#!/usr/bin/env bash

# Usage: ./load_test_ha_compare.sh [1k|10k|100k|1m]
#
# Runs each test file with and without --ha flag and compares the results.
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

# 4) Header
header=$(printf "=== HA Performance Comparison (size=%s) ===\n" "$size")
table_header=$(printf "\n%-36s %-8s %14s %9s\n" "File" "Mode" "Memory(bytes)" "Time(ms)")
separator=$(printf "%s" "$(head -c 69 < /dev/zero | tr '\0' '-')")

{
  echo "$header"
  echo "$table_header"
  echo "$separator"
} | tee "$out"

# 5) Run comparison for each file
for file in "$match_file" "$unmatch_file"; do

  # --- noHA run ---
  echo "Running $file (noHA)..."
  noha_stderr=$(java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar "$JAR" "$file" 2>&1 1>/dev/null) || true
  # Parse: "filename, memory, time"
  noha_mem=$(echo "$noha_stderr" | tail -1 | cut -d',' -f2 | tr -d ' ')
  noha_time=$(echo "$noha_stderr" | tail -1 | cut -d',' -f3 | tr -d ' ')

  # --- HA run ---
  echo "Running $file (HA)..."
  ha_stderr=$(java -Xmx512m -Dorg.slf4j.simpleLogger.logFile=System.out \
       -jar "$JAR" "$file" --ha 2>&1 1>/dev/null) || true
  ha_mem=$(echo "$ha_stderr" | tail -1 | cut -d',' -f2 | tr -d ' ')
  ha_time=$(echo "$ha_stderr" | tail -1 | cut -d',' -f3 | tr -d ' ')

  # --- Calculate overhead ---
  if [ "$noha_time" -gt 0 ] 2>/dev/null; then
    overhead=$(awk "BEGIN { printf \"%.1f\", ($ha_time - $noha_time) / $noha_time * 100 }")
  else
    overhead="N/A"
  fi

  # --- Print results ---
  {
    printf "%-36s %-8s %14s %9s\n" "$file" "noHA" "$noha_mem" "$noha_time"
    printf "%-36s %-8s %14s %9s\n" "$file" "HA" "$ha_mem" "$ha_time"
    printf "%-36s %-8s %14s %+8s%%\n" "" "overhead" "" "$overhead"
    echo ""
  } | tee -a "$out"

done

echo "Results written to $out"

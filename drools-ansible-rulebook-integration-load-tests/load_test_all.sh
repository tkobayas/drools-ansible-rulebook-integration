#!/usr/bin/env bash
# Usage: ./load_test_all.sh
#
# Loops 4 sizes x {match, unmatch} x {noHA, HA-PG} = 16 runs.
# Emits one combined result_all.txt (metric lines) and runs MemoryLeakAnalyzer.
# Requires Docker for PostgreSQL.

set -euo pipefail
source "$(dirname "$0")/lib/common.sh"

SIZES=("1k" "10k" "100k" "1m")
SCENARIOS=("match" "unmatch")
MODES=("noHA" "HA-PG")

OUT="result_all.txt"
LOG="out_all.log"
> "$OUT"
> "$LOG"

require_jar
trap pg_cleanup EXIT
pg_setup

for size in "${SIZES[@]}"; do
  for scenario in "${SCENARIOS[@]}"; do
    if [ "$scenario" = "match" ]; then
      file="24kb_${size}_events.json"
    else
      file="24kb_${size}_events_unmatch.json"
    fi
    for mode in "${MODES[@]}"; do
      if [ "$mode" = "noHA" ]; then
        label="$file (noHA)"
        echo "Running $label..."
        jvm_run "$label" "$file"
      else
        pg_truncate
        label="$file (HA-PG)"
        echo "Running $label..."
        jvm_run "$label" "$file" --ha-db-params "$PG_PARAMS"
      fi
      # Append the metric line from this run to result_all.txt.
      # Metric line begins with the events-json name (optionally followed by " (HA-PG)").
      echo "$_run_stderr" | grep "^${file}" | tail -1 >> "$OUT" || echo "$file (${mode}), FAILED, FAILED" >> "$OUT"
    done
  done
done

echo ""
echo "All 16 runs complete. Result lines:"
cat "$OUT"
echo ""

echo "Running MemoryLeakAnalyzer..."
java -cp "target/classes:target/drools-ansible-rulebook-integration-load-tests-jar-with-dependencies.jar" \
     org.drools.ansible.rulebook.integration.loadtests.analyze.MemoryLeakAnalyzer "$OUT"
ANALYZER_EXIT=$?
echo "Analyzer exit code: $ANALYZER_EXIT"
exit $ANALYZER_EXIT

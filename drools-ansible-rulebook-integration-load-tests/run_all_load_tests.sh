#!/usr/bin/env bash
# Usage: ./run_all_load_tests.sh
#
# Runs all 5 batch load test scripts in sequence.
# Requires: Docker, python3, and the fat JAR (built automatically if missing).

set -euo pipefail
cd "$(dirname "$0")"

JAR="target/drools-ansible-rulebook-integration-load-tests-jar-with-dependencies.jar"
if [ ! -f "$JAR" ]; then
  echo "Fat JAR not found. Building..."
  mvn --batch-mode -pl drools-ansible-rulebook-integration-load-tests -am package -DskipTests -f ../pom.xml
fi

SCRIPTS=(
  load_test_match_unmatch_noHA.sh
  load_test_match_unmatch_noHA_HA-PG.sh
  load_test_retention_noHA_HA-PG.sh
  load_test_temporal_HA-PG.sh
  load_test_failover_HA-PG.sh
)

chmod +x "${SCRIPTS[@]}"

FAILED=()

for script in "${SCRIPTS[@]}"; do
  echo ""
  echo "========================================"
  echo "Running $script"
  echo "========================================"
  if ./"$script"; then
    echo "$script: PASSED"
  else
    echo "$script: FAILED (exit $?)"
    FAILED+=("$script")
  fi
done

echo ""
echo "========================================"
echo "Summary"
echo "========================================"
echo "Total: ${#SCRIPTS[@]}"
echo "Passed: $(( ${#SCRIPTS[@]} - ${#FAILED[@]} ))"
echo "Failed: ${#FAILED[@]}"

if [ ${#FAILED[@]} -gt 0 ]; then
  for f in "${FAILED[@]}"; do
    echo "  - $f"
  done
  exit 1
fi

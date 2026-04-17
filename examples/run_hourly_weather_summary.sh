#!/usr/bin/env bash
#
# Run the hourly weather summary pipeline using spark-submit.
#
# Prerequisites:
#   - SPARK_HOME is set and points to a Spark 4.x installation
#   - Delta Lake JARs are available (via --packages or local cache)
#
# Usage:
#   ./examples/run_hourly_weather_summary.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

DELTA_JAR="${DELTA_JAR:-}"
STORAGE_JAR="${STORAGE_JAR:-}"

if [[ -z "$DELTA_JAR" ]]; then
  DELTA_JAR="$(find ~/.ivy2* -name 'delta-spark_4.1_2.13-4.1.0.jar' 2>/dev/null | head -1)"
fi
if [[ -z "$STORAGE_JAR" ]]; then
  STORAGE_JAR="$(find ~/.ivy2* -name 'delta-storage-4.1.0.jar' 2>/dev/null | head -1)"
fi

if [[ -n "$DELTA_JAR" && -n "$STORAGE_JAR" ]]; then
  echo "Using cached JARs:"
  echo "  delta-spark: $DELTA_JAR"
  echo "  delta-storage: $STORAGE_JAR"
  exec "$SPARK_HOME/bin/spark-submit" \
    --jars "$DELTA_JAR,$STORAGE_JAR" \
    "$SCRIPT_DIR/hourly_weather_summary.py"
else
  echo "No local Delta JARs found — downloading via --packages"
  exec "$SPARK_HOME/bin/spark-submit" \
    --packages "io.delta:delta-spark_2.13:4.1.0" \
    "$SCRIPT_DIR/hourly_weather_summary.py"
fi

#!/usr/bin/env bash
# Запуск PySpark-джобы EduFlow (в контейнере Spark или локально).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
JARS_DIR="${ROOT}/jars"
JAR_NAME="clickhouse-jdbc-0.6.0-all.jar"
JAR="${CLICKHOUSE_JDBC_JAR:-$JARS_DIR/$JAR_NAME}"
DRIVER_URL="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar"

mkdir -p "$JARS_DIR"

if [[ ! -f "$JAR" ]]; then
  echo "[run] JDBC not found at $JAR — downloading…"
  TMP="$(mktemp)"
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 5 --retry-delay 2 -o "$TMP" "$DRIVER_URL"
  elif command -v wget >/dev/null 2>&1; then
    wget -O "$TMP" "$DRIVER_URL"
  else
    echo "ERROR: need curl or wget to download JDBC"
    exit 1
  fi
  # если JAR указывает в jars/ — кладём туда; иначе только по CLICKHOUSE_JDBC_JAR
  DEST="$JAR"
  mkdir -p "$(dirname "$DEST")"
  mv "$TMP" "$DEST"
  echo "[run] saved $DEST ($(wc -c < "$DEST") bytes)"
else
  echo "[run] using existing JDBC: $JAR"
fi

export PYTHONPATH="${ROOT}${PYTHONPATH:+:$PYTHONPATH}"
export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
export CLICKHOUSE_HTTP_PORT="${CLICKHOUSE_HTTP_PORT:-8123}"
export CLICKHOUSE_USER="${CLICKHOUSE_USER:-eduflow}"
export CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-eduflow}"
export CLICKHOUSE_DB="${CLICKHOUSE_DB:-eduflow}"
export CLICKHOUSE_JDBC_JAR="$JAR"

# spark-submit: apache/spark → /opt/spark/bin; bitnami → в PATH
if command -v spark-submit >/dev/null 2>&1; then
  SUBMIT=spark-submit
elif [[ -x /opt/spark/bin/spark-submit ]]; then
  SUBMIT=/opt/spark/bin/spark-submit
elif [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SUBMIT="${SPARK_HOME}/bin/spark-submit"
else
  echo "ERROR: spark-submit not found"
  exit 1
fi

echo "[run] host=$CLICKHOUSE_HOST submit=$SUBMIT"

"$SUBMIT" \
  --master "local[*]" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-2g}" \
  --jars "$JAR" \
  --driver-class-path "$JAR" \
  --conf "spark.executor.extraClassPath=$JAR" \
  --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" \
  "${ROOT}/eduflow_metrics/main.py"

echo "[run] finished"

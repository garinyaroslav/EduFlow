#!/usr/bin/env bash
# Запуск Spark-джобы EduFlow внутри контейнера apache/spark.
# Использование:
#   docker compose run --rm spark /opt/eduflow/run.sh
set -euo pipefail

JARS_DIR="/opt/spark/jars-extra"
DRIVER_JAR="${JARS_DIR}/clickhouse-jdbc-0.6.0-all.jar"
DRIVER_URL="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.0/clickhouse-jdbc-0.6.0-all.jar"

mkdir -p "${JARS_DIR}"

if [[ ! -f "${DRIVER_JAR}" ]]; then
    echo "[run] downloading ClickHouse JDBC driver …"
    # jars volume может быть read-only с хоста — тогда качаем во временную
    if touch "${JARS_DIR}/.write_test" 2>/dev/null; then
        rm -f "${JARS_DIR}/.write_test"
        curl -fsSL -o "${DRIVER_JAR}" "${DRIVER_URL}"
    else
        DRIVER_JAR="/tmp/clickhouse-jdbc-0.6.0-all.jar"
        curl -fsSL -o "${DRIVER_JAR}" "${DRIVER_URL}"
    fi
    echo "[run] driver ready: ${DRIVER_JAR}"
fi

echo "[run] starting compute_metrics.py …"
spark-submit \
    --master "local[*]" \
    --driver-memory 2g \
    --executor-memory 2g \
    --jars "${DRIVER_JAR}" \
    --driver-class-path "${DRIVER_JAR}" \
    --conf "spark.executor.extraClassPath=${DRIVER_JAR}" \
    /opt/eduflow/jobs/compute_metrics.py

echo "[run] finished"

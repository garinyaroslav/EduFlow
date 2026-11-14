"""Точка входа: считает все метрики и пишет в ClickHouse."""

from __future__ import annotations

from datetime import datetime, timezone

from eduflow_metrics.config import load_settings
from eduflow_metrics.io_clickhouse import write_table
from eduflow_metrics.metrics import (
    compute_ai_vs_teacher,
    compute_bottleneck_ranking,
    compute_risk_scores,
)
from eduflow_metrics.spark_session import build_spark


def main() -> None:
    settings = load_settings()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[eduflow] JDBC {settings.jdbc_url}")
    print(f"[eduflow] jar   {settings.jdbc_jar}")
    print(f"[eduflow] at    {now.isoformat()}")

    print("[eduflow] 1/3 risk scores …")
    risk = compute_risk_scores(spark, settings, now)
    write_table(risk, settings, "metric_risk_scores")
    print(f"         → {risk.count()} students")

    print("[eduflow] 2/3 bottleneck ranking …")
    bott = compute_bottleneck_ranking(spark, settings, now)
    write_table(bott, settings, "metric_bottleneck_ranking")
    print(f"         → {bott.count()} nodes")

    print("[eduflow] 3/3 AI vs teacher …")
    ai = compute_ai_vs_teacher(spark, settings, now)
    write_table(ai, settings, "metric_ai_vs_teacher")
    print(f"         → {ai.count()} rows")

    print("[eduflow] done")
    spark.stop()


if __name__ == "__main__":
    main()

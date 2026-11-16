"""Точка входа: все метрики считает Spark и пишет в metric_* таблицы."""

from __future__ import annotations

from datetime import datetime, timezone

from eduflow_metrics.config import load_settings
from eduflow_metrics.io_clickhouse import write_table
from eduflow_metrics.metrics import (
    compute_ai_quality_metrics,
    compute_bottleneck_ranking,
    compute_grade_metrics,
    compute_load_metrics,
    compute_progress_metrics,
    compute_risk_scores,
)
from eduflow_metrics.spark_session import build_spark


def _write_many(settings, mapping: dict) -> None:
    for table, df in mapping.items():
        write_table(df, settings, table)
        print(f"         → {table}: {df.count()} rows")


def main() -> None:
    settings = load_settings()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    spark = build_spark(settings)
    spark.sparkContext.setLogLevel("WARN")

    print(f"[eduflow] JDBC {settings.jdbc_url}")
    print(f"[eduflow] jar   {settings.jdbc_jar}")
    print(f"[eduflow] at    {now.isoformat()}")

    print("[eduflow] progress …")
    _write_many(settings, compute_progress_metrics(spark, settings, now))

    print("[eduflow] risk scores …")
    risk = compute_risk_scores(spark, settings, now)
    write_table(risk, settings, "metric_risk_scores")
    print(f"         → metric_risk_scores: {risk.count()} rows")

    print("[eduflow] bottleneck …")
    bott = compute_bottleneck_ranking(spark, settings, now)
    write_table(bott, settings, "metric_bottleneck_ranking")
    print(f"         → metric_bottleneck_ranking: {bott.count()} rows")

    print("[eduflow] grades …")
    _write_many(settings, compute_grade_metrics(spark, settings, now))

    print("[eduflow] AI quality …")
    _write_many(settings, compute_ai_quality_metrics(spark, settings, now))

    print("[eduflow] load …")
    _write_many(settings, compute_load_metrics(spark, settings, now))

    print("[eduflow] done")
    spark.stop()


if __name__ == "__main__":
    main()

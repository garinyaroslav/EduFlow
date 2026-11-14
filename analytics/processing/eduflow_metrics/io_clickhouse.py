"""Чтение/запись таблиц ClickHouse через JDBC."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings

_DATETIME_COLS: dict[str, tuple[str, ...]] = {
    "node_added": ("ts",),
    "student_activity": ("ts",),
    "grade_assigned": ("ts",),
    "grade_corrected": ("ts",),
    "level_changed": ("ts",),
    "teacher_assignment": ("ts",),
    "roadmaps": ("created_at",),
    "metric_risk_scores": ("computed_at",),
    "metric_bottleneck_ranking": ("computed_at",),
    "metric_ai_vs_teacher": ("computed_at",),
}


def _select_sql(table: str) -> str:
    cols = _DATETIME_COLS.get(table)
    if not cols:
        return f"SELECT * FROM {table}"
    parts = ", ".join(f"formatDateTime({c}, '%Y-%m-%d %H:%i:%s') AS {c}" for c in cols)
    return f"SELECT * REPLACE ({parts}) FROM {table}"


def read_table(spark: SparkSession, settings: Settings, table: str) -> DataFrame:
    q = _select_sql(table)
    df = (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("query", q)
        .option("user", settings.ch_user)
        .option("password", settings.ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("fetchsize", "10000")
        .load()
    )
    for c in _DATETIME_COLS.get(table, ()):
        df = df.withColumn(c, F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"))
    return df


def write_table(
    df: DataFrame,
    settings: Settings,
    table: str,
    mode: str = "overwrite",
) -> None:
    (
        df.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.ch_user)
        .option("password", settings.ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "10000")
        .option("isolationLevel", "NONE")
        .mode(mode)
        .save()
    )

"""AI quality aggregates (поверх compute_ai_vs_teacher)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.metrics.ai_vs_teacher import compute_ai_vs_teacher


def compute_ai_quality_metrics(
    spark: SparkSession, settings: Settings, now: datetime
) -> dict[str, DataFrame]:
    full = compute_ai_vs_teacher(spark, settings, now)

    summary = (
        full.filter((F.col("teacher_id") == 0) & (F.col("method") == "ALL"))
        .select(
            "ai_grades",
            "corrected_cnt",
            "correction_rate",
            "avg_abs_delta",
            "computed_at",
        )
    )

    by_teacher = (
        full.filter((F.col("teacher_id") > 0) & (F.col("method") == "ALL"))
        .select(
            "teacher_id",
            "teacher_name",
            "ai_grades",
            "corrected_cnt",
            "correction_rate",
            "avg_abs_delta",
            "computed_at",
        )
    )

    by_method = (
        full.filter((F.col("teacher_id") == 0) & (F.col("method") != "ALL"))
        .select(
            "method",
            "ai_grades",
            "corrected_cnt",
            "correction_rate",
            "avg_abs_delta",
            "computed_at",
        )
    )

    return {
        "metric_ai_vs_teacher": full,
        "metric_ai_correction_summary": summary,
        "metric_ai_correction_by_teacher": by_teacher,
        "metric_ai_correction_by_method": by_method,
    }

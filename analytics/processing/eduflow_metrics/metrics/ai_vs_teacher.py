"""Статистика AI vs преподаватель (корректировки оценок)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table


def compute_ai_vs_teacher(
    spark: SparkSession,
    settings: Settings,
    now: datetime,
) -> DataFrame:
    grade_assigned = read_table(spark, settings, "grade_assigned").filter(
        F.col("source") == "ai"
    )
    grade_corrected = read_table(spark, settings, "grade_corrected")
    roadmaps = read_table(spark, settings, "roadmaps")
    teachers = read_table(spark, settings, "teachers").withColumnRenamed(
        "name", "teacher_name"
    )

    w = Window.partitionBy("roadmap_id", "student_id", "node_id").orderBy(
        F.col("ts").desc(), F.col("attempt").desc()
    )
    ai = (
        grade_assigned.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("roadmap_id", "student_id", "node_id", "method", "grade", "ts")
        .withColumnRenamed("grade", "ai_grade")
        .withColumnRenamed("ts", "ai_ts")
    )

    w2 = Window.partitionBy("roadmap_id", "student_id", "node_id").orderBy(
        F.col("ts").desc()
    )
    corr = (
        grade_corrected.withColumn("rn", F.row_number().over(w2))
        .filter(F.col("rn") == 1)
        .select("roadmap_id", "student_id", "node_id", "new_grade", "ts")
        .withColumnRenamed("ts", "corr_ts")
    )

    joined = (
        ai.join(corr, ["roadmap_id", "student_id", "node_id"], "left")
        .join(roadmaps.select("roadmap_id", "teacher_id"), "roadmap_id", "left")
        .join(teachers, "teacher_id", "left")
        .withColumn("was_corrected", F.col("new_grade").isNotNull())
        .withColumn(
            "delta",
            F.when(
                F.col("was_corrected"),
                F.col("new_grade").cast("int") - F.col("ai_grade").cast("int"),
            ).otherwise(F.lit(0)),
        )
    )

    def agg_block(df: DataFrame, group_cols: list[str]) -> DataFrame:
        return df.groupBy(*group_cols).agg(
            F.count("*").alias("ai_grades"),
            F.sum(F.col("was_corrected").cast("int")).alias("corrected_cnt"),
            F.round(F.avg(F.col("was_corrected").cast("float")), 4).alias(
                "correction_rate"
            ),
            F.round(F.avg(F.when(F.col("was_corrected"), F.col("delta"))), 2).alias(
                "avg_delta"
            ),
            F.round(
                F.avg(F.when(F.col("was_corrected"), F.abs(F.col("delta")))), 2
            ).alias("avg_abs_delta"),
            F.expr(
                "percentile_approx(IF(was_corrected, abs(delta), NULL), 0.5)"
            ).alias("median_abs_delta"),
        )

    by_teacher_method = agg_block(
        joined.filter(F.col("teacher_id").isNotNull()),
        ["teacher_id", "teacher_name", "method"],
    ).withColumn("method", F.col("method").cast("string"))

    by_teacher_all = (
        agg_block(
            joined.filter(F.col("teacher_id").isNotNull()),
            ["teacher_id", "teacher_name"],
        ).withColumn("method", F.lit("ALL"))
    )

    # teacher_id = 0 означает ALL (Nullable в ORDER BY ClickHouse запрещён)
    by_method = (
        agg_block(joined, ["method"])
        .withColumn("teacher_id", F.lit(0).cast("int"))
        .withColumn("teacher_name", F.lit("ALL"))
        .withColumn("method", F.col("method").cast("string"))
    )
    overall = (
        agg_block(joined, [])
        .withColumn("teacher_id", F.lit(0).cast("int"))
        .withColumn("teacher_name", F.lit("ALL"))
        .withColumn("method", F.lit("ALL"))
    )

    return (
        by_teacher_method.unionByName(by_teacher_all)
        .unionByName(by_method)
        .unionByName(overall)
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
        .withColumn("ai_grades", F.col("ai_grades").cast("long"))
        .withColumn("corrected_cnt", F.col("corrected_cnt").cast("long"))
        .withColumn("correction_rate", F.col("correction_rate").cast("float"))
        .withColumn("avg_delta", F.col("avg_delta").cast("float"))
        .withColumn("avg_abs_delta", F.col("avg_abs_delta").cast("float"))
        .withColumn("median_abs_delta", F.col("median_abs_delta").cast("float"))
        .select(
            "teacher_id",
            "teacher_name",
            "method",
            "ai_grades",
            "corrected_cnt",
            "correction_rate",
            "avg_delta",
            "avg_abs_delta",
            "median_abs_delta",
            "computed_at",
        )
    )

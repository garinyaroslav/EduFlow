"""Рейтинг нод-бутылочных горлышек."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table


def compute_bottleneck_ranking(
    spark: SparkSession,
    settings: Settings,
    now: datetime,
) -> DataFrame:
    node_added = read_table(spark, settings, "node_added")
    grade_assigned = read_table(spark, settings, "grade_assigned")
    level_changed = read_table(spark, settings, "level_changed")
    template_nodes = read_table(spark, settings, "template_nodes")
    templates = read_table(spark, settings, "templates").withColumnRenamed(
        "name", "template_name"
    )

    first_grade = (
        grade_assigned.filter(F.col("attempt") == 1)
        .groupBy("roadmap_id", "student_id", "node_id")
        .agg(F.min("ts").alias("first_grade_ts"))
    )
    time_df = (
        node_added.join(first_grade, ["roadmap_id", "student_id", "node_id"], "inner")
        .withColumn("days", F.datediff(F.col("first_grade_ts"), F.col("ts")).cast("float"))
        .filter(F.col("days") >= 0)
    )
    time_stats = time_df.groupBy("node_id").agg(
        F.count("*").alias("samples"),
        F.avg("days").alias("avg_days"),
        F.expr("percentile_approx(days, 0.5)").alias("median_days"),
    )

    w = Window.partitionBy("student_id", "node_id").orderBy(F.col("ts").desc())
    last_level = (
        level_changed.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .filter(F.col("new_level") >= 1)
    )
    grade_stats = last_level.groupBy("node_id").agg(
        F.count("*").alias("graded"),
        F.avg("new_level").alias("avg_level"),
        (
            F.sum(F.when(F.col("new_level") == 1, 1).otherwise(0)) / F.count("*")
        ).alias("low_level_rate"),
    )

    node_meta = template_nodes.join(templates, "template_id", "left").groupBy("node_id").agg(
        F.first("title").alias("node_title"),
        F.first("check_method").alias("check_method"),
        F.first("template_id").alias("template_id"),
        F.first("template_name").alias("template_name"),
    )

    joined = (
        time_stats.join(grade_stats, "node_id", "full_outer")
        .join(node_meta, "node_id", "left")
        .fillna(
            0,
            subset=[
                "samples",
                "avg_days",
                "median_days",
                "graded",
                "avg_level",
                "low_level_rate",
            ],
        )
    )

    scored = joined.withColumn(
        "bottleneck_score",
        (0.4 * F.col("median_days") + 0.6 * F.col("low_level_rate") * 100).cast("float"),
    )
    w_rank = Window.orderBy(F.col("bottleneck_score").desc())

    return (
        scored.withColumn("rank", F.row_number().over(w_rank).cast("int"))
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
        .withColumn("check_method", F.col("check_method").cast("string"))
        .select(
            "node_id",
            "node_title",
            "check_method",
            "template_id",
            "template_name",
            F.col("samples").cast("int"),
            F.col("median_days").cast("float"),
            F.col("avg_days").cast("float"),
            F.col("low_level_rate").cast("float"),
            F.col("avg_level").cast("float"),
            "bottleneck_score",
            "rank",
            "computed_at",
        )
    )

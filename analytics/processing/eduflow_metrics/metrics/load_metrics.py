"""Нагрузка преподавателей → metric_teacher_load, metric_roadmaps_without_teacher."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table


def compute_load_metrics(
    spark: SparkSession, settings: Settings, now: datetime
) -> dict[str, DataFrame]:
    roadmaps = read_table(spark, settings, "roadmaps")
    teachers = read_table(spark, settings, "teachers").withColumnRenamed(
        "name", "teacher_name"
    )
    groups = read_table(spark, settings, "groups").withColumnRenamed("name", "group_name")
    templates = read_table(spark, settings, "templates").withColumnRenamed(
        "name", "template_name"
    )

    load = (
        roadmaps.filter(F.col("teacher_id").isNotNull())
        .groupBy("teacher_id")
        .agg(
            F.count("*").alias("roadmaps_cnt"),
            F.countDistinct("group_id").alias("groups_cnt"),
        )
        .join(teachers, "teacher_id", "left")
        .withColumn("roadmaps_cnt", F.col("roadmaps_cnt").cast("long"))
        .withColumn("groups_cnt", F.col("groups_cnt").cast("long"))
        .withColumn("teacher_id", F.col("teacher_id").cast("int"))
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
        .select(
            "teacher_id",
            "teacher_name",
            "roadmaps_cnt",
            "groups_cnt",
            "computed_at",
        )
    )

    without = (
        roadmaps.filter(F.col("teacher_id").isNull())
        .join(groups, "group_id", "left")
        .join(templates, "template_id", "left")
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
        .select(
            "roadmap_id",
            "group_id",
            "group_name",
            "template_id",
            "template_name",
            "created_at",
            "computed_at",
        )
    )

    return {
        "metric_teacher_load": load,
        "metric_roadmaps_without_teacher": without,
    }

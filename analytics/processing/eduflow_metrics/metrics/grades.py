"""Агрегаты оценок 1..3 → metric_* таблицы."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table
from eduflow_metrics.metrics.final_grades import compute_final_grades


def _now_col(now: datetime):
    return F.lit(now).cast("timestamp")


def compute_grade_metrics(
    spark: SparkSession, settings: Settings, now: datetime
) -> dict[str, DataFrame]:
    final = compute_final_grades(spark, settings)
    nodes = read_table(spark, settings, "template_nodes")
    templates = read_table(spark, settings, "templates").withColumnRenamed(
        "name", "template_name"
    )
    groups = read_table(spark, settings, "groups").withColumnRenamed(
        "name", "group_name"
    )
    teachers = read_table(spark, settings, "teachers").withColumnRenamed(
        "name", "teacher_name"
    )

    with_meta = (
        final.join(
            nodes.select(
                "node_id",
                "title",
                "check_method",
                F.col("template_id").alias("node_template_id"),
            ).withColumnRenamed("title", "node_title"),
            "node_id",
            "left",
        )
        # template_id already on final from roadmaps; prefer node template if needed
        .join(templates, "template_id", "left")
        .join(groups, "group_id", "left")
        .join(teachers, "teacher_id", "left")
    )

    by_method = (
        with_meta.groupBy("method")
        .agg(
            F.count("*").alias("grades_cnt"),
            F.round(F.avg("final_grade"), 2).alias("avg_grade"),
            F.expr("percentile_approx(final_grade, 0.5)").alias("median_grade"),
        )
        .withColumn("median_grade", F.col("median_grade").cast("float"))
        .withColumn("avg_grade", F.col("avg_grade").cast("float"))
        .withColumn("grades_cnt", F.col("grades_cnt").cast("long"))
        .withColumn("computed_at", _now_col(now))
        .withColumn("method", F.col("method").cast("string"))
    )

    by_node = (
        with_meta.groupBy(
            "node_id", "node_title", "check_method", "node_template_id", "template_name"
        )
        .agg(
            F.count("*").alias("grades_cnt"),
            F.round(F.avg("final_grade"), 2).alias("avg_grade"),
        )
        .withColumn("avg_grade", F.col("avg_grade").cast("float"))
        .withColumn("grades_cnt", F.col("grades_cnt").cast("long"))
        .withColumn("check_method", F.col("check_method").cast("string"))
        .withColumn("computed_at", _now_col(now))
    )

    by_tpl = (
        with_meta.groupBy("template_id", "template_name")
        .agg(
            F.count("*").alias("grades_cnt"),
            F.round(F.avg("final_grade"), 2).alias("avg_grade"),
        )
        .withColumn("avg_grade", F.col("avg_grade").cast("float"))
        .withColumn("grades_cnt", F.col("grades_cnt").cast("long"))
        .withColumn("computed_at", _now_col(now))
    )

    by_group = (
        with_meta.groupBy("group_id", "group_name")
        .agg(
            F.count("*").alias("grades_cnt"),
            F.round(F.avg("final_grade"), 2).alias("avg_grade"),
        )
        .withColumn("avg_grade", F.col("avg_grade").cast("float"))
        .withColumn("grades_cnt", F.col("grades_cnt").cast("long"))
        .withColumn("computed_at", _now_col(now))
    )

    by_teacher = (
        with_meta.filter(F.col("teacher_id").isNotNull())
        .groupBy("teacher_id", "teacher_name")
        .agg(
            F.count("*").alias("grades_cnt"),
            F.round(F.avg("final_grade"), 2).alias("avg_grade"),
        )
        .withColumn("avg_grade", F.col("avg_grade").cast("float"))
        .withColumn("grades_cnt", F.col("grades_cnt").cast("long"))
        .withColumn("teacher_id", F.col("teacher_id").cast("int"))
        .withColumn("computed_at", _now_col(now))
    )

    by_method_tpl = (
        with_meta.groupBy("template_id", "template_name", "method")
        .agg(
            F.count("*").alias("grades_cnt"),
            F.round(F.avg("final_grade"), 2).alias("avg_grade"),
            F.expr("percentile_approx(final_grade, 0.5)").alias("median_grade"),
        )
        .withColumn("avg_grade", F.col("avg_grade").cast("float"))
        .withColumn("median_grade", F.col("median_grade").cast("float"))
        .withColumn("grades_cnt", F.col("grades_cnt").cast("long"))
        .withColumn("method", F.col("method").cast("string"))
        .withColumn("computed_at", _now_col(now))
    )

    return {
        "metric_grades_by_method": by_method,
        "metric_avg_grade_by_node": by_node,
        "metric_avg_grade_by_template": by_tpl,
        "metric_avg_grade_by_group": by_group,
        "metric_avg_grade_by_teacher": by_teacher,
        "metric_grades_by_method_template": by_method_tpl,
    }

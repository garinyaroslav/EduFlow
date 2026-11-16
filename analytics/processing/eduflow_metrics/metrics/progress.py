"""Метрики прогресса → metric_* (без SQL views)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table
from eduflow_metrics.metrics.final_grades import compute_final_grades


def compute_progress_metrics(
    spark: SparkSession, settings: Settings, now: datetime
) -> dict[str, DataFrame]:
    level_changed = read_table(spark, settings, "level_changed")
    node_added = read_table(spark, settings, "node_added")
    roadmaps = read_table(spark, settings, "roadmaps")
    groups = read_table(spark, settings, "groups").withColumnRenamed("name", "group_name")
    templates = read_table(spark, settings, "templates").withColumnRenamed(
        "name", "template_name"
    )
    nodes = read_table(spark, settings, "template_nodes")
    students = read_table(spark, settings, "students")
    grade_assigned = read_table(spark, settings, "grade_assigned")

    # последний level на (student, node) — level == grade 1..3
    w = Window.partitionBy("student_id", "node_id").orderBy(F.col("ts").desc())
    last_level = (
        level_changed.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("student_id", "node_id", "roadmap_id", F.col("new_level").alias("level"))
    )

    # --- level distribution ---
    dist = (
        node_added.join(roadmaps.select("roadmap_id", "group_id", "template_id"), "roadmap_id", "left")
        .join(last_level.select("student_id", "node_id", "level"), ["student_id", "node_id"], "left")
        .withColumn("level", F.when(F.col("level").isNull(), F.lit(0)).otherwise(F.col("level")))
        .join(groups, "group_id", "left")
        .join(templates, "template_id", "left")
        .join(
            nodes.select("node_id", "title", "check_method").withColumnRenamed(
                "title", "node_title"
            ),
            "node_id",
            "left",
        )
        .groupBy(
            "group_id",
            "group_name",
            "template_id",
            "template_name",
            "node_id",
            "node_title",
            "check_method",
            "level",
        )
        .agg(F.count("*").alias("students_cnt"))
        .withColumn("students_cnt", F.col("students_cnt").cast("long"))
        .withColumn("level", F.col("level").cast("int"))
        .withColumn("check_method", F.col("check_method").cast("string"))
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
    )

    # --- completion by week ---
    completed = last_level.filter(F.col("level") >= 1).select(
        F.col("student_id").alias("s2"), F.col("node_id").alias("n2")
    )
    completion = (
        node_added.join(roadmaps.select("roadmap_id", "group_id"), "roadmap_id", "left")
        .join(groups, "group_id", "left")
        .withColumn("week_start", F.date_trunc("week", F.col("ts")).cast("date"))
        .join(
            completed,
            (F.col("student_id") == F.col("s2")) & (F.col("node_id") == F.col("n2")),
            "left",
        )
        .groupBy("group_id", "group_name", "week_start")
        .agg(
            F.count("*").alias("nodes_appeared"),
            F.count(F.col("s2")).alias("nodes_completed"),
        )
        .withColumn(
            "completion_rate",
            F.when(F.col("nodes_appeared") == 0, F.lit(0.0)).otherwise(
                F.col("nodes_completed") / F.col("nodes_appeared")
            ).cast("float"),
        )
        .withColumn("nodes_appeared", F.col("nodes_appeared").cast("long"))
        .withColumn("nodes_completed", F.col("nodes_completed").cast("long"))
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
    )

    # --- avg time to first result ---
    first_g = (
        grade_assigned.filter(F.col("attempt") == 1)
        .groupBy("roadmap_id", "student_id", "node_id")
        .agg(F.min("ts").alias("first_grade_ts"))
    )
    time_df = (
        node_added.join(first_g, ["roadmap_id", "student_id", "node_id"], "inner")
        .withColumn("days", F.datediff(F.col("first_grade_ts"), F.col("ts")).cast("float"))
        .filter(F.col("days") >= 0)
        .join(roadmaps.select("roadmap_id", "template_id"), "roadmap_id", "left")
        .join(
            nodes.select("node_id", "title", "check_method").withColumnRenamed(
                "title", "node_title"
            ),
            "node_id",
            "left",
        )
        .join(templates, "template_id", "left")
    )
    avg_time = (
        time_df.groupBy(
            "node_id", "node_title", "check_method", "template_id", "template_name"
        )
        .agg(
            F.count("*").alias("samples"),
            F.round(F.avg("days"), 2).alias("avg_days"),
            F.expr("percentile_approx(days, 0.5)").alias("median_days"),
        )
        .withColumn("samples", F.col("samples").cast("long"))
        .withColumn("avg_days", F.col("avg_days").cast("float"))
        .withColumn("median_days", F.col("median_days").cast("float"))
        .withColumn("check_method", F.col("check_method").cast("string"))
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
    )

    # --- risk by group (from risk scores pipeline inputs: inactive > 21) ---
    activity = read_table(spark, settings, "student_activity")
    last_act = activity.groupBy("student_id").agg(F.max("ts").alias("last_ts"))
    with_days = (
        students.join(last_act, "student_id", "left")
        .withColumn(
            "days_inactive",
            F.when(F.col("last_ts").isNull(), F.lit(9999)).otherwise(
                F.datediff(F.lit(now), F.col("last_ts"))
            ),
        )
        .withColumn("is_risk", (F.col("days_inactive") > 21) | F.col("last_ts").isNull())
        .join(groups, "group_id", "left")
    )
    totals = students.groupBy("group_id").agg(F.count("*").alias("total_students"))
    risk_by_group = (
        with_days.filter(F.col("is_risk"))
        .groupBy("group_id", "group_name")
        .agg(F.count("*").alias("risk_students"))
        .join(totals, "group_id", "inner")
        .withColumn(
            "risk_share",
            (F.col("risk_students") / F.col("total_students")).cast("float"),
        )
        .withColumn("risk_students", F.col("risk_students").cast("long"))
        .withColumn("total_students", F.col("total_students").cast("long"))
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
    )

    return {
        "metric_level_distribution": dist,
        "metric_completion_by_week": completion,
        "metric_avg_time_to_result": avg_time,
        "metric_risk_by_group": risk_by_group,
    }

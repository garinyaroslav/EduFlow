"""Скоринг группы риска по студентам."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table


def compute_risk_scores(
    spark: SparkSession,
    settings: Settings,
    now: datetime,
) -> DataFrame:
    students = read_table(spark, settings, "students")
    groups = read_table(spark, settings, "groups").withColumnRenamed("name", "group_name")
    activity = read_table(spark, settings, "student_activity")
    node_added = read_table(spark, settings, "node_added")
    level_changed = read_table(spark, settings, "level_changed")

    last_act = activity.groupBy("student_id").agg(F.max("ts").alias("last_activity_ts"))
    with_inactive = students.join(last_act, "student_id", "left").withColumn(
        "days_inactive",
        F.when(F.col("last_activity_ts").isNull(), F.lit(9999)).otherwise(
            F.datediff(F.lit(now), F.col("last_activity_ts"))
        ),
    )

    logins = activity.filter(F.col("activity_type") == "login")
    login_cnt_30 = (
        logins.filter(F.col("ts") >= F.date_sub(F.lit(now), 30))
        .groupBy("student_id")
        .agg(F.count("*").alias("login_count_30d"))
    )
    cnt_7 = (
        logins.filter(F.col("ts") >= F.date_sub(F.lit(now), 7))
        .groupBy("student_id")
        .agg(F.count("*").alias("c7"))
    )
    cnt_prev = (
        logins.filter(
            (F.col("ts") >= F.date_sub(F.lit(now), 14))
            & (F.col("ts") < F.date_sub(F.lit(now), 7))
        )
        .groupBy("student_id")
        .agg(F.count("*").alias("c_prev"))
    )
    trend = (
        students.select("student_id")
        .join(cnt_7, "student_id", "left")
        .join(cnt_prev, "student_id", "left")
        .fillna(0, subset=["c7", "c_prev"])
        .withColumn(
            "login_trend",
            F.when(
                F.col("c_prev") == 0,
                F.when(F.col("c7") > 0, F.lit(2.0)).otherwise(F.lit(0.0)),
            ).otherwise(F.col("c7") / F.col("c_prev")),
        )
        .select("student_id", "login_trend")
    )

    nodes_total = node_added.groupBy("student_id").agg(F.count("*").alias("nodes_total"))
    w = Window.partitionBy("student_id", "node_id").orderBy(F.col("ts").desc())
    last_level = (
        level_changed.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("student_id", "node_id", "new_level")
    )
    nodes_completed = (
        last_level.filter(F.col("new_level") >= 1)
        .groupBy("student_id")
        .agg(F.count("*").alias("nodes_completed"))
    )

    df = (
        with_inactive.join(groups, "group_id", "left")
        .join(login_cnt_30, "student_id", "left")
        .join(trend, "student_id", "left")
        .join(nodes_total, "student_id", "left")
        .join(nodes_completed, "student_id", "left")
        .fillna(0, subset=["login_count_30d", "login_trend", "nodes_total", "nodes_completed"])
        .withColumn(
            "skip_rate",
            F.when(F.col("nodes_total") == 0, F.lit(0.0)).otherwise(
                1.0 - F.col("nodes_completed") / F.col("nodes_total")
            ),
        )
    )

    df = (
        df.withColumn(
            "risk_score",
            F.least(
                F.lit(1.0),
                F.greatest(
                    F.lit(0.0),
                    0.35 * F.least(F.col("days_inactive") / 60.0, F.lit(1.0))
                    + 0.25
                    * (1.0 - F.least(F.col("login_count_30d") / 10.0, F.lit(1.0)))
                    + 0.20
                    * F.when(F.col("login_trend") < 1.0, 1.0 - F.col("login_trend")).otherwise(
                        0.0
                    )
                    + 0.20 * F.col("skip_rate"),
                ),
            ).cast("float"),
        )
        .withColumn(
            "risk_bucket",
            F.when(F.col("risk_score") >= 0.7, "high")
            .when(F.col("risk_score") >= 0.4, "medium")
            .otherwise("low"),
        )
        .withColumn("computed_at", F.lit(now).cast("timestamp"))
    )

    return df.select(
        "student_id",
        "group_id",
        "group_name",
        F.col("name").alias("student_name"),
        F.col("days_inactive").cast("int"),
        F.col("login_count_30d").cast("int"),
        F.col("login_trend").cast("float"),
        F.col("nodes_total").cast("int"),
        F.col("nodes_completed").cast("int"),
        F.col("skip_rate").cast("float"),
        "risk_score",
        "risk_bucket",
        "computed_at",
    )

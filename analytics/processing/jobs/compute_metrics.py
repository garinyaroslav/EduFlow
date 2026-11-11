#!/usr/bin/env python3
"""
EduFlow Spark job: читает события из ClickHouse (JDBC), считает метрики,
которые неудобны в чистом SQL, и пишет результат обратно в таблицы metric_*.

Метрики:
  1. Скоринг группы риска (студент)
  2. Рейтинг бутылочных нод
  3. AI vs преподаватель

Запуск (внутри контейнера spark):
  spark-submit \\
    --master local[*] \\
    --jars /opt/spark/jars/clickhouse-jdbc.jar \\
    --driver-class-path /opt/spark/jars/clickhouse-jdbc.jar \\
    /opt/eduflow/jobs/compute_metrics.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, LongType, TimestampType
)

# ---------------------------------------------------------------------------
# Конфиг из env (docker-compose)
# ---------------------------------------------------------------------------
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "eduflow")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "eduflow")
CH_DB = os.getenv("CLICKHOUSE_DB", "eduflow")

JDBC_URL = f"jdbc:clickhouse://{CH_HOST}:{CH_PORT}/{CH_DB}"
JDBC_PROPS = {
    "user": CH_USER,
    "password": CH_PASSWORD,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

INACTIVE_DAYS_THRESHOLD = int(os.getenv("RISK_INACTIVE_DAYS", "21"))
NOW = datetime.now(timezone.utc).replace(tzinfo=None)


def spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("eduflow-metrics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/clickhouse-jdbc.jar")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/clickhouse-jdbc.jar")
        .getOrCreate()
    )


def read_table(spark: SparkSession, table: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .load()
    )


def write_table(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    """Пишем через JDBC. overwrite = TRUNCATE + INSERT (для наших metric_* это ок)."""
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", CH_USER)
        .option("password", CH_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("batchsize", "10000")
        .mode(mode)
        .save()
    )


# ===========================================================================
# 1. Скоринг группы риска
# ===========================================================================
def compute_risk_scores(spark: SparkSession) -> DataFrame:
    students = read_table(spark, "students")
    groups = read_table(spark, "groups").withColumnRenamed("name", "group_name")
    activity = read_table(spark, "student_activity")
    node_added = read_table(spark, "node_added")
    level_changed = read_table(spark, "level_changed")

    # --- давность активности ---
    last_act = (
        activity.groupBy("student_id")
        .agg(F.max("ts").alias("last_activity_ts"))
    )
    with_inactive = (
        students.join(last_act, "student_id", "left")
        .withColumn(
            "days_inactive",
            F.when(F.col("last_activity_ts").isNull(), F.lit(9999))
             .otherwise(F.datediff(F.lit(NOW), F.col("last_activity_ts")))
        )
    )

    # --- динамика входов (login) за 30 дней и тренд 7д / prev 7д ---
    logins = activity.filter(F.col("activity_type") == "login")
    logins_30 = logins.filter(F.col("ts") >= F.date_sub(F.lit(NOW), 30))
    login_cnt_30 = logins_30.groupBy("student_id").agg(F.count("*").alias("login_count_30d"))

    logins_7 = logins.filter(F.col("ts") >= F.date_sub(F.lit(NOW), 7))
    logins_prev7 = logins.filter(
        (F.col("ts") >= F.date_sub(F.lit(NOW), 14)) & (F.col("ts") < F.date_sub(F.lit(NOW), 7))
    )
    cnt_7 = logins_7.groupBy("student_id").agg(F.count("*").alias("c7"))
    cnt_prev = logins_prev7.groupBy("student_id").agg(F.count("*").alias("c_prev"))

    trend = (
        students.select("student_id")
        .join(cnt_7, "student_id", "left")
        .join(cnt_prev, "student_id", "left")
        .fillna(0, subset=["c7", "c_prev"])
        .withColumn(
            "login_trend",
            F.when(F.col("c_prev") == 0, F.when(F.col("c7") > 0, F.lit(2.0)).otherwise(F.lit(0.0)))
             .otherwise(F.col("c7") / F.col("c_prev"))
        )
        .select("student_id", "login_trend")
    )

    # --- доля пропущенных нод (появились, но level так и 0 / нет level) ---
    nodes_total = node_added.groupBy("student_id").agg(F.count("*").alias("nodes_total"))
    # последний level по (student, node)
    w = Window.partitionBy("student_id", "node_id").orderBy(F.col("ts").desc())
    last_level = (
        level_changed
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("student_id", "node_id", "new_level")
    )
    nodes_completed = (
        last_level.filter(F.col("new_level") >= 1)
        .groupBy("student_id")
        .agg(F.count("*").alias("nodes_completed"))
    )

    # --- сборка ---
    df = (
        with_inactive
        .join(groups, "group_id", "left")
        .join(login_cnt_30, "student_id", "left")
        .join(trend, "student_id", "left")
        .join(nodes_total, "student_id", "left")
        .join(nodes_completed, "student_id", "left")
        .fillna(0, subset=["login_count_30d", "login_trend", "nodes_total", "nodes_completed"])
        .withColumn(
            "skip_rate",
            F.when(F.col("nodes_total") == 0, F.lit(0.0))
             .otherwise(1.0 - F.col("nodes_completed") / F.col("nodes_total"))
        )
    )

    # risk_score = взвешенная сумма нормализованных признаков
    # days_inactive: >21 → вклад растёт до 1 при ~60 днях
    # login_count_30d: мало входов → высокий риск
    # login_trend < 1 → спад активности
    # skip_rate → прямой вклад
    df = df.withColumn(
        "risk_score",
        F.least(F.lit(1.0),
            F.greatest(F.lit(0.0),
                0.35 * F.least(F.col("days_inactive") / 60.0, F.lit(1.0))
              + 0.25 * (1.0 - F.least(F.col("login_count_30d") / 10.0, F.lit(1.0)))
              + 0.20 * F.when(F.col("login_trend") < 1.0, 1.0 - F.col("login_trend")).otherwise(0.0)
              + 0.20 * F.col("skip_rate")
            )
        ).cast("float")
    ).withColumn(
        "risk_bucket",
        F.when(F.col("risk_score") >= 0.7, "high")
         .when(F.col("risk_score") >= 0.4, "medium")
         .otherwise("low")
    ).withColumn("computed_at", F.lit(NOW).cast("timestamp"))

    result = df.select(
        "student_id", "group_id", "group_name",
        F.col("name").alias("student_name"),
        F.col("days_inactive").cast("int"),
        F.col("login_count_30d").cast("int"),
        F.col("login_trend").cast("float"),
        F.col("nodes_total").cast("int"),
        F.col("nodes_completed").cast("int"),
        F.col("skip_rate").cast("float"),
        "risk_score", "risk_bucket", "computed_at"
    )
    return result


# ===========================================================================
# 2. Рейтинг бутылочных нод
# ===========================================================================
def compute_bottleneck_ranking(spark: SparkSession) -> DataFrame:
    node_added = read_table(spark, "node_added")
    grade_assigned = read_table(spark, "grade_assigned")
    level_changed = read_table(spark, "level_changed")
    roadmaps = read_table(spark, "roadmaps")
    template_nodes = read_table(spark, "template_nodes")
    templates = read_table(spark, "templates").withColumnRenamed("name", "template_name")

    # время до первой оценки
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

    # доля низких уровней
    w = Window.partitionBy("student_id", "node_id").orderBy(F.col("ts").desc())
    last_level = (
        level_changed.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .filter(F.col("new_level") >= 1)
    )
    grade_stats = last_level.groupBy("node_id").agg(
        F.count("*").alias("graded"),
        F.avg("new_level").alias("avg_level"),
        (F.sum(F.when(F.col("new_level") == 1, 1).otherwise(0)) / F.count("*")).alias("low_level_rate"),
    )

    # привязка к шаблону/названию (берём любой template, где есть эта нода)
    node_meta = (
        template_nodes
        .join(templates, "template_id", "left")
        .groupBy("node_id")
        .agg(
            F.first("title").alias("node_title"),
            F.first("check_method").alias("check_method"),
            F.first("template_id").alias("template_id"),
            F.first("template_name").alias("template_name"),
        )
    )

    joined = (
        time_stats.join(grade_stats, "node_id", "full_outer")
        .join(node_meta, "node_id", "left")
        .fillna(0, subset=["samples", "avg_days", "median_days", "graded", "avg_level", "low_level_rate"])
    )

    scored = joined.withColumn(
        "bottleneck_score",
        (0.4 * F.col("median_days") + 0.6 * F.col("low_level_rate") * 100).cast("float")
    )

    w_rank = Window.orderBy(F.col("bottleneck_score").desc())
    ranked = (
        scored.withColumn("rank", F.row_number().over(w_rank).cast("int"))
        .withColumn("computed_at", F.lit(NOW).cast("timestamp"))
        .withColumn("check_method", F.col("check_method").cast("string"))
        .select(
            "node_id", "node_title", "check_method",
            "template_id", "template_name",
            F.col("samples").cast("int"),
            F.col("median_days").cast("float"),
            F.col("avg_days").cast("float"),
            F.col("low_level_rate").cast("float"),
            F.col("avg_level").cast("float"),
            "bottleneck_score", "rank", "computed_at"
        )
    )
    return ranked


# ===========================================================================
# 3. AI vs преподаватель
# ===========================================================================
def compute_ai_vs_teacher(spark: SparkSession) -> DataFrame:
    grade_assigned = read_table(spark, "grade_assigned").filter(F.col("source") == "ai")
    grade_corrected = read_table(spark, "grade_corrected")
    roadmaps = read_table(spark, "roadmaps")
    teachers = read_table(spark, "teachers").withColumnRenamed("name", "teacher_name")

    # последняя AI-оценка на (roadmap, student, node)
    w = Window.partitionBy("roadmap_id", "student_id", "node_id").orderBy(F.col("ts").desc(), F.col("attempt").desc())
    ai = (
        grade_assigned.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("roadmap_id", "student_id", "node_id", "method", "grade", "ts")
        .withColumnRenamed("grade", "ai_grade")
        .withColumnRenamed("ts", "ai_ts")
    )

    # последняя коррекция
    w2 = Window.partitionBy("roadmap_id", "student_id", "node_id").orderBy(F.col("ts").desc())
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
            F.when(F.col("was_corrected"), F.col("new_grade").cast("int") - F.col("ai_grade").cast("int"))
             .otherwise(F.lit(0))
        )
    )

    def agg_block(df: DataFrame, group_cols: list) -> DataFrame:
        return df.groupBy(*group_cols).agg(
            F.count("*").alias("ai_grades"),
            F.sum(F.col("was_corrected").cast("int")).alias("corrected_cnt"),
            F.round(F.avg(F.col("was_corrected").cast("float")), 4).alias("correction_rate"),
            F.round(F.avg(F.when(F.col("was_corrected"), F.col("delta"))), 2).alias("avg_delta"),
            F.round(F.avg(F.when(F.col("was_corrected"), F.abs(F.col("delta")))), 2).alias("avg_abs_delta"),
            F.expr("percentile_approx(IF(was_corrected, abs(delta), NULL), 0.5)").alias("median_abs_delta"),
        )

    # по преподавателю × method
    by_teacher_method = (
        agg_block(joined.filter(F.col("teacher_id").isNotNull()), ["teacher_id", "teacher_name", "method"])
        .withColumn("method", F.col("method").cast("string"))
    )

    # по преподавателю (все методы)
    by_teacher_all = (
        agg_block(joined.filter(F.col("teacher_id").isNotNull()), ["teacher_id", "teacher_name"])
        .withColumn("method", F.lit("ALL"))
    )

    # общая строка ALL × method
    by_method = (
        agg_block(joined, ["method"])
        .withColumn("teacher_id", F.lit(0).cast("int"))
        .withColumn("teacher_name", F.lit("ALL"))
        .withColumn("method", F.col("method").cast("string"))
    )

    # общая строка ALL / ALL
    overall = (
        agg_block(joined, [])
        .withColumn("teacher_id", F.lit(0).cast("int"))
        .withColumn("teacher_name", F.lit("ALL"))
        .withColumn("method", F.lit("ALL"))
    )

    result = (
        by_teacher_method
        .unionByName(by_teacher_all)
        .unionByName(by_method)
        .unionByName(overall)
        .withColumn("computed_at", F.lit(NOW).cast("timestamp"))
        .withColumn("ai_grades", F.col("ai_grades").cast("long"))
        .withColumn("corrected_cnt", F.col("corrected_cnt").cast("long"))
        .withColumn("correction_rate", F.col("correction_rate").cast("float"))
        .withColumn("avg_delta", F.col("avg_delta").cast("float"))
        .withColumn("avg_abs_delta", F.col("avg_abs_delta").cast("float"))
        .withColumn("median_abs_delta", F.col("median_abs_delta").cast("float"))
        .select(
            "teacher_id", "teacher_name", "method",
            "ai_grades", "corrected_cnt", "correction_rate",
            "avg_delta", "avg_abs_delta", "median_abs_delta", "computed_at"
        )
    )
    return result


# ===========================================================================
# main
# ===========================================================================
def main() -> None:
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[eduflow] ClickHouse JDBC: {JDBC_URL}")
    print(f"[eduflow] computed_at = {NOW.isoformat()}")

    print("[eduflow] 1/3 risk scores …")
    risk = compute_risk_scores(spark)
    write_table(risk, "metric_risk_scores")
    print(f"         → {risk.count()} students")

    print("[eduflow] 2/3 bottleneck ranking …")
    bott = compute_bottleneck_ranking(spark)
    write_table(bott, "metric_bottleneck_ranking")
    print(f"         → {bott.count()} nodes")

    print("[eduflow] 3/3 AI vs teacher …")
    ai = compute_ai_vs_teacher(spark)
    write_table(ai, "metric_ai_vs_teacher")
    print(f"         → {ai.count()} rows")

    print("[eduflow] done")
    spark.stop()


if __name__ == "__main__":
    main()

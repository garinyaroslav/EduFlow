"""Последняя итоговая оценка 1..3 на (roadmap, student, node)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from eduflow_metrics.config import Settings
from eduflow_metrics.io_clickhouse import read_table


def compute_final_grades(spark: SparkSession, settings: Settings) -> DataFrame:
    ga = read_table(spark, settings, "grade_assigned")
    gc = read_table(spark, settings, "grade_corrected")
    roadmaps = read_table(spark, settings, "roadmaps")

    w = Window.partitionBy("roadmap_id", "student_id", "node_id").orderBy(
        F.col("ts").desc(), F.col("attempt").desc()
    )
    last_ga = (
        ga.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "roadmap_id",
            "student_id",
            "node_id",
            "method",
            "source",
            F.col("grade").alias("original_grade"),
            F.col("ts").alias("assigned_ts"),
        )
    )

    w2 = Window.partitionBy("roadmap_id", "student_id", "node_id").orderBy(
        F.col("ts").desc()
    )
    last_gc = (
        gc.withColumn("rn", F.row_number().over(w2))
        .filter(F.col("rn") == 1)
        .select(
            "roadmap_id",
            "student_id",
            "node_id",
            F.col("new_grade").alias("corrected_grade"),
            F.col("ts").alias("corrected_ts"),
        )
    )

    return (
        last_ga.join(last_gc, ["roadmap_id", "student_id", "node_id"], "left")
        .join(roadmaps.select("roadmap_id", "group_id", "template_id", "teacher_id"), "roadmap_id", "left")
        .withColumn(
            "final_grade",
            F.when(F.col("corrected_grade").isNotNull(), F.col("corrected_grade")).otherwise(
                F.col("original_grade")
            ),
        )
        .withColumn("was_corrected", F.col("corrected_grade").isNotNull())
    )

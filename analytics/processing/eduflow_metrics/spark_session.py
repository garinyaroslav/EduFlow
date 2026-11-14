"""Создание SparkSession с JDBC-драйвером ClickHouse."""

from __future__ import annotations

from pyspark.sql import SparkSession

from eduflow_metrics.config import Settings


def build_spark(settings: Settings, app_name: str = "eduflow-metrics") -> SparkSession:
    jar = settings.jdbc_jar
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraClassPath", jar)
        .config("spark.executor.extraClassPath", jar)
        .config("spark.jars", jar)
        .getOrCreate()
    )

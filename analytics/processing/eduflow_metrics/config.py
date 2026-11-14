"""Конфигурация из переменных окружения."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    ch_host: str
    ch_http_port: str
    ch_user: str
    ch_password: str
    ch_db: str
    risk_inactive_days: int
    jdbc_jar: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:clickhouse://{self.ch_host}:{self.ch_http_port}/{self.ch_db}?ssl=false&compress=0"

    @property
    def jdbc_props(self) -> dict[str, str]:
        return {
            "user": self.ch_user,
            "password": self.ch_password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        }


def load_settings() -> Settings:
    # В Docker host = "clickhouse", локально = "localhost"
    return Settings(
        ch_host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        ch_http_port=os.getenv("CLICKHOUSE_HTTP_PORT", "8123"),
        ch_user=os.getenv("CLICKHOUSE_USER", "eduflow"),
        ch_password=os.getenv("CLICKHOUSE_PASSWORD", "eduflow"),
        ch_db=os.getenv("CLICKHOUSE_DB", "eduflow"),
        risk_inactive_days=int(os.getenv("RISK_INACTIVE_DAYS", "21")),
        jdbc_jar=os.getenv(
            "CLICKHOUSE_JDBC_JAR",
            "/opt/eduflow/jars/clickhouse-jdbc-0.6.0-all.jar",
        ),
    )

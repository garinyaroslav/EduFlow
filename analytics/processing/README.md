# EduFlow Processing

PySpark-проект: читает события из ClickHouse, считает метрики, пишет в `metric_*`.

## Структура

```
processing/
├── jars/
│   └── clickhouse-jdbc-0.6.0-all.jar   # драйвер в репозитории — качать не нужно
├── eduflow_metrics/
│   ├── config.py                      # env → Settings
│   ├── spark_session.py
│   ├── io_clickhouse.py               # read/write JDBC
│   ├── main.py                        # точка входа
│   └── metrics/
│       ├── risk.py                    # группа риска
│       ├── bottleneck.py              # бутылочные ноды
│       └── ai_vs_teacher.py           # AI vs преподаватель
└── run.sh
```

## Запуск в Docker (рекомендуется)

Из корня `analytics/`:

```bash
docker compose up -d clickhouse
docker compose run --rm spark bash /opt/eduflow/run.sh
```

Драйвер уже в `jars/`, сеть Docker резолвит хост `clickhouse`.

## Запуск локально (опционально)

```bash
# Java 17+, pip install pyspark
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_JDBC_JAR="$PWD/jars/clickhouse-jdbc-0.6.0-all.jar"
export PYTHONPATH="$PWD"
./run.sh
```

## Результат

| Таблица | Содержимое |
|---------|------------|
| `metric_risk_scores` | risk_score, bucket по студентам |
| `metric_bottleneck_ranking` | rank нод |
| `metric_ai_vs_teacher` | correction_rate; `teacher_id=0` = ALL |

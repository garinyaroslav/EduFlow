# Spark processing (EduFlow)

PySpark-джоба читает события из ClickHouse по JDBC, считает три блока метрик
и пишет результат в таблицы `metric_*`.

## Что считает

| Блок | Таблица-приёмник | Логика |
| ------ | ------------------ | -------- |
| Группа риска | `metric_risk_scores` | days_inactive, login_count_30d, login_trend (7д/prev7д), skip_rate → risk_score ∈ [0,1], bucket low/medium/high |
| Бутылочные ноды | `metric_bottleneck_ranking` | median_days до первой оценки + low_level_rate → bottleneck_score, rank |
| AI vs преподаватель | `metric_ai_vs_teacher` | доля корректировок, avg/median \|Δ\| по teacher × method (+ строки ALL) |

Формула risk_score:

```
0.35 × min(days_inactive/60, 1)
+ 0.25 × (1 − min(login_count_30d/10, 1))
+ 0.20 × max(0, 1 − login_trend)
+ 0.20 × skip_rate
```

Bottleneck score (как в SQL-слое):

```
0.4 × median_days + 0.6 × low_level_rate × 100
```

## Подготовка

1. Применить таблицы-приёмники:

```bash
docker exec -i eduflow-clickhouse clickhouse-client \
  --user eduflow --password eduflow --multiquery \
  < clickhouse/metrics/05_spark_targets.sql
```

1. Данные уже должны быть загружены генератором + SQL-view’ы (пункт 1).

## Запуск

```bash
# из корня analytics/
docker compose up -d clickhouse   # если ещё не поднят
docker compose run --rm spark /opt/eduflow/run.sh
```

Скрипт `run.sh`:

- скачивает ClickHouse JDBC (`clickhouse-jdbc-0.6.0-all.jar`), если его нет;
- вызывает `spark-submit --master local[*]` с драйвером;
- пишет в `metric_risk_scores`, `metric_bottleneck_ranking`, `metric_ai_vs_teacher`.

## Проверка

```sql
SELECT risk_bucket, count() FROM eduflow.metric_risk_scores GROUP BY risk_bucket;

SELECT rank, node_title, bottleneck_score, median_days, low_level_rate
FROM eduflow.metric_bottleneck_ranking ORDER BY rank LIMIT 10;

SELECT teacher_name, method, correction_rate, avg_abs_delta
FROM eduflow.metric_ai_vs_teacher
ORDER BY teacher_id NULLS FIRST, method;
```

## Структура

```
processing/
├── jobs/compute_metrics.py   # PySpark-джоба
├── run.sh                    # обёртка spark-submit + JDBC
├── jars/                     # сюда можно положить драйвер вручную
└── README.md
```

## Замечания для курсовой

- Джоба идемпотентна: каждый запуск перезаписывает `metric_*` (ReplacingMergeTree + overwrite).
- Для замеров «объём → время» запускай на preset `medium` и `large` и фиксируй wall-clock `run.sh`.
- Spark нужен там, где оконные функции + несколько join’ов + кастомный скоринг удобнее выразить в DataFrame API, чем в чистом SQL.

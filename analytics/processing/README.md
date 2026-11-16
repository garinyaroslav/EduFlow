# EduFlow Processing (PySpark)

Вся логика метрик считается в Spark и пишется в таблицы `eduflow.metric_*`.
SQL views для метрик **не используются**.

## Запуск

```bash
docker compose run --rm spark "/opt/eduflow/run.sh"
```

## Модули

| Модуль | Таблицы |
|--------|---------|
| `metrics/progress.py` | level_distribution, completion_by_week, avg_time_to_result, risk_by_group |
| `metrics/risk.py` | risk_scores |
| `metrics/bottleneck.py` | bottleneck_ranking |
| `metrics/grades.py` | grades_by_method, avg_grade_by_* , grades_by_method_template |
| `metrics/ai_quality.py` | ai_vs_teacher, ai_correction_* |
| `metrics/load_metrics.py` | teacher_load, roadmaps_without_teacher |

Схема таблиц: `clickhouse/init/02_metric_tables.sql`.

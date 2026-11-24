# Benchmark — замеры на больших объёмах

 ключевых SQL-запросов и Spark-джобы при росте объёма данных.

| Preset | Студентов | Ожидаемый объём строк |
|--------|-----------|------------------------|
| tiny   | 300       | ~50–80 тыс.            |
| small  | 3 000     | ~0.5 млн               |
| medium | 30 000    | ~5 млн                 |
| large  | 300 000   | ~50 млн                |

## Что измеряется

**Pipeline**

- `generate` — синтетика (`generator/generate.py`)
- `load` — HTTP bulk insert в ClickHouse
- `spark` — полный пересчёт `metric_*` через PySpark JDBC

**SQL** (median по `--repeats`, по умолчанию 3)

- `level_dist` — распределение уровней
- `completion_week` — завершение нод по неделям (join)
- `time_to_result` — время до первого результата
- `bottleneck` — рейтинг «бутылочных» нод
- `risk_group` — группа риска
- `grades_method` / `avg_grade_node` — оценки
- `ai_correction` — доля правок AI
- `teacher_load` — нагрузка преподавателей
- `full_scan_grades` — полный scan фактов

Результаты пишутся в:

- `benchmark/results/*.json`, `*_timings.csv`, `summary.csv`
- ClickHouse: `eduflow.benchmark_runs`, `eduflow.benchmark_timings`
- Grafana: дашборд **EduFlow — Замеры объёмов**

## Запуск

ClickHouse и (для Spark) docker compose должны быть подняты:

```bash
docker compose up -d
```

### Полный цикл medium

```bash
cd benchmark
python run_benchmark.py --preset medium --full --seed 42 --repeats 3
```

### Large (долго: генерация ~8–10 мин + load + spark)

```bash
python run_benchmark.py --preset large --generate --load --sql --seed 7 --repeats 3
# Spark отдельно, если нужно больше памяти:
# SPARK_DRIVER_MEMORY=4g python run_benchmark.py --preset large --spark
```

## Grafana

Дашборд `05_benchmark.json` — таблица прогонов и графики:

- pipeline: generate / load / spark vs total_rows
- SQL: все запросы vs total_rows
- сравнение пресетов (bar)

## Интерпретация для курсовой

1. Таблица `benchmark_runs` / `summary.csv` — сводка по пресетам.
2. График «объём → время» показывает, растёт ли latency линейно или хуже
   (join-запросы вроде `completion_week` / `bottleneck` обычно растут быстрее
   простого `full_scan_grades`).
3. Spark vs pure SQL: Spark читает через JDBC и пишет metric_*; на medium/large
   это доминирующая фаза pipeline — аргумент в пользу материализации метрик.

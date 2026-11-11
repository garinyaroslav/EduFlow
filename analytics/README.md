# EduFlow Analytics

Конвейер аналитики: события → ClickHouse → SQL-метрики → (Spark) → Grafana.

## Структура

```
analytics/
├── clickhouse/
│   ├── init/01_schema.sql          # event-контракт (пишет генератор и будущее приложение)
│   └── metrics/                    # SQL-слой метрик (VIEW)
│       ├── 01_progress.sql
│       ├── 02_grades.sql
│       ├── 03_ai_quality.sql
│       ├── 04_load.sql
│       └── README.md
├── generator/
│   ├── generate.py                 # синтетические данные
│   ├── load.py                     # загрузка CSV → ClickHouse
│   └── verify.py                   # проверка закономерностей
├── grafana/
│   └── provisioning/
│       └── datasources/clickhouse.yml
└── docker-compose.yml
```

## Быстрый старт

```bash
# 1. Поднять ClickHouse + Grafana
docker compose up -d

# 2. Сгенерировать данные (small ≈ 5M строк)
cd generator
python generate.py --preset small --seed 42
python load.py

# 3. Применить слой метрик.
# HTTP не принимает несколько операторов в одном теле (ClickHouse 26).
for f in ../clickhouse/metrics/0*.sql; do
  echo "=== $f ==="
  docker exec -i eduflow-clickhouse clickhouse-client \
    --user eduflow --password eduflow --multiquery < "$f"
done

# 4. Grafana: http://localhost:3000  (admin / admin)
```

## Метрики (слой 1 — SQL)

Подробности и соответствие метрикам курсовой — в `clickhouse/metrics/README.md`.

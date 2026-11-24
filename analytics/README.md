# EduFlow Analytics

```
generator/     → синтетика → ClickHouse
processing/    → PySpark → metric_* (Spark в Docker, JDBC в jars/)
clickhouse/    → schema + metric tables + benchmark (init при первом up)
grafana/       → datasource + dashboards
benchmark/     → замеры объёмов (SQL + Spark) → таблица и график «объём → время»
```

## Старт

```bash
docker compose down -v   # чистый volume, init применит 01–03
docker compose up -d

cd generator
python generate.py --preset small --seed 42 --out out/small
python load.py out/small --truncate
cd ..

# обработка (JDBC уже в processing/jars/)
chmod +x processing/run.sh
docker compose run --rm spark bash /opt/eduflow/run.sh
```

Grafana: <http://localhost:3000> (admin / admin)

## JDBC без скачивания

Файл `processing/jars/clickhouse-jdbc-0.6.0-all.jar` лежит в проекте и монтируется в контейнер.  
`run.sh` только проверяет, что файл есть — сеть для драйвера не нужна.

## Замеры на больших объёмах (benchmark)

Сценарий для курсовой по «Системам обработки больших данных»:
генерируем **medium (~5M строк)** и **large (~50M)**, замеряем время ключевых
SQL-запросов и Spark-джобы, сравниваем. На выходе — таблица и график «объём → время».

```bash
# демо-цифры в ClickHouse (дашборд сразу заполнен)
cd benchmark && python seed_demo.py

# реальный прогон medium: generate + load + sql + spark
python run_benchmark.py --preset medium --full --seed 42 --repeats 3

# только SQL по уже загруженным данным
python run_benchmark.py --preset medium --sql-only --repeats 3

# large (долго)
python run_benchmark.py --preset large --generate --load --sql --seed 7 --repeats 3
```

| Preset | Студентов | Строк (ориентир) |
|--------|-----------|------------------|
| small  | 3 000     | ~0.5 млн         |
| medium | 30 000    | ~5 млн           |
| large  | 300 000   | ~50 млн          |

- Скрипты и описание: `benchmark/README.md`
- Таблицы CH: `benchmark_runs`, `benchmark_timings` (`clickhouse/init/03_benchmark.sql`)
- Дашборд Grafana: **EduFlow — Замеры объёмов** (`05_benchmark.json`)
- Локальные артефакты: `benchmark/results/summary.csv`, `*.json`

# EduFlow Analytics

```
generator/     → синтетика → ClickHouse
processing/    → PySpark → metric_* (Spark в Docker, JDBC в jars/)
clickhouse/    → schema + SQL views + metric tables (init при первом up)
grafana/       → datasource
```

## Старт

```bash
docker compose down -v   # чистый volume, init применит 01–06
docker compose up -d

cd generator
python generate.py --preset small --seed 42
python load.py
cd ..

# обработка (JDBC уже в processing/jars/)
chmod +x processing/run.sh
docker compose run --rm spark bash /opt/eduflow/run.sh
```

Grafana: http://localhost:3000 (admin / admin)

## JDBC без скачивания

Файл `processing/jars/clickhouse-jdbc-0.6.0-all.jar` лежит в проекте и монтируется в контейнер.  
`run.sh` только проверяет, что файл есть — сеть для драйвера не нужна.

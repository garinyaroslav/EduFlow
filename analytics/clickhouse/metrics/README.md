# ClickHouse metrics layer (EduFlow)

SQL-слой поверх event-схемы (`init/01_schema.sql`).  
Все объекты — **VIEW** (не материализованные). Их можно читать напрямую из Grafana
или из Spark. При росте объёма их легко превратить в Materialized View + таблицу.

## Файлы

| Файл | Группа метрик | Views |
|------|---------------|-------|
| `01_progress.sql` | Прогресс | `v_student_node_level`, `v_level_distribution`, `v_completion_by_week`, `v_time_to_first_result`, `v_avg_time_to_result`, `v_bottleneck_nodes`, `v_student_last_activity`, `v_risk_students`, `v_risk_by_group` |
| `02_grades.sql` | Оценки | `v_final_grade`, `v_avg_grade_by_node`, `v_avg_grade_by_template`, `v_avg_grade_by_group`, `v_avg_grade_by_teacher`, `v_grades_by_method`, `v_grades_by_method_template` |
| `03_ai_quality.sql` | Качество AI | `v_ai_grades`, `v_ai_correction_summary`, `v_ai_correction_by_teacher`, `v_ai_correction_by_method`, `v_ai_delta_distribution` |
| `04_load.sql` | Нагрузка | `v_teacher_load`, `v_roadmaps_without_teacher`, `v_teacher_assignment_history`, `v_teacher_overview` |

## Как применить

После того как подняты ClickHouse и загружены данные генератора:

```bash
# из корня analytics/
clickhouse-client --host localhost --user eduflow --password eduflow \
  --multiquery < clickhouse/metrics/01_progress.sql

clickhouse-client --host localhost --user eduflow --password eduflow \
  --multiquery < clickhouse/metrics/02_grades.sql

clickhouse-client --host localhost --user eduflow --password eduflow \
  --multiquery < clickhouse/metrics/03_ai_quality.sql

clickhouse-client --host localhost --user eduflow --password eduflow \
  --multiquery < clickhouse/metrics/04_load.sql
```

Или одной командой:

```bash
for f in clickhouse/metrics/0*.sql; do
  echo "=== $f ==="
  clickhouse-client --host localhost --user eduflow --password eduflow --multiquery < "$f"
done
```

Через HTTP (если client не установлен):

```bash
for f in clickhouse/metrics/0*.sql; do
  curl -s "http://localhost:8123/?user=eduflow&password=eduflow" --data-binary @"$f"
done
```

## Соответствие метрикам курсовой

| Метрика курсовой | View |
|------------------|------|
| Распределение по уровням 1/2/3 (группа, шаблон, нода) | `v_level_distribution` |
| Доля завершённых нод по группам и неделям | `v_completion_by_week` |
| Среднее время до первого результата | `v_time_to_first_result`, `v_avg_time_to_result` |
| Ноды-бутылочные горлышки | `v_bottleneck_nodes` |
| Группа риска (нет активности > N дней) | `v_risk_students`, `v_risk_by_group` |
| Средняя оценка по ноде / шаблону / группе / преподавателю | `v_avg_grade_by_*` |
| Сравнение методов (moodle / test / review) | `v_grades_by_method`, `v_grades_by_method_template` |
| Доля и величина корректировок AI | `v_ai_correction_summary`, `v_ai_correction_by_teacher`, `v_ai_correction_by_method` |
| Нагрузка преподавателей | `v_teacher_load`, `v_teacher_overview` |

## Замечания

- Уровень освоения: `grade < 60 → 1`, `60..79 → 2`, `≥ 80 → 3` (как в генераторе).
- Финальная оценка учитывает последнюю коррекцию преподавателя (`v_final_grade`).
- Группа риска по умолчанию — > 21 дня без активности (можно поменять в Grafana-запросе).
- `bottleneck_score` = 0.4 × median_days + 0.6 × low_level_rate×100. Порог и веса можно подкрутить.

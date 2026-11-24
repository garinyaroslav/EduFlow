-- =============================================================================
-- Результаты бенчмарков (замеры на больших объёмах).
-- Заполняется скриптом benchmark/run_benchmark.py.
-- =============================================================================

CREATE TABLE IF NOT EXISTS eduflow.benchmark_runs
(
    run_id            String,
    preset            LowCardinality(String),
    seed              UInt32,
    students          UInt64,
    total_rows        UInt64,
    weeks             UInt16,
    -- фазы пайплайна, секунды
    generate_sec      Float64,
    load_sec          Float64,
    spark_sec         Float64,
    -- ключевые SQL-запросы, секунды (median по repeats)
    q_level_dist_sec          Float64,
    q_completion_week_sec     Float64,
    q_time_to_result_sec      Float64,
    q_bottleneck_sec          Float64,
    q_risk_group_sec          Float64,
    q_grades_method_sec       Float64,
    q_avg_grade_node_sec      Float64,
    q_ai_correction_sec       Float64,
    q_teacher_load_sec        Float64,
    q_full_scan_grades_sec    Float64,
    -- мета
    repeats           UInt8,
    host_info         String,
    notes             String,
    measured_at       DateTime('UTC')
) ENGINE = MergeTree
ORDER BY (preset, measured_at, run_id);

-- Нормализованный вид: одна строка = один замер операции (удобно для графиков Grafana)
CREATE TABLE IF NOT EXISTS eduflow.benchmark_timings
(
    run_id        String,
    preset        LowCardinality(String),
    total_rows    UInt64,
    students      UInt64,
    operation     LowCardinality(String),   -- generate | load | spark | sql:<name>
    category      LowCardinality(String),   -- pipeline | sql
    elapsed_sec   Float64,
    measured_at   DateTime('UTC')
) ENGINE = MergeTree
ORDER BY (preset, operation, measured_at);

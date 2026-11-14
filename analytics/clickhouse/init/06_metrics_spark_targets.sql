-- =============================================================================
-- Таблицы-приёмники результатов Spark-джобы
-- Spark пишет сюда после расчёта; Grafana и отчёты читают отсюда.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Скоринг группы риска (по студенту)
-- risk_score ∈ [0, 1]: чем выше, тем больше риск «отвала».
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eduflow.metric_risk_scores
(
    student_id          UInt32,
    group_id            UInt32,
    group_name          String,
    student_name        String,
    days_inactive       UInt32,
    login_count_30d     UInt32,
    login_trend         Float32,          -- отношение входов за последние 7д к предыдущим 7д (0 если не было)
    nodes_total         UInt32,
    nodes_completed     UInt32,
    skip_rate           Float32,          -- доля нод без оценки
    risk_score          Float32,          -- итоговый скор [0..1]
    risk_bucket         LowCardinality(String),  -- 'low' | 'medium' | 'high'
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (group_id, student_id);


-- -----------------------------------------------------------------------------
-- 2. Рейтинг бутылочных нод
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eduflow.metric_bottleneck_ranking
(
    node_id             UInt32,
    node_title          String,
    check_method        LowCardinality(String),
    template_id         UInt32,
    template_name       String,
    samples             UInt32,
    median_days         Float32,
    avg_days            Float32,
    low_level_rate      Float32,          -- доля level=1 среди оценивших
    avg_level           Float32,
    bottleneck_score    Float32,          -- чем выше, тем хуже
    rank                UInt32,           -- 1 = самый «узкий»
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (rank, node_id);


-- -----------------------------------------------------------------------------
-- 3. AI vs преподаватель (по преподавателям + итог)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS eduflow.metric_ai_vs_teacher
(
    teacher_id          UInt32,            -- 0 = общая строка «ALL»
    teacher_name        String,
    method              LowCardinality(String),  -- 'test' | 'review' | 'ALL'
    ai_grades           UInt64,
    corrected_cnt       UInt64,
    correction_rate     Float32,
    avg_delta           Float32,          -- средняя правка со знаком (new - ai)
    avg_abs_delta       Float32,
    median_abs_delta    Float32,
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (teacher_id, method);

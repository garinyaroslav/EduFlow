-- =============================================================================
-- Таблицы метрик EduFlow. Логика расчёта только в Spark (processing/).
-- Init создаёт пустые таблицы; Spark делает overwrite после расчёта.
-- =============================================================================

-- ---------- прогресс ----------
CREATE TABLE IF NOT EXISTS eduflow.metric_level_distribution
(
    group_id        UInt32,
    group_name      String,
    template_id     UInt32,
    template_name   String,
    node_id         UInt32,
    node_title      String,
    check_method    LowCardinality(String),
    level           UInt8,
    students_cnt    UInt64,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (group_id, template_id, node_id, level);

CREATE TABLE IF NOT EXISTS eduflow.metric_completion_by_week
(
    group_id          UInt32,
    group_name        String,
    week_start        Date,
    nodes_appeared    UInt64,
    nodes_completed   UInt64,
    completion_rate   Float32,
    computed_at       DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (group_id, week_start);

CREATE TABLE IF NOT EXISTS eduflow.metric_avg_time_to_result
(
    node_id         UInt32,
    node_title      String,
    check_method    LowCardinality(String),
    template_id     UInt32,
    template_name   String,
    samples         UInt64,
    avg_days        Float32,
    median_days     Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (node_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_risk_by_group
(
    group_id          UInt32,
    group_name        String,
    risk_students     UInt64,
    total_students    UInt64,
    risk_share        Float32,
    computed_at       DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (group_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_risk_scores
(
    student_id          UInt32,
    group_id            UInt32,
    group_name          String,
    student_name        String,
    days_inactive       UInt32,
    login_count_30d     UInt32,
    login_trend         Float32,
    nodes_total         UInt32,
    nodes_completed     UInt32,
    skip_rate           Float32,
    risk_score          Float32,
    risk_bucket         LowCardinality(String),
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (group_id, student_id);

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
    low_level_rate      Float32,
    avg_level           Float32,
    bottleneck_score    Float32,
    rank                UInt32,
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (rank, node_id);

-- ---------- оценки (1..3) ----------
CREATE TABLE IF NOT EXISTS eduflow.metric_grades_by_method
(
    method          LowCardinality(String),
    grades_cnt      UInt64,
    avg_grade       Float32,
    median_grade    Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (method);

CREATE TABLE IF NOT EXISTS eduflow.metric_avg_grade_by_node
(
    node_id         UInt32,
    node_title      String,
    check_method    LowCardinality(String),
    template_id     UInt32,
    template_name   String,
    grades_cnt      UInt64,
    avg_grade       Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (node_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_avg_grade_by_template
(
    template_id     UInt32,
    template_name   String,
    grades_cnt      UInt64,
    avg_grade       Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (template_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_avg_grade_by_group
(
    group_id        UInt32,
    group_name      String,
    grades_cnt      UInt64,
    avg_grade       Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (group_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_avg_grade_by_teacher
(
    teacher_id      UInt32,
    teacher_name    String,
    grades_cnt      UInt64,
    avg_grade       Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (teacher_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_grades_by_method_template
(
    template_id     UInt32,
    template_name   String,
    method          LowCardinality(String),
    grades_cnt      UInt64,
    avg_grade       Float32,
    median_grade    Float32,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (template_id, method);

-- ---------- AI ----------
CREATE TABLE IF NOT EXISTS eduflow.metric_ai_correction_summary
(
    ai_grades           UInt64,
    corrected_cnt       UInt64,
    correction_rate     Float32,
    avg_abs_delta       Float32,
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (computed_at);

CREATE TABLE IF NOT EXISTS eduflow.metric_ai_correction_by_teacher
(
    teacher_id          UInt32,
    teacher_name        String,
    ai_grades           UInt64,
    corrected_cnt       UInt64,
    correction_rate     Float32,
    avg_abs_delta       Float32,
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (teacher_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_ai_correction_by_method
(
    method              LowCardinality(String),
    ai_grades           UInt64,
    corrected_cnt       UInt64,
    correction_rate     Float32,
    avg_abs_delta       Float32,
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (method);

CREATE TABLE IF NOT EXISTS eduflow.metric_ai_vs_teacher
(
    teacher_id          UInt32,
    teacher_name        String,
    method              LowCardinality(String),
    ai_grades           UInt64,
    corrected_cnt       UInt64,
    correction_rate     Float32,
    avg_delta           Float32,
    avg_abs_delta       Float32,
    median_abs_delta    Float32,
    computed_at         DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (teacher_id, method);

-- ---------- нагрузка ----------
CREATE TABLE IF NOT EXISTS eduflow.metric_teacher_load
(
    teacher_id      UInt32,
    teacher_name    String,
    roadmaps_cnt    UInt64,
    groups_cnt      UInt64,
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (teacher_id);

CREATE TABLE IF NOT EXISTS eduflow.metric_roadmaps_without_teacher
(
    roadmap_id      UInt32,
    group_id        UInt32,
    group_name      String,
    template_id     UInt32,
    template_name   String,
    created_at      DateTime('UTC'),
    computed_at     DateTime('UTC')
) ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (roadmap_id);

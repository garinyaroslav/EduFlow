-- =============================================================================
-- EduFlow Analytics — метрики прогресса
-- Распределение уровней, доля завершённых нод, время до результата,
-- бутылочные ноды, группа риска.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Текущий уровень освоения студента по ноде
-- Берём последнее событие level_changed на пару (student, node, roadmap).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_student_node_level AS
SELECT
    roadmap_id,
    student_id,
    node_id,
    argMax(new_level, ts) AS level,
    max(ts)               AS last_change_ts
FROM eduflow.level_changed
GROUP BY roadmap_id, student_id, node_id;


-- -----------------------------------------------------------------------------
-- 2. Распределение студентов по уровням (1/2/3) в разрезе группы / шаблона / ноды
-- Уровень 0 = ещё нет результата (нода появилась, но оценки нет).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_level_distribution AS
SELECT
    r.group_id,
    g.name                                    AS group_name,
    r.template_id,
    t.name                                    AS template_name,
    n.node_id,
    tn.title                                  AS node_title,
    tn.check_method,
    if(l.level = 0 OR l.level IS NULL, 0, l.level) AS level,
    count()                                   AS students_cnt
FROM eduflow.node_added AS n
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = n.roadmap_id
LEFT JOIN eduflow.groups AS g
    ON g.group_id = r.group_id
LEFT JOIN eduflow.templates AS t
    ON t.template_id = r.template_id
LEFT JOIN eduflow.template_nodes AS tn
    ON tn.template_id = r.template_id AND tn.node_id = n.node_id
LEFT JOIN eduflow.v_student_node_level AS l
    ON l.roadmap_id = n.roadmap_id
   AND l.student_id = n.student_id
   AND l.node_id    = n.node_id
GROUP BY
    r.group_id, group_name,
    r.template_id, template_name,
    n.node_id, node_title, tn.check_method,
    level;


-- -----------------------------------------------------------------------------
-- 3. Доля завершённых нод по группам и по неделям (динамика)
-- Нода считается завершённой, если текущий уровень >= 1.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_completion_by_week AS
SELECT
    r.group_id,
    g.name                                          AS group_name,
    toStartOfWeek(n.ts, 1)                          AS week_start,   -- понедельник
    count()                                         AS nodes_appeared,
    countIf(l.level >= 1)                           AS nodes_completed,
    round(countIf(l.level >= 1) / count(), 4)        AS completion_rate
FROM eduflow.node_added AS n
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = n.roadmap_id
LEFT JOIN eduflow.groups AS g
    ON g.group_id = r.group_id
LEFT JOIN eduflow.v_student_node_level AS l
    ON l.roadmap_id = n.roadmap_id
   AND l.student_id = n.student_id
   AND l.node_id    = n.node_id
GROUP BY r.group_id, group_name, week_start
ORDER BY r.group_id, week_start;


-- -----------------------------------------------------------------------------
-- 4. Время от появления ноды до первого результата (оценка)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_time_to_first_result AS
SELECT
    n.roadmap_id,
    n.student_id,
    n.node_id,
    n.ts                                            AS appeared_at,
    min(ga.ts)                                      AS first_grade_at,
    dateDiff('hour', n.ts, min(ga.ts))              AS hours_to_result,
    round(dateDiff('hour', n.ts, min(ga.ts)) / 24, 2) AS days_to_result,
    any(ga.method)                                  AS method,
    any(ga.grade)                                   AS first_grade
FROM eduflow.node_added AS n
INNER JOIN eduflow.grade_assigned AS ga
    ON ga.roadmap_id = n.roadmap_id
   AND ga.student_id = n.student_id
   AND ga.node_id    = n.node_id
   AND ga.attempt    = 1
GROUP BY n.roadmap_id, n.student_id, n.node_id, n.ts;


-- Агрегат: среднее / медиана времени до результата по ноде и методу
CREATE OR REPLACE VIEW eduflow.v_avg_time_to_result AS
SELECT
    t.node_id,
    tn.title                                        AS node_title,
    tn.check_method,
    r.template_id,
    tpl.name                                        AS template_name,
    count()                                         AS samples,
    round(avg(t.days_to_result), 2)                 AS avg_days,
    round(quantile(0.5)(t.days_to_result), 2)       AS median_days,
    round(quantile(0.9)(t.days_to_result), 2)       AS p90_days
FROM eduflow.v_time_to_first_result AS t
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = t.roadmap_id
LEFT JOIN eduflow.template_nodes AS tn
    ON tn.template_id = r.template_id AND tn.node_id = t.node_id
LEFT JOIN eduflow.templates AS tpl
    ON tpl.template_id = r.template_id
GROUP BY t.node_id, node_title, tn.check_method, r.template_id, template_name;


-- -----------------------------------------------------------------------------
-- 5. Ноды-«бутылочные горлышки»
-- Комбинируем: долгое время до результата + высокая доля низких оценок (level=1).
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_bottleneck_nodes AS
WITH
    time_stats AS (
        SELECT
            node_id,
            avg(days_to_result) AS avg_days,
            quantile(0.5)(days_to_result) AS median_days,
            count() AS timed_samples
        FROM eduflow.v_time_to_first_result
        GROUP BY node_id
    ),
    grade_stats AS (
        SELECT
            node_id,
            count() AS graded,
            countIf(level = 1) AS low_level_cnt,
            round(countIf(level = 1) / count(), 4) AS low_level_rate,
            round(avg(level), 2) AS avg_level
        FROM eduflow.v_student_node_level
        WHERE level >= 1
        GROUP BY node_id
    )
SELECT
    coalesce(ts.node_id, gs.node_id)                AS node_id,
    any(tn.title)                                   AS node_title,
    any(tn.check_method)                            AS check_method,
    ts.avg_days,
    ts.median_days,
    ts.timed_samples,
    gs.graded,
    gs.low_level_cnt,
    gs.low_level_rate,
    gs.avg_level,
    -- простой score: чем выше, тем больше «бутылочность»
    round(
        coalesce(ts.median_days, 0) * 0.4
      + coalesce(gs.low_level_rate, 0) * 100 * 0.6
    , 2)                                            AS bottleneck_score
FROM time_stats AS ts
FULL OUTER JOIN grade_stats AS gs ON gs.node_id = ts.node_id
LEFT JOIN eduflow.template_nodes AS tn ON tn.node_id = coalesce(ts.node_id, gs.node_id)
GROUP BY
    node_id, ts.avg_days, ts.median_days, ts.timed_samples,
    gs.graded, gs.low_level_cnt, gs.low_level_rate, gs.avg_level
ORDER BY bottleneck_score DESC;


-- -----------------------------------------------------------------------------
-- 6. Группа риска: студенты без активности N дней
-- Параметр N задаём через параметр Grafana или фиксируем 21 день.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_student_last_activity AS
SELECT
    s.student_id                                    AS student_id,
    s.group_id                                      AS group_id,
    g.name                                          AS group_name,
    s.name                                          AS student_name,
    max(a.ts)                                       AS last_activity_ts,
    dateDiff('day', max(a.ts), now('UTC'))          AS days_inactive
FROM eduflow.students AS s
LEFT JOIN eduflow.student_activity AS a
    ON a.student_id = s.student_id
LEFT JOIN eduflow.groups AS g
    ON g.group_id = s.group_id
GROUP BY
    s.student_id,
    s.group_id,
    g.name,
    s.name;


-- Студенты группы риска (по умолчанию > 21 дня без активности)
CREATE OR REPLACE VIEW eduflow.v_risk_students AS
SELECT
    student_id                                      AS student_id,
    group_id                                        AS group_id,
    group_name                                      AS group_name,
    student_name                                    AS student_name,
    last_activity_ts                                AS last_activity_ts,
    days_inactive                                   AS days_inactive
FROM eduflow.v_student_last_activity
WHERE days_inactive > 21
   OR last_activity_ts IS NULL
ORDER BY days_inactive DESC;


-- Сводка группы риска по группам
-- Без коррелированных подзапросов (ClickHouse 26 их не поддерживает в ORDER BY).
CREATE OR REPLACE VIEW eduflow.v_risk_by_group AS
SELECT
    r.group_id                                      AS group_id,
    r.group_name                                    AS group_name,
    count()                                         AS risk_students,
    any(s.total_students)                           AS total_students,
    round(count() / any(s.total_students), 4)       AS risk_share
FROM eduflow.v_risk_students AS r
INNER JOIN (
    SELECT group_id, count() AS total_students
    FROM eduflow.students
    GROUP BY group_id
) AS s ON s.group_id = r.group_id
GROUP BY r.group_id, r.group_name
ORDER BY risk_share DESC;

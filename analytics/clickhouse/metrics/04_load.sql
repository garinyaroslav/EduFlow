-- =============================================================================
-- EduFlow Analytics — нагрузка преподавателей
-- Число roadmap'ов на преподавателя, история назначений.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Текущая нагрузка: сколько roadmap'ов закреплено за преподавателем сейчас
-- (берём актуальное состояние из таблицы roadmaps)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_teacher_load AS
SELECT
    r.teacher_id,
    t.name                                          AS teacher_name,
    count()                                         AS roadmaps_cnt,
    countDistinct(r.group_id)                       AS groups_cnt,
    countDistinct(r.template_id)                    AS templates_cnt
FROM eduflow.roadmaps AS r
LEFT JOIN eduflow.teachers AS t
    ON t.teacher_id = r.teacher_id
WHERE r.teacher_id IS NOT NULL
GROUP BY r.teacher_id, teacher_name
ORDER BY roadmaps_cnt DESC;


-- -----------------------------------------------------------------------------
-- 2. Roadmap'ы без ответственного преподавателя
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_roadmaps_without_teacher AS
SELECT
    r.roadmap_id,
    r.group_id,
    g.name                                          AS group_name,
    r.template_id,
    tpl.name                                        AS template_name,
    r.created_at
FROM eduflow.roadmaps AS r
LEFT JOIN eduflow.groups AS g
    ON g.group_id = r.group_id
LEFT JOIN eduflow.templates AS tpl
    ON tpl.template_id = r.template_id
WHERE r.teacher_id IS NULL
ORDER BY r.created_at;


-- -----------------------------------------------------------------------------
-- 3. История назначений / снятий (для динамики нагрузки)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_teacher_assignment_history AS
SELECT
    ta.roadmap_id,
    ta.teacher_id,
    t.name                                          AS teacher_name,
    ta.action,
    ta.ts,
    r.group_id,
    r.template_id
FROM eduflow.teacher_assignment AS ta
LEFT JOIN eduflow.teachers AS t
    ON t.teacher_id = ta.teacher_id
LEFT JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = ta.roadmap_id
ORDER BY ta.ts;


-- -----------------------------------------------------------------------------
-- 4. Сводка: нагрузка + средняя оценка студентов этого преподавателя
-- (удобно для одного дашборда «преподаватель»)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_teacher_overview AS
SELECT
    l.teacher_id,
    l.teacher_name,
    l.roadmaps_cnt,
    l.groups_cnt,
    l.templates_cnt,
    g.avg_grade                                     AS avg_student_grade,
    g.grades_cnt,
    c.correction_rate                               AS ai_correction_rate,
    c.avg_abs_delta                                 AS ai_avg_abs_delta
FROM eduflow.v_teacher_load AS l
LEFT JOIN eduflow.v_avg_grade_by_teacher AS g
    ON g.teacher_id = l.teacher_id
LEFT JOIN eduflow.v_ai_correction_by_teacher AS c
    ON c.teacher_id = l.teacher_id
ORDER BY l.roadmaps_cnt DESC;

-- =============================================================================
-- EduFlow Analytics — метрики оценок
-- Средние оценки по ноде / шаблону / группе / преподавателю,
-- сравнение способов проверки.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Последняя (финальная) оценка по ноде
-- Берём последнюю запись grade_assigned; если была коррекция — new_grade.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_final_grade AS
SELECT
    ga.roadmap_id,
    ga.student_id,
    ga.node_id,
    ga.method,
    ga.source,
    if(gc.new_grade IS NOT NULL, gc.new_grade, ga.grade) AS final_grade,
    ga.grade                                            AS original_grade,
    gc.new_grade IS NOT NULL                            AS was_corrected,
    ga.ts                                               AS assigned_ts,
    gc.ts                                               AS corrected_ts
FROM (
    SELECT
        roadmap_id, student_id, node_id, method, source, grade, ts,
        row_number() OVER (PARTITION BY roadmap_id, student_id, node_id ORDER BY ts DESC, attempt DESC) AS rn
    FROM eduflow.grade_assigned
) AS ga
LEFT JOIN (
    SELECT
        roadmap_id, student_id, node_id, new_grade, ts,
        row_number() OVER (PARTITION BY roadmap_id, student_id, node_id ORDER BY ts DESC) AS rn
    FROM eduflow.grade_corrected
) AS gc
    ON gc.roadmap_id = ga.roadmap_id
   AND gc.student_id = ga.student_id
   AND gc.node_id    = ga.node_id
   AND gc.rn = 1
WHERE ga.rn = 1;


-- -----------------------------------------------------------------------------
-- 2. Средняя оценка по ноде
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_avg_grade_by_node AS
SELECT
    f.node_id,
    tn.title                                        AS node_title,
    tn.check_method,
    r.template_id,
    tpl.name                                        AS template_name,
    count()                                         AS grades_cnt,
    round(avg(f.final_grade), 2)                    AS avg_grade,
    round(quantile(0.5)(f.final_grade), 1)          AS median_grade,
    min(f.final_grade)                              AS min_grade,
    max(f.final_grade)                              AS max_grade
FROM eduflow.v_final_grade AS f
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = f.roadmap_id
LEFT JOIN eduflow.template_nodes AS tn
    ON tn.template_id = r.template_id AND tn.node_id = f.node_id
LEFT JOIN eduflow.templates AS tpl
    ON tpl.template_id = r.template_id
GROUP BY f.node_id, node_title, tn.check_method, r.template_id, template_name
ORDER BY avg_grade;


-- -----------------------------------------------------------------------------
-- 3. Средняя оценка по шаблону
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_avg_grade_by_template AS
SELECT
    r.template_id,
    tpl.name                                        AS template_name,
    count()                                         AS grades_cnt,
    round(avg(f.final_grade), 2)                    AS avg_grade,
    round(quantile(0.5)(f.final_grade), 1)          AS median_grade
FROM eduflow.v_final_grade AS f
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = f.roadmap_id
LEFT JOIN eduflow.templates AS tpl
    ON tpl.template_id = r.template_id
GROUP BY r.template_id, template_name
ORDER BY avg_grade;


-- -----------------------------------------------------------------------------
-- 4. Средняя оценка по группе
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_avg_grade_by_group AS
SELECT
    r.group_id,
    g.name                                          AS group_name,
    count()                                         AS grades_cnt,
    round(avg(f.final_grade), 2)                    AS avg_grade,
    round(quantile(0.5)(f.final_grade), 1)          AS median_grade
FROM eduflow.v_final_grade AS f
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = f.roadmap_id
LEFT JOIN eduflow.groups AS g
    ON g.group_id = r.group_id
GROUP BY r.group_id, group_name
ORDER BY avg_grade;


-- -----------------------------------------------------------------------------
-- 5. Средняя оценка по преподавателю (ответственному за roadmap)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_avg_grade_by_teacher AS
SELECT
    r.teacher_id,
    t.name                                          AS teacher_name,
    count()                                         AS grades_cnt,
    round(avg(f.final_grade), 2)                    AS avg_grade,
    round(quantile(0.5)(f.final_grade), 1)          AS median_grade
FROM eduflow.v_final_grade AS f
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = f.roadmap_id
LEFT JOIN eduflow.teachers AS t
    ON t.teacher_id = r.teacher_id
WHERE r.teacher_id IS NOT NULL
GROUP BY r.teacher_id, teacher_name
ORDER BY avg_grade;


-- -----------------------------------------------------------------------------
-- 6. Сравнение способов проверки (moodle / test / review)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_grades_by_method AS
SELECT
    f.method,
    count()                                         AS grades_cnt,
    round(avg(f.final_grade), 2)                    AS avg_grade,
    round(quantile(0.5)(f.final_grade), 1)          AS median_grade,
    round(stddevPop(f.final_grade), 2)              AS stddev_grade,
    min(f.final_grade)                              AS min_grade,
    max(f.final_grade)                              AS max_grade,
    countIf(f.final_grade < 60)                     AS low_cnt,      -- level 1
    countIf(f.final_grade BETWEEN 60 AND 79)        AS mid_cnt,      -- level 2
    countIf(f.final_grade >= 80)                    AS high_cnt      -- level 3
FROM eduflow.v_final_grade AS f
GROUP BY f.method
ORDER BY f.method;


-- Детализация: сравнение методов внутри одного шаблона (сопоставимые ноды)
CREATE OR REPLACE VIEW eduflow.v_grades_by_method_template AS
SELECT
    r.template_id,
    tpl.name                                        AS template_name,
    f.method,
    count()                                         AS grades_cnt,
    round(avg(f.final_grade), 2)                    AS avg_grade,
    round(quantile(0.5)(f.final_grade), 1)          AS median_grade
FROM eduflow.v_final_grade AS f
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = f.roadmap_id
LEFT JOIN eduflow.templates AS tpl
    ON tpl.template_id = r.template_id
GROUP BY r.template_id, template_name, f.method
ORDER BY r.template_id, f.method;

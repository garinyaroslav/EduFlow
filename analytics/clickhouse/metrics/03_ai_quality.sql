-- =============================================================================
-- EduFlow Analytics — качество нейросети
-- Доля AI-оценок, которые преподаватель скорректировал,
-- средняя величина правки, разбивка по преподавателям.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Все AI-оценки (source = ai) с информацией о коррекции
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_ai_grades AS
SELECT
    ga.roadmap_id,
    ga.student_id,
    ga.node_id,
    ga.method,                  -- test или review
    ga.grade                    AS ai_grade,
    ga.ts                       AS ai_ts,
    ga.attempt,
    r.teacher_id,
    t.name                      AS teacher_name,
    gc.new_grade                AS corrected_grade,
    gc.ts                       AS correction_ts,
    gc.new_grade IS NOT NULL    AS was_corrected,
    if(gc.new_grade IS NOT NULL, toInt16(gc.new_grade) - toInt16(ga.grade), 0) AS delta
FROM eduflow.grade_assigned AS ga
INNER JOIN eduflow.roadmaps AS r
    ON r.roadmap_id = ga.roadmap_id
LEFT JOIN eduflow.teachers AS t
    ON t.teacher_id = r.teacher_id
LEFT JOIN eduflow.grade_corrected AS gc
    ON gc.roadmap_id = ga.roadmap_id
   AND gc.student_id = ga.student_id
   AND gc.node_id    = ga.node_id
   -- берём коррекцию, произошедшую после этой AI-оценки
   AND gc.ts >= ga.ts
WHERE ga.source = 'ai';


-- -----------------------------------------------------------------------------
-- 2. Сводка: доля корректировок и средняя величина правки (общая)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_ai_correction_summary AS
SELECT
    count()                                         AS ai_grades_total,
    countIf(was_corrected)                          AS corrected_cnt,
    round(countIf(was_corrected) / count(), 4)      AS correction_rate,
    round(avg(if(was_corrected, abs(delta), NULL)), 2) AS avg_abs_delta,
    round(avg(if(was_corrected, delta, NULL)), 2)   AS avg_delta,          -- со знаком
    round(quantile(0.5)(if(was_corrected, abs(delta), NULL)), 1) AS median_abs_delta
FROM eduflow.v_ai_grades;


-- -----------------------------------------------------------------------------
-- 3. Доля и величина корректировок по преподавателям
-- (метрика «склонности» и качества AI глазами конкретного преподавателя)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_ai_correction_by_teacher AS
SELECT
    teacher_id,
    teacher_name,
    count()                                         AS ai_grades,
    countIf(was_corrected)                          AS corrected_cnt,
    round(countIf(was_corrected) / count(), 4)      AS correction_rate,
    round(avg(if(was_corrected, abs(delta), NULL)), 2) AS avg_abs_delta,
    round(avg(if(was_corrected, delta, NULL)), 2)   AS avg_delta
FROM eduflow.v_ai_grades
WHERE teacher_id IS NOT NULL
GROUP BY teacher_id, teacher_name
ORDER BY correction_rate DESC;


-- -----------------------------------------------------------------------------
-- 4. Доля корректировок по методу (test vs review)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_ai_correction_by_method AS
SELECT
    method,
    count()                                         AS ai_grades,
    countIf(was_corrected)                          AS corrected_cnt,
    round(countIf(was_corrected) / count(), 4)      AS correction_rate,
    round(avg(if(was_corrected, abs(delta), NULL)), 2) AS avg_abs_delta,
    round(avg(if(was_corrected, delta, NULL)), 2)   AS avg_delta
FROM eduflow.v_ai_grades
GROUP BY method
ORDER BY method;


-- -----------------------------------------------------------------------------
-- 5. Распределение величины правок (для гистограммы в Grafana)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW eduflow.v_ai_delta_distribution AS
SELECT
    delta,
    count() AS cnt
FROM eduflow.v_ai_grades
WHERE was_corrected
GROUP BY delta
ORDER BY delta;

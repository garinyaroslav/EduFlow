-- Контракт данных аналитики EduFlow.
-- Эту схему пишут и генератор, и (в будущем) само приложение, на любом стеке.
-- Все таблицы лежат в БД eduflow. Время хранится в UTC.

CREATE DATABASE IF NOT EXISTS eduflow;

-- ---------------------------------------------------------------
-- Справочники (dimensions). Приложение отдаёт их снимком/upsert-ом.
-- ---------------------------------------------------------------

CREATE TABLE IF NOT EXISTS eduflow.groups
(
    group_id UInt32,
    name     String
) ENGINE = ReplacingMergeTree ORDER BY group_id;

CREATE TABLE IF NOT EXISTS eduflow.teachers
(
    teacher_id UInt32,
    name       String
) ENGINE = ReplacingMergeTree ORDER BY teacher_id;

CREATE TABLE IF NOT EXISTS eduflow.students
(
    student_id UInt32,
    group_id   UInt32,
    name       String
) ENGINE = ReplacingMergeTree ORDER BY student_id;

CREATE TABLE IF NOT EXISTS eduflow.templates
(
    template_id UInt32,
    name        String
) ENGINE = ReplacingMergeTree ORDER BY template_id;

-- check_method: ровно один способ проверки на ноду
CREATE TABLE IF NOT EXISTS eduflow.template_nodes
(
    template_id    UInt32,
    node_id        UInt32,
    parent_node_id Nullable(UInt32),
    title          String,
    check_method   Enum8('moodle' = 1, 'test' = 2, 'review' = 3)
) ENGINE = ReplacingMergeTree ORDER BY (template_id, node_id);

-- Роадмап группы. teacher_id = NULL, если ответственный не назначен.
CREATE TABLE IF NOT EXISTS eduflow.roadmaps
(
    roadmap_id  UInt32,
    group_id    UInt32,
    template_id UInt32,
    teacher_id  Nullable(UInt32),
    created_at  DateTime('UTC')
) ENGINE = ReplacingMergeTree ORDER BY roadmap_id;

-- ---------------------------------------------------------------
-- События (facts). Только добавление.
-- ---------------------------------------------------------------

-- Нода появилась в персональном роадмапе студента
CREATE TABLE IF NOT EXISTS eduflow.node_added
(
    roadmap_id UInt32,
    student_id UInt32,
    node_id    UInt32,
    ts         DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (roadmap_id, node_id, student_id, ts);

-- Активность студента. node_id = NULL для входа в систему.
CREATE TABLE IF NOT EXISTS eduflow.student_activity
(
    student_id    UInt32,
    roadmap_id    UInt32,
    node_id       Nullable(UInt32),
    activity_type Enum8('login' = 1, 'test_start' = 2, 'review_submit' = 3),
    ts            DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (student_id, ts);

-- Оценка по ноде. source: moodle (импорт) или ai (тест/ревью нейросетью).
-- attempt: номер попытки (для moodle всегда 1).
CREATE TABLE IF NOT EXISTS eduflow.grade_assigned
(
    roadmap_id UInt32,
    student_id UInt32,
    node_id    UInt32,
    method     Enum8('moodle' = 1, 'test' = 2, 'review' = 3),
    source     Enum8('moodle' = 1, 'ai' = 2),
    attempt    UInt8,
    grade      UInt8,   -- итоговая оценка освоения: 1 | 2 | 3
    ts         DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (roadmap_id, node_id, student_id, ts);

-- Преподаватель скорректировал оценку AI
CREATE TABLE IF NOT EXISTS eduflow.grade_corrected
(
    roadmap_id UInt32,
    student_id UInt32,
    node_id    UInt32,
    teacher_id UInt32,
    old_grade  UInt8,  -- 1..3
    new_grade  UInt8,  -- 1..3
    ts         DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (roadmap_id, node_id, student_id, ts);

-- Смена уровня освоения ноды (0 = ещё нет результата; 1 начальный, 2 средний, 3 высокий)
CREATE TABLE IF NOT EXISTS eduflow.level_changed
(
    roadmap_id UInt32,
    student_id UInt32,
    node_id    UInt32,
    old_level  UInt8,  -- совпадает с grade: 1..3
    new_level  UInt8,  -- совпадает с grade: 1..3
    reason     Enum8('grade' = 1, 'correction' = 2),
    ts         DateTime('UTC')
) ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (roadmap_id, node_id, student_id, ts);

-- Назначение / снятие ответственного преподавателя
CREATE TABLE IF NOT EXISTS eduflow.teacher_assignment
(
    roadmap_id UInt32,
    teacher_id UInt32,
    action     Enum8('assigned' = 1, 'unassigned' = 2),
    ts         DateTime('UTC')
) ENGINE = MergeTree
ORDER BY (roadmap_id, ts);

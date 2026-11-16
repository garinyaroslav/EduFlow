#!/usr/bin/env python3
"""Генератор синтетических данных EduFlow для аналитического пайплайна.

Пишет CSV.gz по схеме clickhouse/init/01_schema.sql и ground_truth.json с
заложенными закономерностями, чтобы можно было проверить, что пайплайн их находит.

Заложенные закономерности:
  * у студента скрытая «способность», у ноды «сложность»; у каждого шаблона
    несколько нод-«бутылочных горлышек» (выше сложность -> ниже оценки, дольше путь);
  * итоговая оценка только 1/2/3; методы смещают её: test занижает, review завышает относительно moodle;
  * ответственные преподаватели правят оценки AI с разной склонностью (чаще,
    когда AI сильно ошибается);
  * часть студентов «отваливается» (чаще слабые), после этого активности нет;
  * активность идёт всплесками перед дедлайнами нод;
  * нагрузка на преподавателей неравномерна, у части роадмапов бывают периоды
    без ответственного.

Зависимости: numpy, pandas.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

PRESETS = {
    "tiny": dict(students=300, teachers=6, templates=2, nodes=15),
    "small": dict(students=3_000, teachers=20, templates=4, nodes=20),
    "medium": dict(students=30_000, teachers=60, templates=8, nodes=25),
    "large": dict(students=300_000, teachers=200, templates=12, nodes=30),
}

DAY = 86_400
WEEK = 7 * DAY
METHODS = ["moodle", "test", "review"]
METHOD_P = [0.40, 0.35, 0.25]
# смещение итоговой оценки 1–3 относительно «истинного» уровня (в баллах шкалы 1–3)
METHOD_BIAS = {"moodle": 0.0, "test": -0.35, "review": 0.40}
METHOD_NOISE = {"moodle": 0.45, "test": 0.55, "review": 0.40}
# сдвиг лог-времени до первого результата
METHOD_DELAY = {"moodle": 0.0, "test": -0.2, "review": 0.3}

TABLES = [
    "groups", "teachers", "students", "templates", "template_nodes", "roadmaps",
    "node_added", "student_activity", "grade_assigned", "grade_corrected",
    "level_changed", "teacher_assignment",
]


class Writer:
    """Дописывает DataFrame-ы в gzip-CSV, по одному файлу на таблицу."""

    def __init__(self, out: Path):
        out.mkdir(parents=True, exist_ok=True)
        self.files = {t: gzip.open(out / f"{t}.csv.gz", "wt", newline="") for t in TABLES}
        self.header_done: set[str] = set()
        self.rows = {t: 0 for t in TABLES}

    def write(self, table: str, df: pd.DataFrame) -> None:
        if df is None or len(df) == 0:
            return
        df.to_csv(
            self.files[table], index=False, header=table not in self.header_done,
            na_rep="\\N", date_format="%Y-%m-%d %H:%M:%S",
        )
        self.header_done.add(table)
        self.rows[table] += len(df)

    def close(self) -> None:
        for f in self.files.values():
            f.close()


def ts(arr) -> pd.Series:
    return pd.to_datetime(np.asarray(arr, dtype="int64"), unit="s")


@dataclass
class Ctx:
    t0: int
    weeks: int
    t_end: int


def level_of(grade: np.ndarray) -> np.ndarray:
    """Итоговая оценка и уровень освоения совпадают: {1, 2, 3}."""
    return np.clip(grade, 1, 3).astype(np.int8)


def make_templates(rng, n_templates: int, n_nodes: int):
    """Шаблоны: дерево нод, метод проверки, скрытая сложность."""
    dims_t, dims_n, truth = [], [], {}
    node_id = 1
    templates = []
    for t in range(1, n_templates + 1):
        m = int(max(8, n_nodes + rng.integers(-3, 4)))
        ids = np.arange(node_id, node_id + m)
        node_id += m
        parent = [None] + [int(ids[(i - 1) // 3]) for i in range(1, m)]
        methods = rng.choice(METHODS, size=m, p=METHOD_P)
        diff = rng.normal(0, 0.6, m)
        bott = rng.choice(np.arange(2, m), size=2, replace=False)
        diff[bott] += 1.4
        dims_t.append({"template_id": t, "name": f"Шаблон {t}"})
        for i in range(m):
            dims_n.append({
                "template_id": t, "node_id": int(ids[i]), "parent_node_id": parent[i],
                "title": f"Тема {t}.{i + 1}", "check_method": methods[i],
            })
        templates.append({"id": t, "node_ids": ids, "methods": np.array(methods), "diff": diff})
        truth[f"template_{t}"] = {"bottleneck_node_ids": [int(ids[b]) for b in bott]}
    nodes_df = pd.DataFrame(dims_n)
    nodes_df["parent_node_id"] = nodes_df["parent_node_id"].astype("Int64")
    return pd.DataFrame(dims_t), nodes_df, templates, truth


def simulate_group(rng, ctx: Ctx, gid: int, student_ids: np.ndarray, tpl: dict,
                   teacher_before: int, teacher_after: int | None, gap: tuple[int, int] | None,
                   teacher_prop: dict[int, float]):
    n, m = len(student_ids), len(tpl["node_ids"])
    node_ids, methods, diff = tpl["node_ids"], tpl["methods"], tpl["diff"]
    is_ai = methods != "moodle"
    weeks, t0, t_end = ctx.weeks, ctx.t0, ctx.t_end

    ability = rng.normal(0, 1, n)
    activity = rng.lognormal(0, 0.4, n)
    p_churn = 0.04 + 0.16 / (1 + np.exp(2 * ability))
    churned = rng.random(n) < p_churn
    churn_ts = np.where(churned, t0 + rng.uniform(3, weeks - 1, n) * WEEK, np.inf)
    late = rng.random(n) < 0.05
    join_ts = np.where(late, t0 + rng.uniform(0, 4, n) * WEEK, t0).astype(np.int64)

    # --- node_added ---
    week_of = np.floor(np.arange(m) * (weeks - 3) / m).astype(int)
    release = t0 + week_of * WEEK + 9 * 3600
    added = np.maximum(release[None, :], join_ts[:, None]).astype(np.int64)
    s_idx, i_idx = np.indices((n, m))
    node_added = pd.DataFrame({
        "roadmap_id": gid, "student_id": student_ids[s_idx.ravel()],
        "node_id": node_ids[i_idx.ravel()], "ts": ts(added.ravel()),
    })

    # --- попытки и оценки ---
    bias = np.array([METHOD_BIAS[x] for x in methods])
    noise = np.array([METHOD_NOISE[x] for x in methods])
    mdelay = np.array([METHOD_DELAY[x] for x in methods])
    # «Истинный» уровень на шкале 1–3 (без смещения метода)
    true_base = 2.0 + 0.55 * (ability[:, None] - diff[None, :])

    mu = np.log(3.5) + 0.40 * (diff[None, :] - ability[:, None]) + mdelay[None, :]
    delay = np.clip(np.exp(mu + 0.5 * rng.standard_normal((n, m))), 0.05, 40) * DAY
    cur_ts = added + delay.astype(np.int64)
    skip = rng.random((n, m)) < (0.03 + 0.05 * (diff[None, :] > 1))
    cur_valid = (cur_ts < churn_ts[:, None]) & (cur_ts < t_end) & ~skip

    rows = []
    last_grade = np.zeros((n, m), dtype=np.int16)
    last_ts = np.zeros((n, m), dtype=np.int64)
    last_att = np.zeros((n, m), dtype=np.int8)
    for k in range(3):
        # повторные попытки слегка поднимают ожидаемый уровень
        true_k = true_base + 0.25 * k
        raw = true_k + bias[None, :] + noise[None, :] * rng.standard_normal((n, m))
        grade = np.clip(np.rint(raw), 1, 3).astype(np.int16)
        event_ts = cur_ts.copy()
        # moodle-оценка приходит с задержкой синхронизации
        lag = (rng.exponential(6 * 3600, (n, m))).astype(np.int64)
        event_ts = np.where(is_ai[None, :], event_ts, event_ts + lag)
        ok = cur_valid & (event_ts < t_end)
        si, ii = np.nonzero(ok)
        if len(si):
            rows.append(pd.DataFrame({
                "s": si, "i": ii, "attempt": k + 1, "grade": grade[si, ii].astype(int), "ts": event_ts[si, ii],
            }))
            last_grade[si, ii] = grade[si, ii]
            last_ts[si, ii] = event_ts[si, ii]
            last_att[si, ii] = k + 1
        # пересдача AI, если оценка низкая (1)
        retry = ok & is_ai[None, :] & (grade <= 1) & (rng.random((n, m)) < 0.75)
        cur_ts = cur_ts + (rng.uniform(1, 7, (n, m)) * DAY).astype(np.int64)
        cur_valid = retry & (cur_ts < churn_ts[:, None]) & (cur_ts < t_end)

    ga = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["s", "i", "attempt", "grade", "ts"])
    ga["m"] = methods[ga["i"].to_numpy()]
    grade_assigned = pd.DataFrame({
        "roadmap_id": gid, "student_id": student_ids[ga["s"]], "node_id": node_ids[ga["i"]],
        "method": ga["m"], "source": np.where(ga["m"] == "moodle", "moodle", "ai"),
        "attempt": ga["attempt"].astype("uint8"), "grade": ga["grade"].astype("uint8"), "ts": ts(ga["ts"]),
    })

    # --- корректировки преподавателя: последняя AI-оценка по ноде ---
    si, ii = np.nonzero((last_att > 0) & is_ai[None, :])
    corr_rows = []
    if len(si):
        g_old = last_grade[si, ii].astype(int)
        ref = true_base[si, ii] + 0.25 * (last_att[si, ii] - 1)
        err = np.abs(g_old - ref)
        t_ts = last_ts[si, ii]
        corr_ts = t_ts + (rng.uniform(0.3, 6, len(si)) * DAY).astype(np.int64)
        # попавшие в период без ответственного переносятся на его конец
        teacher = np.full(len(si), teacher_before, dtype=np.int64)
        keep = np.ones(len(si), dtype=bool)
        if gap is not None:
            g0, g1 = gap
            in_gap = (corr_ts >= g0) & (corr_ts < g1)
            corr_ts = np.where(in_gap, g1 + (rng.uniform(0, 2, len(si)) * DAY).astype(np.int64), corr_ts)
            after = corr_ts >= g1
            if teacher_after is None:
                keep &= corr_ts < g0  # ответственного нет до конца периода
            else:
                teacher = np.where(after, teacher_after, teacher)
        prop = np.array([teacher_prop[int(x)] for x in teacher])
        # чаще правят, когда AI «ошибся» относительно истинного уровня
        p = np.minimum(prop * (0.25 + np.minimum(err / 1.5, 1)), 0.95)
        do = (rng.random(len(si)) < p) & keep & (corr_ts < t_end)
        new = np.clip(np.rint(ref + rng.normal(0, 0.35, len(si))), 1, 3).astype(int)
        do &= new != g_old
        corr_rows = np.nonzero(do)[0]
        c = pd.DataFrame({
            "s": si[corr_rows], "i": ii[corr_rows], "teacher_id": teacher[corr_rows],
            "old_grade": g_old[corr_rows], "new_grade": new[corr_rows], "ts": corr_ts[corr_rows],
        })
    else:
        c = pd.DataFrame(columns=["s", "i", "teacher_id", "old_grade", "new_grade", "ts"])
    grade_corrected = pd.DataFrame({
        "roadmap_id": gid, "student_id": student_ids[c["s"].to_numpy(dtype=int)],
        "node_id": node_ids[c["i"].to_numpy(dtype=int)], "teacher_id": c["teacher_id"].astype("uint32"),
        "old_grade": c["old_grade"].astype("uint8"), "new_grade": c["new_grade"].astype("uint8"),
        "ts": ts(c["ts"]),
    })

    # --- смены уровней ---
    ev = pd.concat([
        pd.DataFrame({"s": ga["s"], "i": ga["i"], "grade": ga["grade"], "ts": ga["ts"], "reason": "grade", "ord": 0}),
        pd.DataFrame({"s": c["s"], "i": c["i"], "grade": c["new_grade"], "ts": c["ts"], "reason": "correction", "ord": 1}),
    ], ignore_index=True)
    if len(ev):
        ev["key"] = ev["s"].astype(np.int64) * m + ev["i"]
        ev = ev.sort_values(["key", "ts", "ord"], kind="stable").reset_index(drop=True)
        ev["level"] = level_of(ev["grade"].to_numpy())
        ev["prev"] = ev.groupby("key")["level"].shift(1).fillna(0).astype(int)
        ev = ev[ev["level"] != ev["prev"]]
    level_changed = pd.DataFrame({
        "roadmap_id": gid, "student_id": student_ids[ev["s"].to_numpy(dtype=int)] if len(ev) else [],
        "node_id": node_ids[ev["i"].to_numpy(dtype=int)] if len(ev) else [],
        "old_level": ev["prev"].astype("uint8") if len(ev) else [],
        "new_level": ev["level"].astype("uint8") if len(ev) else [],
        "reason": ev["reason"] if len(ev) else [], "ts": ts(ev["ts"]) if len(ev) else [],
    })

    # --- активность: попытки + фоновые входы с пиками перед дедлайнами ---
    act_parts = []
    att = ga[ga["m"] != "moodle"]
    if len(att):
        offs = np.where(att["m"] == "test", 20 * 60, 2 * 60)
        act_parts.append(pd.DataFrame({
            "student_id": student_ids[att["s"]], "roadmap_id": gid, "node_id": node_ids[att["i"]].astype(float),
            "activity_type": np.where(att["m"] == "test", "test_start", "review_submit"),
            "ts": att["ts"].to_numpy() - offs,
        }))
    deadline_week = np.clip(week_of + 2, 0, weeks - 1)
    spike = np.bincount(deadline_week, minlength=weeks).astype(float)
    lam = 1.5 * activity[:, None] * (1 + 1.5 * spike[None, :])
    wk_start = (np.arange(weeks) * WEEK + t0)[None, :]
    counts = rng.poisson(lam) * (wk_start >= join_ts[:, None] - WEEK)
    sr, wr = np.nonzero(counts)
    rep = counts[sr, wr]
    sr, wr = np.repeat(sr, rep), np.repeat(wr, rep)
    day = rng.choice(7, size=len(sr), p=[.17, .17, .16, .15, .14, .10, .11])
    hour_p = np.array([1, 1, 1, 1, 1, 1, 2, 3, 5, 7, 8, 8, 8, 8, 8, 8, 8, 9, 10, 11, 11, 10, 8, 4], float)
    hour = rng.choice(24, size=len(sr), p=hour_p / hour_p.sum())
    login_ts = t0 + wr * WEEK + day * DAY + hour * 3600 + rng.integers(0, 3600, len(sr))
    ok = (login_ts < churn_ts[sr]) & (login_ts < t_end)
    act_parts.append(pd.DataFrame({
        "student_id": student_ids[sr[ok]], "roadmap_id": gid, "node_id": np.nan,
        "activity_type": "login", "ts": login_ts[ok],
    }))
    act = pd.concat(act_parts, ignore_index=True)
    act["ts"] = ts(act["ts"]) if not np.issubdtype(act["ts"].dtype, np.datetime64) else act["ts"]
    act["node_id"] = act["node_id"].astype("Int64")

    stats = {"churned": int(churned.sum()), "students": n}
    return node_added, act, grade_assigned, grade_corrected, level_changed, stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--preset", choices=PRESETS, default="small")
    ap.add_argument("--students", type=int)
    ap.add_argument("--teachers", type=int)
    ap.add_argument("--templates", type=int)
    ap.add_argument("--nodes", type=int, help="среднее число нод в шаблоне")
    ap.add_argument("--group-size", type=int, default=25)
    ap.add_argument("--weeks", type=int, default=16)
    ap.add_argument("--start-date", default="2026-09-01")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="out")
    a = ap.parse_args()

    cfg = dict(PRESETS[a.preset])
    for k in ("students", "teachers", "templates", "nodes"):
        if getattr(a, k) is not None:
            cfg[k] = getattr(a, k)

    rng = np.random.default_rng(a.seed)
    t0 = int(pd.Timestamp(a.start_date, tz="UTC").timestamp())
    ctx = Ctx(t0=t0, weeks=a.weeks, t_end=t0 + a.weeks * WEEK)
    out = Path(a.out)
    w = Writer(out)
    started = time.time()

    tmpl_df, nodes_df, templates, truth = make_templates(rng, cfg["templates"], cfg["nodes"])
    w.write("templates", tmpl_df)
    w.write("template_nodes", nodes_df)

    n_teachers = cfg["teachers"]
    teachers = pd.DataFrame({"teacher_id": np.arange(1, n_teachers + 1),
                             "name": [f"Преподаватель {i}" for i in range(1, n_teachers + 1)]})
    w.write("teachers", teachers)
    teacher_prop = {int(t): float(np.clip(rng.beta(1.5, 6), 0.01, 0.9)) for t in teachers["teacher_id"]}
    load_w = rng.pareto(2.0, n_teachers) + 1
    load_w /= load_w.sum()

    # группы
    sizes, left = [], cfg["students"]
    while left > 0:
        s = int(np.clip(rng.normal(a.group_size, 5), 8, 45))
        s = min(s, left) if left - s >= 8 else left
        sizes.append(s)
        left -= s
    n_groups = len(sizes)
    tpl_w = rng.dirichlet(np.ones(len(templates)) * 2)

    groups, students, roadmaps, assign = [], [], [], []
    next_student = 1
    totals = {"churned": 0, "students": 0}
    n_unassigned = 0
    for g, size in enumerate(sizes, start=1):
        sids = np.arange(next_student, next_student + size)
        next_student += size
        tpl = templates[int(rng.choice(len(templates), p=tpl_w))]
        tb = int(rng.choice(teachers["teacher_id"], p=load_w))
        ta, gap = None, None
        r = rng.random()
        if r < 0.11:  # период без ответственного
            g0 = ctx.t0 + int(rng.uniform(4, 10) * WEEK)
            if r < 0.03:  # так и остался без ответственного
                gap, ta = (g0, ctx.t_end + WEEK), None
                n_unassigned += 1
            else:
                g1 = g0 + int(rng.uniform(1, 3) * WEEK)
                ta = int(rng.choice(teachers["teacher_id"], p=load_w))
                gap = (g0, g1)
        created = ctx.t0 - DAY
        assign.append({"roadmap_id": g, "teacher_id": tb, "action": "assigned", "ts": created})
        if gap:
            assign.append({"roadmap_id": g, "teacher_id": tb, "action": "unassigned", "ts": gap[0]})
            if ta is not None:
                assign.append({"roadmap_id": g, "teacher_id": ta, "action": "assigned", "ts": gap[1]})
        cur_teacher = (ta if gap and ta is not None else (None if gap else tb))
        groups.append({"group_id": g, "name": f"ИВТ-{g:03d}"})
        students.append(pd.DataFrame({"student_id": sids, "group_id": g, "name": [f"Студент {x}" for x in sids]}))
        roadmaps.append({"roadmap_id": g, "group_id": g, "template_id": tpl["id"],
                         "teacher_id": cur_teacher, "created_at": created})

        na, act, ga, gc, lc, st = simulate_group(rng, ctx, g, sids, tpl, tb, ta, gap, teacher_prop)
        for name, df in (("node_added", na), ("student_activity", act), ("grade_assigned", ga),
                         ("grade_corrected", gc), ("level_changed", lc)):
            w.write(name, df)
        totals["churned"] += st["churned"]
        totals["students"] += st["students"]
        if g % 200 == 0:
            print(f"  группа {g}/{n_groups}, {time.time() - started:.0f}с")

    w.write("groups", pd.DataFrame(groups))
    w.write("students", pd.concat(students, ignore_index=True))
    rm = pd.DataFrame(roadmaps)
    rm["teacher_id"] = rm["teacher_id"].astype("Int64")
    rm["created_at"] = ts(rm["created_at"])
    w.write("roadmaps", rm)
    ta_df = pd.DataFrame(assign)
    ta_df["ts"] = ts(ta_df["ts"])
    w.write("teacher_assignment", ta_df)
    w.close()

    method_of = {int(n): m for t in templates for n, m in zip(t["node_ids"], t["methods"])}
    truth.update({
        "config": {**cfg, "weeks": a.weeks, "start_date": a.start_date, "seed": a.seed, "groups": n_groups},
        "method_bias": METHOD_BIAS,
        "teacher_correction_propensity": teacher_prop,
        "churned_students": totals["churned"],
        "roadmaps_without_teacher_at_end": n_unassigned,
        "node_methods": {str(k): v for k, v in method_of.items()},
        "level_rule": "grade == level ∈ {1,2,3}; grade is the final mastery score",
    })
    (out / "ground_truth.json").write_text(json.dumps(truth, ensure_ascii=False, indent=1))

    total = sum(w.rows.values())
    print(f"Готово за {time.time() - started:.1f}с, всего строк: {total:,}")
    for t in TABLES:
        print(f"  {t:18s} {w.rows[t]:>12,}")


if __name__ == "__main__":
    main()

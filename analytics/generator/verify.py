#!/usr/bin/env python3
"""Проверка сгенерированных данных: целостность и наличие заложенных эффектов.
Использование: python3 -I verify.py <папка с csv.gz и ground_truth.json>
"""
import json
import sys
from pathlib import Path

import pandas as pd

d = Path(sys.argv[1])
rd = lambda t: pd.read_csv(d / f"{t}.csv.gz", na_values=["\\N"], parse_dates=[c for c in ("ts", "created_at")] if False else None)
T = {t: rd(t) for t in ["students", "roadmaps", "template_nodes", "teachers", "node_added",
                        "student_activity", "grade_assigned", "grade_corrected", "level_changed",
                        "teacher_assignment"]}
for t in ("node_added", "student_activity", "grade_assigned", "grade_corrected", "level_changed", "teacher_assignment"):
    T[t]["ts"] = pd.to_datetime(T[t]["ts"])
truth = json.loads((d / "ground_truth.json").read_text())
ok = True


def check(name, cond, info=""):
    global ok
    ok &= bool(cond)
    print(f"[{'OK' if cond else 'FAIL'}] {name} {info}")


ga, na, gc, lc = T["grade_assigned"], T["node_added"], T["grade_corrected"], T["level_changed"]

# --- целостность ---
check("оценки только по добавленным нодам",
      len(ga.merge(na, on=["student_id", "node_id"], how="left", indicator=True).query("_merge=='left_only'")) == 0)
first = ga.groupby(["student_id", "node_id"])["ts"].min().reset_index().merge(na, on=["student_id", "node_id"], suffixes=("_g", "_a"))
check("первая оценка не раньше появления ноды", (first["ts_g"] >= first["ts_a"]).all())
check("корректировки только по существующим оценкам ai",
      len(gc.merge(ga[ga.source == "ai"], on=["student_id", "node_id"], how="left", indicator=True).query("_merge=='left_only'")) == 0)
check("оценки в 0..100", ga.grade.between(0, 100).all() and gc.new_grade.between(0, 100).all())
check("moodle не корректируется",
      len(gc.merge(ga[ga.source == "moodle"], on=["student_id", "node_id"])) == 0)
check("уровни согласованы с цепочкой (old->new без пропусков)",
      lc.sort_values(["student_id", "node_id", "ts"]).groupby(["student_id", "node_id"]).apply(
          lambda g: (g["old_level"].iloc[1:].to_numpy() == g["new_level"].iloc[:-1].to_numpy()).all()).all())

# --- закономерности ---
meth = {int(k): v for k, v in truth["node_methods"].items()}
ga["method"] = ga["node_id"].map(meth)
by_m = ga[ga.attempt == 1].groupby("method")["grade"].mean().round(1)
print("\nСредняя оценка 1-й попытки по методам:", by_m.to_dict())
check("test < moodle < review", by_m["test"] < by_m["moodle"] < by_m["review"])

bott = {n for k, v in truth.items() if k.startswith("template_") for n in v["bottleneck_node_ids"]}
ga["is_bott"] = ga["node_id"].isin(bott)
mb = ga[ga.attempt == 1].groupby("is_bott")["grade"].mean().round(1)
print("Средняя оценка: бутылочные горлышки vs остальные:", mb.to_dict())
check("на бутылочных нодах оценки ниже", mb[True] < mb[False] - 5)

d = first.assign(days=(first["ts_g"] - first["ts_a"]).dt.total_seconds() / DAY if (DAY := 86400) else 0)
d["is_bott"] = d["node_id"].isin(bott)
md = d.groupby("is_bott")["days"].median().round(1)
print("Медиана дней до 1-го результата:", md.to_dict())
check("на бутылочных нодах путь дольше", md[True] > md[False])

ai_final = ga[ga.source == "ai"].sort_values("attempt").groupby(["student_id", "node_id"]).tail(1)
ai_final = ai_final.merge(T["roadmaps"][["roadmap_id", "teacher_id"]], on="roadmap_id")
corr_by_t = gc.groupby("teacher_id").size()
all_by_t = ai_final.groupby("teacher_id").size()
rate = (corr_by_t / all_by_t).dropna()
prop = pd.Series({int(k): v for k, v in truth["teacher_correction_propensity"].items()})
j = pd.concat([rate.rename("rate"), prop.rename("prop")], axis=1).dropna()
r = j.corr().loc["rate", "prop"]
print(f"\nДоля корректировок AI в целом: {len(gc) / len(ai_final):.1%}; "
      f"корреляция доли по преподавателям с заложенной склонностью: {r:.2f}")
check("доля правок коррелирует со склонностью преподавателя", r > 0.5)

act = T["student_activity"]
act["dow"] = act.ts.dt.dayofweek
act["week"] = ((act.ts - pd.Timestamp(truth["config"]["start_date"])).dt.days // 7)
wk = act[act.activity_type == "login"].groupby("week").size()
print("Входы по неделям:", wk.to_dict())
check("входы неравномерны по неделям (пики)", wk.max() / wk.min() > 1.3)

last_act = act.groupby("student_id")["ts"].max()
horizon = pd.Timestamp(truth["config"]["start_date"]) + pd.Timedelta(weeks=truth["config"]["weeks"])
inactive = (horizon - last_act > pd.Timedelta(days=21)).sum() + (len(T["students"]) - last_act.size)
print(f"Студентов без активности последние 21 день: {inactive} из {len(T['students'])} "
      f"(заложено «отвалившихся»: {truth['churned_students']})")
check("«группа риска» близка к заложенному числу отвалившихся",
      abs(inactive - truth["churned_students"]) <= 0.4 * truth["churned_students"] + 10)

load = T["roadmaps"].dropna(subset=["teacher_id"]).groupby("teacher_id").size()
print("Роадмапов на преподавателя (мин/макс):", int(load.min()), int(load.max()))
print("\nИТОГ:", "всё в порядке" if ok else "есть расхождения")
sys.exit(0 if ok else 1)

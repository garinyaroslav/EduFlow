#!/usr/bin/env python3
"""Замеры EduFlow Analytics на разных объёмах данных.

Сценарии (preset):
  small   ~ 3k студентов  → порядка сотен тысяч строк
  medium  ~ 30k студентов → ~5 млн строк
  large   ~ 300k студентов → ~50 млн строк

Что измеряется:
  1) генерация CSV (generate.py) — опционально
  2) загрузка в ClickHouse (load.py) — опционально
  3) ключевые SQL-запросы (median по --repeats)
  4) полный Spark-пайплайн метрик (docker compose run spark …) — опционально

Результаты:
  * JSON + CSV в benchmark/results/
  * таблицы eduflow.benchmark_runs и eduflow.benchmark_timings (если CH доступен)

Примеры:
  # только SQL по уже загруженным данным (preset берётся из счётчика строк)
  python3 run_benchmark.py --preset medium --sql-only --repeats 3

  # полный цикл: generate → load → sql → spark
  python3 run_benchmark.py --preset medium --full --seed 42 --repeats 3

  # large без Spark (долго)
  python3 run_benchmark.py --preset large --generate --load --sql --seed 7
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

from queries import QUERIES

ROOT = Path(__file__).resolve().parents[1]
GEN_DIR = ROOT / "generator"
BENCH_DIR = ROOT / "benchmark"
RESULTS_DIR = BENCH_DIR / "results"

URL = os.environ.get("CH_URL", "http://localhost:8123")
USER = os.environ.get("CH_USER", "eduflow")
PASSWORD = os.environ.get("CH_PASSWORD", "eduflow")
DB = os.environ.get("CH_DB", "eduflow")
AUTH = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()

PRESET_STUDENTS = {
    "tiny": 300,
    "small": 3_000,
    "medium": 30_000,
    "large": 300_000,
}


def ch_request(query: str, body: bytes | None = None, timeout: int = 600) -> str:
    headers = {"Authorization": AUTH}
    req = urllib.request.Request(
        f"{URL}/?{urllib.parse.urlencode({'query': query})}",
        data=body if body is not None else b"",
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode()


def ch_ok() -> bool:
    try:
        return ch_request("SELECT 1").strip() == "1"
    except Exception as e:
        print(f"[ch] недоступен ({URL}): {e}")
        return False


def ensure_benchmark_tables() -> None:
    sql_path = ROOT / "clickhouse" / "init" / "03_benchmark.sql"
    text = sql_path.read_text()
    # простой сплит по ; с игнором комментариев-блоков
    for stmt in text.split(";"):
        s = "\n".join(
            ln for ln in stmt.splitlines()
            if not ln.strip().startswith("--")
        ).strip()
        if s:
            ch_request(s)


def count_total_rows() -> int:
    tables = [
        "groups", "teachers", "students", "templates", "template_nodes", "roadmaps",
        "node_added", "student_activity", "grade_assigned", "grade_corrected",
        "level_changed", "teacher_assignment",
    ]
    total = 0
    for t in tables:
        try:
            total += int(ch_request(f"SELECT count() FROM {DB}.{t}").strip())
        except Exception:
            pass
    return total


def count_students() -> int:
    try:
        return int(ch_request(f"SELECT count() FROM {DB}.students").strip())
    except Exception:
        return 0


def time_sql(name: str, sql: str, repeats: int) -> float:
    """Возвращает median секунд. FORMAT Null — не читаем результат."""
    q = sql.strip().rstrip(";") + " FORMAT Null"
    times: list[float] = []
    for i in range(repeats):
        t0 = time.perf_counter()
        ch_request(q)
        dt = time.perf_counter() - t0
        times.append(dt)
        print(f"    {name:22s}  try {i + 1}/{repeats}: {dt:.3f}s")
    return statistics.median(times)


def run_generate(preset: str, seed: int, out: Path, weeks: int) -> float:
    out.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, str(GEN_DIR / "generate.py"),
        "--preset", preset,
        "--seed", str(seed),
        "--weeks", str(weeks),
        "--out", str(out),
    ]
    print(f"[generate] {' '.join(cmd)}")
    t0 = time.perf_counter()
    subprocess.run(cmd, check=True, cwd=str(GEN_DIR))
    return time.perf_counter() - t0


def run_load(data_dir: Path, truncate: bool = True) -> float:
    cmd = [sys.executable, str(GEN_DIR / "load.py"), str(data_dir)]
    if truncate:
        cmd.append("--truncate")
    print(f"[load] {' '.join(cmd)}")
    t0 = time.perf_counter()
    subprocess.run(cmd, check=True, cwd=str(GEN_DIR))
    return time.perf_counter() - t0


def run_spark() -> float:
    """Запуск через docker compose (как в README)."""
    cmd = [
        "docker", "compose", "run", "--rm", "spark", "/opt/eduflow/run.sh",
    ]
    print(f"[spark] {' '.join(cmd)}")
    t0 = time.perf_counter()
    subprocess.run(cmd, check=True, cwd=str(ROOT))
    return time.perf_counter() - t0


def save_local(run: dict, timings: list[dict]) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    rid = run["run_id"]
    (RESULTS_DIR / f"{rid}.json").write_text(
        json.dumps({"run": run, "timings": timings}, ensure_ascii=False, indent=2)
    )
    csv_path = RESULTS_DIR / f"{rid}_timings.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["run_id", "preset", "total_rows", "students", "operation", "category", "elapsed_sec", "measured_at"],
        )
        w.writeheader()
        for row in timings:
            w.writerow(row)
    # накопительный summary
    summary = RESULTS_DIR / "summary.csv"
    write_header = not summary.exists()
    with summary.open("a", newline="", encoding="utf-8") as f:
        fields = list(run.keys())
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            w.writeheader()
        w.writerow(run)
    print(f"[save] {RESULTS_DIR / (rid + '.json')}")
    print(f"[save] {csv_path}")
    print(f"[save] {summary}")


def insert_ch(run: dict, timings: list[dict]) -> None:
    ensure_benchmark_tables()
    # benchmark_runs — одна строка
    cols = [
        "run_id", "preset", "seed", "students", "total_rows", "weeks",
        "generate_sec", "load_sec", "spark_sec",
        "q_level_dist_sec", "q_completion_week_sec", "q_time_to_result_sec",
        "q_bottleneck_sec", "q_risk_group_sec", "q_grades_method_sec",
        "q_avg_grade_node_sec", "q_ai_correction_sec", "q_teacher_load_sec",
        "q_full_scan_grades_sec",
        "repeats", "host_info", "notes", "measured_at",
    ]
    values = []
    for c in cols:
        v = run.get(c, 0 if c.endswith("_sec") or c in ("seed", "students", "total_rows", "weeks", "repeats") else "")
        if isinstance(v, str):
            values.append("'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'")
        elif v is None:
            values.append("0")
        else:
            values.append(str(v))
    ch_request(
        f"INSERT INTO {DB}.benchmark_runs ({', '.join(cols)}) VALUES ({', '.join(values)})"
    )
    # timings
    for t in timings:
        ch_request(
            f"INSERT INTO {DB}.benchmark_timings "
            f"(run_id, preset, total_rows, students, operation, category, elapsed_sec, measured_at) VALUES ("
            f"'{t['run_id']}', '{t['preset']}', {t['total_rows']}, {t['students']}, "
            f"'{t['operation']}', '{t['category']}', {t['elapsed_sec']}, '{t['measured_at']}')"
        )
    print(f"[ch] записано run_id={run['run_id']} → benchmark_runs / benchmark_timings")


def main() -> None:
    ap = argparse.ArgumentParser(description="EduFlow volume benchmark")
    ap.add_argument("--preset", choices=["tiny", "small", "medium", "large"], default="medium")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--weeks", type=int, default=16)
    ap.add_argument("--repeats", type=int, default=3, help="повторы SQL для median")
    ap.add_argument("--out", type=Path, default=None, help="каталог CSV генератора")
    ap.add_argument("--generate", action="store_true")
    ap.add_argument("--load", action="store_true")
    ap.add_argument("--sql", action="store_true", help="замерить SQL")
    ap.add_argument("--spark", action="store_true", help="замерить Spark-джобу")
    ap.add_argument("--full", action="store_true", help="generate+load+sql+spark")
    ap.add_argument("--notes", type=str, default="")
    ap.add_argument("--no-ch-write", action="store_true", help="не писать результаты в ClickHouse")
    a = ap.parse_args()

    if a.full:
        a.generate = a.load = a.sql = a.spark = True

    if not any([a.generate, a.load, a.sql, a.spark]):
        ap.error("укажите --full или флаги --generate --load --sql --spark")

    out = a.out or (GEN_DIR / "out" / a.preset)
    run_id = f"{a.preset}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:6]}"
    measured_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    host_info = f"{platform.node()} | {platform.platform()} | py{platform.python_version()}"

    generate_sec = load_sec = spark_sec = 0.0
    sql_times: dict[str, float] = {k: 0.0 for k in QUERIES}

    if a.generate:
        generate_sec = run_generate(a.preset, a.seed, out, a.weeks)
        print(f"[generate] {generate_sec:.2f}s → {out}")

    need_ch = a.load or a.sql or a.spark
    if need_ch and not ch_ok():
        sys.exit(1)

    if a.load:
        load_sec = run_load(out, truncate=True)
        print(f"[load] {load_sec:.2f}s")

    total_rows = count_total_rows() if need_ch else 0
    students = count_students() if need_ch else PRESET_STUDENTS.get(a.preset, 0)
    print(f"[data] students={students:,}  total_rows={total_rows:,}")

    if a.sql:
        print(f"[sql] {len(QUERIES)} запросов × {a.repeats} repeats (median)")
        for name, sql in QUERIES.items():
            sql_times[name] = time_sql(name, sql, a.repeats)

    if a.spark:
        spark_sec = run_spark()
        print(f"[spark] {spark_sec:.2f}s")

    run = {
        "run_id": run_id,
        "preset": a.preset,
        "seed": a.seed,
        "students": students,
        "total_rows": total_rows,
        "weeks": a.weeks,
        "generate_sec": round(generate_sec, 4),
        "load_sec": round(load_sec, 4),
        "spark_sec": round(spark_sec, 4),
        "q_level_dist_sec": round(sql_times["level_dist"], 4),
        "q_completion_week_sec": round(sql_times["completion_week"], 4),
        "q_time_to_result_sec": round(sql_times["time_to_result"], 4),
        "q_bottleneck_sec": round(sql_times["bottleneck"], 4),
        "q_risk_group_sec": round(sql_times["risk_group"], 4),
        "q_grades_method_sec": round(sql_times["grades_method"], 4),
        "q_avg_grade_node_sec": round(sql_times["avg_grade_node"], 4),
        "q_ai_correction_sec": round(sql_times["ai_correction"], 4),
        "q_teacher_load_sec": round(sql_times["teacher_load"], 4),
        "q_full_scan_grades_sec": round(sql_times["full_scan_grades"], 4),
        "repeats": a.repeats,
        "host_info": host_info,
        "notes": a.notes,
        "measured_at": measured_at,
    }

    timings: list[dict] = []
    def add_t(op: str, cat: str, sec: float) -> None:
        if sec and sec > 0:
            timings.append({
                "run_id": run_id,
                "preset": a.preset,
                "total_rows": total_rows,
                "students": students,
                "operation": op,
                "category": cat,
                "elapsed_sec": round(sec, 4),
                "measured_at": measured_at,
            })

    add_t("generate", "pipeline", generate_sec)
    add_t("load", "pipeline", load_sec)
    add_t("spark", "pipeline", spark_sec)
    for name, sec in sql_times.items():
        add_t(f"sql:{name}", "sql", sec)

    save_local(run, timings)
    if need_ch and not a.no_ch_write:
        try:
            insert_ch(run, timings)
        except Exception as e:
            print(f"[ch] не удалось записать результаты: {e}")

    print("\n=== summary ===")
    print(f"preset={a.preset}  rows={total_rows:,}  students={students:,}")
    print(f"generate={generate_sec:.2f}s  load={load_sec:.2f}s  spark={spark_sec:.2f}s")
    if a.sql:
        for name, sec in sql_times.items():
            print(f"  sql:{name:20s} {sec:.3f}s")


if __name__ == "__main__":
    main()

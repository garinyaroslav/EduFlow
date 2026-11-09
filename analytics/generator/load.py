#!/usr/bin/env python3
"""Загрузка CSV.gz, созданных generate.py, в ClickHouse по HTTP (без внешних зависимостей).

  python3 load.py out/                 # дописать
  python3 load.py out/ --truncate      # очистить таблицы перед загрузкой

Параметры подключения берутся из переменных окружения:
  CH_URL (http://localhost:8123), CH_USER (eduflow), CH_PASSWORD (eduflow), CH_DB (eduflow)
"""
import argparse
import base64
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

# порядок: справочники, затем события
TABLES = ["groups", "teachers", "students", "templates", "template_nodes", "roadmaps",
          "node_added", "student_activity", "grade_assigned", "grade_corrected",
          "level_changed", "teacher_assignment"]

URL = os.environ.get("CH_URL", "http://localhost:8123")
USER = os.environ.get("CH_USER", "eduflow")
PASSWORD = os.environ.get("CH_PASSWORD", "eduflow")
DB = os.environ.get("CH_DB", "eduflow")
AUTH = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()


def request(query: str, body=None, size: int | None = None, gzip_body: bool = False):
    headers = {"Authorization": AUTH}
    if gzip_body:
        headers["Content-Encoding"] = "gzip"
    if size is not None:
        headers["Content-Length"] = str(size)
    req = urllib.request.Request(f"{URL}/?{urllib.parse.urlencode({'query': query})}",
                                 data=body if body is not None else b"", headers=headers, method="POST")
    with urllib.request.urlopen(req) as r:
        return r.read().decode()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dir")
    ap.add_argument("--truncate", action="store_true")
    a = ap.parse_args()
    d = Path(a.dir)

    for t in TABLES:
        if not (d / f"{t}.csv.gz").exists():
            sys.exit(f"нет файла {d / (t + '.csv.gz')}")
    for t in TABLES:
        if a.truncate:
            request(f"TRUNCATE TABLE {DB}.{t}")
        f = d / f"{t}.csv.gz"
        with open(f, "rb") as fh:
            request(f"INSERT INTO {DB}.{t} FORMAT CSVWithNames", fh, f.stat().st_size, gzip_body=True)
        n = request(f"SELECT count() FROM {DB}.{t}").strip()
        print(f"{t:18s} в таблице: {n}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ClickHouse Schema & Settings → JSON exporter (structure-only, no data)

Совместимость: проверено с clickhouse-client 22.1.3.7 (official build).

Что экспортируем:
- Список БД (кроме system / INFORMATION_SCHEMA), SHOW CREATE DATABASE
- Все объекты из system.tables по каждой БД (включая VIEW/MATERIALIZED VIEW), SHOW CREATE TABLE
  * пропускаем внутренние .inner.* таблицы
- Колонки из system.columns (тип, дефолты, codec, ttl, comment) с фолбэком на старые версии
- Dictionaries (если есть): system.dictionaries + SHOW CREATE DICTIONARY
- Настройки сервера, отличные от дефолтов: system.settings WHERE changed = 1
- Информация о кластерах (если доступна): system.clusters

Требования:
- Доступный в PATH `clickhouse-client` (либо запускаем через --dsn, напр. docker exec ...)
- Доступ к чтению system.* и SHOW CREATE

Примеры:
  python ch_export_schema.py --host 127.0.0.1 --user default --database mydb --out dump.json
  python ch_export_schema.py --dsn "clickhouse-client -h 127.0.0.1 --port 9000 -udefault --password secret" --out dump.json
  python ch_export_schema.py --dsn "docker exec -i clickhouse clickhouse-client -h localhost --port 9000 -udefault --password 4095" --database vz --out dump.json
"""

import argparse
import json
import subprocess
import shlex
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# ---------------------------
# Helpers
# ---------------------------

def ch_str(s: str) -> str:
    """Безопасный строковый литерал для ClickHouse: '...'
    Используем в WHERE database = '...'/table='...'.
    """
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'"

def run_cli(dsn_prefix: str, sql: str, json_mode: bool = True) -> Any:
    """Выполнить SQL через clickhouse-client и (опционально) распарсить JSON-ответ."""
    fmt = "JSON" if json_mode else "TabSeparated"
    cmd = f'{dsn_prefix} -q {shlex.quote(sql)} --format={fmt}'
    try:
        out = subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        sys.stderr.write(f"[ERR] {e}\n{e.output.decode('utf-8', 'ignore')}\n")
        raise
    if json_mode:
        return json.loads(out.decode('utf-8'))
    return out.decode('utf-8')

def guess_dsn(args) -> str:
    """Собираем команду для clickhouse-client. Если --dsn задан, доверяем её полностью."""
    if args.dsn:
        return args.dsn
    parts = ["clickhouse-client"]
    if args.host:
        parts += ["-h", str(args.host)]
    if args.port:
        parts += ["--port", str(args.port)]
    if args.user:
        parts += ["-u", str(args.user)]
    if args.password is not None:
        parts += ["--password", str(args.password)]
    if args.secure:
        parts += ["--secure"]  # TLS (обычно порт 9440, если настроено на сервере)
    return " ".join(shlex.quote(p) for p in parts)

# ---------------------------
# Metadata fetchers
# ---------------------------

def list_databases(dsn: str) -> List[str]:
    j = run_cli(dsn, "SELECT name FROM system.databases ORDER BY name", True)
    # фильтруем служебные БД без зависимости от регистра
    return [
        r["name"] for r in j["data"]
        if r.get("name", "").lower() not in ("system", "information_schema")
    ]

def list_tables(dsn: str, db: str) -> List[Dict[str, Any]]:
    # Важно: использовать строковый литерал, а не "json.dumps(db)" → ИНАЧЕ двойные кавычки сломают запрос.
    sql = f"""
    SELECT
      name,
      engine
    FROM system.tables
    WHERE database = {ch_str(db)}
    ORDER BY name
    """
    j = run_cli(dsn, sql, True)
    return j["data"]

def show_create_database(dsn: str, db: str) -> str:
    sql = f"SHOW CREATE DATABASE `{db}`"
    j = run_cli(dsn, sql, True)
    if j["data"]:
        row = j["data"][0]
        if row.get("statement"):
            return row["statement"]
        if row.get("create_query"):
            return row["create_query"]
    return run_cli(dsn, sql, False).strip()

def show_create_table(dsn: str, db: str, table: str) -> str:
    sql = f"SHOW CREATE TABLE `{db}`.`{table}`"
    j = run_cli(dsn, sql, True)
    if j["data"]:
        row = j["data"][0]
        if row.get("statement"):
            return row["statement"]
        if row.get("create_query"):
            return row["create_query"]
    return run_cli(dsn, sql, False).strip()

def list_columns(dsn: str, db: str, table: str) -> List[Dict[str, Any]]:
    """Вернуть список колонок с деталями. Пытаемся полную форму, при ошибке — минимальную."""
    sql_full = f"""
    SELECT
      name,
      type,
      position,
      default_kind,
      default_expression,
      comment,
      codec_expression,
      ttl_expression
    FROM system.columns
    WHERE database = {ch_str(db)} AND table = {ch_str(table)}
    ORDER BY position, name
    """
    try:
        j = run_cli(dsn, sql_full, True)
        cols = []
        for r in j["data"]:
            cols.append({
                "name": r.get("name"),
                "type": r.get("type"),
                "default_kind": r.get("default_kind"),
                "default_expression": r.get("default_expression"),
                "comment": r.get("comment"),
                "codec_expression": r.get("codec_expression"),
                "ttl_expression": r.get("ttl_expression"),
            })
        return cols
    except Exception as e_full:
        sql_min = f"""
        SELECT
          name,
          type
        FROM system.columns
        WHERE database = {ch_str(db)} AND table = {ch_str(table)}
        ORDER BY name
        """
        try:
            j = run_cli(dsn, sql_min, True)
            return [{"name": r.get("name"), "type": r.get("type")} for r in j["data"]]
        except Exception as e_min:
            return [{"_error": f"system.columns failed: {e_min} (full query failed: {e_full})"}]

def changed_settings(dsn: str) -> List[Dict[str, Any]]:
    sql = "SELECT name, value, changed, description FROM system.settings WHERE changed = 1 ORDER BY name"
    try:
        j = run_cli(dsn, sql, True)
        return j["data"]
    except Exception:
        return []

def cluster_info(dsn: str) -> List[Dict[str, Any]]:
    try:
        j = run_cli(dsn, "SELECT * FROM system.clusters ORDER BY cluster, shard_num, replica_num", True)
        essentials = []
        for r in j["data"]:
            essentials.append({
                "cluster": r.get("cluster"),
                "shard_num": r.get("shard_num"),
                "replica_num": r.get("replica_num"),
                "host_name": r.get("host_name"),
                "port": r.get("port"),
                "is_local": r.get("is_local"),
                "user": r.get("user"),
                "secure": r.get("secure"),
            })
        return essentials
    except Exception:
        return []

def list_dictionaries(dsn: str, db: str) -> List[str]:
    try:
        sql = f"SELECT name FROM system.dictionaries WHERE database = {ch_str(db)} ORDER BY name"
        j = run_cli(dsn, sql, True)
        return [r["name"] for r in j["data"]]
    except Exception:
        return []

def show_create_dictionary(dsn: str, db: str, dict_name: str) -> str:
    sql = f"SHOW CREATE DICTIONARY `{db}`.`{dict_name}`"
    try:
        j = run_cli(dsn, sql, True)
        if j["data"]:
            row = j["data"][0]
            if row.get("statement"):
                return row["statement"]
            if row.get("create_query"):
                return row["create_query"]
        return run_cli(dsn, sql, False).strip()
    except Exception as e:
        return f"-- SHOW CREATE DICTIONARY failed: {e}"

def server_version(dsn: str) -> Optional[str]:
    try:
        j = run_cli(dsn, "SELECT version()", True)
        if j["data"]:
            # в 22.1 имя колонки может быть "version()"
            return str(list(j["data"][0].values())[0])
    except Exception:
        pass
    return None

# ---------------------------
# Main
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Export ClickHouse schema & settings to JSON (structure only).")
    ap.add_argument("--host", help="ClickHouse host (default: localhost)")
    ap.add_argument("--port", type=int, help="ClickHouse TCP port (default: 9000)")
    ap.add_argument("--user", help="Username (default: current user)")
    ap.add_argument("--password", help="Password (default: none)")
    ap.add_argument("--secure", action="store_true", help="Use secure TCP (--secure)")
    ap.add_argument("--database", help="Limit export to this database only")
    ap.add_argument("--out", default="clickhouse_schema_dump.json", help="Output JSON file")
    ap.add_argument("--dsn", help="Full clickhouse-client command prefix to use instead of flags")
    ap.add_argument("--no-dicts", action="store_true", help="Skip dictionaries export")
    args = ap.parse_args()

    dsn = guess_dsn(args)

    result: Dict[str, Any] = {
        "export_version": 1,
        "generator": "ch_export_schema.py",
        "server_version": server_version(dsn),
        "server_changed_settings": changed_settings(dsn),
        "clusters": cluster_info(dsn),
        "databases": {}
    }

    databases = [args.database] if args.database else list_databases(dsn)

    for db in databases:
        db_entry: Dict[str, Any] = {
            "show_create": show_create_database(dsn, db),
            "objects": {},        # tables/views/materialized views
            "dictionaries": {}    # dicts
        }

        # Таблицы/вьюхи из system.tables
        for t in list_tables(dsn, db):
            name = t.get("name")
            if not name:
                continue
            # Пропустим внутренние .inner.* чтобы не плодить шум
            if name.startswith(".inner."):
                continue
            try:
                ddl = show_create_table(dsn, db, name)
            except Exception as e:
                ddl = f"-- SHOW CREATE TABLE failed: {e}"
            try:
                cols = list_columns(dsn, db, name)
            except Exception as e:
                cols = [{"_error": f"system.columns failed: {e}"}]
            db_entry["objects"][name] = {
                "engine": t.get("engine"),
                "columns": cols,
                "show_create": ddl
            }

        # Словари (если не отключили флагом)
        if not args.no_dicts:
            for dict_name in list_dictionaries(dsn, db):
                db_entry["dictionaries"][dict_name] = {
                    "show_create": show_create_dictionary(dsn, db, dict_name)
                }

        result["databases"][db] = db_entry

    out_path = Path(args.out).resolve()
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote {out_path}")

if __name__ == "__main__":
    main()
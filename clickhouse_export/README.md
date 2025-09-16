# ClickHouse Schema & Settings → JSON Exporter

**ClickHouse Schema & Settings → JSON Exporter** — это Python-скрипт для экспорта **структуры базы данных ClickHouse** (DDL) и изменённых настроек сервера в **JSON-формате**.  
Он **не выгружает данные**, а только схему, параметры и кластеры.  
Полезно для бэкапов, ревью архитектуры и документирования ClickHouse.

##  Возможности  

- Экспортирует **список баз данных** (кроме `system` и `INFORMATION_SCHEMA`) с их `SHOW CREATE DATABASE`.
- Экспортирует **все объекты** из `system.tables` по каждой БД:
  - Таблицы, VIEW и MATERIALIZED VIEW.
  - Пропускает внутренние `.inner.*` таблицы.
  - Сохраняет `SHOW CREATE TABLE` для каждого объекта.
- Экспортирует **структуру колонок**:
  - типы данных;
  - значения по умолчанию;
  - codec / ttl / комментарии (если поддерживается версией CH).
- Экспортирует **словари** (`system.dictionaries`) + `SHOW CREATE DICTIONARY` (если не отключено).
- Сохраняет **изменённые настройки сервера** (`system.settings WHERE changed = 1`).
- Сохраняет **информацию о кластерах** (`system.clusters`), если доступна.
- Выводит всё в один JSON-файл с иерархией.

##  Требования  

- Python 3.6+  
- Доступный в `PATH` `clickhouse-client` (либо Docker, либо любой CLI с доступом к ClickHouse).  
- Права на чтение `system.*` и `SHOW CREATE`.  

Установка зависимостей Python:
```bash
pip install argparse
# json, subprocess, shlex, pathlib и typing встроены в стандартную библиотеку
```

##  Использование  

### Базовый пример:
```bash
python ch_export_schema.py --host 127.0.0.1 --user default --database mydb --out dump.json
```

### Через DSN (своё clickhouse-client):
```bash
python ch_export_schema.py   --dsn "clickhouse-client -h 127.0.0.1 --port 9000 -udefault --password secret"   --out dump.json
```

### Через Docker:
```bash
python ch_export_schema.py   --dsn "docker exec -i clickhouse clickhouse-client -h localhost --port 9000 -udefault --password 4095"   --database vz --out dump.json
```

### Аргументы командной строки  

| Параметр             | Описание                                                   |
|----------------------|------------------------------------------------------------|
| `--host`             | Хост ClickHouse (по умолчанию localhost)                    |
| `--port`             | TCP порт (по умолчанию 9000)                                |
| `--user`             | Имя пользователя                                           |
| `--password`         | Пароль                                                     |
| `--secure`           | Использовать TLS (`--secure`)                               |
| `--database`         | Ограничить экспорт одной БД (по умолчанию все)              |
| `--out`              | Выходной JSON-файл (по умолчанию `clickhouse_schema_dump.json`) |
| `--dsn`              | Полная команда для `clickhouse-client` (альтернатива флагам)|
| `--no-dicts`         | Пропустить экспорт словарей                                |

##  Структура JSON  

Пример верхнего уровня:
```json
{
  "export_version": 1,
  "generator": "ch_export_schema.py",
  "server_version": "22.1.3.7",
  "server_changed_settings": [ ... ],
  "clusters": [ ... ],
  "databases": {
    "vz": {
      "show_create": "CREATE DATABASE vz ENGINE = Atomic",
      "objects": {
        "search_query_big": {
          "engine": "MergeTree",
          "columns": [
            {"name": "id_sq", "type": "Int32"},
            {"name": "search_query", "type": "String"}
          ],
          "show_create": "CREATE TABLE vz.search_query_big (...)"
        }
      },
      "dictionaries": {}
    }
  }
}
```

##  Как это работает  

Скрипт:
1. Определяет список баз данных (через `system.databases`).
2. Для каждой базы:
   - Сохраняет `SHOW CREATE DATABASE`.
   - Получает список таблиц и их движков.
   - Для каждой таблицы выполняет `SHOW CREATE TABLE` и `system.columns`.
   - (Опционально) выгружает словари.
3. Сохраняет изменённые настройки сервера.
4. Сохраняет список кластеров (шарды, реплики).
5. Формирует JSON и пишет его на диск.

##  Безопасность  

- Скрипт не запрашивает пароли интерактивно — используйте флаги или DSN.  
- Для TLS используйте `--secure` и порт 9440.  

##  Лицензия  

MIT (или любая подходящая внутренняя лицензия).  

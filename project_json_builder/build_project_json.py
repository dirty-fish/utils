
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path

# --------------------
# Настройки по умолчанию
# --------------------
DEFAULT_EXCLUDED_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp",
    ".mp4", ".mov", ".avi", ".mkv",
    ".pdf",
    ".exe", ".dll", ".so", ".dylib",
    ".bin", ".o", ".a", ".class",
    ".zip", ".tar", ".gz", ".bz2", ".xz", ".7z",
    ".ico", ".icns",
    ".lock",
}

DEFAULT_EXCLUDED_DIRS = {
    ".git", ".hg", ".svn", ".idea", ".vscode", "__pycache__", "node_modules", "dist", "build", "out", ".next", ".cache",
    # расширения исключений
    ".venv", "venv", ".mypy_cache", ".pytest_cache", ".ruff_cache",
    ".terraform", ".gradle", ".sbt",
}

# Лимиты на сборку
MAX_FILES = 2000
MAX_TOTAL_BYTES = 20_000_000  # ~20 MB

# Простая карта расширений -> язык
LANG_BY_EXT = {
    ".py": "python",
    ".ipynb": "json",
    ".js": "javascript",
    ".jsx": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".java": "java",
    ".kt": "kotlin",
    ".go": "go",
    ".rs": "rust",
    ".c": "c",
    ".h": "c",
    ".cpp": "cpp",
    ".hpp": "cpp",
    ".cc": "cpp",
    ".cs": "csharp",
    ".php": "php",
    ".rb": "ruby",
    ".swift": "swift",
    ".m": "objectivec",
    ".mm": "objectivec",
    ".scala": "scala",
    ".sh": "bash",
    ".ps1": "powershell",
    ".sql": "sql",
    ".lua": "lua",
    ".r": "r",
    ".pl": "perl",
    ".clj": "clojure",
    ".hs": "haskell",
    ".dart": "dart",
    ".html": "html",
    ".htm": "html",
    ".css": "css",
    ".scss": "css",
    ".sass": "css",
    ".json": "json",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    ".md": "markdown",
    ".txt": "plain",
    ".ini": "ini",
    ".env": "env",
    ".dockerfile": "dockerfile",
    "Dockerfile": "dockerfile",
    ".makefile": "make",
    "Makefile": "make",
    ".csv": "plain",
}

DOC_NAMES = {"readme", "license", "copying", "changelog", "contributing"}

# --------------------
# Вспомогательные функции
# --------------------
def norm_ext(e: str) -> str:
    """Нормализует расширение из аргумента -e/--exclude."""
    e = e.strip().lower()
    e = e.lstrip("*")  # поддержка *.log
    return e if e.startswith(".") else f".{e}"

def detect_language(path: Path) -> str:
    """
    Определяем язык:
    - Dockerfile/Makefile в любом регистре
    - dot-файлы без суффикса (например, .env)
    - по suffix как обычно
    """
    name = path.name
    lower_name = name.lower()
    ext = path.suffix.lower()

    # Спец-имена без расширения
    special_names = {"dockerfile": "dockerfile", "makefile": "make"}
    if lower_name in special_names:
        return special_names[lower_name]

    # Dot-файлы без расширения: .env, .env.example и т.п.
    if name.startswith(".") and lower_name in LANG_BY_EXT:
        return LANG_BY_EXT[lower_name]

    # Обычный случай
    return LANG_BY_EXT.get(ext, "plain")

def detect_type(path: Path) -> str:
    """
    'code' для исходников/конфигов; 'text' для документации.
    Расширяем список doc-имён.
    """
    name = path.name
    stem = path.stem.lower()
    ext = path.suffix.lower()

    if stem in DOC_NAMES or name.upper() in {"README", "LICENSE"} or ext in {".md", ".txt"}:
        return "text"
    return "code"

def looks_like_text(path: Path, sample_bytes: int = 4096) -> bool:
    """
    Грубая эвристика: если много нулевых байт/непечатных символов — считаем бинарником.
    """
    try:
        with path.open("rb") as f:
            chunk = f.read(sample_bytes)
        if b"\x00" in chunk:
            return False
        non_printable = sum(b < 9 or (13 < b < 32) for b in chunk)
        return len(chunk) == 0 or non_printable / max(1, len(chunk)) < 0.2
    except Exception:
        return False

def read_text_file(path: Path, max_bytes: int = 5_000_000) -> str:
    # Защита от очень больших файлов
    size = path.stat().st_size
    if size > max_bytes:
        with path.open("rb") as f:
            chunk = f.read(max_bytes)
        return chunk.decode("utf-8", errors="replace")
    # нормальный путь
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        try:
            return path.read_text(encoding="utf-16")
        except Exception:
            return path.read_text(encoding="latin-1", errors="replace")

def read_csv_first_5_records(path: Path) -> str:
    """
    Возвращает первые 5 логических записей CSV как корректный CSV-текст.
    Учитывает кавычки и переносы в ячейках.
    """
    rows = []
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= 5:
                    break
                rows.append(row)
    except Exception:
        with path.open("r", encoding="latin-1", errors="replace", newline="") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i >= 5:
                    break
                rows.append(row)

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerows(rows)
    return out.getvalue()

def is_excluded_file(path: Path, excluded_exts: set) -> bool:
    name_lower = path.name.lower()
    if name_lower in {".ds_store"}:
        return True
    ext = path.suffix.lower()
    return ext in excluded_exts

def should_skip_dir(path: Path) -> bool:
    return path.name in DEFAULT_EXCLUDED_DIRS

def validate_schema(payload: dict) -> None:
    """
    Минимальная строгая валидация соответствия шаблону ChatGPT 5:
    - project_name: str
    - description: str (может быть пустой)
    - excluded_file_types: list[str]
    - files: list[ {path:str, type:"code"|"text", language:str, content:str} ]
    """
    if not isinstance(payload, dict):
        raise ValueError("Корневой JSON должен быть объектом.")
    for key in ("project_name", "files", "excluded_file_types"):
        if key not in payload:
            raise ValueError(f"Отсутствует обязательное поле: {key}")
    if not isinstance(payload["project_name"], str) or not payload["project_name"]:
        raise ValueError("project_name должен быть непустой строкой.")
    if "description" in payload and not isinstance(payload["description"], str):
        raise ValueError("description должен быть строкой, если указан.")
    if not isinstance(payload["excluded_file_types"], list) or not all(isinstance(x, str) for x in payload["excluded_file_types"]):
        raise ValueError("excluded_file_types должен быть массивом строк.")
    if not isinstance(payload["files"], list):
        raise ValueError("files должен быть массивом объектов файлов.")

    for i, f in enumerate(payload["files"], 1):
        if not isinstance(f, dict):
            raise ValueError(f"files[{i}] не объект.")
        for req in ("path", "type", "language", "content"):
            if req not in f:
                raise ValueError(f"files[{i}] отсутствует поле: {req}")
        if not isinstance(f["path"], str) or not f["path"]:
            raise ValueError(f"files[{i}].path должен быть непустой строкой.")
        if f["type"] not in ("code", "text"):
            raise ValueError(f"files[{i}].type должен быть 'code' или 'text'.")
        if not isinstance(f["language"], str) or not f["language"]:
            raise ValueError(f"files[{i}].language должен быть непустой строкой.")
        if not isinstance(f["content"], str):
            raise ValueError(f"files[{i}].content должен быть строкой.")

# --------------------
# Основная сборка
# --------------------
def build_payload(root: Path, extra_excluded_exts: set) -> dict:
    project_name = root.name

    excluded = set(DEFAULT_EXCLUDED_EXTS) | {norm_ext(e) for e in extra_excluded_exts}

    files = []
    total_bytes = 0

    for dirpath, dirnames, filenames in os.walk(root):
        # фильтруем каталоги на месте
        dirnames[:] = [d for d in dirnames if not should_skip_dir(Path(dirpath) / d)]

        for fname in sorted(filenames):
            if len(files) >= MAX_FILES:
                break

            fpath = Path(dirpath) / fname

            # игнор символических ссылок
            if fpath.is_symlink():
                continue

            rel_path = fpath.relative_to(root)
            ext = fpath.suffix.lower()

            # исключённые типы
            if is_excluded_file(fpath, excluded):
                continue

            # CSV: берём первые 5 логических записей
            try:
                if ext == ".csv":
                    content = read_csv_first_5_records(fpath)
                    language = "plain"  # по сути текст/табличка
                    ftype = "text"
                else:
                    # хейристика против бинарников без расширения
                    if not looks_like_text(fpath):
                        continue

                    language = detect_language(fpath)
                    ftype = detect_type(fpath)
                    content = read_text_file(fpath)
            except Exception as e:
                print(f"[warn] Пропуск файла {rel_path}: {e}", file=sys.stderr)
                continue

            files.append({
                "path": str(rel_path).replace(os.sep, "/"),
                "type": ftype,
                "language": language,
                "content": content
            })

            total_bytes += len(content.encode("utf-8", errors="ignore"))
            if total_bytes > MAX_TOTAL_BYTES:
                print(f"[info] Достигнут лимит размера ~{MAX_TOTAL_BYTES} байт, остановка.", file=sys.stderr)
                break

        if len(files) >= MAX_FILES or total_bytes > MAX_TOTAL_BYTES:
            break

    payload = {
        "project_name": project_name,
        "description": "",
        "files": files,
        "excluded_file_types": sorted(excluded),
    }

    # строгая проверка
    validate_schema(payload)
    return payload

# --------------------
# CLI
# --------------------
def main():
    parser = argparse.ArgumentParser(
        description="Собирает JSON со структурой проекта и содержимым текстовых/кодовых файлов. Для .csv берёт первые 5 логических записей."
    )
    parser.add_argument("folder", nargs="?", help="Путь к папке проекта. Если не задан, будет запрошен интерактивно.")
    parser.add_argument("--exclude", "-e", action="append", default=[],
                        help="Доп. расширение для исключения (с точкой или без неё). Можно указать несколько раз, напр: -e .log -e .svg -e *.bak")
    parser.add_argument("--output", "-o", default=None,
                        help="Имя выходного файла JSON (по умолчанию: project_structure.json в корне папки).")
    args = parser.parse_args()

    if not args.folder:
        try:
            folder = input("Укажите путь к папке проекта: ").strip().strip('"').strip("'")
        except EOFError:
            print("Не указан путь к папке.", file=sys.stderr)
            sys.exit(1)
    else:
        folder = args.folder

    root = Path(folder).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        print(f"Папка не найдена: {root}", file=sys.stderr)
        sys.exit(1)

    try:
        payload = build_payload(root, set(args.exclude))
    except ValueError as ve:
        print(f"[schema error] {ve}", file=sys.stderr)
        sys.exit(2)

    output_path = Path(args.output) if args.output else (root / "project_structure.json")
    try:
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Ошибка записи {output_path}: {e}", file=sys.stderr)
        sys.exit(3)

    print(f"Готово: {output_path}")

if __name__ == "__main__":
    main()

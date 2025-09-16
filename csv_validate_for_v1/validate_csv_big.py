#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import gzip
import io
import os
import sys
from typing import List, Optional, Tuple, Dict

EXPECTED_HEADER = ["Unnamed: 0", "name", "frequency"]

# На некоторых системах sys.maxsize может не пролезть в field_size_limit
try:
    csv.field_size_limit(sys.maxsize)
except Exception:
    pass

def open_text_utf8_strict(path: str):
    # Читаем строго как UTF-8, без автозамены символов
    return open(path, "r", encoding="utf-8-sig", newline="")

def check_utf8_streaming(path: str) -> Optional[str]:
    """Проверяем, что файл валидный UTF-8. Возвращаем None, если всё ок, иначе краткое описание."""
    try:
        with open(path, "rb") as fb:
            _ = io.TextIOWrapper(fb, encoding="utf-8", errors="strict", newline="")
            for _ in _:
                pass
        return None
    except UnicodeDecodeError as e:
        # Покажем позицию и примерную причину
        return f"UnicodeDecodeError @ byte {e.start}: {e.reason}"

def read_header(path: str) -> List[str]:
    with open_text_utf8_strict(path) as f:
        r = csv.reader(f)
        return next(r, []) or []

def validate_row(row: List[str], line_no: int) -> Tuple[Optional[str], Optional[str]]:
    """
    Валидируем строку.
    Возвращаем (error_code, message) или (None, None) если всё ок.
    """
    if len(row) != 3:
        return ("fields_count", f"Строка {line_no}: ожидается 3 поля, получено {len(row)} — {row!r}")

    idx_raw, name, freq_raw = row

    # 'Unnamed: 0' — целое
    try:
        int(idx_raw)
    except Exception:
        return ("idx_not_int", f"Строка {line_no}: 'Unnamed: 0' не int — {idx_raw!r}")

    # name — непустой
    if name is None or name.strip() == "":
        return ("name_empty", f"Строка {line_no}: 'name' пустая")

    # 'frequency' — целое >= 0
    try:
        fr = int(freq_raw)
        if fr < 0:
            return ("freq_negative", f"Строка {line_no}: 'frequency' < 0 — {freq_raw!r}")
    except Exception:
        return ("freq_not_int", f"Строка {line_no}: 'frequency' не int — {freq_raw!r}")

    return (None, None)

def open_errors_writer(path: str):
    """
    Открываем writer для файла ошибок.
    Если имя оканчивается на .gz — пишем GZIP.
    Возвращаем (file_handle, csv_writer)
    """
    if path.endswith(".gz"):
        fh = gzip.open(path, "wt", encoding="utf-8", newline="")
    else:
        fh = open(path, "w", encoding="utf-8", newline="")
    w = csv.writer(fh)
    w.writerow(["line_no", "error_code", "message", "col0", "col1", "col2", "raw_len"])
    return fh, w

def validate_csv(
    big_csv: str,
    sample_csv: Optional[str],
    out_clean: Optional[str],
    errors_out: Optional[str],
    sample_per_type: int,
    max_print_errors: int,
    progress_every: int
) -> None:
    # 1) Кодировка
    utf8_problem = check_utf8_streaming(big_csv)
    if utf8_problem:
        print(f"❌ Файл не валидный UTF-8: {utf8_problem}")
        print("   Пересохраните как CSV UTF-8 (или прогоните через iconv).")
        return
    print("✅ Кодировка: UTF-8")

    # 2) Заголовок: как в образце (если задан) или EXPECTED_HEADER
    expected_header = EXPECTED_HEADER
    if sample_csv:
        hdr = read_header(sample_csv)
        if hdr:
            expected_header = hdr
    print(f"Ожидаемый заголовок: {expected_header}")

    # 3) Проверим заголовок большого файла
    with open_text_utf8_strict(big_csv) as f:
        r = csv.reader(f)
        header = next(r, None)
        if header != expected_header:
            print("❌ Заголовок не совпадает.")
            print(f"   В файле:   {header}")
            print(f"   Ожидался:  {expected_header}")
            return
    print("✅ Заголовок совпадает")

    # 4) Потоковая проверка строк
    total = 0
    valid = 0
    # Счётчики по типам ошибок + семплы для отображения
    err_counts: Dict[str, int] = {}
    err_samples: Dict[str, List[str]] = {}

    # writer для очищенного файла
    clean_fh = None
    clean_wr = None
    if out_clean:
        clean_fh = open(out_clean, "w", encoding="utf-8", newline="")
        clean_wr = csv.writer(clean_fh)
        clean_wr.writerow(expected_header)

    # writer для полного лога ошибок
    err_fh = None
    err_wr = None
    if errors_out:
        err_fh, err_wr = open_errors_writer(errors_out)

    printed = 0

    with open_text_utf8_strict(big_csv) as f:
        r = csv.reader(f)
        _ = next(r, None)  # пропускаем заголовок
        for line_no, row in enumerate(r, start=2):
            total += 1

            code, msg = validate_row(row, line_no)
            if code is None:
                valid += 1
                if clean_wr:
                    clean_wr.writerow(row)
            else:
                # счётчики
                err_counts[code] = err_counts.get(code, 0) + 1

                # семпл для отображения (ограниченный)
                if code not in err_samples:
                    err_samples[code] = []
                if len(err_samples[code]) < sample_per_type:
                    err_samples[code].append(msg)

                # полный лог (в файл)
                if err_wr:
                    c0 = row[0] if len(row) > 0 else ""
                    c1 = row[1] if len(row) > 1 else ""
                    c2 = row[2] if len(row) > 2 else ""
                    err_wr.writerow([line_no, code, msg, c0, c1, c2, len(row)])

                # Печать первых max_print_errors в stdout (по всем типам суммарно)
                if printed < max_print_errors:
                    print("  ·", msg)
                    printed += 1

            if progress_every and total % progress_every == 0:
                print(f"[progress] обработано {total:,} строк…", flush=True)

    if clean_fh:
        clean_fh.close()
    if err_fh:
        err_fh.close()

    # 5) Отчёт
    print("—" * 70)
    print(f"Строк данных (без заголовка): {total:,}")
    print(f"Валидных строк:               {valid:,}")
    print(f"Невалидных строк:             {total - valid:,}")

    if out_clean:
        print(f"Очищенный CSV:                {os.path.abspath(out_clean)}")
    if errors_out:
        print(f"Полный лог ошибок:            {os.path.abspath(errors_out)}")

    if err_counts:
        print("\nСводка по типам ошибок:")
        for code, cnt in sorted(err_counts.items(), key=lambda x: -x[1]):
            print(f"  {code:>14}: {cnt:,}")

        print("\nСемпл ошибок по типам (до", sample_per_type, "на тип):")
        for code, samples in err_samples.items():
            print(f"  [{code}]")
            for s in samples:
                print("    -", s)
    else:
        print("\nОшибок формата не найдено. ✅")

def main():
    ap = argparse.ArgumentParser(
        description="Стриминговая проверка большого CSV (~миллионы строк) на соответствие образцу."
    )
    ap.add_argument("--big", required=True, help="Путь к большому CSV (проверяемый файл)")
    ap.add_argument("--sample", help="CSV-образец для заголовка (если не задан, ждём стандартный)")
    ap.add_argument("--out-clean", help="Сохранить очищенный CSV (только валидные строки)")
    ap.add_argument("--errors-out", help="Сохранить ПОЛНЫЙ лог ошибок (CSV, можно .gz)")
    ap.add_argument("--sample-per-type", type=int, default=5,
                    help="Сколько примеров на каждый тип ошибки показать в консоли (по умолчанию 5)")
    ap.add_argument("--max-print-errors", type=int, default=50,
                    help="Сколько ошибок максимально печатать в stdout (по умолчанию 50)")
    ap.add_argument("--progress-every", type=int, default=100000,
                    help="Каждые N строк печатать прогресс (0 чтобы отключить)")
    args = ap.parse_args()

    validate_csv(
        big_csv=args.big,
        sample_csv=args.sample,
        out_clean=args.out_clean,
        errors_out=args.errors_out,
        sample_per_type=args.sample_per_type,
        max_print_errors=args.max_print_errors,
        progress_every=args.progress_every
    )

if __name__ == "__main__":
    main()

from __future__ import annotations

from pathlib import Path
import shutil
import sys


def normalize_csv_file(csv_path: Path) -> bool:
    """
    Нормализует CSV-файл по той же логике, что и utils.process_csv_file:
    - читает файл как UTF-8;
    - разбивает каждую строку по tab;
    - если столбцов 3 и больше, очищает первый столбец от внешних кавычек,
      заменяет двойные кавычки на одинарные и добавляет префикс ^1;
    - сохраняет только первые 3 столбца;
    - строки без нужного формата записывает как есть.
    """
    temp_path = csv_path.with_name(csv_path.name + ".tmp")

    try:
        with csv_path.open("r", encoding="utf-8") as infile, temp_path.open(
            "w", encoding="utf-8", newline=""
        ) as outfile:
            for line in infile:
                parts = line.strip().split("\t")

                if len(parts) >= 3:
                    first_col = parts[0]
                    first_col = first_col.strip('"')
                    first_col = first_col.replace('""', '"')
                    formatted_first_col = f"^1{first_col}"
                    new_line = f"{formatted_first_col}\t{parts[1]}\t{parts[2]}"
                    outfile.write(new_line + "\n")
                else:
                    outfile.write(line)

        shutil.move(str(temp_path), str(csv_path))
        return True
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        return False


def iter_csv_files(directory: Path) -> list[Path]:
    return sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    )


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    csv_files = iter_csv_files(script_dir)

    if not csv_files:
        print(f"CSV-файлы не найдены: {script_dir}")
        return 0

    ok_count = 0
    fail_count = 0

    print(f"Найдено CSV-файлов: {len(csv_files)}")
    print(f"Папка: {script_dir}")

    for csv_file in csv_files:
        success = normalize_csv_file(csv_file)
        if success:
            ok_count += 1
            print(f"OK  {csv_file.name}")
        else:
            fail_count += 1
            print(f"ERR {csv_file.name}")

    print(f"Готово. Успешно: {ok_count}, ошибок: {fail_count}")
    return 1 if fail_count else 0


if __name__ == "__main__":
    sys.exit(main())

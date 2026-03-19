from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from options import size_options


BT_DO_NOT_SAVE_CHANGES = 1
TEMPLATE_DIR = Path(__file__).resolve().parent / "bartender datamatrix"
SIZE_SUBSTRING_CANDIDATES = ("Этикетка X из Y", "Размер", "Size")
SERIAL_SUBSTRING_CANDIDATES = ("Сериализованный номер", "SerialNumber", "Serial")


class BarTenderPrintError(RuntimeError):
    """Raised when BarTender printing cannot be completed."""


@dataclass(frozen=True)
class PrintContext:
    order_name: str
    document_id: str
    csv_path: str
    template_path: str
    size: str
    label_count: int


def build_print_context(order_name: str, document_id: str, csv_path: str) -> PrintContext:
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise BarTenderPrintError(f"CSV-файл не найден: {csv_file}")

    template_path = find_template_path()
    size = extract_size_from_order_name(order_name)
    label_count = count_csv_records(csv_file)

    if label_count <= 0:
        raise BarTenderPrintError(f"В CSV нет строк для печати: {csv_file}")

    return PrintContext(
        order_name=order_name,
        document_id=document_id,
        csv_path=str(csv_file),
        template_path=str(template_path),
        size=size,
        label_count=label_count,
    )


def find_template_path() -> Path:
    if not TEMPLATE_DIR.exists():
        raise BarTenderPrintError(f"Папка с шаблоном не найдена: {TEMPLATE_DIR}")

    templates = sorted(path for path in TEMPLATE_DIR.iterdir() if path.is_file() and path.suffix.lower() == ".btw")
    if not templates:
        raise BarTenderPrintError(f"В папке {TEMPLATE_DIR} не найден шаблон .btw")

    return templates[0]


def extract_size_from_order_name(order_name: str) -> str:
    normalized_order_name = str(order_name or "").upper()
    variants = sorted({str(value).upper() for value in size_options}, key=len, reverse=True)

    tokens = [
        token.strip("()[]{}.,;:/\\|_-")
        for token in re.split(r"\s+", normalized_order_name)
        if token.strip()
    ]

    for variant in variants:
        normalized_variant = variant.replace(",", ".")
        if any(token == variant or token == normalized_variant for token in tokens):
            return variant

    raise BarTenderPrintError(
        f"Не удалось определить размер из названия заявки '{order_name}'. "
        f"Ожидаю один из размеров: {', '.join(variants)}"
    )


def count_csv_records(csv_path: Path) -> int:
    record_count = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        for raw_line in csv_file:
            line = raw_line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if parts and parts[0].strip():
                record_count += 1

    return record_count


def print_labels(context: PrintContext) -> None:
    import pythoncom
    import win32com.client  # type: ignore

    pythoncom.CoInitialize()
    app = None
    fmt = None

    try:
        app = win32com.client.DispatchEx("BarTender.Application")
        app.Visible = False
        fmt = app.Formats.Open(context.template_path, False, "")

        _configure_database(fmt, context.csv_path)
        _configure_size(fmt, context.size)
        _configure_serialization(fmt)
        _configure_print_setup(fmt, context.label_count)

        fmt.PrintOut(False, False)
    except BarTenderPrintError:
        raise
    except Exception as exc:
        raise BarTenderPrintError(f"BarTender вернул ошибку: {exc}") from exc
    finally:
        if fmt is not None:
            try:
                fmt.Close(BT_DO_NOT_SAVE_CHANGES)
            except Exception:
                pass
        if app is not None:
            try:
                app.Quit(BT_DO_NOT_SAVE_CHANGES)
            except Exception:
                pass
        pythoncom.CoUninitialize()


def _configure_database(bt_format, csv_path: str) -> None:
    databases = getattr(bt_format, "Databases", None)
    if databases is None or int(getattr(databases, "Count", 0)) <= 0:
        raise BarTenderPrintError(
            "В шаблоне BarTender не настроена текстовая база данных. "
            "Откройте .btw и подключите CSV как Text File."
        )

    configured = False
    last_error = None

    for index in range(1, int(databases.Count) + 1):
        try:
            database = databases.GetDatabase(index)
            database.TextFile.FileName = csv_path
            configured = True
        except Exception as exc:
            last_error = exc

    if not configured:
        raise BarTenderPrintError(
            "Не удалось переназначить CSV-файл для шаблона BarTender."
            + (f" Последняя ошибка: {last_error}" if last_error else "")
        )

    try:
        bt_format.UseDatabase = True
    except Exception:
        pass

    try:
        bt_format.PrintSetup.RefreshDatabases = True
    except Exception:
        pass


def _configure_size(bt_format, size: str) -> None:
    size_name = _find_named_substring_name(bt_format, SIZE_SUBSTRING_CANDIDATES)
    _set_named_substring_value(bt_format, size_name, size)


def _configure_serialization(bt_format) -> None:
    serial_name = _find_named_substring_name(bt_format, SERIAL_SUBSTRING_CANDIDATES)
    _set_named_substring_value(bt_format, serial_name, "1")

    serial_substring = bt_format.NamedSubStrings.GetSubString(serial_name)
    serial_substring.SerializeBy = "1"
    serial_substring.SerializeEvery = 1

    try:
        serial_substring.Rollover = False
    except Exception:
        pass


def _configure_print_setup(bt_format, label_count: int) -> None:
    try:
        bt_format.PrintSetup.IdenticalCopiesOfLabel = 1
    except Exception:
        pass

    try:
        bt_format.PrintSetup.NumberSerializedLabels = label_count
    except Exception:
        pass

    try:
        bt_format.PrintSetup.EnablePrompting = False
    except Exception:
        pass

    try:
        bt_format.SelectRecordsAtPrint = False
    except Exception:
        pass


def _find_named_substring_name(bt_format, candidates: Sequence[str]) -> str:
    named_substrings = getattr(bt_format, "NamedSubStrings", None)
    if named_substrings is None:
        raise BarTenderPrintError(
            "В шаблоне BarTender не найдены именованные источники данных."
        )

    names: list[str] = []
    count = int(getattr(named_substrings, "Count", 0))
    for index in range(1, count + 1):
        substring = named_substrings.GetSubString(index)
        name = str(substring.Name)
        names.append(name)
        if _matches_candidate(name, candidates):
            return name

    raise BarTenderPrintError(
        "В шаблоне BarTender не найден нужный именованный источник данных. "
        f"Искал: {', '.join(candidates)}. Найдено: {', '.join(names) if names else 'ничего'}"
    )


def _set_named_substring_value(bt_format, name: str, value: str) -> None:
    try:
        bt_format.SetNamedSubStringValue(name, value)
        return
    except Exception:
        pass

    try:
        bt_format.NamedSubStrings.SetSubString(name, value)
        return
    except Exception as exc:
        raise BarTenderPrintError(
            f"Не удалось установить значение '{value}' для подстроки '{name}': {exc}"
        ) from exc


def _matches_candidate(value: str, candidates: Iterable[str]) -> bool:
    normalized_value = _normalize_name(value)
    return any(normalized_value == _normalize_name(candidate) for candidate in candidates)


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())

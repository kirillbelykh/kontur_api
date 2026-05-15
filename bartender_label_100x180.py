from __future__ import annotations

import base64
import csv
import re
import subprocess
import tempfile
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


BT_DO_NOT_SAVE_CHANGES = 1
TEMPLATE_ROOT_DIR = Path(__file__).resolve().parent / "BarTender наклейки 100х180"
HR_TEMPLATE_GROUP_DIR = TEMPLATE_ROOT_DIR / "Латекс, Нитрил, HR"
AGGREGATION_CODES_DIR = Path.home() / "Desktop" / "Агрег коды км"
MARKING_CODES_DIR = Path.home() / "Desktop" / "Коды км"
BARTENDER_SDK_DLL = Path(
    r"C:\Program Files\Seagull\BarTender 2022\SDK\Assemblies\Seagull.BarTender.Print.dll"
)
POWERSHELL_EXE = "powershell.exe"
TEMP_PRINT_ARTIFACT_RETENTION_SECONDS = 300
PRINT_SUBMIT_ATTEMPTS = 2
PRINT_SUBPROCESS_TIMEOUT_SECONDS = 90
PRINT_SUBMIT_RETRY_DELAY_SECONDS = 2
TEXT_OBJECT_TYPE = "2"
COPIES_OBJECT_TYPE = "2048"
SERIAL_OBJECT_TYPE = "4096"
AGGREGATION_SOURCE_KIND = "aggregation"
MARKING_SOURCE_KIND = "marking"
DETAIL_FIELD_MARKERS = (
    "размер",
    "партия",
    "дата изготовления",
    "срок годности",
    "количество",
)
GTIN_COLUMN = "GTIN"
FULL_NAME_COLUMN = "Полное наименование товара"
UNITS_COLUMN = "Количество единиц употребления в потребительской упаковке"
SIZE_COLUMN = "Размер"
COLOR_COLUMN = "Цвет"
SIMPL_COLUMN = "Упрощенно"
_SDK_PRINT_LOCK = threading.Lock()


class BarTenderLabel100x180Error(RuntimeError):
    """Raised when 100x180 BarTender label printing cannot be completed."""


@dataclass(frozen=True)
class LabelTemplateInfo:
    name: str
    category: str
    relative_path: str
    path: str
    data_source_kind: str


@dataclass(frozen=True)
class AggregationCsvInfo:
    name: str
    folder_name: str
    path: str
    record_count: int
    modified_timestamp: float


@dataclass(frozen=True)
class OrderMetadata:
    document_id: str
    order_name: str
    gtin: str
    full_name: str
    simpl_name: str
    size: str
    batch: str
    color: str
    units_per_pack: int
    order_quantity: int


@dataclass(frozen=True)
class LabelPrint100x180Context:
    document_id: str
    order_name: str
    template_path: str
    aggregation_csv_path: str
    printer_name: str
    data_source_kind: str
    template_category: str
    label_count: int
    gtin: str
    size: str
    batch: str
    color: str
    manufacture_date: str
    expiration_date: str
    quantity_pairs: int
    quantity_pairs_word: str
    units_per_pack: int
    dispenser_count: int
    package_text: str | None


def list_100x180_templates() -> list[LabelTemplateInfo]:
    if not TEMPLATE_ROOT_DIR.exists():
        return []

    templates: list[LabelTemplateInfo] = []
    for template_path in sorted(TEMPLATE_ROOT_DIR.rglob("*.btw")):
        if template_path.name.lower() == "document1.btw":
            continue

        relative_path = template_path.relative_to(TEMPLATE_ROOT_DIR)
        category = relative_path.parts[0] if relative_path.parts else "Шаблоны"
        templates.append(
            LabelTemplateInfo(
                name=template_path.stem,
                category=category,
                relative_path=str(relative_path).replace("\\", " / "),
                path=str(template_path),
                data_source_kind=_resolve_template_data_source_kind(template_path),
            )
        )

    return templates


def list_hr_100x180_templates() -> list[LabelTemplateInfo]:
    return [item for item in list_100x180_templates() if item.data_source_kind == AGGREGATION_SOURCE_KIND]


def list_aggregation_csv_files() -> list[AggregationCsvInfo]:
    return _list_csv_files(AGGREGATION_CODES_DIR)


def list_marking_csv_files() -> list[AggregationCsvInfo]:
    return _list_csv_files(MARKING_CODES_DIR)


def resolve_order_metadata(order_data: dict[str, Any], df: pd.DataFrame) -> OrderMetadata:
    order_name = str(order_data.get("order_name") or "").strip()
    document_id = str(order_data.get("document_id") or "").strip()
    gtin = _extract_gtin(order_data)
    if not gtin:
        raise BarTenderLabel100x180Error(
            "У выбранного заказа не найден GTIN. Нельзя восстановить товарные данные."
        )

    row = _lookup_nomenclature_row_by_gtin(df, gtin)
    if row is None:
        raise BarTenderLabel100x180Error(
            f"GTIN {gtin} не найден в nomenclature.xlsx. Нельзя определить данные для этикетки."
        )

    size = _extract_size_from_order_name(order_name) or _normalize_size_from_table(row.get(SIZE_COLUMN))
    if not size:
        raise BarTenderLabel100x180Error(
            f"Не удалось определить размер из заявки '{order_name}'."
        )

    batch = _extract_batch_from_order_name(order_name)
    if not batch:
        raise BarTenderLabel100x180Error(
            f"Не удалось определить номер партии из заявки '{order_name}'."
        )

    units_per_pack = _parse_positive_int(row.get(UNITS_COLUMN), field_name="Единиц в упаковке")
    order_quantity = _extract_order_quantity(order_data)

    full_name = _normalize_optional_text(row.get(FULL_NAME_COLUMN)) or _extract_position_name(order_data)
    simpl_name = _normalize_optional_text(row.get(SIMPL_COLUMN)) or _normalize_optional_text(order_data.get("simpl"))

    return OrderMetadata(
        document_id=document_id,
        order_name=order_name,
        gtin=gtin,
        full_name=full_name,
        simpl_name=simpl_name,
        size=size,
        batch=batch,
        color=_normalize_optional_text(row.get(COLOR_COLUMN)),
        units_per_pack=units_per_pack,
        order_quantity=order_quantity,
    )


def build_label_print_context(
    *,
    df: pd.DataFrame,
    order_data: dict[str, Any],
    template_path: str,
    aggregation_csv_path: str,
    printer_name: str,
    manufacture_date: str,
    expiration_date: str,
    quantity_value: str | int | None = None,
) -> LabelPrint100x180Context:
    template_file = Path(template_path)
    if not template_file.exists():
        raise BarTenderLabel100x180Error(f"Шаблон BarTender не найден: {template_file}")

    csv_file = Path(aggregation_csv_path)
    if not csv_file.exists():
        raise BarTenderLabel100x180Error(f"CSV для печати не найден: {csv_file}")

    printer_name_text = str(printer_name or "").strip()
    if not printer_name_text:
        raise BarTenderLabel100x180Error("Не выбран принтер для печати этикеток 100x180.")

    label_count = count_csv_records(csv_file)
    if label_count <= 0:
        raise BarTenderLabel100x180Error(f"В CSV нет строк для печати: {csv_file}")

    manufacture_date_text = _normalize_year_month(manufacture_date, "Дата изготовления")
    expiration_date_text = _normalize_year_month(expiration_date, "Срок годности")

    metadata = resolve_order_metadata(order_data, df)
    data_source_kind = _resolve_template_data_source_kind(template_file)

    quantity_pairs = _parse_quantity_pairs(quantity_value)

    if data_source_kind == AGGREGATION_SOURCE_KIND:
        if quantity_pairs % metadata.units_per_pack != 0:
            raise BarTenderLabel100x180Error(
                "Количество должно быть кратно значению 'Единиц в упаковке'. "
                f"Сейчас: {quantity_pairs}, в упаковке: {metadata.units_per_pack}."
            )

        dispenser_count = quantity_pairs // metadata.units_per_pack
        package_text: str | None = (
            f"({dispenser_count} {_pluralize_ru(dispenser_count, 'диспенсер', 'диспенсера', 'диспенсеров')} "
            f"по {metadata.units_per_pack} {_pluralize_ru(metadata.units_per_pack, 'пара', 'пары', 'пар')})"
        )
    else:
        dispenser_count = 0
        package_text = None

    template_category = _resolve_template_category(template_file)
    return LabelPrint100x180Context(
        document_id=metadata.document_id,
        order_name=metadata.order_name,
        template_path=str(template_file),
        aggregation_csv_path=str(csv_file),
        printer_name=printer_name_text,
        data_source_kind=data_source_kind,
        template_category=template_category,
        label_count=label_count,
        gtin=metadata.gtin,
        size=metadata.size,
        batch=metadata.batch,
        color=metadata.color,
        manufacture_date=manufacture_date_text,
        expiration_date=expiration_date_text,
        quantity_pairs=quantity_pairs,
        quantity_pairs_word=_pluralize_ru(quantity_pairs, "пара", "пары", "пар"),
        units_per_pack=metadata.units_per_pack,
        dispenser_count=dispenser_count,
        package_text=package_text,
    )


def print_100x180_labels(context: LabelPrint100x180Context) -> None:
    with _SDK_PRINT_LOCK:
        _ensure_unique_label_values(Path(context.aggregation_csv_path), context.data_source_kind)
        temp_template_path = _prepare_template_copy(context)
        submitted_to_bartender = False

        try:
            _run_sdk_database_print(
                template_path=temp_template_path,
                csv_path=Path(context.aggregation_csv_path),
                record_count=context.label_count,
                job_name=context.order_name,
                printer_name=context.printer_name,
            )
            submitted_to_bartender = True
        finally:
            if submitted_to_bartender:
                _schedule_delayed_print_artifact_cleanup(temp_template_path)
            else:
                try:
                    temp_template_path.unlink()
                except OSError:
                    pass


def count_csv_records(csv_path: Path) -> int:
    rows, _delimiter = _read_csv_rows(csv_path)
    return len(rows)


def _schedule_delayed_print_artifact_cleanup(
    file_path: Path,
    *,
    delay_seconds: int = TEMP_PRINT_ARTIFACT_RETENTION_SECONDS,
) -> None:
    target_path = Path(file_path)

    def _cleanup() -> None:
        try:
            time.sleep(max(1, int(delay_seconds)))
            target_path.unlink()
        except OSError:
            pass
        except Exception:
            pass

    threading.Thread(
        target=_cleanup,
        name=f"kontur-bt-cleanup-{target_path.stem[:24]}",
        daemon=True,
    ).start()


def _list_csv_files(root_dir: Path) -> list[AggregationCsvInfo]:
    if not root_dir.exists():
        return []

    files: list[AggregationCsvInfo] = []
    for csv_path in root_dir.rglob("*.csv"):
        if not csv_path.is_file():
            continue

        files.append(
            AggregationCsvInfo(
                name=csv_path.stem,
                folder_name=csv_path.parent.name,
                path=str(csv_path),
                record_count=count_csv_records(csv_path),
                modified_timestamp=csv_path.stat().st_mtime,
            )
        )

    files.sort(key=lambda item: item.modified_timestamp, reverse=True)
    return files


def _resolve_template_category(template_path: Path) -> str:
    try:
        return template_path.resolve().relative_to(TEMPLATE_ROOT_DIR.resolve()).parts[0]
    except (ValueError, IndexError):
        return ""


def _resolve_template_data_source_kind(template_path: Path) -> str:
    category = _resolve_template_category(template_path)
    if category == HR_TEMPLATE_GROUP_DIR.name:
        return AGGREGATION_SOURCE_KIND
    return MARKING_SOURCE_KIND


def _prepare_template_copy(context: LabelPrint100x180Context) -> Path:
    import pythoncom
    import win32com.client  # type: ignore

    pythoncom.CoInitialize()
    app = None
    bt_format = None
    temp_template_path = Path(tempfile.gettempdir()) / f"kontur_label_100x180_{uuid.uuid4().hex}.btw"

    try:
        app = win32com.client.DispatchEx("BarTender.Application")
        app.Visible = False
        bt_format = app.Formats.Open(context.template_path, False, "")
        _bind_format_to_selected_printer(bt_format, context.printer_name)

        _configure_template_objects(bt_format, context)
        bt_format.SaveAs(str(temp_template_path), False)
        return temp_template_path
    except BarTenderLabel100x180Error:
        raise
    except Exception as exc:
        raise BarTenderLabel100x180Error(
            f"Не удалось подготовить шаблон 100x180: {exc}"
        ) from exc
    finally:
        if bt_format is not None:
            try:
                bt_format.Close(BT_DO_NOT_SAVE_CHANGES)
            except Exception:
                pass
        if app is not None:
            try:
                app.Quit(BT_DO_NOT_SAVE_CHANGES)
            except Exception:
                pass
        pythoncom.CoUninitialize()


def _bind_format_to_selected_printer(bt_format, printer_name: str) -> bool:
    normalized_printer_name = str(printer_name or "").strip()
    if not normalized_printer_name:
        return False

    try:
        bt_format.PrintSetup.EnablePrompting = False
    except Exception:
        pass

    try:
        bt_format.PrintSetup.PrinterName = normalized_printer_name
        return True
    except Exception:
        # Некоторые ПК/драйверы BarTender не дают менять принтер на COM-этапе.
        # В таком случае не срываем печать: SDK-этап ниже всё равно повторно
        # назначит принтер перед отправкой задания в spooler.
        return False


def _configure_template_objects(bt_format, context: LabelPrint100x180Context) -> None:
    raw_xml = getattr(bt_format.Objects, "ExportDataSourceValuesToXML", "")
    if not raw_xml:
        raise BarTenderLabel100x180Error(
            "BarTender не вернул XML объектов шаблона 100x180."
        )

    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise BarTenderLabel100x180Error(
            f"Не удалось разобрать XML шаблона 100x180: {exc}"
        ) from exc

    object_elements = list(root.findall(".//Object"))
    if not object_elements:
        raise BarTenderLabel100x180Error(
            "В шаблоне 100x180 не найдено ни одного объекта с источником данных."
        )

    details_object = _find_text_object_by_markers(
        object_elements,
        required_markers=("Размер", "Партия", "Дата изготовления", "Количество"),
    )
    description_object = _find_optional_text_object_by_marker(object_elements, "цвет:")
    serial_text_object = _find_optional_serial_text_object(
        object_elements,
        excluded_objects=(details_object, description_object),
    )
    copies_object = _find_object_by_type(object_elements, COPIES_OBJECT_TYPE, required=False)
    serial_object = _find_object_by_type(object_elements, SERIAL_OBJECT_TYPE, required=False)

    _update_details_object(details_object, context)

    if description_object is not None:
        _update_description_object(description_object, context.color)

    if serial_text_object is not None:
        _reset_serial_text_object(serial_text_object)
    if copies_object is not None:
        _write_first_substring_value(copies_object, "1")
    if serial_object is not None:
        _write_first_substring_value(serial_object, "1")

    try:
        bt_format.Objects.ImportDataSourceValuesFromXML(ET.tostring(root, encoding="unicode"))
    except Exception as exc:
        raise BarTenderLabel100x180Error(
            f"Не удалось применить изменения к шаблону 100x180: {exc}"
        ) from exc


def _update_details_object(details_object: ET.Element, context: LabelPrint100x180Context) -> None:
    values = _get_substring_values(details_object)
    updated_fields = {
        "size": False,
        "batch": False,
        "manufacture_date": False,
        "expiration_date": False,
        "quantity": False,
    }

    for index, value in enumerate(values):
        normalized_value = value.lower()

        if "размер" in normalized_value:
            updated_fields["size"] |= _replace_field_value(
                values, index, "Размер", context.size, allow_adjacent=True
            )

        if "партия" in normalized_value:
            updated_fields["batch"] |= _replace_field_value(
                values, index, "Партия", context.batch, allow_adjacent=True
            )

        if "дата изготовления" in normalized_value:
            updated_fields["manufacture_date"] |= _replace_field_value(
                values, index, "Дата изготовления", context.manufacture_date, allow_adjacent=True
            )

        if "срок годности" in normalized_value:
            updated_fields["expiration_date"] |= _replace_field_value(
                values, index, "Срок годности", context.expiration_date, allow_adjacent=True
            )

        if "количество" in normalized_value:
            updated_fields["quantity"] |= _replace_quantity_value(values, index, context)

    if not all(updated_fields.values()):
        missing = ", ".join(field for field, is_updated in updated_fields.items() if not is_updated)
        raise BarTenderLabel100x180Error(
            f"Не удалось обновить все динамические поля блока 100x180. Не найдены: {missing}."
        )

    _set_substring_values(details_object, values)


def _replace_field_value(
    values: list[str],
    index: int,
    label: str,
    replacement: str,
    *,
    allow_adjacent: bool,
) -> bool:
    updated = False
    has_adjacent_value = allow_adjacent and index + 1 < len(values) and _is_plain_value_substring(values[index + 1])
    adjacent_original_value = values[index + 1] if has_adjacent_value else ""

    new_current_value, inline_updated = _replace_inline_field_value(values[index], label, replacement)
    if inline_updated:
        if has_adjacent_value and adjacent_original_value.strip():
            values[index] = _strip_inline_field_value(new_current_value, label)
        else:
            values[index] = new_current_value
        updated = True

    if has_adjacent_value and (adjacent_original_value.strip() or not inline_updated):
        values[index + 1] = _replace_preserving_linebreak(values[index + 1], replacement)
        updated = True

    return updated


def _replace_quantity_value(
    values: list[str], index: int, context: LabelPrint100x180Context
) -> bool:
    updated = False

    new_current_value, inline_updated = _replace_inline_quantity_value(
        values[index], context.quantity_pairs, context.quantity_pairs_word
    )
    if inline_updated:
        values[index] = new_current_value
        updated = True

    if index + 1 < len(values) and _is_plain_value_substring(values[index + 1]):
        if context.package_text:
            if _string_contains_digits(values[index]):
                next_value = context.quantity_pairs_word
            else:
                next_value = f"{context.quantity_pairs} {context.quantity_pairs_word}"

            next_value = f"{next_value}\r   {context.package_text}"
        else:
            next_value = _replace_adjacent_quantity_digits(values[index], values[index + 1], context.quantity_pairs)

        values[index + 1] = _replace_preserving_linebreak(values[index + 1], next_value)
        updated = True

    return updated


def _replace_inline_field_value(value: str, label: str, replacement: str) -> tuple[str, bool]:
    pattern = re.compile(rf"(?iu)({re.escape(label)}\s*)(\S[^\r\n]*)")
    updated_value, replaced_count = pattern.subn(
        lambda match: f"{match.group(1)}{replacement}",
        value,
        count=1,
    )
    return updated_value, replaced_count > 0


def _strip_inline_field_value(value: str, label: str) -> str:
    pattern = re.compile(rf"(?iu)({re.escape(label)}\s*)(\S[^\r\n]*)")
    stripped_value, _replaced_count = pattern.subn(
        lambda match: match.group(1),
        value,
        count=1,
    )
    return stripped_value


def _replace_inline_quantity_value(
    value: str, quantity_pairs: int, quantity_pairs_word: str
) -> tuple[str, bool]:
    updated = False

    value, replaced_count = re.subn(
        r"(?iu)(Количество\s*)(\d+)",
        lambda match: f"{match.group(1)}{quantity_pairs}",
        value,
        count=1,
    )
    if replaced_count:
        updated = True

    return value, updated


def _is_plain_value_substring(value: str) -> bool:
    stripped_value = value.strip()
    if not stripped_value:
        return True
    return not _contains_detail_marker(stripped_value)


def _contains_detail_marker(value: str) -> bool:
    normalized_value = value.lower()
    return any(marker in normalized_value for marker in DETAIL_FIELD_MARKERS)


def _update_description_object(description_object: ET.Element, color: str) -> None:
    values = _get_substring_values(description_object)
    updated = False

    for index, value in enumerate(values):
        if "цвет:" not in value.lower():
            continue

        if color:
            values[index] = re.sub(
                r"(?i)(цвет:\s*)([^\r\n]+)",
                lambda match: f"{match.group(1)}{color}",
                value,
                count=1,
            )
        else:
            values[index] = re.sub(
                r"(?i)(^|\r)-?цвет:\s*[^\r\n]*(?=\r|$)",
                lambda match: match.group(1),
                value,
                count=1,
            )
            values[index] = re.sub(r"\r{2,}", "\r", values[index])
        updated = True

    if updated:
        _set_substring_values(description_object, values)


def _find_text_object_by_markers(
    object_elements: list[ET.Element], *, required_markers: tuple[str, ...]
) -> ET.Element:
    for element in object_elements:
        if element.attrib.get("Type") != TEXT_OBJECT_TYPE:
            continue

        values = _get_substring_values(element)
        joined = "\n".join(values).lower()
        if all(marker.lower() in joined for marker in required_markers):
            return element

    raise BarTenderLabel100x180Error(
        "Не найден текстовый объект шаблона 100x180 со всеми обязательными полями "
        f"{', '.join(required_markers)}."
    )


def _find_optional_text_object_by_marker(object_elements: list[ET.Element], marker: str) -> ET.Element | None:
    for element in object_elements:
        if element.attrib.get("Type") != TEXT_OBJECT_TYPE:
            continue

        if any(marker.lower() in value.lower() for value in _get_substring_values(element)):
            return element

    return None


def _find_optional_serial_text_object(
    object_elements: list[ET.Element],
    *,
    excluded_objects: tuple[ET.Element | None, ...] = (),
) -> ET.Element | None:
    excluded_ids = {id(element) for element in excluded_objects if element is not None}
    candidates: list[tuple[int, str, ET.Element]] = []

    for element in object_elements:
        if id(element) in excluded_ids or element.attrib.get("Type") != TEXT_OBJECT_TYPE:
            continue

        joined_value = "".join(_get_substring_values(element)).strip()
        if not joined_value.isdigit():
            continue

        candidates.append((len(joined_value), element.attrib.get("Name", ""), element))

    if not candidates:
        return None

    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[0][2]


def _find_object_by_type(
    object_elements: list[ET.Element], object_type: str, *, required: bool
) -> ET.Element | None:
    for element in object_elements:
        if element.attrib.get("Type") == object_type:
            return element

    if required:
        raise BarTenderLabel100x180Error(f"В шаблоне 100x180 не найден объект типа {object_type}.")

    return None


def _get_substring_values(object_element: ET.Element) -> list[str]:
    return [_decode_value(node.text) for node in object_element.findall("./SubString/Value")]


def _set_substring_values(object_element: ET.Element, values: list[str]) -> None:
    value_nodes = object_element.findall("./SubString/Value")
    if len(value_nodes) != len(values):
        raise BarTenderLabel100x180Error(
            f"У объекта '{object_element.attrib.get('Name', '<без имени>')}' неожиданно изменилось число подстрок."
        )

    for value_node, value in zip(value_nodes, values):
        value_node.text = _encode_value(value)


def _write_first_substring_value(object_element: ET.Element, value: str) -> None:
    values = _get_substring_values(object_element)
    if not values:
        raise BarTenderLabel100x180Error(
            f"У объекта '{object_element.attrib.get('Name', '<без имени>')}' нет подстрок для записи."
        )
    values[0] = value
    _set_substring_values(object_element, values)


def _reset_serial_text_object(object_element: ET.Element) -> None:
    values = _get_substring_values(object_element)
    if not values:
        return

    values[0] = _replace_preserving_linebreak(values[0], _build_serial_seed(values[0]))
    _set_substring_values(object_element, values)


def _decode_value(raw_value: str | None) -> str:
    if not raw_value:
        return ""
    try:
        return base64.b64decode(raw_value).decode("utf-16le")
    except Exception:
        return ""


def _encode_value(value: str) -> str:
    return base64.b64encode(str(value).encode("utf-16le")).decode("ascii")


def _replace_preserving_linebreak(original_value: str, new_value: str) -> str:
    linebreak_suffix = ""
    for candidate in ("\r\n", "\r", "\n"):
        if original_value.endswith(candidate):
            linebreak_suffix = candidate
            break
    base_value = original_value[: -len(linebreak_suffix)] if linebreak_suffix else original_value
    if not base_value:
        return f"{new_value}{linebreak_suffix}"

    leading_whitespace = re.match(r"^\s*", base_value)
    trailing_whitespace = re.search(r"\s*$", base_value)
    prefix = leading_whitespace.group(0) if leading_whitespace else ""
    suffix = trailing_whitespace.group(0) if trailing_whitespace else ""

    if not base_value.strip():
        return f"{prefix}{new_value}{linebreak_suffix}"

    return f"{prefix}{new_value}{suffix}{linebreak_suffix}"


def _parse_quantity_pairs(quantity_value: str | int | None) -> int:
    raw_value = str(quantity_value or "").strip().replace(" ", "")
    if not raw_value:
        raise BarTenderLabel100x180Error("Заполните поле 'Количество'.")

    try:
        quantity_pairs = int(raw_value)
    except ValueError as exc:
        raise BarTenderLabel100x180Error(
            "Поле 'Количество' должно содержать целое число."
        ) from exc

    if quantity_pairs <= 0:
        raise BarTenderLabel100x180Error("Количество должно быть больше нуля.")

    return quantity_pairs


def _normalize_year_month(value: str | int | None, field_name: str) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        raise BarTenderLabel100x180Error(
            "Заполните поля 'Дата изготовления' и 'Срок годности'."
        )

    supported_formats = (
        "%Y-%m",
        "%Y.%m",
        "%Y/%m",
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%d.%m.%Y",
        "%d/%m/%Y",
    )

    for date_format in supported_formats:
        try:
            parsed_date = datetime.strptime(raw_value, date_format)
            return parsed_date.strftime("%Y-%m")
        except ValueError:
            continue

    raise BarTenderLabel100x180Error(
        f"Поле '{field_name}' укажите в формате ГГГГ-ММ, например 2026-01."
    )


def _parse_positive_int(value: Any, *, field_name: str) -> int:
    try:
        normalized = str(value).strip().replace(",", ".")
        parsed_value = int(float(normalized))
    except (TypeError, ValueError) as exc:
        raise BarTenderLabel100x180Error(
            f"Не удалось определить '{field_name}' из справочника GTIN."
        ) from exc

    if parsed_value <= 0:
        raise BarTenderLabel100x180Error(
            f"Значение '{field_name}' должно быть больше нуля."
        )

    return parsed_value


def _normalize_optional_text(value: Any) -> str:
    if value is None:
        return ""

    try:
        is_na = pd.isna(value)
    except Exception:
        is_na = False

    if isinstance(is_na, bool) and is_na:
        return ""

    prepared = str(value).strip()
    if prepared.lower() in {"nan", "none", "null", "nat"}:
        return ""
    return prepared


def _extract_gtin(order_data: dict[str, Any]) -> str:
    direct_gtin = str(order_data.get("gtin") or "").strip()
    if direct_gtin:
        return direct_gtin

    positions = order_data.get("positions") or []
    if positions:
        return str((positions[0] or {}).get("gtin") or "").strip()

    return ""


def _normalize_gtin_value(value: Any) -> str:
    prepared = _normalize_optional_text(value)
    if not prepared:
        return ""

    digits_only = re.sub(r"\D", "", prepared)
    if not digits_only:
        return prepared

    normalized = digits_only.lstrip("0")
    return normalized or "0"


def _lookup_nomenclature_row_by_gtin(df: pd.DataFrame, gtin: str) -> pd.Series | None:
    if GTIN_COLUMN not in df.columns:
        return None

    target_gtin = _normalize_gtin_value(gtin)
    if not target_gtin:
        return None

    normalized_gtins = df[GTIN_COLUMN].map(_normalize_gtin_value)
    match = df[normalized_gtins == target_gtin]
    if match.empty:
        return None

    return match.iloc[0]


def _extract_position_name(order_data: dict[str, Any]) -> str:
    positions = order_data.get("positions") or []
    if not positions:
        return ""
    return str((positions[0] or {}).get("name") or "").strip()


def _extract_order_quantity(order_data: dict[str, Any]) -> int:
    positions = order_data.get("positions") or []
    if not positions:
        return 0
    try:
        return int((positions[0] or {}).get("quantity") or 0)
    except (TypeError, ValueError):
        return 0


def _extract_size_from_order_name(order_name: str) -> str:
    normalized_order_name = str(order_name or "").upper()
    match = re.search(r"\b(XXL|XL|L|M|S|XS)\b", normalized_order_name)
    if match:
        return match.group(1)

    numeric_match = re.search(r"\b(\d+[.,]\d+)\b", str(order_name or ""))
    return numeric_match.group(1) if numeric_match else ""


def _extract_batch_from_order_name(order_name: str) -> str:
    match = re.search(r"\b(\d{6})\b", str(order_name or ""))
    return match.group(1) if match else ""


def _normalize_size_from_table(raw_size: Any) -> str:
    value = _normalize_optional_text(raw_size).upper()
    bracket_match = re.search(r"\(([^)]+)\)", value)
    if bracket_match:
        return bracket_match.group(1).strip().upper()

    for candidate in ("XXL", "XL", "L", "M", "S", "XS"):
        if candidate in value:
            return candidate

    numeric_match = re.search(r"(\d+[.,]\d+|\d+)", value)
    if numeric_match:
        return numeric_match.group(1)

    return value


def _read_csv_rows(csv_path: Path) -> tuple[list[list[str]], str]:
    encodings = ("utf-8-sig", "utf-8", "cp1251")
    last_error: Exception | None = None

    for encoding in encodings:
        try:
            sample_line = ""
            with csv_path.open("r", encoding=encoding, newline="") as csv_file:
                for raw_line in csv_file:
                    if str(raw_line or "").strip():
                        sample_line = raw_line
                        break

            delimiter = "\t"
            if ";" in sample_line and "\t" not in sample_line:
                delimiter = ";"
            elif "," in sample_line and "\t" not in sample_line and ";" not in sample_line:
                delimiter = ","

            rows: list[list[str]] = []
            with csv_path.open("r", encoding=encoding, newline="") as csv_file:
                reader = csv.reader(csv_file, delimiter=delimiter)
                for row in reader:
                    prepared = [str(cell or "") for cell in row]
                    if len(prepared) == 1:
                        raw_line = str(prepared[0] or "")
                        if delimiter == "\t" and "\t" in raw_line:
                            prepared = raw_line.split("\t")
                        elif delimiter == ";" and ";" in raw_line:
                            prepared = raw_line.split(";")
                        elif delimiter == "," and "," in raw_line:
                            prepared = raw_line.split(",")
                    if any(cell.strip() for cell in prepared):
                        rows.append(prepared)

            return rows, delimiter
        except UnicodeDecodeError as exc:
            last_error = exc

    if last_error is not None:
        raise BarTenderLabel100x180Error(f"Не удалось прочитать CSV для печати: {csv_path}") from last_error
    raise BarTenderLabel100x180Error(f"Не удалось прочитать CSV для печати: {csv_path}")


def _ensure_unique_label_values(csv_path: Path, data_source_kind: str) -> None:
    rows, _delimiter = _read_csv_rows(csv_path)
    if len(rows) < 2:
        return

    seen_values: dict[str, int] = {}
    duplicate_values: list[str] = []

    for row in rows:
        if not row:
            continue
        primary_value = str(row[0] or "").strip()
        if not primary_value:
            continue
        if primary_value in seen_values:
            duplicate_values.append(primary_value)
            continue
        seen_values[primary_value] = 1

    if not duplicate_values:
        return

    entity_label = "коды маркировки" if data_source_kind == MARKING_SOURCE_KIND else "коды для печати"
    preview = ", ".join(_format_duplicate_value_preview(value) for value in duplicate_values[:5])
    raise BarTenderLabel100x180Error(
        f"В CSV для печати найдены дублирующиеся {entity_label}: {len(duplicate_values)} шт. "
        f"Примеры: {preview}"
    )


def _format_duplicate_value_preview(value: str, *, limit: int = 64) -> str:
    prepared = str(value or "").replace("\x1d", "\\x1d").strip()
    if len(prepared) <= limit:
        return prepared
    return f"{prepared[: limit - 1]}…"


def _string_contains_digits(value: str) -> bool:
    return bool(re.search(r"\d", str(value or "")))


def _replace_adjacent_quantity_digits(current_value: str, adjacent_value: str, quantity_pairs: int) -> str:
    if _string_contains_digits(current_value):
        if not _string_contains_digits(adjacent_value):
            return adjacent_value

        trimmed_adjacent = re.sub(r"^\s*\d+\s*", " ", str(adjacent_value or ""), count=1)
        if trimmed_adjacent != str(adjacent_value or ""):
            return trimmed_adjacent
        return adjacent_value

    updated_value, replaced_count = re.subn(
        r"\d+",
        str(quantity_pairs),
        str(adjacent_value or ""),
        count=1,
    )
    if not replaced_count:
        return adjacent_value

    updated_value = re.sub(
        rf"^{quantity_pairs}(?=[^\d\s\r\n])",
        f"{quantity_pairs} ",
        updated_value,
        count=1,
    )
    return updated_value


def _build_serial_seed(value: str) -> str:
    stripped_value = str(value or "").strip()
    if stripped_value.isdigit() and len(stripped_value) > 1:
        return str(1).zfill(len(stripped_value))
    return "1"


def _pluralize_ru(value: int, singular: str, few: str, many: str) -> str:
    remainder10 = value % 10
    remainder100 = value % 100

    if remainder10 == 1 and remainder100 != 11:
        return singular
    if remainder10 in (2, 3, 4) and remainder100 not in (12, 13, 14):
        return few
    return many


def _run_sdk_database_print(
    template_path: Path,
    csv_path: Path,
    record_count: int,
    job_name: str,
    printer_name: str | None = None,
    print_now: bool = True,
) -> None:
    if not BARTENDER_SDK_DLL.exists():
        raise BarTenderLabel100x180Error(f"Не найден BarTender Print SDK: {BARTENDER_SDK_DLL}")

    script_path = Path(tempfile.gettempdir()) / f"kontur_100x180_sdk_{uuid.uuid4().hex}.ps1"
    script_path.write_text(_build_powershell_script(), encoding="utf-8-sig")

    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    command = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script_path),
        str(template_path),
        str(csv_path),
        str(record_count),
        str(job_name),
        str(printer_name or ""),
        str(BARTENDER_SDK_DLL),
        "1" if print_now else "0",
    ]

    last_error = ""
    try:
        for attempt in range(1, PRINT_SUBMIT_ATTEMPTS + 1):
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    creationflags=creationflags,
                    timeout=PRINT_SUBPROCESS_TIMEOUT_SECONDS,
                )
            except FileNotFoundError as exc:
                raise BarTenderLabel100x180Error(
                    "Не найден powershell.exe. Без него нельзя запустить печать этикеток 100x180."
                ) from exc
            except subprocess.TimeoutExpired as exc:
                last_error = (
                    f"BarTender SDK не ответил за {PRINT_SUBPROCESS_TIMEOUT_SECONDS} сек. "
                    "Повтор автоматически не выполнялся, чтобы не напечатать дубли."
                )
                break

            if completed.returncode == 0:
                return

            last_error = _extract_process_error(completed.stderr or completed.stdout)
            if completed.returncode == 10:
                break
            if attempt < PRINT_SUBMIT_ATTEMPTS:
                time.sleep(PRINT_SUBMIT_RETRY_DELAY_SECONDS)
    finally:
        try:
            script_path.unlink()
        except OSError:
            pass

    raise BarTenderLabel100x180Error(
        "Не удалось отправить печать этикеток 100x180 в BarTender."
        + (f" Причина: {last_error}" if last_error else "")
    )


def _build_powershell_script() -> str:
    return """
param(
    [string]$TemplatePath,
    [string]$CsvPath,
    [int]$RecordCount,
    [string]$JobName,
    [string]$PrinterName,
    [string]$SdkPath,
    [string]$PrintNow
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

Add-Type -Path $SdkPath

function Format-BarTenderMessages([Seagull.BarTender.Print.Messages]$Messages) {
    if ($null -eq $Messages -or $Messages.Count -lt 1) {
        return ''
    }

    $parts = @()
    foreach ($btMessage in $Messages) {
        $text = [string]$btMessage.Text
        if ($text) {
            $parts += ('{0}: {1}' -f $btMessage.Severity, $text)
        }
    }
    return ($parts -join ' | ')
}

function Test-BarTenderMessagesHaveError([Seagull.BarTender.Print.Messages]$Messages) {
    if ($null -eq $Messages -or $Messages.Count -lt 1) {
        return $false
    }
    return [bool]$Messages.HasError
}

$engine = $null
$format = $null

try {
    $engine = New-Object Seagull.BarTender.Print.Engine $true
    $format = $engine.Documents.Open($TemplatePath)

    if ($format.DatabaseConnections.Count -lt 1) {
        throw 'В шаблоне 100x180 нет подключенной текстовой базы данных.'
    }

    if ($RecordCount -lt 1) {
        throw 'В CSV нет записей для печати.'
    }

    $currentDb = $format.DatabaseConnections.Item(0)
    $newDb = New-Object Seagull.BarTender.Print.Database.TextFile($currentDb.Name)
    $newDb.FileName = $CsvPath
    $newDb.Delimitation = $currentDb.Delimitation
    $newDb.FieldDelimiter = $currentDb.FieldDelimiter
    $newDb.NumberOfFields = $currentDb.NumberOfFields
    $newDb.UseFieldNamesFromFirstRecord = $currentDb.UseFieldNamesFromFirstRecord

    $format.DatabaseConnections.SetDatabaseConnection($newDb)
    $format.PrintSetup.UseDatabase = $true
    $format.PrintSetup.ReloadTextDatabaseFields = $true
    $format.PrintSetup.EnablePrompting = $false
    $format.PrintSetup.SelectRecordsAtPrint = $false
    $format.PrintSetup.RecordRange = ('1-' + $RecordCount)

    if ($format.PrintSetup.SupportsIdenticalCopies) {
        $format.PrintSetup.IdenticalCopiesOfLabel = 1
    }

    if ($format.PrintSetup.SupportsSerializedLabels) {
        $format.PrintSetup.NumberOfSerializedLabels = 1
    }

    if ($JobName) {
        $format.PrintSetup.JobName = $JobName
    }

    if ($PrinterName) {
        $printer = Get-CimInstance Win32_Printer -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -eq $PrinterName } |
            Select-Object -First 1
        if ($null -eq $printer) {
            throw "Принтер '$PrinterName' не найден в Windows."
        }
        if ($printer.WorkOffline) {
            throw "Принтер '$PrinterName' сейчас в автономном режиме."
        }
        $format.PrintSetup.PrinterName = $PrinterName
    }

    if ($PrintNow -eq '1') {
        [Seagull.BarTender.Print.Messages]$messages = New-Object Seagull.BarTender.Print.Messages
        $result = $format.Print($JobName, 60000, [ref]$messages)
        $messageText = Format-BarTenderMessages $messages
        $hasErrorMessages = Test-BarTenderMessagesHaveError $messages
        if ($result -ne [Seagull.BarTender.Print.Result]::Success -and $hasErrorMessages) {
            throw ("BarTender вернул статус {0}.{1}" -f $result, $(if ($messageText) { " $messageText" } else { '' }))
        }
        if ($result -ne [Seagull.BarTender.Print.Result]::Success -and -not $messageText) {
            throw ("BarTender вернул статус {0} без подробного сообщения." -f $result)
        }
        if ($messageText) {
            [Console]::Out.WriteLine($messageText)
        }
    }
}
catch {
    $message = $_.Exception.Message
    if (-not $message) {
        $message = $_.ToString()
    }

    [Console]::Error.WriteLine($message)
    if ($message -like 'BarTender вернул статус*') {
        exit 10
    }
    exit 1
}
finally {
    if ($format -ne $null) {
        $format.Close([Seagull.BarTender.Print.SaveOptions]::DoNotSaveChanges)
    }

    if ($engine -ne $null) {
        $engine.Stop()
    }
}
""".strip()


def _extract_process_error(raw_output: str) -> str:
    lines = [line.strip() for line in str(raw_output or "").splitlines() if line.strip()]
    if not lines:
        return ""
    if len(lines) == 1:
        return lines[0]
    return " | ".join(lines[-3:])

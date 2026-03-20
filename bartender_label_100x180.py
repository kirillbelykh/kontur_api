from __future__ import annotations

import base64
import csv
import re
import subprocess
import tempfile
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

    return OrderMetadata(
        document_id=document_id,
        order_name=order_name,
        gtin=gtin,
        full_name=str(row.get(FULL_NAME_COLUMN) or _extract_position_name(order_data) or "").strip(),
        simpl_name=str(row.get(SIMPL_COLUMN) or order_data.get("simpl") or "").strip(),
        size=size,
        batch=batch,
        color=str(row.get(COLOR_COLUMN) or "").strip(),
        units_per_pack=units_per_pack,
        order_quantity=order_quantity,
    )


def build_label_print_context(
    *,
    df: pd.DataFrame,
    order_data: dict[str, Any],
    template_path: str,
    aggregation_csv_path: str,
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

    label_count = count_csv_records(csv_file)
    if label_count <= 0:
        raise BarTenderLabel100x180Error(f"В CSV нет строк для печати: {csv_file}")

    manufacture_date_text = _normalize_year_month(manufacture_date, "Дата изготовления")
    expiration_date_text = _normalize_year_month(expiration_date, "Срок годности")

    metadata = resolve_order_metadata(order_data, df)
    data_source_kind = _resolve_template_data_source_kind(template_file)

    if data_source_kind == AGGREGATION_SOURCE_KIND:
        quantity_pairs = _parse_quantity_pairs(quantity_value)
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
        quantity_pairs = metadata.units_per_pack
        dispenser_count = 0
        package_text = None

    template_category = _resolve_template_category(template_file)
    return LabelPrint100x180Context(
        document_id=metadata.document_id,
        order_name=metadata.order_name,
        template_path=str(template_file),
        aggregation_csv_path=str(csv_file),
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
    temp_template_path = _prepare_template_copy(context)

    try:
        _run_sdk_database_print(
            template_path=temp_template_path,
            csv_path=Path(context.aggregation_csv_path),
            record_count=context.label_count,
            job_name=context.order_name,
        )
    finally:
        try:
            temp_template_path.unlink()
        except OSError:
            pass


def count_csv_records(csv_path: Path) -> int:
    record_count = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if any(str(cell).strip() for cell in row):
                record_count += 1
    return record_count


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
    copies_object = _find_object_by_type(object_elements, COPIES_OBJECT_TYPE, required=False)
    serial_object = _find_object_by_type(object_elements, SERIAL_OBJECT_TYPE, required=False)

    _update_details_object(details_object, context)

    if description_object is not None and context.color:
        _update_description_object(description_object, context.color)

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

    new_current_value, inline_updated = _replace_inline_field_value(values[index], label, replacement)
    if inline_updated:
        values[index] = new_current_value
        updated = True

    if allow_adjacent and index + 1 < len(values) and _is_plain_value_substring(values[index + 1]):
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
            next_value = f"{context.quantity_pairs_word}\r   {context.package_text}"
        else:
            next_value = context.quantity_pairs_word
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

    value, replaced_count = re.subn(
        r"(?iu)(Количество\s*\d+\s*)(пара|пары|пар)\b",
        lambda match: f"{match.group(1)}{quantity_pairs_word}",
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

        values[index] = re.sub(
            r"(?i)(цвет:\s*)([^\r\n]+)",
            lambda match: f"{match.group(1)}{color}",
            value,
            count=1,
        )
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
    return f"{new_value}{linebreak_suffix}"


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


def _extract_gtin(order_data: dict[str, Any]) -> str:
    direct_gtin = str(order_data.get("gtin") or "").strip()
    if direct_gtin:
        return direct_gtin

    positions = order_data.get("positions") or []
    if positions:
        return str((positions[0] or {}).get("gtin") or "").strip()

    return ""


def _lookup_nomenclature_row_by_gtin(df: pd.DataFrame, gtin: str) -> pd.Series | None:
    if GTIN_COLUMN not in df.columns:
        return None

    match = df[df[GTIN_COLUMN].astype(str).str.strip() == str(gtin).strip()]
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
    value = str(raw_size or "").upper().strip()
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


def _pluralize_ru(value: int, singular: str, few: str, many: str) -> str:
    remainder10 = value % 10
    remainder100 = value % 100

    if remainder10 == 1 and remainder100 != 11:
        return singular
    if remainder10 in (2, 3, 4) and remainder100 not in (12, 13, 14):
        return few
    return many


def _run_sdk_database_print(template_path: Path, csv_path: Path, record_count: int, job_name: str) -> None:
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
        str(BARTENDER_SDK_DLL),
    ]

    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags,
        )
    except FileNotFoundError as exc:
        raise BarTenderLabel100x180Error(
            "Не найден powershell.exe. Без него нельзя запустить печать этикеток 100x180."
        ) from exc
    finally:
        try:
            script_path.unlink()
        except OSError:
            pass

    if completed.returncode != 0:
        process_error = _extract_process_error(completed.stderr or completed.stdout)
        raise BarTenderLabel100x180Error(
            "Не удалось отправить печать этикеток 100x180 в BarTender."
            + (f" Причина: {process_error}" if process_error else "")
        )


def _build_powershell_script() -> str:
    return """
param(
    [string]$TemplatePath,
    [string]$CsvPath,
    [int]$RecordCount,
    [string]$JobName,
    [string]$SdkPath
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

Add-Type -Path $SdkPath

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

    [void]$format.Print($JobName, 60000)
}
catch {
    $message = $_.Exception.Message
    if (-not $message) {
        $message = $_.ToString()
    }

    [Console]::Error.WriteLine($message)
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

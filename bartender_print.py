from __future__ import annotations

import base64
import re
import subprocess
import tempfile
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

from options import size_options


BT_DO_NOT_SAVE_CHANGES = 1
TEMPLATE_DIR = Path(__file__).resolve().parent / "bartender datamatrix"
BARTENDER_SDK_DLL = Path(
    r"C:\Program Files\Seagull\BarTender 2022\SDK\Assemblies\Seagull.BarTender.Print.dll"
)
POWERSHELL_EXE = "powershell.exe"
TEXT_OBJECT_TYPE = "2"
SERIAL_OBJECT_TYPE = "4096"
COPIES_OBJECT_TYPE = "2048"
KNOWN_SIZES = tuple(sorted({str(value).upper() for value in size_options}, key=len, reverse=True))


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
    tokens = [
        token.strip("()[]{}.,;:/\\|_-")
        for token in re.split(r"\s+", normalized_order_name)
        if token.strip()
    ]

    for variant in KNOWN_SIZES:
        normalized_variant = variant.replace(",", ".")
        if any(token == variant or token == normalized_variant for token in tokens):
            return variant

    raise BarTenderPrintError(
        f"Не удалось определить размер из названия заявки '{order_name}'. "
        f"Ожидаю один из размеров: {', '.join(KNOWN_SIZES)}"
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
    temp_template_path = _prepare_template_copy(context)

    try:
        _run_sdk_print(
            template_path=temp_template_path,
            csv_path=Path(context.csv_path),
            label_count=context.label_count,
            job_name=context.order_name,
        )
    finally:
        try:
            temp_template_path.unlink()
        except OSError:
            pass


def _prepare_template_copy(context: PrintContext) -> Path:
    import pythoncom
    import win32com.client  # type: ignore

    pythoncom.CoInitialize()
    app = None
    bt_format = None
    temp_template_path = Path(tempfile.gettempdir()) / f"kontur_bt_{uuid.uuid4().hex}.btw"

    try:
        app = win32com.client.DispatchEx("BarTender.Application")
        app.Visible = False
        bt_format = app.Formats.Open(context.template_path, False, "")

        _configure_template_objects(bt_format, context.size)
        bt_format.SaveAs(str(temp_template_path), False)

        return temp_template_path
    except BarTenderPrintError:
        raise
    except Exception as exc:
        raise BarTenderPrintError(f"Не удалось подготовить временный шаблон BarTender: {exc}") from exc
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


def _configure_template_objects(bt_format, size: str) -> None:
    raw_xml = getattr(bt_format.Objects, "ExportDataSourceValuesToXML", "")
    if not raw_xml:
        raise BarTenderPrintError(
            "BarTender не вернул значения объектов шаблона. Проверьте, что .btw открывается вручную."
        )

    try:
        root = ET.fromstring(raw_xml)
    except ET.ParseError as exc:
        raise BarTenderPrintError(f"Не удалось разобрать XML объектов BarTender: {exc}") from exc

    object_elements = list(root.findall(".//Object"))
    if not object_elements:
        raise BarTenderPrintError("В шаблоне BarTender не найдено ни одного объекта с источником данных.")

    size_object = _find_size_text_object(object_elements)
    serial_text_object = _find_serial_text_object(object_elements)
    serial_source_object = _find_object_by_type(object_elements, SERIAL_OBJECT_TYPE)
    copies_object = _find_object_by_type(object_elements, COPIES_OBJECT_TYPE, required=False)

    if size_object is serial_text_object:
        raise BarTenderPrintError(
            "BarTender вернул только один текстовый объект, поэтому размер и сериализованный номер нельзя настроить раздельно."
        )

    _write_object_value(size_object, size)
    _write_object_value(serial_text_object, "1")
    _write_object_value(serial_source_object, "1")

    if copies_object is not None:
        _write_object_value(copies_object, "1")

    try:
        bt_format.Objects.ImportDataSourceValuesFromXML(ET.tostring(root, encoding="unicode"))
    except Exception as exc:
        raise BarTenderPrintError(f"Не удалось обновить объекты шаблона BarTender: {exc}") from exc


def _find_size_text_object(object_elements: list[ET.Element]) -> ET.Element:
    text_objects = [element for element in object_elements if element.attrib.get("Type") == TEXT_OBJECT_TYPE]
    if not text_objects:
        raise BarTenderPrintError("В шаблоне нет текстового объекта для вывода размера.")

    matching_size_objects = [element for element in text_objects if _is_known_size(_read_object_value(element))]
    if len(matching_size_objects) == 1:
        return matching_size_objects[0]
    if len(matching_size_objects) > 1:
        return matching_size_objects[0]

    non_numeric_objects = [element for element in text_objects if not _read_object_value(element).isdigit()]
    if len(non_numeric_objects) == 1:
        return non_numeric_objects[0]

    raise BarTenderPrintError(
        "Не удалось определить текстовый объект размера в шаблоне BarTender. "
        f"Найдены текстовые объекты: {_describe_objects(text_objects)}"
    )


def _find_serial_text_object(object_elements: list[ET.Element]) -> ET.Element:
    text_objects = [element for element in object_elements if element.attrib.get("Type") == TEXT_OBJECT_TYPE]
    numeric_objects = [element for element in text_objects if _read_object_value(element).isdigit()]

    if len(numeric_objects) == 1:
        return numeric_objects[0]
    if len(numeric_objects) > 1:
        return numeric_objects[0]

    raise BarTenderPrintError(
        "Не удалось определить текстовый объект сериализованного номера в шаблоне BarTender. "
        f"Найдены текстовые объекты: {_describe_objects(text_objects)}"
    )


def _find_object_by_type(
    object_elements: list[ET.Element], object_type: str, *, required: bool = True
) -> ET.Element | None:
    matching_objects = [element for element in object_elements if element.attrib.get("Type") == object_type]
    if matching_objects:
        return matching_objects[0]

    if required:
        raise BarTenderPrintError(
            f"В шаблоне BarTender не найден объект типа {object_type}. "
            f"Найдены объекты: {_describe_objects(object_elements)}"
        )

    return None


def _read_object_value(object_element: ET.Element) -> str:
    value_node = object_element.find("./SubString/Value")
    if value_node is None or not value_node.text:
        return ""

    try:
        return base64.b64decode(value_node.text).decode("utf-16le")
    except Exception:
        return ""


def _write_object_value(object_element: ET.Element, value: str) -> None:
    value_node = object_element.find("./SubString/Value")
    if value_node is None:
        raise BarTenderPrintError(
            f"У объекта '{object_element.attrib.get('Name', '<без имени>')}' нет узла Value для записи."
        )

    value_node.text = base64.b64encode(str(value).encode("utf-16le")).decode("ascii")


def _is_known_size(value: str) -> bool:
    normalized_value = str(value or "").strip().upper()
    normalized_variants = {variant for variant in KNOWN_SIZES}
    normalized_variants.update(variant.replace(",", ".") for variant in KNOWN_SIZES)
    return normalized_value in normalized_variants


def _describe_objects(object_elements: list[ET.Element]) -> str:
    descriptions: list[str] = []
    for element in object_elements:
        name = element.attrib.get("Name", "<без имени>")
        value = _read_object_value(element)
        descriptions.append(f"{name}='{value}'")
    return ", ".join(descriptions) if descriptions else "ничего"


def _run_sdk_print(template_path: Path, csv_path: Path, label_count: int, job_name: str, *, print_now: bool = True) -> None:
    if not BARTENDER_SDK_DLL.exists():
        raise BarTenderPrintError(f"Не найден BarTender Print SDK: {BARTENDER_SDK_DLL}")

    script_path = Path(tempfile.gettempdir()) / f"kontur_bt_sdk_{uuid.uuid4().hex}.ps1"
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
        str(label_count),
        str(job_name),
        str(BARTENDER_SDK_DLL),
        "1" if print_now else "0",
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
        raise BarTenderPrintError("Не найден powershell.exe. Без него нельзя запустить печать через BarTender SDK.") from exc
    finally:
        try:
            script_path.unlink()
        except OSError:
            pass

    if completed.returncode != 0:
        process_error = _extract_process_error(completed.stderr or completed.stdout)
        raise BarTenderPrintError(
            "Не удалось отправить печать в BarTender."
            + (f" Причина: {process_error}" if process_error else "")
        )


def _build_powershell_script() -> str:
    return """
param(
    [string]$TemplatePath,
    [string]$CsvPath,
    [int]$LabelCount,
    [string]$JobName,
    [string]$SdkPath,
    [string]$PrintNow
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
        throw 'В шаблоне BarTender нет подключенной текстовой базы данных.'
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

    if ($JobName) {
        $format.PrintSetup.JobName = $JobName
    }

    if ($format.PrintSetup.SupportsIdenticalCopies) {
        $format.PrintSetup.IdenticalCopiesOfLabel = 1
    }

    if (-not $format.PrintSetup.SupportsSerializedLabels) {
        throw 'Шаблон BarTender не поддерживает сериализованные этикетки.'
    }

    $format.PrintSetup.NumberOfSerializedLabels = $LabelCount

    if ($PrintNow -eq '1') {
        [void]$format.Print($JobName, 60000)
    }
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

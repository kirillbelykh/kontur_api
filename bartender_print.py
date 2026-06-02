from __future__ import annotations

import base64
import json
import re
import subprocess
import tempfile
import threading
import time
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
BARTENDER_EXE_NAME = "bartend.exe"
POWERSHELL_EXE = "powershell.exe"
TEXT_OBJECT_TYPE = "2"
SERIAL_OBJECT_TYPE = "4096"
COPIES_OBJECT_TYPE = "2048"
KNOWN_SIZES = tuple(sorted({str(value).upper() for value in size_options}, key=len, reverse=True))
PRINT_SUBMIT_ATTEMPTS = 2
PRINT_SUBPROCESS_TIMEOUT_SECONDS = 90
PRINT_SUBMIT_RETRY_DELAY_SECONDS = 2
_SDK_PRINT_LOCK = threading.Lock()


class BarTenderPrintError(RuntimeError):
    """Raised when BarTender printing cannot be completed."""


def _is_bartender_process_limit_error(message: str) -> bool:
    normalized = str(message or "").strip().lower()
    if not normalized:
        return False
    return (
        "too many process instances of bartender" in normalized
        or "stop a few bartend.exe instances" in normalized
        or "shared desktop heap" in normalized
    )


def _should_fallback_to_com_print(message: str) -> bool:
    normalized = str(message or "").strip().lower()
    if not normalized:
        return False
    return _is_bartender_process_limit_error(normalized) or (
        "permission to run bartender" in normalized
        or "does not have permission to run bartender" in normalized
    )


def _resolve_bartender_exe_path() -> Path:
    candidate_paths = [
        BARTENDER_SDK_DLL.parents[2] / BARTENDER_EXE_NAME,
        Path(r"C:\Program Files\Seagull\BarTender 2022") / BARTENDER_EXE_NAME,
        Path(r"C:\Program Files (x86)\Seagull\BarTender 2022") / BARTENDER_EXE_NAME,
    ]
    for candidate_path in candidate_paths:
        if candidate_path.exists():
            return candidate_path
    raise BarTenderPrintError(
        f"Не найден {BARTENDER_EXE_NAME}. Проверьте установку BarTender."
    )


def _cleanup_headless_bartender_processes() -> None:
    command = (
        "$targets = Get-Process bartend -ErrorAction SilentlyContinue | "
        "Where-Object { $_.MainWindowHandle -eq 0 }; "
        "if ($targets) { $targets | Stop-Process -Force -ErrorAction SilentlyContinue }"
    )
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    try:
        subprocess.run(
            [
                POWERSHELL_EXE,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                command,
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags,
            timeout=15,
        )
    except Exception:
        pass


@dataclass(frozen=True)
class PrintContext:
    order_name: str
    document_id: str
    csv_path: str
    template_path: str
    printer_name: str
    size: str
    label_count: int
    selected_record_number: int | None


def build_print_context(
    order_name: str,
    document_id: str,
    csv_path: str,
    printer_name: str | None = None,
    selected_record_number: int | None = None,
) -> PrintContext:
    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise BarTenderPrintError(f"CSV-файл не найден: {csv_file}")

    printer_name_text = str(printer_name or "").strip()
    if not printer_name_text:
        raise BarTenderPrintError("Не выбран принтер для печати.")

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
        printer_name=printer_name_text,
        size=size,
        label_count=label_count,
        selected_record_number=selected_record_number,
    )


def list_installed_printers() -> tuple[list[str], str | None]:
    try:
        import win32print  # type: ignore

        printer_flags = win32print.PRINTER_ENUM_LOCAL | getattr(
            win32print, "PRINTER_ENUM_CONNECTIONS", 0
        )
        raw_printers = win32print.EnumPrinters(printer_flags)
        printer_names: list[str] = []

        for printer_info in raw_printers:
            printer_name = ""
            if isinstance(printer_info, tuple) and len(printer_info) >= 3:
                printer_name = str(printer_info[2] or "").strip()
            elif isinstance(printer_info, dict):
                printer_name = str(
                    printer_info.get("pPrinterName") or printer_info.get("name") or ""
                ).strip()

            if printer_name and printer_name not in printer_names:
                printer_names.append(printer_name)

        default_printer = None
        try:
            default_printer = str(win32print.GetDefaultPrinter() or "").strip() or None
        except Exception:
            default_printer = None

        return _normalize_printer_listing(printer_names, default_printer)
    except Exception:
        return _list_installed_printers_via_powershell()


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
    with _SDK_PRINT_LOCK:
        temp_template_path = _prepare_template_copy(context)

        try:
            _run_sdk_print(
                template_path=temp_template_path,
                csv_path=Path(context.csv_path),
                label_count=context.label_count,
                job_name=context.order_name,
                printer_name=context.printer_name,
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
        _bind_format_to_selected_printer(bt_format, context.printer_name)

        _configure_template_objects(
            bt_format,
            context.size,
            selected_record_number=context.selected_record_number,
        )
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
        # На части ПК смена принтера через COM ломается из-за драйвера/порта.
        # Не блокируем печать целиком: ниже PowerShell SDK снова устанавливает
        # выбранный принтер непосредственно перед печатью.
        return False


def _configure_template_objects(
    bt_format,
    size: str,
    *,
    selected_record_number: int | None = None,
) -> None:
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

    serial_text_value = "1" if selected_record_number is None else str(selected_record_number)

    _write_object_value(size_object, size)
    _write_object_value(serial_text_object, serial_text_value)
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


def _run_sdk_print(
    template_path: Path,
    csv_path: Path,
    label_count: int,
    job_name: str,
    *,
    printer_name: str | None = None,
    print_now: bool = True,
) -> None:
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
        str(printer_name or ""),
        str(BARTENDER_SDK_DLL),
        "1" if print_now else "0",
    ]

    last_error = ""
    cleanup_attempted = False
    fallback_error = ""
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
                raise BarTenderPrintError("Не найден powershell.exe. Без него нельзя запустить печать через BarTender SDK.") from exc
            except subprocess.TimeoutExpired:
                last_error = (
                    f"BarTender SDK не ответил за {PRINT_SUBPROCESS_TIMEOUT_SECONDS} сек. "
                    "Повтор автоматически не выполнялся, чтобы не напечатать дубли."
                )
                break

            if completed.returncode == 0:
                return

            last_error = _extract_process_error(completed.stderr or completed.stdout)
            if (
                not cleanup_attempted
                and attempt < PRINT_SUBMIT_ATTEMPTS
                and _is_bartender_process_limit_error(last_error)
            ):
                cleanup_attempted = True
                _cleanup_headless_bartender_processes()
                time.sleep(PRINT_SUBMIT_RETRY_DELAY_SECONDS)
                continue
            if completed.returncode == 10:
                break
            if attempt < PRINT_SUBMIT_ATTEMPTS:
                time.sleep(PRINT_SUBMIT_RETRY_DELAY_SECONDS)
    finally:
        try:
            script_path.unlink()
        except OSError:
            pass

    if print_now and _should_fallback_to_com_print(last_error):
        try:
            _run_com_print(
                template_path=template_path,
                csv_path=csv_path,
                label_count=label_count,
                job_name=job_name,
                printer_name=printer_name,
            )
            return
        except Exception as exc:
            fallback_error = str(exc).strip()
            try:
                _run_bartender_command_line_print(
                    template_path=template_path,
                    csv_path=csv_path,
                    job_name=job_name,
                    printer_name=printer_name,
                )
                return
            except Exception as cli_exc:
                fallback_error = (
                    f"{fallback_error} | Командная печать BarTender тоже не сработала: {cli_exc}"
                ).strip()

    error_parts = []
    if last_error:
        error_parts.append(f"Причина SDK: {last_error}")
    if fallback_error:
        error_parts.append(f"Резервная COM-печать тоже не сработала: {fallback_error}")

    raise BarTenderPrintError(
        "Не удалось отправить печать в BarTender."
        + (f" {' '.join(error_parts)}" if error_parts else "")
    )


def _run_com_print(
    template_path: Path,
    csv_path: Path,
    label_count: int,
    job_name: str,
    *,
    printer_name: str | None = None,
) -> None:
    import pythoncom
    import win32com.client  # type: ignore

    pythoncom.CoInitialize()
    app = None
    bt_format = None

    try:
        app = win32com.client.DispatchEx("BarTender.Application")
        app.Visible = True
        bt_format = app.Formats.Open(str(template_path), False, "")
        _bind_format_to_selected_printer(bt_format, str(printer_name or ""))
        _rebind_com_text_database(bt_format, csv_path)
        _configure_com_print_setup(
            bt_format,
            record_count=label_count,
            job_name=job_name,
        )
        bt_format.PrintOut(False, False)
    except BarTenderPrintError:
        raise
    except Exception as exc:
        raise BarTenderPrintError(str(exc)) from exc
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


def _rebind_com_text_database(bt_format, csv_path: Path) -> None:
    databases = getattr(bt_format, "Databases", None)
    if databases is None:
        raise BarTenderPrintError("В шаблоне BarTender нет Databases для COM-печати.")

    bt_database = None
    access_errors: list[str] = []
    for accessor_name in ("GetDatabase", "Item"):
        accessor = getattr(databases, accessor_name, None)
        if accessor is None:
            continue
        try:
            bt_database = accessor(1)
            if bt_database is not None:
                break
        except Exception as exc:
            access_errors.append(str(exc).strip())

    if bt_database is None:
        details = f": {' | '.join(access_errors)}" if access_errors else ""
        raise BarTenderPrintError(
            "В шаблоне BarTender не удалось найти текстовую базу данных для COM-печати"
            + details
        )

    update_errors: list[str] = []
    text_file = getattr(bt_database, "TextFile", None)
    if text_file is not None:
        try:
            text_file.FileName = str(csv_path)
            return
        except Exception as exc:
            update_errors.append(str(exc).strip())

    try:
        bt_database.FileName = str(csv_path)
        return
    except Exception as exc:
        update_errors.append(str(exc).strip())

    details = f": {' | '.join(update_errors)}" if update_errors else ""
    raise BarTenderPrintError(
        f"Не удалось переназначить CSV для COM-печати BarTender{details}"
    )


def _configure_com_print_setup(bt_format, *, record_count: int, job_name: str) -> None:
    if record_count < 1:
        raise BarTenderPrintError("В CSV нет записей для печати.")

    print_setup = getattr(bt_format, "PrintSetup", None)
    if print_setup is None:
        raise BarTenderPrintError("BarTender не вернул PrintSetup для COM-печати.")

    try:
        bt_format.UseDatabase = True
    except Exception:
        pass

    for attribute_name, attribute_value in (
        ("UseDatabase", True),
        ("ReloadTextDatabaseFields", True),
        ("EnablePrompting", False),
        ("SelectRecordsAtPrint", False),
    ):
        try:
            setattr(print_setup, attribute_name, attribute_value)
        except Exception:
            pass

    if job_name:
        try:
            print_setup.JobName = job_name
        except Exception:
            pass

    try:
        if getattr(print_setup, "SupportsIdenticalCopies", False):
            print_setup.IdenticalCopiesOfLabel = 1
    except Exception:
        pass

    try:
        if getattr(print_setup, "SupportsSerializedLabels", False):
            print_setup.NumberOfSerializedLabels = 1
    except Exception:
        pass

    try:
        print_setup.RecordRange = "1" if record_count == 1 else f"1-{record_count}"
    except Exception as exc:
        raise BarTenderPrintError(
            f"Не удалось настроить RecordRange для COM-печати BarTender: {exc}"
        ) from exc


def _run_bartender_command_line_print(
    template_path: Path,
    csv_path: Path,
    job_name: str,
    *,
    printer_name: str | None = None,
) -> None:
    bartender_exe_path = _resolve_bartender_exe_path()
    command = [
        str(bartender_exe_path),
        "/RUN",
        f"/AF={template_path}",
        f"/D={csv_path}",
        "/P",
        "/X",
        "/NOSPLASH",
    ]
    if printer_name:
        command.append(f"/PRN={printer_name}")
    if job_name:
        command.append(f"/PrintJobName={job_name}")

    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=PRINT_SUBPROCESS_TIMEOUT_SECONDS + 30,
        )
    except FileNotFoundError as exc:
        raise BarTenderPrintError(
            f"Не найден {bartender_exe_path}. Проверьте установку BarTender."
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BarTenderPrintError(
            "BarTender не завершил командную печать вовремя."
        ) from exc

    if completed.returncode != 0:
        error_text = _extract_process_error(completed.stderr or completed.stdout)
        raise BarTenderPrintError(
            "BarTender вернул ошибку при командной печати."
            + (f" Причина: {error_text}" if error_text else "")
        )


def _build_powershell_script() -> str:
    return """
param(
    [string]$TemplatePath,
    [string]$CsvPath,
    [int]$LabelCount,
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

    if ($format.PrintSetup.SupportsIdenticalCopies) {
        $format.PrintSetup.IdenticalCopiesOfLabel = 1
    }

    if (-not $format.PrintSetup.SupportsSerializedLabels) {
        throw 'Шаблон BarTender не поддерживает сериализованные этикетки.'
    }

    if ($LabelCount -lt 1) {
        throw 'В CSV нет записей для печати.'
    }

    if ($LabelCount -eq 1) {
        $format.PrintSetup.RecordRange = '1'
    }
    else {
        $format.PrintSetup.RecordRange = ('1-' + $LabelCount)
    }

    $format.PrintSetup.NumberOfSerializedLabels = 1

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


def _normalize_printer_listing(
    printer_names: list[str], default_printer: str | None
) -> tuple[list[str], str | None]:
    unique_printers = sorted(
        {str(printer or "").strip() for printer in printer_names if str(printer or "").strip()},
        key=str.casefold,
    )
    normalized_default = str(default_printer or "").strip() or None

    if normalized_default and normalized_default not in unique_printers:
        unique_printers.insert(0, normalized_default)

    return unique_printers, normalized_default


def _list_installed_printers_via_powershell() -> tuple[list[str], str | None]:
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    script = """
$printers = @(Get-CimInstance Win32_Printer | Sort-Object Name | Select-Object -ExpandProperty Name)
$defaultPrinter = Get-CimInstance Win32_Printer | Where-Object { $_.Default } | Select-Object -First 1 -ExpandProperty Name
@{
    printers = $printers
    defaultPrinter = $defaultPrinter
} | ConvertTo-Json -Depth 3
""".strip()

    try:
        completed = subprocess.run(
            [
                POWERSHELL_EXE,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                script,
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags,
        )
    except FileNotFoundError:
        return [], None

    if completed.returncode != 0:
        return [], None

    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return [], None

    printers = payload.get("printers") or []
    if isinstance(printers, str):
        printers = [printers]

    default_printer = payload.get("defaultPrinter")
    if isinstance(default_printer, list):
        default_printer = default_printer[0] if default_printer else None

    return _normalize_printer_listing(list(printers), str(default_printer or "").strip() or None)


def _extract_process_error(raw_output: str) -> str:
    lines = [line.strip() for line in str(raw_output or "").splitlines() if line.strip()]
    if not lines:
        return ""

    if len(lines) == 1:
        return lines[0]

    return " | ".join(lines[-3:])

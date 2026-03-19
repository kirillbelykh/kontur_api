import os
import socket
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from dotenv import load_dotenv
from utils import process_csv_file
from logger import logger
from cryptopro import find_certificate_by_thumbprint, sign_data

from history_db import OrderHistoryDB


# ------------------- Инициализация окружения -------------------
load_dotenv()
history_db = OrderHistoryDB()

# ------------------- Константы конфигурации -------------------
BASE: str = os.getenv("BASE_URL", "")
ORGANIZATION_ID: str = os.getenv("ORGANIZATION_ID", "")
WAREHOUSE_ID: str = os.getenv("WAREHOUSE_ID", "")
PRODUCT_GROUP: str = os.getenv("PRODUCT_GROUP", "")
RELEASE_METHOD_TYPE: str = os.getenv("RELEASE_METHOD_TYPE", "")
CIS_TYPE: str = os.getenv("CIS_TYPE", "")
FILLING_METHOD: str = os.getenv("FILLING_METHOD", "")

BASE_URL_CONFIG_ERROR = "BASE_URL не настроен. Укажите BASE_URL в .env"
ORDER_AVAILABILITY_POLL_ATTEMPTS = 10
ORDER_AVAILABILITY_POLL_INTERVAL_SECONDS = 2
ORDER_STATUS_POLL_ATTEMPTS = 30
ORDER_STATUS_POLL_INTERVAL_SECONDS = 10
PDF_EXPORT_POLL_ATTEMPTS = 24
EXPORT_POLL_ATTEMPTS = 60
EXPORT_POLL_INTERVAL_SECONDS = 5


def _require_base_url() -> str:
    base_url = BASE.strip().rstrip("/")
    if not base_url:
        raise RuntimeError(BASE_URL_CONFIG_ERROR)
    return base_url


def _preview_text(value: Any, limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _log_status_change(label: str, entity_id: str, previous_status: Optional[str], current_status: Any) -> str:
    normalized_status = str(current_status or "unknown")
    if normalized_status != previous_status:
        logger.info("%s %s: %s", label, entity_id, normalized_status)
    return normalized_status


def _summarize_document_ids(items: List[Dict[str, Any]], field_name: str) -> str:
    document_ids = [str(item.get(field_name)) for item in items if item.get(field_name)]
    if not document_ids:
        return "без documentId"
    preview = ", ".join(document_ids[:3])
    if len(document_ids) > 3:
        preview += f", +{len(document_ids) - 3} ещё"
    return preview


def _wait_for_order_availability(
    session: requests.Session,
    base_url: str,
    document_id: str,
    document_number: str,
    attempts: int = ORDER_AVAILABILITY_POLL_ATTEMPTS,
    interval_seconds: int = ORDER_AVAILABILITY_POLL_INTERVAL_SECONDS,
) -> bool:
    previous_status = None

    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(f"{base_url}/api/v1/codes-order/{document_id}/availability-status", timeout=15)
            if resp.status_code == 417:
                status = "pending"
            else:
                resp.raise_for_status()
                status = resp.text.strip().strip('"')
        except requests.exceptions.RequestException as e:
            logger.error("Проверка доступности документа %s: %s", document_number, e)
            return False

        previous_status = _log_status_change("Доступность документа", document_number, previous_status, status)
        if status == "available":
            return True

        if attempt < attempts:
            time.sleep(interval_seconds)

    logger.warning(
        "Документ %s не стал доступен за %s сек",
        document_number,
        attempts * interval_seconds,
    )
    return False


def codes_order(session: requests.Session, document_number: str,
                product_group: str, release_method_type: str,
                positions: list[dict],
                filling_method: str = FILLING_METHOD, thumbprint: str | None = None) -> dict | None:
    try:
        base_url = _require_base_url()
    except RuntimeError as e:
        logger.error(str(e))
        return None

    signed_orders_payload: list[dict] = []

    logger.info("Создание документа кодов: %s", document_number)
    url_create = f"{base_url}/api/v1/codes-order?warehouseId={WAREHOUSE_ID}"
    body = {
        "documentNumber": document_number,
        "comment": "",
        "productGroup": product_group,
        "releaseMethodType": release_method_type,
        "fillingMethod": filling_method,
        "cisType": CIS_TYPE,
        "positions": [
            {
                "gtin": p["gtin"],
                "name": p.get("name", ""),
                "tnvedCode": p.get("tnvedCode", ""),
                "quantity": p.get("quantity", 1),
                "certificateDocument": None,
                "setsGtinUnits": []
            } for p in positions
        ]
    }

    try:
        resp = session.post(url_create, json=body, timeout=30)
        resp.raise_for_status()
        created = resp.json()
        document_id = created.get("id") if isinstance(created, dict) else str(created).strip('"')
    except Exception as e:
        logger.error("Создание документа %s: %s", document_number, e)
        return None

    logger.info("Документ создан: %s", document_id)

    if not _wait_for_order_availability(session, base_url, document_id, document_number):
        return None

    # проверка сертификата
    if thumbprint:
        try:
            resp = session.get(
                f"{base_url}/api/v1/organizations/{ORGANIZATION_ID}/employees/has-certificate?thumbprint={thumbprint}",
                timeout=15,
            )
            resp.raise_for_status()
            if not resp.json():
                logger.error("Сертификат не зарегистрирован в организации")
                return None
        except Exception as e:
            logger.error("Проверка сертификата: %s", e)
            return None
    else:
        logger.info("Thumbprint не задан: будет использован первый доступный сертификат с ПК")

    # обновление токена OMS
    cert = find_certificate_by_thumbprint(thumbprint)
    if not cert:
        logger.error(f"Сертификат для подписи не найден (thumbprint={thumbprint})")
        return None

    # получение orders-for-sign
    try:
        resp = session.get(f"{base_url}/api/v1/codes-order/{document_id}/orders-for-sign", timeout=15)
        resp.raise_for_status()
        orders_to_sign = resp.json()
        if not isinstance(orders_to_sign, list):
            logger.error("Некорректный формат orders_for_sign: %s", _preview_text(orders_to_sign))
            return None
    except Exception as e:
        logger.error("Получение данных для подписи: %s", e)
        return None

    logger.info("Документ %s: получено %s частей для подписи", document_number, len(orders_to_sign))

    # подпись каждого order
    for o in orders_to_sign:
        oid = o["id"]
        b64content = o["base64Content"]
        logger.debug("Подписываем order id=%s (base64 length=%s)", oid, len(b64content))
        try:
            signature_b64 = sign_data(cert, b64content, b_detached=True)
            signed_orders_payload.append({"id": oid, "base64Content": signature_b64})
        except Exception as e:
            logger.error("Ошибка подписи order %s: %s", oid, e)
            return None

    # отправка документа
    try:
        send_url = f"{base_url}/api/v1/codes-order/{document_id}/send"
        payload = {"signedOrders": signed_orders_payload}
        r_send = session.post(send_url, json=payload, timeout=30)
        r_send.raise_for_status()
        logger.info("Документ %s отправлен на выпуск", document_number)
    except Exception as e:
        logger.error("Отправка документа %s: %s", document_number, e)
        return None

    # финальный статус
    try:
        r_fin = session.get(f"{base_url}/api/v1/codes-order/{document_id}", timeout=15)
        r_fin.raise_for_status()
        doc = r_fin.json()
        logger.info("Финальный статус документа %s: %s", document_number, doc.get("status"))
        
        # СОХРАНЕНИЕ В ИСТОРИЮ ПРИ УСПЕШНОМ ВЫПОЛНЕНИИ
        try:
            # Извлекаем данные для истории
            product_name = positions[0].get("name", "Неизвестно") if positions else "Неизвестно"
            gtin = positions[0].get("gtin", "") if positions else ""
            
            # Создаем запись для истории
            history_entry = {
                "order_name": document_number,
                "document_id": document_id,
                "status": "Выполнен",  # или другой статус, который вы используете
                "filename": None,  # заполнится при скачивании
                "simpl": product_group,
                "full_name": product_name,
                "gtin": gtin,
                "positions": positions
            }
            
            # Сохраняем в историю
            history_db.add_order(history_entry)
            logger.info("Заказ %s сохранен в историю", document_number)
            
        except Exception as history_error:
            logger.error("Ошибка сохранения в историю: %s", history_error)
        
        return doc
        
    except Exception as e:
        logger.error("Получение финального статуса: %s", e)
        return None


def check_order_status(session: requests.Session, document_id: str) -> str:
    """
    Быстрая проверка статуса заказа без ожидания
    """
    try:
        base_url = _require_base_url()
        resp_status = session.get(f"{base_url}/api/v1/codes-order/{document_id}", timeout=15)
        resp_status.raise_for_status()
        doc = resp_status.json()
        return doc.get("status", "unknown")
    except Exception as e:
        logger.error("Ошибка проверки статуса заказа %s: %s", document_id, e)
        return "error"


def download_codes(session: requests.Session, document_id: str, order_name: str) -> Optional[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    Скачивает PDF, CSV и XLS (если доступны) для заказа document_id и сохраняет их в папку:
      Desktop / "pdf-коды км" / <safe_order_name>/
    Файлы сохраняются с общей базой имени, производной от order_name.
    После скачивания CSV файла автоматически обрабатывает его.
    Возвращает кортеж (pdf_path, csv_path, xls_path). Если файл не скачан — соответствующий элемент = None.
    """
    try:
        base_url = _require_base_url()
    except RuntimeError as e:
        logger.error(str(e))
        return None

    logger.info("Начало скачивания PDF/CSV/XLS для заказа %s (%r)", document_id, order_name)

    def make_full_url(url: str) -> str:
        if not url:
            return ""
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return urljoin(base_url, url)

    # 1) дождаться статуса released (polling)
    max_attempts = ORDER_STATUS_POLL_ATTEMPTS
    attempt = 0
    status = None
    previous_status = None
    while attempt < max_attempts:
        try:
            resp_status = session.get(f"{base_url}/api/v1/codes-order/{document_id}", timeout=15)
            resp_status.raise_for_status()
            doc = resp_status.json()
            status = doc.get("status")
            previous_status = _log_status_change("Статус заказа", document_id, previous_status, status)
            if status in ("released", "received"):
                break
            time.sleep(ORDER_STATUS_POLL_INTERVAL_SECONDS)
            attempt += 1
        except Exception as e:
            logger.error("Ошибка проверки статуса заказа %s: %s", document_id, e)
            return None

    if status not in ("released", "received"):
        logger.error(
            "Заказ %s не перешёл в 'released' за %s сек",
            document_id,
            max_attempts * ORDER_STATUS_POLL_INTERVAL_SECONDS,
        )
        return None

    # Подготовка каталога для сохранения: Desktop / "pdf-коды км" / <safe_order_name>
    try:
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        parent_dir = os.path.join(desktop, "Коды км")
        # безопасное имя папки
        safe_order_name = "".join(c for c in (order_name or document_id) if c.isalnum() or c in " -_").strip()
        if not safe_order_name:
            safe_order_name = document_id
        safe_order_name = safe_order_name[:120]
        target_dir = os.path.join(parent_dir, safe_order_name)
        os.makedirs(target_dir, exist_ok=True)
        # базовое безопасное имя файла
        safe_base = safe_order_name[:100]
    except Exception as e:
        logger.error("Ошибка при подготовке пути сохранения: %s", e)
        return None

    # Заголовки (включая куки из session)
    cookies_dict = session.cookies.get_dict()
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()]) if cookies_dict else ""
    headers = {"User-Agent": "Mozilla/5.0", "Referer": base_url}
    if cookie_str:
        headers["Cookie"] = cookie_str

    pdf_path = None
    csv_path = None
    xls_path = None

    # ---------------- PDF export ----------------
    try:
        # получить templateId для size "2x2"
        resp_templates = session.get(
            f"{base_url}/api/v1/print-templates?organizationId={ORGANIZATION_ID}&formTypes=codesOrder",
            timeout=15,
        )
        resp_templates.raise_for_status()
        templates = resp_templates.json()
        template_id = None
        for t in templates:
            if t.get("name") == "Этикетка 30x20" or t.get("size") == "30х20" or t.get("dekkoId") == "30x20Template_v2":
                template_id = t.get("id")
                break
        if template_id:
            export_url = f"{base_url}/api/v1/codes-order/{document_id}/export/pdf?splitByGtins=false&templateId={template_id}"
            resp_export = session.post(export_url, timeout=30)
            resp_export.raise_for_status()
            export_data = resp_export.json()
            result_id = export_data.get("resultId")
            logger.info("PDF export запущен для %s, resultId=%s", document_id, result_id)

            # polling результата
            file_url = None
            attempts_pdf = 0
            previous_pdf_status = None
            while attempts_pdf < PDF_EXPORT_POLL_ATTEMPTS:
                resp_result = session.get(f"{base_url}/api/v1/codes-order/{document_id}/export/pdf/{result_id}", timeout=15)
                resp_result.raise_for_status()
                result_data = resp_result.json()
                status_pdf = result_data.get("status")
                previous_pdf_status = _log_status_change("PDF export", document_id, previous_pdf_status, status_pdf)
                if status_pdf == "success":
                    file_infos = result_data.get("fileInfos", [])
                    if file_infos:
                        file_url = file_infos[0].get("fileUrl") or file_infos[0].get("fileUrlAbsolute") or None
                    break
                time.sleep(EXPORT_POLL_INTERVAL_SECONDS)
                attempts_pdf += 1

            if file_url:
                full_file_url = make_full_url(file_url)
                safe_pdf_name = f"{safe_base}.pdf"
                pdf_path = os.path.join(target_dir, safe_pdf_name)
                try:
                    logger.debug(f"Downloading PDF from {full_file_url}")
                    r = session.get(full_file_url, timeout=60, headers=headers, stream=True, allow_redirects=True)
                    r.raise_for_status()
                    with open(pdf_path, "wb") as fh:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                fh.write(chunk)
                    logger.info(f"PDF сохранён: {pdf_path}")
                except Exception as e:
                    logger.error("Ошибка скачивания PDF %s: %s", full_file_url, e)
                    pdf_path = None
            else:
                logger.warning("PDF export для %s завершился без fileUrl", document_id)
        else:
            logger.warning("Шаблон '30x20' для PDF не найден — пропускаем PDF экспорт")
    except Exception as e:
        logger.exception("Ошибка в PDF-части для %s", document_id)
        pdf_path = None

    # ---------------- CSV export ----------------
    try:
        export_csv_url = f"{base_url}/api/v1/codes-order/{document_id}/export/csv?splitByGtins=false"
        resp_csv_export = session.post(export_csv_url, timeout=30)
        resp_csv_export.raise_for_status()
        csv_export_data = resp_csv_export.json()
        csv_result_id = csv_export_data.get("resultId")
        logger.info("CSV export запущен для %s, resultId=%s", document_id, csv_result_id)

        # polling CSV result
        file_infos = None
        attempts_csv = 0
        previous_csv_status = None
        while attempts_csv < EXPORT_POLL_ATTEMPTS:
            resp_csv_status = session.get(f"{base_url}/api/v1/codes-order/{document_id}/export/csv/{csv_result_id}", timeout=15)
            resp_csv_status.raise_for_status()
            csv_status_data = resp_csv_status.json()
            status_csv = csv_status_data.get("status")
            previous_csv_status = _log_status_change("CSV export", document_id, previous_csv_status, status_csv)
            if status_csv == "success":
                file_infos = csv_status_data.get("fileInfos", [])
                break
            time.sleep(EXPORT_POLL_INTERVAL_SECONDS)
            attempts_csv += 1

        if file_infos:
            finfo = file_infos[0]
            file_id = finfo.get("fileId")
            download_csv_url = f"{base_url}/api/v1/codes-order/{document_id}/export/csv/{csv_result_id}/download/{file_id}"
            # защитить имя файла
            safe_csv_name = f"{order_name}_csv.csv"
            if any(ch in safe_csv_name for ch in ("/", "\\", ":", "*", "?", "\"", "<", ">", "|")):
                safe_csv_name = f"{safe_base}.csv"
            csv_path = os.path.join(target_dir, safe_csv_name)
            try:
                logger.debug(f"Downloading CSV from {download_csv_url}")
                r = session.get(download_csv_url, timeout=60, headers=headers, stream=True, allow_redirects=True)
                r.raise_for_status()
                with open(csv_path, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
                logger.info(f"CSV сохранён: {csv_path}")
                if process_csv_file(csv_path):
                    logger.info("CSV обработан: %s", csv_path)
                else:
                    logger.error("Ошибка при обработке CSV файла: %s", csv_path)
                
            except Exception as e:
                logger.error("Ошибка скачивания CSV %s: %s", download_csv_url, e)
                csv_path = None
        else:
            logger.warning("CSV export для %s не вернул fileInfos в пределах таймаута", document_id)
    except Exception as e:
        logger.exception("Ошибка в CSV-части для %s", document_id)
        csv_path = None

    # ---------------- XLS export ----------------
    try:
        export_xls_url = f"{base_url}/api/v1/codes-order/{document_id}/export/xls?splitByGtins=false"
        resp_xls_export = session.post(export_xls_url, timeout=30)
        resp_xls_export.raise_for_status()
        xls_export_data = resp_xls_export.json()
        xls_result_id = xls_export_data.get("resultId")
        logger.info("XLS export запущен для %s, resultId=%s", document_id, xls_result_id)

        # polling XLS result
        file_infos = None
        attempts_xls = 0
        previous_xls_status = None
        while attempts_xls < EXPORT_POLL_ATTEMPTS:
            resp_xls_status = session.get(f"{base_url}/api/v1/codes-order/{document_id}/export/xls/{xls_result_id}", timeout=15)
            resp_xls_status.raise_for_status()
            xls_status_data = resp_xls_status.json()
            status_xls = xls_status_data.get("status")
            previous_xls_status = _log_status_change("XLS export", document_id, previous_xls_status, status_xls)
            if status_xls == "success":
                file_infos = xls_status_data.get("fileInfos", [])
                break
            time.sleep(EXPORT_POLL_INTERVAL_SECONDS)
            attempts_xls += 1

        if file_infos:
            finfo = file_infos[0]
            file_id = finfo.get("fileId")
            download_xls_url = f"{base_url}/api/v1/codes-order/{document_id}/export/xls/{xls_result_id}/download/{file_id}"
            safe_xls_name = f"{order_name}.xls"
            if any(ch in safe_xls_name for ch in ("/", "\\", ":", "*", "?", "\"", "<", ">", "|")):
                safe_xls_name = f"{safe_base}.xls"
            xls_path = os.path.join(target_dir, safe_xls_name)
            try:
                logger.debug(f"Downloading XLS from {download_xls_url}")
                r = session.get(download_xls_url, timeout=60, headers=headers, stream=True, allow_redirects=True)
                r.raise_for_status()
                with open(xls_path, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            fh.write(chunk)
                logger.info(f"XLS сохранён: {xls_path}")
            except Exception as e:
                logger.error("Ошибка скачивания XLS %s: %s", download_xls_url, e)
                xls_path = None
        else:
            logger.warning("XLS export для %s не вернул fileInfos в пределах таймаута", document_id)
    except Exception as e:
        logger.exception("Ошибка в XLS-части для %s", document_id)
        xls_path = None

    # Вернуть кортеж путей (возможно некоторые элементы None)
    return pdf_path, csv_path, xls_path



def put_into_circulation(
    session: requests.Session,
    codes_order_id: str,
    organization_id: str = ORGANIZATION_ID,
    thumbprint: Optional[str] = None,
    # опциональные поля для PATCH /production (если нужно обновить метаданные)
    production_patch: Optional[Dict[str, Any]] = None,
    # автоперезапросы / таймауты
    check_poll_interval: int = 5,      # сек для /codes-checking polling (короткий интервал)
    check_poll_attempts: int = 24,     # сколько попыток (24*5=120s default)       
) -> Tuple[bool, Dict[str, Any]]:
    """
    Выполняет ввод в оборот (codes introduction) для указанного codes_order_id.
    Возвращает (ok: bool, result: dict) где result содержит поля:
      - introduction_id
      - created_introduction (ответ GET после создания)
      - check_status (последний ответ /codes-checking)
      - production (последний GET /production)
      - generate_items (raw items from generate-multiple)
      - send_response (ответ от send-multiple)
      - final_introduction, final_check (последние GET ответы)
      - errors (список сообщений об ошибках)
    """
    result: Dict[str, Any] = {"errors": []}
    try:
        base_url = _require_base_url()
        use_ip_workaround = False

        # 1) create-from-codes-order
        try:
            parsed_base = urlparse(base_url)
            target_host = parsed_base.hostname or "mk.kontur.ru"
            socket.getaddrinfo(target_host, parsed_base.port or 443)
            logger.debug("DNS успешно разрешён для %s", target_host)
            url_create = (
                f"{base_url}/api/v1/codes-introduction/create-from-codes-order/{codes_order_id}"
                "?isImportFts=false&isAccompanyingDocumentNeeds=false"
            )
        except socket.gaierror as dns_err:
            logger.warning("DNS-разрешение для ввода в оборот провалено: %s. Используем fallback по IP.", dns_err)
            # Workaround: Используем IP вместо домена
            ip = '46.17.200.242'
            target_host = parsed_base.hostname or "mk.kontur.ru"
            original_base = base_url.replace(target_host, ip, 1)
            url_create = (
                f"{original_base}/api/v1/codes-introduction/create-from-codes-order/{codes_order_id}"
                "?isImportFts=false&isAccompanyingDocumentNeeds=false"
            )
            use_ip_workaround = True
        # Добавляем заголовок Host для workaround (если используем IP)
        headers = {'Host': target_host} if use_ip_workaround else {}

        if use_ip_workaround:
            # Отключаем проверку SSL только для fallback-запроса на IP.
            r = session.post(url_create, headers=headers, timeout=30, verify=False)
        else:
            r = session.post(url_create, headers=headers, timeout=30)
        logger.info(
            "Создание ввода в оборот для %s: HTTP %s, ответ=%s",
            codes_order_id,
            r.status_code,
            _preview_text(r.text),
        )
        r.raise_for_status()
        intro_id = r.text.strip().strip('"')
        result["introduction_id"] = intro_id
        logger.info("Создана заявка ввода в оборот: %s", intro_id)
        
        # 2) initial GET introduction
        r_intro = session.get(f"{base_url}/api/v1/codes-introduction/{intro_id}", timeout=15)
        r_intro.raise_for_status()
        result["created_introduction"] = r_intro.json()

        # 3) poll codes-checking until status indicates no errors
        check_ok = False
        attempts = 0
        last_check = None
        previous_check_status = None
        while attempts < check_poll_attempts:
            r_check = session.get(f"{base_url}/api/v1/codes-checking/{intro_id}", timeout=15)
            if r_check.status_code == 200:
                last_check = r_check.json()
                result["check_status_latest"] = last_check
                status = last_check.get("status")
                previous_check_status = _log_status_change(
                    "codes-checking",
                    intro_id,
                    previous_check_status,
                    status,
                )
                if status in ("inProgress", "doesNotHaveErrors", "created", "checked", "noErrors"):  # возможные статусы
                    check_ok = True
                    break
            else:
                logger.debug("codes-checking %s returned HTTP %s", intro_id, r_check.status_code)
            attempts += 1
            time.sleep(check_poll_interval)

        if not check_ok:
            msg = f"codes-checking для {intro_id} не перешёл в OK-статус после {check_poll_attempts} попыток"
            logger.warning(msg)
            result["errors"].append(msg)
            # продолжим, возможно всё равно можно отправить (но лучше вернуть ошибку)
            # return False, result

        # 4) GET production (получаем структуру production)
        try:
            r_prod = session.get(f"{base_url}/api/v1/codes-introduction/{intro_id}/production", timeout=15)
            r_prod.raise_for_status()
            result["production"] = r_prod.json()
        except Exception as e:
            logger.warning("Не удалось получить /production для %s: %s", intro_id, e)
            result["errors"].append(f"production GET error: {e}")

        # 5) Если переданы поля для PATCH /production — применим
        if production_patch:
            try:
                patch_url = f"{base_url}/api/v1/codes-introduction/{intro_id}/production"
                logger.info("PATCH production %s", patch_url)
                r_patch = session.patch(patch_url, json=production_patch, timeout=30)
                r_patch.raise_for_status()
                result["production_patch_response"] = r_patch.json() if r_patch.content else {"status": "ok"}
                # обновим production
                r_prod2 = session.get(f"{base_url}/api/v1/codes-introduction/{intro_id}/production", timeout=15)
                r_prod2.raise_for_status()
                result["production_after_patch"] = r_prod2.json()
            except Exception as e:
                logger.exception("Ошибка PATCH production для %s", intro_id)
                result["errors"].append(f"production PATCH error: {e}")

        # 6) попытка автозаполнения позиций (autocomplete) — не обязателен, но делаем если нужно
        try:
            auto_url = f"{base_url}/api/v1/codes-introduction/{intro_id}/positions/autocomplete"
            logger.info("POST positions/autocomplete для %s", intro_id)
            r_auto = session.post(auto_url, timeout=30)
            # многие реализации возвращают 204/200/empty; не обязательно требовать body
            if r_auto.status_code not in (200, 204):
                logger.debug("autocomplete для %s returned HTTP %s", intro_id, r_auto.status_code)
            result["autocomplete_status"] = r_auto.status_code
        except Exception as e:
            logger.warning("autocomplete для %s failed: %s", intro_id, e)
            result["errors"].append(f"autocomplete error: {e}")

        # 7) проверить наличие сертификата у сотрудника в организации
        try:
            if thumbprint:
                r_cert_check = session.get(
                    f"{base_url}/api/v1/organizations/{organization_id}/employees/has-certificate?thumbprint={thumbprint}",
                    timeout=15,
                )
                r_cert_check.raise_for_status()
                has_cert = bool(r_cert_check.json())
                result["has_certificate"] = has_cert
                if not has_cert:
                    msg = "Сертификат с указанным thumbprint не зарегистрирован в организации"
                    logger.error(msg)
                    result["errors"].append(msg)
                    return False, result
        except Exception as e:
            logger.exception("Ошибка проверки сертификата для %s", intro_id)
            result["errors"].append(f"cert check error: {e}")
            return False, result

        # 8) GET generate-multiple -> получим массив объектов base64Content
        try:
            gen_url = f"{base_url}/api/v1/codes-introduction/{intro_id}/generate-multiple"
            r_gen = session.get(gen_url, timeout=30)
            r_gen.raise_for_status()
            gen_items = r_gen.json()
            result["generate_items_raw"] = gen_items
            if not isinstance(gen_items, list) or not gen_items:
                msg = "generate-multiple вернул пустой список"
                logger.error(msg)
                result["errors"].append(msg)
                return False, result
            logger.info(
                "generate-multiple для %s: %s документов (%s)",
                intro_id,
                len(gen_items),
                _summarize_document_ids(gen_items, "documentId"),
            )
        except Exception as e:
            logger.exception("Ошибка generate-multiple для %s", intro_id)
            result["errors"].append(f"generate-multiple error: {e}")
            return False, result

        # 9) подпись каждого base64Content
        cert = find_certificate_by_thumbprint(thumbprint)
        if not cert:
            msg = f"Сертификат для подписи не найден (thumbprint={thumbprint})"
            logger.error(msg)
            result["errors"].append(msg)
            return False, result

        signed_payloads: List[Dict[str, str]] = []
        for item in gen_items:
            docid = item.get("documentId")
            b64 = item.get("base64Content")
            if not b64:
                result["errors"].append(f"item {docid} missing base64Content")
                continue
            sig = None
            try:
                sig = sign_data(cert, b64, b_detached=True)
                if isinstance(sig, tuple):
                    sig = sig[0]
            except Exception as e:
                logger.warning("Подпись документа %s завершилась ошибкой: %s", docid, e)
                try:
                    sig = sign_data(cert, b64, b_detached=True)
                    if isinstance(sig, tuple):
                        sig = sig[0]
                except Exception as e2:
                    logger.exception("Не удалось подписать %s ни attached ни detached: %s", docid, e2)
                    result["errors"].append(f"sign failed for {docid}: {e2}")
                    continue
            if not sig:
                result["errors"].append(f"signature empty for {docid}")
                continue
            signed_payloads.append({"documentId": docid, "signedContent": sig})

        if not signed_payloads:
            msg = "Нет подписанных документов для отправки"
            logger.error(msg)
            result["errors"].append(msg)
            return False, result

        result["signed_payloads_preview"] = [{"documentId": p["documentId"], "signed_len": len(p["signedContent"])} for p in signed_payloads]


        # 11) отправка send-multiple
        try:
            send_url = f"{base_url}/api/v1/codes-introduction/{intro_id}/send-multiple"
            logger.info("Отправка send-multiple для %s (%s документов)", intro_id, len(signed_payloads))
            r_send = session.post(send_url, json=signed_payloads, timeout=30)
            r_send.raise_for_status()
            try:
                result["send_response"] = r_send.json()
            except Exception:
                result["send_response"] = _preview_text(r_send.text)
        except Exception as e:
            logger.exception("Ошибка send-multiple: %s", e)
            result["errors"].append(f"send-multiple error: {e}")
            return False, result

        # 12) финальные GET'ы
        try:
            r_final_intro = session.get(f"{base_url}/api/v1/codes-introduction/{intro_id}", timeout=15)
            r_final_intro.raise_for_status()
            result["final_introduction"] = r_final_intro.json()
        except Exception as e:
            logger.warning("final introduction GET failed: %s", e)

        try:
            r_final_check = session.get(f"{base_url}/api/v1/codes-checking/{intro_id}", timeout=15)
            r_final_check.raise_for_status()
            result["final_check"] = r_final_check.json()
        except Exception as e:
            logger.warning("final checking GET failed: %s", e)

        # Всё успешно
        ok = not bool(result["errors"])
        return ok, result
        
    except RuntimeError as e:
        result["errors"].append(str(e))
        logger.error(str(e))
        return False, result
    except Exception as e:
        result["errors"].append(str(e))
        logger.error("Ошибка в perform_introduction_from_order: %s", e)
        return False, result


def make_task_on_tsd(
    session: requests.Session,
    codes_order_id: str,
    positions_data: List[Dict[str, str]],
    production_patch: Dict[str, Any],
) -> Tuple[bool, Dict[str, Any]]:
    """
    Создаёт задание ввода в оборот через ТСД.
    """
    result: Dict[str, Any] = {"errors": []}
    
    try:
        base_url = _require_base_url()
        logger.info("ТСД: начало создания задания для заказа %s", codes_order_id)
        
        # 1. Создаем документ ввода в оборот
        url_create = f"{base_url}/api/v1/codes-introduction?warehouseId={WAREHOUSE_ID}"
        req_payload = {
            "introductionType": "introduction",
            "productGroup": PRODUCT_GROUP,
        }
        
        # Отправляем POST запрос для создания документа
        r_create = session.post(url_create, json=req_payload, timeout=30)
        logger.info("ТСД: создание документа HTTP %s, ответ=%s", r_create.status_code, _preview_text(r_create.text))
        
        r_create.raise_for_status()
        document_id = r_create.text.strip().strip('"')
        result["introduction_id"] = document_id
        logger.info("ТСД: создан документ %s", document_id)

        # 2. Обновляем данные production
        url_production = f"{base_url}/api/v1/codes-introduction/{document_id}/production"
        
        # Формируем полный payload для production
        production_payload = {
            "documentNumber": production_patch["documentNumber"],
            "producerInn": "",
            "productionDate": production_patch["productionDate"] + "T00:00:00.000+03:00",
            "productionType": "ownProduction",
            "warehouseId": WAREHOUSE_ID,
            "expirationType": "milkMoreThan72",
            "expirationDate": production_patch["expirationDate"] + "T00:00:00.000+03:00",
            "containsUtilisationReport": True,
            "usageType": "verified",
            "cisType": "unit",
            "fillingMethod": "tsd",
            "batchNumber": production_patch["batchNumber"],
            "isAutocompletePositionsDataNeeded": True,
            "productsHasSameDates": True,
            "productGroup": "wheelChairs"
        }
        
        r_production = session.patch(url_production, json=production_payload, timeout=30)
        logger.info("ТСД: production HTTP %s", r_production.status_code)
        
        r_production.raise_for_status()
        result["production_response"] = r_production.json() if r_production.content else {}
        logger.info("ТСД: production обновлён")

        # 3. Добавляем позиции в документ (упрощенная версия без загрузки XLS)
        url_positions = f"{base_url}/api/v1/codes-introduction/{document_id}/positions"
        
        # Форматируем позиции для API
        positions_payload: Dict[str, List[Dict[str, Any]]] = {"rows": []}
        for pos in positions_data:
            position = {
                "name": pos["name"],
                "gtin": pos["gtin"],
                "tnvedCode": production_patch.get("TnvedCode", ""),
                "certificateDocumentNumber": "",
                "certificateDocumentDate": "",
                "costInKopecksWithVat": 0,
                "exciseInKopecks": 0,
                "productGroup": "wheelChairs"
            }
            positions_payload["rows"].append(position)
        
        r_positions = session.post(url_positions, json=positions_payload, timeout=30)
        logger.info("ТСД: позиции HTTP %s", r_positions.status_code)
        
        r_positions.raise_for_status()
        result["positions_response"] = r_positions.json() if r_positions.content else {}
        logger.info("ТСД: добавлено %s позиций", len(positions_data))

        # 4. Отправляем задание на ТСД
        url_send_tsd = f"{base_url}/api/v1/codes-introduction/{document_id}/send-to-tsd"
        
        r_send_tsd = session.post(url_send_tsd, timeout=30)
        logger.info("ТСД: отправка задания HTTP %s", r_send_tsd.status_code)
        
        r_send_tsd.raise_for_status()
        result["send_to_tsd_response"] = r_send_tsd.json() if r_send_tsd.content else {}
        logger.info("ТСД: задание отправлено")

        # 5. Получаем финальный статус документа
        url_final = f"{base_url}/api/v1/codes-introduction/{document_id}"
        r_final = session.get(url_final, timeout=15)
        r_final.raise_for_status()
        result["final_introduction"] = r_final.json()
        logger.info("ТСД: финальный статус %s", _preview_text(result["final_introduction"]))

        return True, result

    except RuntimeError as e:
        logger.error(str(e))
        result["errors"].append(str(e))
        return False, result
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else "unknown"
        response_text = e.response.text if e.response is not None else str(e)
        error_msg = f"HTTP ошибка {status_code}: {_preview_text(response_text)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    except requests.exceptions.RequestException as e:
        error_msg = f"Ошибка сети: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    except Exception as e:
        error_msg = f"Неожиданная ошибка: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    
def mark_order_as_tsd_created(document_id: str, intro_number: str = ""):
    """
    Помечает заказ как обработанный (задание на ТСД создано)
    """
    try:
        history_db.mark_tsd_created(document_id, intro_number)
        logger.info("Заказ %s помечен как обработанный для ТСД", document_id)
    except Exception as e:
        logger.error("Ошибка обновления истории: %s", e)

def save_codes_order_to_history(order_data: Dict[str, Any], result: Dict[str, Any], success: bool):
    """
    Сохраняет данные о выполненном заказе кодов в историю
    """
    try:
        # Получаем document_id из результата
        document_id = None
        if success and isinstance(result, dict):
            document_id = result.get("documentId") or result.get("id")
        
        # Формируем данные для сохранения
        history_entry = {
            "order_name": order_data.get("document_number", "Unknown"),
            "document_id": document_id,
            "status": "Выполнен" if success else "Ошибка",
            "filename": None,
            "simpl": order_data.get("product_group", ""),
            "full_name": _get_product_name_from_order_data(order_data),
            "gtin": _get_gtin_from_order_data(order_data),
            "positions": order_data.get("positions", [])
        }
        
        if success:
            history_db.add_order(history_entry)
            logger.info("Заказ %s сохранен в историю", order_data.get("document_number", "Unknown"))
        else:
            logger.warning("Заказ не сохранен в историю из-за ошибки")
        
    except Exception as e:
        logger.error("Ошибка сохранения истории заказов кодов: %s", e)

def _get_product_name_from_order_data(order_data: Dict[str, Any]) -> str:
    """Извлекает название товара из данных заказа"""
    positions = order_data.get("positions", [])
    if positions and len(positions) > 0:
        return positions[0].get("name", "Неизвестно")
    return "Неизвестно"

def _get_gtin_from_order_data(order_data: Dict[str, Any]) -> str:
    """Извлекает GTIN из данных заказа"""
    positions = order_data.get("positions", [])
    if positions and len(positions) > 0:
        return positions[0].get("gtin", "")
    return ""

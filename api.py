import os
import json
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from logger import logger
from cryptopro import find_certificate_by_thumbprint, sign_data, refresh_oms_token



# ------------------- Инициализация окружения -------------------
load_dotenv()


# ------------------- Константы конфигурации -------------------
BASE: str = os.getenv("BASE_URL", "")
ORGANIZATION_ID: str = os.getenv("ORGANIZATION_ID", "")
OMS_ID: str = os.getenv("OMS_ID", "")
WAREHOUSE_ID: str = os.getenv("WAREHOUSE_ID", "")
PRODUCT_GROUP: str = os.getenv("PRODUCT_GROUP", "")
RELEASE_METHOD_TYPE: str = os.getenv("RELEASE_METHOD_TYPE", "")
CIS_TYPE: str = os.getenv("CIS_TYPE", "")
FILLING_METHOD: str = os.getenv("FILLING_METHOD", "")


# ------------------- Пути к отладочным JSON-файлам -------------------
DEBUG_DIR = Path(__file__).resolve().parent
LAST_SINGLE_REQ = DEBUG_DIR / "last_single_request.json"
LAST_SINGLE_RESP = DEBUG_DIR / "last_single_response.json"
LAST_MULTI_LOG = DEBUG_DIR / "last_multistep_log.json"

# ---------------- API flows ----------------
def codes_order(session: requests.Session, document_number: str,
                    product_group: str, release_method_type: str,
                    positions: list[dict],
                    filling_method: str = "productsCatalog", thumbprint: str | None = None) -> dict | None:

    signed_orders_payload: list[dict] = []

    logger.info(f"Создание документа: {document_number}")
    url_create = f"{BASE}/api/v1/codes-order?warehouseId={WAREHOUSE_ID}"
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
        logger.error(f"Создание документа {document_number}: {e}")
        return None

    logger.info(f"Документ создан: {document_id}")

    # проверка доступности
    try:
        resp = session.get(f"{BASE}/api/v1/codes-order/{document_id}/availability-status", timeout=15)
        resp.raise_for_status()
        status = resp.text.strip('"')
        if status != "available":
            logger.warning(f"Документ {document_number} недоступен: {status}")
            return None
    except Exception as e:
        logger.error(f"Проверка доступности документа {document_number}: {e}")
        return None

    # проверка сертификата
    if thumbprint:
        try:
            resp = session.get(f"{BASE}/api/v1/organizations/{ORGANIZATION_ID}/employees/has-certificate?thumbprint={thumbprint}", timeout=15)
            resp.raise_for_status()
            if not resp.json():
                logger.error("Сертификат не зарегистрирован в организации")
                return None
        except Exception as e:
            logger.error(f"Проверка сертификата: {e}")
            return None

    # обновление токена OMS
    cert = find_certificate_by_thumbprint(thumbprint)
    if not cert:
        logger.error(f"Сертификат для подписи не найден (thumbprint={thumbprint})")
        return None

    # получение orders-for-sign
    try:
        resp = session.get(f"{BASE}/api/v1/codes-order/{document_id}/orders-for-sign", timeout=15)
        resp.raise_for_status()
        orders_to_sign = resp.json()
        if not isinstance(orders_to_sign, list):
            logger.error(f"Некорректный формат orders_for_sign: {orders_to_sign}")
            return None
    except Exception as e:
        logger.error(f"Получение данных для подписи: {e}")
        return None

    # подпись каждого order
    for o in orders_to_sign:
        oid = o["id"]
        b64content = o["base64Content"]
        logger.info(f"Подписываем order id={oid} (base64Content length={len(b64content)})")
        try:
            signature_b64 = sign_data(cert, b64content, b_detached=True)  # detached для orders
            signed_orders_payload.append({"id": oid, "base64Content": signature_b64})
        except Exception as e:
            logger.error(f"Ошибка подписи order {oid}: {e}")
            return None

    # отправка документа
    try:
        if not refresh_oms_token(session, cert, str(ORGANIZATION_ID)):
            logger.error("Не удалось обновить токен OMS")
            return None

        send_url = f"{BASE}/api/v1/codes-order/{document_id}/send"
        payload = {"signedOrders": signed_orders_payload}
        r_send = session.post(send_url, json=payload, timeout=30)
        r_send.raise_for_status()
        logger.info("Отправка прошла успешно (detached signature)")
    except Exception as e:
        logger.info(f"Отправка документа {document_number}: {e}")
        return None

    # финальный статус
    try:
        r_fin = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
        r_fin.raise_for_status()
        doc = r_fin.json()
        logger.info(f"Финальный статус документа: {doc.get('status')}")
        return doc
    except Exception as e:
        logger.error(f"Получение финального статуса: {e}")
        return None


def check_order_status(session: requests.Session, document_id: str) -> str:
    """
    Быстрая проверка статуса заказа без ожидания
    """
    try:
        resp_status = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
        resp_status.raise_for_status()
        doc = resp_status.json()
        return doc.get("status", "unknown")
    except Exception as e:
        logger.error(f"Ошибка проверки статуса заказа {document_id}: {e}")
        return "error"
    
    
def download_codes(session: requests.Session, document_id: str, order_name: str) -> Optional[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    Скачивает PDF, CSV и XLS (если доступны) для заказа document_id и сохраняет их в папку:
      Desktop / "pdf-коды км" / <safe_order_name>/
    Файлы сохраняются с общей базой имени, производной от order_name.
    Возвращает кортеж (pdf_path, csv_path, xls_path). Если файл не скачан — соответствующий элемент = None.
    """
    logger.info(f"Начало скачивания PDF/CSV/XLS для заказа {document_id} ({order_name!r})")

    from urllib.parse import urljoin

    def make_full_url(url: str) -> str:
        if not url:
            return ""
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return urljoin(BASE, url)

    # 1) дождаться статуса released (polling)
    max_attempts = 10  # 5 минут (10 * 30s)
    attempt = 0
    status = None
    while attempt < max_attempts:
        try:
            resp_status = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
            resp_status.raise_for_status()
            doc = resp_status.json()
            status = doc.get("status")
            logger.info(f"Статус заказа {document_id}: {status}")
            if status == "released":
                break
            time.sleep(30)
            attempt += 1
        except Exception as e:
            logger.error(f"Ошибка проверки статуса заказа {document_id}: {e}", exc_info=True)
            return None

    if status != "released":
        logger.error(f"Заказ {document_id} не перешёл в 'released' за {max_attempts * 30} сек")
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
        logger.error(f"Ошибка при подготовке пути сохранения: {e}", exc_info=True)
        return None

    # Заголовки (включая куки из session)
    cookies_dict = session.cookies.get_dict()
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()]) if cookies_dict else ""
    headers = {"User-Agent": "Mozilla/5.0", "Referer": BASE}
    if cookie_str:
        headers["Cookie"] = cookie_str

    pdf_path = None
    csv_path = None
    xls_path = None

    # ---------------- PDF export ----------------
    try:
        # получить templateId для size "30x20"
        resp_templates = session.get(f"{BASE}/api/v1/print-templates?organizationId={ORGANIZATION_ID}&formTypes=codesOrder", timeout=15)
        resp_templates.raise_for_status()
        templates = resp_templates.json()
        template_id = None
        for t in templates:
            if t.get("size") == "30x20":
                template_id = t.get("id")
                break
        if template_id:
            export_url = f"{BASE}/api/v1/codes-order/{document_id}/export/pdf?splitByGtins=false&templateId={template_id}"
            resp_export = session.post(export_url, timeout=30)
            resp_export.raise_for_status()
            export_data = resp_export.json()
            result_id = export_data.get("resultId")
            logger.info(f"PDF export started for {document_id}, resultId: {result_id}")

            # polling результата
            file_url = None
            attempts_pdf = 0
            while attempts_pdf < 12:  # ~2 минуты
                resp_result = session.get(f"{BASE}/api/v1/codes-order/{document_id}/export/pdf/{result_id}", timeout=15)
                resp_result.raise_for_status()
                result_data = resp_result.json()
                if result_data.get("status") == "success":
                    file_infos = result_data.get("fileInfos", [])
                    if file_infos:
                        file_url = file_infos[0].get("fileUrl") or file_infos[0].get("fileUrlAbsolute") or None
                    break
                time.sleep(10)
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
                    logger.error(f"Ошибка скачивания PDF (requests) {full_file_url}: {e}", exc_info=True)
                    pdf_path = None
            else:
                logger.warning(f"PDF export для {document_id} завершился без fileUrl")
        else:
            logger.warning("Шаблон '30x20' для PDF не найден — пропускаем PDF экспорт")
    except Exception as e:
        logger.exception(f"Ошибка в PDF-части для {document_id}: {e}")
        pdf_path = None

    # ---------------- CSV export ----------------
    try:
        export_csv_url = f"{BASE}/api/v1/codes-order/{document_id}/export/csv?splitByGtins=false"
        resp_csv_export = session.post(export_csv_url, timeout=30)
        resp_csv_export.raise_for_status()
        csv_export_data = resp_csv_export.json()
        csv_result_id = csv_export_data.get("resultId")
        logger.info(f"CSV export started for {document_id}, resultId: {csv_result_id}")

        # polling CSV result
        file_infos = None
        attempts_csv = 0
        while attempts_csv < 30:  # до ~5 минут (30 * 10s)
            resp_csv_status = session.get(f"{BASE}/api/v1/codes-order/{document_id}/export/csv/{csv_result_id}", timeout=15)
            resp_csv_status.raise_for_status()
            csv_status_data = resp_csv_status.json()
            status_csv = csv_status_data.get("status")
            logger.info(f"CSV export status for {document_id}: {status_csv}")
            if status_csv == "success":
                file_infos = csv_status_data.get("fileInfos", [])
                break
            time.sleep(10)
            attempts_csv += 1

        if file_infos:
            finfo = file_infos[0]
            file_id = finfo.get("fileId")
            download_csv_url = f"{BASE}/api/v1/codes-order/{document_id}/export/csv/{csv_result_id}/download/{file_id}"
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
            except Exception as e:
                logger.error(f"Ошибка скачивания CSV (requests) {download_csv_url}: {e}", exc_info=True)
                csv_path = None
        else:
            logger.warning(f"CSV export для {document_id} не вернул fileInfos в пределах таймаута")
    except Exception as e:
        logger.exception(f"Ошибка в CSV-части для {document_id}: {e}")
        csv_path = None

    # ---------------- XLS export ----------------
    try:
        export_xls_url = f"{BASE}/api/v1/codes-order/{document_id}/export/xls?splitByGtins=false"
        resp_xls_export = session.post(export_xls_url, timeout=30)
        resp_xls_export.raise_for_status()
        xls_export_data = resp_xls_export.json()
        xls_result_id = xls_export_data.get("resultId")
        logger.info(f"XLS export started for {document_id}, resultId: {xls_result_id}")

        # polling XLS result
        file_infos = None
        attempts_xls = 0
        while attempts_xls < 30:  # до ~5 минут
            resp_xls_status = session.get(f"{BASE}/api/v1/codes-order/{document_id}/export/xls/{xls_result_id}", timeout=15)
            resp_xls_status.raise_for_status()
            xls_status_data = resp_xls_status.json()
            status_xls = xls_status_data.get("status")
            logger.info(f"XLS export status for {document_id}: {status_xls}")
            if status_xls == "success":
                file_infos = xls_status_data.get("fileInfos", [])
                break
            time.sleep(10)
            attempts_xls += 1

        if file_infos:
            finfo = file_infos[0]
            file_id = finfo.get("fileId")
            download_xls_url = f"{BASE}/api/v1/codes-order/{document_id}/export/xls/{xls_result_id}/download/{file_id}"
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
                logger.error(f"Ошибка скачивания XLS (requests) {download_xls_url}: {e}", exc_info=True)
                xls_path = None
        else:
            logger.warning(f"XLS export для {document_id} не вернул fileInfos в пределах таймаута")
    except Exception as e:
        logger.exception(f"Ошибка в XLS-части для {document_id}: {e}")
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
        # 1) create-from-codes-order
        try:
            addr = socket.getaddrinfo('mk.kontur.ru', 443)
            logger.info(f"DNS успешно разрешён: {addr}")
            url_create = f"{BASE}/api/v1/codes-introduction/create-from-codes-order/{codes_order_id}?isImportFts=false&isAccompanyingDocumentNeeds=false"
            extra_kwargs = {}  # Без verify=False
        except socket.gaierror as dns_err:
            logger.error(f"DNS-разрешение провалено: {dns_err}. Используем workaround с IP.")
            # Workaround: Используем IP вместо домена
            ip = '46.17.200.242'
            original_base = BASE.replace('mk.kontur.ru', ip)
            url_create = f"{original_base}/api/v1/codes-introduction/create-from-codes-order/{codes_order_id}?isImportFts=false&isAccompanyingDocumentNeeds=false"
            extra_kwargs = {'verify': False}  # Отключаем проверку SSL (небезопасно!)
        # Добавляем заголовок Host для workaround (если используем IP)
        headers = {'Host': 'mk.kontur.ru'} if 'ip' in locals() else {}
        
        r = session.post(url_create, headers=headers, timeout=30, **extra_kwargs)
        logger.info(f"Ответ сервера: {r.text}")
        r.raise_for_status()
        intro_id = r.text.strip().strip('"')
        result["introduction_id"] = intro_id
        logger.info("Создана заявка ввода в оборот: %s", intro_id)
        
        # 2) initial GET introduction
        r_intro = session.get(f"{BASE}/api/v1/codes-introduction/{intro_id}", timeout=15)
        r_intro.raise_for_status()
        result["created_introduction"] = r_intro.json()

        # 3) poll codes-checking until status indicates no errors
        check_ok = False
        attempts = 0
        last_check = None
        while attempts < check_poll_attempts:
            r_check = session.get(f"{BASE}/api/v1/codes-checking/{intro_id}", timeout=15)
            if r_check.status_code == 200:
                last_check = r_check.json()
                result["check_status_latest"] = last_check
                status = last_check.get("status")
                logger.info("codes-checking status for %s: %s", intro_id, status)
                if status in ("inProgress", "doesNotHaveErrors", "created", "checked", "noErrors"):  # возможные статусы
                    check_ok = True
                    break
            else:
                logger.debug("codes-checking returned non-200: %s", r_check.status_code)
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
            r_prod = session.get(f"{BASE}/api/v1/codes-introduction/{intro_id}/production", timeout=15)
            r_prod.raise_for_status()
            result["production"] = r_prod.json()
        except Exception as e:
            logger.warning("Не удалось получить /production: %s", e)
            result["errors"].append(f"production GET error: {e}")

        # 5) Если переданы поля для PATCH /production — применим
        if production_patch:
            try:
                patch_url = f"{BASE}/api/v1/codes-introduction/{intro_id}/production"
                logger.info("PATCH production %s", patch_url)
                r_patch = session.patch(patch_url, json=production_patch, timeout=30)
                r_patch.raise_for_status()
                result["production_patch_response"] = r_patch.json() if r_patch.content else {"status": "ok"}
                # обновим production
                r_prod2 = session.get(f"{BASE}/api/v1/codes-introduction/{intro_id}/production", timeout=15)
                r_prod2.raise_for_status()
                result["production_after_patch"] = r_prod2.json()
            except Exception as e:
                logger.exception("Ошибка PATCH production: %s", e)
                result["errors"].append(f"production PATCH error: {e}")

        # 6) попытка автозаполнения позиций (autocomplete) — не обязателен, но делаем если нужно
        try:
            auto_url = f"{BASE}/api/v1/codes-introduction/{intro_id}/positions/autocomplete"
            logger.info("POST positions/autocomplete ...")
            r_auto = session.post(auto_url, timeout=30)
            # многие реализации возвращают 204/200/empty; не обязательно требовать body
            if r_auto.status_code not in (200, 204):
                logger.debug("autocomplete returned status %s", r_auto.status_code)
            result["autocomplete_status"] = r_auto.status_code
        except Exception as e:
            logger.warning("autocomplete failed: %s", e)
            result["errors"].append(f"autocomplete error: {e}")

        # 7) проверить наличие сертификата у сотрудника в организации
        try:
            if thumbprint:
                r_cert_check = session.get(f"{BASE}/api/v1/organizations/{organization_id}/employees/has-certificate?thumbprint={thumbprint}", timeout=15)
                r_cert_check.raise_for_status()
                has_cert = bool(r_cert_check.json())
                result["has_certificate"] = has_cert
                if not has_cert:
                    msg = "Сертификат с указанным thumbprint не зарегистрирован в организации"
                    logger.error(msg)
                    result["errors"].append(msg)
                    return False, result
        except Exception as e:
            logger.exception("Ошибка проверки сертификата: %s", e)
            result["errors"].append(f"cert check error: {e}")
            return False, result

        # 8) GET generate-multiple -> получим массив объектов base64Content
        try:
            gen_url = f"{BASE}/api/v1/codes-introduction/{intro_id}/generate-multiple"
            r_gen = session.get(gen_url, timeout=30)
            logger.info(f"Ответ сервера при вводе в оборот: {r_gen.json()}")
            r_gen.raise_for_status()
            gen_items = r_gen.json()
            result["generate_items_raw"] = gen_items
            if not isinstance(gen_items, list) or not gen_items:
                msg = "generate-multiple вернул пустой список"
                logger.error(msg)
                result["errors"].append(msg)
                return False, result
        except Exception as e:
            logger.exception("Ошибка generate-multiple: %s", e)
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
            docid = item.get("documentId") or item.get("documentId")
            b64 = item.get("base64Content")
            if not b64:
                result["errors"].append(f"item {docid} missing base64Content")
                continue
            # попробуем сначала attached (b_detached=False), если ошибка — detached True
            sig = None
            try:
                sig = sign_data(cert, b64, b_detached=True)
                # sign_data в твоей реализации возвращает строку (или кортеж?), убедимся что строка:
                if isinstance(sig, tuple):
                    sig = sig[0]
            except Exception as e:
                logger.warning("Attached sign failed for %s: %s — попробуем detached", docid, e)
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
            send_url = f"{BASE}/api/v1/codes-introduction/{intro_id}/send-multiple"
            logger.info("Отправка send-multiple ...")
            r_send = session.post(send_url, json=signed_payloads, timeout=30)
            r_send.raise_for_status()
            # API возвращает массив / подтверждение — сохраним
            try:
                result["send_response"] = r_send.json()
            except Exception:
                result["send_response"] = r_send.text
        except Exception as e:
            logger.exception("Ошибка send-multiple: %s", e)
            result["errors"].append(f"send-multiple error: {e}")
            return False, result

        # 12) финальные GET'ы
        try:
            r_final_intro = session.get(f"{BASE}/api/v1/codes-introduction/{intro_id}", timeout=15)
            r_final_intro.raise_for_status()
            result["final_introduction"] = r_final_intro.json()
        except Exception as e:
            logger.warning("final introduction GET failed: %s", e)

        try:
            r_final_check = session.get(f"{BASE}/api/v1/codes-checking/{intro_id}", timeout=15)
            r_final_check.raise_for_status()
            result["final_check"] = r_final_check.json()
        except Exception as e:
            logger.warning("final checking GET failed: %s", e)

        # Всё успешно
        ok = not bool(result["errors"])
        return ok, result
        
    except Exception as e:
        result["errors"].append(str(e))
        logger.error(f"Ошибка в perform_introduction_from_order: {e}")
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
        logger.info(f"🚀 Начало создания задания ТСД для заказа {codes_order_id}")
        
        # 1. Создаем документ ввода в оборот
        url_create = f"{BASE}/api/v1/codes-introduction?warehouseId={WAREHOUSE_ID}"
        logger.info(f"📝 Создаем документ: {url_create}")
        req_payload = {
            "introductionType": "introduction",
            "productGroup": PRODUCT_GROUP,
        }
        
        # Отправляем POST запрос для создания документа
        r_create = session.post(url_create, json=req_payload, timeout=30)
        logger.info(f"📡 Статус создания: {r_create.status_code}")
        logger.info(f"📡 Ответ создания: {r_create.text}")
        
        r_create.raise_for_status()
        document_id = r_create.text.strip().strip('"')
        result["introduction_id"] = document_id
        logger.info(f"✅ Создан документ: {document_id}")

        # 2. Обновляем данные production
        url_production = f"{BASE}/api/v1/codes-introduction/{document_id}/production"
        logger.info(f"⚙️ Обновляем production: {url_production}")
        
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
        
        logger.info(f"📦 Production payload: {production_payload}")
        r_production = session.patch(url_production, json=production_payload, timeout=30)
        logger.info(f"📡 Статус production: {r_production.status_code}")
        
        r_production.raise_for_status()
        result["production_response"] = r_production.json() if r_production.content else {}
        logger.info("✅ Production данные обновлены")

        # 3. Добавляем позиции в документ (упрощенная версия без загрузки XLS)
        url_positions = f"{BASE}/api/v1/codes-introduction/{document_id}/positions"
        logger.info(f"📋 Добавляем позиции: {url_positions}")
        
        # Форматируем позиции для API
        positions_payload = {"rows": []}
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
        
        logger.info(f"📦 Positions payload: {positions_payload}")
        r_positions = session.post(url_positions, json=positions_payload, timeout=30)
        logger.info(f"📡 Статус позиций: {r_positions.status_code}")
        
        r_positions.raise_for_status()
        result["positions_response"] = r_positions.json() if r_positions.content else {}
        logger.info(f"✅ Добавлено {len(positions_data)} позиций")

        # 4. Отправляем задание на ТСД
        url_send_tsd = f"{BASE}/api/v1/codes-introduction/{document_id}/send-to-tsd"
        logger.info(f"📱 Отправляем на ТСД: {url_send_tsd}")
        
        r_send_tsd = session.post(url_send_tsd, timeout=30)
        logger.info(f"📡 Статус отправки ТСД: {r_send_tsd.status_code}")
        
        r_send_tsd.raise_for_status()
        result["send_to_tsd_response"] = r_send_tsd.json() if r_send_tsd.content else {}
        logger.info("✅ Задание отправлено на ТСД")

        # 5. Получаем финальный статус документа
        url_final = f"{BASE}/api/v1/codes-introduction/{document_id}"
        r_final = session.get(url_final, timeout=15)
        r_final.raise_for_status()
        result["final_introduction"] = r_final.json()
        logger.info(f"✅ Финальный статус: {result['final_introduction']}")

        return True, result

    except requests.exceptions.HTTPError as e:
        error_msg = f"❌ HTTP ошибка {e.response.status_code}: {e.response.text}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    except requests.exceptions.RequestException as e:
        error_msg = f"❌ Ошибка сети: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
    except Exception as e:
        error_msg = f"❌ Неожиданная ошибка: {str(e)}"
        logger.error(error_msg)
        result["errors"].append(error_msg)
        return False, result
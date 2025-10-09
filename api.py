import os
import json
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv

from logger import logger
from cryptopro import find_certificate_by_thumbprint, sign_data



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
    
    
def download_codes(session: requests.Session, document_id: str, order_name: str) -> Optional[str]:
    """
    Скачивает PDF, CSV и XLS для заказа и сохраняет в папку на рабочем столе.
    Возвращает имя папки с файлами или None в случае ошибки.
    """
    logger.info(f"Начало скачивания файлов для заказа {document_id} ('{order_name}')")

    def make_full_url(url: str) -> str:
        """Преобразует относительный URL в абсолютный"""
        if not url:
            return ""
        if url.startswith(("http://", "https://")):
            return url
        return urljoin(BASE, url)

    def safe_filename(name: str) -> str:
        """Создает безопасное имя файла"""
        return "".join(c for c in name if c.isalnum() or c in " -_").strip()

    def wait_for_status() -> bool:
        """Ожидает перехода заказа в статус released/received"""
        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                resp = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
                resp.raise_for_status()
                status = resp.json().get("status")
                logger.info(f"Статус заказа {document_id}: {status}")
                
                if status in ("released", "received"):
                    return True
                    
                if attempt < max_attempts - 1:
                    time.sleep(30)
                    
            except Exception as e:
                logger.error(f"Ошибка проверки статуса заказа {document_id}: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(30)
        
        logger.error(f"Заказ {document_id} не перешёл в 'released' за {max_attempts * 30} сек")
        return False

    def download_file(url: str, filepath: str, file_type: str) -> bool:
        """Скачивает файл по URL"""
        try:
            logger.debug(f"Скачивание {file_type} из {url}")
            response = session.get(url, timeout=60, stream=True, allow_redirects=True)
            response.raise_for_status()
            
            with open(filepath, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"{file_type} сохранён: {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка скачивания {file_type}: {e}")
            return False

    def wait_for_export_result(document_id: str, result_id: str, export_type: str) -> Optional[str]:
        """Ожидает завершения экспорта и возвращает URL файла"""
        max_attempts = 30 if export_type == "CSV" else 12
        
        for attempt in range(max_attempts):
            try:
                url = f"{BASE}/api/v1/codes-order/{document_id}/export/{export_type.lower()}/{result_id}"
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                
                status = data.get("status")
                logger.info(f"{export_type} export status for {document_id}: {status}")
                
                if status == "success":
                    file_infos = data.get("fileInfos", [])
                    if file_infos:
                        file_info = file_infos[0]
                        return file_info.get("fileUrl") or file_info.get("fileUrlAbsolute")
                    break
                    
            except Exception as e:
                logger.error(f"Ошибка проверки статуса {export_type} экспорта: {e}")
            
            if attempt < max_attempts - 1:
                time.sleep(10)
        
        return None

    def export_file(document_id: str, export_type: str, params: str = "") -> Optional[str]:
        """Запускает экспорт файла и возвращает result_id"""
        try:
            url = f"{BASE}/api/v1/codes-order/{document_id}/export/{export_type.lower()}{params}"
            resp = session.post(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            result_id = data.get("resultId")
            logger.info(f"{export_type} export started for {document_id}, resultId: {result_id}")
            return result_id
        except Exception as e:
            logger.error(f"Ошибка запуска {export_type} экспорта: {e}")
            return None

    # Основная логика функции

    # 1. Ожидаем готовности заказа
    if not wait_for_status():
        return None

    # 2. Подготавливаем папку для сохранения
    try:
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        parent_dir = os.path.join(desktop, "Коды км")
        safe_order_name = safe_filename(order_name or document_id)
        if not safe_order_name:
            safe_order_name = document_id
        safe_order_name = safe_order_name[:120]
        target_dir = os.path.join(parent_dir, safe_order_name)
        os.makedirs(target_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"Ошибка создания папки: {e}")
        return None

    # 3. Скачиваем PDF
    pdf_success = False
    try:
        # Ищем шаблон для PDF
        resp = session.get(
            f"{BASE}/api/v1/print-templates?organizationId={ORGANIZATION_ID}&formTypes=codesOrder", 
            timeout=15
        )
        resp.raise_for_status()
        templates = resp.json()
        
        template_id = None
        for template in templates:
            if (template.get("name") == "Этикетка 2х2см" or 
                template.get("size") == "2х2" or 
                template.get("dekkoId") == "20x20Template_v2"):
                template_id = template.get("id")
                break
        
        if template_id:
            result_id = export_file(document_id, "PDF", f"?splitByGtins=false&templateId={template_id}")
            if result_id:
                file_url = wait_for_export_result(document_id, result_id, "PDF")
                if file_url:
                    full_url = make_full_url(file_url)
                    pdf_path = os.path.join(target_dir, f"{safe_order_name}.pdf")
                    pdf_success = download_file(full_url, pdf_path, "PDF")
    except Exception as e:
        logger.error(f"Ошибка при скачивании PDF: {e}")

    # 4. Скачиваем CSV
    csv_success = False
    try:
        result_id = export_file(document_id, "CSV", "?splitByGtins=false")
        if result_id:
            file_infos = None
            # Ждем завершения экспорта
            for attempt in range(30):
                try:
                    url = f"{BASE}/api/v1/codes-order/{document_id}/export/csv/{result_id}"
                    resp = session.get(url, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                    
                    status = data.get("status")
                    logger.info(f"CSV export status for {document_id}: {status}")
                    
                    if status == "success":
                        file_infos = data.get("fileInfos", [])
                        break
                except Exception as e:
                    logger.error(f"Ошибка проверки CSV статуса: {e}")
                
                if attempt < 29:
                    time.sleep(10)
            
            if file_infos:
                file_info = file_infos[0]
                file_id = file_info.get("fileId")
                download_url = f"{BASE}/api/v1/codes-order/{document_id}/export/csv/{result_id}/download/{file_id}"
                csv_path = os.path.join(target_dir, f"{safe_order_name}.csv")
                csv_success = download_file(download_url, csv_path, "CSV")
    except Exception as e:
        logger.error(f"Ошибка при скачивании CSV: {e}")

    # 5. Скачиваем XLS
    xls_success = False
    try:
        result_id = export_file(document_id, "XLS", "?splitByGtins=false")
        if result_id:
            file_url = wait_for_export_result(document_id, result_id, "XLS")
            if file_url:
                full_url = make_full_url(file_url)
                xls_path = os.path.join(target_dir, f"{safe_order_name}.xls")
                xls_success = download_file(full_url, xls_path, "XLS")
    except Exception as e:
        logger.error(f"Ошибка при скачивании XLS: {e}")

    # 6. Возвращаем результат
    if pdf_success or csv_success or xls_success:
        logger.info(f"✅ Файлы успешно скачаны в папку: {target_dir}")
        return safe_order_name
    else:
        logger.error(f"❌ Не удалось скачать ни один файл для заказа {document_id}")
        return None




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
        logger.info(f"📡 Статус production: {r_production.status_code}")
        
        r_production.raise_for_status()
        result["production_response"] = r_production.json() if r_production.content else {}
        logger.info("✅ Production данные обновлены")

        # 3. Добавляем позиции в документ (упрощенная версия без загрузки XLS)
        url_positions = f"{BASE}/api/v1/codes-introduction/{document_id}/positions"
        
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
        
        r_positions = session.post(url_positions, json=positions_payload, timeout=30)
        logger.info(f"📡 Статус позиций: {r_positions.status_code}")
        
        r_positions.raise_for_status()
        result["positions_response"] = r_positions.json() if r_positions.content else {}
        logger.info(f"✅ Добавлено {len(positions_data)} позиций")

        # 4. Отправляем задание на ТСД
        url_send_tsd = f"{BASE}/api/v1/codes-introduction/{document_id}/send-to-tsd"
        
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
import json
import os
from pathlib import Path
from typing import Tuple, Optional
import requests
import datetime
from logger import logger
import win32com.client
from win32com.client import Dispatch
import pythoncom
from dotenv import load_dotenv
import time

load_dotenv()

# ---------------- config ----------------
BASE = os.getenv("BASE_URL")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID")
OMS_ID = os.getenv("OMS_ID")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")
PRODUCT_GROUP = os.getenv("PRODUCT_GROUP")
RELEASE_METHOD_TYPE = os.getenv("RELEASE_METHOD_TYPE")
CIS_TYPE = os.getenv("CIS_TYPE")
FILLING_METHOD = os.getenv("FILLING_METHOD")

# debug files
LAST_SINGLE_REQ = Path("last_single_request.json")
LAST_SINGLE_RESP = Path("last_single_response.json")
LAST_MULTI_LOG = Path("last_multistep_log.json")

# CAdES / CAPICOM constants
CADES_BES = 1
CADESCOM_BASE64_TO_BINARY = 1
CAPICOM_ENCODE_BASE64 = 0
CAPICOM_AUTHENTICATED_ATTRIBUTE_SIGNING_TIME = 0
CAPICOM_CURRENT_USER_STORE = 2
CAPICOM_MY_STORE = "My"
CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED = 2


# ---------- certificate utilities (pywin32 / CAdES) ----------
def find_certificate_by_thumbprint(thumbprint: Optional[str] = None):
    pythoncom.CoInitialize()
    store = win32com.client.Dispatch("CAdESCOM.Store")
    store.Open(CAPICOM_CURRENT_USER_STORE, CAPICOM_MY_STORE, CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED)
    found = None
    try:
        for cert in store.Certificates:
            try:
                if thumbprint:
                    if getattr(cert, "Thumbprint", "").lower() == thumbprint.lower():
                        found = cert
                        break
                else:
                    found = cert
                    break
            except Exception:
                continue
    finally:
        store.Close()
    pythoncom.CoUninitialize()
    return found

def sign_data(cert, base64_content: str, b_detached: bool = False) -> str:
    """
    Подписывает данные из base64-строки CAdES-BES подписью.
    base64_content - base64-строка данных для подписи.
    b_detached - True для отсоединенной (detached), False для присоединенной (attached).
    Возвращает подпись в base64 (ASCII).
    """
    pythoncom.CoInitialize()
    signer = Dispatch("CAdESCOM.CPSigner")
    signer.Certificate = cert

    oSigningTimeAttr = Dispatch("CAdESCOM.CPAttribute")
    oSigningTimeAttr.Name = CAPICOM_AUTHENTICATED_ATTRIBUTE_SIGNING_TIME
    oSigningTimeAttr.Value = datetime.datetime.now()
    signer.AuthenticatedAttributes2.Add(oSigningTimeAttr)

    signed_data = Dispatch("CAdESCOM.CadesSignedData")
    signed_data.ContentEncoding = CADESCOM_BASE64_TO_BINARY
    signed_data.Content = base64_content
    signature = signed_data.SignCades(signer, CADES_BES, b_detached, CAPICOM_ENCODE_BASE64)

    if isinstance(signature, bytes):
        signature = signature.decode("ascii", errors="ignore")
    pythoncom.CoUninitialize()
    return signature.replace("\r", "").replace("\n", "")

def post_with_winhttp(url, payload, headers=None):
    win_http = Dispatch('WinHTTP.WinHTTPRequest.5.1')
    win_http.Open("POST", url, False)
    win_http.SetRequestHeader("User-Agent", "Mozilla/5.0")
    win_http.SetRequestHeader("Accept", "application/json, text/plain, */*")
    win_http.SetRequestHeader("Content-Type", "application/json; charset=utf-8")
    if headers:
        for k, v in headers.items():
            win_http.SetRequestHeader(k, v)
    win_http.Send(json.dumps(payload))
    win_http.WaitForResponse()
    status = win_http.Status
    response_text = win_http.ResponseText
    all_headers = win_http.GetAllResponseHeaders()
    if status != 200:
        raise Exception(f"WinHTTP POST failed: Status {status} - {response_text}")
    return status, response_text, all_headers


# ---------------- Refresh OMS token ----------------
def refresh_oms_token(session: requests.Session, cert, organization_id: str) -> bool:
    logger.info("Обновление токена OMS...")
    url_auth = f"{BASE}/api/v1/crpt/auth?organizationId={organization_id}"

    try:
        resp_get = session.get(url_auth, timeout=15)
        resp_get.raise_for_status()
        challenges = resp_get.json()
        logger.debug(f"Ответ /crpt/auth GET: {json.dumps(challenges, indent=2)}")
        if not isinstance(challenges, list):
            logger.error(f"[ERR] Некорректный формат challenges: {challenges}")
            return False
    except Exception as e:
        logger.error(f"[ERR] GET challenges для OMS: {e}")
        return False

    payload = []
    for ch in challenges:
        if ch['productGroup'] in ['oms', 'trueApi']:
            try:
                sig = sign_data(cert, ch["base64Data"], b_detached=False)  # attached для auth
                payload.append({
                    "uuid": ch["uuid"],
                    "productGroup": ch["productGroup"],
                    "base64Data": sig  # trueApi/oms используют одно поле
                })
            except Exception as e:
                logger.error(f"Подпись challenge для {ch['productGroup']} (uuid={ch['uuid']}): {e}")
                return False


    if not payload:
        logger.error("Нет challenge для OMS в ответе")
        return False

    try:
        cookies_dict = session.cookies.get_dict()
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()]) if cookies_dict else ""
        custom_headers = {"Cookie": cookie_str} if cookie_str else None
        status, resp_text, all_headers = post_with_winhttp(url_auth, payload, headers=custom_headers)

        # Обновление cookies в session из Set-Cookie
        set_cookie_lines = [line.strip()[len("Set-Cookie:"):].strip() for line in all_headers.splitlines() if line.strip().startswith("Set-Cookie:")]
        if set_cookie_lines:
            temp_resp = requests.Response()
            temp_resp.headers['Set-Cookie'] = set_cookie_lines
            temp_resp.status_code = status
            temp_resp.url = url_auth
            session.cookies.update(temp_resp.cookies)

        logger.info(f"Токен OMS обновлён успешно. Ответ: {resp_text}")
        return True
    except Exception as e:
        logger.error(f"POST signed challenges для OMS: {e}")
        return False

# ---------------- API flows ----------------
def try_single_post(session: requests.Session, document_number: str,
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

# ---------------- Download PDF ----------------
def get_with_winhttp(url: str, headers: dict | None = None):
    """
    Выполнить GET через WinHTTP и вернуть (status, response_bytes, all_headers)
    """
    win_http = Dispatch('WinHTTP.WinHTTPRequest.5.1')
    win_http.Open("GET", url, False)
    # стандартные заголовки
    win_http.SetRequestHeader("User-Agent", "Mozilla/5.0")
    if headers:
        for k, v in headers.items():
            # не пересоздаём User-Agent
            if k.lower() == "user-agent":
                continue
            win_http.SetRequestHeader(k, v)
    win_http.Send()
    win_http.WaitForResponse()
    status = int(win_http.Status)
    # ResponseBody в COM возвращает бинарные данные
    try:
        body = win_http.ResponseBody  # binary
    except Exception:
        body = None
    all_headers = win_http.GetAllResponseHeaders()
    return status, body, all_headers


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
    
    
def download_codes_pdf_and_convert(session: requests.Session, document_id: str, order_name: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Скачивает PDF и CSV для заказа document_id и сохраняет их в папку:
      Desktop / "pdf-коды км" / <safe_order_name>/
    Файлы сохраняются с общей базой имени, производной от order_name.
    Возвращает кортеж (pdf_path, csv_path). Если файл не скачан — соответствующий элемент = None.
    WinHTTP / COM fallback удалён — только requests.
    """
    logger.info(f"Начало скачивания PDF+CSV для заказа {document_id} ({order_name!r})")

    # helper: сделать абсолютный URL (если fileUrl относительный)
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
        parent_dir = os.path.join(desktop, "pdf-коды км")
        # безопасное имя папки
        safe_order_name = "".join(c for c in (order_name or document_id) if c.isalnum() or c in " -_").strip()
        if not safe_order_name:
            safe_order_name = document_id
        # Ограничим длину имени папки
        safe_order_name = safe_order_name[:120]
        target_dir = os.path.join(parent_dir, safe_order_name)
        os.makedirs(target_dir, exist_ok=True)
        # базовое имя файла
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
                        # в fileInfos может быть fileUrl (абсолютный/относительный)
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
            file_name = finfo.get("fileName") or f"{safe_base}.csv"
            # скачать через endpoint /export/csv/{resultId}/download/{fileId}
            download_csv_url = f"{BASE}/api/v1/codes-order/{document_id}/export/csv/{csv_result_id}/download/{file_id}"
            safe_csv_name = order_name
            # защитить имя файла
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

    # Вернуть кортеж путей (возможно один из них None)
    return pdf_path, csv_path

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



def download_codes_pdf_and_convert(session: requests.Session, document_id: str, order_name: str) -> Optional[Tuple[str, Optional[str]]]:
    """
    Скачивает PDF для заказа document_id, сохраняет его как <order_name>.pdf на рабочем столе.
    Конвертацию в CSV убрал — функция возвращает (pdf_path, None).
    """
    logger.info(f"Начало скачивания PDF для заказа {document_id}")

    # Параметры polling статуса заказа
    max_attempts = 10  # 5 мин / 30 сек = 10
    attempt = 0
    status = None
    while attempt < max_attempts:
        try:
            resp_status = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
            resp_status.raise_for_status()
            doc = resp_status.json()
            status = doc.get("status")
            logger.info(f"Статус заказа: {status}")
            if status == "released":
                break
            time.sleep(30)
            attempt += 1
        except Exception as e:
            logger.error(f"Ошибка проверки статуса: {e}", exc_info=True)
            return None

    if status != "released":
        logger.error(f"Заказ не перешел в 'released' за {max_attempts * 30} сек")
        return None

    # Получить шаблоны печати
    try:
        resp_templates = session.get(f"{BASE}/api/v1/print-templates?organizationId={ORGANIZATION_ID}&formTypes=codesOrder", timeout=15)
        resp_templates.raise_for_status()
        templates = resp_templates.json()
        logger.debug(f"Шаблоны: {json.dumps(templates, indent=2, ensure_ascii=False)}")
    except Exception as e:
        logger.error(f"Ошибка получения шаблонов: {e}", exc_info=True)
        return None

    # Найти шаблон с size "30x20"
    template_id = None
    for t in templates:
        if t.get("size") == "30x20":
            template_id = t.get("id")
            break
    if not template_id:
        logger.error("Шаблон '30x20' не найден")
        return None

    # POST для экспорта PDF
    try:
        export_url = f"{BASE}/api/v1/codes-order/{document_id}/export/pdf?splitByGtins=false&templateId={template_id}"
        resp_export = session.post(export_url, timeout=30)
        resp_export.raise_for_status()
        export_data = resp_export.json()
        result_id = export_data.get("resultId")
        logger.info(f"Экспорт начат, resultId: {result_id}")
    except Exception as e:
        logger.error(f"Ошибка экспорта PDF: {e}", exc_info=True)
        return None

    # Поллинг статуса экспорта (макс 2 мин, каждые 10 сек)
    max_attempts_export = 12  # 2 мин / 10 сек = 12
    attempt_export = 0
    file_url = None
    while attempt_export < max_attempts_export:
        try:
            resp_result = session.get(f"{BASE}/api/v1/codes-order/{document_id}/export/pdf/{result_id}", timeout=15)
            resp_result.raise_for_status()
            result_data = resp_result.json()
            result_status = result_data.get("status")
            logger.info(f"Статус экспорта: {result_status}")
            if result_status == "success":
                file_infos = result_data.get("fileInfos", [])
                if file_infos:
                    file_url = file_infos[0].get("fileUrl")
                break
            time.sleep(10)
            attempt_export += 1
        except Exception as e:
            logger.error(f"Ошибка проверки статуса экспорта: {e}", exc_info=True)
            return None

    if not file_url:
        logger.error("Экспорт не завершился успехом или файл не найден")
        return None

    logger.debug(f"fileUrl: {file_url}")

    # Подготавливаем целевую папку на Desktop
    try:
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        target_dir = os.path.join(desktop, "pdf-коды км")
        os.makedirs(target_dir, exist_ok=True)
        pdf_filename = f"{order_name}.pdf"
        pdf_path = os.path.join(target_dir, pdf_filename)
    except Exception as e:
        logger.error(f"Ошибка при подготовке пути сохранения: {e}", exc_info=True)
        return None

    # Подготовим заголовки с куками вручную (cookie_str)
    cookies_dict = session.cookies.get_dict()
    cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()]) if cookies_dict else ""

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": BASE,
    }
    if cookie_str:
        headers["Cookie"] = cookie_str

    # Попытка 1: requests с ручным Cookie header (обычно работает)
    pdf_bytes = None
    try:
        logger.debug("Пробуем скачать PDF через requests с явным Cookie header")
        resp_pdf = session.get(file_url, timeout=30, headers=headers, stream=True, allow_redirects=True)
        if resp_pdf.status_code == 401:
            logger.warning(f"requests GET вернул 401 для {file_url}; попробуем WinHTTP fallback. Response headers: {resp_pdf.headers}")
            raise requests.HTTPError(f"401 for {file_url}")
        resp_pdf.raise_for_status()
        # Собираем байты в память — пригодится для возможных дальнейших обращений
        pdf_bytes = resp_pdf.content
        with open(pdf_path, 'wb') as f:
            f.write(pdf_bytes)
        logger.info(f"PDF скачан: {pdf_path}")
    except Exception as e_req:
        logger.warning(f"Не удалось скачать через requests: {e_req}", exc_info=True)

    # Попытка 2: WinHTTP GET (fallback)
    if pdf_bytes is None:
        try:
            logger.debug("Пробуем скачать PDF через WinHTTP (COM) fallback")
            status, body, all_headers = get_with_winhttp(file_url, headers=headers)
            logger.debug(f"WinHTTP status={status}; headers: {all_headers}")
            if status != 200:
                logger.error(f"WinHTTP GET failed: status={status}; headers={all_headers}")
                return None
            if body is None:
                logger.error("WinHTTP вернул пустое тело ответа")
                return None

            # Попытаться привести тело к байтам
            try:
                pdf_bytes = bytes(body)
            except Exception:
                pdf_bytes = body

            with open(pdf_path, 'wb') as f:
                f.write(pdf_bytes)
            logger.info(f"PDF скачан (WinHTTP): {pdf_path}")
        except Exception as e_win:
            logger.error(f"Ошибка скачивания PDF (WinHTTP fallback): {e_win}", exc_info=True)
            logger.debug(f"Cookie used: {cookie_str}")
            return None

    # Конвертация в CSV удалена — возвращаем путь к PDF и None для CSV (чтобы было обратимо)
    return pdf_path, None

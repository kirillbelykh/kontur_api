import json
from pathlib import Path
from typing import Dict, Optional
import requests
import base64
import datetime
import win32com.client
from win32com.client import Dispatch
import pythoncom

# ---------------- config ----------------
COOKIES_FILE = Path("kontur_cookies.json")
CONFIG__DATA_FILE = Path("config.json")
with CONFIG__DATA_FILE.open(encoding="utf-8") as f:
    config = json.load(f)

BASE = config["base_url"]
ORGANIZATION_ID = config["organization_id"]
OMS_ID = config["oms_id"]
WAREHOUSE_ID = config["warehouse_id"]
PRODUCT_GROUP = config["product_group"]
RELEASE_METHOD_TYPE = config["release_method_type"]
CIS_TYPE = config["cis_type"]
FILLING_METHOD = config["filling_method"]


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

# ---------------- helpers ----------------
def load_cookies() -> Optional[Dict[str, str]]:
    if COOKIES_FILE.exists():
        try:
            data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data:
                return data
        except Exception:
            pass
    return None

def make_session_with_cookies(cookies: Optional[Dict[str, str]]) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
    })
    if cookies:
        for k, v in cookies.items():
            s.cookies.set(k, v, domain="mk.kontur.ru", path="/")
    return s

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
    print("[INFO] Обновление токена OMS...")
    url_auth = f"{BASE}/api/v1/crpt/auth?organizationId={organization_id}"

    try:
        resp_get = session.get(url_auth, timeout=15)
        resp_get.raise_for_status()
        challenges = resp_get.json()
        print(f"[DEBUG] Ответ /crpt/auth GET: {json.dumps(challenges, indent=2)}")
        if not isinstance(challenges, list):
            print(f"[ERR] Некорректный формат challenges: {challenges}")
            return False
    except Exception as e:
        print(f"[ERR] GET challenges для OMS: {e}")
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
                print(f"[ERR] Подпись challenge для {ch['productGroup']} (uuid={ch['uuid']}): {e}")
                return False


    if not payload:
        print("[ERR] Нет challenge для OMS в ответе")
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

        print(f"[OK] Токен OMS обновлён успешно. Ответ: {resp_text}")
        return True
    except Exception as e:
        print(f"[ERR] POST signed challenges для OMS: {e}")
        return False

# ---------------- API flows ----------------
def try_single_post(session: requests.Session, document_number: str,
                    product_group: str, release_method_type: str,
                    positions: list[dict],
                    filling_method: str = "productsCatalog", thumbprint: str | None = None) -> dict | None:

    signed_orders_payload: list[dict] = []

    print(f"[INFO] Создание документа: {document_number}")
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
        print(f"[ERR] Создание документа {document_number}: {e}")
        return None

    print(f"[INFO] Документ создан: {document_id}")

    # проверка доступности
    try:
        resp = session.get(f"{BASE}/api/v1/codes-order/{document_id}/availability-status", timeout=15)
        resp.raise_for_status()
        status = resp.text.strip('"')
        if status != "available":
            print(f"[WARN] Документ {document_number} недоступен: {status}")
            return None
    except Exception as e:
        print(f"[ERR] Проверка доступности документа {document_number}: {e}")
        return None

    # проверка сертификата
    if thumbprint:
        try:
            resp = session.get(f"{BASE}/api/v1/organizations/{ORGANIZATION_ID}/employees/has-certificate?thumbprint={thumbprint}", timeout=15)
            resp.raise_for_status()
            if not resp.json():
                print("[ERR] Сертификат не зарегистрирован в организации")
                return None
        except Exception as e:
            print(f"[ERR] Проверка сертификата: {e}")
            return None

    # обновление токена OMS
    cert = find_certificate_by_thumbprint(thumbprint)
    if not cert:
        print(f"[ERR] Сертификат для подписи не найден (thumbprint={thumbprint})")
        return None

    # получение orders-for-sign
    try:
        resp = session.get(f"{BASE}/api/v1/codes-order/{document_id}/orders-for-sign", timeout=15)
        resp.raise_for_status()
        orders_to_sign = resp.json()
        if not isinstance(orders_to_sign, list):
            print(f"[ERR] Некорректный формат orders_for_sign: {orders_to_sign}")
            return None
    except Exception as e:
        print(f"[ERR] Получение данных для подписи: {e}")
        return None

    # подпись каждого order
    for o in orders_to_sign:
        oid = o["id"]
        b64content = o["base64Content"]
        print(f"[INFO] Подписываем order id={oid} (base64Content length={len(b64content)})")
        try:
            signature_b64 = sign_data(cert, b64content, b_detached=True)  # detached для orders
            signed_orders_payload.append({"id": oid, "base64Content": signature_b64})
        except Exception as e:
            print(f"[ERR] Ошибка подписи order {oid}: {e}")
            return None

    # отправка документа
    try:
        if not refresh_oms_token(session, cert, ORGANIZATION_ID):
            print("[ERR] Не удалось обновить токен OMS")
            return None

        send_url = f"{BASE}/api/v1/codes-order/{document_id}/send"
        payload = {"signedOrders": signed_orders_payload}
        r_send = session.post(send_url, json=payload, timeout=30)
        r_send.raise_for_status()
        print("[OK] Отправка прошла успешно (detached signature)")
    except Exception as e:
        print(f"[ERR] Отправка документа {document_number}: {e}")
        return None

    # финальный статус
    try:
        r_fin = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
        r_fin.raise_for_status()
        doc = r_fin.json()
        print(f"[OK] Финальный статус документа: {doc.get('status')}")
        return doc
    except Exception as e:
        print(f"[ERR] Получение финального статуса: {e}")
        return None
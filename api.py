import json
from pathlib import Path
from typing import Dict, Optional
import requests
import base64
import datetime
import win32com.client
from win32com.client import Dispatch
import json


# ---------------- config ----------------
COOKIES_FILE = Path("kontur_cookies.json")
BASE = "https://mk.kontur.ru"

# Проверь/измени при необходимости:
ORGANIZATION_ID = "5cda50fa-523f-4bb5-85b6-66d7241b23cd"
WAREHOUSE_ID = "59739360-7d62-434b-ad13-4617c87a6d13"
PRODUCT_GROUP = "wheelChairs"   # как в devtools у тебя
RELEASE_METHOD_TYPE = "production"
CIS_TYPE = "unit"
FILLING_METHOD = "productsCatalog"

# debug files
LAST_SINGLE_REQ = Path("last_single_request.json")
LAST_SINGLE_RESP = Path("last_single_response.json")
LAST_MULTI_LOG = Path("last_multistep_log.json")

# CAdES / CAPICOM constants
CADES_BES = 1
CAPICOM_ENCODE_BASE64 = 0
CAPICOM_AUTHENTICATED_ATTRIBUTE_SIGNING_TIME = 0
CAPICOM_CURRENT_USER_STORE = 2
CAPICOM_MY_STORE = "My"
CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED = 2

# ---------------- helpers ----------------
def load_cookies() -> Optional[Dict[str,str]]:
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
                    # если thumbprint не указан — берем первый сертификат с закрытым ключом (попробуем)
                    # Простейшая эвристика: берем первый попавшийся
                    found = cert
                    break
            except Exception:
                continue
    finally:
        store.Close()
    return found

def sign_base64_content_with_cades(cert, base64_content: str) -> str:
    """
    Подписывает base64-строку откреплённой (detached) CAdES-BES подписью.
    Возвращает подпись в base64 (ASCII).
    """
    # сервер ждёт, что мы подпишем строку base64 (а не её декодированные байты)
    content_bytes = base64_content.encode("utf-8")
    double_b64 = base64.b64encode(content_bytes).decode("ascii")

    signer = win32com.client.Dispatch("CAdESCOM.CPSigner")
    signer.Certificate = cert

    # атрибут времени подписи (по возможности)
    try:
        attr = win32com.client.Dispatch("CAdESCOM.CPAttribute")
        attr.Name = CAPICOM_AUTHENTICATED_ATTRIBUTE_SIGNING_TIME
        attr.Value = datetime.datetime.now()
        signer.AuthenticatedAttributes2.Add(attr)
    except Exception:
        pass

    signed = win32com.client.Dispatch("CAdESCOM.CadesSignedData")
    try:
        signed.ContentEncodingType = CAPICOM_ENCODE_BASE64
    except Exception:
        pass
    signed.Content = double_b64

    # detached = True => откреплённая подпись
    signature = signed.SignCades(signer, CADES_BES, True, CAPICOM_ENCODE_BASE64)

    if isinstance(signature, bytes):
        signature = signature.decode("ascii", errors="ignore")
    return signature.replace("\r", "").replace("\n", "")



# ---------------- API flows ----------------
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

    # находим сертификат
    cert = find_certificate_by_thumbprint(thumbprint)
    if not cert:
        print(f"[ERR] Сертификат для подписи не найден (thumbprint={thumbprint})")
        return None

    # функция detached-подписи
    def sign_detached(b64_content: str) -> str:
        signer = Dispatch("CAdESCOM.CPSigner")
        signer.Certificate = cert
        signed_data = Dispatch("CAdESCOM.CadesSignedData")
        signed_data.Content = b64_content
        signature = signed_data.SignCades(signer, 1)  # CADESCOM_CADES_BES = detached
        return signature

    # подпись каждого order
    for o in orders_to_sign:
        oid = o["id"]
        b64content = o["base64Content"]
        print(f"[INFO] Подписываем order id={oid} (base64Content length={len(b64content)})")
        try:
            signature_b64 = sign_detached(b64content)
            signed_orders_payload.append({"id": oid, "base64Content": signature_b64})
        except Exception as e:
            print(f"[ERR] Ошибка подписи order {oid}: {e}")
            return None

    # отправка документа
    try:
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

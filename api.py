# main.py
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import requests

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



# ---------------- API flows ----------------
def try_single_post(session: requests.Session, document_number: str,
                    product_group: str, release_method_type: str,
                    positions: List[Dict[str, Any]],
                    filling_method: str = "productsCatalog", thumbprint: str = None) -> Optional[Dict[str, Any]]:
    import json

    print(f"[INFO] Создание документа: {document_number}")
    url_create = f"{BASE}/api/v1/codes-order?warehouseId={WAREHOUSE_ID}"

    # Подготовка тела запроса
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
        print(f"[DEBUG] Response text: {resp.text[:500]}{'...' if len(resp.text) > 500 else ''}")
        resp.raise_for_status()
    except Exception as e:
        print(f"[ERR] Создание документа {document_number} — Exception: {e}")
        return None

    data = resp.json()
    document_id = data
    print(f"[INFO] Документ создан: {document_id}")

    # Проверка доступности
    url_avail = f"{BASE}/api/v1/codes-order/{document_id}/availability-status"
    try:
        resp = session.get(url_avail, timeout=15)
        resp.raise_for_status()
        status = resp.text.strip('"')
        print(f"[INFO] Статус доступности документа: {status}")
    except Exception as e:
        print(f"[ERR] Проверка доступности документа {document_number} — Exception: {e}")
        return None

    if status != "available":
        print(f"[WARN] Документ {document_number} недоступен для отправки: {status}")
        return None

    # Проверка сертификата
    if thumbprint:
        url_cert = f"{BASE}/api/v1/organizations/{ORGANIZATION_ID}/employees/has-certificate?thumbprint={thumbprint}"
        try:
            resp = session.get(url_cert, timeout=15)
            resp.raise_for_status()
            has_cert = resp.json()
            print(f"[DEBUG] Certificate check response: {has_cert}")
            if not has_cert:
                print(f"[ERR] Нет сертификата для подписи документа {document_number}")
                return None
            print(f"[INFO] Сертификат доступен для подписи")
        except Exception as e:
            print(f"[ERR] Проверка сертификата — Exception: {e}")
            return None

    # Получение данных для подписи
    url_sign_data = f"{BASE}/api/v1/codes-order/{document_id}/orders-for-sign"
    try:
        resp = session.get(url_sign_data, timeout=15)
        resp.raise_for_status()
        orders_to_sign = resp.json()
    except Exception as e:
        print(f"[ERR] Получение данных для подписи — Exception: {e}")
        return None

    # Проверка формата данных для подписи
    if not isinstance(orders_to_sign, list) or not all("id" in o and "base64Content" in o for o in orders_to_sign):
        print(f"[ERR] Данные для подписи имеют неверный формат: {orders_to_sign}")
        return None

    # Подписание и отправка
    url_send = f"{BASE}/api/v1/codes-order/{document_id}/send"
    payload = {"signedOrders": [{"id": document_id}]}
    try:
        resp = session.post(url_send, json=payload, timeout=30)
        print(f"[DEBUG] Send response text: {resp.text[:500]}{'...' if len(resp.text) > 500 else ''}")
        resp.raise_for_status()
        print(f"[INFO] Документ {document_number} успешно подписан и отправлен")
    except Exception as e:
        print(f"[ERR] Отправка документа — Exception: {e}")
        return None

    # Получение финального статуса документа
    url_final = f"{BASE}/api/v1/codes-order/{document_id}"
    try:
        resp = session.get(url_final, timeout=15)
        resp.raise_for_status()
        document_data = resp.json()
        print(f"[OK] Финальный статус документа: {document_data.get('status')}")
        print(f"[DEBUG] Actions: {json.dumps(document_data.get('actions', {}), ensure_ascii=False, indent=2)}")
        return document_data
    except Exception as e:
        print(f"[ERR] Получение финального статуса — Exception: {e}")
        return None

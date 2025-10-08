import win32com.client
from win32com.client import Dispatch
import pythoncom
from typing import Optional
import datetime
from winhttp import post_with_winhttp
import requests
from logger import logger
import json

BASE = "https://mk.kontur.ru"

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
                pythoncom.CoUninitialize()
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
        pythoncom.CoUninitialize()
        return True
    except Exception as e:
        logger.error(f"POST signed challenges для OMS: {e}")
        return False
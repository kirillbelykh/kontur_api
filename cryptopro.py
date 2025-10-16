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
    logger.debug(f"Вход в find_certificate_by_thumbprint с thumbprint: {thumbprint}")
    pythoncom.CoInitialize()
    logger.debug("Вызван CoInitialize")
    store = win32com.client.Dispatch("CAdESCOM.Store")
    logger.debug("Создан объект CAdESCOM.Store")
    store.Open(CAPICOM_CURRENT_USER_STORE, CAPICOM_MY_STORE, CAPICOM_STORE_OPEN_MAXIMUM_ALLOWED)
    logger.debug("Хранилище открыто")
    found = None
    try:
        logger.debug(f"Итерация по {store.Certificates.Count} сертификатам")
        for cert in store.Certificates:
            try:
                cert_thumb = getattr(cert, "Thumbprint", "").lower()
                logger.debug(f"Проверка сертификата с thumbprint: {cert_thumb}")
                if thumbprint:
                    if cert_thumb == thumbprint.lower():
                        found = cert
                        logger.debug(f"Найден подходящий сертификат с thumbprint: {cert_thumb}")
                        break
                else:
                    found = cert
                    logger.debug(f"Найден первый сертификат с thumbprint: {cert_thumb}")
                    break
            except Exception as e:
                logger.warning(f"Исключение при проверке сертификата: {e}")
                continue
    finally:
        store.Close()
        logger.debug("Хранилище закрыто")
    pythoncom.CoUninitialize()
    logger.debug("Вызван CoUninitialize")
    if found:
        logger.info(f"Сертификат найден с thumbprint: {getattr(found, 'Thumbprint', 'Неизвестно')}")
    else:
        logger.warning("Сертификат не найден")
    return found

def sign_data(cert, base64_content: str, b_detached: bool = False) -> str:
    """
    Подписывает данные из base64-строки CAdES-BES подписью.
    base64_content - base64-строка данных для подписи.
    b_detached - True для отсоединенной (detached), False для присоединенной (attached).
    Возвращает подпись в base64 (ASCII).
    """
    cert_thumb = getattr(cert, "Thumbprint", "Неизвестно")
    logger.debug(f"Вход в sign_data с thumbprint сертификата: {cert_thumb}, длина base64_content: {len(base64_content)}, b_detached: {b_detached}")
    pythoncom.CoInitialize()
    logger.debug("Вызван CoInitialize")
    signer = Dispatch("CAdESCOM.CPSigner")
    logger.debug("Создан объект CAdESCOM.CPSigner")
    signer.Certificate = cert
    logger.debug("Сертификат установлен в signer")

    oSigningTimeAttr = Dispatch("CAdESCOM.CPAttribute")
    logger.debug("Создан объект CAdESCOM.CPAttribute для времени подписи")
    oSigningTimeAttr.Name = CAPICOM_AUTHENTICATED_ATTRIBUTE_SIGNING_TIME
    signing_time = datetime.datetime.now()
    oSigningTimeAttr.Value = signing_time
    logger.debug(f"Атрибут времени подписи установлен на: {signing_time}")
    signer.AuthenticatedAttributes2.Add(oSigningTimeAttr)
    logger.debug("Атрибут времени подписи добавлен в signer")
    signed_data = Dispatch("CAdESCOM.CadesSignedData")
    logger.debug("Создан объект CAdESCOM.CadesSignedData")
    signed_data.ContentEncoding = CADESCOM_BASE64_TO_BINARY
    logger.debug("ContentEncoding установлен на CADESCOM_BASE64_TO_BINARY")
    signed_data.Content = base64_content
    logger.debug("Content установлен на base64_content")
    try:
        signature = signed_data.SignCades(signer, CADES_BES, b_detached, CAPICOM_ENCODE_BASE64)
        logger.debug("SignCades вызван успешно")
    except Exception as e:
        logger.error(f"Исключение во время SignCades: {e}")
        raise

    if isinstance(signature, bytes):
        signature = signature.decode("ascii", errors="ignore")
        logger.debug("Подпись декодирована из байтов в ascii")
    signature = signature.replace("\r", "").replace("\n", "")
    logger.debug(f"Длина очищенной подписи: {len(signature)}")
    pythoncom.CoUninitialize()
    logger.debug("Вызван CoUninitialize")
    logger.info(f"Данные успешно подписаны сертификатом с thumbprint: {cert_thumb}")
    return signature

# ---------------- Refresh OMS token ----------------
def refresh_oms_token(session: requests.Session, cert, organization_id: str) -> bool:
    cert_thumb = getattr(cert, "Thumbprint", "Неизвестно")
    logger.info(f"Обновление токена OMS для organization_id: {organization_id} с thumbprint сертификата: {cert_thumb}")
    url_auth = f"{BASE}/api/v1/crpt/auth?organizationId={organization_id}"
    logger.debug(f"URL аутентификации: {url_auth}")

    try:
        resp_get = session.get(url_auth, timeout=15)
        logger.debug(f"Отправлен GET-запрос на {url_auth}")
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
            logger.debug(f"Обработка вызова для productGroup: {ch['productGroup']}, uuid: {ch['uuid']}")
            logger.debug(f"Длина base64Data вызова: {len(ch['base64Data'])}")
            try:
                pythoncom.CoUninitialize()
                logger.debug("Вызван CoUninitialize перед подписью")
                sig = sign_data(cert, ch["base64Data"], b_detached=False)  # attached для auth
                logger.debug(f"Длина подписанных данных: {len(sig)}")
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

    logger.debug(f"Payload для POST: {json.dumps(payload, indent=2)}")

    try:
        cookies_dict = session.cookies.get_dict()
        cookie_str = "; ".join([f"{k}={v}" for k, v in cookies_dict.items()]) if cookies_dict else ""
        logger.debug(f"Строка Cookie: {cookie_str}")
        custom_headers = {"Cookie": cookie_str} if cookie_str else None
        logger.debug(f"Пользовательские заголовки: {custom_headers}")
        status, resp_text, all_headers = post_with_winhttp(url_auth, payload, headers=custom_headers)
        logger.debug(f"Статус ответа post_with_winhttp: {status}")
        logger.debug(f"Текст ответа post_with_winhttp: {resp_text}")
        logger.debug(f"Все заголовки post_with_winhttp: {all_headers}")
        # Обновление cookies в session из Set-Cookie
        set_cookie_lines = [line.strip()[len("Set-Cookie:"):].strip() for line in all_headers.splitlines() if line.strip().startswith("Set-Cookie:")]
        logger.debug(f"Строки Set-Cookie: {set_cookie_lines}")
        if set_cookie_lines:
            temp_resp = requests.Response()
            temp_resp.headers['Set-Cookie'] = set_cookie_lines
            temp_resp.status_code = status
            temp_resp.url = url_auth
            session.cookies.update(temp_resp.cookies)
            logger.debug("Cookies сессии обновлены")

        logger.info(f"Токен OMS обновлён успешно. Ответ: {resp_text}")
        pythoncom.CoUninitialize()
        logger.debug("Вызван CoUninitialize после POST")
        return True
    except Exception as e:
        logger.error(f"POST signed challenges для OMS: {e}")
        return False
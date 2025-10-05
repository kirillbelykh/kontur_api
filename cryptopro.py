import win32com.client
from win32com.client import Dispatch
import pythoncom
from typing import Optional
import datetime

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
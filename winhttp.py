from win32com.client import Dispatch
import pythoncom
import json


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
    pythoncom.CoUninitialize()
    return status, response_text, all_headers

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
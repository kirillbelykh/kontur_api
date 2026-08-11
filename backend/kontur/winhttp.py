"""WinHTTP helpers (Windows COM).

Imports are lazy so non-Windows environments can import dependent modules.
"""

from __future__ import annotations

import json
from typing import Any


def _require_winhttp_dispatch() -> tuple[Any, Any]:
    try:
        import pythoncom
        from win32com.client import Dispatch
    except ImportError as exc:
        raise RuntimeError(
            "WinHTTP требует Windows и pywin32. "
            "Установите pywin32 на рабочей станции."
        ) from exc
    return pythoncom, Dispatch


def post_with_winhttp(url, payload, headers=None):
    pythoncom, Dispatch = _require_winhttp_dispatch()
    win_http = Dispatch("WinHTTP.WinHTTPRequest.5.1")
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


def get_with_winhttp(url: str, headers: dict | None = None):
    """
    Выполнить GET через WinHTTP и вернуть (status, response_bytes, all_headers)
    """
    _pythoncom, Dispatch = _require_winhttp_dispatch()
    win_http = Dispatch("WinHTTP.WinHTTPRequest.5.1")
    win_http.Open("GET", url, False)
    win_http.SetRequestHeader("User-Agent", "Mozilla/5.0")
    if headers:
        for k, v in headers.items():
            if k.lower() == "user-agent":
                continue
            win_http.SetRequestHeader(k, v)
    win_http.Send()
    win_http.WaitForResponse()
    status = int(win_http.Status)
    try:
        body = win_http.ResponseBody
    except Exception:
        body = None
    all_headers = win_http.GetAllResponseHeaders()
    return status, body, all_headers

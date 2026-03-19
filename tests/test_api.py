import os
import sys
import types
import unittest
from unittest.mock import Mock, patch

import requests

os.environ.setdefault("HISTORY_SYNC_ENABLED", "0")
os.environ.setdefault("BASE_URL", "https://mk.kontur.ru")

winreg_stub = types.ModuleType("winreg")
winreg_stub.HKEY_CURRENT_USER = object()
winreg_stub.OpenKey = lambda *args, **kwargs: (_ for _ in ()).throw(FileNotFoundError())
winreg_stub.QueryValue = lambda *args, **kwargs: ""
sys.modules.setdefault("winreg", winreg_stub)

win32com_stub = types.ModuleType("win32com")
win32com_client_stub = types.ModuleType("win32com.client")
win32com_client_stub.Dispatch = lambda *args, **kwargs: None
win32com_stub.client = win32com_client_stub
sys.modules.setdefault("win32com", win32com_stub)
sys.modules.setdefault("win32com.client", win32com_client_stub)

pythoncom_stub = types.ModuleType("pythoncom")
pythoncom_stub.CoInitialize = lambda: None
pythoncom_stub.CoUninitialize = lambda: None
sys.modules.setdefault("pythoncom", pythoncom_stub)

winhttp_stub = types.ModuleType("winhttp")
winhttp_stub.post_with_winhttp = lambda *args, **kwargs: None
sys.modules.setdefault("winhttp", winhttp_stub)

import api


class FakeResponse:
    def __init__(self, *, status_code=200, text="", json_data=None, headers=None, content=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data
        self.headers = headers or {}
        if content is not None:
            self.content = content
        elif text:
            self.content = text.encode("utf-8")
        elif json_data is not None:
            self.content = b"{}"
        else:
            self.content = b""

    def json(self):
        if self._json_data is None:
            raise ValueError("JSON body is missing")
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def iter_content(self, chunk_size=8192):
        if self.content:
            yield self.content


class ApiTests(unittest.TestCase):
    def test_codes_order_waits_until_document_becomes_available(self):
        session = Mock()
        session.post.side_effect = [
            FakeResponse(json_data={"id": "DOC-42"}),
            FakeResponse(json_data={"ok": True}),
        ]
        session.get.side_effect = [
            FakeResponse(status_code=417, text="Expectation Failed"),
            FakeResponse(text='"available"'),
            FakeResponse(json_data=[{"id": "ORDER-1", "base64Content": "AAAA"}]),
            FakeResponse(json_data={"status": "released"}),
        ]

        with (
            patch.object(api, "find_certificate_by_thumbprint", return_value=object()),
            patch.object(api, "sign_data", return_value="signed-content"),
            patch.object(api.history_db, "add_order") as add_order_mock,
            patch.object(api.time, "sleep", return_value=None) as sleep_mock,
        ):
            result = api.codes_order(
                session=session,
                document_number="ORDER-42",
                product_group="wheelchairs",
                release_method_type="SELF_MADE",
                positions=[{"gtin": "04600000000000", "name": "Test product", "quantity": 1}],
                thumbprint=None,
            )

        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "released")
        self.assertEqual(session.get.call_count, 4)
        self.assertIn("/availability-status", session.get.call_args_list[0].args[0])
        self.assertIn("/availability-status", session.get.call_args_list[1].args[0])
        sleep_mock.assert_called_once_with(api.ORDER_AVAILABILITY_POLL_INTERVAL_SECONDS)
        add_order_mock.assert_called_once()

    def test_make_task_on_tsd_returns_clear_error_when_base_url_missing(self):
        session = Mock()

        with patch.object(api, "BASE", ""):
            ok, result = api.make_task_on_tsd(
                session=session,
                codes_order_id="DOC-TSD-1",
                positions_data=[],
                production_patch={
                    "documentNumber": "DOC-TSD-1",
                    "productionDate": "2026-03-19",
                    "expirationDate": "2026-04-19",
                    "batchNumber": "BATCH-1",
                },
            )

        self.assertFalse(ok)
        self.assertEqual(result["errors"], [api.BASE_URL_CONFIG_ERROR])
        session.post.assert_not_called()


if __name__ == "__main__":
    unittest.main()

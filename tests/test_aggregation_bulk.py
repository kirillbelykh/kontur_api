import base64
import json
import os
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import requests

from aggregation_bulk import BulkAggregationService


class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text or (json.dumps(json_data) if json_data is not None else "")

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(self.text)
            error.response = self
            raise error

    def json(self):
        if self._json_data is None:
            raise ValueError("No JSON payload")
        return self._json_data


def make_content_for_sign(participant_id, unit_serial_number, sntins):
    payload = {
        "participantId": participant_id,
        "aggregationUnits": [
            {
                "unitSerialNumber": unit_serial_number,
                "aggregationType": "AGGREGATION",
                "sntins": sntins,
            }
        ],
    }
    return {
        "documentId": "doc-1",
        "base64Content": base64.b64encode(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        ).decode("ascii"),
    }


def make_kontur_session(get_func, post_func, tsd_token=None):
    jar = requests.cookies.RequestsCookieJar()
    if tsd_token:
        jar.set("tsdToken", tsd_token, domain="mk.kontur.ru", path="/")
    return SimpleNamespace(get=get_func, post=post_func, cookies=jar)


class BulkAggregationServiceTests(unittest.TestCase):
    def build_service(self, true_api_session, max_workers=1):
        return BulkAggregationService(
            kontur_base_url="https://mk.kontur.ru",
            warehouse_id="warehouse-1",
            true_api_base_url="https://markirovka.crpt.ru/api/v3/true-api",
            true_api_product_group="wheelchairs",
            page_size=2,
            batch_size=1000,
            poll_interval_seconds=0.0,
            document_timeout_seconds=0.2,
            parent_clear_timeout_seconds=0.2,
            kontur_send_timeout_seconds=0.2,
            max_workers=max_workers,
            sleep_func=lambda seconds: None,
            true_api_session=true_api_session,
        )

    def test_uses_production_true_api_host_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TRUE_API_BASE_URL", None)
            os.environ.pop("TRUE_API_SANDBOX", None)
            os.environ.pop("CRPT_SANDBOX", None)
            service = BulkAggregationService(
                kontur_base_url="https://mk.kontur.ru",
                warehouse_id="warehouse-1",
                true_api_session=SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None),
            )

        self.assertEqual(service.true_api_base_url, "https://markirovka.crpt.ru/api/v3/true-api")
        self.assertEqual(service.true_api_info_base_url, "https://markirovka.crpt.ru/api/v4/true-api")

    def test_uses_sandbox_true_api_host_when_crpt_sandbox_enabled(self):
        with patch.dict(os.environ, {"CRPT_SANDBOX": "true"}, clear=False):
            os.environ.pop("TRUE_API_BASE_URL", None)
            os.environ.pop("TRUE_API_SANDBOX", None)
            service = BulkAggregationService(
                kontur_base_url="https://mk.kontur.ru",
                warehouse_id="warehouse-1",
                true_api_session=SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None),
            )

        self.assertEqual(service.true_api_base_url, "https://markirovka.sandbox.crptech.ru/api/v3/true-api")
        self.assertEqual(service.true_api_info_base_url, "https://markirovka.sandbox.crptech.ru/api/v4/true-api")

    def test_processes_independent_aggregates_in_parallel(self):
        barrier = threading.Barrier(2)
        code_threads = set()

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [
                        {"documentId": "doc-1", "aggregateCode": "AK-1", "status": "readyForSend"},
                        {"documentId": "doc-2", "aggregateCode": "AK-2", "status": "readyForSend"},
                    ],
                    "total": 2,
                })
            if path.startswith("/api/v1/aggregates/") and path.endswith("/codes"):
                code_threads.add(threading.current_thread().name)
                barrier.wait(timeout=1.0)
                return FakeResponse(json_data={"aggregateCodes": [], "reaggregationCodes": []})
            if path.startswith("/api/v1/aggregates/"):
                document_id = path.rsplit("/", 1)[-1]
                return FakeResponse(json_data={
                    "documentId": document_id,
                    "aggregateCode": document_id.replace("doc", "AK"),
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            raise AssertionError(f"Unexpected GET {url} {params}")

        service = self.build_service(
            SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None),
            max_workers=2,
        )
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=lambda *args, **kwargs: FakeResponse(json_data={"ok": True})),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "unused",
            sign_text_func=lambda cert, data, detached: "unused",
        )

        self.assertEqual(summary.ready_found, 2)
        self.assertEqual(summary.processed, 2)
        self.assertEqual(summary.skipped_empty, 2)
        self.assertEqual(summary.errors, 0)
        self.assertEqual(len(code_threads), 2)

    def test_paginates_ready_aggregates(self):
        kontur_state = {"send_called": False}

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                offset = params["offset"]
                if offset == 0:
                    return FakeResponse(json_data={
                        "items": [
                            {"documentId": "doc-1", "aggregateCode": "AK-1", "status": "readyForSend"},
                            {"documentId": "doc-2", "aggregateCode": "AK-2", "status": "readyForSend"},
                        ],
                        "total": 3,
                    })
                if offset == 2:
                    return FakeResponse(json_data={
                        "items": [
                            {"documentId": "doc-3", "aggregateCode": "AK-3", "status": "readyForSend"},
                        ],
                        "total": 3,
                    })
            if path.startswith("/api/v1/aggregates/") and path.endswith("/codes"):
                return FakeResponse(json_data={"aggregateCodes": [], "reaggregationCodes": []})
            if path.startswith("/api/v1/aggregates/"):
                document_id = path.rsplit("/", 1)[-1]
                return FakeResponse(json_data={
                    "documentId": document_id,
                    "aggregateCode": document_id.replace("doc", "AK"),
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, timeout=None):
            kontur_state["send_called"] = True
            raise AssertionError("send must not be called for empty aggregates")

        kontur_session = SimpleNamespace(get=kontur_get, post=kontur_post)
        true_api_session = SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None)
        service = self.build_service(true_api_session)
        progress_calls = []

        summary = service.run(
            kontur_session=kontur_session,
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "unused",
            sign_text_func=lambda cert, data, detached: "unused",
            progress_callback=lambda processed, total: progress_calls.append((processed, total)),
        )

        self.assertEqual(summary.ready_found, 3)
        self.assertEqual(summary.processed, 3)
        self.assertEqual(summary.skipped_empty, 3)
        self.assertEqual(summary.errors, 0)
        self.assertFalse(kontur_state["send_called"])
        self.assertIn((3, 3), progress_calls)

    def test_filters_ready_aggregates_by_comment(self):
        requested_docs = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [
                        {
                            "documentId": "doc-1",
                            "aggregateCode": "AK-1",
                            "status": "readyForSend",
                            "comment": "Коляска прогулочная",
                        },
                        {
                            "documentId": "doc-2",
                            "aggregateCode": "AK-2",
                            "status": "readyForSend",
                            "comment": "Ходунки",
                        },
                    ],
                    "total": 2,
                })
            if path.startswith("/api/v1/aggregates/") and path.endswith("/codes"):
                requested_docs.append(path.split("/")[4])
                return FakeResponse(json_data={"aggregateCodes": [], "reaggregationCodes": []})
            if path.startswith("/api/v1/aggregates/"):
                document_id = path.rsplit("/", 1)[-1]
                return FakeResponse(json_data={
                    "documentId": document_id,
                    "aggregateCode": document_id.replace("doc", "AK"),
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                    "comment": "Коляска прогулочная" if document_id == "doc-1" else "Ходунки",
                })
            raise AssertionError(f"Unexpected GET {url} {params}")

        service = self.build_service(SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=lambda *args, **kwargs: FakeResponse(json_data={"ok": True})),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "unused",
            sign_text_func=lambda cert, data, detached: "unused",
            comment_filter="прогулоч",
        )

        self.assertEqual(summary.ready_found, 1)
        self.assertEqual(summary.processed, 1)
        self.assertEqual(summary.skipped_empty, 1)
        self.assertEqual(requested_docs, ["doc-1"])

    def test_stops_run_when_true_api_is_unreachable(self):
        log_messages = []
        true_post_calls = {"count": 0}

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [
                        {
                            "documentId": "doc-1",
                            "aggregateCode": "AK-1",
                            "status": "readyForSend",
                            "productGroup": "wheelChairs",
                        },
                        {
                            "documentId": "doc-2",
                            "aggregateCode": "AK-2",
                            "status": "readyForSend",
                            "productGroup": "wheelChairs",
                        },
                    ],
                    "total": 2,
                })
            if path.endswith("/codes"):
                return FakeResponse(json_data={
                    "aggregateCodes": [{"ttisCode": "01046501180412952156bej,nSIQ*?="}],
                    "reaggregationCodes": [],
                })
            if path.startswith("/api/v1/aggregates/"):
                document_id = path.rsplit("/", 1)[-1]
                return FakeResponse(json_data={
                    "documentId": document_id,
                    "aggregateCode": document_id.replace("doc", "AK"),
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            raise AssertionError(f"Unexpected GET {url} {params}")

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                raise requests.ConnectionError(
                    "HTTPSConnectionPool(host='markirovka.crpt.ru', port=443): "
                    "Caused by NameResolutionError(\"Failed to resolve\")"
                )
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            true_post_calls["count"] += 1
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=lambda *args, **kwargs: FakeResponse(json_data={"ok": True})),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "unused",
            sign_text_func=lambda cert, data, detached: "unused",
            log_callback=log_messages.append,
        )

        self.assertEqual(summary.ready_found, 2)
        self.assertEqual(summary.processed, 1)
        self.assertEqual(summary.errors, 1)
        self.assertEqual(true_post_calls["count"], 0)
        self.assertTrue(any("DNS-имя не разрешается" in message for message in log_messages))
        self.assertTrue(any("Проведение остановлено" in message for message in log_messages))

    def test_introduced_foreign_parent_disaggregates_then_sends_kontur(self):
        raw_codes = [
            "01046501180412952156bej,nSIQ*?=",
            "0104650118041295215Bb<&2ChWtC,;",
        ]
        sntins = raw_codes[:]
        sign_content = make_content_for_sign("7843316794", "04650118042603180000000007", sntins)
        detail_calls = {"count": 0}
        cises_calls = {"count": 0}
        doc_info_calls = {"count": 0}
        true_api_posts = []
        kontur_send_payloads = []
        text_sign_inputs = []
        base64_sign_inputs = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [
                        {
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "readyForSend",
                            "productGroup": "wheelChairs",
                        }
                    ],
                    "total": 1,
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={
                    "aggregateCodes": [{"ttisCode": code} for code in raw_codes],
                    "reaggregationCodes": [],
                })
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=sign_content)
            if path == "/api/v1/aggregates/doc-1":
                detail_calls["count"] += 1
                status = "readyForSend" if detail_calls["count"] < 3 else "sentForApprove"
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": status,
                    "productGroup": "wheelChairs",
                })
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, timeout=None):
            kontur_send_payloads.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH_CHALLENGE"})
            if "/doc/" in url and url.endswith("/info"):
                doc_info_calls["count"] += 1
                status = "REGISTERED" if doc_info_calls["count"] == 1 else "CHECKED_OK"
                return FakeResponse(json_data={"status": status})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            true_api_posts.append((url, params, json))
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                cises_calls["count"] += 1
                parent = "04650118042603180000000099" if cises_calls["count"] == 1 else None
                return FakeResponse(json_data=[
                    {
                        "result": {
                            "requestedCis": code,
                            "cis": code,
                            "status": "INTRODUCED",
                            "ownerInn": "7843316794",
                            "parent": parent,
                        }
                    }
                    for code in sntins
                ])
            if url.endswith("/lk/documents/create"):
                return FakeResponse(json_data={"id": "doc-disagg-1"})
            raise AssertionError(f"Unexpected TRUE POST {url}")

        kontur_session = SimpleNamespace(get=kontur_get, post=kontur_post)
        true_api_session = SimpleNamespace(get=true_get, post=true_post)
        service = self.build_service(true_api_session)

        summary = service.run(
            kontur_session=kontur_session,
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: base64_sign_inputs.append((data, detached)) or "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: text_sign_inputs.append((data, detached)) or f"TEXT_SIGNATURE_{len(text_sign_inputs)}",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 1)
        self.assertEqual(summary.disaggregated_parents, 1)
        self.assertEqual(summary.errors, 0)
        self.assertEqual(len(kontur_send_payloads), 1)
        self.assertEqual(kontur_send_payloads[0][1], {"signedContent": "BASE64_SIGNATURE"})
        self.assertEqual(base64_sign_inputs, [(sign_content["base64Content"], True)])
        self.assertEqual(text_sign_inputs[0], ("AUTH_CHALLENGE", False))

        create_call = next(call for call in true_api_posts if call[0].endswith("/lk/documents/create"))
        self.assertEqual(create_call[2]["type"], "DISAGGREGATION_DOCUMENT")
        decoded_doc = json.loads(base64.b64decode(create_call[2]["product_document"]).decode("utf-8"))
        self.assertEqual(decoded_doc, {
            "participant_inn": "7843316794",
            "products_list": [{"uitu": "04650118042603180000000099"}],
        })

    def test_non_introduced_foreign_parent_confirm_yes_disaggregates_and_skips_current(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        sign_content = make_content_for_sign("7843316794", "04650118042603180000000007", [raw_code])
        created_docs = []
        send_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [{"documentId": "doc-1", "aggregateCode": "04650118042603180000000007", "status": "readyForSend"}],
                    "total": 1,
                })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=sign_content)
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, timeout=None):
            send_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            if "/doc/" in url and url.endswith("/info"):
                return FakeResponse(json_data={"status": "CHECKED_OK"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "APPLIED",
                        "ownerInn": "7843316794",
                        "parent": "04650118042603180000000099",
                    }
                }])
            if url.endswith("/lk/documents/create"):
                created_docs.append(json)
                return FakeResponse(json_data={"id": "doc-disagg-1"})
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=kontur_post),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.skipped_due_to_status, 1)
        self.assertEqual(summary.disaggregated_parents, 1)
        self.assertEqual(len(created_docs), 1)
        self.assertEqual(send_calls, [])

    def test_non_introduced_foreign_parent_confirm_no_skips_without_disaggregation(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        created_docs = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [{"documentId": "doc-1", "aggregateCode": "04650118042603180000000007", "status": "readyForSend"}],
                    "total": 1,
                })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            raise AssertionError(f"Unexpected GET {url} {params}")

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "EMITTED",
                        "ownerInn": "7843316794",
                        "parent": "04650118042603180000000099",
                    }
                }])
            if url.endswith("/lk/documents/create"):
                created_docs.append(json)
                return FakeResponse(json_data={"id": "doc-disagg-1"})
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=lambda *args, **kwargs: FakeResponse(json_data={"ok": True})),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: False,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.skipped_due_to_status, 1)
        self.assertEqual(summary.disaggregated_parents, 0)
        self.assertEqual(created_docs, [])

    def test_document_error_status_counts_as_error(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        detail_calls = {"count": 0}
        doc_info_calls = {"count": 0}
        send_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [{"documentId": "doc-1", "aggregateCode": "04650118042603180000000007", "status": "readyForSend"}],
                    "total": 1,
                })
            if path == "/api/v1/aggregates/doc-1":
                detail_calls["count"] += 1
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=make_content_for_sign("7843316794", "04650118042603180000000007", [raw_code]))
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, timeout=None):
            send_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            if "/doc/" in url and url.endswith("/info"):
                doc_info_calls["count"] += 1
                return FakeResponse(json_data={"status": "CHECKED_NOT_OK"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                        "parent": "04650118042603180000000099",
                    }
                }])
            if url.endswith("/lk/documents/create"):
                return FakeResponse(json_data={"id": "doc-disagg-1"})
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=kontur_post),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.errors, 1)
        self.assertEqual(send_calls, [])

    def test_introduced_foreign_parent_confirm_no_skips_without_disaggregation(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        confirm_calls = []
        created_docs = []
        send_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                return FakeResponse(json_data={
                    "items": [{"documentId": "doc-1", "aggregateCode": "04650118042603180000000007", "status": "readyForSend"}],
                    "total": 1,
                })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": "readyForSend",
                    "productGroup": "wheelChairs",
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            raise AssertionError(f"Unexpected GET {url} {params}")

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                        "parent": "04650118042603180000000099",
                    }
                }])
            if url.endswith("/lk/documents/create"):
                created_docs.append(json)
                return FakeResponse(json_data={"id": "doc-disagg-1"})
            raise AssertionError(f"Unexpected TRUE POST {url}")

        def kontur_post(url, json=None, timeout=None):
            send_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=SimpleNamespace(get=kontur_get, post=kontur_post),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: confirm_calls.append((title, message)) or False,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.disaggregated_parents, 0)
        self.assertEqual(created_docs, [])
        self.assertEqual(send_calls, [])
        self.assertEqual(len(confirm_calls), 1)
        self.assertIn("04650118042603180000000099", confirm_calls[0][1])

    def test_approve_failed_aggregate_uses_tsd_recovery_before_send(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        sign_content = make_content_for_sign("7843316794", "04650118042603180000000007", [raw_code])
        detail_payloads = iter([
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "approveFailed",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "returnedToTsd",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "sentForApprove",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
        ])
        post_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "readyForSend":
                    return FakeResponse(json_data={"items": [], "total": 0})
                if status_filter == "approveFailed":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "approveFailed",
                            "productGroup": "wheelChairs",
                        }],
                        "total": 1,
                    })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data=next(detail_payloads))
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=sign_content)
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json, headers))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                    }
                }])
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=make_kontur_session(kontur_get, kontur_post, tsd_token="tsd-1"),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.ready_found, 1)
        self.assertEqual(summary.sent_for_approve, 1)
        self.assertEqual(summary.errors, 0)
        self.assertEqual(len(post_calls), 3)
        self.assertTrue(post_calls[0][0].endswith("/api/v1/aggregates/doc-1/return-to-tsd"))
        self.assertTrue(post_calls[1][0].endswith("/tsd/api/v1/documents/aggregates/doc-1"))
        self.assertEqual(post_calls[1][1], {"codes": [raw_code]})
        self.assertEqual(post_calls[1][2]["Accept"], "*/*")
        self.assertTrue(post_calls[2][0].endswith("/api/v1/aggregates/doc-1/send"))
        self.assertEqual(post_calls[2][1], {"signedContent": "BASE64_SIGNATURE"})

    def test_ready_for_send_does_not_use_tsd_recovery(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        sign_content = make_content_for_sign("7843316794", "04650118042603180000000007", [raw_code])
        detail_payloads = iter([
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "sentForApprove",
                "productGroup": "wheelChairs",
            },
        ])
        post_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "readyForSend":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "readyForSend",
                            "productGroup": "wheelChairs",
                        }],
                        "total": 1,
                    })
                if status_filter == "approveFailed":
                    return FakeResponse(json_data={"items": [], "total": 0})
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data=next(detail_payloads))
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=sign_content)
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                    }
                }])
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=make_kontur_session(kontur_get, kontur_post),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 1)
        self.assertEqual(summary.errors, 0)
        self.assertEqual(post_calls, [
            ("https://mk.kontur.ru/api/v1/aggregates/doc-1/send", {"signedContent": "BASE64_SIGNATURE"})
        ])

    def test_approve_failed_without_tsd_token_fails_before_tsd_post(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        detail_payloads = iter([
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "approveFailed",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "returnedToTsd",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
        ])
        post_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "readyForSend":
                    return FakeResponse(json_data={"items": [], "total": 0})
                if status_filter == "approveFailed":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "approveFailed",
                            "productGroup": "wheelChairs",
                        }],
                        "total": 1,
                    })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data=next(detail_payloads))
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                    }
                }])
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=make_kontur_session(kontur_get, kontur_post),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.errors, 1)
        self.assertEqual(post_calls, [
            ("https://mk.kontur.ru/api/v1/aggregates/doc-1/return-to-tsd", None)
        ])

    def test_approve_failed_without_allow_return_to_tsd_fails(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        post_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "readyForSend":
                    return FakeResponse(json_data={"items": [], "total": 0})
                if status_filter == "approveFailed":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "approveFailed",
                            "productGroup": "wheelChairs",
                        }],
                        "total": 1,
                    })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": "approveFailed",
                    "productGroup": "wheelChairs",
                    "actions": {"allowReturnToTsd": False, "allowSave": True},
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                    }
                }])
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=make_kontur_session(kontur_get, kontur_post, tsd_token="tsd-1"),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.errors, 1)
        self.assertEqual(post_calls, [])

    def test_approve_failed_tsd_recovery_times_out_before_ready_for_send(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        detail_calls = {"count": 0}
        post_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "readyForSend":
                    return FakeResponse(json_data={"items": [], "total": 0})
                if status_filter == "approveFailed":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "approveFailed",
                            "productGroup": "wheelChairs",
                        }],
                        "total": 1,
                    })
            if path == "/api/v1/aggregates/doc-1":
                detail_calls["count"] += 1
                status = "approveFailed" if detail_calls["count"] == 1 else "returnedToTsd"
                return FakeResponse(json_data={
                    "documentId": "doc-1",
                    "aggregateCode": "04650118042603180000000007",
                    "status": status,
                    "productGroup": "wheelChairs",
                    "actions": {"allowReturnToTsd": True, "allowSave": True},
                })
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                    }
                }])
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=make_kontur_session(kontur_get, kontur_post, tsd_token="tsd-1"),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 0)
        self.assertEqual(summary.errors, 1)
        self.assertEqual(len(post_calls), 2)
        self.assertTrue(post_calls[0][0].endswith("/return-to-tsd"))
        self.assertTrue(post_calls[1][0].endswith("/tsd/api/v1/documents/aggregates/doc-1"))

    def test_approve_failed_recovery_starts_after_foreign_parent_disaggregation(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        sign_content = make_content_for_sign("7843316794", "04650118042603180000000007", [raw_code])
        detail_payloads = iter([
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "approveFailed",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "returnedToTsd",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "sentForApprove",
                "productGroup": "wheelChairs",
                "actions": {"allowReturnToTsd": True, "allowSave": True},
            },
        ])
        post_calls = []
        created_docs = []
        cis_requests = {"count": 0}

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "readyForSend":
                    return FakeResponse(json_data={"items": [], "total": 0})
                if status_filter == "approveFailed":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "approveFailed",
                            "productGroup": "wheelChairs",
                        }],
                        "total": 1,
                    })
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data=next(detail_payloads))
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=sign_content)
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        def true_get(url, params=None, headers=None, timeout=None):
            if url.endswith("/auth/key"):
                return FakeResponse(json_data={"uuid": "uuid-1", "data": "AUTH"})
            if "/doc/" in url and url.endswith("/info"):
                return FakeResponse(json_data={"status": "CHECKED_OK"})
            raise AssertionError(f"Unexpected TRUE GET {url}")

        def true_post(url, params=None, headers=None, json=None, timeout=None):
            if url.endswith("/auth/simpleSignIn"):
                return FakeResponse(json_data={"token": "token-1"})
            if url.endswith("/cises/short/list"):
                cis_requests["count"] += 1
                parent = "04650118042603180000000099" if cis_requests["count"] == 1 else None
                return FakeResponse(json_data=[{
                    "result": {
                        "requestedCis": raw_code,
                        "cis": raw_code,
                        "status": "INTRODUCED",
                        "ownerInn": "7843316794",
                        "parent": parent,
                    }
                }])
            if url.endswith("/lk/documents/create"):
                created_docs.append(json)
                return FakeResponse(json_data={"id": "doc-disagg-1"})
            raise AssertionError(f"Unexpected TRUE POST {url}")

        service = self.build_service(SimpleNamespace(get=true_get, post=true_post))
        summary = service.run(
            kontur_session=make_kontur_session(kontur_get, kontur_post, tsd_token="tsd-1"),
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            sign_text_func=lambda cert, data, detached: "TEXT_SIGNATURE",
            confirm_callback=lambda title, message: True,
        )

        self.assertEqual(summary.sent_for_approve, 1)
        self.assertEqual(summary.errors, 0)
        self.assertEqual(len(created_docs), 1)
        self.assertTrue(post_calls[0][0].endswith("/return-to-tsd"))
        self.assertTrue(post_calls[1][0].endswith("/tsd/api/v1/documents/aggregates/doc-1"))

    def test_matches_comment_filter_by_stable_tokens_when_comment_is_mojibake(self):
        self.assertTrue(
            BulkAggregationService._matches_comment_filter(
                "כאע הטאד S 260316 (249ך)",
                "лат диаг s 260316 (249к)",
            )
        )
        self.assertFalse(
            BulkAggregationService._matches_comment_filter(
                "כאע הטאד M 260316 (249ך)",
                "лат диаг s 260316 (249к)",
            )
        )

    def test_run_tsd_refill_by_name_replays_returned_to_tsd_aggregate(self):
        raw_code = "01046501180412952156bej,nSIQ*?="
        sign_content = make_content_for_sign("7843316794", "04650118042603180000000007", [raw_code])
        detail_payloads = iter([
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "returnedToTsd",
                "productGroup": "wheelChairs",
                "comment": "כאע הטאד S 260316 (249ך)",
                "actions": {"allowReturnToTsd": False, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
                "comment": "כאע הטאד S 260316 (249ך)",
                "actions": {"allowReturnToTsd": False, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "readyForSend",
                "productGroup": "wheelChairs",
                "comment": "כאע הטאד S 260316 (249ך)",
                "actions": {"allowReturnToTsd": False, "allowSave": True},
            },
            {
                "documentId": "doc-1",
                "aggregateCode": "04650118042603180000000007",
                "status": "sentForApprove",
                "productGroup": "wheelChairs",
                "comment": "כאע הטאד S 260316 (249ך)",
                "actions": {"allowReturnToTsd": False, "allowSave": True},
            },
        ])
        post_calls = []

        def kontur_get(url, params=None, timeout=None):
            path = url.split("https://mk.kontur.ru", 1)[1]
            if path == "/api/v1/aggregates":
                status_filter = params["statuses"]
                if status_filter == "returnedToTsd":
                    return FakeResponse(json_data={
                        "items": [{
                            "documentId": "doc-1",
                            "aggregateCode": "04650118042603180000000007",
                            "status": "returnedToTsd",
                            "productGroup": "wheelChairs",
                            "comment": "כאע הטאד S 260316 (249ך)",
                        }],
                        "total": 1,
                    })
                return FakeResponse(json_data={"items": [], "total": 0})
            if path == "/api/v1/aggregates/doc-1":
                return FakeResponse(json_data=next(detail_payloads))
            if path == "/api/v1/aggregates/doc-1/codes":
                return FakeResponse(json_data={"aggregateCodes": [{"ttisCode": raw_code}], "reaggregationCodes": []})
            if path == "/api/v1/aggregates/doc-1/content-for-sign":
                return FakeResponse(json_data=sign_content)
            raise AssertionError(f"Unexpected GET {url} {params}")

        def kontur_post(url, json=None, headers=None, timeout=None):
            post_calls.append((url, json))
            return FakeResponse(json_data={"ok": True})

        service = self.build_service(SimpleNamespace(get=lambda *args, **kwargs: None, post=lambda *args, **kwargs: None))
        kontur_session = make_kontur_session(kontur_get, kontur_post)

        summary = service.run_tsd_refill(
            kontur_session=kontur_session,
            cert_provider=lambda: object(),
            sign_base64_func=lambda cert, data, detached: "BASE64_SIGNATURE",
            tsd_token="tsd-123",
            comment_filter="лат диаг S 260316 (249к)",
        )

        self.assertEqual(service.get_cookie_value(kontur_session, "tsdToken"), "tsd-123")
        self.assertEqual(summary.ready_found, 1)
        self.assertEqual(summary.sent_for_approve, 1)
        self.assertEqual(summary.errors, 0)
        self.assertEqual(len(post_calls), 2)
        self.assertTrue(post_calls[0][0].endswith("/tsd/api/v1/documents/aggregates/doc-1"))
        self.assertTrue(post_calls[1][0].endswith("/api/v1/aggregates/doc-1/send"))


if __name__ == "__main__":
    unittest.main()

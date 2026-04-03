import types
import unittest
from unittest import mock

import ui_v2.api_bridge as api_bridge


class ApiBridgeUiV2Tests(unittest.TestCase):
    def setUp(self):
        self.bridge = api_bridge.ApiBridge()

    def test_get_default_date_window_uses_shared_helper(self):
        with mock.patch.object(
            api_bridge,
            "get_default_production_window",
            return_value=("01-01-2026", "01-01-2031"),
        ):
            self.assertEqual(
                self.bridge.get_default_date_window(),
                {
                    "production_date": "01-01-2026",
                    "expiration_date": "01-01-2031",
                },
            )

    def test_create_aggregation_codes_splits_large_request_into_99_batches(self):
        batch_calls = []

        def fake_create(_session, comment, count):
            batch_calls.append((comment, count))
            return [f"agg-{len(batch_calls)}-{index}" for index in range(count)]

        with (
            mock.patch.object(self.bridge, "_create_aggregate_codes", side_effect=fake_create),
            mock.patch.object(
                self.bridge,
                "_run_with_session_retry",
                side_effect=lambda action, **_kwargs: action(object()),
            ),
            mock.patch.object(self.bridge, "_invalidate_aggregation_cache") as invalidate_mock,
            mock.patch.object(self.bridge, "_log"),
        ):
            result = self.bridge.create_aggregation_codes("латекс S", 250)

        self.assertTrue(result["success"])
        self.assertEqual(result["created_count"], 250)
        self.assertEqual(result["batch_count"], 3)
        self.assertEqual(len(result["items"]), 250)
        self.assertEqual(batch_calls, [("латекс S", 99), ("латекс S", 99), ("латекс S", 52)])
        invalidate_mock.assert_called_once_with()

    def test_create_tsd_tasks_allows_repeat_send_for_order_already_sent_to_tsd(self):
        history_order = {
            "document_id": "doc-1",
            "order_name": "Повторная заявка",
            "full_name": "Перчатки",
            "simpl": "Перчатки",
            "gtin": "04607012345678",
            "tsd_created": True,
        }
        fake_runtime = types.SimpleNamespace(
            history_db=types.SimpleNamespace(get_order_by_document_id=lambda document_id: history_order if document_id == "doc-1" else None),
            download_items=[],
            document_status_cache={},
        )

        with (
            mock.patch.object(self.bridge, "_parse_iso_date", side_effect=lambda value, **_kwargs: value),
            mock.patch.object(self.bridge, "_find_download_item", return_value=None),
            mock.patch.object(self.bridge, "_create_tsd_task_with_retry", return_value=(True, {"introduction_id": "intro-2"})) as create_mock,
            mock.patch.object(self.bridge, "_mark_tsd_created_local") as mark_mock,
            mock.patch.object(self.bridge, "_log"),
            mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
            mock.patch.object(api_bridge, "remove_order_by_document_id", return_value=False) as remove_mock,
        ):
            result = self.bridge.create_tsd_tasks(
                ["doc-1"],
                "INT-1",
                "01-02-2026",
                "01-02-2031",
                "260318",
            )

        self.assertTrue(result["success"])
        self.assertEqual(len(result["results"]), 1)
        self.assertFalse(result["errors"])
        create_mock.assert_called_once()
        retried_item = create_mock.call_args.kwargs["item"]
        self.assertEqual(retried_item["document_id"], "doc-1")
        self.assertEqual(retried_item["status"], "Готов для ТСД")
        mark_mock.assert_called_once_with("doc-1", "intro-2")
        remove_mock.assert_called_once_with(fake_runtime.download_items, "doc-1")


if __name__ == "__main__":
    unittest.main()

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

    def test_upload_intro_positions_from_file_runs_autocomplete(self):
        upload_response = mock.Mock()
        upload_response.raise_for_status.return_value = None
        upload_response.status_code = 201
        upload_response.content = b""

        autocomplete_response = mock.Mock()
        autocomplete_response.status_code = 204
        autocomplete_response.content = b""

        session = mock.Mock()
        session.post.side_effect = [upload_response, autocomplete_response]

        with mock.patch.object(self.bridge, "_log"):
            result = self.bridge._upload_intro_positions_from_file(
                session,
                "intro-123",
                rows_payload={"rows": [{"code": "010000000000000021ABC"}]},
            )

        self.assertIsNone(result)
        self.assertEqual(session.post.call_count, 2)
        upload_call = session.post.call_args_list[0]
        autocomplete_call = session.post.call_args_list[1]
        self.assertTrue(upload_call.args[0].endswith("/api/v1/codes-introduction/intro-123/positions"))
        self.assertEqual(upload_call.kwargs["json"], {"rows": [{"code": "010000000000000021ABC"}]})
        self.assertTrue(autocomplete_call.args[0].endswith("/api/v1/codes-introduction/intro-123/positions/autocomplete"))

    def test_prepare_marking_match_result_allows_partial_matches(self):
        match_result = {
            "matched": {"010000000000000021ABC": {"full_code": "010000000000000021ABC\x1d91EE11\x1d92TAIL"}},
            "groups": [{"order_name": "test", "codes": [{"full_code": "010000000000000021ABC\x1d91EE11\x1d92TAIL"}]}],
            "unmatched": ["010000000000000021MISS"],
            "scanned_files": 12,
        }

        with mock.patch.object(self.bridge, "_log") as log_mock:
            result = self.bridge._prepare_marking_match_result(
                match_result,
                action_label="Ввод в оборот выбранных АК",
            )

        self.assertEqual(result["matched_count"], 1)
        self.assertEqual(result["unmatched_count"], 1)
        self.assertEqual(result["unmatched_preview"], ["010000000000000021MISS"])
        self.assertEqual(result["scanned_files"], 12)
        log_mock.assert_called_once()

    def test_prepare_marking_match_result_raises_when_no_full_codes_found(self):
        match_result = {
            "matched": {},
            "groups": [],
            "unmatched": ["010000000000000021MISS"],
            "scanned_files": 4,
        }

        with self.assertRaisesRegex(RuntimeError, "Не удалось найти полные коды"):
            self.bridge._prepare_marking_match_result(
                match_result,
                action_label="Ввод в оборот выбранных АК",
            )


if __name__ == "__main__":
    unittest.main()

from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

import ui_v2.api_bridge as api_bridge


class ApiBridgeUiV2Tests(unittest.TestCase):
    def setUp(self):
        self.bridge = api_bridge.ApiBridge()

    def test_normalize_ui_text_repairs_latin1_cp1251_mojibake(self):
        broken = "\u00c7\u00e0\u00e3\u00f0\u00f3\u00e6\u00e0\u00e5\u00ec"
        self.assertEqual(api_bridge._normalize_ui_text(broken), "\u0417\u0430\u0433\u0440\u0443\u0436\u0430\u0435\u043c")

    def test_normalize_ui_text_repairs_cp1251_utf8_mojibake(self):
        correct = "\u0423\u043a\u0430\u0436\u0438\u0442\u0435 \u043d\u0430\u0437\u0432\u0430\u043d\u0438\u0435 \u0437\u0430\u044f\u0432\u043a\u0438."
        broken = correct.encode("utf-8").decode("cp1251")
        self.assertEqual(api_bridge._normalize_ui_text(broken), correct)

    def test_normalize_ui_text_keeps_correct_russian(self):
        correct = "\u0417\u0430\u043f\u0443\u0441\u043a\u0430\u0435\u043c \u0432\u0432\u043e\u0434 \u0432 \u043e\u0431\u043e\u0440\u043e\u0442"
        self.assertEqual(api_bridge._normalize_ui_text(correct), correct)

    def test_desktop_data_dir_resolves_existing_marking_codes_folder(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            desktop_dir = temp_root / "Desktop"
            target_dir = desktop_dir / "\u041a\u043e\u0434\u044b \u043a\u043c"
            target_dir.mkdir(parents=True)

            with mock.patch.object(api_bridge.Path, "home", return_value=temp_root):
                resolved = api_bridge._desktop_data_dir(api_bridge.MARKING_CODES_DIRNAME)

            self.assertEqual(resolved, target_dir)

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

    def test_get_orders_view_state_uses_fast_local_history_snapshot(self):
        history_items = [
            {
                "document_id": f"doc-{index}",
                "order_name": f"Order {index}",
                "status": "created",
            }
            for index in range(300)
        ]
        fake_runtime = types.SimpleNamespace(
            order_queue=[],
            session_orders=[],
            history_db=types.SimpleNamespace(get_all_orders=lambda: history_items),
        )
        normalized_ids = []

        def fake_normalize(item, *, session=None, include_marking_status=False):
            self.assertIsNone(session)
            self.assertFalse(include_marking_status)
            normalized_ids.append(item["document_id"])
            return {"document_id": item["document_id"]}

        with (
            mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
            mock.patch.object(self.bridge, "_ensure_session_safely") as ensure_session_mock,
            mock.patch.object(self.bridge, "_get_deleted_document_ids", return_value={"doc-0"}),
            mock.patch.object(self.bridge, "_load_deleted_orders", return_value=[]),
            mock.patch.object(self.bridge, "_normalize_history_item", side_effect=fake_normalize),
        ):
            result = self.bridge.get_orders_view_state()

        self.assertNotIn("error", result)
        self.assertEqual(len(result["history"]), 250)
        self.assertEqual(normalized_ids, [f"doc-{index}" for index in range(1, 251)])
        ensure_session_mock.assert_not_called()

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
        self.assertEqual(api_bridge._normalize_ui_text(retried_item["status"]), "Готов для ТСД")
        mark_mock.assert_called_once_with("doc-1", "intro-2")
        remove_mock.assert_called_once_with(fake_runtime.download_items, "doc-1")

    def test_introduce_orders_auto_downloads_missing_files_before_intro(self):
        item = {
            "document_id": "doc-1",
            "order_name": "Order 1",
            "status": "released",
            "filename": "",
            "csv_path": "",
            "simpl": "Gloves",
        }

        def fake_download(_session, current_item, log_prefix=""):
            current_item["filename"] = "codes.csv"
            current_item["csv_path"] = "codes.csv"
            current_item["status"] = "Скачан"
            return current_item

        with (
            mock.patch.object(self.bridge, "_ensure_session", return_value=object()),
            mock.patch.object(self.bridge, "_get_thumbprint", return_value="thumb"),
            mock.patch.object(self.bridge, "_parse_iso_date", side_effect=lambda value, **_kwargs: value),
            mock.patch.object(self.bridge, "_get_order_for_document_id", return_value=item),
            mock.patch.object(self.bridge, "_download_order_internal", side_effect=fake_download) as download_mock,
            mock.patch.object(self.bridge, "_sync_history_from_download_item"),
            mock.patch.object(self.bridge, "_log"),
            mock.patch.object(api_bridge, "put_into_circulation", return_value=(True, {"introduction_id": "intro-1"})) as intro_mock,
        ):
            result = self.bridge.introduce_orders(
                ["doc-1"],
                "01-01-2026",
                "01-01-2031",
                "260330",
            )

        self.assertTrue(result["success"])
        download_mock.assert_called_once()
        intro_mock.assert_called_once()

    def test_download_selected_aggregations_saves_separate_files_by_comment(self):
        aggregates = [
            types.SimpleNamespace(
                aggregate_code="A1",
                document_id="doc-1",
                status="readyForSend",
                includes_units_count=1,
                comment="Alpha",
                product_group="wheelChairs",
                codes_check_errors_count=0,
            ),
            types.SimpleNamespace(
                aggregate_code="A2",
                document_id="doc-2",
                status="readyForSend",
                includes_units_count=1,
                comment="Alpha",
                product_group="wheelChairs",
                codes_check_errors_count=0,
            ),
            types.SimpleNamespace(
                aggregate_code="B1",
                document_id="doc-3",
                status="readyForSend",
                includes_units_count=1,
                comment="Beta",
                product_group="wheelChairs",
                codes_check_errors_count=0,
            ),
        ]

        with (
            mock.patch.object(self.bridge, "_resolve_aggregate_infos_by_ids", return_value=aggregates),
            mock.patch.object(self.bridge, "_save_simple_aggregation_csv", side_effect=lambda items, filename: f"C:/tmp/{filename}") as save_mock,
            mock.patch.object(self.bridge, "_run_with_session_retry", side_effect=lambda action, **_kwargs: action(object())),
            mock.patch.object(self.bridge, "_log"),
        ):
            result = self.bridge.download_selected_aggregations(["doc-1", "doc-2", "doc-3"])

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 3)
        self.assertEqual(len(result["saved_paths"]), 2)
        self.assertEqual([group["comment"] for group in result["groups"]], ["Alpha", "Beta"])
        self.assertEqual(save_mock.call_count, 2)

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

        with self.assertRaises(RuntimeError) as error_context:
            self.bridge._prepare_marking_match_result(
                match_result,
                action_label="Ввод в оборот выбранных АК",
            )
        self.assertIn("Не удалось найти полные коды", api_bridge._normalize_ui_text(str(error_context.exception)))

    def test_introduce_selected_aggregations_sends_when_document_stays_created_after_codes_check(self):
        aggregate = types.SimpleNamespace(
            document_id="agg-doc-1",
            aggregate_code="AGG-1",
            product_group="wheelChairs",
            status="readyForSend",
            comment="test",
            includes_units_count=1,
            codes_check_errors_count=0,
        )
        fake_state = types.SimpleNamespace(
            status="EMITTED",
            api_error=None,
            raw_code="010000000000000021ABC",
            sntin="010000000000000021ABC",
        )
        fake_service = types.SimpleNamespace(
            fetch_aggregate_codes=lambda _session, _document_id: (["010000000000000021ABC"], []),
            _resolve_true_product_group=lambda product_group: product_group,
            fetch_code_states=lambda **_kwargs: [fake_state],
        )
        fake_runtime = types.SimpleNamespace(bulk_aggregation_service=fake_service)

        with (
            mock.patch.object(self.bridge, "_get_certificate", return_value=object()),
            mock.patch.object(self.bridge, "_parse_iso_date", side_effect=lambda value, **_kwargs: value),
            mock.patch.object(self.bridge, "_resolve_aggregate_infos_by_ids", return_value=[aggregate]),
            mock.patch.object(self.bridge, "_match_saved_marking_codes", return_value={
                "matched": {"010000000000000021ABC": {"full_code": "010000000000000021ABC\x1d91EE11\x1d92TAIL"}},
                "groups": [{
                    "order_name": "order-1",
                    "gtin": "04650118041257",
                    "full_name": "Перчатки",
                    "source_path": "codes.csv",
                    "codes": [{"full_code": "010000000000000021ABC\x1d91EE11\x1d92TAIL"}],
                }],
                "unmatched": [],
                "scanned_files": 1,
            }),
            mock.patch.object(self.bridge, "_lookup_intro_product_metadata", return_value={
                "gtin": "04650118041257",
                "full_name": "Перчатки",
                "simpl_name": "Перчатки",
                "tnved_code": "EE11",
            }),
            mock.patch.object(self.bridge, "_create_exact_intro_file_document", return_value="intro-123"),
            mock.patch.object(self.bridge, "_build_intro_upload_rows", return_value={"rows": [{"code": "010000000000000021ABC"}]}),
            mock.patch.object(self.bridge, "_upload_intro_positions_from_file"),
            mock.patch.object(self.bridge, "_wait_for_intro_codes_check", return_value={"status": "doesNotHaveErrors"}),
            mock.patch.object(self.bridge, "_get_intro_production_state", return_value={"documentStatus": "created", "positions": []}),
            mock.patch.object(self.bridge, "_get_intro_document_state", return_value={"documentStatus": "created"}),
            mock.patch.object(self.bridge, "_sign_and_send_intro_document", return_value={
                "generated_count": 1,
                "send_response": {"ok": True},
                "final_introduction": {"documentStatus": "introduced"},
                "final_check": {"status": "doesNotHaveErrors"},
            }) as sign_mock,
            mock.patch.object(self.bridge, "_log"),
            mock.patch.object(self.bridge, "_run_with_session_retry", side_effect=lambda action, **_kwargs: action(object())),
            mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
        ):
            result = self.bridge.introduce_selected_aggregations(
                ["agg-doc-1"],
                "01-01-2026",
                "01-01-2031",
                "260318",
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["summary"]["introduced_codes"], 1)
        sign_mock.assert_called_once()

    def test_introduce_selected_aggregations_skips_codes_with_unavailable_true_api_status(self):
        aggregate = types.SimpleNamespace(
            document_id="agg-doc-1",
            aggregate_code="AGG-1",
            product_group="wheelChairs",
            status="readyForSend",
            comment="test",
            includes_units_count=2,
            codes_check_errors_count=0,
        )
        good_state = types.SimpleNamespace(
            status="EMITTED",
            api_error=None,
            raw_code="010000000000000021GOOD",
            sntin="010000000000000021GOOD",
        )
        bad_state = types.SimpleNamespace(
            status="UNKNOWN",
            api_error="True API error",
            raw_code="010000000000000021BAD",
            sntin="010000000000000021BAD",
        )

        fetch_code_states_mock = mock.Mock(
            side_effect=lambda **kwargs: (
                [good_state, bad_state]
                if len(kwargs["raw_codes"]) == 2
                else [bad_state]
            )
        )
        fake_service = types.SimpleNamespace(
            fetch_aggregate_codes=lambda _session, _document_id: ([good_state.raw_code, bad_state.raw_code], []),
            _resolve_true_product_group=lambda product_group: product_group,
            fetch_code_states=fetch_code_states_mock,
        )
        fake_runtime = types.SimpleNamespace(bulk_aggregation_service=fake_service)

        with (
            mock.patch.object(self.bridge, "_get_certificate", return_value=object()),
            mock.patch.object(self.bridge, "_parse_iso_date", side_effect=lambda value, **_kwargs: value),
            mock.patch.object(self.bridge, "_resolve_aggregate_infos_by_ids", return_value=[aggregate]),
            mock.patch.object(self.bridge, "_match_saved_marking_codes", return_value={
                "matched": {good_state.raw_code: {"full_code": "010000000000000021GOOD\x1d91EE11\x1d92TAIL"}},
                "groups": [{
                    "order_name": "order-1",
                    "gtin": "04650118041257",
                    "full_name": "Перчатки",
                    "source_path": "codes.csv",
                    "codes": [{"full_code": "010000000000000021GOOD\x1d91EE11\x1d92TAIL"}],
                }],
                "unmatched": [],
                "scanned_files": 1,
            }),
            mock.patch.object(self.bridge, "_lookup_intro_product_metadata", return_value={
                "gtin": "04650118041257",
                "full_name": "Перчатки",
                "simpl_name": "Перчатки",
                "tnved_code": "EE11",
            }),
            mock.patch.object(self.bridge, "_create_exact_intro_file_document", return_value="intro-123"),
            mock.patch.object(self.bridge, "_build_intro_upload_rows", return_value={"rows": [{"code": "010000000000000021GOOD"}]}),
            mock.patch.object(self.bridge, "_upload_intro_positions_from_file"),
            mock.patch.object(self.bridge, "_wait_for_intro_codes_check", return_value={"status": "doesNotHaveErrors"}),
            mock.patch.object(self.bridge, "_get_intro_production_state", return_value={"documentStatus": "created", "positions": []}),
            mock.patch.object(self.bridge, "_get_intro_document_state", return_value={"documentStatus": "created"}),
            mock.patch.object(self.bridge, "_sign_and_send_intro_document", return_value={
                "generated_count": 1,
                "send_response": {"ok": True},
                "final_introduction": {"documentStatus": "introduced"},
                "final_check": {"status": "doesNotHaveErrors"},
            }) as sign_mock,
            mock.patch.object(self.bridge, "_log"),
            mock.patch.object(self.bridge, "_run_with_session_retry", side_effect=lambda action, **_kwargs: action(object())),
            mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
        ):
            result = self.bridge.introduce_selected_aggregations(
                ["agg-doc-1"],
                "01-01-2026",
                "01-01-2031",
                "260330",
            )

        self.assertTrue(result["success"])
        self.assertEqual(result["summary"]["introduced_codes"], 1)
        self.assertEqual(result["summary"]["skipped_api_error_codes"], 1)
        self.assertEqual(result["summary"]["skipped_api_error_preview"], [bad_state.sntin])
        self.assertGreaterEqual(fetch_code_states_mock.call_count, 2)
        sign_mock.assert_called_once()

    def test_preview_100x180_label_requests_manual_form_when_auto_metadata_fails(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            template_path = temp_root / "template.btw"
            template_path.write_text("template", encoding="utf-8")
            csv_path = temp_root / "codes.csv"
            csv_path.write_text("010000000000000021ABC\t04650118041257\tName\n", encoding="utf-8")
            order_data = {"document_id": "doc-1", "order_name": "Order M 260330", "gtin": "04650118041257"}
            fake_runtime = types.SimpleNamespace(
                history_db=types.SimpleNamespace(get_order_by_document_id=lambda document_id: order_data if document_id == "doc-1" else None)
            )

            with (
                mock.patch.object(self.bridge, "_load_nomenclature_df", return_value=object()),
                mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
                mock.patch.object(api_bridge, "list_label_templates", return_value=[
                    types.SimpleNamespace(path=str(template_path), data_source_kind="marking", category="Templates", name="Template")
                ]),
                mock.patch.object(api_bridge, "build_label_print_context", side_effect=RuntimeError("GTIN 04650118041257 не найден в nomenclature.xlsx")),
                mock.patch.object(self.bridge, "_log"),
            ):
                result = self.bridge.preview_100x180_label(
                    {
                        "sheet_format": "100x180",
                        "document_id": "doc-1",
                        "template_path": str(template_path),
                        "csv_path": str(csv_path),
                        "printer_name": "Printer",
                        "manufacture_date": "2026-01",
                        "expiration_date": "2031-01",
                        "quantity_value": "10",
                    }
                )

        self.assertTrue(result["success"])
        self.assertTrue(result["needs_manual_input"])
        self.assertIn("fields", result["manual_form"])

    def test_preview_100x180_label_uses_manual_override_when_provided(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            template_path = temp_root / "template.btw"
            template_path.write_text("template", encoding="utf-8")
            csv_path = temp_root / "codes.csv"
            csv_path.write_text("010000000000000021ABC\t04650118041257\tName\n", encoding="utf-8")
            order_data = {"document_id": "doc-1", "order_name": "Order 260330", "gtin": ""}
            fake_runtime = types.SimpleNamespace(
                history_db=types.SimpleNamespace(get_order_by_document_id=lambda document_id: order_data if document_id == "doc-1" else None)
            )

            with (
                mock.patch.object(self.bridge, "_load_nomenclature_df", return_value=object()),
                mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
                mock.patch.object(api_bridge, "list_label_templates", return_value=[
                    types.SimpleNamespace(path=str(template_path), data_source_kind="marking", category="Templates", name="Template")
                ]),
                mock.patch.object(api_bridge, "build_label_print_context", side_effect=RuntimeError("GTIN missing")),
                mock.patch.object(self.bridge, "_parse_iso_date", side_effect=lambda value, **_kwargs: f"{value}-01" if len(str(value)) == 7 else value),
                mock.patch.object(self.bridge, "_log"),
            ):
                result = self.bridge.preview_100x180_label(
                    {
                        "sheet_format": "100x180",
                        "document_id": "doc-1",
                        "template_path": str(template_path),
                        "csv_path": str(csv_path),
                        "printer_name": "Printer",
                        "manufacture_date": "2026-01",
                        "expiration_date": "2031-01",
                        "quantity_value": "10",
                        "manual_override": {
                            "enabled": True,
                            "gtin": "04650118041257",
                            "size": "M",
                            "batch": "260330",
                            "color": "",
                            "units_per_pack": "10",
                        },
                    }
                )

        self.assertTrue(result["success"])
        self.assertEqual(result["preview"]["size"], "M")
        self.assertEqual(result["preview"]["batch"], "260330")
        self.assertTrue(result["preview"]["manual_override_used"])

    def test_print_download_order_supports_single_record_number(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            csv_path = temp_root / "codes.csv"
            csv_path.write_text(
                "010000000000000021AAA\t04650118041257\tName A\n"
                "010000000000000021BBB\t04650118041257\tName B\n",
                encoding="utf-8",
            )
            item = {"document_id": "doc-1", "order_name": "Order 1"}
            built_contexts = []
            cleanup_callbacks = []

            def fake_build_print_context(**kwargs):
                built_contexts.append(kwargs)
                return types.SimpleNamespace(
                    order_name="Order 1",
                    document_id="doc-1",
                    csv_path=kwargs["csv_path"],
                    template_path="template.btw",
                    printer_name=kwargs["printer_name"],
                    size="M",
                    label_count=1,
                )

            def capture_background_job(**kwargs):
                cleanup_callbacks.append(kwargs["cleanup"])

            with (
                mock.patch.object(self.bridge, "_find_download_item", return_value=item),
                mock.patch.object(self.bridge, "_resolve_order_csv_path", return_value=str(csv_path)),
                mock.patch.object(api_bridge, "build_print_context", side_effect=fake_build_print_context),
                mock.patch.object(self.bridge, "_run_background_job", side_effect=capture_background_job),
                mock.patch.object(self.bridge, "_log"),
            ):
                result = self.bridge.print_download_order("doc-1", "Printer", 2)

            self.assertTrue(result["success"])
            self.assertEqual(result["selection"]["selected_record_number"], 2)
            self.assertEqual(len(built_contexts), 1)
            self.assertNotEqual(built_contexts[0]["csv_path"], str(csv_path))
            selected_csv = Path(built_contexts[0]["csv_path"])
            self.assertIn("010000000000000021BBB", selected_csv.read_text(encoding="utf-8-sig"))
            cleanup_callbacks[0]()
            self.assertFalse(selected_csv.exists())

    def test_print_100x180_label_returns_after_queueing_background_print(self):
        order_data = {"document_id": "doc-1", "order_name": "Заказ 1"}
        single_context = types.SimpleNamespace(document_id="doc-1", order_name="Заказ 1")
        selection = {
            "print_scope": "single",
            "selected_record_number": 3,
            "record_preview": {"value_short": "CODE-3"},
            "cleanup_path": "temp.csv",
        }
        fake_runtime = types.SimpleNamespace(
            history_db=types.SimpleNamespace(get_order_by_document_id=lambda document_id: order_data if document_id == "doc-1" else None)
        )

        background_jobs = []

        def capture_background_job(**kwargs):
            background_jobs.append(kwargs)

        with (
            mock.patch.object(api_bridge, "_get_runtime", return_value=fake_runtime),
            mock.patch.object(
                self.bridge,
                "_resolve_label_template_info",
                return_value={"path": "template.btw", "name": "Шаблон"},
            ),
            mock.patch.object(self.bridge, "_resolve_label_print_selection", return_value=selection),
            mock.patch.object(
                self.bridge,
                "_resolve_label_context",
                return_value={"context": single_context, "used_manual_override": False},
            ),
            mock.patch.object(self.bridge, "_serialize_label_preview", return_value={"document_id": "doc-1", "order_name": "Заказ 1"}),
            mock.patch.object(self.bridge, "_cleanup_label_selection") as cleanup_mock,
            mock.patch.object(self.bridge, "_run_background_job", side_effect=capture_background_job),
            mock.patch.object(api_bridge, "print_label_sheet") as print_mock,
            mock.patch.object(self.bridge, "_log") as log_mock,
        ):
            result = self.bridge.print_100x180_label(
                {
                    "sheet_format": "100x180",
                    "document_id": "doc-1",
                    "template_path": "template.btw",
                    "csv_path": "codes.csv",
                    "printer_name": "Printer",
                    "manufacture_date": "01-01-2026",
                    "expiration_date": "01-01-2031",
                    "quantity_value": "200",
                    "print_scope": "single",
                    "record_number": 3,
                }
            )

            self.assertTrue(result["success"])
            self.assertEqual(result["preview"]["document_id"], "doc-1")
            cleanup_mock.assert_not_called()
            self.assertEqual(len(background_jobs), 1)
            background_kwargs = background_jobs[0]
            self.assertEqual(background_kwargs["error_log_channel"], "labels")
            self.assertEqual(background_kwargs["error_log_prefix"], "Ошибка печати 100x180")
            print_mock.assert_not_called()
            background_kwargs["action"]()
            print_mock.assert_called_once_with(single_context)
            background_kwargs["cleanup"]()
            cleanup_mock.assert_called_once_with(
                selection,
                delay_seconds=api_bridge.LABEL_PRINT_SELECTION_CLEANUP_DELAY_SECONDS,
            )
            logged_messages = [str(call.args[1]) for call in log_mock.call_args_list]
            self.assertTrue(any("фоновую очередь" in message for message in logged_messages))
            self.assertTrue(any("BarTender принял задание" in message for message in logged_messages))


if __name__ == "__main__":
    unittest.main()

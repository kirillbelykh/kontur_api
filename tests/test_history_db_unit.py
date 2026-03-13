import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from history_db import OrderHistoryDB


class OrderHistoryDBTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_path = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_migrates_legacy_orders_into_repo_history_file(self):
        db_path = self.base_path / "full_orders_history.json"
        legacy_path = self.base_path / "orders_history.json"
        legacy_payload = {
            "orders": [
                {
                    "document_id": "LEGACY-1",
                    "order_name": "legacy order",
                    "status": "Ожидает",
                }
            ]
        }
        legacy_path.write_text(json.dumps(legacy_payload, ensure_ascii=False), encoding="utf-8")

        db = OrderHistoryDB(db_file=str(db_path), legacy_db_files=[str(legacy_path)])

        migrated_order = db.get_order_by_document_id("LEGACY-1")
        self.assertIsNotNone(migrated_order)
        self.assertEqual(migrated_order["order_name"], "legacy order")

    def test_add_order_updates_existing_record_instead_of_duplicating_it(self):
        db = OrderHistoryDB(db_file=str(self.base_path / "full_orders_history.json"), legacy_db_files=[])

        db.add_order({
            "document_id": "DOC-1",
            "order_name": "first",
            "status": "Ожидает",
        })
        db.add_order({
            "document_id": "DOC-1",
            "status": "Скачан",
            "filename": "codes.csv",
            "gtin": "1234567890123",
        })

        all_orders = db.get_all_orders()
        self.assertEqual(len(all_orders), 1)
        self.assertEqual(all_orders[0]["status"], "Скачан")
        self.assertEqual(all_orders[0]["filename"], "codes.csv")
        self.assertEqual(all_orders[0]["gtin"], "1234567890123")

    def test_mark_tsd_created_updates_existing_order(self):
        db = OrderHistoryDB(db_file=str(self.base_path / "full_orders_history.json"), legacy_db_files=[])
        db.add_order({
            "document_id": "DOC-2",
            "order_name": "for tsd",
            "status": "Скачан",
        })

        db.mark_tsd_created("DOC-2", "INTRO-77")

        order = db.get_order_by_document_id("DOC-2")
        self.assertIsNotNone(order)
        self.assertTrue(order["tsd_created"])
        self.assertEqual(order["tsd_intro_number"], "INTRO-77")

    def test_ignores_unavailable_legacy_path(self):
        db_path = self.base_path / "full_orders_history.json"
        unavailable_legacy = self.base_path / "legacy_unavailable.json"
        original_exists = Path.exists

        def fake_exists(path_obj):
            if path_obj == unavailable_legacy:
                raise OSError("network unavailable")
            return original_exists(path_obj)

        with patch("history_db.Path.exists", autospec=True, side_effect=fake_exists):
            db = OrderHistoryDB(db_file=str(db_path), legacy_db_files=[str(unavailable_legacy)])

        self.assertEqual(db.get_all_orders(), [])

    def test_merge_prefers_newer_record_values(self):
        db = OrderHistoryDB(db_file=str(self.base_path / "full_orders_history.json"), legacy_db_files=[])
        current = {
            "document_id": "DOC-4",
            "status": "Ожидает",
            "updated_at": "2026-03-01T10:00:00",
            "updated_by": "pc-1",
        }
        incoming = {
            "document_id": "DOC-4",
            "status": "Скачан",
            "updated_at": "2026-03-01T11:00:00",
            "updated_by": "pc-2",
        }

        merged = db._merge_order_records(current, incoming)

        self.assertEqual(merged["status"], "Скачан")
        self.assertEqual(merged["updated_by"], "pc-2")
        self.assertEqual(merged["updated_at"], "2026-03-01T11:00:00")

    def test_merge_history_payloads_keeps_orders_from_both_sources(self):
        db = OrderHistoryDB(db_file=str(self.base_path / "full_orders_history.json"), legacy_db_files=[])
        remote_data = {
            "orders": [
                {
                    "document_id": "REMOTE-1",
                    "status": "Ожидает",
                    "created_at": "2026-03-01T09:00:00",
                    "updated_at": "2026-03-01T09:00:00",
                }
            ]
        }
        local_data = {
            "orders": [
                {
                    "document_id": "LOCAL-1",
                    "status": "Скачан",
                    "created_at": "2026-03-01T10:00:00",
                    "updated_at": "2026-03-01T10:00:00",
                }
            ]
        }

        merged = db._merge_history_payloads(remote_data, local_data)
        merged_ids = {order["document_id"] for order in merged["orders"]}

        self.assertEqual(merged_ids, {"REMOTE-1", "LOCAL-1"})

    def test_init_requests_startup_sync_with_push(self):
        db_path = self.base_path / "full_orders_history.json"
        with patch.object(OrderHistoryDB, "sync_with_github", autospec=True) as sync_mock:
            OrderHistoryDB(db_file=str(db_path), legacy_db_files=[])

        sync_mock.assert_called_once()
        _, kwargs = sync_mock.call_args
        self.assertTrue(kwargs.get("force"))
        self.assertTrue(kwargs.get("push"))
        self.assertEqual(kwargs.get("reason"), "startup")

    def test_add_order_attempts_sync_even_when_record_is_unchanged(self):
        db = OrderHistoryDB(db_file=str(self.base_path / "full_orders_history.json"), legacy_db_files=[])
        order_data = {
            "document_id": "DOC-SYNC-1",
            "order_name": "sync test",
            "status": "Ожидает",
        }

        with patch.object(db, "_sync_with_github_locked", autospec=True, return_value=False) as sync_mock:
            db.add_order(order_data)
            db.add_order(order_data)

        self.assertEqual(sync_mock.call_count, 2)
        for _, kwargs in sync_mock.call_args_list:
            self.assertTrue(kwargs.get("push"))
            self.assertEqual(kwargs.get("reason"), "add_order")


if __name__ == "__main__":
    unittest.main()

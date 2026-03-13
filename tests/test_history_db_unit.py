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


if __name__ == "__main__":
    unittest.main()

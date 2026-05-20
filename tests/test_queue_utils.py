from pathlib import Path
import tempfile
import unittest

from queue_utils import (
    get_download_tab_status,
    get_intro_tab_status,
    get_tsd_tab_status,
)


class QueueUtilsTests(unittest.TestCase):
    def test_download_tab_status_depends_on_local_files_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "order.csv"
            csv_path.write_text("code\n", encoding="utf-8")
            self.assertEqual(get_download_tab_status({"csv_path": str(csv_path)}), "Скачан")
        self.assertEqual(get_download_tab_status({"csv_path": r"C:\missing\order.csv"}), "Не скачан")

    def test_intro_tab_status_maps_only_introduced_orders(self):
        self.assertEqual(get_intro_tab_status({"status": "introduced"}), "Введены в оборот")
        self.assertEqual(get_intro_tab_status({"status": "applied"}), "Введены в оборот")
        self.assertEqual(get_intro_tab_status({"status": "released"}), "Не введены в оборот")

    def test_tsd_tab_status_uses_workflow_specific_labels(self):
        self.assertEqual(get_tsd_tab_status({"status": "introduced"}), "Введены в оборот")
        self.assertEqual(get_tsd_tab_status({"status": "downloaded", "tsd_created": True}), "Отправлено")
        self.assertEqual(get_tsd_tab_status({"status": "downloaded"}), "Наполнен на ТСД")
        self.assertEqual(get_tsd_tab_status({"status": "created"}), "Не отправлено")


if __name__ == "__main__":
    unittest.main()

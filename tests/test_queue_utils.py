import unittest

from queue_utils import (
    is_order_ready_for_intro,
    is_order_ready_for_tsd,
    remove_order_by_document_id,
)


class OrderReadinessTests(unittest.TestCase):
    def test_intro_requires_downloaded_status(self):
        self.assertTrue(is_order_ready_for_intro({"document_id": "doc-1", "status": "Скачан"}))
        self.assertFalse(is_order_ready_for_intro({"document_id": "doc-1", "status": "Ожидает"}))

    def test_intro_allows_order_with_downloaded_file(self):
        self.assertTrue(
            is_order_ready_for_intro({"document_id": "doc-1", "status": "Из истории", "filename": "codes.csv"})
        )

    def test_tsd_allows_fresh_order_before_download(self):
        self.assertTrue(is_order_ready_for_tsd({"document_id": "doc-1", "status": "Ожидает"}))
        self.assertTrue(is_order_ready_for_tsd({"document_id": "doc-1", "status": "Скачан"}))
        self.assertFalse(is_order_ready_for_tsd({"document_id": "doc-1", "status": "Ошибка генерации"}))


class RemoveOrderByDocumentIdTests(unittest.TestCase):
    def test_removes_matching_order_in_place(self):
        download_list = [
            {"document_id": "doc-1", "status": "Скачан"},
            {"document_id": "doc-2", "status": "Скачан"},
        ]

        removed = remove_order_by_document_id(download_list, "doc-1")

        self.assertTrue(removed)
        self.assertEqual(download_list, [{"document_id": "doc-2", "status": "Скачан"}])

    def test_returns_false_when_order_missing(self):
        download_list = [{"document_id": "doc-2", "status": "Скачан"}]

        removed = remove_order_by_document_id(download_list, "doc-1")

        self.assertFalse(removed)
        self.assertEqual(download_list, [{"document_id": "doc-2", "status": "Скачан"}])

    def test_returns_false_for_empty_document_id(self):
        download_list = [{"document_id": "doc-2", "status": "Скачан"}]

        removed = remove_order_by_document_id(download_list, "")

        self.assertFalse(removed)
        self.assertEqual(download_list, [{"document_id": "doc-2", "status": "Скачан"}])


if __name__ == "__main__":
    unittest.main()

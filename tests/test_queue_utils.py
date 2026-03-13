import unittest

from queue_utils import remove_order_by_document_id


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

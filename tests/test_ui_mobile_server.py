import unittest

import ui_mobile.server_mobile as mobile_server


class UiMobileServerTests(unittest.TestCase):
    def test_mobile_client_config_keeps_labels_and_printing_enabled(self):
        self.assertFalse(mobile_server.CLIENT_CONFIG["disableLabels"])
        self.assertFalse(mobile_server.CLIENT_CONFIG["disablePrinting"])

    def test_mobile_server_allows_label_printing_methods(self):
        for method_name in (
            "get_labels_state",
            "preview_100x180_label",
            "print_100x180_label",
            "print_download_order",
        ):
            self.assertIn(method_name, mobile_server.ALLOWED_METHODS)
            self.assertNotIn(method_name, mobile_server.BLOCKED_METHODS)


if __name__ == "__main__":
    unittest.main()

import unittest

import ui_mobile.server_mobile as mobile_server


class UiMobileServerTests(unittest.TestCase):
    def test_normalize_route_falls_back_to_orders(self):
        self.assertEqual(mobile_server._normalize_route("download"), "download")
        self.assertEqual(mobile_server._normalize_route("unknown"), "orders")

    def test_build_client_config_supports_embedded_mode(self):
        config = mobile_server._build_client_config(embedded=True, initial_route="labels")
        self.assertTrue(config["embeddedMode"])
        self.assertEqual(config["initialRoute"], "labels")

    def test_mobile_client_config_keeps_labels_and_printing_enabled(self):
        self.assertFalse(mobile_server.CLIENT_CONFIG["disableLabels"])
        self.assertFalse(mobile_server.CLIENT_CONFIG["disablePrinting"])

    def test_mobile_server_allows_label_printing_methods(self):
        for method_name in (
            "acknowledge_wms_chz_request",
            "acknowledge_wms_chz_requests",
            "archive_selected_aggregations",
            "archive_wms_chz_requests",
            "get_auth_state",
            "get_chz_requests_view_state",
            "get_labels_state",
            "mark_wms_chz_request_ready",
            "mark_wms_chz_requests_ready",
            "preview_100x180_label",
            "print_100x180_label",
            "print_download_order",
            "prolong_kontur_access",
            "remove_order_item",
            "restore_wms_chz_requests",
        ):
            self.assertIn(method_name, mobile_server.ALLOWED_METHODS)
            self.assertNotIn(method_name, mobile_server.BLOCKED_METHODS)


if __name__ == "__main__":
    unittest.main()

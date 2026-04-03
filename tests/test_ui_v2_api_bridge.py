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


if __name__ == "__main__":
    unittest.main()

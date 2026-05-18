import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import cookies


class _FakeThread:
    def __init__(self, *args, **kwargs):
        self.target = kwargs.get("target")
        self.daemon = kwargs.get("daemon", False)
        self.name = kwargs.get("name", "")
        self.started = False

    def start(self):
        self.started = True

    def is_alive(self):
        return self.started


class CookiesProlongationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.state_path = Path(self.temp_dir.name) / "kontur_access_prolongation.json"
        self.original_thread = cookies._PROLONGATION_THREAD
        cookies._PROLONGATION_THREAD = None

    def tearDown(self):
        cookies._PROLONGATION_THREAD = self.original_thread
        self.temp_dir.cleanup()

    def test_prolongation_state_is_due_when_state_file_is_missing(self):
        with (
            mock.patch.object(cookies, "PROLONGATION_STATE_FILE", self.state_path),
            mock.patch.dict(
                os.environ,
                {
                    cookies.PROLONGATION_ENABLED_ENV: "1",
                    cookies.PROLONGATION_INTERVAL_HOURS_ENV: "9",
                },
                clear=False,
            ),
        ):
            state = cookies.get_kontur_access_prolongation_state()

        self.assertTrue(state["enabled"])
        self.assertTrue(state["due"])
        self.assertEqual(state["last_success_ts"], 0.0)
        self.assertEqual(state["seconds_until_due"], 0.0)

    def test_prolong_kontur_access_skips_when_not_due(self):
        recent_ts = 1_800_000_000.0
        self.state_path.write_text(
            json.dumps({"last_success_ts": recent_ts}, ensure_ascii=False),
            encoding="utf-8",
        )

        with (
            mock.patch.object(cookies, "PROLONGATION_STATE_FILE", self.state_path),
            mock.patch.dict(
                os.environ,
                {
                    cookies.PROLONGATION_ENABLED_ENV: "1",
                    cookies.PROLONGATION_INTERVAL_HOURS_ENV: "9",
                },
                clear=False,
            ),
            mock.patch("time.time", return_value=recent_ts + 60.0),
            mock.patch.object(cookies, "_run_kontur_access_prolongation_browser_flow") as flow_mock,
        ):
            result = cookies.prolong_kontur_access(force=False)

        self.assertTrue(result["success"])
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "not_due")
        flow_mock.assert_not_called()

    def test_successful_prolongation_updates_state_file(self):
        with (
            mock.patch.object(cookies, "PROLONGATION_STATE_FILE", self.state_path),
            mock.patch.dict(
                os.environ,
                {
                    cookies.PROLONGATION_ENABLED_ENV: "1",
                    cookies.PROLONGATION_INTERVAL_HOURS_ENV: "9",
                },
                clear=False,
            ),
            mock.patch.object(cookies, "_run_kontur_access_prolongation_browser_flow") as flow_mock,
        ):
            result = cookies.prolong_kontur_access(force=True)

        self.assertTrue(result["success"])
        self.assertTrue(result["performed"])
        flow_mock.assert_called_once_with()
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        self.assertIn("last_attempt_ts", payload)
        self.assertIn("last_success_ts", payload)
        self.assertEqual(payload.get("last_error"), "")

    def test_worker_starts_only_once_per_process(self):
        with (
            mock.patch.object(cookies, "_prolongation_enabled", return_value=True),
            mock.patch.object(cookies.threading, "Thread", side_effect=lambda *args, **kwargs: _FakeThread(*args, **kwargs)),
        ):
            first_result = cookies.ensure_kontur_access_prolongation_worker_started()
            first_thread = cookies._PROLONGATION_THREAD
            second_result = cookies.ensure_kontur_access_prolongation_worker_started()

        self.assertTrue(first_result)
        self.assertTrue(second_result)
        self.assertIsNotNone(first_thread)
        self.assertTrue(first_thread.started)
        self.assertIs(cookies._PROLONGATION_THREAD, first_thread)

    def test_detects_driver_version_mismatch_error(self):
        exc = RuntimeError(
            "Message: session not created: This version of ChromeDriver only supports Chrome version 144. "
            "Current browser version is 146.0.7680.2170"
        )
        self.assertTrue(cookies._is_driver_version_mismatch_error(exc))

    def test_create_chrome_driver_falls_back_to_selenium_manager_on_version_mismatch(self):
        driver_path = Path(self.temp_dir.name) / "yandexdriver.exe"
        driver_path.write_text("stub", encoding="utf-8")
        options = object()
        driver_instance = object()
        call_log = []

        def fake_chrome(*args, **kwargs):
            call_log.append(kwargs)
            if "service" in kwargs:
                raise RuntimeError(
                    "Message: session not created: This version of ChromeDriver only supports Chrome version 144. "
                    "Current browser version is 146.0.7680.2170"
                )
            return driver_instance

        webdriver_module = mock.Mock()
        webdriver_module.Chrome = mock.Mock(side_effect=fake_chrome)
        service_cls = mock.Mock(side_effect=lambda path: {"path": path})

        result = cookies._create_chrome_driver(
            webdriver_module,
            service_cls,
            options,
            driver_path=driver_path,
            purpose_label="Получение cookies",
        )

        self.assertIs(result, driver_instance)
        self.assertEqual(len(call_log), 2)
        self.assertIn("service", call_log[0])
        self.assertNotIn("service", call_log[1])
        self.assertIs(call_log[1]["options"], options)


if __name__ == "__main__":
    unittest.main()

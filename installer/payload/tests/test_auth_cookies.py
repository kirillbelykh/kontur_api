"""Regression tests for Kontur cookie orchestration."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import backend.auth.service as service
import backend.auth.store as store
from backend.auth.constants import REQUIRED_COOKIE_FIELDS


def _valid_cookie_dict() -> dict[str, str]:
    return {field: f"value-{field}" for field in REQUIRED_COOKIE_FIELDS}


class AuthServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.cookies_file = Path(self.temp_dir.name) / "kontur_cookies.json"
        store.clear_memoized_cookies()
        store.set_cookie_refresh_in_progress(False)
        store.cookie_refresh_event().set()

    def tearDown(self):
        store.clear_memoized_cookies()
        store.set_cookie_refresh_in_progress(False)
        store.cookie_refresh_event().set()
        self.temp_dir.cleanup()

    def _write_cookie_file(self, *, age_seconds: float = 10.0) -> dict[str, str]:
        cookies = _valid_cookie_dict()
        payload = {
            "timestamp": 1_800_000_000.0 - age_seconds,
            "cookies": cookies,
        }
        self.cookies_file.write_text(json.dumps(payload), encoding="utf-8")
        return cookies

    def test_get_valid_cookies_rejects_fresh_file_without_api_validation(self):
        """Fresh local TTL must not skip Selenium when Kontur rejects cookies."""
        cookies = self._write_cookie_file(age_seconds=30.0)
        selenium_cookies = _valid_cookie_dict()
        selenium_cookies["token"] = "fresh-from-browser"

        with (
            mock.patch.object(store, "COOKIES_FILE", self.cookies_file),
            mock.patch.object(store, "LEGACY_COOKIES_FILE", self.cookies_file),
            mock.patch("backend.auth.service.validate_kontur_session", side_effect=[False, True]) as validate_mock,
            mock.patch("backend.auth.service.load_cookies_from_yandex_profile", return_value=None),
            mock.patch("backend.auth.service.get_cookies", return_value=selenium_cookies) as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
            mock.patch("time.time", return_value=1_800_000_000.0),
        ):
            result = service.get_valid_cookies(force_refresh=False)

        self.assertEqual(result, selenium_cookies)
        selenium_mock.assert_called_once_with()
        self.assertGreaterEqual(validate_mock.call_count, 2)
        self.assertEqual(validate_mock.call_args_list[0].args[0], cookies)

    def test_get_valid_cookies_reuses_live_file_cookies(self):
        cookies = self._write_cookie_file(age_seconds=30.0)

        with (
            mock.patch.object(store, "COOKIES_FILE", self.cookies_file),
            mock.patch.object(store, "LEGACY_COOKIES_FILE", self.cookies_file),
            mock.patch("backend.auth.service.validate_kontur_session", return_value=True) as validate_mock,
            mock.patch("backend.auth.service.get_cookies") as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
            mock.patch("time.time", return_value=1_800_000_000.0),
        ):
            result = service.get_valid_cookies(force_refresh=False)

        self.assertEqual(result, cookies)
        selenium_mock.assert_not_called()
        validate_mock.assert_called()

    def test_force_refresh_prefers_profile_then_selenium(self):
        profile_cookies = _valid_cookie_dict()
        profile_cookies["token"] = "from-profile"

        with (
            mock.patch("backend.auth.service.validate_kontur_session", return_value=True),
            mock.patch("backend.auth.service.load_cookies_from_yandex_profile", return_value=profile_cookies),
            mock.patch("backend.auth.service.get_cookies") as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
        ):
            result = service.get_valid_cookies(force_refresh=True)

        self.assertEqual(result, profile_cookies)
        selenium_mock.assert_not_called()


class KonturCheckTests(unittest.TestCase):
    def test_validate_kontur_session_rejects_login_redirect(self):
        from backend.auth.kontur_check import validate_kontur_session

        cookies = _valid_cookie_dict()
        response = mock.Mock()
        response.status_code = 200
        response.url = "https://login.kontur.ru/Login"
        response.headers = {"Content-Type": "application/json"}
        response.json.return_value = {"id": "user-1"}

        with mock.patch("backend.auth.kontur_check.requests.get", return_value=response):
            self.assertFalse(validate_kontur_session(cookies))

    def test_validate_kontur_session_accepts_user_payload(self):
        from backend.auth.kontur_check import validate_kontur_session

        cookies = _valid_cookie_dict()
        response = mock.Mock()
        response.status_code = 200
        response.url = "https://mk.kontur.ru/api/v1/user"
        response.headers = {"Content-Type": "application/json"}
        response.json.return_value = {"id": "user-1", "email": "a@b.c"}

        with mock.patch("backend.auth.kontur_check.requests.get", return_value=response):
            self.assertTrue(validate_kontur_session(cookies))


class BrowserOptionsTests(unittest.TestCase):
    def test_background_mode_keeps_offscreen_window(self):
        from backend.auth.browser import build_browser_options

        fake_options = mock.Mock()
        fake_options.arguments = []

        def add_argument(value):
            fake_options.arguments.append(value)

        fake_options.add_argument.side_effect = add_argument
        fake_options.add_experimental_option = mock.Mock()

        with mock.patch("selenium.webdriver.chrome.options.Options", return_value=fake_options):
            build_browser_options(
                browser_path=Path("browser.exe"),
                profile_user_data_dir=Path("profile"),
                profile_directory="Default",
                headless=False,
            )

        self.assertTrue(any("-32000" in arg for arg in fake_options.arguments))
        self.assertFalse(any(arg.startswith("--headless") for arg in fake_options.arguments))

    def test_true_headless_mode_uses_chrome_headless(self):
        from backend.auth.browser import build_browser_options

        fake_options = mock.Mock()
        fake_options.arguments = []

        def add_argument(value):
            fake_options.arguments.append(value)

        fake_options.add_argument.side_effect = add_argument
        fake_options.add_experimental_option = mock.Mock()

        with mock.patch("selenium.webdriver.chrome.options.Options", return_value=fake_options):
            build_browser_options(
                browser_path=Path("browser.exe"),
                profile_user_data_dir=Path("profile"),
                profile_directory="Default",
                headless=True,
            )

        self.assertTrue(any(arg == "--headless=new" for arg in fake_options.arguments))
        # Off-screen flags stay even in opt-in true headless (d647455).
        self.assertTrue(any("-32000" in arg for arg in fake_options.arguments))


class EnsureSessionBridgeTests(unittest.TestCase):
    def test_ensure_session_uses_get_valid_cookies_orchestration(self):
        import backend.app.api_bridge as api_bridge

        bridge = api_bridge.ApiBridge()
        runtime = api_bridge._get_runtime()
        runtime.session = None
        runtime.session_created_at = 0.0
        cookies = _valid_cookie_dict()
        fake_session = object()

        with (
            mock.patch.object(api_bridge, "get_valid_cookies", return_value=cookies) as get_valid_mock,
            mock.patch.object(api_bridge.cookies_module, "validate_kontur_session", return_value=True),
            mock.patch.object(api_bridge, "make_session_with_cookies", return_value=fake_session),
        ):
            session = bridge._ensure_session(force_refresh=True, force_browser_refresh=True)

        self.assertIs(session, fake_session)
        get_valid_mock.assert_called_once_with(force_refresh=True)


if __name__ == "__main__":
    unittest.main()

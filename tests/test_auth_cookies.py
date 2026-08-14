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
        service._LAST_SELENIUM_OK_AT = 0.0
        service._LAST_SELENIUM_TRY_AT = 0.0
        self.lan_patcher = mock.patch("backend.auth.service.fetch_cookies_from_lan", return_value=None)
        self.lan_patcher.start()

    def tearDown(self):
        self.lan_patcher.stop()
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
            mock.patch.object(store, "COOKIES_FILE", self.cookies_file),
            mock.patch.object(store, "LEGACY_COOKIES_FILE", self.cookies_file),
            mock.patch("backend.auth.service.validate_kontur_session", return_value=True),
            mock.patch("backend.auth.service.load_cookies_from_yandex_profile", return_value=profile_cookies),
            mock.patch("backend.auth.service.get_cookies") as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
        ):
            # No file cookies → fall through to yandex-profile.
            result = service.get_valid_cookies(force_refresh=True)

        self.assertEqual(result, profile_cookies)
        selenium_mock.assert_not_called()

    def test_force_refresh_prefers_lan_before_profile(self):
        lan_cookies = _valid_cookie_dict()
        lan_cookies["token"] = "from-lan"

        with (
            mock.patch.object(store, "COOKIES_FILE", self.cookies_file),
            mock.patch.object(store, "LEGACY_COOKIES_FILE", self.cookies_file),
            mock.patch("backend.auth.service.validate_kontur_session", return_value=True),
            mock.patch("backend.auth.service.fetch_cookies_from_lan", return_value=lan_cookies),
            mock.patch("backend.auth.service.load_cookies_from_yandex_profile") as profile_mock,
            mock.patch("backend.auth.service.get_cookies") as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
        ):
            result = service.get_valid_cookies(force_refresh=True)

        self.assertEqual(result, lan_cookies)
        profile_mock.assert_not_called()
        selenium_mock.assert_not_called()

    def test_failed_selenium_is_not_retried_within_debounce(self):
        self._write_cookie_file(age_seconds=30.0)
        with (
            mock.patch.object(store, "COOKIES_FILE", self.cookies_file),
            mock.patch.object(store, "LEGACY_COOKIES_FILE", self.cookies_file),
            mock.patch("backend.auth.service.validate_kontur_session", return_value=False),
            mock.patch("backend.auth.service.load_cookies_from_yandex_profile", return_value=None),
            mock.patch("backend.auth.service.get_cookies", return_value=None) as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
        ):
            first = service.get_valid_cookies(force_refresh=True)
            second = service.get_valid_cookies(force_refresh=True)

        self.assertIsNone(first)
        self.assertIsNone(second)
        selenium_mock.assert_called_once_with()

    def test_missing_cookie_file_still_launches_selenium_after_debounce(self):
        """No saved cookies → debounce must not skip the browser."""
        with (
            mock.patch.object(store, "COOKIES_FILE", self.cookies_file),
            mock.patch.object(store, "LEGACY_COOKIES_FILE", self.cookies_file),
            mock.patch("backend.auth.service.validate_kontur_session", return_value=False),
            mock.patch("backend.auth.service.load_cookies_from_yandex_profile", return_value=None),
            mock.patch("backend.auth.service.get_cookies", return_value=None) as selenium_mock,
            mock.patch("backend.auth.service.save_cookies_to_file", return_value=True),
        ):
            first = service.get_valid_cookies(force_refresh=True)
            second = service.get_valid_cookies(force_refresh=True)

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(selenium_mock.call_count, 2)


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
        self.assertTrue(any("AutomationControlled" in arg for arg in fake_options.arguments))
        self.assertFalse(any(arg.startswith("--headless") for arg in fake_options.arguments))
        fake_options.add_experimental_option.assert_any_call(
            "excludeSwitches", ["enable-automation"]
        )

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
        self.assertTrue(any("-32000" in arg for arg in fake_options.arguments))


class BrowserSessionErrorTests(unittest.TestCase):
    def test_detects_driver_version_mismatch(self):
        from backend.auth.browser import is_driver_version_mismatch

        exc = RuntimeError(
            "session not created: This version of ChromeDriver only supports Chrome version 146"
        )
        self.assertTrue(is_driver_version_mismatch(exc))

    def test_profile_lock_is_not_a_version_mismatch(self):
        from backend.auth.browser import is_driver_version_mismatch

        exc = RuntimeError(
            "session not created: DevToolsActivePort file doesn't exist; Chrome instance exited"
        )
        self.assertFalse(is_driver_version_mismatch(exc))

    def test_get_cookies_uses_persistent_program_profile(self):
        from backend.auth import browser as browser_mod
        from backend.auth.constants import SELENIUM_PROFILE_DIRECTORY, SELENIUM_USER_DATA_DIR

        captured: dict[str, list[str]] = {}

        def fake_chrome(*, service, options):
            captured["args"] = list(options.arguments)
            raise RuntimeError("stop")

        with (
            mock.patch("selenium.webdriver.Chrome", side_effect=fake_chrome),
            mock.patch.object(Path, "exists", return_value=True),
        ):
            browser_mod.get_cookies(
                driver_path=Path("driver/yandexdriver.exe"),
                browser_path=Path("browser.exe"),
            )

        args = captured["args"]
        self.assertIn(f"--user-data-dir={SELENIUM_USER_DATA_DIR.resolve()}", args)
        self.assertIn(f"--profile-directory={SELENIUM_PROFILE_DIRECTORY}", args)
        self.assertIn("--remote-debugging-port=0", args)
        self.assertFalse(any("incognito" in a.lower() for a in args))

    def test_get_cookies_launches_chrome_only_once(self):
        """Even max_retries>1 must not open a second Yandex window."""
        from backend.auth import browser as browser_mod

        lock_error = RuntimeError(
            "session not created: Chrome failed to start: crashed. "
            "(session not created: DevToolsActivePort file doesn't exist)"
        )

        with (
            mock.patch.object(browser_mod, "terminate_yandex_browser_processes", return_value=2) as term_mock,
            mock.patch("selenium.webdriver.Chrome", side_effect=lock_error) as chrome_mock,
            mock.patch.object(Path, "exists", return_value=True),
            mock.patch("tempfile.mkdtemp") as mkdtemp_mock,
        ):
            result = browser_mod.get_cookies(
                driver_path=Path("driver/yandexdriver.exe"),
                browser_path=Path("browser.exe"),
                profile_user_data_dir=Path("User Data"),
                profile_directory="Vinsent O`neal",
                max_retries=3,
            )

        self.assertIsNone(result)
        term_mock.assert_not_called()
        mkdtemp_mock.assert_not_called()
        self.assertEqual(chrome_mock.call_count, 1)

    def test_failed_launch_does_not_kill_browser_processes(self):
        """Failed launch must not TerminateProcess leftover browser.exe."""
        from backend.auth import browser as browser_mod

        generic_error = RuntimeError("some random selenium failure")

        with (
            mock.patch.object(browser_mod, "_terminate_pids", return_value=1) as term_mock,
            mock.patch("selenium.webdriver.Chrome", side_effect=generic_error),
            mock.patch.object(Path, "exists", return_value=True),
            mock.patch("tempfile.mkdtemp") as mkdtemp_mock,
        ):
            result = browser_mod.get_cookies(
                driver_path=Path("driver/yandexdriver.exe"),
                browser_path=Path("browser.exe"),
                profile_user_data_dir=Path("User Data"),
                profile_directory="Vinsent O`neal",
                max_retries=2,
            )

        self.assertIsNone(result)
        term_mock.assert_not_called()
        mkdtemp_mock.assert_not_called()

    def test_get_cookies_does_not_repair_driver_with_second_launch(self):
        from backend.auth import browser as browser_mod

        version_error = RuntimeError(
            "session not created: This version of ChromeDriver only supports Chrome version 146"
        )

        with (
            mock.patch.object(browser_mod, "ensure_yandex_driver_updated", return_value=True) as ensure_mock,
            mock.patch("selenium.webdriver.Chrome", side_effect=version_error) as chrome_mock,
            mock.patch.object(Path, "exists", return_value=True),
            mock.patch("tempfile.mkdtemp") as mkdtemp_mock,
        ):
            result = browser_mod.get_cookies(
                driver_path=Path("driver/yandexdriver.exe"),
                browser_path=Path("browser.exe"),
                profile_user_data_dir=Path("User Data"),
                profile_directory="Default",
                max_retries=2,
            )

        self.assertIsNone(result)
        ensure_mock.assert_not_called()
        mkdtemp_mock.assert_not_called()
        self.assertEqual(chrome_mock.call_count, 1)

    def test_profile_xpath_is_first_step(self):
        from backend.auth import browser as browser_mod

        self.assertEqual(
            browser_mod.STEP1_NAME_XPATH,
            '//*[@id="root"]/div/div/div[1]/div[2]/div/div/div/div/div[2]'
            "/div/div/div/div/div/div/div[1]/div/div/div/div[1]/div/div",
        )

    def test_warehouse_xpaths_prefer_lakhta_card(self):
        from backend.auth import browser as browser_mod

        self.assertEqual(
            browser_mod.STEP2_WAREHOUSE_XPATH,
            '//*[@id="root"]/div/div/div[2]/div/div/div[1]/div[3]/ul/li/div[2]',
        )

    def test_click_warehouse_card_uses_warehouse_xpaths(self):
        from backend.auth import browser as browser_mod

        driver = mock.Mock()
        with mock.patch.object(browser_mod, "click_first_available", return_value=True) as click_mock:
            ok = browser_mod._click_warehouse_card(driver)

        self.assertTrue(ok)
        click_mock.assert_called_once()
        self.assertEqual(click_mock.call_args.args[1], [browser_mod.STEP2_WAREHOUSE_XPATH])

    def test_complete_kontur_certificate_login_clicks_cert_then_confirm(self):
        from backend.auth import browser as browser_mod

        driver = mock.Mock()
        with mock.patch.object(browser_mod, "_click_profile_card", return_value=True) as click_mock:
            ok = browser_mod.complete_kontur_certificate_login(driver)

        self.assertTrue(ok)
        click_mock.assert_called_once()

    def test_background_options_keep_real_profile_flags(self):
        from backend.auth.browser import build_browser_options

        with mock.patch("selenium.webdriver.chrome.options.Options") as options_cls:
            options = options_cls.return_value
            build_browser_options(
                browser_path=Path("browser.exe"),
                profile_user_data_dir=Path("User Data"),
                profile_directory="Vinsent O`neal",
                headless=False,
            )

        args = [call.args[0] for call in options.add_argument.call_args_list]
        self.assertTrue(any(a.startswith("--user-data-dir=") for a in args))
        self.assertIn("--profile-directory=Vinsent O`neal", args)
        self.assertIn("--window-position=-32000,-32000", args)
        self.assertFalse(any("headless" in a for a in args))
        self.assertFalse(any("incognito" in a.lower() for a in args))


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
        get_valid_mock.assert_called_once_with(force_refresh=True, force_browser=True)


if __name__ == "__main__":
    unittest.main()

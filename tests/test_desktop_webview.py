"""Desktop shell must persist WebView localStorage between launches."""

from __future__ import annotations

import unittest
from pathlib import Path

from backend.app.desktop import REPO_ROOT, webview_persistence_kwargs


class WebviewPersistenceTests(unittest.TestCase):
    def test_private_mode_is_off_and_storage_lives_in_runtime(self) -> None:
        kwargs = webview_persistence_kwargs()
        self.assertFalse(kwargs["private_mode"])
        storage = Path(str(kwargs["storage_path"]))
        self.assertEqual(storage, REPO_ROOT / "runtime" / "webview")
        self.assertTrue(storage.is_dir())

    def test_colorref_from_hex_is_bgr(self) -> None:
        from backend.app.desktop import colorref_from_hex

        self.assertEqual(colorref_from_hex("#18181b"), 0x001B1818)
        self.assertEqual(colorref_from_hex("121212"), 0x00121212)
        self.assertEqual(colorref_from_hex("#0d1723"), 0x0023170D)
        self.assertEqual(colorref_from_hex("#abc"), 0x00CCBBAA)
        self.assertIsNone(colorref_from_hex("not-a-color"))
        self.assertIsNone(colorref_from_hex(""))

    def test_apply_window_chrome_paints_dark_caption_and_resets_light(self) -> None:
        from unittest import mock

        from backend.app import desktop as desktop_app

        calls: list[tuple[int, int, int]] = []

        def fake_set(_hwnd: int, attribute: int, value: int) -> None:
            calls.append((attribute, value, _hwnd))

        with (
            mock.patch.object(desktop_app, "_find_app_hwnd", return_value=42),
            mock.patch.object(desktop_app, "_set_dwm_attribute", side_effect=fake_set),
        ):
            self.assertTrue(
                desktop_app.apply_window_chrome(dark=True, caption="#18181b", text="#dededf")
            )
            self.assertTrue(desktop_app.apply_window_chrome(dark=False, caption="#18181b", text="#dededf"))

        dark_caption = [value for attribute, value, _hwnd in calls if attribute == desktop_app.DWMWA_CAPTION_COLOR]
        self.assertEqual(dark_caption[0], 0x001B1818)
        self.assertEqual(dark_caption[1], desktop_app.DWMWA_COLOR_DEFAULT)


if __name__ == "__main__":
    unittest.main()

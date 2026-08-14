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


if __name__ == "__main__":
    unittest.main()

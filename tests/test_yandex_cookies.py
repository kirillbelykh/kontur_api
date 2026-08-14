"""Read cookies from a Yandex profile sqlite without opening the browser."""

from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from backend.auth.constants import REQUIRED_COOKIE_FIELDS
from backend.auth.yandex_cookies import _read_file_shared, load_cookies_from_yandex_profile


class SharedFileReadTests(unittest.TestCase):
    def test_read_file_shared_returns_bytes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.bin"
            path.write_bytes(b"kontur-cookies")
            self.assertEqual(_read_file_shared(path), b"kontur-cookies")


class YandexProfileCookiesTests(unittest.TestCase):
    def test_reads_plain_kontur_cookies_from_network_db(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            user_data = Path(temp_dir)
            db_path = user_data / "Default" / "Network" / "Cookies"
            db_path.parent.mkdir(parents=True)
            connection = sqlite3.connect(db_path)
            try:
                connection.execute(
                    "CREATE TABLE cookies (host_key TEXT, name TEXT, value TEXT, encrypted_value BLOB)"
                )
                for field in REQUIRED_COOKIE_FIELDS:
                    connection.execute(
                        "INSERT INTO cookies VALUES (?, ?, ?, ?)",
                        (".kontur.ru", field, f"value-{field}", b""),
                    )
                connection.commit()
            finally:
                connection.close()

            cookies = load_cookies_from_yandex_profile(user_data, "Default")

        self.assertEqual(cookies, {field: f"value-{field}" for field in REQUIRED_COOKIE_FIELDS})

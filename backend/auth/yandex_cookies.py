"""Read Kontur cookies from the local Yandex Browser profile database."""

from __future__ import annotations

import base64
import ctypes
import json
import os
import sqlite3
import tempfile
from ctypes import wintypes
from pathlib import Path
from typing import Dict, Optional

from backend.services.logger import logger

from backend.auth.constants import SELENIUM_PROFILE_DIRECTORY, SELENIUM_USER_DATA_DIR
from backend.auth.store import validate_cookies


def _load_yandex_cookie_key(user_data_dir: Path) -> Optional[bytes]:
    try:
        local_state = json.loads((user_data_dir / "Local State").read_text(encoding="utf-8"))
        encrypted_key = base64.b64decode(local_state["os_crypt"]["encrypted_key"])
        if encrypted_key.startswith(b"DPAPI"):
            encrypted_key = encrypted_key[5:]
        import win32crypt

        return win32crypt.CryptUnprotectData(encrypted_key, None, None, None, 0)[1]
    except Exception as exc:
        logger.debug("Could not read the Yandex Browser encryption key: %s", exc)
        return None


def _decrypt_yandex_cookie(value: bytes, key: Optional[bytes]) -> Optional[str]:
    if not value:
        return ""
    try:
        if value.startswith((b"v10", b"v11")) and key:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM

            nonce = value[3:15]
            return AESGCM(key).decrypt(nonce, value[15:], None).decode("utf-8")
        import win32crypt

        return win32crypt.CryptUnprotectData(value, None, None, None, 0)[1].decode("utf-8")
    except Exception as exc:
        logger.debug("Could not decrypt a Yandex Browser cookie: %s", exc)
        return None


def _read_file_shared(path: Path) -> bytes:
    """Read a file while Yandex still has it open (FILE_SHARE_READ|WRITE|DELETE)."""
    if os.name != "nt":
        return path.read_bytes()

    GENERIC_READ = 0x80000000
    FILE_SHARE_READ = 0x00000001
    FILE_SHARE_WRITE = 0x00000002
    FILE_SHARE_DELETE = 0x00000004
    OPEN_EXISTING = 3
    FILE_ATTRIBUTE_NORMAL = 0x80
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    CreateFileW = kernel32.CreateFileW
    CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    CreateFileW.restype = wintypes.HANDLE

    handle = CreateFileW(
        str(path),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        None,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL,
        None,
    )
    if handle == INVALID_HANDLE_VALUE or not handle:
        raise ctypes.WinError(ctypes.get_last_error())

    try:
        size = ctypes.c_longlong(0)
        if not kernel32.GetFileSizeEx(handle, ctypes.byref(size)):
            raise ctypes.WinError(ctypes.get_last_error())
        if size.value <= 0:
            return b""
        buf = ctypes.create_string_buffer(size.value)
        read = wintypes.DWORD(0)
        if not kernel32.ReadFile(handle, buf, size.value, ctypes.byref(read), None):
            raise ctypes.WinError(ctypes.get_last_error())
        return buf.raw[: read.value]
    finally:
        kernel32.CloseHandle(handle)


def _copy_sqlite_tree(src: Path, dest: Path) -> None:
    dest.write_bytes(_read_file_shared(src))
    for extra in (src.parent / f"{src.name}-wal", src.parent / f"{src.name}-journal"):
        if not extra.exists():
            continue
        try:
            dest.with_name(dest.name + extra.name[len(src.name) :]).write_bytes(
                _read_file_shared(extra)
            )
        except OSError:
            pass


def _cookie_db_candidates(user_data_dir: Path, profile_directory: str) -> list[Path]:
    profile_root = user_data_dir / profile_directory
    return [
        profile_root / "Network" / "Cookies",
        profile_root / "Cookies",
    ]


def load_cookies_from_yandex_profile(
    user_data_dir: Optional[Path] = SELENIUM_USER_DATA_DIR,
    profile_directory: str = SELENIUM_PROFILE_DIRECTORY,
) -> Optional[Dict[str, str]]:
    """Read Kontur cookies from the app's persistent Yandex profile."""
    if not user_data_dir:
        return None
    user_data_dir = Path(user_data_dir)
    cookies_db = next((path for path in _cookie_db_candidates(user_data_dir, profile_directory) if path.exists()), None)
    if cookies_db is None:
        return None

    fd, temp_name = tempfile.mkstemp(prefix="kontur_cookies_", suffix=".sqlite")
    os.close(fd)
    temporary_db = Path(temp_name)
    try:
        copied = False
        try:
            _copy_sqlite_tree(cookies_db, temporary_db)
            copied = True
        except OSError as exc:
            logger.debug("Shared copy of Yandex Cookies failed: %s", exc)

        if copied:
            connection = sqlite3.connect(temporary_db)
        else:
            uri = cookies_db.resolve().as_uri() + "?mode=ro&immutable=1&nolock=1"
            connection = sqlite3.connect(uri, uri=True)
        try:
            key = _load_yandex_cookie_key(user_data_dir)
            rows = connection.execute(
                "SELECT name, value, encrypted_value FROM cookies WHERE host_key LIKE ?",
                ("%kontur.ru",),
            ).fetchall()
        finally:
            connection.close()
        cookies: Dict[str, str] = {}
        for name, plain_value, encrypted_value in rows:
            value = str(plain_value or "") or _decrypt_yandex_cookie(encrypted_value or b"", key)
            if name and value is not None:
                cookies[str(name)] = value
        is_valid, missing = validate_cookies(cookies)
        if not is_valid:
            logger.info("Cookies в профиле Яндекса неполные: %s", missing)
            return None
        logger.info("Прочитали cookies из профиля Яндекса (%s ключей)", len(cookies))
        return cookies
    except Exception as exc:
        logger.warning("Не удалось прочитать cookies из профиля Яндекса: %s", exc)
        return None
    finally:
        for leftover in (
            temporary_db,
            Path(str(temporary_db) + "-wal"),
            Path(str(temporary_db) + "-journal"),
        ):
            try:
                leftover.unlink(missing_ok=True)
            except PermissionError:
                logger.debug("Temporary browser cookie copy is still locked: %s", leftover)

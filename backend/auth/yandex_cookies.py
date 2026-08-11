"""Read Kontur cookies from the local Yandex Browser profile database."""

from __future__ import annotations

import base64
import json
import shutil
import sqlite3
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional

from backend.services.logger import logger

from backend.auth.constants import PROFILE_DIRECTORY, PROFILE_USER_DATA_DIR
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


def load_cookies_from_yandex_profile(
    user_data_dir: Optional[Path] = PROFILE_USER_DATA_DIR,
    profile_directory: str = PROFILE_DIRECTORY,
) -> Optional[Dict[str, str]]:
    """Read Kontur cookies from the user's normal Yandex Browser profile."""
    if not user_data_dir:
        return None
    user_data_dir = Path(user_data_dir)
    cookies_db = user_data_dir / profile_directory / "Network" / "Cookies"
    if not cookies_db.exists():
        return None

    temporary_db = Path(tempfile.mkstemp(prefix="kontur_cookies_", suffix=".sqlite")[1])
    try:
        last_error: Optional[Exception] = None
        copied = False
        # Browser often locks Cookies exclusively; retry briefly in case the
        # lock is transient (startup / flush). Shared-read open is preferred.
        for attempt in range(1, 4):
            try:
                try:
                    raw = cookies_db.read_bytes()
                    temporary_db.write_bytes(raw)
                except OSError:
                    shutil.copy2(cookies_db, temporary_db)
                copied = True
                break
            except OSError as exc:
                last_error = exc
                logger.debug(
                    "Yandex Cookies DB locked (attempt %s/3): %s",
                    attempt,
                    exc,
                )
                time.sleep(0.35 * attempt)
        if not copied:
            # Last resort: open sqlite in immutable URI mode (may still fail).
            try:
                uri = cookies_db.resolve().as_uri() + "?mode=ro&immutable=1"
                connection = sqlite3.connect(uri, uri=True)
            except Exception as exc:
                raise last_error or exc
        else:
            connection = sqlite3.connect(temporary_db)
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
        is_valid, _ = validate_cookies(cookies)
        return cookies if is_valid else None
    except Exception as exc:
        winerr = getattr(exc, "winerror", None)
        if winerr == 32 or isinstance(exc, PermissionError):
            logger.warning(
                "Could not read cookies from Yandex Browser: profile DB is locked "
                "(close the browser or rely on saved file cookies). %s",
                exc,
            )
        else:
            logger.warning("Could not read cookies from Yandex Browser: %s", exc)
        return None
    finally:
        try:
            temporary_db.unlink(missing_ok=True)
        except PermissionError:
            logger.debug("Temporary browser cookie copy is still locked: %s", temporary_db)

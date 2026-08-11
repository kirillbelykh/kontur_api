"""Live Kontur API validation for saved cookies."""

from __future__ import annotations

from typing import Dict, Optional
from urllib.parse import urlparse

import requests

from logger import logger

from auth.constants import BASE_URL, USER_INFO_URL
from auth.store import validate_cookies

_LOGIN_HINTS = (
    "login",
    "identity",
    "passport",
    "auth.kontur",
    "accounts.kontur",
)


def _looks_like_login_url(url: str) -> bool:
    normalized = str(url or "").strip().lower()
    if not normalized:
        return True
    return any(hint in normalized for hint in _LOGIN_HINTS)


def validate_kontur_session(cookies: Optional[Dict[str, str]]) -> bool:
    """Check saved cookies against Kontur before trusting a local cache."""
    is_valid, _ = validate_cookies(cookies or {})
    if not is_valid:
        return False

    try:
        response = requests.get(
            USER_INFO_URL,
            cookies=cookies,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=15,
            allow_redirects=True,
        )
    except requests.RequestException as exc:
        logger.warning("Could not validate saved Kontur cookies: %s", exc)
        return False

    if response.status_code != 200:
        logger.info("Kontur cookie validation failed: HTTP %s", response.status_code)
        return False

    final_url = str(response.url or "")
    parsed = urlparse(final_url)
    expected_host = urlparse(BASE_URL).hostname or "mk.kontur.ru"
    if parsed.hostname != expected_host:
        logger.info("Kontur cookie validation redirected away from API host: %s", final_url)
        return False
    if _looks_like_login_url(final_url):
        logger.info("Kontur cookie validation hit login page: %s", final_url)
        return False

    content_type = str(response.headers.get("Content-Type") or "").lower()
    if "json" not in content_type:
        logger.info("Kontur cookie validation returned non-JSON payload (%s)", content_type or "unknown")
        return False

    try:
        payload = response.json()
    except ValueError:
        logger.info("Kontur cookie validation returned invalid JSON")
        return False

    if not isinstance(payload, dict) or not payload:
        logger.info("Kontur cookie validation returned empty user payload")
        return False

    if payload.get("error") or payload.get("statusCode") in {401, 403}:
        logger.info("Kontur cookie validation returned auth error payload")
        return False

    return True

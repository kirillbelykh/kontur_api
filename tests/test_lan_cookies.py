"""LAN cookie share: pull from a peer, skip when disabled."""

from __future__ import annotations

import unittest
from unittest import mock

from backend.auth import lan_cookies
from backend.auth.constants import REQUIRED_COOKIE_FIELDS


def _valid_cookies() -> dict[str, str]:
    return {field: f"value-{field}" for field in REQUIRED_COOKIE_FIELDS}


class FetchLanCookiesTests(unittest.TestCase):
    def test_disabled_share_does_not_touch_network(self):
        with (
            mock.patch.dict("os.environ", {"KONTUR_COOKIE_SHARE": "0"}, clear=False),
            mock.patch("backend.auth.lan_cookies.requests.get") as get_mock,
        ):
            self.assertIsNone(lan_cookies.fetch_cookies_from_lan())
        get_mock.assert_not_called()

    def test_pulls_cookies_from_first_ok_peer(self):
        cookies = _valid_cookies()
        response = mock.Mock()
        response.status_code = 200
        response.json.return_value = {"ok": True, "cookies": cookies}

        with (
            mock.patch.dict("os.environ", {"CHZ_BRIDGE_TOKEN": "secret", "KONTUR_COOKIE_SHARE": "1"}, clear=False),
            mock.patch.object(lan_cookies, "_candidate_hosts", return_value=["10.0.0.5"]),
            mock.patch.object(lan_cookies, "_remember_peer"),
            mock.patch("backend.auth.lan_cookies.requests.get", return_value=response) as get_mock,
        ):
            result = lan_cookies.fetch_cookies_from_lan()

        self.assertEqual(result, cookies)
        get_mock.assert_called_once()
        self.assertEqual(get_mock.call_args.kwargs["headers"]["X-CHZ-Token"], "secret")

"""Pull Kontur cookies from other PCs on the same LAN. No IP list required.

Probes the local /24 on the existing CHZ bridge port (8791) with the shared
CHZ token. A peer that already has fresh cookies answers; Selenium stays last.
"""

from __future__ import annotations

import ipaddress
import os
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

import requests

from backend.auth.constants import AUTH_RUNTIME_DIR
from backend.auth.store import validate_cookies
from backend.services.logger import logger

_PEER_CACHE = AUTH_RUNTIME_DIR / "lan_peer.txt"
_DEFAULT_PORT = 8791
_WORKERS = 32
_TIMEOUT = 0.45


def _share_enabled() -> bool:
    raw = str(os.getenv("KONTUR_COOKIE_SHARE", "1")).strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _share_token() -> str:
    return str(os.getenv("CHZ_BRIDGE_TOKEN") or "").strip()


def _share_port() -> int:
    raw = str(os.getenv("CHZ_BRIDGE_PORT") or _DEFAULT_PORT).strip()
    try:
        port = int(raw)
    except ValueError:
        return _DEFAULT_PORT
    return port if 1 <= port <= 65535 else _DEFAULT_PORT


def _local_ipv4s() -> set[str]:
    found: set[str] = set()
    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            found.add(info[4][0])
    except OSError:
        pass
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.settimeout(0.2)
        probe.connect(("8.8.8.8", 80))
        found.add(probe.getsockname()[0])
        probe.close()
    except OSError:
        pass
    found.discard("127.0.0.1")
    found.discard("0.0.0.0")
    return found


def _cached_peer() -> Optional[str]:
    try:
        host = _PEER_CACHE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return host or None


def _remember_peer(host: str) -> None:
    try:
        AUTH_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        _PEER_CACHE.write_text(host, encoding="utf-8")
    except OSError:
        pass


def _candidate_hosts() -> list[str]:
    local = _local_ipv4s()
    ordered: list[str] = []
    seen: set[str] = set()

    def add(host: str) -> None:
        if host in seen or host in local:
            return
        seen.add(host)
        ordered.append(host)

    cached = _cached_peer()
    if cached:
        add(cached)

    for ip in sorted(local):
        try:
            network = ipaddress.ip_network(f"{ip}/24", strict=False)
        except ValueError:
            continue
        if not network.is_private:
            continue
        for host in network.hosts():
            add(str(host))
    return ordered


def _pull_from_host(host: str, *, token: str, port: int) -> Optional[Dict[str, str]]:
    url = f"http://{host}:{port}/api/auth/cookies"
    try:
        response = requests.get(
            url,
            headers={"X-CHZ-Token": token, "Accept": "application/json"},
            timeout=_TIMEOUT,
        )
    except requests.RequestException:
        return None
    if response.status_code != 200:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    if not isinstance(payload, dict) or not payload.get("ok"):
        return None
    cookies = payload.get("cookies")
    if not isinstance(cookies, dict):
        return None
    cookies_str = {str(key): str(value) for key, value in cookies.items() if key}
    ok, _missing = validate_cookies(cookies_str)
    if not ok:
        return None
    _remember_peer(host)
    return cookies_str


def fetch_cookies_from_lan() -> Optional[Dict[str, str]]:
    """Ask other Kontur PCs on this subnet for a fresh cookie file."""
    if not _share_enabled():
        return None
    token = _share_token()
    if not token:
        logger.debug("LAN cookie share skipped: CHZ_BRIDGE_TOKEN is empty")
        return None

    hosts = _candidate_hosts()
    if not hosts:
        return None

    port = _share_port()
    logger.info("Ищем cookies в локальной сети (%s хостов, порт %s)", len(hosts), port)

    with ThreadPoolExecutor(max_workers=_WORKERS) as pool:
        futures = {
            pool.submit(_pull_from_host, host, token=token, port=port): host for host in hosts
        }
        for future in as_completed(futures):
            try:
                cookies = future.result()
            except Exception:
                continue
            if cookies:
                logger.info("Получили cookies по LAN от %s", futures[future])
                return cookies
    return None

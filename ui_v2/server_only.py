from __future__ import annotations

import os
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("LOG_FILE", str(Path(__file__).resolve().parent.parent / "runtime" / "logs" / "lookup.log"))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from api_bridge import ApiBridge
from chz_bridge_server import start_chz_bridge_server
from ui_mobile.server_mobile import start_mobile_servers, stop_mobile_servers


def _env_flag(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw_value = str(os.getenv(name) or "").strip()
    if not raw_value:
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    return parsed if 1 <= parsed <= 65535 else default


def main() -> None:
    api = ApiBridge()
    api.start_session_auto_refresh()

    bridge_server = None
    try:
        bridge_server = start_chz_bridge_server(api)
    except OSError:
        bridge_server = None

    mobile_bundle = None
    if _env_flag("WMS_EMBED_SERVER_ENABLED", True):
        try:
            mobile_bundle = start_mobile_servers(
                api,
                host=str(os.getenv("WMS_EMBED_SERVER_HOST") or "127.0.0.1").strip() or "127.0.0.1",
                port=_env_int("WMS_EMBED_SERVER_PORT", 8787),
                https_port=_env_int("WMS_EMBED_SERVER_HTTPS_PORT", 8788),
                enable_https=_env_flag("WMS_EMBED_SERVER_HTTPS_ENABLED", False),
            )
        except OSError:
            mobile_bundle = None

    shutdown_requested = False

    def _handle_signal(_signum, _frame) -> None:
        nonlocal shutdown_requested
        shutdown_requested = True

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(signum, _handle_signal)
        except ValueError:
            continue

    try:
        while not shutdown_requested:
            time.sleep(1.0)
    finally:
        if mobile_bundle is not None:
            stop_mobile_servers(mobile_bundle)
        if bridge_server is not None:
            bridge_server.shutdown()
            bridge_server.server_close()


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import os
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any

from logger import logger
from ui_v2.api_bridge import ApiBridge


BRIDGE_HOST = "0.0.0.0"
BRIDGE_PORT = 8791


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def _is_bridge_enabled() -> bool:
    value = str(os.getenv("CHZ_BRIDGE_ENABLED", "1")).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _bridge_host() -> str:
    return str(os.getenv("CHZ_BRIDGE_HOST") or BRIDGE_HOST).strip() or BRIDGE_HOST


def _bridge_port() -> int:
    raw_value = str(os.getenv("CHZ_BRIDGE_PORT") or BRIDGE_PORT).strip()
    try:
        port = int(raw_value)
    except ValueError:
        return BRIDGE_PORT
    return port if 1 <= port <= 65535 else BRIDGE_PORT


def _bridge_token() -> str:
    return str(os.getenv("CHZ_BRIDGE_TOKEN") or "").strip()


class ChzBridgeRequestHandler(BaseHTTPRequestHandler):
    bridge: ApiBridge

    def __init__(self, *args, bridge: ApiBridge | None = None, **kwargs):
        self.bridge = bridge or ApiBridge()
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args) -> None:
        logger.info("CHZ bridge %s - %s", self.address_string(), format % args)

    def _write_json(self, payload: Any, *, status: int = HTTPStatus.OK) -> None:
        body = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        payload = self.rfile.read(length) if length > 0 else b"{}"
        if not payload:
            return {}
        parsed = json.loads(payload.decode("utf-8"))
        return parsed if isinstance(parsed, dict) else {}

    def _authorize(self) -> None:
        token = _bridge_token()
        if not token:
            raise PermissionError("CHZ bridge token is not configured.")
        provided = str(self.headers.get("X-CHZ-Token") or "").strip()
        if provided != token:
            raise PermissionError("Invalid CHZ bridge token.")

    def do_GET(self) -> None:  # noqa: N802
        if self.path.rstrip("/") == "/api/chz/health":
            state = self.bridge.get_orders_view_state(force_sync=False)
            self._write_json(
                {
                    "ok": True,
                    "mode": "desktop-chz-bridge",
                    "active_requests": len(state.get("wms_chz_active") or []),
                }
            )
            return
        self._write_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        if self.path.rstrip("/") != "/api/chz/requests":
            self._write_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            self._authorize()
            payload = self._read_json_body()
            result = self.bridge.receive_wms_chz_request(payload)
            if result.get("success"):
                self._write_json(result, status=HTTPStatus.CREATED)
            else:
                self._write_json(result, status=HTTPStatus.BAD_REQUEST)
        except PermissionError as exc:
            self._write_json({"success": False, "error": str(exc)}, status=HTTPStatus.UNAUTHORIZED)
        except json.JSONDecodeError as exc:
            self._write_json({"success": False, "error": f"Invalid JSON: {exc}"}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            logger.exception("CHZ bridge request failed: %s", exc)
            self._write_json({"success": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)


def _build_server(bridge: ApiBridge) -> ThreadingHTTPServer:
    handler = partial(ChzBridgeRequestHandler, bridge=bridge)
    server = ThreadingHTTPServer((_bridge_host(), _bridge_port()), handler)
    server.daemon_threads = True
    return server


def start_chz_bridge_server(bridge: ApiBridge) -> ThreadingHTTPServer | None:
    if not _is_bridge_enabled():
        logger.info("CHZ bridge server is disabled by configuration")
        return None

    server = _build_server(bridge)
    worker = Thread(target=server.serve_forever, name="ChzBridgeServer", daemon=True)
    worker.start()
    logger.info("CHZ bridge server started on http://%s:%s/api/chz/requests", _bridge_host(), _bridge_port())
    return server

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import socket
import ssl
import sys
import threading
import time
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent.parent
UI_ROOT = REPO_ROOT / "ui_v2" / "ui"
CERTS_DIR = Path(__file__).resolve().parent / "certs"

sys.path.insert(0, str(REPO_ROOT / "ui_v2"))
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("LOG_FILE", str(REPO_ROOT / "lookup.log"))
load_dotenv(REPO_ROOT / ".env")

from api_bridge import ApiBridge  # noqa: E402


CLIENT_CONFIG = {
    "browserMode": True,
    "mobileMode": True,
    "disableLabels": False,
    "disablePrinting": False,
    "apiBase": "/api/call",
    "appTitle": "Kontur Mobile",
    "brandTitle": "Kontur Mobile",
    "subtitleSuffix": "Мобильная web-версия для локальной сети.",
}

BLOCKED_METHODS: set[str] = set()

ALLOWED_METHODS = {
    "add_order_item",
    "approve_aggregations",
    "approve_selected_aggregations",
    "clear_logs",
    "clear_order_queue",
    "create_aggregation_codes",
    "create_order",
    "create_tsd_tasks",
    "delete_order",
    "download_aggregation_codes",
    "download_selected_aggregations",
    "get_aggregation_state",
    "get_default_date_window",
    "get_download_state",
    "get_history",
    "get_intro_state",
    "get_labels_state",
    "get_logs",
    "get_options",
    "get_orders_view_state",
    "get_session_info",
    "get_tsd_state",
    "introduce_aggregations",
    "introduce_orders",
    "introduce_saved_order_exact",
    "introduce_selected_aggregations",
    "lookup_gtin",
    "lookup_gtin_by_code",
    "manual_download_order",
    "preview_100x180_label",
    "print_100x180_label",
    "print_download_order",
    "refresh_session",
    "refill_aggregations",
    "restore_deleted_order",
    "submit_order_queue",
    "sync_download_statuses",
}


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")


def _discover_ipv4_addresses() -> list[str]:
    addresses: set[str] = {"127.0.0.1"}
    try:
        hostname = socket.gethostname()
        for item in socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM):
            addresses.add(item[4][0])
    except Exception:
        pass
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            addresses.add(sock.getsockname()[0])
    except Exception:
        pass
    return sorted(addresses)


def _build_browser_config() -> str:
    return (
        "<script>"
        f"window.__KONTUR_CLIENT_CONFIG__ = {json.dumps(CLIENT_CONFIG, ensure_ascii=False)};"
        "</script>"
    )


def _render_index_html() -> bytes:
    html = (UI_ROOT / "index.html").read_text(encoding="utf-8", errors="ignore")
    injected = html.replace("</head>", f"{_build_browser_config()}\n</head>", 1)
    return injected.encode("utf-8")


def _ensure_https_assets() -> tuple[Path, Path, Path] | None:
    try:
        import trustme  # type: ignore
    except Exception:
        return None

    CERTS_DIR.mkdir(parents=True, exist_ok=True)
    ca_pem = CERTS_DIR / "kontur-mobile-ca.pem"
    ca_cer = CERTS_DIR / "kontur-mobile-ca.cer"
    cert_pem = CERTS_DIR / "kontur-mobile-server.pem"
    key_pem = CERTS_DIR / "kontur-mobile-server.key"

    if ca_pem.exists() and cert_pem.exists() and key_pem.exists():
        if not ca_cer.exists():
            ca_cer.write_bytes(ca_pem.read_bytes())
        return ca_pem, cert_pem, key_pem

    hosts = {"localhost", "127.0.0.1", socket.gethostname()}
    hosts.update(_discover_ipv4_addresses())
    ca = trustme.CA()
    cert = ca.issue_cert(*sorted(hosts))
    ca.cert_pem.write_to_path(ca_pem)
    ca_cer.write_bytes(ca_pem.read_bytes())
    cert.cert_chain_pems[0].write_to_path(cert_pem)
    cert.private_key_pem.write_to_path(key_pem)
    return ca_pem, cert_pem, key_pem


class MobileRequestHandler(SimpleHTTPRequestHandler):
    bridge: ApiBridge

    def __init__(self, *args, directory: str | None = None, bridge: ApiBridge | None = None, **kwargs):
        self.bridge = bridge or ApiBridge()
        super().__init__(*args, directory=directory, **kwargs)

    def log_message(self, format: str, *args) -> None:
        message = format % args
        print(f"[ui_mobile] {self.address_string()} - {message}")

    def _write_bytes(self, payload: bytes, *, content_type: str, status: int = HTTPStatus.OK) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(payload)

    def _write_json(self, payload: Any, *, status: int = HTTPStatus.OK) -> None:
        self._write_bytes(_json_bytes(payload), content_type="application/json; charset=utf-8", status=status)

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise ValueError(f"Некорректный JSON: {exc}") from exc
        return payload if isinstance(payload, dict) else {}

    def _serve_static_file(self, relative_path: str) -> None:
        target = (UI_ROOT / relative_path.lstrip("/")).resolve()
        if not str(target).startswith(str(UI_ROOT.resolve())) or not target.exists() or not target.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        mime, _ = mimetypes.guess_type(str(target))
        self._write_bytes(
            target.read_bytes(),
            content_type=(mime or "application/octet-stream") + ("; charset=utf-8" if mime and (mime.startswith("text/") or mime in {"application/javascript", "application/json"}) else ""),
        )

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        if path in {"/", "/index.html"}:
            self._write_bytes(_render_index_html(), content_type="text/html; charset=utf-8")
            return
        if path == "/api/health":
            self._write_json({"ok": True, "mode": "browser-mobile", "time": time.time()})
            return
        if path == "/api/config":
            self._write_json(CLIENT_CONFIG)
            return
        if path == "/favicon.ico":
            icon = REPO_ROOT / "icon.ico"
            if icon.exists():
                self._write_bytes(icon.read_bytes(), content_type="image/x-icon")
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        if path.startswith("/certs/"):
            cert_file = (CERTS_DIR / path.removeprefix("/certs/")).resolve()
            if str(cert_file).startswith(str(CERTS_DIR.resolve())) and cert_file.exists() and cert_file.is_file():
                mime, _ = mimetypes.guess_type(str(cert_file))
                self._write_bytes(cert_file.read_bytes(), content_type=mime or "application/octet-stream")
            else:
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return
        if path in {"/app.js", "/styles.css"}:
            self._serve_static_file(path)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path or ""
        if not path.startswith("/api/call/"):
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        method = unquote(path.removeprefix("/api/call/")).strip()
        if not method or method.startswith("_") or method in BLOCKED_METHODS or method not in ALLOWED_METHODS:
            self._write_json({"error": f"Метод недоступен в мобильной web-версии: {method}"}, status=HTTPStatus.FORBIDDEN)
            return

        target = getattr(self.bridge, method, None)
        if not callable(target):
            self._write_json({"error": f"Метод не найден: {method}"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            payload = self._read_json_body()
            args = payload.get("args") or []
            kwargs = payload.get("kwargs") or {}
            result = target(*args, **kwargs)
            self._write_json(result)
        except Exception as exc:
            self._write_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)


def _build_http_server(host: str, port: int, bridge: ApiBridge) -> ThreadingHTTPServer:
    handler = partial(MobileRequestHandler, directory=str(UI_ROOT), bridge=bridge)
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server


def _build_https_server(host: str, port: int, bridge: ApiBridge) -> tuple[ThreadingHTTPServer, Path] | None:
    assets = _ensure_https_assets()
    if not assets:
        return None
    ca_pem, cert_pem, key_pem = assets
    server = _build_http_server(host, port, bridge)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert_pem), keyfile=str(key_pem))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    return server, ca_pem


def _server_thread(server: ThreadingHTTPServer) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def _print_urls(host: str, http_port: int, https_port: int | None, has_https: bool) -> None:
    addresses = _discover_ipv4_addresses()
    public_hosts = addresses if host == "0.0.0.0" else [host]
    print("\nKontur Mobile запущен.\n")
    for item in public_hosts:
        print(f"HTTP  : http://{item}:{http_port}/")
    if has_https and https_port:
        for item in public_hosts:
            print(f"HTTPS : https://{item}:{https_port}/")
        print(f"\nCA cert для iPhone: http://{public_hosts[0]}:{http_port}/certs/kontur-mobile-ca.cer")
    else:
        print("\nHTTPS пока не включён. Если нужен локальный сертификат, переустановите зависимости через setup.bat.")
    print("\nОстановить сервер: Ctrl+C\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kontur Mobile browser server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--https-port", type=int, default=8788)
    parser.add_argument("--no-https", action="store_true")
    args = parser.parse_args()

    bridge = ApiBridge()
    http_server = _build_http_server(args.host, args.port, bridge)
    threads = [_server_thread(http_server)]
    https_server: ThreadingHTTPServer | None = None
    ca_pem: Path | None = None

    if not args.no_https:
        https_bundle = _build_https_server(args.host, args.https_port, bridge)
        if https_bundle:
            https_server, ca_pem = https_bundle
            threads.append(_server_thread(https_server))

    _print_urls(args.host, args.port, args.https_port if https_server else None, https_server is not None)
    if ca_pem:
        print(f"CA cert сохранён: {ca_pem}")

    try:
        while any(thread.is_alive() for thread in threads):
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        http_server.shutdown()
        http_server.server_close()
        if https_server is not None:
            https_server.shutdown()
            https_server.server_close()


if __name__ == "__main__":
    main()

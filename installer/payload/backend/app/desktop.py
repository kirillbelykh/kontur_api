"""Desktop PyWebView entry — loads the React frontend."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("LOG_FILE", str(REPO_ROOT / "runtime" / "logs" / "lookup.log"))
load_dotenv(REPO_ROOT / ".env")

try:
    import webview
except ImportError as exc:
    raise SystemExit("PyWebView not installed. Run: uv sync") from exc

from backend.app.api_bridge import ApiBridge
from backend.app.chz_bridge_server import start_chz_bridge_server


def _resolve_frontend_url() -> str:
    """Prefer Vite dev server when set; otherwise load production build."""
    dev_url = str(os.getenv("VITE_DEV_URL") or "").strip()
    if dev_url:
        return dev_url
    index_path = REPO_ROOT / "frontend" / "dist" / "index.html"
    if index_path.exists():
        return index_path.resolve().as_uri()
    # Fallback: archived static UI while React dist is building
    legacy = REPO_ROOT / "archive" / "ui_v2_static" / "ui" / "index.html"
    if legacy.exists():
        return legacy.resolve().as_uri()
    raise SystemExit(
        "Frontend not found. Run `cd frontend && npm install && npm run build`, "
        "or set VITE_DEV_URL=http://127.0.0.1:5173"
    )


def _resolve_pythonw() -> str:
    executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        return str(executable)
    pythonw = executable.with_name("pythonw.exe")
    if pythonw.exists():
        return str(pythonw)
    return str(executable)


def _ensure_desktop_shortcut() -> None:
    shortcut_path = Path.home() / "Desktop" / "Контур Маркировка.lnk"
    icon_path = REPO_ROOT / "assets" / "icons" / "icon.ico"
    try:
        import win32com.client  # type: ignore

        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortCut(str(shortcut_path))
        shortcut.TargetPath = _resolve_pythonw()
        shortcut.Arguments = f'"{REPO_ROOT / "main.py"}"'
        shortcut.WorkingDirectory = str(REPO_ROOT)
        shortcut.Description = "Контур Маркировка"
        if icon_path.exists():
            shortcut.IconLocation = str(icon_path)
        shortcut.Save()
    except Exception:
        pass


def main() -> None:
    _ensure_desktop_shortcut()
    api = ApiBridge()
    api.start_session_auto_refresh()
    try:
        start_chz_bridge_server(api)
    except OSError:
        pass

    window = webview.create_window(
        title="Контур Маркировка",
        url=_resolve_frontend_url(),
        js_api=api,
        width=1440,
        height=900,
        min_size=(1100, 700),
    )
    window.events.loaded += lambda _window: api.start_session_auto_refresh()
    debug_mode = os.getenv("KONTUR_UI_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    webview.start(debug=debug_mode)


if __name__ == "__main__":
    main()

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
os.environ.setdefault("LOG_FILE", str(Path(__file__).resolve().parent.parent / "runtime" / "logs" / "lookup.log"))
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

try:
    import webview
except ImportError as exc:
    raise SystemExit(
        "PyWebView not installed. Run: pip install -r ui_v2/requirements_v2.txt"
    ) from exc

from api_bridge import ApiBridge
from chz_bridge_server import start_chz_bridge_server
from ui_mobile.server_mobile import start_mobile_servers


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


def _resolve_pythonw() -> str:
    executable = Path(sys.executable)
    if executable.name.lower() == "pythonw.exe":
        return str(executable)
    pythonw = executable.with_name("pythonw.exe")
    if pythonw.exists():
        return str(pythonw)
    return str(executable)


def _ensure_desktop_shortcut() -> None:
    shortcut_path = Path.home() / "Desktop" / "KonturTestAPI.lnk"
    script_path = Path(__file__).resolve()
    repo_root = script_path.parent.parent
    icon_path = repo_root / "assets" / "icons" / "icon.ico"
    if not icon_path.exists():
        icon_path = repo_root / "icon.ico"

    try:
        import win32com.client  # type: ignore

        shell = win32com.client.Dispatch("WScript.Shell")
        shortcut = shell.CreateShortCut(str(shortcut_path))
        shortcut.TargetPath = _resolve_pythonw()
        shortcut.Arguments = f'"{script_path}"'
        shortcut.WorkingDirectory = str(repo_root)
        shortcut.Description = "KonturTestAPI"
        if icon_path.exists():
            shortcut.IconLocation = str(icon_path)
        shortcut.Save()
    except Exception:
        pass


def _install_desktop_scroll_overrides(window: webview.Window) -> None:
    window.load_css(
        """
        body {
          overflow-y: auto !important;
          overflow-x: hidden !important;
        }

        .app-shell {
          min-height: max(100vh, var(--app-height)) !important;
          height: auto !important;
          align-items: start;
        }

        .sidebar {
          position: sticky;
          top: 0;
          align-self: start;
          min-height: var(--app-height);
        }

        .main-shell {
          grid-template-rows: auto auto 28px !important;
          overflow: visible !important;
        }

        .content-area {
          overflow: visible !important;
        }
        """
    )


def main():
    _ensure_desktop_shortcut()
    api = ApiBridge()
    api.start_session_auto_refresh()
    try:
        start_chz_bridge_server(api)
    except OSError:
        pass
    if _env_flag("WMS_EMBED_SERVER_ENABLED", True):
        try:
            start_mobile_servers(
                api,
                host=str(os.getenv("WMS_EMBED_SERVER_HOST") or "127.0.0.1").strip() or "127.0.0.1",
                port=_env_int("WMS_EMBED_SERVER_PORT", 8787),
                https_port=_env_int("WMS_EMBED_SERVER_HTTPS_PORT", 8788),
                enable_https=_env_flag("WMS_EMBED_SERVER_HTTPS_ENABLED", False),
            )
        except OSError:
            pass
    index_path = Path(__file__).resolve().parent / "ui" / "index.html"
    window = webview.create_window(
        title="KonturTestAPI [TEST]",
        url=index_path.resolve().as_uri(),
        js_api=api,
        width=1440,
        height=900,
        min_size=(1100, 700),
    )
    window.events.loaded += _install_desktop_scroll_overrides
    window.events.loaded += lambda _window: api.start_session_auto_refresh()
    debug_mode = os.getenv("KONTUR_UI_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    webview.start(debug=debug_mode)


if __name__ == "__main__":
    main()

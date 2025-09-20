from pathlib import Path
import requests
import json
from typing import Dict, Optional


COOKIES_FILE = Path("cookies.json")
# ---------------- helpers ----------------
def load_cookies() -> Optional[Dict[str, str]]:
    if COOKIES_FILE.exists():
        try:
            data = json.loads(COOKIES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data:
                return data
        except Exception:
            pass
    return None

def make_session_with_cookies(cookies: Optional[Dict[str, str]]) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
    })
    if cookies:
        for k, v in cookies.items():
            s.cookies.set(k, v, domain="mk.kontur.ru", path="/")
    return s

def get_tnved_code(simpl: str):
    simpl_lower = simpl.lower()
    if any(word in simpl_lower for word in ["хир", "микро", "ультра", "гинек", "дв пара"]):
        return "4015120001"
    else:
        return "4015120009"
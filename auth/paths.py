"""Resolve Yandex Browser binary and profile paths."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional

from logger import logger


def find_yandex_paths() -> Dict[str, Optional[Path | str]]:
    """Find Yandex Browser binary, user-data root and profile directory."""
    paths: Dict[str, Optional[Path | str]] = {
        "browser": None,
        "user_data": None,
        "profile_directory": None,
    }

    if sys.platform.startswith("win"):
        try:
            import winreg

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Classes\YandexBrowserHTML\shell\open\command",
            ) as key:
                value = winreg.QueryValue(key, "")
                if value:
                    browser_path = value.split('"')[1] if '"' in value else value.split()[0]
                    paths["browser"] = Path(browser_path)
        except Exception:
            pass

    if not paths["browser"] or not Path(str(paths["browser"])).exists():
        possible_browser_paths = [
            Path(os.environ.get("LOCALAPPDATA", "")) / "Yandex/YandexBrowser/Application/browser.exe",
            Path(os.environ.get("PROGRAMFILES", "")) / "Yandex/YandexBrowser/Application/browser.exe",
            Path(os.environ.get("PROGRAMFILES(X86)", "")) / "Yandex/YandexBrowser/Application/browser.exe",
        ]
        for candidate_browser_path in possible_browser_paths:
            if candidate_browser_path.exists():
                paths["browser"] = candidate_browser_path
                break

    user_data_root: Optional[Path] = None
    browser_path = Path(str(paths["browser"])) if paths["browser"] else None
    if browser_path and browser_path.exists():
        user_data_paths = [
            Path(os.environ.get("LOCALAPPDATA", "")) / "Yandex/YandexBrowser/User Data",
            browser_path.parent.parent / "User Data",
        ]
        for user_data_path in user_data_paths:
            if user_data_path.exists():
                user_data_root = user_data_path
                break

    if browser_path and not user_data_root:
        user_data_root = Path(os.environ.get("LOCALAPPDATA", "")) / "Yandex/YandexBrowser/User Data"

    profile_candidates: list[str] = []
    configured_profile = str(os.getenv("KONTUR_YANDEX_PROFILE") or "").strip()
    if configured_profile:
        profile_candidates.append(configured_profile)

    if user_data_root and user_data_root.exists():
        local_state_path = user_data_root / "Local State"
        if local_state_path.exists():
            try:
                local_state = json.loads(local_state_path.read_text(encoding="utf-8"))
                profile_info = local_state.get("profile") if isinstance(local_state, dict) else {}
                if isinstance(profile_info, dict):
                    last_used = str(profile_info.get("last_used") or "").strip()
                    if last_used:
                        profile_candidates.append(last_used)
                    for profile_name in profile_info.get("last_active_profiles") or []:
                        normalized_name = str(profile_name or "").strip()
                        if normalized_name:
                            profile_candidates.append(normalized_name)
                    info_cache = profile_info.get("info_cache") or {}
                    if isinstance(info_cache, dict):
                        for profile_name in info_cache.keys():
                            normalized_name = str(profile_name or "").strip()
                            if normalized_name:
                                profile_candidates.append(normalized_name)
            except Exception as exc:
                logger.debug("Не удалось прочитать Local State Yandex Browser: %s", exc)

    profile_candidates.extend(["Default", "Profile 1"])

    normalized_profile_candidates: list[str] = []
    seen_profiles: set[str] = set()
    for candidate in profile_candidates:
        normalized_candidate = str(candidate or "").strip()
        lowered = normalized_candidate.lower()
        if not normalized_candidate or lowered in seen_profiles:
            continue
        seen_profiles.add(lowered)
        normalized_profile_candidates.append(normalized_candidate)

    selected_profile_directory: Optional[str] = None
    if user_data_root:
        for profile_name in normalized_profile_candidates:
            if (user_data_root / profile_name).exists():
                selected_profile_directory = profile_name
                break
    if not selected_profile_directory and normalized_profile_candidates:
        selected_profile_directory = normalized_profile_candidates[0]

    paths["user_data"] = user_data_root
    paths["profile_directory"] = selected_profile_directory
    return paths

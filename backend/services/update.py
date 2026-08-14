# updater.py — git-based desktop updates
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

try:
    import tkinter.messagebox as mbox
except Exception:  # pragma: no cover - headless / no tk
    mbox = None  # type: ignore[assignment]

from backend.services.logger import logger
from backend.services.win_subprocess import hidden_console_kwargs

HISTORY_FILE = "full_orders_history.json"
STASH_EXCLUDES = (f":(exclude){HISTORY_FILE}", ":(exclude)runtime/backups")


def _git_kwargs():
    """kwargs для subprocess: скрыть консоль на Windows и вернуть текст."""
    env = os.environ.copy()
    # stash создаёт коммит и падает, если на ПК не заданы user.name / user.email
    env.setdefault("GIT_AUTHOR_NAME", "KonturMarkirovka")
    env.setdefault("GIT_AUTHOR_EMAIL", "kontur@markirovka.local")
    env.setdefault("GIT_COMMITTER_NAME", env["GIT_AUTHOR_NAME"])
    env.setdefault("GIT_COMMITTER_EMAIL", env["GIT_AUTHOR_EMAIL"])
    return {"text": True, "env": env, **hidden_console_kwargs()}


def _run_git(args, repo_dir, check=True):
    return subprocess.run(
        ["git"] + args,
        cwd=repo_dir,
        check=check,
        capture_output=True,
        **_git_kwargs(),
    )


def _capture_git(args, repo_dir):
    return subprocess.check_output(["git"] + args, cwd=repo_dir, **_git_kwargs())


def _kill_git_processes():
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/F", "/IM", "git.exe"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        else:
            subprocess.run(
                ["pkill", "-f", "git"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception:
        logger.debug("Не удалось завершить зависшие git-процессы", exc_info=True)


def _ensure_index_lock_removed(repo_dir, attempts=6, delay=0.3):
    lock_path = os.path.join(repo_dir, ".git", "index.lock")
    for _ in range(attempts):
        if os.path.exists(lock_path):
            _kill_git_processes()
            try:
                os.remove(lock_path)
                return
            except PermissionError:
                time.sleep(delay)
                continue
            except FileNotFoundError:
                return
        else:
            return


def default_repo_dir() -> str:
    """Repo root for kontur_api (backend/services/update.py → parents[2])."""
    here = Path(__file__).resolve()
    candidates = [here.parents[2], Path.cwd()]
    for candidate in candidates:
        if (candidate / ".git").exists():
            return str(candidate)
    return str(here.parents[2])


def _default_pre_cleanup():
    """Закрыть рут-логгеры и собрать мусор, чтобы файлы могли быть заменены."""
    # Молча: смысл функции — освободить файловые хэндлы логов перед git-обновлением;
    # логирование здесь снова открыло бы файл, а сбой flush/close ни на что не влияет.
    try:
        import gc
        import logging

        root = logging.getLogger()
        for h in root.handlers[:]:
            try:
                h.flush()
            except Exception:
                pass
            try:
                h.close()
            except Exception:
                pass
            try:
                root.removeHandler(h)
            except Exception:
                pass
        gc.collect()
    except Exception:
        pass


def _resolve_commits(repo_dir: str) -> tuple[str, str]:
    _ensure_index_lock_removed(repo_dir)
    try:
        _run_git(["fetch", "origin", "--prune"], repo_dir)
    except subprocess.CalledProcessError as exc:
        print("git fetch failed:", exc)

    local_commit = _capture_git(["rev-parse", "HEAD"], repo_dir).strip()
    try:
        remote_commit = _capture_git(["rev-parse", "origin/main"], repo_dir).strip()
    except subprocess.CalledProcessError:
        remote_commit = _capture_git(["ls-remote", "origin", "main"], repo_dir).split()[0].strip()
    return local_commit, remote_commit


def _behind_count(repo_dir: str, local_commit: str, remote_commit: str) -> int:
    if local_commit == remote_commit:
        return 0
    try:
        out = _capture_git(
            ["rev-list", "--count", f"{local_commit}..{remote_commit}"],
            repo_dir,
        ).strip()
        return int(out or 0)
    except Exception:
        return 1 if local_commit != remote_commit else 0


def probe_updates(repo_dir: Optional[str] = None) -> Dict[str, Any]:
    """Non-interactive: fetch + compare HEAD vs origin/main."""
    repo = repo_dir or default_repo_dir()
    try:
        if not (Path(repo) / ".git").exists():
            return {
                "update_available": False,
                "error": "Репозиторий git не найден",
                "repo_dir": repo,
            }
        local_commit, remote_commit = _resolve_commits(repo)
        behind = _behind_count(repo, local_commit, remote_commit)
        return {
            "update_available": behind > 0,
            "local_commit": local_commit,
            "remote_commit": remote_commit,
            "behind_count": behind,
            "repo_dir": repo,
        }
    except Exception as exc:
        return {"update_available": False, "error": str(exc), "repo_dir": repo}


def _restart_executable() -> str:
    """Prefer pythonw so the restarted app has no console window."""
    executable = Path(sys.executable)
    if executable.name.lower() == "python.exe":
        sibling = executable.with_name("pythonw.exe")
        if sibling.exists():
            return str(sibling)
    return str(executable)


def schedule_process_restart(delay_sec: float = 0.8) -> None:
    """Spawn a fresh process, then exit this one. Safe from the JS-bridge thread.

    os.execl from a pywebview callback would replace the worker thread, not the
    desktop process. Spawn first so the operator is not left without a window.
    """

    def _restart() -> None:
        try:
            from backend.app.api_bridge import stop_background_workers

            stop_background_workers()
        except Exception:
            logger.debug("Не удалось остановить фоновые потоки перед перезапуском", exc_info=True)
        python = _restart_executable()
        cwd = default_repo_dir()
        kwargs = hidden_console_kwargs()
        if os.name == "nt":
            kwargs["creationflags"] = int(kwargs.get("creationflags") or 0) | int(
                getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            )
        try:
            subprocess.Popen([python, *sys.argv], cwd=cwd, close_fds=False, **kwargs)
        except Exception:
            logger.exception("Не удалось запустить новый процесс после обновления")
            return
        try:
            import webview

            for window in list(getattr(webview, "windows", None) or []):
                try:
                    window.destroy()
                except Exception:
                    logger.debug("Не удалось закрыть окно webview перед выходом", exc_info=True)
        except Exception:
            logger.debug("webview недоступен при перезапуске", exc_info=True)
        os._exit(0)

    timer = threading.Timer(delay_sec, _restart)
    timer.daemon = True
    timer.start()


def porcelain_path(line: str) -> str:
    raw = line.rstrip("\n")
    if len(raw) < 4:
        return ""
    body = raw[3:]
    if " -> " in body:
        body = body.split(" -> ", 1)[1]
    return body.strip().strip('"').replace("\\", "/")


def is_operator_local_path(path: str) -> bool:
    normalized = path.replace("\\", "/").lstrip("./")
    return normalized == HISTORY_FILE or normalized.startswith("runtime/backups/")


def local_changes_need_stash(porcelain: str) -> bool:
    """История заказов на каждом ПК своя — её не прячем в stash и не блокируем обновление."""
    for line in porcelain.splitlines():
        path = porcelain_path(line)
        if path and not is_operator_local_path(path):
            return True
    return False


def _backup_order_history(repo: str) -> Optional[str]:
    src = Path(repo) / HISTORY_FILE
    if not src.exists():
        return None
    dest_dir = Path(repo) / "runtime" / "backups" / "history"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"full_orders_history-{time.strftime('%Y%m%d-%H%M%S')}.json"
    shutil.copy2(src, dest)
    return str(dest)


def _restore_order_history(repo: str, backup: Optional[str]) -> None:
    if not backup:
        return
    src = Path(backup)
    if not src.exists():
        return
    shutil.copy2(src, Path(repo) / HISTORY_FILE)


def apply_update(
    repo_dir: Optional[str] = None,
    *,
    pre_update_cleanup: Optional[Callable[[], None]] = None,
    auto_restart: bool = False,
    allow_hard_reset: bool = False,
) -> Dict[str, Any]:
    """Non-interactive fast-forward to origin/main (optional hard reset)."""
    repo = repo_dir or default_repo_dir()
    if pre_update_cleanup is None:
        pre_update_cleanup = _default_pre_cleanup

    try:
        probe = probe_updates(repo)
        if probe.get("error"):
            return {"success": False, "error": probe["error"]}
        if not probe.get("update_available"):
            return {
                "success": True,
                "updated": False,
                "message": "Уже актуальная версия",
                "local_commit": probe.get("local_commit"),
                "remote_commit": probe.get("remote_commit"),
            }

        try:
            pre_update_cleanup()
        except Exception:
            logger.debug("pre_update_cleanup завершился с ошибкой — продолжаем обновление", exc_info=True)
        _ensure_index_lock_removed(repo)

        try:
            status = _capture_git(["status", "--porcelain"], repo)
            has_local_changes = bool(status.strip())
        except Exception:
            status = ""
            has_local_changes = False

        history_backup = _backup_order_history(repo)
        stash_created = False
        try:
            if local_changes_need_stash(status):
                stashed = _run_git(
                    ["stash", "push", "-u", "-m", "autostash-before-update", "--", *STASH_EXCLUDES],
                    repo,
                    check=False,
                )
                if stashed.returncode == 0:
                    stash_created = True
                else:
                    logger.warning(
                        "stash пропущен, обновляем без него: %s",
                        (stashed.stderr or stashed.stdout or "").strip(),
                    )

            if has_local_changes:
                _run_git(["checkout", "--", HISTORY_FILE], repo, check=False)

            try:
                _run_git(["merge", "--ff-only", "origin/main"], repo)
            except subprocess.CalledProcessError:
                if not allow_hard_reset:
                    if stash_created:
                        try:
                            _run_git(["stash", "pop"], repo)
                        except Exception:
                            logger.warning("Локальные изменения остались в stash — выполните `git stash pop` вручную", exc_info=True)
                    return {
                        "success": False,
                        "error": "Не удалось выполнить fast-forward. Ветка расходится с origin/main.",
                    }
                try:
                    _run_git(["reset", "--hard", "origin/main"], repo)
                except subprocess.CalledProcessError:
                    if stash_created:
                        try:
                            _run_git(["stash", "pop"], repo)
                        except Exception:
                            logger.warning("Локальные изменения остались в stash — выполните `git stash pop` вручную", exc_info=True)
                    return {"success": False, "error": "Не удалось выполнить принудительное обновление"}

            if stash_created:
                try:
                    _run_git(["stash", "pop"], repo)
                except subprocess.CalledProcessError:
                    logger.warning("Локальные изменения остались в stash — выполните `git stash pop` вручную", exc_info=True)

            new_head = _capture_git(["rev-parse", "HEAD"], repo).strip()
            result: Dict[str, Any] = {
                "success": True,
                "updated": True,
                "restarted": False,
                "local_commit": new_head,
                "remote_commit": probe.get("remote_commit"),
                "message": "Обновление установлено. Перезапустите приложение.",
            }

            if auto_restart:
                result["restarted"] = True
                result["message"] = "Обновление установлено. Перезапуск…"
                schedule_process_restart()

            return result
        finally:
            _restore_order_history(repo, history_backup)
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def check_for_updates(repo_dir=None, pre_update_cleanup=None, auto_restart=True):
    """
    Legacy interactive path (tkinter messagebox).
    Возвращает True если обновление выполнено (до рестарта), False если не выполнялось.
    """
    if repo_dir is None:
        repo_dir = default_repo_dir()

    if pre_update_cleanup is None:
        pre_update_cleanup = _default_pre_cleanup

    try:
        probe = probe_updates(repo_dir)
        if probe.get("error"):
            if mbox:
                mbox.showerror("Ошибка", probe["error"])
            return False
        if not probe.get("update_available"):
            return False

        if mbox:
            ans = mbox.askyesno(
                "Обновление доступно",
                "Обнаружена новая версия приложения.\nУстановить обновление сейчас?",
            )
            if not ans:
                return False
        else:
            return False

        result = apply_update(
            repo_dir,
            pre_update_cleanup=pre_update_cleanup,
            auto_restart=False,
            allow_hard_reset=False,
        )
        if not result.get("success") or not result.get("updated"):
            err = result.get("error") or "Обновление не выполнено"
            if "fast-forward" in err.lower() or "расходится" in err.lower():
                if mbox:
                    resp = mbox.askyesno(
                        "Конфликт версий",
                        "Не удалось выполнить fast-forward (ветка расходится с origin).\n"
                        "Выполнить принудительное обновление (git reset --hard origin/main)?\n\n"
                        "Внимание: это удалит локальные незакоммиченные изменения "
                        "и незапушенные коммиты в ветке.",
                    )
                    if resp:
                        result = apply_update(
                            repo_dir,
                            pre_update_cleanup=pre_update_cleanup,
                            auto_restart=False,
                            allow_hard_reset=True,
                        )
            if not result.get("success") or not result.get("updated"):
                if mbox:
                    mbox.showerror("Ошибка", result.get("error") or err)
                return False

        if mbox:
            mbox.showinfo("Обновление", "Обновление успешно установлено!")
        if auto_restart:
            python = sys.executable
            os.execl(python, python, *sys.argv)
        return True
    except Exception as exc:
        print("Ошибка при обновлении:", exc)
        if mbox:
            mbox.showerror("Ошибка", f"Не удалось выполнить обновление: {exc}")
        return False

# updater.py
import os
import sys
import time
import subprocess
import tkinter.messagebox as mbox

def _git_kwargs():
    """kwargs для subprocess: скрыть консоль на Windows и вернуть текст."""
    kwargs = {'text': True}
    if os.name == 'nt':
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = 0
        kwargs.update({'startupinfo': si, 'creationflags': getattr(subprocess, 'CREATE_NO_WINDOW', 0)})
    return kwargs

def _run_git(args, repo_dir, check=True):
    return subprocess.run(["git"] + args, cwd=repo_dir, check=check, **_git_kwargs())

def _capture_git(args, repo_dir):
    return subprocess.check_output(["git"] + args, cwd=repo_dir, **_git_kwargs())

def _kill_git_processes():
    try:
        if os.name == 'nt':
            subprocess.run(["taskkill", "/F", "/IM", "git.exe"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            subprocess.run(["pkill", "-f", "git"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

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

def _default_pre_cleanup():
    """Ведение: закрыть рута-логгеры и собрать мусор, чтобы файлы могли быть заменены."""
    try:
        import logging, gc
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

def check_for_updates(repo_dir=None, pre_update_cleanup=None, auto_restart=True):
    """
    Проверяет origin/main и предлагает обновление.
    repo_dir - папка репозитория (по умолчанию — папка файла, который импортировал модуль).
    pre_update_cleanup - callable(), вызывается перед обновлением (закрыть логгеры и т.п.)
    auto_restart - если True, после успешного обновления будет os.execl(...) для перезапуска.
    Возвращает True если обновление выполнено (до рестарта), False если не выполнялось.
    """
    if repo_dir is None:
        repo_dir = os.path.abspath(os.path.dirname(__file__))

    if pre_update_cleanup is None:
        pre_update_cleanup = _default_pre_cleanup

    try:
        _ensure_index_lock_removed(repo_dir)

        # fetch — обновляем refs
        try:
            _run_git(["fetch", "origin", "--prune"], repo_dir)
        except subprocess.CalledProcessError as e:
            # если fetch упал — fallback к ls-remote дальше
            print("git fetch failed:", e)

        # получаем хеши
        try:
            local_commit = _capture_git(["rev-parse", "HEAD"], repo_dir).strip()
        except subprocess.CalledProcessError:
            mbox.showerror("Ошибка", "Не удалось определить локальный коммит в репозитории.")
            return False

        try:
            # prefer origin/main (после fetch)
            remote_commit = _capture_git(["rev-parse", "origin/main"], repo_dir).strip()
        except subprocess.CalledProcessError:
            # fallback — ls-remote
            try:
                remote_commit = subprocess.check_output(["git", "ls-remote", "origin", "main"], cwd=repo_dir, text=True).split()[0]
            except Exception as e:
                print("ls-remote failed:", e)
                mbox.showerror("Ошибка", "Не удалось определить удалённый коммит.")
                return False

        if local_commit == remote_commit:
            return False  # актуально

        # спрашиваем пользователя
        ans = mbox.askyesno("Обновление доступно", "🔄 Обнаружена новая версия приложения.\nУстановить обновление сейчас?")
        if not ans:
            return False

        # подготовка
        try:
            pre_update_cleanup()
        except Exception:
            pass
        _ensure_index_lock_removed(repo_dir)

        # есть ли локальные изменения?
        try:
            status = _capture_git(["status", "--porcelain"], repo_dir)
            has_local_changes = bool(status.strip())
        except Exception:
            has_local_changes = False

        stash_created = False
        if has_local_changes:
            try:
                _run_git(["stash", "push", "-u", "-m", "autostash-before-update"], repo_dir)
                stash_created = True
            except subprocess.CalledProcessError as e:
                mbox.showerror("Ошибка", "Не удалось временно сохранить локальные изменения. Обновление отменено.")
                return False

        # пытаемся безболезненно (fast-forward)
        try:
            _run_git(["merge", "--ff-only", "origin/main"], repo_dir)
        except subprocess.CalledProcessError:
            # не получилось — спрашиваем про принудительный reset
            resp = mbox.askyesno(
                "Конфликт версий",
                "Не удалось выполнить fast-forward (ветка расходится с origin).\n"
                "Выполнить принудительное обновление (git reset --hard origin/main)?\n\n"
                "Внимание: это удалит локальные незакоммиченные изменения и незапушенные коммиты в ветке."
            )
            if not resp:
                if stash_created:
                    try:
                        _run_git(["stash", "pop"], repo_dir)
                    except Exception:
                        pass
                return False
            try:
                _run_git(["reset", "--hard", "origin/main"], repo_dir)
            except subprocess.CalledProcessError:
                mbox.showerror("Ошибка", "Не удалось выполнить принудительное обновление.")
                if stash_created:
                    try:
                        _run_git(["stash", "pop"], repo_dir)
                    except Exception:
                        pass
                return False

        # восстановление stash (если был)
        if stash_created:
            try:
                _run_git(["stash", "pop"], repo_dir)
            except subprocess.CalledProcessError:
                mbox.showwarning("Внимание", "Обновление установлено, но при восстановлении локальных изменений возникли конфликты.\nПроверьте репозиторий вручную.")

        mbox.showinfo("Обновление", "✅ Обновление успешно установлено!")
        if auto_restart:
            python = sys.executable
            os.execl(python, python, *sys.argv)

        return True

    except Exception as exc:
        print("Ошибка при обновлении:", exc)
        mbox.showerror("Ошибка", f"Не удалось выполнить обновление: {exc}")
        return False

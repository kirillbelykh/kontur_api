"""Self-check for non-interactive git update probe helpers."""
from __future__ import annotations

from backend.services import update as update_mod


def main() -> None:
    repo = update_mod.default_repo_dir()
    assert repo, "repo dir empty"
    result = update_mod.probe_updates(repo)
    assert "update_available" in result, result
    assert isinstance(result["update_available"], bool), result
    if result.get("error"):
        # Offline / no remote is acceptable for the self-check shape
        print("probe_updates error (ok for offline):", result["error"])
        return
    assert result.get("local_commit"), result
    assert result.get("remote_commit"), result
    assert "behind_count" in result, result
    print(
        "ok",
        "available=",
        result["update_available"],
        "behind=",
        result["behind_count"],
        "local=",
        str(result["local_commit"])[:7],
        "remote=",
        str(result["remote_commit"])[:7],
    )


if __name__ == "__main__":
    main()

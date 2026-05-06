from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from cookies import run_kontur_access_prolongation_service


load_dotenv(Path(__file__).resolve().parent / ".env")


def main() -> None:
    run_kontur_access_prolongation_service()


if __name__ == "__main__":
    main()

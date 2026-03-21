from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


sys.path.insert(0, str(_repo_root()))
os.environ.setdefault("HISTORY_SYNC_ENABLED", "0")
os.environ.setdefault("LOG_FILE", str((_repo_root() / "ui_v2_true_status.log").resolve()))

from aggregation_bulk import BulkAggregationService
from cryptopro import find_certificate_by_thumbprint, sign_text_data
from get_thumb import find_certificate_thumbprint


def _translate_status(value: Any) -> str:
    mapping = {
        "INTRODUCED": "Введен в оборот",
        "APPLIED": "В обороте",
        "EMITTED": "Эмитирован",
        "WRITTEN_OFF": "Выведен из оборота",
        "RETIRED": "Выведен из оборота",
        "DISAGGREGATED": "Расформирован",
        "UNKNOWN": "Неизвестно",
    }
    raw = str(value or "").strip()
    if not raw:
        return "Неизвестно"
    return mapping.get(raw, mapping.get(raw.upper(), raw))


def _format_status_counts(counts: Counter[str]) -> str:
    return ", ".join(f"{_translate_status(status)}: {count}" for status, count in counts.items())


def _read_codes_sample(csv_path: str, limit: int) -> list[str]:
    path = Path(csv_path)
    if not path.exists():
        return []

    result: list[str] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            for cell in row:
                value = str(cell or "").strip()
                if not value:
                    continue
                if re.search(r"[А-Яа-яA-Za-z]", value) and len(value) < 24:
                    continue
                if value in seen:
                    continue
                seen.add(value)
                result.append(value)
                if len(result) >= limit:
                    return result
    return result


def _payload_from_states(states) -> dict[str, Any]:
    errors = [state.api_error for state in states if state.api_error]
    counts = Counter(str(state.status or "UNKNOWN") for state in states)
    total = sum(counts.values())
    if errors and len(errors) == len(states):
        return {
            "ok": False,
            "raw": "UNKNOWN",
            "label": "Честный Знак не вернул статусы кодов",
            "source": "true_api",
            "summary": "; ".join(errors[:3]),
            "sample_size": len(states),
        }
    if len(counts) == 1:
        raw_status = next(iter(counts.keys()))
        return {
            "ok": True,
            "raw": raw_status,
            "label": _translate_status(raw_status),
            "source": "true_api",
            "summary": _format_status_counts(counts),
            "sample_size": len(states),
        }
    if counts.get("INTRODUCED"):
        return {
            "ok": True,
            "raw": "PARTIAL_INTRODUCED",
            "label": f"Частично введен в оборот ({counts.get('INTRODUCED', 0)}/{total})",
            "source": "true_api",
            "summary": _format_status_counts(counts),
            "sample_size": len(states),
        }
    return {
        "ok": True,
        "raw": "MIXED",
        "label": "Смешанный статус кодов",
        "source": "true_api",
        "summary": _format_status_counts(counts),
        "sample_size": len(states),
    }


def main() -> int:
    if len(sys.argv) < 3:
        print(json.dumps({"ok": False, "error": "Not enough arguments"}, ensure_ascii=False))
        return 1

    csv_path = sys.argv[1]
    limit = int(sys.argv[2] or "25")
    try:
        codes = _read_codes_sample(csv_path, limit)
        if not codes:
            print(json.dumps({"ok": False, "error": "No codes found"}, ensure_ascii=False))
            return 1

        thumbprint = find_certificate_thumbprint()
        if not thumbprint:
            print(json.dumps({"ok": False, "error": "Certificate thumbprint not found"}, ensure_ascii=False))
            return 1

        cert = find_certificate_by_thumbprint(thumbprint)
        if not cert:
            print(json.dumps({"ok": False, "error": "Certificate not found"}, ensure_ascii=False))
            return 1

        service = BulkAggregationService()
        product_group = service._resolve_true_product_group(os.getenv("PRODUCT_GROUP", "wheelChairs"))  # type: ignore[attr-defined]
        states = service.fetch_code_states(
            cert=cert,
            sign_text_func=sign_text_data,
            product_group=product_group,
            raw_codes=codes,
        )
        if not states:
            print(json.dumps({"ok": False, "error": "No states returned"}, ensure_ascii=False))
            return 1

        print(json.dumps(_payload_from_states(states), ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Any

import bartender_label_100x180 as base_labels
import pandas as pd


AGGREGATION_SOURCE_KIND = base_labels.AGGREGATION_SOURCE_KIND
MARKING_SOURCE_KIND = base_labels.MARKING_SOURCE_KIND
DEFAULT_LABEL_SHEET_FORMAT = "100x180"

_REPO_ROOT = Path(__file__).resolve().parent.parent
_FORMAT_CONFIG = {
    "100x180": {
        "key": "100x180",
        "label": "100x180",
        "root_dir": _REPO_ROOT / "BarTender наклейки 100х180",
        "hr_group_name": "Латекс, Нитрил, HR",
    },
    "100x136": {
        "key": "100x136",
        "label": "100x136",
        "root_dir": _REPO_ROOT / "BarTender наклейки 100х136",
        "hr_group_name": "латекс, нитрил, HR",
    },
}
_FORMAT_CONFIG_LOCK = Lock()


def normalize_label_sheet_format(sheet_format: Any) -> str:
    normalized = str(sheet_format or "").strip().lower()
    if normalized in _FORMAT_CONFIG:
        return normalized
    return DEFAULT_LABEL_SHEET_FORMAT


def list_label_sheet_formats() -> list[dict[str, str]]:
    return [
        {
            "key": config["key"],
            "label": config["label"],
        }
        for config in _FORMAT_CONFIG.values()
    ]


def list_label_templates(sheet_format: Any) -> list[base_labels.LabelTemplateInfo]:
    with _patched_template_config(sheet_format):
        return base_labels.list_100x180_templates()


def build_label_print_context(
    *,
    sheet_format: Any,
    df: pd.DataFrame,
    order_data: dict[str, Any],
    template_path: str,
    aggregation_csv_path: str,
    printer_name: str,
    manufacture_date: str,
    expiration_date: str,
    quantity_value: str | int | None = None,
):
    with _patched_template_config(sheet_format):
        return base_labels.build_label_print_context(
            df=df,
            order_data=order_data,
            template_path=template_path,
            aggregation_csv_path=aggregation_csv_path,
            printer_name=printer_name,
            manufacture_date=manufacture_date,
            expiration_date=expiration_date,
            quantity_value=quantity_value,
        )


def print_label_sheet(context) -> None:
    base_labels.print_100x180_labels(context)


def list_aggregation_csv_files():
    return base_labels.list_aggregation_csv_files()


def list_marking_csv_files():
    return base_labels.list_marking_csv_files()


def resolve_order_metadata(order_data: dict[str, Any], df: pd.DataFrame):
    return base_labels.resolve_order_metadata(order_data, df)


def format_label_sheet_title(sheet_format: Any) -> str:
    config = _get_format_config(sheet_format)
    return str(config["label"])


@contextmanager
def _patched_template_config(sheet_format: Any):
    config = _get_format_config(sheet_format)
    with _FORMAT_CONFIG_LOCK:
        previous_root_dir = base_labels.TEMPLATE_ROOT_DIR
        previous_hr_dir = base_labels.HR_TEMPLATE_GROUP_DIR
        base_labels.TEMPLATE_ROOT_DIR = Path(config["root_dir"])
        base_labels.HR_TEMPLATE_GROUP_DIR = base_labels.TEMPLATE_ROOT_DIR / str(config["hr_group_name"])
        try:
            yield config
        finally:
            base_labels.TEMPLATE_ROOT_DIR = previous_root_dir
            base_labels.HR_TEMPLATE_GROUP_DIR = previous_hr_dir


def _get_format_config(sheet_format: Any) -> dict[str, Any]:
    normalized = normalize_label_sheet_format(sheet_format)
    return _FORMAT_CONFIG[normalized]

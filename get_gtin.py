import re

import pandas as pd

from logger import logger


GTIN_COLUMN = "GTIN"
FULL_NAME_COLUMN = "Полное наименование товара"
SIMPLIFIED_COLUMN = "Упрощенно"
SIZE_COLUMN = "Размер"
UNITS_COLUMN = "Количество единиц употребления в потребительской упаковке"
COLOR_COLUMN = "Цвет"
VENCHIK_COLUMN = "венчик"


def _normalize_units_value(value) -> str:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    return digits or raw


def _normalize_string_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.lower()


def _extract_size_from_table(size_value) -> str:
    if not isinstance(size_value, str):
        return ""

    size_text = size_value.lower().strip()
    match = re.search(r"\(([A-Z]+)\)", size_value.upper())
    if match:
        return match.group(1).lower()

    if "сверхбольшой" in size_text or "xl" in size_text:
        return "xl"
    if "большой" in size_text or size_text == "l":
        return "l"
    if "средний" in size_text or size_text == "m":
        return "m"
    if "маленький" in size_text or size_text == "s":
        return "s"

    numeric_match = re.search(r"(\d+[.,]?\d*)", size_text)
    if numeric_match:
        return numeric_match.group(1).replace(",", ".")

    return size_text


def _normalize_input_size(size_value: str) -> str:
    size_text = str(size_value or "").strip().lower()
    size_mapping = {
        "s": "s",
        "маленький": "s",
        "m": "m",
        "средний": "m",
        "l": "l",
        "большой": "l",
        "xl": "xl",
        "сверхбольшой": "xl",
    }

    if size_text in size_mapping:
        return size_mapping[size_text]

    numeric_match = re.search(r"(\d+[.,]?\d*)", size_text)
    if numeric_match:
        return numeric_match.group(1).replace(",", ".")

    return size_text


def _ensure_lookup_columns(df: pd.DataFrame) -> None:
    for column in (
        GTIN_COLUMN,
        FULL_NAME_COLUMN,
        SIMPLIFIED_COLUMN,
        SIZE_COLUMN,
        UNITS_COLUMN,
        COLOR_COLUMN,
        VENCHIK_COLUMN,
    ):
        if column not in df.columns:
            df[column] = ""


def lookup_gtin(
    df: pd.DataFrame,
    simpl_name: str,
    size: str,
    units_per_pack: str,
    color: str | None = None,
    venchik: str | None = None,
) -> tuple[str | None, str | None]:
    """
    Поиск GTIN и полного наименования по заданным параметрам.
    Возвращает (gtin, full_name) или (None, None), если не найдено.
    """
    try:
        _ensure_lookup_columns(df)

        simpl = str(simpl_name or "").strip().lower()
        normalized_size = _normalize_input_size(size)
        units_value = _normalize_units_value(units_per_pack)
        color_value = str(color or "").strip().lower()
        venchik_value = str(venchik or "").strip().lower()

        df["normalized_size"] = df[SIZE_COLUMN].apply(_extract_size_from_table)

        simpl_series = _normalize_string_series(df[SIMPLIFIED_COLUMN])
        units_series = df[UNITS_COLUMN].map(_normalize_units_value)
        color_series = _normalize_string_series(df[COLOR_COLUMN])
        venchik_series = _normalize_string_series(df[VENCHIK_COLUMN])

        exact_condition = (
            (simpl_series == simpl)
            & (df["normalized_size"] == normalized_size)
            & (units_series == units_value)
        )
        if venchik_value:
            exact_condition &= venchik_series == venchik_value
        if color_value:
            exact_condition &= color_series == color_value

        exact_matches = df[exact_condition]
        if not exact_matches.empty:
            row = exact_matches.iloc[0]
            return str(row[GTIN_COLUMN]).strip(), str(row[FULL_NAME_COLUMN]).strip()

        partial_condition = (
            simpl_series.str.contains(simpl, na=False, regex=False)
            & (df["normalized_size"] == normalized_size)
            & (units_series == units_value)
        )
        if venchik_value:
            partial_condition &= venchik_series == venchik_value
        if color_value:
            partial_condition &= color_series == color_value

        partial_matches = df[partial_condition]
        if not partial_matches.empty:
            row = partial_matches.iloc[0]
            return str(row[GTIN_COLUMN]).strip(), str(row[FULL_NAME_COLUMN]).strip()

        logger.debug(
            "Не найдено совпадений для: simpl=%s, size=%s, units=%s, color=%s, venchik=%s",
            simpl,
            normalized_size,
            units_value,
            color_value or "-",
            venchik_value or "-",
        )
        available_sizes = df[simpl_series == simpl]["normalized_size"].unique()
        logger.debug("Доступные размеры для %s: %s", simpl, list(available_sizes))
    except Exception:
        logger.exception("Ошибка в lookup_gtin")

    return None, None


def lookup_by_gtin(df: pd.DataFrame, gtin: str) -> tuple[str | None, str | None]:
    """
    Поиск товара по GTIN.
    Возвращает кортеж (Полное наименование, Упрощенное имя) или (None, None), если не найдено.
    """
    try:
        gtin_str = str(gtin).strip()
        if GTIN_COLUMN not in df.columns:
            logger.warning("В DataFrame нет колонки 'GTIN'")
            return None, None

        match = df[df[GTIN_COLUMN].astype(str).str.strip() == gtin_str]
        if not match.empty:
            row = match.iloc[0]
            full_name = str(row.get(FULL_NAME_COLUMN, "")).strip()
            simpl_name = str(row.get(SIMPLIFIED_COLUMN, "")).strip()
            return full_name, simpl_name
    except Exception:
        logger.exception("Ошибка в lookup_by_gtin для GTIN=%s", gtin)

    return None, None

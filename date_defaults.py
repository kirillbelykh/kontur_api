from __future__ import annotations

from datetime import date, datetime
from typing import Tuple


def _shift_month(year: int, month: int, delta_months: int) -> tuple[int, int]:
    month_index = (year * 12 + (month - 1)) + delta_months
    shifted_year, shifted_month_index = divmod(month_index, 12)
    return shifted_year, shifted_month_index + 1


def get_default_production_window(reference: date | datetime | None = None) -> Tuple[str, str]:
    """
    Возвращает окно дат по бизнес-правилу:
    - дата производства: первое число месяца, который завершает 3-месячный интервал с текущим месяцем включительно;
    - дата окончания: та же дата, но через 5 лет.

    Пример: 13.03.2026 -> 01.01.2026 / 01.01.2031
    """
    if reference is None:
        base_date = date.today()
    elif isinstance(reference, datetime):
        base_date = reference.date()
    else:
        base_date = reference

    production_year, production_month = _shift_month(base_date.year, base_date.month, -2)
    production_date = date(production_year, production_month, 1)
    expiration_date = date(production_year + 5, production_month, 1)

    return (
        production_date.strftime("%d-%m-%Y"),
        expiration_date.strftime("%d-%m-%Y"),
    )

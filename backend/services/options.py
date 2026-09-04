# ==== Опции выбора ====
# Имена в simplified_options должны совпадать с колонкой «Упрощенно»
# в data/nomenclature.xlsx (с учётом регистра вроде «латекс HR»).

simplified_options = [
    "стер латекс 1-хлор", "стер латекс", "стер латекс 2-хлор", "стер нитрил",
    "хир", "хир 1-хлор", "хир с полимерным", "хир 2-хлор", "хир изопрен",
    "хир нитрил", "ультра", "гинекология", "двойная пара", "микрохирургия",
    "ортопедия", "латекс диаг гладкие", "латекс диаг", "латекс 2-хлор",
    "латекс с полимерным", "латекс удлиненный", "латекс анатомической",
    "латекс HR", "латекс 1-хлор", "нитрил диаг", "нитрил диаг HR короткий",
    "нитрил диаг HR удлиненный",
]

# Товары, у которых в номенклатуре несколько цветов — цвет обязателен.
# Сравнение с выбранным именем всегда через product_requires_color().
color_required = [
    "латекс 1-хлор",
    "латекс 2-хлор",
    "латекс HR",
    "латекс анатомической",
    "латекс диаг",
    "латекс с полимерным",
    "латекс удлиненный",
    "нитрил диаг",
    "нитрил диаг HR короткий",
    "нитрил диаг HR удлиненный",
    "ультра",
]

venchik_required = [
    "гинекология", "микрохирургия", "ортопедия",
]

color_options = ["белый", "зеленый", "натуральный", "розовый", "синий", "фиолетовый", "черный"]
venchik_options = ["с венчиком", "без венчика"]

size_options = [
    "XS", "S", "M", "L", "XL", "5,0", "5,5", "6,0", "6,5",
    "7,0", "7,5", "8,0", "8,5", "9,0", "9,5", "10,0",
]

units_options = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 25, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 125, 250, 500,
]


def _normalized_name(value: str | None) -> str:
    return str(value or "").strip().lower()


_COLOR_REQUIRED_SET = {_normalized_name(item) for item in color_required}
_VENCHIK_REQUIRED_SET = {_normalized_name(item) for item in venchik_required}


def product_requires_color(name: str | None) -> bool:
    return _normalized_name(name) in _COLOR_REQUIRED_SET


def product_requires_venchik(name: str | None) -> bool:
    return _normalized_name(name) in _VENCHIK_REQUIRED_SET

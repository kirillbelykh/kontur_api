"""Сделать углы иконки прозрачными.

Исходник — чёрный скруглённый квадрат с белой «K» на СПЛОШНОМ белом фоне,
поэтому на тёмном рабочем столе видны белые уголки. Заливаем внешний белый
фон прозрачностью flood-fill'ом от четырёх углов: белая «K» в центре окружена
чёрной рамкой и заливкой не затрагивается.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageFilter

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE = REPO_ROOT / "assets" / "icons" / "icon.png"

PNG_TARGETS = [
    REPO_ROOT / "assets" / "icons" / "icon.png",
    REPO_ROOT / "installer" / "payload" / "assets" / "icons" / "icon.png",
]
ICO_TARGETS = [
    REPO_ROOT / "assets" / "icons" / "kontur.ico",
    REPO_ROOT / "installer" / "payload" / "assets" / "icons" / "icon.ico",
]
ICO_SIZES = [(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)]

MARKER = (255, 0, 255)
FILL_THRESHOLD = 110  # белый фон = 254; чёрная рамка (~0) заливку останавливает


def build_transparent_icon(source: Path) -> Image.Image:
    original = Image.open(source).convert("RGBA")
    width, height = original.size

    # Помечаем внешний белый фон уникальным цветом от каждого угла
    probe = original.convert("RGB")
    for corner in [(0, 0), (width - 1, 0), (0, height - 1), (width - 1, height - 1)]:
        ImageDraw.floodfill(probe, corner, MARKER, thresh=FILL_THRESHOLD)

    # alpha=0 там, где пиксель совпал с маркером (внешний фон), иначе 255
    marker_band = Image.new("RGB", original.size, MARKER)
    diff = ImageChops.difference(probe, marker_band).convert("L")
    alpha = diff.point(lambda value: 255 if value > 0 else 0)

    # Мягкое сглаживание скруглённого края (сплошную заливку внутри не трогает)
    alpha = alpha.filter(ImageFilter.GaussianBlur(radius=0.8))

    result = original.copy()
    result.putalpha(alpha)
    return result


def main() -> None:
    icon = build_transparent_icon(SOURCE)

    for target in PNG_TARGETS:
        target.parent.mkdir(parents=True, exist_ok=True)
        icon.save(target, format="PNG")
        print(f"PNG  -> {target}")

    for target in ICO_TARGETS:
        target.parent.mkdir(parents=True, exist_ok=True)
        icon.save(target, format="ICO", sizes=ICO_SIZES)
        print(f"ICO  -> {target}")


if __name__ == "__main__":
    main()

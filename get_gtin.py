import logging
import pandas as pd

# -----------------------------
# logging (минимальные сообщения в терминал, подробности в файл)
# -----------------------------
LOG_FILE = "lookup.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -----------------------------
# Lookup GTIN in nomenclature.xlsx
# -----------------------------
def lookup_gtin(
    df: pd.DataFrame,
    simpl_name: str,
    size: str,
    units_per_pack: str,
    color: str | None = None,
    venchik: str | None = None
) -> tuple[str | None, str | None]:
    """
    Поиск GTIN и полного наименования по заданным полям.
    Возвращает (gtin, full_name) или (None, None), если не найдено.
    """
    try:
        simpl = simpl_name.strip().lower()
        size_l = str(size).strip().lower()
        units_str = str(units_per_pack).strip()
        color_l = color.strip().lower() if color else None
        venchik_l = venchik.strip().lower() if venchik else None

        # Гарантия наличия нужных колонок
        required_cols = [
            'GTIN',
            'Полное наименование товара',
            'Упрощенно',
            'Размер',
            'Количество единиц употребления в потребительской упаковке',
            'Цвет',
            'венчик'
        ]
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""

        # --- Точный поиск ---
        cond = (
            df['Упрощенно'].astype(str).str.strip().str.lower() == simpl
        ) & (
            df['Размер'].astype(str).str.strip().str.lower().str.contains(size_l, na=False)
        ) & (
            df['Количество единиц употребления в потребительской упаковке'].astype(str).str.strip() == units_str
        )

        if venchik_l:
            cond &= df['венчик'].astype(str).str.strip().str.lower() == venchik_l
        if color_l:
            cond &= df['Цвет'].astype(str).str.strip().str.lower() == color_l

        matches = df[cond]
        if not matches.empty:
            row = matches.iloc[0]
            return (
                str(row['GTIN']).strip(),
                str(row['Полное наименование товара']).strip()
            )

        # --- Частичный поиск ---
        cond2 = (
            df['Упрощенно'].astype(str).str.strip().str.lower().str.contains(simpl, na=False)
        ) & (
            df['Размер'].astype(str).str.strip().str.lower().str.contains(size_l, na=False)
        )
        if venchik_l:
            cond2 &= df['венчик'].astype(str).str.strip().str.lower() == venchik_l
        if color_l:
            cond2 &= df['Цвет'].astype(str).str.strip().str.lower() == color_l

        matches2 = df[cond2]
        if not matches2.empty:
            row = matches2.iloc[0]
            return (
                str(row['GTIN']).strip(),
                str(row['Полное наименование товара']).strip()
            )

    except Exception as e:
        logging.exception("Ошибка в lookup_gtin")

    # если ничего не нашли
    return None, None


# -----------------------------
# Lookup by GTIN
# -----------------------------
def lookup_by_gtin(df: pd.DataFrame, gtin: str) -> tuple[str | None, str | None]:
    """
    Поиск товара по GTIN.
    Возвращает кортеж (Полное наименование, Упрощенное имя) или (None, None), если не найдено.
    """
    try:
        gtin_str = str(gtin).strip()
        if 'GTIN' not in df.columns:
            logging.warning("В DataFrame нет колонки 'GTIN'")
            return None, None

        match = df[df['GTIN'].astype(str).str.strip() == gtin_str]
        if not match.empty:
            row = match.iloc[0]
            full_name = str(row.get('Полное наименование товара', '')).strip()
            simpl_name = str(row.get('Упрощенно', '')).strip()
            return full_name, simpl_name

    except Exception as e:
        logging.exception(f"Ошибка в lookup_by_gtin для GTIN={gtin}")

    return None, None
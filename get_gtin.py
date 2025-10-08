from logger import logger
import pandas as pd


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
        size_input = str(size).strip().lower()
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

        # Функция для нормализации размера из таблицы
        def extract_size_from_table(size_str):
            """Извлекает размер в формате S/M/L/XL из строки типа 'СВЕРХБОЛЬШОЙ (XL)'"""
            if not isinstance(size_str, str):
                return ""
            
            # Ищем содержимое в скобках
            import re
            match = re.search(r'\(([A-Z]+)\)', size_str.upper())
            if match:
                return match.group(1).lower()
            
            # Если скобок нет, пытаемся определить по ключевым словам
            size_str_lower = size_str.lower()
            if "сверхбольшой" in size_str_lower or "xl" in size_str_lower:
                return "xl"
            elif "большой" in size_str_lower or "l" in size_str_lower:
                return "l"
            elif "средний" in size_str_lower or "m" in size_str_lower:
                return "m"
            elif "маленький" in size_str_lower or "s" in size_str_lower:
                return "s"
            
            return size_str_lower

        # Нормализуем размеры в DataFrame
        df['normalized_size'] = df['Размер'].apply(extract_size_from_table)
        
        # Нормализуем входной размер
        size_mapping = {
            's': 's', 'маленький': 's',
            'm': 'm', 'средний': 'm',
            'l': 'l', 'большой': 'l', 
            'xl': 'xl', 'сверхбольшой': 'xl'
        }
        normalized_input_size = size_mapping.get(size_input, size_input)

        # --- Точный поиск ---
        cond = (
            df['Упрощенно'].astype(str).str.strip().str.lower() == simpl
        ) & (
            df['normalized_size'] == normalized_input_size
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

        # --- Частичный поиск (только по simpl_name) ---
        cond2 = (
            df['Упрощенно'].astype(str).str.strip().str.lower().str.contains(simpl, na=False)
        ) & (
            df['normalized_size'] == normalized_input_size
        ) & (
            df['Количество единиц употребления в потребительской упаковке'].astype(str).str.strip() == units_str
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

        # Логируем для отладки
        logger.debug(f"Не найдено совпадений для: simpl={simpl}, size={normalized_input_size}, units={units_str}")
        available_sizes = df[df['Упрощенно'].str.lower() == simpl]['normalized_size'].unique()
        logger.debug(f"Доступные размеры для {simpl}: {list(available_sizes)}")

    except Exception as e:
        logger.exception("Ошибка в lookup_gtin")

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
            logger.warning("В DataFrame нет колонки 'GTIN'")
            return None, None

        match = df[df['GTIN'].astype(str).str.strip() == gtin_str]
        if not match.empty:
            row = match.iloc[0]
            full_name = str(row.get('Полное наименование товара', '')).strip()
            simpl_name = str(row.get('Упрощенно', '')).strip()
            return full_name, simpl_name

    except Exception as e:
        logger.exception(f"Ошибка в lookup_by_gtin для GTIN={gtin}")

    return None, None
import os
import logging
import json
import copy
import uuid
import pandas as pd
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple
from get_gtin import lookup_gtin
from api import make_session_with_cookies, try_single_post, create_order_multistep, load_cookies


# Константы (фиксированные для всех заказов)
WAREHOUSE_ID = "59739360-7d62-434b-ad13-4617c87a6d13"
PRODUCT_GROUP = "wheelChairs"
RELEASE_METHOD_TYPE = "production"
CIS_TYPE = "unit"
FILLING_METHOD = "productsCatalog"

# -----------------------------
# Data container
# -----------------------------
@dataclass
class OrderItem:
    order_name: str         # Заявка № или текст для "Заказ кодов №"
    simpl_name: str         # Упрощенно
    size: str               # Размер
    units_per_pack: str     # Количество единиц в упаковке (строка, для поиска)
    codes_count: int        # Количество кодов для заказа
    gtin: str = ""          # найдём перед запуском воркеров
    full_name: str = ""     # опционально: полное наименование из справочника
    tnved_code: str = ""

# helper prints only for prompts / summary
def ui_print(msg: str):
    print(msg)

    
# Настройка логгирования (можешь убрать / настроить путь)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ==== Опции выбора ====
simplified_options = [
    "стер лат 1-хлор", "стер лат", "стер лат 2-хлор", "стер нитрил",
    "хир", "хир 1-хлор", "хир с полимерным", "хир 2-хлор", "хир изопрен",
    "хир нитрил", "ультра", "гинекология", "двойная пара", "микрохирургия",
    "ортопедия", "латекс диаг гладкие", "латекс диаг", "латекс 2-хлор",
    "латекс с полимерным", "латекс удлиненный", "латекс анатомической",
    "латекс hr", "латекс 1-хлор", "нитрил диаг", "нитрил диаг hr короткий",
    "нитрил диаг hr удлиненный"
]

color_required = [
    "латекс 1-хлор", "латекс 2-хлор", "латекс HR", "латекс анатомической",
    "латекс диаг", "латекс диаг гладкие", "латекс с полимерным",
    "латекс удлиненный", "нитрил диаг", "нитрил диаг HR короткий",
    "нитрил диаг HR удлиненный", "стер лат 1-хлор", "стер лат 2-хлор",
    "ультра"
]

venchik_required = [
    "гинекология", "микрохирургия", "ортопедия"
]

color_options = ["белый", "зеленый", "натуральный", "розовый", "синий", "фиолетовый", "черный"]
venchik_options = ["с венчиком", "без венчика"]

size_options = [
    "XS", "S", "M", "L", "XL", "5,0", "5,5", "6,0", "6,5",
    "7,0", "7,5", "8,0", "8,5", "9,0", "9,5", "10,0"
]

units_options = [1,2,3,4,5,6,7,8,9,10,20,25,30,40,50,60,70,80,90,100,110,120,125,250,500]

def choose_option(options: List, prompt: str):
    print(f"\n{prompt}:")
    for i, option in enumerate(options, start=1):
        print(f"{i}. {option}")
    while True:
        choice = input("Введите номер: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(options):
            return options[int(choice)-1]
        print("Неверный выбор. Попробуйте снова.")


def print_collected(collected: List[OrderItem]):
    if not collected:
        print("\n--- Накопленные позиции: пусто ---\n")
        return
    print("\n--- Накопленные позиции ---")
    for idx, it in enumerate(collected, start=1):
        uid = getattr(it, "_uid", "no-uid")
        print(f"{idx}. uid={uid} | {it.simpl_name} | {it.size} | {it.units_per_pack} уп. | GTIN {it.gtin} | к-во: {it.codes_count} | заявка: '{it.order_name}'")
    print("---------------------------\n")


def choose_delete_index(collected: List[OrderItem]) -> Optional[int]:
    """
    Пользователь может ввести индекс позиции (1-based) или 'uid:<id>'.
    Если пустая строка — отменяем удаление.
    """
    if not collected:
        ui_print("Нет позиций для удаления.")
        return None

    print_collected(collected)
    inp = input("Введите номер позиции для удаления или 'uid:<id>' (пусто = отмена): ").strip()
    
    if inp == "":
        ui_print("Удаление отменено.")
        return None

    if inp.lower().startswith("uid:"):
        uid_to_remove = inp.split("uid:", 1)[1].strip()
        for i, it in enumerate(collected):
            if getattr(it, "_uid", None) == uid_to_remove:
                return i
        ui_print("UID не найден.")
        return None

    if not inp.isdigit():
        ui_print("Неверный ввод.")
        return None

    idx = int(inp) - 1
    if idx < 0 or idx >= len(collected):
        ui_print("Индекс вне диапазона.")
        return None

    return idx


def safe_perform(it) -> Tuple[bool, str]:
    """
    API-обёртка для OrderItem.
    Берёт order_name от пользователя и подставляет как номер заявки.
    """
    try:
        payload = asdict(it)
        payload["_uid"] = getattr(it, "_uid", None)

        # order_name = то, что ввёл пользователь в терминале
        document_number = payload.get("order_name") or "NO_NAME"

        # собираем список позиций
        positions = [{
            "gtin": payload.get("gtin"),
            "name": payload.get("full_name") or payload.get("simpl_name") or "",
            "tnvedCode": payload.get("tnved_code", ""),
            "quantity": payload.get("codes_count", 1),
        }]

        # cookies → session
        cookies = load_cookies()
        if not cookies:
            # try import get_cookies from get_cookies
            try:
                from cookies import get_cookies as external_collect  # type: ignore
                print("Calling get_cookiesesin cookies...")
                cookies = external_collect()
            except Exception as e:
                print("Cannot import/call get_cookies module:", e)
                print("Either run get_cookies.py manually or fix import.")
                return

        if not cookies:
            print("Cookies not obtained; aborting.")
            return
        session = make_session_with_cookies(cookies)

        # --- пробуем быстрый POST ---
        resp = try_single_post(
            session,
            WAREHOUSE_ID,
            document_number,
            PRODUCT_GROUP,
            RELEASE_METHOD_TYPE,
            positions,
            cis_type=CIS_TYPE,
            filling_method=FILLING_METHOD,
        )
        if resp is not None and resp.status_code in (200, 201):
            return True, f"Успех: {resp.text}"

        # --- fallback: многошаговый сценарий ---
        res = create_order_multistep(
            session,
            WAREHOUSE_ID,
            document_number,
            PRODUCT_GROUP,
            RELEASE_METHOD_TYPE,
            positions,
            cis_type=CIS_TYPE,
            filling_method=FILLING_METHOD,
        )
        return True, f"Многошаговый успех: {res}"

    except Exception as e:
        logging.exception("Ошибка при API-вызове вместо Selenium")
        return False, f"Exception: {e}"

    

def main():
    NOMENCLATURE_XLSX = "data/nomenclature.xlsx"
    if not os.path.exists(NOMENCLATURE_XLSX):
        ui_print(f"ERROR: файл {NOMENCLATURE_XLSX} не найден.")
        return

    df = pd.read_excel(NOMENCLATURE_XLSX)
    df.columns = df.columns.str.strip()

    ui_print("=== Kontur Automation — ввод позиций ===")
    collected: List[OrderItem] = []

    while True:
        print("\nПоиск по GTIN?")
        print("1. Да")
        print("2. Нет")
        gtin_choice = input("Выбор (1/2): ").strip()

        if gtin_choice == "1":
            order_name = input("Заявка (текст, будет вставлен в 'Заказ кодов №'): ").strip()
            if not order_name:
                ui_print("Нужно ввести заявку.")
                continue
            gtin_input = input("Введите GTIN: ").strip()
            if not gtin_input:
                ui_print("GTIN пустой — отмена.")
                continue
            try:
                codes_count = int(input("Количество кодов (целое): ").strip())
            except:
                ui_print("Неверно введено количество кодов. Попробуй ещё раз.")
                continue
            
            # tnved для ручного ввода по GTIN — берём "по умолчанию"
            tnved_code = "4015120009"

            it = OrderItem(
                order_name=order_name,
                simpl_name="по GTIN",
                size="не указано",
                units_per_pack="не указано",
                codes_count=codes_count,
                gtin=gtin_input,
                full_name="",
                tnved_code=tnved_code
            )
            setattr(it, "_uid", uuid.uuid4().hex)
            collected.append(it)
            ui_print(f"Добавлено по GTIN: {gtin_input} — {codes_count} кодов — заявка '{order_name}'")
            print_collected(collected)
        
        elif gtin_choice == "2":
            order_name = input("\nЗаявка (текст, будет вставлен в 'Заказ кодов №'): ").strip()
            if not order_name:
                ui_print("Нужно ввести заявку.")
                continue

            simpl = choose_option(simplified_options, "Выберите вид товара")
            color = None
            if simpl.lower() in [c.lower() for c in color_required]:
                color = choose_option(color_options, "Выберите цвет")
            venchik = None
            if simpl.lower() in [c.lower() for c in venchik_required]:
                venchik = choose_option(venchik_options, "С венчиком/без венчика?")
            size = choose_option(size_options, "Выберите размер")
            units = choose_option(units_options, "Выберите количество единиц в упаковке")

            try:
                codes_count = int(input("Количество кодов (целое): ").strip())
            except:
                ui_print("Неверно введено количество кодов. Попробуй ещё раз.")
                continue

            gtin, full_name = lookup_gtin(df, simpl, size, units, color, venchik)
            if not gtin:
                ui_print(f"GTIN не найден для ({simpl}, {size}, {units}, {color}, {venchik}) — позиция не добавлена.")
                continue

            # Определяем ТНВЭД по упрощённому названию
            simpl_lower = simpl.lower()
            if any(word in simpl_lower for word in ["хир", "микро", "ультра", "гинек", "дв пара"]):
                tnved_code = "4015120001"
            else:
                tnved_code = "4015120009"

            it = OrderItem(
                order_name=order_name,
                simpl_name=simpl,
                size=size,
                units_per_pack=units,
                codes_count=codes_count,
                gtin=gtin,
                full_name=full_name or "",
                tnved_code=tnved_code
            )
            setattr(it, "_uid", uuid.uuid4().hex)
            collected.append(it)
            ui_print(
                f"Добавлено: {simpl} ({size}, {units} уп., {color or 'без цвета'}) — "
                f"GTIN {gtin} — {codes_count} кодов — ТНВЭД {tnved_code} — заявка '{order_name}'"
            )
            print_collected(collected)

        else:
            ui_print("Неверный выбор — попробуйте снова.")
            continue


        # меню действий
        while True:
            print("\nДействия:")
            print("1 - Ввести ещё позицию")
            print("2 - Удалить позицию (по индексу или uid:... )")
            print("3 - Показать накопленные позиции")
            print("4 - Выполнить все накопленные позиции")
            print("0 - Выйти без выполнения")
            action = input("Выбор (0/1/2/3/4): ").strip()
            if action == "1":
                break
            elif action == "2":
                idx = choose_delete_index(collected)
                if idx is None:
                    continue
                removed = collected.pop(idx)
                ui_print(f"Удалена позиция #{idx+1}: uid={getattr(removed,'_uid',None)} | {removed.simpl_name} — GTIN {removed.gtin}")
                print_collected(collected)
            elif action == "3":
                print_collected(collected)
            elif action == "4":
                # подтверждение + snapshot
                print_collected(collected)
                confirm = input(f"Подтвердите выполнение {len(collected)} задач(и)? (y/n): ").strip().lower()
                if confirm != "y":
                    ui_print("Выполнение отменено пользователем.")
                    continue

                # делаем жёсткую глубокую копию коллекции (snapshot)
                to_process = copy.deepcopy(collected)

                # сохраняем snapshot на диск для дебага (включаем _uid в дамп)
                try:
                    snapshot = []
                    for x in to_process:
                        d = asdict(x)
                        d["_uid"] = getattr(x, "_uid", None)
                        snapshot.append(d)
                    with open("last_snapshot.json", "w", encoding="utf-8") as f:
                        json.dump(snapshot, f, ensure_ascii=False, indent=2)
                    logging.info("Saved last_snapshot.json (snapshot of to_process).")
                except Exception:
                    logging.exception("Не удалось сохранить last_snapshot.json")

                # контроль того, что snapshot действительно сформирован
                if not to_process:
                    ui_print("Нет накопленных позиций — выходим.")
                    return

                # перед запуском проверим, что в snapshot нет позиций, которые были удалены (защитный лог)
                current_uids = {getattr(x, "_uid", None) for x in collected}
                snapshot_uids = [getattr(x, "_uid", None) for x in to_process]
                # если какие-то UID отсутствуют — логируем (но всё равно запускаем snapshot)
                missing = [u for u in snapshot_uids if u not in current_uids]
                if missing:
                    logging.warning(f"В snapshot есть UID'ы, которых нет в текущем collected: {missing}")
                    # это маловероятно при deepcopy, но логируем для диагностики

                ui_print(f"\nБудет выполнено {len(to_process)} задач(и) ПОСЛЕДОВАТЕЛЬНО.")
                ui_print("Запуск...")
                results = []
                success_count = 0
                fail_count = 0
                for it in to_process:
                    uid = getattr(it, "_uid", None)
                    ui_print(f"Запуск позиции uid={uid}: {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}'")
                    ok, msg = safe_perform(it)
                    results.append((ok, msg, it))
                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                    ui_print(f"[{'OK' if ok else 'ERR'}] uid={uid} {it.simpl_name} — {msg}")
                
                ui_print("\n=== Выполнение завершено ===")
                ui_print(f"Успешно: {success_count}, Ошибок: {fail_count}.")

                # подробный отчёт
                if any(not r[0] for r in results):
                    print("\nНеудачные позиции:")
                    for ok, msg, it in results:
                        if not ok:
                            print(f" - uid={getattr(it,'_uid',None)} | {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}' => {msg}")


                # Оставляем collected как есть (так безопаснее); при желании можно удалить успешно выполненные позиции
                return
            elif action == "0":
                ui_print("Выход без выполнения.")
                return
            else:
                ui_print("Неверный выбор. Попробуйте снова.")


if __name__ == "__main__":
    main()

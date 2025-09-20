import os
import logging
import json
import copy
import uuid
import pandas as pd
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple
from get_gtin import lookup_gtin
from api import make_session_with_cookies, try_single_post, load_cookies
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk
import sys

# Константы (фиксированные для всех заказов)
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
    cisType: str = ""

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

# Настройка логгирования (можешь убрать / настроить путь)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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
            "tnvedCode": payload.get("tnved_code"),
            "quantity": payload.get("codes_count", 1),
            "cisType": payload.get("cisType")
        }]

        # cookies → session
        cookies = load_cookies()
        if not cookies:
            try:
                from cookies import get_cookies as external_collect  # type: ignore
                print("Calling get_cookies in cookies...")
                cookies = external_collect()
            except Exception as e:
                print("Cannot import/call get_cookies module:", e)
                return False, f"Cannot get cookies: {e}"

        if not cookies:
            print("Cookies not obtained; aborting.")
            return False, "Cookies not obtained"

        session = make_session_with_cookies(cookies)

        # --- пробуем быстрый POST ---
        resp = try_single_post(
            session,
            document_number,
            PRODUCT_GROUP,
            RELEASE_METHOD_TYPE,
            positions,
            filling_method=FILLING_METHOD,
            thumbprint="08f40b694898598b3922b69277b79fd2c84d9c85"
        )

        if not resp:
            return False, "No response from API"

        # проверка дублирования: если documentId уже есть, не создаём новую заявку
        document_id = resp.get("documentId") or resp.get("id")  # зависит от API
        status = resp.get("status") or "unknown"

        print("[OK] ФИНАЛЬНЫЙ СТАТУС ДОКУМЕНТА:", status)
        return True, f"Document {document_number} processed, status: {status}, id: {document_id}"

    except Exception as e:
        logging.exception("Ошибка при API-вызове вместо Selenium")
        return False, f"Exception: {e}"

class App(ctk.CTk):
    def __init__(self, df):
        super().__init__()
        self.title("Kontur Automation")
        self.geometry("800x800")
        self.df = df
        self.collected: List[OrderItem] = []

        # Input frame
        input_frame = ctk.CTkFrame(self)
        input_frame.pack(pady=10, padx=10, fill="x")

        ctk.CTkLabel(input_frame, text="Заявка (текст для 'Заказ кодов №'):").grid(row=0, column=0, pady=5, padx=5, sticky="w")
        self.order_entry = ctk.CTkEntry(input_frame, width=400)
        self.order_entry.grid(row=0, column=1, pady=5, padx=5)

        self.gtin_var = ctk.StringVar(value="No")
        ctk.CTkRadioButton(input_frame, text="Поиск по GTIN", variable=self.gtin_var, value="Yes", command=self.toggle_mode).grid(row=1, column=0, pady=5, padx=5)
        ctk.CTkRadioButton(input_frame, text="Выбор опций", variable=self.gtin_var, value="No", command=self.toggle_mode).grid(row=1, column=1, pady=5, padx=5)

        # GTIN frame
        self.gtin_frame = ctk.CTkFrame(input_frame)
        ctk.CTkLabel(self.gtin_frame, text="GTIN:").grid(row=0, column=0, pady=5, padx=5, sticky="w")
        self.gtin_entry = ctk.CTkEntry(self.gtin_frame, width=400)
        self.gtin_entry.grid(row=0, column=1, pady=5, padx=5)

        # Select frame
        self.select_frame = ctk.CTkFrame(input_frame)
        ctk.CTkLabel(self.select_frame, text="Вид товара:").grid(row=0, column=0, pady=5, padx=5, sticky="w")
        self.simpl_combo = ctk.CTkComboBox(self.select_frame, values=simplified_options, command=self.update_options, width=400)
        self.simpl_combo.grid(row=0, column=1, pady=5, padx=5)

        self.color_label = ctk.CTkLabel(self.select_frame, text="Цвет:")
        self.color_combo = ctk.CTkComboBox(self.select_frame, values=color_options, width=400)

        self.venchik_label = ctk.CTkLabel(self.select_frame, text="С венчиком/без венчика?")
        self.venchik_combo = ctk.CTkComboBox(self.select_frame, values=venchik_options, width=400)

        ctk.CTkLabel(self.select_frame, text="Размер:").grid(row=3, column=0, pady=5, padx=5, sticky="w")
        self.size_combo = ctk.CTkComboBox(self.select_frame, values=size_options, width=400)
        self.size_combo.grid(row=3, column=1, pady=5, padx=5)

        ctk.CTkLabel(self.select_frame, text="Количество единиц в упаковке:").grid(row=4, column=0, pady=5, padx=5, sticky="w")
        self.units_combo = ctk.CTkComboBox(self.select_frame, values=[str(u) for u in units_options], width=400)
        self.units_combo.grid(row=4, column=1, pady=5, padx=5)

        # Codes count (common)
        ctk.CTkLabel(input_frame, text="Количество кодов:").grid(row=2, column=0, pady=5, padx=5, sticky="w")
        self.codes_entry = ctk.CTkEntry(input_frame, width=400)
        self.codes_entry.grid(row=2, column=1, pady=5, padx=5)

        # Add button
        add_btn = ctk.CTkButton(input_frame, text="Добавить позицию", command=self.add_item)
        add_btn.grid(row=5, column=0, columnspan=2, pady=10)

        # Initial mode
        self.toggle_mode()

        # Treeview
        columns = ("idx", "uid", "simpl_name", "size", "units_per_pack", "gtin", "codes_count", "order_name")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=10)
        self.tree.heading("idx", text="#")
        self.tree.heading("uid", text="UID")
        self.tree.heading("simpl_name", text="Упрощенно")
        self.tree.heading("size", text="Размер")
        self.tree.heading("units_per_pack", text="Упаковка")
        self.tree.heading("gtin", text="GTIN")
        self.tree.heading("codes_count", text="Кодов")
        self.tree.heading("order_name", text="Заявка")
        self.tree.pack(pady=10, padx=10, fill="both", expand=True)

        # Buttons frame
        btn_frame = ctk.CTkFrame(self)
        btn_frame.pack(pady=10, fill="x")

        delete_btn = ctk.CTkButton(btn_frame, text="Удалить позицию", command=self.delete_item)
        delete_btn.pack(side="left", padx=10)

        execute_btn = ctk.CTkButton(btn_frame, text="Выполнить все", command=self.execute_all)
        execute_btn.pack(side="left", padx=10)

        exit_btn = ctk.CTkButton(btn_frame, text="Выйти", command=self.quit)
        exit_btn.pack(side="left", padx=10)

        # Log textbox
        self.log_text = ctk.CTkTextbox(self, height=150)
        self.log_text.pack(pady=10, padx=10, fill="x")

        # Style Treeview for dark mode
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#2b2b2b", fieldbackground="#2b2b2b", foreground="white")
        style.configure("Treeview.Heading", background="#3a3a3a", foreground="white")
        style.map("Treeview", background=[("selected", "#1f6aa5")])

    def toggle_mode(self):
        if self.gtin_var.get() == "Yes":
            self.select_frame.grid_forget()
            self.gtin_frame.grid(row=3, column=0, columnspan=2, pady=5, padx=5)
        else:
            self.gtin_frame.grid_forget()
            self.select_frame.grid(row=3, column=0, columnspan=2, pady=5, padx=5)
            self.update_options()

    def update_options(self, value=None):
        simpl = self.simpl_combo.get().lower()
        if simpl in [c.lower() for c in color_required]:
            self.color_label.grid(row=1, column=0, pady=5, padx=5, sticky="w")
            self.color_combo.grid(row=1, column=1, pady=5, padx=5)
        else:
            self.color_label.grid_forget()
            self.color_combo.grid_forget()

        if simpl in [c.lower() for c in venchik_required]:
            self.venchik_label.grid(row=2, column=0, pady=5, padx=5, sticky="w")
            self.venchik_combo.grid(row=2, column=1, pady=5, padx=5)
        else:
            self.venchik_label.grid_forget()
            self.venchik_combo.grid_forget()

    def add_item(self):
        order_name = self.order_entry.get().strip()
        if not order_name:
            self.log_insert("Нужно ввести заявку.")
            return

        try:
            codes_count = int(self.codes_entry.get().strip())
        except ValueError:
            self.log_insert("Неверно введено количество кодов. Попробуй ещё раз.")
            return

        if self.gtin_var.get() == "Yes":
            gtin_input = self.gtin_entry.get().strip()
            if not gtin_input:
                self.log_insert("GTIN пустой — отмена.")
                return
            tnved_code = "4015120009"
            it = OrderItem(
                order_name=order_name,
                simpl_name="по GTIN",
                size="не указано",
                units_per_pack="не указано",
                codes_count=codes_count,
                gtin=gtin_input,
                full_name="",
                tnved_code=tnved_code,
                cisType=CIS_TYPE
            )
            self.log_insert(f"Добавлено по GTIN: {gtin_input} — {codes_count} кодов — заявка '{order_name}'")
        else:
            simpl = self.simpl_combo.get()
            color = self.color_combo.get() if self.color_combo.winfo_viewable() else None
            venchik = self.venchik_combo.get() if self.venchik_combo.winfo_viewable() else None
            size = self.size_combo.get()
            units = self.units_combo.get()

            if not all([simpl, size, units]):
                self.log_insert("Заполните все обязательные поля.")
                return

            gtin, full_name = lookup_gtin(self.df, simpl, size, units, color, venchik)
            if not gtin:
                self.log_insert(f"GTIN не найден для ({simpl}, {size}, {units}, {color}, {venchik}) — позиция не добавлена.")
                return

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
                tnved_code=tnved_code,
                cisType=CIS_TYPE
            )
            self.log_insert(
                f"Добавлено: {simpl} ({size}, {units} уп., {color or 'без цвета'}) — "
                f"GTIN {gtin} — {codes_count} кодов — ТНВЭД {tnved_code} — заявка '{order_name}'"
            )

        setattr(it, "_uid", uuid.uuid4().hex)
        self.collected.append(it)
        self.update_tree()

    def update_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for idx, it in enumerate(self.collected, start=1):
            self.tree.insert("", "end", values=(
                idx, getattr(it, "_uid", "no-uid"), it.simpl_name, it.size, it.units_per_pack,
                it.gtin, it.codes_count, it.order_name
            ))

    def delete_item(self):
        selected = self.tree.selection()
        if not selected:
            self.log_insert("Нет выбранной позиции для удаления.")
            return
        idx = self.tree.index(selected[0])
        removed = self.collected.pop(idx)
        self.log_insert(f"Удалена позиция: uid={getattr(removed, '_uid', None)} | {removed.simpl_name} — GTIN {removed.gtin}")
        self.update_tree()

    def execute_all(self):
        if not self.collected:
            self.log_insert("Нет накопленных позиций.")
            return

        confirm = tk.messagebox.askyesno("Подтверждение", f"Подтвердите выполнение {len(self.collected)} задач(и)?")
        if not confirm:
            self.log_insert("Выполнение отменено пользователем.")
            return

        to_process = copy.deepcopy(self.collected)

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

        self.log_insert(f"\nБудет выполнено {len(to_process)} задач(и) ПОСЛЕДОВАТЕЛЬНО.")
        self.log_insert("Запуск...")
        results = []
        success_count = 0
        fail_count = 0
        for it in to_process:
            uid = getattr(it, "_uid", None)
            self.log_insert(f"Запуск позиции uid={uid}: {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}'")
            ok, msg = safe_perform(it)
            results.append((ok, msg, it))
            if ok:
                success_count += 1
            else:
                fail_count += 1
            self.log_insert(f"[{'OK' if ok else 'ERR'}] uid={uid} {it.simpl_name} — {msg}")

        self.log_insert("\n=== Выполнение завершено ===")
        self.log_insert(f"Успешно: {success_count}, Ошибок: {fail_count}.")

        if any(not r[0] for r in results):
            self.log_insert("\nНеудачные позиции:")
            for ok, msg, it in results:
                if not ok:
                    self.log_insert(f" - uid={getattr(it,'_uid',None)} | {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}' => {msg}")

    def log_insert(self, msg: str):
        self.log_text.insert("end", f"{msg}\n")
        self.log_text.see("end")

if __name__ == "__main__":
    NOMENCLATURE_XLSX = "data/nomenclature.xlsx"
    if not os.path.exists(NOMENCLATURE_XLSX):
        print(f"ERROR: файл {NOMENCLATURE_XLSX} не найден.")
    else:
        df = pd.read_excel(NOMENCLATURE_XLSX)
        df.columns = df.columns.str.strip()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        app = App(df)
        app.mainloop()
import os
import copy
import uuid
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
import queue
import time
from datetime import datetime, timedelta
from logger import logger
import pandas as pd
from dataclasses import dataclass, asdict
from typing import List, Tuple, Dict, Any
from get_gtin import lookup_gtin, lookup_by_gtin
from api import codes_order, download_codes, make_task_on_tsd
from cookies import get_valid_cookies
from utils import make_session_with_cookies, get_tnved_code, save_snapshot, save_order_history
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk
from dotenv import load_dotenv
from options import (
    simplified_options, color_required, venchik_required,
    color_options, venchik_options, size_options, units_options
)

load_dotenv()

# Константы 
BASE = os.getenv("BASE_URL")
PRODUCT_GROUP = os.getenv("PRODUCT_GROUP")
RELEASE_METHOD_TYPE = os.getenv("RELEASE_METHOD_TYPE")
CIS_TYPE = os.getenv("CIS_TYPE")  
FILLING_METHOD = os.getenv("FILLING_METHOD")  
THUMBPRINT = os.getenv("THUMBPRINT")
NOMENCLATURE_XLSX = "data/nomenclature.xlsx"

# -----------------------------
# Data container
# -----------------------------
@dataclass
class OrderItem:
    order_name: str         # Заявка № или текст для "Заказ кодов"
    simpl_name: str         # Упрощенно
    size: str               # Размер
    units_per_pack: str     # Количество единиц в упаковке (строка, для поиска)
    codes_count: int        # Количество кодов для заказа
    gtin: str = ""          # найдём перед запуском воркеров
    full_name: str = ""     # опционально: полное наименование из справочника
    tnved_code: str = ""    # Тнвэд-код
    cisType: str = ""       # тип кода (CIS_TYPE из .env)

def make_order_to_kontur(it) -> Tuple[bool, str]:
    """
    API-обёртка для OrderItem.
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
        cookies = None
        try:
            logger.info("Получаем cookies...")
            cookies = get_valid_cookies()
        except Exception as e:
            logger.error("Ошибка при получении cookies:", e)
            return False, f"Cannot get cookies: {e}"

        if not cookies:
            logger.info("Cookies не получены; прерываем выполнение.")
            return False, "Cookies not obtained"

        session = make_session_with_cookies(cookies)

        # --- пробуем быстрый POST ---
        resp = codes_order(
            session,
            str(document_number),
            str(PRODUCT_GROUP),
            str(RELEASE_METHOD_TYPE),
            positions,
            filling_method=str(FILLING_METHOD),
            thumbprint=str(THUMBPRINT)
        )

        if not resp:
            return False, "No response from API"

        # проверка дублирования: если documentId уже есть, не создаём новую заявку
        document_id = resp.get("documentId") or resp.get("id")  # зависит от API
        status = resp.get("status") or "unknown"

        logger.info("ФИНАЛЬНЫЙ СТАТУС ДОКУМЕНТА:", status)
        return True, f"Document {document_number} processed, status: {status}, id: {document_id}"

    except Exception as e:
        return False, f"Exception: {e}"

class App(ctk.CTk):
    def __init__(self, df):
        super().__init__()
        self.title("Kontur Marking")
        self.geometry("800x700")
        self.df = df
        self.collected: List[OrderItem] = []
        self.download_list: List[dict] = []  # [{'document_id': str, 'status': str, 'filename': str or None, 'order_name': str}]
        
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

        #THREADING
        self.auto_download_queue = queue.Queue()
        self.auto_download_thread = None
        self.stop_auto_download = False
        self.download_workers = []
        self.max_workers = 3  # Максимальное количество одновременных скачиваний
        # Executor для фоновой обработки
        self.intro_executor = ThreadPoolExecutor(max_workers=2)  # Меньше потоков для стабильности
        
        # Tabview for sections
        self.tabview = ctk.CTkTabview(self)
        self.tabview.pack(pady=10, padx=10, fill="both", expand=True)

        # Tab 1: Создание заказов
        tab_create = self.tabview.add("Создание заказов")

        # Input frame
        input_frame = ctk.CTkFrame(tab_create)
        input_frame.pack(pady=10, padx=10, fill="x")

        ctk.CTkLabel(input_frame, text="Заявка №:").grid(row=0, column=0, pady=5, padx=5, sticky="w")
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

        # Добавляем поддержку вставки/копирования через правый клик, сочетания клавиш и русскую раскладку
        self._add_entry_context_menu(self.gtin_entry)

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

        # Codes count (common) - перемещено вниз
        ctk.CTkLabel(input_frame, text="Количество кодов:").grid(row=5, column=0, pady=5, padx=5, sticky="w")
        self.codes_entry = ctk.CTkEntry(input_frame, width=400)
        self.codes_entry.grid(row=5, column=1, pady=5, padx=5)

        # Add button - теперь под полем "Количество кодов"
        add_btn = ctk.CTkButton(input_frame, text="Добавить позицию", command=self.add_item)
        add_btn.grid(row=6, column=0, columnspan=2, pady=10)

        # Initial mode
        self.toggle_mode()

        # Treeview for orders
        columns = ("idx",  "full_name", "simpl_name", "size", "units_per_pack", "gtin", "codes_count", "order_name", "uid")
        self.tree = ttk.Treeview(tab_create, columns=columns, show="headings", height=10)
        self.tree.heading("idx", text="Порядковый номер")
        self.tree.heading("full_name", text="Наименование")
        self.tree.heading("simpl_name", text="Упрощенно")
        self.tree.heading("size", text="Размер")
        self.tree.heading("units_per_pack", text="Упаковка")
        self.tree.heading("gtin", text="GTIN")
        self.tree.heading("codes_count", text="Кодов")
        self.tree.heading("order_name", text="Заявка")
        self.tree.heading("uid", text="UID")
        self.tree.pack(pady=10, padx=10, fill="both", expand=True)

        # Buttons frame for create tab
        btn_frame = ctk.CTkFrame(tab_create)
        btn_frame.pack(pady=10, fill="x")

        delete_btn = ctk.CTkButton(btn_frame, text="Удалить позицию", command=self.delete_item)
        delete_btn.pack(side="left", padx=10)

        execute_btn = ctk.CTkButton(btn_frame, text="Выполнить все", command=self.execute_all)
        execute_btn.pack(side="left", padx=10)
        
        clear_btn = ctk.CTkButton(btn_frame, text="Очистить", command=self.clear_all)
        clear_btn.pack(side="left", padx=10)

        # Log textbox for create tab
        self.log_text = ctk.CTkTextbox(tab_create, height=150)
        self.log_text.pack(pady=10, padx=10, fill="x")

        # Ограничение доступа только для чтения/копирования
        self.log_text.configure(state="disabled")  # Блокирует редактирование

        # Добавляем контекстное меню для копирования
        self.log_text.bind("<Button-3>", self._show_log_context_menu)  # Правая кнопка мыши

        # Разрешаем стандартные сочетания клавиш для копирования
        self.log_text.bind("<Control-c>", lambda e: self._copy_log_text())
        self.log_text.bind("<Control-C>", lambda e: self._copy_log_text())
    
        # Style Treeview for dark mode
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#2b2b2b", fieldbackground="#2b2b2b", foreground="white")
        style.configure("Treeview.Heading", background="#3a3a3a", foreground="white")
        style.map("Treeview", background=[("selected", "#1f6aa5")])

        # Tab 2: Скачивание кодов
        tab_download = self.tabview.add("Скачивание кодов")

        # Treeview for downloads
        download_columns = ("order_name", "status", "filename", "document_id")
        self.download_tree = ttk.Treeview(tab_download, columns=download_columns, show="headings", height=10)
        self.download_tree.heading("order_name", text="Заявка")
        self.download_tree.heading("status", text="Статус")
        self.download_tree.heading("filename", text="Файл")
        self.download_tree.heading("document_id", text="ID заказа")
        self.download_tree.pack(pady=10, padx=10, fill="both", expand=True)

        # Buttons for download tab
        download_btn_frame = ctk.CTkFrame(tab_download)
        download_btn_frame.pack(pady=10, fill="x")


        # Log textbox for download tab
        self.download_log_text = ctk.CTkTextbox(tab_download, height=150)
        self.download_log_text.pack(pady=10, padx=10, fill="x")

        # Initial update
        self.update_download_tree()
        # Запускаем автоматическое скачивание при старте
        self.start_auto_download()

        self.setup_introduction_tab()
        self.setup_introduction_tsd_tab()


    def _add_entry_context_menu(self, entry: ctk.CTkEntry):
        """Добавляет контекстное меню (правый клик) и обработку вставки через клавиши для поля entry.

        Исправляет проблему, когда в русской раскладке Ctrl+C/Ctrl+V не срабатывают — обрабатываем
        комбинации по символам как в латинской, так и в кириллической раскладках, а также альтернативные
        сочетания (Shift-Insert, Ctrl-Insert, Shift-Delete).
        """
        menu = tk.Menu(self, tearoff=0)

        def _paste(event=None):
            try:
                clip = self.clipboard_get()
            except Exception:
                return "break"
            try:
                # Если что-то выделено — заменяем
                try:
                    sel_first = entry.index("sel.first")
                    sel_last = entry.index("sel.last")
                    entry.delete(sel_first, sel_last)
                except Exception:
                    pass
                entry.insert("insert", clip)
            except Exception:
                pass
            return "break"

        def _copy(event=None):
            try:
                sel = entry.selection_get()
                self.clipboard_clear()
                self.clipboard_append(sel)
            except Exception:
                pass
            return "break"

        def _cut(event=None):
            try:
                sel_first = entry.index("sel.first")
                sel_last = entry.index("sel.last")
                sel = entry.get()[sel_first:sel_last]
                self.clipboard_clear()
                self.clipboard_append(sel)
                entry.delete(sel_first, sel_last)
            except Exception:
                pass
            return "break"

        def _select_all(event=None):
            try:
                entry.select_range(0, 'end')
                entry.icursor('end')
            except Exception:
                pass
            return "break"

        menu.add_command(label="Вставить", command=_paste)
        menu.add_command(label="Копировать", command=_copy)
        menu.add_command(label="Вырезать", command=_cut)
        menu.add_separator()
        menu.add_command(label="Выделить всё", command=_select_all)

        # Правый клик (Button-3) для большинства ОС
        def _show_menu(event):
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()
            return "break"

        entry.bind('<Button-3>', _show_menu)
        # Поддержка для macOS (Control-Button-1) и некоторых окружений
        entry.bind('<Control-Button-1>', _show_menu)

        # Обработка комбинаций клавиш: учитываем как латинские, так и кириллические буквы
        # mapping: c -> с, v -> м, x -> ч, a -> ф (русская раскладка)
        paste_keys = {'v', 'м'}
        copy_keys = {'c', 'с'}
        cut_keys = {'x', 'ч'}
        select_keys = {'a', 'ф'}

        def _on_ctrl_key(event):
            key = ''
            try:
                key = (event.keysym or '').lower()
            except Exception:
                pass
            # event.char иногда содержит символ, попробуем и его
            if not key:
                try:
                    key = (event.char or '').lower()
                except Exception:
                    key = ''

            if key in paste_keys:
                return _paste(event)
            if key in copy_keys:
                return _copy(event)
            if key in cut_keys:
                return _cut(event)
            if key in select_keys:
                return _select_all(event)
            # не обработали — вернуть None, чтобы прочие сочетания работали как обычно
            return None

        # Привязываем унифицированный обработчик для Ctrl+Key и Command+Key (mac)
        entry.bind('<Control-Key>', _on_ctrl_key)
        entry.bind('<Control-KeyRelease>', lambda e: 'break')
        entry.bind('<Command-Key>', _on_ctrl_key)

        # Альтернативные сочетания
        entry.bind('<Shift-Insert>', _paste)
        entry.bind('<Control-Insert>', _copy)
        entry.bind('<Shift-Delete>', _cut)

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
            full_name, simpl = lookup_by_gtin(self.df, gtin_input)
            tnved_code = get_tnved_code(simpl or "")
            if not simpl:
                self.log_insert(f"GTIN {gtin_input} не найден в справочнике — позиция не добавлена.")
                return
            it = OrderItem(
                order_name=order_name,
                simpl_name=simpl,
                size="не указано",
                units_per_pack="не указано",
                codes_count=codes_count,
                gtin=gtin_input,
                full_name=full_name or "",
                tnved_code=tnved_code,
                cisType=str(CIS_TYPE)
            )
            self.log_insert(f"✅Добавлено по GTIN: {gtin_input} — {codes_count} кодов — заявка № {order_name}")
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

            tnved_code = get_tnved_code(simpl)

            it = OrderItem(
                order_name=order_name,
                simpl_name=simpl,
                size=size,
                units_per_pack=units,
                codes_count=codes_count,
                gtin=gtin,
                full_name=full_name or "",
                tnved_code=tnved_code,
                cisType=str(CIS_TYPE)
            )
            self.log_insert(
                f"✅Добавлено: {simpl} ({size}, {units} уп., {color or 'без цвета'}) — "
                f"GTIN {gtin} — {codes_count} кодов — ТНВЭД {tnved_code} — заявка № {order_name}"
            )

        setattr(it, "_uid", uuid.uuid4().hex)
        self.collected.append(it)
        self.update_tree()

    def update_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        for idx, it in enumerate(self.collected, start=1):
            self.tree.insert("", "end", values=(
                idx, it.full_name, it.simpl_name, it.size, it.units_per_pack,   
                it.gtin, it.codes_count, it.order_name, getattr(it, "_uid", "no-uid")
            ))

    def delete_item(self):
        selected = self.tree.selection()
        if not selected:
            self.log_insert("Нет выбранной позиции для удаления.")
            return
        idx = self.tree.index(selected[0])
        removed = self.collected.pop(idx)
        self.log_insert(f"Удалена позиция: {removed.simpl_name} — GTIN {removed.gtin}")
        self.update_tree()

    def clear_all(self):
        """Очищает все данные: список заказов, дерево и поля ввода"""
        try:
            # Очищаем список собранных позиций
            self.collected.clear()
            
            # Очищаем дерево заказов
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # Очищаем поле ввода заявки
            self.order_entry.delete(0, "end")
            
            # Очищаем поле ввода GTIN (если активно)
            if hasattr(self, 'gtin_entry'):
                self.gtin_entry.delete(0, "end")
            
            # Сбрасываем комбо-боксы к значениям по умолчанию
            if hasattr(self, 'simpl_combo'):
                self.simpl_combo.set("")
            
            if hasattr(self, 'color_combo'):
                self.color_combo.set("")
            
            if hasattr(self, 'venchik_combo'):
                self.venchik_combo.set("")
            
            if hasattr(self, 'size_combo'):
                self.size_combo.set("")
            
            if hasattr(self, 'units_combo'):
                self.units_combo.set("")
            
            # Очищаем поле количества кодов
            if hasattr(self, 'codes_entry'):
                self.codes_entry.delete(0, "end")
            
            # Очищаем лог (опционально)
            self.log_text.configure(state="normal")
            self.log_text.delete("1.0", "end")
            self.log_text.configure(state="disabled")
            
            # Выводим сообщение об успешной очистке
            self.log_insert("Все данные успешно очищены")
            
        except Exception as e:
            self.log_insert(f"Ошибка при очистке данных: {e}")


    def execute_all(self):
        if not self.collected:
            self.log_insert("Нет накопленных позиций.")
            return

        confirm = tk.messagebox.askyesno("Подтверждение", f"Подтвердите выполнение {len(self.collected)} задач(и)?")
        if not confirm:
            self.log_insert("Выполнение отменено пользователем.")
            return

        to_process = copy.deepcopy(self.collected)

        save_snapshot(to_process)
        save_order_history(to_process)
        
        self.log_insert(f"\nБудет выполнено {len(to_process)} заказов.")
        self.log_insert("Запуск...")
        results = []
        success_count = 0
        fail_count = 0
        for it in to_process:
            uid = getattr(it, "_uid", None)
            self.log_insert(f"Запуск позиции: {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}'")
            ok, msg = make_order_to_kontur(it)
            results.append((ok, msg, it))
            if ok:
                success_count += 1
                # Parse document_id from msg (assuming format "Document ... id: {id}")
                try:
                    document_id = msg.split("id: ")[1].strip()
                    self.download_list.append({
                        'order_name': it.order_name,
                        'document_id': document_id,
                        'status': 'Ожидает',
                        'filename': None,
                        'simpl': it.simpl_name
                    })
                    self.update_download_tree()
                except:
                    self.log_insert(f"Не удалось извлечь document_id из: {msg}")
            else:
                fail_count += 1

        self._start_auto_download_for_new_orders()

        self.log_insert("\n=== Выполнение завершено ===")
        self.log_insert(f"Успешно: {success_count}, Ошибок: {fail_count}.")

        if any(not r[0] for r in results):
            self.log_insert("\nНеудачные позиции:")
            for ok, msg, it in results:
                if not ok:
                    self.log_insert(f" - uid={getattr(it,'_uid',None)} | {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}' => {msg}")


    def _reset_input_fields(self):
        """Сбрасывает поля ввода к значениям по умолчанию"""
        try:
            # Сбрасываем комбо-боксы
            comboboxes = ['simpl_combo', 'color_combo', 'venchik_combo', 'size_combo', 'units_combo']
            for combo_name in comboboxes:
                if hasattr(self, combo_name):
                    getattr(self, combo_name).set("")
            
            # Можно также очистить поле заявки, если нужно
            # self.order_entry.delete(0, "end")
            
        except Exception as e:
            print(f"Ошибка при сбросе полей ввода: {e}")

    def _start_auto_download_for_new_orders(self):
        """Добавляет новые заказы в систему автоматического скачивания"""
        for item in self.download_list:
            if item['status'] == 'Ожидает':
                # Можно сразу проверить статус или подождать следующей итерации worker'а
                pass


    def start_auto_download(self):
        """Запускает систему автоматического скачивания"""
        self.stop_auto_download = False
        
        # Поток для обработки очереди заказов
        self.auto_download_thread = threading.Thread(target=self._auto_download_worker, daemon=True)
        self.auto_download_thread.start()
        
        # Потоки-воркеры для скачивания
        for i in range(self.max_workers):
            worker = threading.Thread(target=self._download_worker, daemon=True, args=(i,))
            worker.start()
            self.download_workers.append(worker)
        
        self.download_log_insert("Автоматическое скачивание запущено")

    def stop_auto_download_system(self):
        """Останавливает систему автоматического скачивания"""
        self.stop_auto_download = True
        self.download_log_insert("Автоматическое скачивание остановлено")

    def _auto_download_worker(self):
        """Фоновый worker для проверки статусов заказов"""
        while not self.stop_auto_download:
            try:
                # Проверяем заказы каждые 30 секунд
                time.sleep(30)
                
                if not self.download_list:
                    continue
                    
                # Получаем cookies для сессии
                try:
                    cookies = get_valid_cookies()
                    if not cookies:
                        continue
                    session = make_session_with_cookies(cookies)
                except Exception as e:
                    self.after(0, lambda: self.download_log_insert(f"Ошибка получения cookies: {e}"))
                    continue
                
                # Проверяем статусы заказов
                for item in self.download_list:
                    if self.stop_auto_download:
                        break
                        
                    if item['status'] == 'Ожидает':
                        document_id = item['document_id']
                        
                        # Проверяем статус заказа
                        try:
                            status = self._check_order_status(session, document_id)
                            if status == 'released':
                                # Добавляем в очередь на скачивание
                                self.auto_download_queue.put(item)
                                # Обновляем статус в GUI
                                self.after(0, lambda i=item: self._update_item_status(i, 'В очереди на скачивание'))
                        except Exception as e:
                            self.after(0, lambda e=e: self.download_log_insert(f"Ошибка проверки статуса: {e}"))
                            
            except Exception as e:
                self.after(0, lambda e=e: self.download_log_insert(f"Ошибка в auto_download_worker: {e}"))

    def _download_worker(self, worker_id):
        """Worker для скачивания PDF"""
        while not self.stop_auto_download:
            try:
                # Берем задание из очереди (с таймаутом для graceful shutdown)
                try:
                    item = self.auto_download_queue.get(timeout=5)
                except queue.Empty:
                    continue
                    
                self.after(0, lambda i=item: self._update_item_status(i, 'Скачивается'))
                
                # Скачиваем PDF
                try:
                    cookies = get_valid_cookies()
                    if not cookies:
                        self.after(0, lambda i=item: self._update_item_status(i, 'Ошибка: нет cookies'))
                        continue
                        
                    session = make_session_with_cookies(cookies)
                    filename = download_codes(session, item['document_id'], item['order_name'])
                    
                    if filename:
                        self.after(0, lambda i=item, f=filename: self._finish_download(i, f, 'Скачан'))
                    else:
                        self.after(0, lambda i=item: self._update_item_status(i, 'Ошибка скачивания'))
                        
                except Exception as e:
                    self.after(0, lambda i=item, e=e: self._update_item_status(i, f'Ошибка: {str(e)}'))
                    
                finally:
                    self.auto_download_queue.task_done()
                    
            except Exception as e:
                self.after(0, lambda e=e: self.download_log_insert(f"Ошибка в download_worker {worker_id}: {e}"))

    def _check_order_status(self, session, document_id):
        """Проверяет статус заказа (укороченная версия без ожидания)"""
        try:
            resp_status = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
            resp_status.raise_for_status()
            doc = resp_status.json()
            return doc.get("status", "unknown")
        except Exception as e:
            raise Exception(f"Ошибка проверки статуса {document_id}: {e}")

    def _update_item_status(self, item, new_status):
        """Обновляет статус элемента в основном потоке"""
        item['status'] = new_status
        self.update_download_tree()
        self.download_log_insert(f"Заказ {item['document_id']}: {new_status}")

    def _finish_download(self, item, filename, status):
        """Завершает скачивание и обsновляет интерфейс"""
        item['status'] = status
        item['filename'] = filename
        self.update_download_tree()
        self.download_log_insert(f"Успешно скачан: {filename}")

    def update_download_tree(self):
        for item in self.download_tree.get_children():
            self.download_tree.delete(item)
        for it in self.download_list:
            self.download_tree.insert("", "end", values=(
                it['order_name'], it['document_id'], it['status'], it['filename'] or "-"
            ))

    def log_insert(self, msg: str):
        """Выводит сообщение в лог (с ограничением доступа только для чтения)"""
        try:
            self.log_text.configure(state="normal")
            self.log_text.insert("end", f"{msg}\n")
            self.log_text.see("end")  # Автопрокрутка к новому сообщению
            self.log_text.configure(state="disabled")
        except Exception as e:
            print(f"Ошибка при записи в лог: {e}")

    def _show_log_context_menu(self, event):
        """Показывает контекстное меню для текстового поля лога"""
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="Копировать", command=self._copy_log_text)
        menu.add_command(label="Выделить все", command=self._select_all_log_text)
        menu.add_separator()
        menu.add_command(label="Очистить лог", command=self._clear_log_text)
        
        menu.tk_popup(event.x_root, event.y_root)

    def _copy_log_text(self):
        """Копирует выделенный текст из лога в буфер обмена"""
        try:
            # Временно включаем редактирование для копирования
            self.log_text.configure(state="normal")
            
            # Копируем выделенный текст
            selected_text = self.log_text.get("sel.first", "sel.last")
            if selected_text:
                self.clipboard_clear()
                self.clipboard_append(selected_text)
        except tk.TclError:
            # Если ничего не выделено
            pass
        finally:
            # Возвращаем в режим только для чтения
            self.log_text.configure(state="disabled")

    def _select_all_log_text(self):
        """Выделяет весь текст в логе"""
        try:
            self.log_text.configure(state="normal")
            self.log_text.tag_add("sel", "1.0", "end")
            self.log_text.configure(state="disabled")
        except:
            pass

    def _clear_log_text(self):
        """Очищает содержимое лога"""
        try:
            self.log_text.configure(state="normal")
            self.log_text.delete("1.0", "end")
            self.log_text.configure(state="disabled")
        except:
            pass

    def download_log_insert(self, msg: str):
        self.download_log_text.insert("end", f"{msg}\n")
        self.download_log_text.see("end")

    def on_closing(self):
        """Вызывается при закрытии приложения"""
        self.stop_auto_download_system()
        self.destroy()

    def setup_introduction_tab(self):
        """Создаёт таб 'Ввод в оборот' — вызвать из __init__ после создания tabview."""
        tab_intro = self.tabview.add("Ввод в оборот")
        self.intro_tab = tab_intro

        # Treeview для заказов (берём из download_list те, что имеют файл / скачаны)
        intro_columns = ("order_name", "document_id", "status", "filename")
        self.intro_tree = ttk.Treeview(tab_intro, columns=intro_columns, show="headings", height=10, selectmode="extended")
        self.intro_tree.heading("order_name", text="Заявка")
        self.intro_tree.heading("document_id", text="ID заказа")
        self.intro_tree.heading("status", text="Статус")
        self.intro_tree.heading("filename", text="Файл")
        self.intro_tree.pack(padx=10, pady=10, fill="both", expand=True)

        # Контейнер для полей ввода
        intro_inputs = ctk.CTkFrame(tab_intro)
        intro_inputs.pack(padx=10, pady=5, fill="x")

        # Первая строка
        ctk.CTkLabel(intro_inputs, text="Дата производства (ДД-ММ-ГГГГ):").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.prod_date_entry = ctk.CTkEntry(intro_inputs, width=200, placeholder_text="ДД-ММ-ГГГГ")
        self.prod_date_entry.grid(row=0, column=1, padx=5, pady=5)

        # Вторая строка
        ctk.CTkLabel(intro_inputs, text="Дата окончания (ДД-ММ-ГГГГ):").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.exp_date_entry = ctk.CTkEntry(intro_inputs, width=200, placeholder_text="ДД-ММ-ГГГГ")
        self.exp_date_entry.grid(row=1, column=1, padx=5, pady=5)

        ctk.CTkLabel(intro_inputs, text="Номер партии:").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.batch_entry = ctk.CTkEntry(intro_inputs, width=200)
        self.batch_entry.grid(row=2, column=1, padx=5, pady=5)

        # Заполняем текущей датой по умолчанию в формате ДД-ММ-ГГГГ
        today = datetime.now().strftime("%d-%m-%Y")
        self.prod_date_entry.insert(0, today)

        # Через 2 года как дату окончания по умолчанию в формате ДД-ММ-ГГГГ
        future_date = (datetime.now() + timedelta(days=1826)).strftime("%d-%m-%Y")
        self.exp_date_entry.insert(0, future_date)


        # Кнопки
        btn_frame = ctk.CTkFrame(tab_intro)
        btn_frame.pack(padx=10, pady=5, fill="x")

        self.intro_btn = ctk.CTkButton(btn_frame, text="Ввести в оборот выбранные", command=self.on_introduce_clicked)
        self.intro_btn.pack(side="left", padx=5)

        self.intro_refresh_btn = ctk.CTkButton(btn_frame, text="Обновить список", command=self.update_introduction_tree)
        self.intro_refresh_btn.pack(side="left", padx=5)

        self.intro_clear_btn = ctk.CTkButton(btn_frame, text="Очистить лог", command=self.clear_intro_log)
        self.intro_clear_btn.pack(side="left", padx=5)

        # Лог
        self.intro_log_text = ctk.CTkTextbox(tab_intro, height=150)
        self.intro_log_text.pack(padx=10, pady=10, fill="both", expand=True)
        self.intro_log_text.configure(state="disabled")  # Только для чтения


        # Инициализация отображения
        self.update_introduction_tree()
    
    # Функция для преобразования даты из ДД-ММ-ГГГГ в ГГГГ-ММ-ДД
    def convert_date_format(self, date_str):
        """Преобразует дату из формата ДД-ММ-ГГГГ в ГГГГ-ММ-ДД"""
        try:
            if date_str and len(date_str) == 10 and date_str[2] == '-' and date_str[5] == '-':
                day, month, year = date_str.split('-')
                # Проверяем корректность даты
                datetime(int(year), int(month), int(day))
                return f"{year}-{month}-{day}"
        except (ValueError, IndexError):
            # Если дата некорректна или в другом формате, возвращаем как есть
            pass
        return date_str


    def clear_intro_log(self):
        """Очищает лог ввода в оборот"""
        try:
            self.intro_log_text.configure(state="normal")
            self.intro_log_text.delete("1.0", "end")
            self.intro_log_text.configure(state="disabled")
        except Exception as e:
            print(f"Ошибка очистки лога: {e}")

    def intro_log_insert(self, text: str):
        """Удобная функция логирования в таб 'Ввод' (вызовы только из GUI-потока)."""
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = f"{now} - {text}"
            
            self.intro_log_text.configure(state="normal")
            self.intro_log_text.insert("end", msg + "\n")
            self.intro_log_text.see("end")
            self.intro_log_text.configure(state="disabled")
        except Exception as e:
            print(f"Ошибка записи в лог: {e}")

    def update_introduction_tree(self):
        """Наполнить дерево заказами, у которых status == 'Скачан'"""
        try:
            # Очистить дерево
            for item in self.intro_tree.get_children():
                self.intro_tree.delete(item)
            
            # Добавить записи из self.download_list
            for item in self.download_list:
                # показываем только скачанные заказы
                if item.get("status") == "Скачан" and item.get("document_id"):
                    vals = (
                        item.get("order_name", ""), 
                        item.get("document_id", ""), 
                        item.get("status", ""), 
                        item.get("filename", "")
                    )
                    self.intro_tree.insert("", "end", iid=item.get("document_id"), values=vals)
        except Exception as e:
            self.intro_log_insert(f"Ошибка обновления дерева: {e}")

    def get_selected_intro_items(self):
        """Возвращает список объектов download_list, соответствующих выбранным строкам в intro_tree."""
        try:
            sel = self.intro_tree.selection()
            selected = []
            id_to_item = {it['document_id']: it for it in self.download_list if it.get('document_id')}
            
            for iid in sel:
                it = id_to_item.get(iid)
                if it:
                    selected.append(it)
            return selected
        except Exception as e:
            self.intro_log_insert(f"Ошибка получения выбранных элементов: {e}")
            return []

    def validate_iso_date(self, s: str) -> bool:
        """Проверка формата YYYY-MM-DD."""
        try:
            if not s:
                return False
            datetime.strptime(s, "%Y-%m-%d")
            return True
        except ValueError:
            return False
    def on_introduce_clicked(self):
        """Обработчик кнопки — собирает данные, запускает threads для выбранных заказов."""
        try:
            selected_items = self.get_selected_intro_items()
            if not selected_items:
                self.intro_log_insert("❌ Не выбрано ни одного заказа.")
                return

            # При получении данных используем преобразование:
            prod_date = self.convert_date_format(self.prod_date_entry.get().strip())
            exp_date = self.convert_date_format(self.exp_date_entry.get().strip())
            batch_num = self.batch_entry.get().strip()
            thumbprint = THUMBPRINT

            # Валидация
            errors = []
            
            if not batch_num:
                errors.append("Введите номер партии.")
            if not thumbprint:
                errors.append("Введите отпечаток сертификата.")

            if errors:
                for error in errors:
                    self.intro_log_insert(f"❌ {error}")
                return


            # Отключаем кнопку пока выполняется
            self.intro_btn.configure(state="disabled")
            self.intro_log_insert(f"🚀 Запуск ввода в оборот для {len(selected_items)} заказа(ов)...")
            self.intro_log_insert(f"📅 Дата производства: {prod_date}, Окончание: {exp_date}, Партия: {batch_num}")

            # Запускаем задачи
            futures = []
            for it in selected_items:
                docid = it["document_id"]
                order_name = it.get("order_name", "Unknown")
                simpl_name = it.get("simpl")
                self.intro_log_insert(f"⏳ Добавлен в очередь: {order_name} (ID: {docid})")
                tnved_code = get_tnved_code(simpl_name)
                # Формируем production_patch
                production_patch = {
                    "comment": "",
                    "documentNumber": order_name,
                    "productionType": "ownProduction",
                    "warehouseId": "59739364-7d62-434b-ad13-4617c87a6d13",
                    "expirationType": "milkMoreThan72",
                    "containsUtilisationReport": "true",
                    "usageType": "verified",
                    "cisType": "unit",
                    "fillingMethod": "file",
                    "isAutocompletePositionsDataNeeded": "true",
                    "productsHasSameDates": "true",
                    "isForKegs": "true",
                    "productionDate": prod_date,
                    "expirationDate": exp_date,
                    "batchNumber": batch_num,
                    "TnvedCode": tnved_code
                }
                
                fut = self.intro_executor.submit(self._intro_worker, it, production_patch, thumbprint)
                futures.append((fut, it))

            # Мониторинг завершения
            def monitor():
                completed = 0
                for fut, it in futures:
                    try:
                        ok, msg = fut.result(timeout=600)  # 10 минут таймаут
                        self.after(0, self._on_intro_finished, it, ok, msg)
                        completed += 1
                    except Exception as e:
                        self.after(0, self._on_intro_finished, it, False, f"Таймаут или ошибка: {e}")
                        completed += 1
                
                # Всё завершено - разблокируем кнопку
                self.after(0, lambda: self.intro_btn.configure(state="normal"))
                self.after(0, lambda: self.intro_log_insert(f"✅ Все задачи завершены ({completed}/{len(futures)})"))

            threading.Thread(target=monitor, daemon=True).start()

        except Exception as e:
            self.intro_log_insert(f"❌ Ошибка при запуске ввода в оборот: {e}")
            self.intro_btn.configure(state="normal")
    def _intro_worker(self, item: dict, production_patch: dict, thumbprint: str) -> Tuple[bool, str]:
        """
        Фоновая задача — производит ввод в оборот для одного заказа.
        Возвращает (ok, message).
        """
        document_id = item["document_id"]
        
        try:
            # Получаем cookies/session
            cookies = get_valid_cookies()
            if not cookies:
                return False, "Не удалось получить cookies"
            
            session = make_session_with_cookies(cookies)
            
            # Импортируем функцию из api.py
            from api import put_into_circulation
            
            # Вызываем API функцию
            ok, result = put_into_circulation(
                session=session,
                codes_order_id=document_id,
                production_patch=production_patch,
                organization_id=os.getenv("ORGANIZATION_ID"),
                thumbprint=THUMBPRINT,
                check_poll_interval=10,      # Увеличим интервалы для стабильности
                check_poll_attempts=30,      # Больше попыток
            )
            
            if ok:
                intro_id = result.get("introduction_id", "Unknown")
                return True, f"Успешно. Introduction ID: {intro_id}"
            else:
                errors = result.get("errors", [])
                error_msg = "; ".join(errors) if errors else "Неизвестная ошибка"
                return False, error_msg
                
        except Exception as e:
            return False, f"Исключение: {str(e)}"
        
    def _on_intro_finished(self, item: dict, ok: bool, msg: str):
        """Обновление GUI после завершения одного задания (в главном потоке)."""
        try:
            docid = item.get("document_id")
            order_name = item.get("order_name", "Unknown")
            
            if ok:
                self.intro_log_insert(f"✅ Заявка на ввод в оборот отправлена!")
                item["status"] = "Введен в оборот"
                # Можно также изменить цвет строки или добавить пометку
            else:
                self.intro_log_insert(f"❌ ОШИБКА: {order_name} (ID: {docid}) - {msg}")
                item["status"] = "Ошибка ввода"
            
            # Обновляем отображение
            self.update_introduction_tree()
            self.update_download_tree()  # Если у вас есть этот метод
            
        except Exception as e:
            self.intro_log_insert(f"❌ Ошибка при обработке результата: {e}")

    def setup_introduction_tsd_tab(self):
        """Создаёт таб 'Ввод в оборот (ТСД)'."""
        tab_tsd = self.tabview.add("Ввод в оборот (ТСД)")
        self.tsd_tab = tab_tsd

        # Treeview для заказов (аналогично intro_tree)
        tsd_columns = ("order_name", "document_id", "status", "filename")
        self.tsd_tree = ttk.Treeview(tab_tsd, columns=tsd_columns, show="headings", height=10, selectmode="extended")
        self.tsd_tree.heading("order_name", text="Заявка")
        self.tsd_tree.heading("document_id", text="ID заказа")
        self.tsd_tree.heading("status", text="Статус")
        self.tsd_tree.heading("filename", text="Файл")
        self.tsd_tree.pack(padx=10, pady=10, fill="both", expand=True)

        # Контейнер для полей ввода
        tsd_inputs = ctk.CTkFrame(tab_tsd)
        tsd_inputs.pack(padx=10, pady=5, fill="x")

        # Ровные поля — метки в первом столбце, поля во втором
        ctk.CTkLabel(tsd_inputs, text="Ввод в оборот №:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.tsd_intro_number_entry = ctk.CTkEntry(tsd_inputs, width=200)
        self.tsd_intro_number_entry.grid(row=0, column=1, padx=5, pady=5)

        ctk.CTkLabel(tsd_inputs, text="Дата производства (ДД-ММ-ГГГГ):").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.tsd_prod_date_entry = ctk.CTkEntry(tsd_inputs, width=200)
        self.tsd_prod_date_entry.grid(row=1, column=1, padx=5, pady=5)

        ctk.CTkLabel(tsd_inputs, text="Дата окончания (ДД-ММ-ГГГГ):").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.tsd_exp_date_entry = ctk.CTkEntry(tsd_inputs, width=200)
        self.tsd_exp_date_entry.grid(row=2, column=1, padx=5, pady=5)

        ctk.CTkLabel(tsd_inputs, text="Номер партии:").grid(row=3, column=0, sticky="w", padx=5, pady=5)
        self.tsd_batch_entry = ctk.CTkEntry(tsd_inputs, width=200)
        self.tsd_batch_entry.grid(row=3, column=1, padx=5, pady=5)

        # Кнопки
        btn_frame = ctk.CTkFrame(tab_tsd)
        btn_frame.pack(padx=10, pady=5, fill="x")

        self.tsd_btn = ctk.CTkButton(btn_frame, text="Отправить на ТСД", command=self.on_tsd_clicked)
        self.tsd_btn.pack(side="left", padx=5)

        self.tsd_refresh_btn = ctk.CTkButton(btn_frame, text="Обновить список", command=self.update_tsd_tree)
        self.tsd_refresh_btn.pack(side="left", padx=5)

        # Лог
        self.tsd_log_text = ctk.CTkTextbox(tab_tsd, height=150)
        self.tsd_log_text.pack(padx=10, pady=10, fill="x")

        today = datetime.now().strftime("%d-%m-%Y")
        self.tsd_prod_date_entry.insert(0, today)

        future_date = (datetime.now() + timedelta(days=1826)).strftime("%d-%m-%Y")
        self.tsd_exp_date_entry.insert(0, future_date)

        # Инициализация
        self.update_tsd_tree()

    def tsd_log_insert(self, text: str):
        """Удобная функция логирования в таб 'ТСД' (вызовы только из GUI-потока)."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = f"{now} - {text}\n"
        try:
            self.tsd_log_text.insert("end", msg)
            self.tsd_log_text.see("end")
        except Exception:
            pass

    def update_tsd_tree(self):
        """Наполнить дерево заказами, у которых status == 'Скачан' или filename != None"""
        # Очистить дерево
        for i in self.tsd_tree.get_children():
            self.tsd_tree.delete(i)
        # Добавить записи из self.download_list
        for item in self.download_list:
            if item.get("status") in ("Скачан", "Downloaded", "Ожидает") or item.get("filename"):
                vals = (item.get("order_name"), item.get("document_id"), item.get("status"), item.get("filename") or "")
                self.tsd_tree.insert("", "end", iid=item.get("document_id"), values=vals)

    def get_selected_tsd_items(self):
        """Возвращает список объектов download_list, соответствующих выбранным строкам в tsd_tree."""
        sel = self.tsd_tree.selection()
        selected = []
        id_to_item = {it['document_id']: it for it in self.download_list}
        for iid in sel:
            docid = iid
            it = id_to_item.get(docid)
            if it:
                selected.append(it)
        return selected

    def on_tsd_clicked(self):
        """Обработчик кнопки — собирает данные, запускает threads для выбранных заказов."""
        try:
            self.tsd_log_insert("🔍 Начало обработки нажатия кнопки ТСД...")
            
            # Получаем выбранные элементы
            selected_items = self.get_selected_tsd_items()
            self.tsd_log_insert(f"📋 Выбрано элементов: {len(selected_items)}")
            
            for item in selected_items:
                self.tsd_log_insert(f"   - {item.get('order_name', 'Unknown')} (ID: {item.get('document_id', 'Unknown')})")
            
            if not selected_items:
                self.tsd_log_insert("❌ Не выбрано ни одного заказа.")
                return

            # Получаем данные из полей ввода
            intro_number = self.tsd_intro_number_entry.get().strip()
            prod_date_raw = self.tsd_prod_date_entry.get().strip()
            exp_date_raw = self.tsd_exp_date_entry.get().strip()
            batch_num = self.tsd_batch_entry.get().strip()
            
            
            self.tsd_log_insert(f"📅 Получены данные из полей: into_num='{intro_number}', prod='{prod_date_raw}', exp='{exp_date_raw}', batch='{batch_num}'")

            # Преобразуем даты
            try:
                prod_date = self.convert_date_format(prod_date_raw)
                exp_date = self.convert_date_format(exp_date_raw)
                self.tsd_log_insert(f"📅 Преобразованные даты: prod='{prod_date}', exp='{exp_date}'")
            except Exception as e:
                self.tsd_log_insert(f"❌ Ошибка преобразования дат: {e}")
                return

            # Валидация
            errors = []
            if not intro_number:
                errors.append("Введите номер ввода в оборот.")
            if not batch_num:
                errors.append("Введите номер партии.")
            if not prod_date:
                errors.append("Неверная дата производства.")
            if not exp_date:
                errors.append("Неверная дата окончания срока годности.")

            if errors:
                for error in errors:
                    self.tsd_log_insert(f"❌ {error}")
                return

            # Отключаем кнопку пока выполняется
            self.tsd_btn.configure(state="disabled")
            self.tsd_log_insert("🚀 Запуск создания заданий ТСД...")
            self.tsd_log_insert(f"📊 Будет обработано заказов: {len(selected_items)}")

            # Запускаем задачи
            futures = []
            for it in selected_items:
                try:
                    docid = it["document_id"]
                    self.tsd_log_insert(f"Нашли doc_id для поиска gtin: {docid}")
                    simpl_name = it.get("simpl", "")
                    full_name = it.get("full_name")

                    
                    self.tsd_log_insert(f"⏳ Подготовка заказа: {intro_number} (ID: {docid})")
                    
                    # Получаем GTIN из исходных данных заказа
                    gtin = self._get_gtin_for_order(docid)
                    self.tsd_log_insert(f"   GTIN: {gtin}")
                    
                    if not gtin:
                        self.tsd_log_insert(f"⚠️ Не найден GTIN для заказа {intro_number}, пропускаем")
                        continue
                    
                    # Получаем TNVED код
                    tnved_code = get_tnved_code(simpl_name)
                    self.tsd_log_insert(f"   TNVED: {tnved_code}")
                    
                    # Формируем данные позиций
                    positions_data = [{
                        "name": full_name, 
                        "gtin": f"0{gtin}"
                    }]
                    
                    # Формируем production_patch
                    production_patch = {
                        "documentNumber": intro_number,
                        "productionDate": prod_date,
                        "expirationDate": exp_date,
                        "batchNumber": batch_num,
                        "TnvedCode": tnved_code
                    }
                    
                    self.tsd_log_insert(f"📦 Данные для API: {production_patch}")
                    
                    # Запускаем задачу
                    fut = self.intro_executor.submit(self._tsd_worker, it, positions_data, production_patch, THUMBPRINT)
                    futures.append((fut, it))
                    self.tsd_log_insert(f"✅ Задача для {intro_number} добавлена в очередь")
                    
                except Exception as e:
                    self.tsd_log_insert(f"❌ Ошибка при подготовке заказа {it.get('order_name', 'Unknown')}: {e}")
                    import traceback
                    self.tsd_log_insert(f"🔍 Детали: {traceback.format_exc()}")

            if not futures:
                self.tsd_log_insert("❌ Нет задач для выполнения")
                self.tsd_btn.configure(state="normal")
                return

            # Создаём нитку-отслеживатель
            def monitor():
                try:
                    self.tsd_log_insert("👀 Мониторинг запущен...")
                    completed = 0
                    for fut, it in futures:
                        try:
                            self.tsd_log_insert(f"⏳ Ожидание завершения задачи {completed + 1}/{len(futures)}...")
                            ok, result = fut.result(timeout=300)  # 5 минут таймаут
                            
                            # Формируем сообщение
                            if ok:
                                intro_id = result.get('introduction_id', 'unknown')
                                msg = f"Успех: introduction_id = {intro_id}"
                            else:
                                errors = result.get('errors', ['unknown error'])
                                msg = f"Ошибка: {'; '.join(errors)}"
                            
                            self.after(0, self._on_tsd_finished, it, ok, msg)
                            completed += 1
                            self.tsd_log_insert(f"✅ Задача {completed}/{len(futures)} завершена: {'УСПЕХ' if ok else 'ОШИБКА'}")
                            
                        except Exception as e:
                            error_msg = f"Исключение при выполнении задачи: {e}"
                            self.after(0, self._on_tsd_finished, it, False, error_msg)
                            completed += 1
                            self.tsd_log_insert(f"❌ Задача {completed}/{len(futures)} завершена с ошибкой: {e}")
                            import traceback
                            self.tsd_log_insert(f"🔍 Детали ошибки: {traceback.format_exc()}")
                    
                    self.tsd_log_insert(f"🎉 Все задачи завершены ({completed}/{len(futures)})")
                    
                except Exception as e:
                    self.tsd_log_insert(f"💥 Критическая ошибка в мониторе: {e}")
                    import traceback
                    self.tsd_log_insert(f"🔍 Детали: {traceback.format_exc()}")
                finally:
                    # Всегда разблокируем кнопку
                    self.after(0, lambda: self.tsd_btn.configure(state="normal"))
                    self.after(0, lambda: self.tsd_log_insert("🔓 Кнопка разблокирована"))

            # Запускаем мониторинг в отдельном потоке
            monitor_thread = threading.Thread(target=monitor, daemon=True)
            monitor_thread.start()
            self.tsd_log_insert("📊 Мониторинг задач запущен в фоне")

        except Exception as e:
            self.tsd_log_insert(f"💥 Критическая ошибка в on_tsd_clicked: {e}")
            import traceback
            self.tsd_log_insert(f"🔍 Детали: {traceback.format_exc()}")
            self.tsd_btn.configure(state="normal")

    def _tsd_worker(self, item: dict, positions_data: List[Dict[str, str]], production_patch: dict, thumbprint: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Фоновая задача — производит ввод в оборот для одного заказа item.
        Возвращает (ok, result: dict).
        """
        try:
            self.tsd_log_insert(f"🔧 Начало работы _tsd_worker для {item.get('order_name', 'Unknown')}")
            
            # получаем cookies/session
            try:
                self.tsd_log_insert("🍪 Получение cookies...")
                cookies = get_valid_cookies()
            except Exception as e:
                error_msg = f"Cannot get cookies: {e}"
                self.tsd_log_insert(f"❌ {error_msg}")
                return False, {"errors": [error_msg]}

            if not cookies:
                error_msg = "Cookies not available"
                self.tsd_log_insert(f"❌ {error_msg}")
                return False, {"errors": [error_msg]}

            self.tsd_log_insert("✅ Cookies получены")
            session = make_session_with_cookies(cookies)

            document_id = item["document_id"]
            self.tsd_log_insert(f"📄 Document ID: {document_id}")

            # ВЫЗОВ API
            try:
                self.tsd_log_insert("📡 Вызов API make_task_on_tsd...")
                
                ok, result = make_task_on_tsd(
                    session=session,
                    codes_order_id=document_id,
                    positions_data=positions_data,
                    production_patch=production_patch,
                )
                self.tsd_log_insert(f"📡 Результат API: {'УСПЕХ' if ok else 'ОШИБКА'}")
                return ok, result
                
            except Exception as e:
                error_msg = f"Ошибка при вызове API: {e}"
                self.tsd_log_insert(f"❌ {error_msg}")
                import traceback
                self.tsd_log_insert(f"🔍 Детали API ошибки: {traceback.format_exc()}")
                return False, {"errors": [error_msg]}
                
        except Exception as e:
            error_msg = f"Общая ошибка в _tsd_worker: {e}"
            self.tsd_log_insert(f"❌ {error_msg}")
            import traceback
            self.tsd_log_insert(f"🔍 Детали общей ошибки: {traceback.format_exc()}")
            return False, {"errors": [error_msg]}

    def _on_tsd_finished(self, item: dict, ok: bool, msg: str):
        """Обновление GUI после завершения одного задания (в главном потоке)."""
        docid = item.get("document_id")
        if ok:
            self.tsd_log_insert(f"[OK] {docid} — {msg}")
            # пометим заказ как введённый
            item["status"] = "Отправлено на ТСД"
        else:
            self.tsd_log_insert(f"[ERR] {docid} — {msg}")
            item["status"] = "Ошибка ТСД"

        # обновить таблицы
        self.update_tsd_tree()
        # self.update_download_tree()  # Если есть такая функция для другой таблицы, раскомментируйте
    def _get_gtin_for_order(self, document_id: str) -> str:
        """Получает GTIN для заказа по document_id"""
        try:
            self.tsd_log_insert(f"🔍 Поиск GTIN для document_id: {document_id}")
            
            # Ищем в collected
            for item in self.collected:
                if hasattr(item, '_uid') and item._uid == document_id:
                    gtin = getattr(item, 'gtin', '')
                    self.tsd_log_insert(f"✅ Найден GTIN в collected: {gtin}")
                    return gtin
            
            # Ищем в download_list по связанным данным
            for dl_item in self.download_list:
                if dl_item.get('document_id') == document_id:
                    order_name = dl_item.get('order_name', '')
                    self.tsd_log_insert(f"🔍 Поиск в collected по order_name: {order_name}")
                    
                    # Ищем в collected по order_name
                    for collected_item in self.collected:
                        if getattr(collected_item, 'order_name', '') == order_name:
                            gtin = getattr(collected_item, 'gtin', '')
                            self.tsd_log_insert(f"✅ Найден GTIN по order_name: {gtin}")
                            return gtin
            
            self.tsd_log_insert("❌ GTIN не найден")
            return ""
        except Exception as e:
            self.tsd_log_insert(f"❌ Ошибка при получении GTIN для {document_id}: {e}")
            return ""

if __name__ == "__main__":
    if not os.path.exists(NOMENCLATURE_XLSX):
        logger.error(f"файл {NOMENCLATURE_XLSX} не найден.")
    else:
        df = pd.read_excel(NOMENCLATURE_XLSX)
        df.columns = df.columns.str.strip()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        app = App(df)
        app.mainloop()

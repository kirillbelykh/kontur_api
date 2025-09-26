import os
import json
import copy
import uuid
from logger import logger
import pandas as pd
from dataclasses import dataclass, asdict
from typing import List, Tuple
from get_gtin import lookup_gtin, lookup_by_gtin
from api import try_single_post, download_codes_pdf_and_convert
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
        resp = try_single_post(
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
        self.geometry("800x800")
        self.df = df
        self.collected: List[OrderItem] = []
        self.download_list: List[dict] = []  # [{'document_id': str, 'status': str, 'filename': str or None, 'order_name': str}]

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
        download_columns = ("order_name", "document_id", "status", "filename")
        self.download_tree = ttk.Treeview(tab_download, columns=download_columns, show="headings", height=10)
        self.download_tree.heading("order_name", text="Заявка")
        self.download_tree.heading("document_id", text="ID заказа")
        self.download_tree.heading("status", text="Статус")
        self.download_tree.heading("filename", text="Файл")
        self.download_tree.pack(pady=10, padx=10, fill="both", expand=True)

        # Buttons for download tab
        download_btn_frame = ctk.CTkFrame(tab_download)
        download_btn_frame.pack(pady=10, fill="x")

        download_btn = ctk.CTkButton(download_btn_frame, text="Скачать все", command=self.download_all)
        download_btn.pack(side="left", padx=10)

        refresh_btn = ctk.CTkButton(download_btn_frame, text="Обновить статусы", command=self.refresh_download_statuses)
        refresh_btn.pack(side="left", padx=10)

        # Log textbox for download tab
        self.download_log_text = ctk.CTkTextbox(tab_download, height=150)
        self.download_log_text.pack(pady=10, padx=10, fill="x")

        # Initial update
        self.update_download_tree()

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
            self.log_insert(f"✅Добавлено по GTIN: {gtin_input} — {codes_count} кодов — заявка '{order_name}'")
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
                idx, getattr(it, "_uid", "no-uid"), it.full_name, it.simpl_name, it.size, it.units_per_pack,
                it.gtin, it.codes_count, it.order_name
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
                        'filename': None
                    })
                    self.update_download_tree()
                except:
                    self.log_insert(f"Не удалось извлечь document_id из: {msg}")
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

        self._clear_after_execution()
    def _clear_after_execution(self):
        """Очищает данные после выполнения заказов"""
        try:
            # Очищаем основной список заказов
            self.collected.clear()
            
            # Очищаем дерево заказов
            for item in self.tree.get_children():
                self.tree.delete(item)
            
            # Очищаем поле количества кодов (опционально)
            if hasattr(self, 'codes_entry'):
                self.codes_entry.delete(0, "end")
            
            # Сбрасываем комбо-боксы к значениям по умолчанию (опционально)
            self._reset_input_fields()
            
            self.log_insert("Память очищена. Можно создавать новые заказы.")
            
        except Exception as e:
            self.log_insert(f"Ошибка при очистке памяти: {e}")

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

    def download_all(self):
        if not self.download_list:
            self.download_log_insert("Нет заказов для скачивания.")
            return

        # cookies → session
        cookies = None
        try:
            logger.info("Получаем cookies для скачивания...")
            cookies = get_valid_cookies()
        except Exception as e:
            self.download_log_insert(f"Ошибка при получении cookies: {e}")
            return

        if not cookies:
            self.download_log_insert("Cookies не получены; прерываем скачивание.")
            return

        session = make_session_with_cookies(cookies)

        for item in self.download_list:
            if item['status'] != 'Ожидает':
                continue  # Пропустить уже скачанные или с ошибкой

            self.download_log_insert(f"Скачивание для заказа {item['document_id']} ({item['order_name']})...")
            filename = download_codes_pdf_and_convert(session, item['document_id'], item['order_name'])
            if filename:
                item['status'] = 'Скачан'
                item['filename'] = filename
                self.download_log_insert(f"Успешно скачано: {filename}")
            else:
                item['status'] = 'Ошибка'
                self.download_log_insert("Ошибка скачивания")
            self.update_download_tree()

    def refresh_download_statuses(self):
        # Здесь можно добавить логику обновления статусов без скачивания, если нужно
        self.update_download_tree()
        self.download_log_insert("Статусы обновлены.")

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

import os
import copy
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime, timedelta
from logger import logger
import pandas as pd # type: ignore
from dataclasses import dataclass, asdict
from typing import List, Tuple, Dict, Any
from get_gtin import lookup_gtin, lookup_by_gtin
from api import codes_order, download_codes, make_task_on_tsd
from cookies import get_valid_cookies
from utils import make_session_with_cookies, get_tnved_code, save_snapshot, save_order_history
import update
import customtkinter as ctk
import tkinter as tk
import tkinter.messagebox as mbox
from tkinter import ttk, font
from dotenv import load_dotenv # type: ignore
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

class SessionManager:
    _lock = threading.Lock()
    _session = None
    _last_update = 0
    _lifetime = 60 * 13  # 13 минут
    _update_event = threading.Event()
    _update_thread = None
    _initialized = False

    @classmethod
    def initialize(cls):
        """Инициализация менеджера сессий - запускается при старте приложения"""
        if not cls._initialized:
            cls._initialized = True
            # Сразу запускаем фоновый процесс
            cls.start_background_update()
            # Принудительно запускаем первое обновление
            cls._update_event.set()

    @classmethod
    def start_background_update(cls):
        """Запуск фонового процесса обновления cookies"""
        if cls._update_thread is None or not cls._update_thread.is_alive():
            cls._update_thread = threading.Thread(
                target=cls._background_update_worker, 
                daemon=True,
                name="SessionUpdater"
            )
            cls._update_thread.start()
            print("✅ Фоновое обновление cookies запущено")

    @classmethod
    def _background_update_worker(cls):
        """Фоновый процесс для регулярного обновления cookies"""
        while True:
            try:
                # Ждем 13 минут или принудительного запроса
                update_triggered = cls._update_event.wait(timeout=cls._lifetime)
                
                print(f"🔧 Фоновое обновление cookies: {'принудительное' if update_triggered else 'плановое'}")
                
                # Получаем новые cookies
                cookies = get_valid_cookies()
                new_session = make_session_with_cookies(cookies)
                
                with cls._lock:
                    cls._session = new_session
                    cls._last_update = time.time()
                    
                print(f"✅ Cookies успешно обновлены. Следующее обновление через 13 минут")
                
                # Сбрасываем событие для следующей итерации
                cls._update_event.clear()
                
            except Exception as e:
                print(f"❌ Ошибка при фоновом обновлении cookies: {e}")
                # При ошибке ждем 1 минуту и пробуем снова
                time.sleep(60)

    @classmethod
    def get_session(cls):
        """Получение текущей сессии (блокирующий вызов только при первом обращении)"""
        cls.initialize()  # Гарантируем инициализацию
        
        with cls._lock:
            now = time.time()
            
            # Если сессии нет или она просрочена, создаем синхронно
            if cls._session is None or now - cls._last_update > cls._lifetime:
                print("⚠️  Синхронное получение cookies (сессия отсутствует или просрочена)")
                cookies = get_valid_cookies()
                cls._session = make_session_with_cookies(cookies)
                cls._last_update = now
                # Запускаем фоновое обновление для следующего цикла
                cls._update_event.set()
            elif now - cls._last_update > cls._lifetime * 0.8:
                # Если сессия скоро устареет, запускаем фоновое обновление заранее
                cls._update_event.set()
                
            return cls._session

    @classmethod
    def trigger_immediate_update(cls):
        """Принудительно запустить обновление cookies"""
        cls._update_event.set()
        print("🔄 Принудительное обновление cookies запущено")

    @classmethod
    def get_session_info(cls):
        """Информация о текущей сессии (для отладки)"""
        with cls._lock:
            now = time.time()
            age = now - cls._last_update if cls._last_update else 0
            return {
                "has_session": cls._session is not None,
                "age_seconds": age,
                "minutes_until_update": max(0, cls._lifetime - age) / 60
            }

def make_order_to_kontur(it, session) -> Tuple[bool, str]:
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
        
        # Настройка темы и внешнего вида
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        repo_dir = os.path.abspath(os.path.dirname(__file__))
        update.check_for_updates(repo_dir=repo_dir, pre_update_cleanup=self.cleanup_before_update, auto_restart=True)
        
        self.title("Kontur Marking")
        self.geometry("1000x800")
        self.minsize(900, 700)
        self._setup_fonts()

        self.df = df
        self.collected: List[OrderItem] = []
        self.download_list: List[dict] = []
        
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        SessionManager.initialize()
        
        # THREADING
        self.download_executor = ThreadPoolExecutor(max_workers=2)
        self.status_check_executor = ThreadPoolExecutor(max_workers=1)
        self.auto_download_active = False
        self.execute_all_executor = ThreadPoolExecutor(max_workers=3)
        self.intro_executor = ThreadPoolExecutor(max_workers=3)
        self.intro_tsd_executor = ThreadPoolExecutor(max_workers=3)
        
        self._setup_ui()
        self.start_auto_status_check()
        
        # Atributes for linter
        self.prod_date_entry: ctk.CTkEntry | None = None
        self.exp_date_entry: ctk.CTkEntry | None = None
        self.intro_number_entry: ctk.CTkEntry | None = None
        self.batch_entry: ctk.CTkEntry | None = None

        # TSD status check
        self.sent_to_tsd_items = set()
    
    def cleanup_before_update(self):
        """Очистка ресурсов перед обновлением."""
        try:
            # Завершаем все активные потоки
            self.auto_download_active = False
            self.download_executor.shutdown(wait=False)
            self.status_check_executor.shutdown(wait=False)
            self.execute_all_executor.shutdown(wait=False)
            self.intro_executor.shutdown(wait=False)
            self.intro_tsd_executor.shutdown(wait=False)
            print("✅ Потоки остановлены перед обновлением.")
        except Exception as e:
            print(f"⚠️ Ошибка при очистке перед обновлением: {e}")

    def _setup_fonts(self):
        """Настройка системы шрифтов"""
        # Проверяем доступные шрифты
        available_fonts = font.families()
        
        # Приоритетные шрифты (от наиболее предпочтительных к менее)
        preferred_fonts = [
            "Segoe UI Variable Display",  # Windows 11
            "Segoe UI",                   # Windows 10/11
            "Arial",                      # Универсальный
            "Tahoma",                     # Хорошая читаемость
            "Verdana",                    # Широкий
            "Microsoft Sans Serif",       # Классический Windows
            "Calibri",                    # Современный
            "DejaVu Sans",                # Кроссплатформенный
        ]
        
        # Выбираем первый доступный шрифт
        self.font_family = "TkDefaultFont"
        for font_name in preferred_fonts:
            if font_name in available_fonts:
                self.font_family = font_name
                break
        
        print(f"Используется шрифт: {self.font_family}")
        
        # Создаем систему шрифтов
        self.fonts = {
            "title": ctk.CTkFont(family=self.font_family, size=24, weight="bold"),
            "heading": ctk.CTkFont(family=self.font_family, size=16, weight="bold"),
            "subheading": ctk.CTkFont(family=self.font_family, size=14, weight="bold"),
            "normal": ctk.CTkFont(family=self.font_family, size=12),
            "small": ctk.CTkFont(family=self.font_family, size=11),
            "button": ctk.CTkFont(family=self.font_family, size=12, weight="bold"),
        }
        
        # Устанавливаем шрифт по умолчанию для основных виджетов
        self._set_default_fonts()

    def _set_default_fonts(self):
        """Устанавливает шрифты по умолчанию для всех виджетов"""
        try:
            # Получаем текущую тему
            current_theme = ctk.get_appearance_mode()
            
            # Настраиваем стандартные шрифты через тему
            normal_font = self.fonts["normal"]
            button_font = self.fonts["button"]
            
            # Создаем кастомную тему с нужными шрифтами
            ctk.set_default_color_theme("blue")  # или другая базовая тема
            
            # Для CTkFont можно установить шрифты при создании виджетов
            # или через конфигурацию отдельных виджетов
            
        except Exception as e:
            print(f"Ошибка при установке шрифтов: {e}")
            
    def _setup_ui(self):
        """Настройка основного интерфейса с использованием кастомных шрифтов"""
        # Главный контейнер
        self.main_container = ctk.CTkFrame(self)
        self.main_container.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Заголовок с кастомным шрифтом
        self.header_frame = ctk.CTkFrame(self.main_container, height=70)
        self.header_frame.pack(fill="x", pady=(0, 20))
        self.header_frame.pack_propagate(False)
        
        ctk.CTkLabel(
            self.header_frame, 
            text="Kontur Marking System", 
            font=self.fonts["title"]  # Используем кастомный шрифт
        ).pack(side="left", padx=25, pady=20)
        
        # Tabview
        self.tabview = ctk.CTkTabview(self.main_container)
        self.tabview.pack(fill="both", expand=True)
        
        # Создаем все табы
        self._setup_create_tab()
        self._setup_download_tab()
        self._setup_introduction_tab()
        self._setup_introduction_tsd_tab()
        
        # Статус бар с малым шрифтом
        self.status_bar = ctk.CTkLabel(
            self.main_container, 
            text="Готов к работе", 
            anchor="w",
            font=self.fonts["small"]
        )
        self.status_bar.pack(fill="x", pady=(10, 0))

    def _setup_create_tab(self):
        """Таб создания заказов с кастомными шрифтами"""
        tab_create = self.tabview.add("📦 Создание заказов")
        
        # Основной контейнер с сеткой
        main_frame = ctk.CTkFrame(tab_create)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Левая панель - форма ввода
        input_frame = ctk.CTkFrame(main_frame)
        input_frame.pack(side="left", fill="y", padx=(0, 10))
        
        # Заголовок формы с подзаголовочным шрифтом
        ctk.CTkLabel(
            input_frame, 
            text="Добавление позиции", 
            font=self.fonts["subheading"]
        ).pack(pady=(15, 15))
        
        # Поля ввода
        form_frame = ctk.CTkFrame(input_frame)
        form_frame.pack(fill="x", padx=15, pady=10)
        
        # Заявка №
        ctk.CTkLabel(form_frame, text="Заявка №:", font=self.fonts["normal"]).grid(row=0, column=0, sticky="w", pady=10)
        self.order_entry = ctk.CTkEntry(form_frame, width=250, placeholder_text="Введите номер заявки", font=self.fonts["normal"])
        self.order_entry.grid(row=0, column=1, pady=10, padx=(10, 0))
        
        # Режим поиска
        ctk.CTkLabel(form_frame, text="Режим поиска:", font=self.fonts["normal"]).grid(row=1, column=0, sticky="w", pady=10)
        mode_frame = ctk.CTkFrame(form_frame)
        mode_frame.grid(row=1, column=1, sticky="w", pady=10, padx=(10, 0))
        
        self.gtin_var = ctk.StringVar(value="No")
        ctk.CTkRadioButton(mode_frame, text="Поиск по GTIN", variable=self.gtin_var, value="Yes", 
                        command=self.toggle_mode, font=self.fonts["normal"]).pack(side="left", padx=(0, 10))
        ctk.CTkRadioButton(mode_frame, text="Выбор опций", variable=self.gtin_var, value="No", 
                        command=self.toggle_mode, font=self.fonts["normal"]).pack(side="left")
        
        # GTIN frame (изначально скрыт)
        self.gtin_frame = ctk.CTkFrame(form_frame)
        ctk.CTkLabel(self.gtin_frame, text="GTIN:", font=self.fonts["normal"]).grid(row=0, column=0, sticky="w", pady=10)
        self.gtin_entry = ctk.CTkEntry(self.gtin_frame, width=250, placeholder_text="Введите GTIN", font=self.fonts["normal"])
        self.gtin_entry.grid(row=0, column=1, pady=10, padx=(10, 0))
        self._add_entry_context_menu(self.gtin_entry)
        
        # Select frame
        self.select_frame = ctk.CTkFrame(form_frame)
        
        # Вид товара
        ctk.CTkLabel(self.select_frame, text="Вид товара:", font=self.fonts["normal"]).grid(row=0, column=0, sticky="w", pady=10)
        self.simpl_combo = ctk.CTkComboBox(self.select_frame, values=simplified_options, 
                                        command=self.update_options, width=250, font=self.fonts["normal"])
        self.simpl_combo.grid(row=0, column=1, pady=10, padx=(10, 0))
        
        # Цвет
        self.color_label = ctk.CTkLabel(self.select_frame, text="Цвет:", font=self.fonts["normal"])
        self.color_combo = ctk.CTkComboBox(self.select_frame, values=color_options, width=250, font=self.fonts["normal"])
        
        # Венчик
        self.venchik_label = ctk.CTkLabel(self.select_frame, text="Венчик:", font=self.fonts["normal"])
        self.venchik_combo = ctk.CTkComboBox(self.select_frame, values=venchik_options, width=250, font=self.fonts["normal"])
        
        # Размер
        ctk.CTkLabel(self.select_frame, text="Размер:", font=self.fonts["normal"]).grid(row=3, column=0, sticky="w", pady=10)
        self.size_combo = ctk.CTkComboBox(self.select_frame, values=size_options, width=250, font=self.fonts["normal"])
        self.size_combo.grid(row=3, column=1, pady=10, padx=(10, 0))
        
        # Упаковка
        ctk.CTkLabel(self.select_frame, text="Единиц в упаковке:", font=self.fonts["normal"]).grid(row=4, column=0, sticky="w", pady=10)
        self.units_combo = ctk.CTkComboBox(self.select_frame, values=[str(u) for u in units_options], width=250, font=self.fonts["normal"])
        self.units_combo.grid(row=4, column=1, pady=10, padx=(10, 0))
        
        # Количество кодов
        ctk.CTkLabel(form_frame, text="Количество кодов:", font=self.fonts["normal"]).grid(row=6, column=0, sticky="w", pady=10)
        self.codes_entry = ctk.CTkEntry(form_frame, width=250, placeholder_text="Введите количество", font=self.fonts["normal"])
        self.codes_entry.grid(row=6, column=1, pady=10, padx=(10, 0))
        
        # Кнопка добавления
        add_btn = ctk.CTkButton(
            form_frame, 
            text="➕ Добавить позицию", 
            command=self.add_item,
            height=35,
            fg_color="#2AA876",
            hover_color="#228B69",
            font=self.fonts["button"]
        )
        add_btn.grid(row=7, column=0, columnspan=2, pady=20)
        
        self.toggle_mode()
        
        # Правая панель - таблица и лог
        right_frame = ctk.CTkFrame(main_frame)
        right_frame.pack(side="right", fill="both", expand=True)
        
        # Таблица
        table_frame = ctk.CTkFrame(right_frame)
        table_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        # Заголовок таблицы
        ctk.CTkLabel(
            table_frame, 
            text="Список позиций", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(10, 5))
        
        columns = ("idx", "full_name", "simpl_name", "size", "units_per_pack", "gtin", "codes_count", "order_name", "uid")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=12)
        
        # Заголовки
        headers = {
            "idx": "№", "full_name": "Наименование", "simpl_name": "Упрощенно",
            "size": "Размер", "units_per_pack": "Упаковка", "gtin": "GTIN",
            "codes_count": "Кодов", "order_name": "Заявка", "uid": "UID"
        }
        
        for col, text in headers.items():
            self.tree.heading(col, text=text)
            self.tree.column(col, width=80 if col == "idx" else 120)
        
        # Scrollbar для таблицы
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Кнопки управления
        btn_frame = ctk.CTkFrame(right_frame)
        btn_frame.pack(fill="x", pady=(0, 10))
        
        delete_btn = ctk.CTkButton(
            btn_frame, 
            text="🗑️ Удалить", 
            command=self.delete_item, 
            width=120,
            font=self.fonts["button"]
        )
        delete_btn.pack(side="left", padx=5)
        
        self.execute_btn = ctk.CTkButton(
            btn_frame, 
            text="⚡ Выполнить все", 
            command=self.execute_all,
            width=120,
            fg_color="#2E86C1",
            hover_color="#2874A6",
            font=self.fonts["button"]
        )
        self.execute_btn.pack(side="left", padx=5)
        
        clear_btn = ctk.CTkButton(
            btn_frame, 
            text="🧹 Очистить", 
            command=self.clear_all, 
            width=120,
            font=self.fonts["button"]
        )
        clear_btn.pack(side="left", padx=5)
        
        # Лог
        log_frame = ctk.CTkFrame(right_frame)
        log_frame.pack(fill="both", expand=True, pady=(5, 10))  # добавил немного отступа сверху/снизу

        ctk.CTkLabel(
            log_frame, 
            text="Лог операций:", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(10, 5))

        # Увеличиваем высоту поля лога
        self.log_text = ctk.CTkTextbox(log_frame, height=250, font=self.fonts["normal"])  # было 150, стало 250
        self.log_text.pack(fill="both", expand=True, padx=5, pady=(0, 5))
        self.log_text.configure(state="disabled")

        
        # Контекстное меню для лога
        self.log_text.bind("<Button-3>", self._show_log_context_menu)
        self.log_text.bind("<Control-c>", lambda e: self._copy_log_text())
        self.log_text.bind("<Control-C>", lambda e: self._copy_log_text())
        
        # Стиль для таблицы
        self._configure_treeview_style()
    
    def _setup_download_tab(self):
        """Таб скачивания кодов"""
        tab_download = self.tabview.add("📥 Скачивание кодов")
        
        main_frame = ctk.CTkFrame(tab_download)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Верхняя часть - таблица
        table_frame = ctk.CTkFrame(main_frame)
        table_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        ctk.CTkLabel(table_frame, text="Список заказов для скачивания:", 
                    font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(10, 5))
        
        download_columns = ("order_name", "status", "filename", "document_id")
        self.download_tree = ttk.Treeview(table_frame, columns=download_columns, show="headings", height=12)
        
        headers = {
            "order_name": "Заявка", "status": "Статус", 
            "filename": "Файл", "document_id": "ID заказа"
        }
        
        for col, text in headers.items():
            self.download_tree.heading(col, text=text)
            self.download_tree.column(col, width=150)
        
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.download_tree.yview)
        self.download_tree.configure(yscrollcommand=scrollbar.set)
        self.download_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Нижняя часть - лог
        log_frame = ctk.CTkFrame(main_frame)
        log_frame.pack(fill="both", expand=True)
        
        ctk.CTkLabel(log_frame, text="Лог скачивания:", 
                    font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(10, 5))
        
        self.download_log_text = ctk.CTkTextbox(log_frame, height=150)
        self.download_log_text.pack(fill="both", expand=True, padx=5, pady=(0, 5))
        self.download_log_text.configure(state="disabled")


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
                f"✅Добавлено: {simpl} {size}, {units} уп., {color or ''} — "
                f"GTIN {gtin} — {codes_count} код(ов) — ТНВЭД {tnved_code} — заявка № {order_name}"
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
        """Запуск выполнения всех накопленных позиций в многопоточном режиме"""
        try:
            if not self.collected:
                self.log_insert("Нет накопленных позиций.")
                return

            confirm = tk.messagebox.askyesno("Подтверждение", f"Подтвердите выполнение {len(self.collected)} задач(и)?") # type: ignore
            if not confirm:
                self.log_insert("Выполнение отменено пользователем.")
                return

            to_process = copy.deepcopy(self.collected)
            save_snapshot(to_process)
            save_order_history(to_process)
            
            self.log_insert(f"\nБудет выполнено {len(to_process)} заказов.")
            
            # Отключаем кнопку выполнения на время работы
            self.execute_btn.configure(state="disabled")  # Предполагается, что у вас есть такая кнопка
            
            # Запускаем задачи в ThreadPoolExecutor
            futures = []
            for it in to_process:
                session = SessionManager.get_session()
                fut = self.execute_all_executor.submit(self._execute_worker, it, session)
                futures.append((fut, it))

            # Мониторинг завершения задач
            def execute_all_monitor():
                completed = 0
                success_count = 0
                fail_count = 0
                results = []
                
                for fut, it in futures:
                    try:
                        # Ждем завершения задачи с таймаутом
                        ok, msg = fut.result(timeout=60)  # 1 минута таймаут
                        results.append((ok, msg, it))
                        
                        # Обновляем GUI в основном потоке
                        self.after(0, self._on_execute_finished, it, ok, msg)
                        
                        if ok:
                            success_count += 1
                        else:
                            fail_count += 1
                            
                        completed += 1
                        
                    except Exception as e:
                        error_msg = f"Таймаут или ошибка выполнения: {e}"
                        self.after(0, self._on_execute_finished, it, False, error_msg)
                        fail_count += 1
                        completed += 1
                
                # Все задачи завершены - разблокируем кнопку и выводим итоги
                self.after(0, self._on_all_execute_finished, success_count, fail_count, results)
                
                # Запускаем автоматическую загрузку
                self.after(0, self.start_auto_status_check)

            # Запускаем мониторинг в отдельном потоке
            threading.Thread(target=execute_all_monitor, daemon=True).start()

        except Exception as e:
            self.log_insert(f"❌ Ошибка при запуске выполнения: {e}")
            # В случае ошибки разблокируем кнопку
            self.execute_btn.configure(state="normal")

    def _execute_worker(self, order_item, session):
        """Воркер для выполнения одного заказа в отдельном потоке"""
        try:
            self.log_insert(f"🎬 Запуск позиции: {order_item.simpl_name}  GTIN {order_item.gtin}  заявка № {order_item.order_name}")
            ok, msg = make_order_to_kontur(order_item, session)
            return ok, msg
        except Exception as e:
            return False, f"Ошибка в воркере: {e}"

    def _on_execute_finished(self, order_item, ok, msg):
        """Обработчик завершения выполнения одного заказа"""
        if ok:
            self.log_insert(f"✨ Заявка «{order_item.order_name}» на {order_item.simpl_name} успешно создана ✅")
            try:
                # Парсим document_id из сообщения
                document_id = msg.split("id: ")[1].strip()
                self.download_list.append({
                    'order_name': order_item.order_name,
                    'document_id': document_id,
                    'status': 'Ожидает',
                    'filename': None,
                    'simpl': order_item.simpl_name,
                    'full_name': order_item.full_name
                })
                self.update_download_tree()
            except Exception as e:
                self.log_insert(f"⚠️ Не удалось извлечь document_id из: {msg} - {e}")
        else:
            self.log_insert(f"❌ Ошибка: {order_item.simpl_name} | заявка '{order_item.order_name}' => {msg}")

    def _on_all_execute_finished(self, success_count, fail_count, results):
        """Обработчик завершения всех задач"""
        # Разблокируем кнопку
        self.execute_btn.configure(state="normal")
        
        self.log_insert("\n=== ВЫПОЛНЕНИЕ ЗАВЕРШЕНО ===")
        self.log_insert(f"✅ Успешно: {success_count}\n❌ Ошибок: {fail_count}")

        # Выводим список неудачных позиций
        if any(not r[0] for r in results):
            self.log_insert("\nНеудачные позиции:")
            for ok, msg, it in results:
                if not ok:
                    uid = getattr(it, '_uid', None)
                    self.log_insert(f" - uid={uid} | {it.simpl_name} | GTIN {it.gtin} | заявка '{it.order_name}' => {msg}")
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


    def start_auto_status_check(self):
        """Запускает автоматическую проверку статусов заказов"""
        if self.auto_download_active:
            return
            
        self.auto_download_active = True
        self.download_log_insert("🔄 Автоматическая проверка статусов запущена")
        
        def status_check_worker():
            while self.auto_download_active:
                try:
                    # Проверяем каждые 2 секунды
                    time.sleep(2)
                    
                    # Получаем заказы, которые ожидают скачивания
                    pending_orders = [item for item in self.download_list 
                                if item['status'] not in ['Скачивается', 'Скачан']]
                    if not pending_orders:
                        continue
                    
                    
                    # Проверяем статусы и запускаем скачивание для готовых
                    for item in pending_orders:
                        if not self.auto_download_active:
                            break
                            
                        try:
                            # Проверяем статус заказа
                            status = self._check_order_status(item['document_id'])
                            
                            if status == 'released':
                                self.download_log_insert(f"✅ Заказ {item['order_name']} готов к скачиванию")
                                # Запускаем скачивание в отдельном потоке
                                self.download_executor.submit(self._download_order, item)
                                item['status'] = 'В обработке'
                                self.after(0, self.update_download_tree)
                            elif status in ['processing', 'created']:
                                item['status'] = 'В обработке'
                                self.after(0, self.update_download_tree)
                            elif status == 'error':
                                item['status'] = 'Ошибка генерации'
                                self.after(0, self.update_download_tree)
                                
                        except Exception as e:
                            self.download_log_insert(f"❌ Ошибка проверки заказа {item['order_name']}: {e}")
                            continue
                            
                except Exception as e:
                    self.download_log_insert(f"❌ Ошибка в статус-чекере: {e}")
                    time.sleep(30)  # Ждем перед повторной попыткой
        
        # Запускаем в отдельном потоке
        threading.Thread(target=status_check_worker, daemon=True).start()

    def _check_order_status(self, document_id):
        """Проверяет статус заказа"""
        try:
            
            session = SessionManager.get_session()
            
            resp = session.get(f"{BASE}/api/v1/codes-order/{document_id}", timeout=15)
            resp.raise_for_status()
            
            doc = resp.json()
            return doc.get("status", "unknown")
            
        except Exception as e:
            raise Exception(f"Ошибка проверки статуса {document_id}: {e}")

    def _download_order(self, item):
        """Скачивает заказ в отдельном потоке"""
        try:
            # Обновляем статус в главном потоке
            self.after(0, lambda: self._update_download_status(item, 'Скачивается'))
            
            session = SessionManager.get_session()
            
            # Скачиваем файл
            filename = download_codes(session, item['document_id'], item['order_name'])
            
            if filename:
                # Успешное скачивание - обновляем в главном потоке
                self.after(0, lambda: self._finish_download(item, filename))
            else:
                self.after(0, lambda: self._update_download_status(item, 'Ошибка скачивания'))
                
        except Exception as e:
            self.after(0, lambda: self._update_download_status(item, f'Ошибка: {e}'))

    def _update_download_status(self, item, status):
        """Обновляет статус скачивания в UI"""
        try:
            item['status'] = status
            self.update_download_tree()
            self.download_log_insert(f"📦 {item['order_name']}: {status}")
            # Принудительно обновляем интерфейс
            self.update_idletasks()
        except Exception as e:
            print(f"Ошибка обновления статуса: {e}")

    def _finish_download(self, item, filename):
        """Завершает скачивание"""
        try:
            item['status'] = 'Скачан'
            item['filename'] = filename
            self.update_download_tree()
            self.download_log_insert(f"✅ Успешно скачан: {filename}")
            # Принудительно обновляем интерфейс
            self.update_idletasks()
        except Exception as e:
            print(f"Ошибка завершения скачивания: {e}")


    def _add_to_download_list(self, order_item, document_id):
        """Добавляет заказ в список для скачивания"""
        # Проверяем, нет ли уже такого заказа
        for item in self.download_list:
            if item['document_id'] == document_id:
                return
                
        new_item = {
            'order_name': order_item.order_name,
            'document_id': document_id,
            'status': 'Ожидает',
            'filename': None,
            'simpl': order_item.simpl_name
        }
        
        self.download_list.append(new_item)
        self.update_download_tree()
        self.download_log_insert(f"📝 Добавлен в очередь скачивания: {order_item.order_name}")

    def update_download_tree(self):
        """Обновляет таблицу скачиваний"""
        for item in self.download_tree.get_children():
            self.download_tree.delete(item)
            
        for item in self.download_list:
            self.download_tree.insert("", "end", values=(
                item['order_name'],
                item['status'],
                item['filename'] or "-",
                item['document_id']
            ))

    def download_log_insert(self, msg: str):
        """Добавляет сообщение в лог скачиваний"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.download_log_text.insert("end", f"[{timestamp}] {msg}\n")
        self.download_log_text.see("end")

    def on_closing(self):
        self.auto_download_active = False
        for executor in [self.download_executor, self.status_check_executor,
                        self.execute_all_executor, self.intro_executor, self.intro_tsd_executor]:
            executor.shutdown(wait=False, cancel_futures=True)
        self.destroy()
        
    def _setup_introduction_tab(self):
        """Таб ввода в оборот"""
        tab_intro = self.tabview.add("🔄 Ввод в оборот")
        self.intro_tab = tab_intro
        
        main_frame = ctk.CTkFrame(tab_intro)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Левая часть
        left_frame = ctk.CTkFrame(main_frame)
        left_frame.pack(side="left", fill="both", expand=True, padx=(0, 5))
        
        # Верхняя часть - таблица
        table_frame = ctk.CTkFrame(left_frame)
        table_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        ctk.CTkLabel(table_frame, text="Доступные заказы:", 
                    font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(10, 5))
        
        intro_columns = ("order_name", "document_id", "status", "filename")
        self.intro_tree = ttk.Treeview(table_frame, columns=intro_columns, show="headings", 
                                    height=10, selectmode="extended")
        
        headers = {
            "order_name": "Заявка", "document_id": "ID заказа",
            "status": "Статус", "filename": "Файл"
        }
        
        for col, text in headers.items():
            self.intro_tree.heading(col, text=text)
            self.intro_tree.column(col, width=150)
        
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.intro_tree.yview)
        self.intro_tree.configure(yscrollcommand=scrollbar.set)
        self.intro_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Средняя часть - форма ввода
        form_frame = ctk.CTkFrame(left_frame)
        form_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(form_frame, text="Параметры ввода:", 
                    font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", pady=10, columnspan=4)
        
        # Сетка для полей ввода
        labels = [
            ("Дата производства (ДД-ММ-ГГГГ):", "prod_date_entry"),
            ("Дата окончания (ДД-ММ-ГГГГ):", "exp_date_entry"),
            ("Номер партии:", "batch_entry")
        ]
        
        for i, (label_text, attr_name) in enumerate(labels):
            ctk.CTkLabel(form_frame, text=label_text).grid(row=i+1, column=0, sticky="w", pady=8, padx=5)
            entry = ctk.CTkEntry(form_frame, width=200)
            entry.grid(row=i+1, column=1, pady=8, padx=5)
            setattr(self, attr_name, entry)
        
        # Заполнение дат по умолчанию
        today = datetime.now().strftime("%d-%m-%Y")
        future_date = (datetime.now() + timedelta(days=1826)).strftime("%d-%m-%Y")
        self.prod_date_entry.insert(0, today) # type: ignore
        self.exp_date_entry.insert(0, future_date) # type: ignore
        
        # Кнопки
        btn_frame = ctk.CTkFrame(left_frame)
        btn_frame.pack(fill="x", pady=(0, 10))
        
        self.intro_btn = ctk.CTkButton(
            btn_frame, 
            text="🔄 Ввести в оборот", 
            command=self.on_introduce_clicked,
            fg_color="#2AA876",
            hover_color="#228B69"
        )
        self.intro_btn.pack(side="left", padx=5)
        
        self.intro_refresh_btn = ctk.CTkButton(btn_frame, text="🔄 Обновить", command=self.update_introduction_tree)
        self.intro_refresh_btn.pack(side="left", padx=5)
        
        self.intro_clear_btn = ctk.CTkButton(btn_frame, text="🧹 Очистить лог", command=self.clear_intro_log)
        self.intro_clear_btn.pack(side="left", padx=5)
        
        # Правая часть - лог
        right_frame = ctk.CTkFrame(main_frame)
        right_frame.pack(side="right", fill="both", expand=True, padx=(5, 0))
        
        log_frame = ctk.CTkFrame(right_frame)
        log_frame.pack(fill="both", expand=True)
        
        ctk.CTkLabel(log_frame, text="Лог операций:", 
                    font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(10, 5))
        
        self.intro_log_text = ctk.CTkTextbox(log_frame)
        self.intro_log_text.pack(fill="both", expand=True, padx=5, pady=(0, 5))
        self.intro_log_text.configure(state="disabled")
        
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
                if item.get("status") in ("Скачан", "Downloaded", "Ожидает") and item.get("document_id"):
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
            prod_date = self.convert_date_format(self.prod_date_entry.get().strip()) # type: ignore
            exp_date = self.convert_date_format(self.exp_date_entry.get().strip()) # type: ignore
            batch_num = self.batch_entry.get().strip() # type: ignore
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
                
                fut = self.intro_executor.submit(self._intro_worker, it, production_patch, thumbprint) # type: ignore
                futures.append((fut, it))

            # Мониторинг завершения
            def intro_monitor():
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

            threading.Thread(target=intro_monitor, daemon=True).start()

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
            session = SessionManager.get_session()
            
            # Импортируем функцию из api.py
            from api import put_into_circulation
            
            # Вызываем API функцию
            ok, result = put_into_circulation(
                session=session,
                codes_order_id=document_id,
                production_patch=production_patch,
                organization_id=os.getenv("ORGANIZATION_ID"), # type: ignore
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

    def _setup_introduction_tsd_tab(self):
        """Таб ввода в оборот (ТСД)"""
        tab_tsd = self.tabview.add("📱 Ввод в оборот (ТСД)")
        self.tsd_tab = tab_tsd
        
        main_frame = ctk.CTkFrame(tab_tsd)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Используем grid для разделения на левую и правую части
        main_frame.grid_columnconfigure(0, weight=1)  # Левая колонка - таблица и форма
        main_frame.grid_columnconfigure(1, weight=1)  # Правая колонка - лог
        main_frame.grid_rowconfigure(1, weight=1)     # Вторая строка - растягиваемая
        
        # Левая часть - таблица и форма
        left_frame = ctk.CTkFrame(main_frame)
        left_frame.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 5))
        
        # Таблица в левой части
        table_frame = ctk.CTkFrame(left_frame)
        table_frame.pack(fill="both", expand=True, pady=(0, 10))
        
        ctk.CTkLabel(table_frame, text="Доступные заказы:", 
                    font=ctk.CTkFont(weight="bold")).pack(anchor="w", pady=(10, 5))
        
        tsd_columns = ("order_name", "document_id", "status", "filename")
        self.tsd_tree = ttk.Treeview(table_frame, columns=tsd_columns, show="headings", 
                                height=12, selectmode="extended")
        
        headers = {
            "order_name": "Заявка", "document_id": "ID заказа",
            "status": "Статус", "filename": "Файл"
        }
        
        for col, text in headers.items():
            self.tsd_tree.heading(col, text=text)
            self.tsd_tree.column(col, width=150)
        
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tsd_tree.yview)
        self.tsd_tree.configure(yscrollcommand=scrollbar.set)
        self.tsd_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Форма ввода в левой части
        form_frame = ctk.CTkFrame(left_frame)
        form_frame.pack(fill="x", pady=10)
        
        ctk.CTkLabel(form_frame, text="Параметры ТСД:", 
                    font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", pady=10, columnspan=4)
        
        # Сетка для полей ввода
        tsd_labels = [
            ("Ввод в оборот №:", "tsd_intro_number_entry"),
            ("Дата производства (ДД-ММ-ГГГГ):", "tsd_prod_date_entry"),
            ("Дата окончания (ДД-ММ-ГГГГ):", "tsd_exp_date_entry"),
            ("Номер партии:", "tsd_batch_entry")
        ]
        
        for i, (label_text, attr_name) in enumerate(tsd_labels):
            ctk.CTkLabel(form_frame, text=label_text).grid(row=i+1, column=0, sticky="w", pady=8, padx=5)
            entry = ctk.CTkEntry(form_frame, width=200)
            entry.grid(row=i+1, column=1, pady=8, padx=5)
            setattr(self, attr_name, entry)
        
        # Заполнение дат по умолчанию
        today = datetime.now().strftime("%d-%m-%Y")
        future_date = (datetime.now() + timedelta(days=1826)).strftime("%d-%m-%Y")
        self.tsd_prod_date_entry.insert(0, today) # type: ignore
        self.tsd_exp_date_entry.insert(0, future_date) # type: ignore
        
        # Кнопки в левой части
        btn_frame = ctk.CTkFrame(left_frame)
        btn_frame.pack(fill="x", pady=(0, 10))
        
        self.tsd_btn = ctk.CTkButton(
            btn_frame, 
            text="📱 Отправить на ТСД", 
            command=self.on_tsd_clicked,
            fg_color="#E67E22",
            hover_color="#D35400"
        )
        self.tsd_btn.pack(side="left", padx=5)
        
        self.tsd_refresh_btn = ctk.CTkButton(btn_frame, text="🔄 Обновить", command=self.update_tsd_tree)
        self.tsd_refresh_btn.pack(side="left", padx=5)
        
        self.tsd_clear_btn = ctk.CTkButton(btn_frame, text="🧹 Очистить лог", command=self.clear_tsd_log)
        self.tsd_clear_btn.pack(side="left", padx=5)
        
        # Правая часть - только лог
        right_frame = ctk.CTkFrame(main_frame)
        right_frame.grid(row=0, column=1, rowspan=2, sticky="nsew", padx=(5, 0))
        right_frame.grid_rowconfigure(1, weight=1)  # Лог будет растягиваться
        right_frame.grid_columnconfigure(0, weight=1)

        # Заголовок лога
        ctk.CTkLabel(right_frame, text="Лог ТСД:", 
                    font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", pady=(10, 5), padx=10)

        # Большое поле для лога - занимает всю правую часть
        self.tsd_log_text = ctk.CTkTextbox(right_frame, font=ctk.CTkFont(size=13))
        self.tsd_log_text.grid(row=1, column=0, sticky="nsew", padx=15, pady=(0, 10))
        self.tsd_log_text.configure(state="disabled")
        
        self.update_tsd_tree()

    def clear_tsd_log(self):
        """Очищает лог ТСД"""
        try:
            # Включаем редактирование для очистки
            self.tsd_log_text.configure(state="normal")
            # Удаляем весь текст
            self.tsd_log_text.delete("1.0", "end")
            # Возвращаем состояние "только чтение"
            self.tsd_log_text.configure(state="disabled")
            
            # Добавляем сообщение о том, что лог очищен
            self.tsd_log_text.configure(state="normal")
            self.tsd_log_text.insert("end", f"[{datetime.now().strftime('%H:%M:%S')}] Лог очищен\n")
            self.tsd_log_text.configure(state="disabled")
            
            # Прокручиваем к последнему сообщению
            self.tsd_log_text.see("end")
            
        except Exception as e:
            print(f"Ошибка при очистке лога ТСД: {e}")

    def _configure_treeview_style(self):
        """Настройка стиля таблиц"""
        style = ttk.Style()
        style.theme_use("clam")
        
        # Стиль для Treeview
        style.configure("Treeview",
                       background="#2b2b2b",
                       foreground="white",
                       fieldbackground="#2b2b2b",
                       borderwidth=0)
        
        style.configure("Treeview.Heading",
                       background="#3a3a3a",
                       foreground="white",
                       relief="flat",
                       font=('TkDefaultFont', 10, 'bold'))
        
        style.map("Treeview",
                 background=[('selected', '#1f6aa5')],
                 foreground=[('selected', 'white')])
        
        style.map("Treeview.Heading",
                 background=[('active', '#4a4a4a')])

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
        """Наполнить дерево заказами, у которых status == 'Скачан' или filename != None, но не отправлены на ТСД"""
        # Очистить дерево
        for i in self.tsd_tree.get_children():
            self.tsd_tree.delete(i)
        
        # Добавить записи из self.download_list, которые не были отправлены на ТСД
        for item in self.download_list:
            document_id = item.get("document_id")
            # Показываем только если статус подходящий И заказ еще не отправлялся на ТСД
            if (item.get("status") in ("Скачан", "Скачивается", "Downloaded", "Ожидает") or item.get("filename")) and document_id not in self.sent_to_tsd_items:
                vals = (
                    item.get("order_name"), 
                    document_id, 
                    item.get("status"), 
                    item.get("filename") or ""
                )
                self.tsd_tree.insert("", "end", iid=document_id, values=vals)

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
            intro_number = self.tsd_intro_number_entry.get().strip() # type: ignore
            prod_date_raw = self.tsd_prod_date_entry.get().strip() # type: ignore
            exp_date_raw = self.tsd_exp_date_entry.get().strip() # type: ignore
            batch_num = self.tsd_batch_entry.get().strip() # type: ignore
            
            
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
                    session = SessionManager.get_session()
                    fut = self.intro_tsd_executor.submit(self._tsd_worker, it, positions_data, production_patch, session)
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
            def tsd_monitor():
                try:
                    self.tsd_log_insert("👀 Мониторинг запущен...")
                    completed = 0
                    for fut, it in futures:
                        try:
                            self.tsd_log_insert(f"⏳ Ожидание завершения задачи {completed + 1}/{len(futures)}...")
                            ok, result = fut.result(timeout=15)
                            
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
            monitor_thread = threading.Thread(target=tsd_monitor, daemon=True)
            monitor_thread.start()
            self.tsd_log_insert("📊 Мониторинг задач запущен в фоне")

        except Exception as e:
            self.tsd_log_insert(f"💥 Критическая ошибка в on_tsd_clicked: {e}")
            import traceback
            self.tsd_log_insert(f"🔍 Детали: {traceback.format_exc()}")
            self.tsd_btn.configure(state="normal")

    def _tsd_worker(self, item: dict, positions_data: List[Dict[str, str]], production_patch: dict, session) -> Tuple[bool, Dict[str, Any]]:
        """
        Фоновая задача — производит ввод в оборот для одного заказа item.
        Возвращает (ok, result: dict).
        """
        try:
            self.tsd_log_insert(f"🔧 Начало работы _tsd_worker для {item.get('order_name', 'Unknown')}")

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
            # Добавляем в множество отправленных заказов
            self.sent_to_tsd_items.add(docid)
            # пометим заказ как введённый
            item["status"] = "Отправлено на ТСД"
        else:
            self.tsd_log_insert(f"[ERR] {docid} — {msg}")
            item["status"] = "Ошибка ТСД"
            # Не добавляем в sent_to_tsd_items при ошибке

        # обновить таблицы
        self.update_tsd_tree()
        
    def _get_gtin_for_order(self, document_id: str) -> str:
        """Получает GTIN для заказа по document_id"""
        try:
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

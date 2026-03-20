import csv
import os
import copy
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime
from logger import logger
import pandas as pd # type: ignore
from dataclasses import dataclass, asdict
from typing import List, Tuple, Dict, Any
from get_gtin import lookup_gtin, lookup_by_gtin
from api import codes_order, download_codes, make_task_on_tsd
from cookies import get_valid_cookies
from utils import make_session_with_cookies, get_tnved_code, save_snapshot, save_order_history
from date_defaults import get_default_production_window
from queue_utils import (
    is_order_ready_for_intro,
    is_order_ready_for_tsd,
    remove_order_by_document_id,
)
from get_thumb import get_thumbprint
from history_db import OrderHistoryDB
from bartender_print import (
    BarTenderPrintError,
    build_print_context,
    list_installed_printers,
    print_labels,
)
from bartender_label_100x180 import (
    AGGREGATION_SOURCE_KIND,
    AggregationCsvInfo,
    BarTenderLabel100x180Error,
    LabelTemplateInfo,
    MARKING_SOURCE_KIND,
    build_label_print_context as build_100x180_label_print_context,
    list_aggregation_csv_files,
    list_100x180_templates,
    list_marking_csv_files,
    print_100x180_labels,
    resolve_order_metadata,
)
import update
import customtkinter as ctk
from customtkinter import CTkScrollableFrame
import tkinter as tk
import tkinter.messagebox as mbox
from tkinter import ttk, font
from dotenv import load_dotenv # type: ignore
from options import (
    simplified_options, color_required, venchik_required,
    color_options, venchik_options, size_options, units_options
)
from aggregation_bulk import BulkAggregationService
from cryptopro import find_certificate_by_thumbprint, sign_data, sign_text_data

load_dotenv()

# Константы 
BASE = os.getenv("BASE_URL")
PRODUCT_GROUP = os.getenv("PRODUCT_GROUP")
RELEASE_METHOD_TYPE = os.getenv("RELEASE_METHOD_TYPE")
CIS_TYPE = os.getenv("CIS_TYPE")  
FILLING_METHOD = os.getenv("FILLING_METHOD")  
THUMBPRINT = get_thumbprint()
NOMENCLATURE_XLSX = "data/nomenclature.xlsx"
LABEL_PRINT_REFRESH_CACHE_SECONDS = 30

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
                    
                print("✅ Cookies успешно обновлены. Следующее обновление через 13 минут")
                
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

        logger.info(f"ФИНАЛЬНЫЙ СТАТУС ДОКУМЕНТА:{status}")
        return True, f"Document {document_number} processed, status: {status}, id: {document_id}"

    except Exception as e:
        return False, f"Exception: {e}"

class App(ctk.CTk):
    def __init__(self, df):
        super().__init__()
        
        # Расширенные настройки темы
        self.current_theme = "dark"
        self.color_themes = {
            "dark": {
                "primary": "#2E86C1",
                "secondary": "#1E3A5F", 
                "accent": "#FF6B35",
                "success": "#27AE60",
                "warning": "#F39C12",
                "error": "#E74C3C",
                "bg_primary": "#1A1A2E",
                "bg_secondary": "#16213E",
                "text_primary": "#FFFFFF",
                "text_secondary": "#B0B0B0"
            },
            "light": {
                "primary": "#3498DB",
                "secondary": "#EBF5FB",
                "accent": "#E67E22",
                "success": "#2ECC71",
                "warning": "#F1C40F",
                "error": "#E74C3C",
                "bg_primary": "#F8F9FA",
                "bg_secondary": "#FFFFFF",
                "text_primary": "#2C3E50",
                "text_secondary": "#566573"
            }
        }
        
        # Применяем тему
        ctk.set_appearance_mode(self.current_theme)
        ctk.set_default_color_theme("blue")
        
        self._repo_dir = os.path.abspath(os.path.dirname(__file__))
        # Настройка окна
        self.title("Kontur Marking System")
        self.is_fullscreen = False
        self.attributes('-fullscreen', self.is_fullscreen)
        self.minsize(1300, 700)
        self.geometry("1200x800")  # Начальный размер окна
        
        # Современные шрифты
        self._setup_modern_fonts()
        
        # Инициализация данных
        self.df = df
        self.collected: List[OrderItem] = []
        self.download_list: List[dict] = []
        
        # Инициализация атрибутов UI
        self._init_ui_attributes()
        
        # Создание интерфейса
        self._setup_modern_ui()
        
        # Центрируем окно после создания UI
        self.center_window()
        
        # Остальная инициализация...
        self.sent_to_tsd_items = set()
        self.history_db = OrderHistoryDB()
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        SessionManager.initialize()
        
        # Threading
        self.download_executor = ThreadPoolExecutor(max_workers=2)
        self.status_check_executor = ThreadPoolExecutor(max_workers=1)
        self.print_executor = ThreadPoolExecutor(max_workers=1)
        self.auto_download_active = False
        self.print_in_progress = False
        self.execute_all_executor = ThreadPoolExecutor(max_workers=3)
        self.intro_executor = ThreadPoolExecutor(max_workers=3)
        self.intro_tsd_executor = ThreadPoolExecutor(max_workers=3)
        self.bulk_aggregation_service = BulkAggregationService()
        
        self.start_auto_status_check()
        # Проверяем обновления после появления окна, чтобы не тормозить старт.
        self.after(1200, self._check_for_updates_deferred)
        
        # Atributes for linter
        self.prod_date_entry: ctk.CTkEntry | None = None
        self.exp_date_entry: ctk.CTkEntry | None = None
        self.intro_number_entry: ctk.CTkEntry | None = None
        self.batch_entry: ctk.CTkEntry | None = None

    def toggle_fullscreen(self, event=None):
        """Переключение полноэкранного режима"""
        self.is_fullscreen = not self.is_fullscreen
        self.attributes('-fullscreen', self.is_fullscreen)
        
        if not self.is_fullscreen:
            self.geometry("1200x800")  # Восстанавливаем размер при выходе из полноэкранного
            self.center_window()
        
        # Обновляем текст кнопки
        if hasattr(self, 'fullscreen_button') and self.fullscreen_button:
            if self.is_fullscreen:
                self.fullscreen_button.configure(text="⛶ Оконный режим")
            else:
                self.fullscreen_button.configure(text="⛶ Полный экран")
                
        # Обновляем статус бар (если он уже создан)
        if hasattr(self, 'status_bar') and self.status_bar:
            self.status_bar.configure(
                text=f"Режим: {'полноэкранный' if self.is_fullscreen else 'оконный'}"
            )
            self.after(3000, lambda: self._reset_status_bar())

    def _reset_status_bar(self):
        """Сброс статус бара к стандартному сообщению"""
        if hasattr(self, 'status_bar') and self.status_bar:
            self.status_bar.configure(text="Готов к работе")
    
    def center_window(self):
        """Центрирование окна на экране"""
        self.update_idletasks()
        width = self.winfo_width()
        height = self.winfo_height()
        x = (self.winfo_screenwidth() // 2) - (width // 2)
        y = (self.winfo_screenheight() // 2) - (height // 2)
        self.geometry(f'{width}x{height}+{x}+{y}')

    def _setup_modern_fonts(self):
        """Современные шрифты для приложения"""
        self.fonts = {
            "title": ctk.CTkFont(family="Segoe UI", size=20, weight="bold"),
            "subheading": ctk.CTkFont(family="Segoe UI", size=14, weight="bold"),
            "normal": ctk.CTkFont(family="Segoe UI", size=12, weight="normal"),
            "small": ctk.CTkFont(family="Segoe UI", size=10, weight="normal"),
            "button": ctk.CTkFont(family="Segoe UI", size=12, weight="bold"),
            "monospace": ctk.CTkFont(family="Cascadia Code", size=11, weight="normal"),
            "nav": ctk.CTkFont(family="Segoe UI", size=13, weight="normal"),
            "nav_bold": ctk.CTkFont(family="Segoe UI", size=13, weight="bold")
        }

    def _init_ui_attributes(self):
        """Инициализация атрибутов UI"""
        # Атрибуты для агрегации
        self.agg_mode_var = None
        self.count_entry = None
        self.comment_entry = None
        self.agg_create_name_entry = None
        self.agg_create_count_entry = None
        self.create_agg_btn = None
        self.download_agg_btn = None
        self.bulk_agg_btn = None
        self.bulk_agg_name_entry = None
        self.bulk_agg_by_name_btn = None
        self.agg_tabview = None
        self.agg_progress = None
        self.agg_log_text = None
        self.download_printer_combo = None
        self.download_printer_refresh_button = None
        self.download_printer_names: list[str] = []
        self.download_default_printer_name = None

        # Атрибуты для печати этикеток 100x180
        self.label_print_agg_tree = None
        self.label_print_order_tree = None
        self.label_print_button = None
        self.label_print_mfg_entry = None
        self.label_print_exp_entry = None
        self.label_print_quantity_entry = None
        self.label_print_summary_label = None
        self.label_print_log_text = None
        self.label_print_source_title_label = None
        self.label_print_source_hint_label = None
        self.label_print_templates_frame = None
        self.label_print_selected_template_path = None
        self.label_print_printer_combo = None
        self.label_print_printer_refresh_button = None
        self.label_print_printer_names: list[str] = []
        self.label_print_default_printer_name = None
        self.label_print_order_metadata_cache: dict[str, Any] = {}
        self.label_print_order_display_cache: dict[str, dict[str, str]] = {}
        self.label_print_refresh_in_progress = False
        self.label_print_data_loaded = False
        self.label_print_last_refresh_at = 0.0
        self.label_print_template_cards = {}
        self.label_print_templates: list[LabelTemplateInfo] = []
        self.label_print_aggregation_files: list[AggregationCsvInfo] = []
        self.label_print_marking_files: list[AggregationCsvInfo] = []
        self.label_print_orders: list[dict] = []
        self.label_print_agg_by_iid = {}
        self.label_print_order_by_iid = {}
        self.label_print_in_progress = False
        
        # Атрибуты для навигации
        self.sidebar_frame = None
        self.main_content = None
        self.theme_button = None
        self.status_bar = None
        self.connection_indicator = None
        self.nav_buttons = {}
        self.content_frames = {}
        self.active_content_frame = "create"
        
        # Атрибут для управления полноэкранным режимом
        self.is_fullscreen = True

    def _setup_modern_ui(self):
        """Создание современного интерфейса с боковой панелью"""
        # Главный контейнер
        self.main_container = ctk.CTkFrame(self, corner_radius=0)
        self.main_container.pack(fill="both", expand=True)
        
        # Создаем layout с боковой панелью и основным контентом
        self._create_sidebar()
        self._create_main_content()
        
        # Статус бар внизу
        self._create_status_bar()
        
        # Добавляем обработчик клавиши ESC для выхода из полноэкранного режима
        self.bind('<Escape>', self.toggle_fullscreen)
        self.bind('<F11>', self.toggle_fullscreen)

    def _create_sidebar(self):
        """Создание современной боковой панели с улучшенным дизайном"""
        self.sidebar_frame = ctk.CTkFrame(
            self.main_container, 
            width=292,
            corner_radius=0,
            fg_color=self._get_color("bg_secondary")
        )
        self.sidebar_frame.pack(side="left", fill="y")
        self.sidebar_frame.pack_propagate(False)
        
        # Логотип и заголовок с улучшенным дизайном
        logo_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        logo_frame.pack(pady=(30, 25), padx=25, fill="x")
        
        # Современный логотип
        logo_container = ctk.CTkFrame(logo_frame, fg_color="transparent")
        logo_container.pack(fill="x")
        
        # Иконка логотипа в круге
        logo_icon_frame = ctk.CTkFrame(
            logo_container, 
            width=50, 
            height=50,
            corner_radius=25,
            fg_color=self._get_color("primary"),
            bg_color="transparent"
        )
        logo_icon_frame.pack()
        logo_icon_frame.pack_propagate(False)
        
        ctk.CTkLabel(
            logo_icon_frame,
            text="⚡",
            font=("Segoe UI", 20),
            text_color="white"
        ).pack(expand=True)
        
        ctk.CTkLabel(
            logo_container,
            text="Kontur Marking",
            font=ctk.CTkFont(family="Segoe UI", size=18, weight="bold"),
            text_color=self._get_color("text_primary")
        ).pack(pady=(12, 0))
        
        ctk.CTkLabel(
            logo_container,
            text="Management System",
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="normal"),
            text_color=self._get_color("text_secondary")
        ).pack(pady=(2, 0))
        
        # Разделитель
        separator = ctk.CTkFrame(
            logo_frame, 
            height=1, 
            fg_color=self._get_color("secondary")
        )
        separator.pack(fill="x", pady=(20, 0))
        
        # Навигация с современными иконками
        nav_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        nav_frame.pack(pady=15, padx=20, fill="x")
        
        # ИНИЦИАЛИЗИРУЕМ nav_buttons как пустой словарь ПЕРЕД созданием кнопок
        self.nav_buttons = {}
        
        # Современные иконки и названия разделов
        nav_items = [
            ("create", "КМ", "Заказ кодов", self.show_create_frame),
            ("download", "ЗК", "Загрузка кодов", self.show_download_frame),
            ("intro", "ВО", "Ввод в оборот", self.show_intro_frame),
            ("intro_tsd", "ТС", "Задание на ТСД", self.show_intro_tsd_frame),
            ("aggregation", "АК", "Коды агрегации", self.show_aggregation_frame),
            ("label_print", "ПЭ", "Печать этикеток", self.show_label_print_frame),
        ]
        
        nav_font = ctk.CTkFont(family="Segoe UI", size=13, weight="normal")
        nav_font_bold = ctk.CTkFont(family="Segoe UI", size=13, weight="bold")
        nav_icon_font = ctk.CTkFont(family="Segoe UI", size=10, weight="bold")
        
        for nav_id, icon, title, command in nav_items:
            nav_item_frame = ctk.CTkFrame(nav_frame, fg_color="transparent", height=52)
            nav_item_frame.pack(fill="x", pady=3)
            nav_item_frame.pack_propagate(False)
            
            active_indicator = ctk.CTkFrame(
                nav_item_frame, 
                width=4, 
                fg_color="transparent",
                corner_radius=2
            )
            active_indicator.pack(side="left", fill="y", padx=(2, 0))
            
            card = ctk.CTkFrame(
                nav_item_frame,
                height=48,
                corner_radius=12,
                fg_color="transparent",
                border_width=1,
                border_color="transparent",
            )
            card.pack(side="left", fill="x", expand=True, padx=(10, 0))
            card.pack_propagate(False)
            card.grid_columnconfigure(0, minsize=36)
            card.grid_columnconfigure(1, weight=1)

            icon_label = ctk.CTkLabel(
                card,
                text=icon,
                width=38,
                height=28,
                anchor="center",
                font=nav_icon_font,
                text_color=self._get_color("text_secondary"),
                fg_color=self._get_color("secondary"),
                corner_radius=8,
            )
            icon_label.grid(row=0, column=0, sticky="nsw", padx=(14, 10), pady=12)

            text_label = ctk.CTkLabel(
                card,
                text=title,
                anchor="w",
                justify="left",
                font=nav_font,
                text_color=self._get_color("text_primary"),
            )
            text_label.grid(row=0, column=1, sticky="nsew", padx=(0, 14), pady=12)

            clickable_widgets = (nav_item_frame, card, icon_label, text_label)
            for widget in clickable_widgets:
                widget.bind("<Button-1>", lambda event, cb=command: cb())
                widget.bind("<Enter>", lambda event, current_id=nav_id: self._animate_nav_hover(current_id, True))
                widget.bind("<Leave>", lambda event, current_id=nav_id: self._animate_nav_hover(current_id, False))
            
            self.nav_buttons[nav_id] = {
                'card': card,
                'icon': icon_label,
                'label': text_label,
                'indicator': active_indicator,
                'frame': nav_item_frame,
                'font_normal': nav_font,
                'font_bold': nav_font_bold,
            }
        
        # Гибкое пространство между навигацией и нижней частью
        spacer = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent", height=0)
        spacer.pack(fill="both", expand=True)
        
        # Нижняя часть сайдбара с улучшенным дизайном
        bottom_frame = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        bottom_frame.pack(side="bottom", fill="x", padx=20, pady=20)
        
        # Разделитель
        bottom_separator = ctk.CTkFrame(
            bottom_frame, 
            height=1, 
            fg_color=self._get_color("secondary")
        )
        bottom_separator.pack(fill="x", pady=(0, 15))

        
        # Кнопка выхода из полноэкранного режима
        self.fullscreen_button = ctk.CTkButton(
            bottom_frame,
            text="Оконный режим",
            command=self.toggle_fullscreen,
            height=42,
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="normal"),
            fg_color="transparent",
            hover_color=self._get_color("secondary"),
            text_color=self._get_color("text_secondary"),
            border_width=1,
            border_color=self._get_color("secondary"),
            corner_radius=12,
            anchor="w",
        )
        self.fullscreen_button.pack(fill="x")
    
    def _setup_navigation_animations(self):
        """Настройка анимаций для навигации"""
        for nav_id, elements in self.nav_buttons.items():
            for widget_key in ("frame", "card", "icon", "label"):
                widget = elements.get(widget_key)
                if widget is None:
                    continue
                widget.bind('<Enter>', lambda e, current_id=nav_id: self._animate_nav_hover(current_id, True))
                widget.bind('<Leave>', lambda e, current_id=nav_id: self._animate_nav_hover(current_id, False))

    def _animate_nav_hover(self, nav_id, is_hover):
        """Анимация при наведении на элемент навигации"""
        elements = self.nav_buttons.get(nav_id)
        if not elements:
            return

        card = elements.get("card")
        if card is None or nav_id == getattr(self, "active_content_frame", ""):
            return

        if is_hover:
            card.configure(fg_color=self._get_color("secondary"))
        else:
            card.configure(fg_color="transparent")

    def _get_theme_icon(self, theme):
        """Возвращает иконку для кнопки темы"""
        # В реальном приложении здесь должны быть пути к файлам иконок
        # Для примера используем текстовые символы
        if theme == "light":
            return "☀️"
        else:
            return "🌙"

    def _get_fullscreen_icon(self, mode):
        """Возвращает иконку для кнопки полноэкранного режима"""
        if mode == "fullscreen":
            return "⛶"
        else:
            return "⛶"

    def _update_navigation_style(self, active_frame):
        """Обновление стиля навигации с современными эффектами"""
        frame_to_nav_id = {
            "create": "create",
            "download": "download",
            "intro": "intro",
            "intro_tsd": "intro_tsd",
            "aggregation": "aggregation",
            "label_print": "label_print",
        }
        
        active_nav_id = frame_to_nav_id.get(active_frame, "")
        
        for nav_id, elements in self.nav_buttons.items():
            card = elements.get("card")
            icon_label = elements.get("icon")
            text_label = elements.get("label")
            if nav_id == active_nav_id:
                if card is not None:
                    card.configure(
                        fg_color=self._get_color("primary"),
                        border_color=self._get_color("primary"),
                    )
                if text_label is not None:
                    text_label.configure(text_color="white", font=elements['font_bold'])
                if icon_label is not None:
                    icon_label.configure(
                        text_color=self._get_color("primary"),
                        fg_color="white",
                    )
                elements['indicator'].configure(fg_color=self._get_color("accent"))
            else:
                if card is not None:
                    card.configure(
                        fg_color="transparent",
                        border_color="transparent",
                    )
                if text_label is not None:
                    text_label.configure(
                        text_color=self._get_color("text_primary"),
                        font=elements['font_normal'],
                    )
                if icon_label is not None:
                    icon_label.configure(
                        text_color=self._get_color("text_secondary"),
                        fg_color=self._get_color("secondary"),
                    )
                elements['indicator'].configure(fg_color="transparent")

    def _create_main_content(self):
        """Создание основного контента с переключаемыми фреймами"""
        self.main_content = ctk.CTkFrame(self.main_container, corner_radius=0)
        self.main_content.pack(side="right", fill="both", expand=True)
        
        # Создаем фреймы для каждого раздела
        self.content_frames = {}
        
        # Создаем все фреймы
        self._setup_create_frame()
        self._setup_download_frame()
        self._setup_introduction_frame()
        self._setup_introduction_tsd_frame()
        self._setup_aggregation_frame()
        self._setup_label_print_frame()
        
        # Показываем первый фрейм по умолчанию
        self.show_content_frame("create")

    def _create_status_bar(self):
        """Создание современного статус-бара"""
        status_frame = ctk.CTkFrame(
            self.main_container,
            height=30,
            corner_radius=0,
            fg_color=self._get_color("bg_secondary")
        )
        status_frame.pack(side="bottom", fill="x")
        status_frame.pack_propagate(False)

        self.status_bar = ctk.CTkLabel(
            status_frame,
            text="Готов к работе",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        )
        self.status_bar.pack(side="left", padx=20, pady=5)

        # Индикатор подключения
        self.connection_indicator = ctk.CTkLabel(
            status_frame,
            text="● Онлайн",
            font=self.fonts["small"],
            text_color=self._get_color("success")
        )
        self.connection_indicator.pack(side="right", padx=20, pady=5)

    def show_content_frame(self, frame_name):
        """Показывает указанный фрейм и скрывает остальные"""
        for name, frame in self.content_frames.items():
            if name == frame_name:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
        self.active_content_frame = frame_name
        self._update_navigation_style(frame_name)

    def show_create_frame(self):
        """Показать раздел создания заказов"""
        self.show_content_frame("create")

    def show_download_frame(self):
        """Показать раздел загрузки кодов"""
        self.show_content_frame("download")
        self._refresh_download_printers()

    def show_intro_frame(self):
        """Показать раздел введения в оборот"""
        self.show_content_frame("intro")

    def show_intro_tsd_frame(self):
        """Показать раздел введения TSD"""
        self.show_content_frame("intro_tsd")

    def show_aggregation_frame(self):
        """Показать раздел кодов агрегации"""
        self.show_content_frame("aggregation")

    def show_label_print_frame(self):
        """Показать раздел печати этикеток"""
        self.show_content_frame("label_print")
        self._refresh_label_print_data()

    def _get_color(self, color_name):
        """Получение цвета из текущей темы"""
        theme = self.color_themes[self.current_theme]
        return theme.get(color_name, "#FFFFFF")


    def _update_theme_colors(self):
        """Обновление цветов интерфейса при смене темы"""
        if hasattr(self, 'sidebar_frame') and self.sidebar_frame:
            self.sidebar_frame.configure(fg_color=self._get_color("bg_secondary"))
        if hasattr(self, 'fullscreen_button') and self.fullscreen_button:
            self.fullscreen_button.configure(
                hover_color=self._get_color("secondary"),
                text_color=self._get_color("text_secondary"),
                border_color=self._get_color("secondary"),
            )
        
        if hasattr(self, 'status_bar') and self.status_bar:
            status_frame = self.status_bar.master
            if status_frame:
                status_frame.configure(fg_color=self._get_color("bg_secondary"))
            self.status_bar.configure(text_color=self._get_color("text_secondary"))
        
        # Обновляем навигацию с новой структурой
        if hasattr(self, 'nav_buttons') and self.nav_buttons:
            self._update_navigation_style(getattr(self, "active_content_frame", "create"))

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
        except Exception as e:
            logger.error(f"⚠️ Ошибка при очистке перед обновлением: {e}")

    def _check_for_updates_deferred(self):
        """Отложенная проверка обновлений после старта GUI."""
        try:
            update.check_for_updates(
                repo_dir=self._repo_dir,
                pre_update_cleanup=self.cleanup_before_update,
                auto_restart=True,
            )
        except Exception as e:
            logger.error(f"⚠️ Ошибка при проверке обновлений: {e}")

    def _load_history_to_download_list(self):
        """Загружает заказы без заданий на ТСД из истории в download_list"""
        try:
            history_orders = self.history_db.get_orders_without_tsd()

            existing_ids = {item.get("document_id") for item in self.download_list}

            loaded_count = 0
            for order in history_orders:
                if order.get("document_id") not in existing_ids:
                    # Приводим к формату download_list с флагом from_history
                    download_item = {
                        "order_name": order.get("order_name"),
                        "document_id": order.get("document_id"),
                        "status": "Из истории",  # Специальный статус для заказов из истории
                        "filename": order.get("filename"),
                        "simpl": order.get("simpl"),
                        "full_name": order.get("full_name"),
                        "gtin": order.get("gtin"),
                        "history_entry": order,
                        "from_history": True,  # Флаг, что это заказ из истории
                        "downloading": False   # Не скачиваем автоматически
                    }
                    self.download_list.append(download_item)
                    loaded_count += 1
                    print(f"📥 Загружен заказ из истории: {order.get('order_name')} (GTIN: {order.get('gtin')})")

            if hasattr(self, 'tsd_tree'):
                self.update_tsd_tree()
                
            print(f"📚 Всего загружено {loaded_count} заказов из истории (автоскачивание отключено)")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки истории: {e}")
            print(f"❌ Ошибка загрузки истории в download_list: {e}")


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
            # Создаем кастомную тему с нужными шрифтами
            ctk.set_default_color_theme("blue")  # или другая базовая тема
            
            # Для CTkFont можно установить шрифты при создании виджетов
            # или через конфигурацию отдельных виджетов
            
        except Exception as e:
            logger.error(f"Ошибка при установке шрифтов: {e}")

    def _setup_aggregation_frame(self):
        """Современный фрейм кодов агрегации"""
        self.content_frames["aggregation"] = CTkScrollableFrame(self.main_content, corner_radius=0)
        
        # Основной контейнер
        main_frame = ctk.CTkFrame(self.content_frames["aggregation"], corner_radius=15)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Заголовок с иконкой
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 30))
        
        ctk.CTkLabel(
            header_frame,
            text="📊",
            font=("Segoe UI", 48),
            text_color=self._get_color("primary")
        ).pack(side="left", padx=(0, 15))
        
        title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_frame.pack(side="left", fill="y")
        
        ctk.CTkLabel(
            title_frame,
            text="Коды агрегации",
            font=self.fonts["title"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w")
        
        ctk.CTkLabel(
            title_frame,
            text="Загрузка и управление агрегационными кодами",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w")

        self.agg_tabview = ctk.CTkTabview(main_frame, corner_radius=12)
        self.agg_tabview.pack(fill="x", pady=(0, 20))
        self.agg_tabview.add("Создание АК")
        self.agg_tabview.add("Скачивание АК")
        self.agg_tabview.add("Проведение АК")

        label_width = 190
        compact_entry_width = 240
        primary_button_width = 250
        secondary_button_width = 210

        def configure_form_grid(frame):
            frame.grid_columnconfigure(0, weight=0, minsize=label_width)
            frame.grid_columnconfigure(1, weight=1)

        # Таб создания
        create_tab = self.agg_tabview.tab("Создание АК")
        create_card = ctk.CTkFrame(create_tab, corner_radius=12)
        create_card.pack(fill="x", padx=10, pady=10)

        ctk.CTkLabel(
            create_card,
            text="Создание кодов агрегации",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w", padx=20, pady=(20, 10))

        create_input_frame = ctk.CTkFrame(create_card, fg_color="transparent")
        create_input_frame.pack(fill="x", padx=20, pady=(0, 20))
        configure_form_grid(create_input_frame)

        ctk.CTkLabel(
            create_input_frame,
            text="Название:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
            anchor="w",
            width=label_width
        ).grid(row=0, column=0, sticky="w", padx=(0, 18), pady=(0, 12))

        self.agg_create_name_entry = ctk.CTkEntry(
            create_input_frame,
            width=420,
            placeholder_text="Введите название агрегации...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.agg_create_name_entry.grid(row=0, column=1, sticky="ew", pady=(0, 12))

        ctk.CTkLabel(
            create_input_frame,
            text="Количество агрегатов:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
            anchor="w",
            width=label_width
        ).grid(row=1, column=0, sticky="w", padx=(0, 18))

        self.agg_create_count_entry = ctk.CTkEntry(
            create_input_frame,
            width=compact_entry_width,
            placeholder_text="Введите количество...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.agg_create_count_entry.grid(row=1, column=1, sticky="w")

        create_actions_frame = ctk.CTkFrame(create_input_frame, fg_color="transparent")
        create_actions_frame.grid(row=2, column=1, sticky="w", pady=(16, 0))

        self.create_agg_btn = ctk.CTkButton(
            create_actions_frame,
            text="⚡ Генерировать",
            command=self.start_aggregation_generation,
            width=primary_button_width,
            height=45,
            font=self.fonts["button"],
            fg_color=self._get_color("primary"),
            hover_color=self._get_color("accent"),
            corner_radius=8,
            border_width=0
        )
        self.create_agg_btn.pack(anchor="w")

        # Таб скачивания
        download_tab = self.agg_tabview.tab("Скачивание АК")
        download_card = ctk.CTkFrame(download_tab, corner_radius=12)
        download_card.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkLabel(
            download_card,
            text="Поиск и скачивание АК",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w", padx=20, pady=(20, 10))
        
        download_form_frame = ctk.CTkFrame(download_card, fg_color="transparent")
        download_form_frame.pack(fill="x", padx=20, pady=(0, 20))
        configure_form_grid(download_form_frame)

        ctk.CTkLabel(
            download_form_frame,
            text="Режим поиска:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
            anchor="w",
            width=label_width
        ).grid(row=0, column=0, sticky="w", padx=(0, 18), pady=(0, 12))
        
        self.agg_mode_var = ctk.StringVar(value="count")
        mode_options_frame = ctk.CTkFrame(download_form_frame, fg_color="transparent")
        mode_options_frame.grid(row=0, column=1, sticky="w", pady=(0, 12))
        
        ctk.CTkRadioButton(
            mode_options_frame,
            text="🔢 По количеству",
            variable=self.agg_mode_var,
            value="count",
            command=self.toggle_aggregation_mode,
            font=self.fonts["normal"],
            border_color=self._get_color("primary"),
            hover_color=self._get_color("accent")
        ).pack(side="left", padx=(0, 20))
        
        ctk.CTkRadioButton(
            mode_options_frame,
            text="📝 По наименованию", 
            variable=self.agg_mode_var,
            value="comment",
            command=self.toggle_aggregation_mode,
            font=self.fonts["normal"],
            border_color=self._get_color("primary"),
            hover_color=self._get_color("accent")
        ).pack(side="left")

        self.count_frame = ctk.CTkFrame(download_form_frame, fg_color="transparent")
        self.count_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        configure_form_grid(self.count_frame)

        ctk.CTkLabel(
            self.count_frame,
            text="Количество кодов:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
            anchor="w",
            width=label_width
        ).grid(row=0, column=0, sticky="w", padx=(0, 18))
        
        self.count_entry = ctk.CTkEntry(
            self.count_frame,
            width=compact_entry_width,
            placeholder_text="Введите количество...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.count_entry.grid(row=0, column=1, sticky="w")
        
        self.comment_frame = ctk.CTkFrame(download_form_frame, fg_color="transparent")
        self.comment_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 12))
        configure_form_grid(self.comment_frame)
        
        ctk.CTkLabel(
            self.comment_frame,
            text="Наименование товара:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
            anchor="w",
            width=label_width
        ).grid(row=0, column=0, sticky="w", padx=(0, 18))
        
        self.comment_entry = ctk.CTkEntry(
            self.comment_frame,
            width=420,
            placeholder_text="Введите наименование...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.comment_entry.grid(row=0, column=1, sticky="ew")
        self.comment_frame.grid_remove()

        download_actions_frame = ctk.CTkFrame(download_form_frame, fg_color="transparent")
        download_actions_frame.grid(row=2, column=1, sticky="w")

        self.download_agg_btn = ctk.CTkButton(
            download_actions_frame,
            text="🚀 Загрузить коды агрегации",
            command=self.start_aggregation_download,
            width=primary_button_width,
            height=45,
            font=self.fonts["button"],
            fg_color=self._get_color("primary"),
            hover_color=self._get_color("accent"),
            corner_radius=8,
            border_width=0
        )
        self.download_agg_btn.pack(anchor="w")

        # Таб проведения
        conduct_tab = self.agg_tabview.tab("Проведение АК")
        conduct_card = ctk.CTkFrame(conduct_tab, corner_radius=12)
        conduct_card.pack(fill="x", padx=10, pady=10)

        ctk.CTkLabel(
            conduct_card,
            text="Проведение readyForSend АК",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w", padx=20, pady=(20, 10))

        ctk.CTkLabel(
            conduct_card,
            text="Поиск по наименованию использует тот же принцип, что и скачивание АК.",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w", padx=20)

        conduct_form_frame = ctk.CTkFrame(conduct_card, fg_color="transparent")
        conduct_form_frame.pack(fill="x", padx=20, pady=(15, 20))
        configure_form_grid(conduct_form_frame)

        ctk.CTkLabel(
            conduct_form_frame,
            text="Наименование товара:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
            anchor="w",
            width=label_width
        ).grid(row=0, column=0, sticky="w", padx=(0, 18))

        self.bulk_agg_name_entry = ctk.CTkEntry(
            conduct_form_frame,
            width=420,
            placeholder_text="Введите наименование...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.bulk_agg_name_entry.grid(row=0, column=1, sticky="ew")

        conduct_actions_frame = ctk.CTkFrame(conduct_form_frame, fg_color="transparent")
        conduct_actions_frame.grid(row=1, column=1, sticky="w", pady=(16, 0))

        self.bulk_agg_by_name_btn = ctk.CTkButton(
            conduct_actions_frame,
            text="✅ Провести",
            command=self.start_bulk_aggregation_approve_by_name,
            width=secondary_button_width,
            height=45,
            font=self.fonts["button"],
            fg_color=self._get_color("success"),
            hover_color=self._get_color("accent"),
            corner_radius=8,
            border_width=0
        )
        self.bulk_agg_by_name_btn.pack(side="left", padx=(0, 12))

        self.bulk_agg_btn = ctk.CTkButton(
            conduct_actions_frame,
            text="✅ Провести все АК",
            command=self.start_bulk_aggregation_approve,
            width=secondary_button_width,
            height=45,
            font=self.fonts["button"],
            fg_color=self._get_color("primary"),
            hover_color=self._get_color("accent"),
            corner_radius=8,
            border_width=0
        )
        self.bulk_agg_btn.pack(side="left")

        progress_card = ctk.CTkFrame(main_frame, corner_radius=12)
        progress_card.pack(fill="x", pady=(0, 20))

        ctk.CTkLabel(
            progress_card,
            text="Прогресс операции",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w", padx=20, pady=(20, 10))

        progress_frame = ctk.CTkFrame(progress_card, fg_color="transparent")
        progress_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        ctk.CTkLabel(
            progress_frame,
            text="Прогресс:",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w")
        
        self.agg_progress = ctk.CTkProgressBar(
            progress_frame,
            height=6,
            corner_radius=3,
            progress_color=self._get_color("success")
        )
        self.agg_progress.pack(fill="x", pady=(5, 0))
        self.agg_progress.set(0)
        
        # Карточка лога
        log_card = ctk.CTkFrame(main_frame, corner_radius=12)
        log_card.pack(fill="both", expand=True, pady=(0, 20))
        
        ctk.CTkLabel(
            log_card,
            text="📋 Лог операций",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w", padx=20, pady=(20, 10))
        
        # Современное текстовое поле лога
        self.agg_log_text = ctk.CTkTextbox(
            log_card,
            height=200,
            font=self.fonts["monospace"],
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.agg_log_text.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        self.agg_log_text.configure(state="disabled")

    def toggle_aggregation_mode(self):
        """Переключение между режимами поиска кодов агрегации"""
        if self.agg_mode_var.get() == "count":
            self.comment_frame.grid_remove()
            self.count_frame.grid()
        else:
            self.count_frame.grid_remove()
            self.comment_frame.grid()

    def log_aggregation_message(self, message):
        """Добавление сообщения в лог агрегации"""
        try:
            if hasattr(self, 'agg_log_text') and self.agg_log_text is not None:
                self.agg_log_text.configure(state="normal")
                self.agg_log_text.insert("end", f"{message}\n")
                self.agg_log_text.see("end")
                self.agg_log_text.configure(state="disabled")
                self.update_idletasks()
            else:
                print(f"AGG LOG: {message}")
        except Exception as e:
            print(f"Ошибка при логировании в агрегационном табе: {e}")

    def update_aggregation_progress(self, value):
        """Обновление прогресс-бара агрегации"""
        self.agg_progress.set(value)
        self.update_idletasks()

    def _run_in_ui_thread(self, callback, wait=False):
        """Выполняет callback в главном потоке Tk."""
        if threading.current_thread() is threading.main_thread():
            try:
                return callback()
            except (RuntimeError, tk.TclError) as exc:
                logger.debug("Пропускаем UI callback: %s", exc)
                return None

        result = {}
        event = threading.Event()

        def wrapped():
            try:
                result["value"] = callback()
            except Exception as exc:
                result["error"] = exc
            finally:
                event.set()

        try:
            self.after(0, wrapped)
        except (RuntimeError, tk.TclError) as exc:
            logger.debug("Не удалось передать callback в UI-поток: %s", exc)
            return None
        if not wait:
            return None

        event.wait(timeout=5)
        if not event.is_set():
            logger.debug("UI callback не выполнился вовремя")
            return None
        if "error" in result:
            if isinstance(result["error"], (RuntimeError, tk.TclError)):
                logger.debug("Пропускаем UI callback после ошибки: %s", result["error"])
                return None
            raise result["error"]
        return result.get("value")

    def log_aggregation_message_threadsafe(self, message):
        self._run_in_ui_thread(lambda: self.log_aggregation_message(message))

    def update_aggregation_progress_threadsafe(self, processed, total):
        value = 0 if total <= 0 else max(0.0, min(1.0, processed / total))
        self._run_in_ui_thread(lambda: self.update_aggregation_progress(value))

    def set_status_bar_threadsafe(self, message):
        self._run_in_ui_thread(
            lambda: self.status_bar.configure(text=message)
            if hasattr(self, "status_bar") and self.status_bar is not None
            else None
        )

    def set_bulk_aggregation_ui_state(self, running, active_action=None):
        def apply_state():
            if self.create_agg_btn is not None:
                self.create_agg_btn.configure(state="disabled" if running else "normal")
            if self.download_agg_btn is not None:
                self.download_agg_btn.configure(state="disabled" if running else "normal")
            if self.bulk_agg_name_entry is not None:
                self.bulk_agg_name_entry.configure(state="disabled" if running else "normal")
            if self.bulk_agg_by_name_btn is not None:
                self.bulk_agg_by_name_btn.configure(
                    state="disabled" if running else "normal",
                    text="Проведение..." if running and active_action == "by_name" else "✅ Провести",
                )
            if self.bulk_agg_btn is not None:
                self.bulk_agg_btn.configure(
                    state="disabled" if running else "normal",
                    text="Проведение..." if running and active_action == "all" else "✅ Провести все АК",
                )

        self._run_in_ui_thread(apply_state)

    def ask_yes_no_threadsafe(self, title, message):
        return bool(self._run_in_ui_thread(lambda: mbox.askyesno(title, message), wait=True))

    def show_info_threadsafe(self, title, message):
        self._run_in_ui_thread(lambda: mbox.showinfo(title, message))

    def show_error_threadsafe(self, title, message):
        self._run_in_ui_thread(lambda: mbox.showerror(title, message))

    def start_aggregation_download(self):
        """Запуск процесса скачивания кодов агрегации в отдельном потоке"""
        try:
            # Проверяем инициализацию
            if (self.agg_mode_var is None or self.count_entry is None or 
                self.comment_entry is None or self.download_agg_btn is None):
                self.log_aggregation_message("❌ Ошибка: интерфейс не инициализирован")
                return
            
            # Получаем значения
            mode = self.agg_mode_var.get()
            
            if mode == "count":
                count_text = self.count_entry.get().strip()
                if not count_text or not count_text.isdigit():
                    self.log_aggregation_message("❌ Ошибка: введите корректное количество")
                    return
                if int(count_text) <= 0:
                    self.log_aggregation_message("❌ Ошибка: количество должно быть больше 0")
                    return
                target_value = count_text
            else:
                comment_text = self.comment_entry.get().strip()
                if not comment_text:
                    self.log_aggregation_message("❌ Ошибка: введите наименование товара")
                    return
                target_value = comment_text
            
            # Блокируем кнопку на время загрузки
            self.download_agg_btn.configure(state="disabled", text="Загрузка...")
            
            # Запускаем в отдельном потоке
            self.download_executor.submit(
                self.download_aggregation_process, 
                mode, 
                target_value
            )
            
        except Exception as e:
            print(f"Критическая ошибка в start_aggregation_download: {e}")
            self.log_aggregation_message(f"❌ Критическая ошибка: {str(e)}")

    def start_aggregation_generation(self):
        """Запуск процесса создания кодов агрегации в отдельном потоке"""
        try:
            if (
                self.agg_create_name_entry is None
                or self.agg_create_count_entry is None
                or self.create_agg_btn is None
            ):
                self.log_aggregation_message("❌ Ошибка: интерфейс создания не инициализирован")
                return

            comment = self.agg_create_name_entry.get().strip()
            count_text = self.agg_create_count_entry.get().strip()

            if not comment:
                self.log_aggregation_message("❌ Ошибка: введите название")
                return

            if not count_text or not count_text.isdigit():
                self.log_aggregation_message("❌ Ошибка: введите корректное количество агрегатов")
                return

            count = int(count_text)
            if count <= 0:
                self.log_aggregation_message("❌ Ошибка: количество агрегатов должно быть больше 0")
                return

            self.create_agg_btn.configure(state="disabled", text="Генерация...")
            self.download_executor.submit(self.generate_aggregation_process, comment, count)

        except Exception as e:
            print(f"Критическая ошибка в start_aggregation_generation: {e}")
            self.log_aggregation_message(f"❌ Критическая ошибка генерации: {str(e)}")

    def start_bulk_aggregation_approve(self):
        """Запуск массового проведения всех АК, доступных для повторного проведения."""
        try:
            if self.bulk_agg_btn is None:
                self.log_aggregation_message("❌ Ошибка: кнопка проведения не инициализирована")
                return

            self.set_bulk_aggregation_ui_state(True, active_action="all")
            self.log_aggregation_message("🚀 Запускаем проведение АК в статусах readyForSend и approveFailed")
            self.update_aggregation_progress(0)
            self.download_executor.submit(self.bulk_aggregation_approve_process, None)
        except Exception as e:
            logger.exception("Критическая ошибка запуска проведения всех АК")
            self.log_aggregation_message(f"❌ Критическая ошибка запуска: {e}")
            self.set_bulk_aggregation_ui_state(False)

    def start_bulk_aggregation_approve_by_name(self):
        """Запуск проведения АК readyForSend/approveFailed по наименованию."""
        try:
            if self.bulk_agg_name_entry is None or self.bulk_agg_by_name_btn is None:
                self.log_aggregation_message("❌ Ошибка: интерфейс проведения по наименованию не инициализирован")
                return

            comment_filter = self.bulk_agg_name_entry.get().strip()
            if not comment_filter:
                self.log_aggregation_message("❌ Ошибка: введите наименование для проведения АК")
                return

            self.set_bulk_aggregation_ui_state(True, active_action="by_name")
            self.log_aggregation_message(
                f"🚀 Запускаем проведение АК readyForSend/approveFailed по наименованию: {comment_filter}"
            )
            self.update_aggregation_progress(0)
            self.download_executor.submit(self.bulk_aggregation_approve_process, comment_filter)
        except Exception as e:
            logger.exception("Критическая ошибка запуска проведения АК по наименованию")
            self.log_aggregation_message(f"❌ Критическая ошибка запуска: {e}")
            self.set_bulk_aggregation_ui_state(False)

    def bulk_aggregation_approve_process(self, comment_filter=None):
        """Фоновый процесс проверки и проведения АК."""
        try:
            self.log_aggregation_message_threadsafe("🔐 Получаем сессию Контур.Маркировки...")
            status_message = "Проведение АК readyForSend/approveFailed..."
            if comment_filter:
                status_message = f"Проведение АК по наименованию: {comment_filter}"
            self.set_status_bar_threadsafe(status_message)
            session = SessionManager.get_session()

            if not session:
                raise RuntimeError("Не удалось получить сессию Контур.Маркировки")

            summary = self.bulk_aggregation_service.run(
                kontur_session=session,
                cert_provider=lambda: find_certificate_by_thumbprint(THUMBPRINT),
                sign_base64_func=sign_data,
                sign_text_func=sign_text_data,
                log_callback=self.log_aggregation_message_threadsafe,
                progress_callback=self.update_aggregation_progress_threadsafe,
                confirm_callback=self.ask_yes_no_threadsafe,
                comment_filter=comment_filter,
            )

            if comment_filter:
                self.log_aggregation_message_threadsafe(
                    f"🔎 Фильтр по наименованию: {comment_filter}"
                )

            if summary.ready_found == 0:
                self.log_aggregation_message_threadsafe(
                    "ℹ️ АК в статусах readyForSend/approveFailed для проведения не найдены"
                )
            else:
                self.log_aggregation_message_threadsafe("📌 Итоги проведения АК:")
                for line in summary.to_lines():
                    self.log_aggregation_message_threadsafe(f"• {line}")

            self.set_status_bar_threadsafe(
                f"АК: отправлено {summary.sent_for_approve}, ошибок {summary.errors}"
            )
            self.show_info_threadsafe(
                "Проведение АК завершено",
                "\n".join(summary.to_lines()),
            )
        except Exception as e:
            logger.exception("Ошибка проведения АК")
            self.log_aggregation_message_threadsafe(f"❌ Ошибка проведения АК: {e}")
            self.set_status_bar_threadsafe("Ошибка проведения АК")
            self.show_error_threadsafe("Ошибка проведения АК", str(e))
        finally:
            self.set_bulk_aggregation_ui_state(False)
            self._run_in_ui_thread(lambda: self.update_aggregation_progress(0))

    def _initialize_aggregation_widgets(self):
        """Инициализирует виджеты агрегационного таба, если таб уже существует"""
        try:
            # Получаем существующий таб
            tab_name = "📥 Скачивание кодов агрегации"
            if tab_name not in self.tabview._tab_dict:
                print(f"Таб {tab_name} не найден")
                return
            
            # Получаем фрейм таба
            tab_frame = self.tabview._tab_dict[tab_name]
            
            # Ищем виджеты в дочерних элементах
            for child in tab_frame.winfo_children():
                if isinstance(child, ctk.CTkFrame):
                    for widget in child.winfo_children():
                        # Ищем переключатели режимов
                        if isinstance(widget, ctk.CTkFrame):
                            for sub_widget in widget.winfo_children():
                                if isinstance(sub_widget, ctk.CTkRadioButton):
                                    if sub_widget.cget("text") == "По количеству" and sub_widget.cget("variable"):
                                        self.agg_mode_var = sub_widget.cget("variable")
                                        break
                        
                        # Ищем поле ввода количества
                        if isinstance(widget, ctk.CTkFrame) and hasattr(widget, 'winfo_children'):
                            for sub_widget in widget.winfo_children():
                                if isinstance(sub_widget, ctk.CTkEntry) and sub_widget.cget("placeholder_text") == "Введите количество":
                                    self.count_entry = sub_widget
                                
                        # Ищем поле ввода комментария
                        if isinstance(widget, ctk.CTkFrame) and hasattr(widget, 'winfo_children'):
                            for sub_widget in widget.winfo_children():
                                if isinstance(sub_widget, ctk.CTkEntry) and sub_widget.cget("placeholder_text") == "Введите наименование товара":
                                    self.comment_entry = sub_widget
                        
                        # Ищем кнопку загрузки
                        if isinstance(widget, ctk.CTkButton) and "Загрузить коды агрегации" in widget.cget("text"):
                            self.download_agg_btn = widget
                        
                        # Ищем прогресс-бар
                        if isinstance(widget, ctk.CTkProgressBar):
                            self.agg_progress = widget
                        
                        # Ищем текстовое поле лога
                        if isinstance(widget, ctk.CTkTextbox):
                            self.agg_log_text = widget
            
            # Если не нашли переменную, создаем новую
            if not hasattr(self, 'agg_mode_var') or self.agg_mode_var is None:
                self.agg_mode_var = ctk.StringVar(value="count")
                
            print("Виджеты агрегационного таба инициализированы")
            
        except Exception as e:
            print(f"Ошибка при инициализации виджетов агрегации: {e}")

    def download_aggregation_process(self, mode, target_value):
        """Процесс скачивания кодов агрегации с использованием SessionManager"""
        try:
            self.log_aggregation_message("Начинаем загрузку...")
            self.update_aggregation_progress(0.1)
            
            # Получаем сессию через SessionManager
            logger.info("🔐 Получаем сессию...")
            session = SessionManager.get_session()
            self.update_aggregation_progress(0.3)
            
            if not session:
                logger.error("❌ Не удалось получить сессию")
                return
            
            logger.info("✅ Сессия успешно получена")
            self.update_aggregation_progress(0.5)
            
            # Загружаем данные
            self.log_aggregation_message("🚀 Загружаем коды агрегации...")
            limit = int(target_value) if mode == "count" else None
            codes = self.download_aggregate_codes(
                session=session,
                mode=mode,
                target_value=target_value,
                limit=limit
            )
            self.update_aggregation_progress(0.8)
            
            if codes:
                # Сохраняем файл
                if mode == "count":
                    filename = f"Коды_агрегации_{target_value}_шт.csv"
                else:
                    safe_comment = "".join(c for c in target_value if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    safe_comment = safe_comment.replace(' ', '_')[:30]
                    filename = f"{safe_comment}_{len(codes)}.csv"
                
                save_path = self.save_simple_csv(codes, filename)
                self.update_aggregation_progress(1.0)
                
                self.log_aggregation_message(f"✅ Успешно загружено {len(codes)} кодов агрегации")
                self.log_aggregation_message(f"💾 Файл сохранен: {save_path}")
                
                # Показываем уведомление в статус баре
                self.set_status_bar_threadsafe(f"Загружено {len(codes)} кодов агрегации")
            else:
                logger.error("❌ Не удалось загрузить данные")
                
        except Exception as e:
            logger.error(f"❌ Ошибка: {str(e)}")
        finally:
            # Разблокируем кнопку
            self.download_agg_btn.configure(state="normal", text="🚀 Загрузить коды агрегации")
            self.update_aggregation_progress(0)

    def generate_aggregation_process(self, comment, count):
        """Процесс создания кодов агрегации с использованием SessionManager"""
        try:
            self.log_aggregation_message(f"🚀 Создаем агрегационные коды: {comment} ({count} шт.)")
            self.update_aggregation_progress(0.1)

            logger.info("🔐 Получаем сессию для создания агрегации...")
            session = SessionManager.get_session()
            self.update_aggregation_progress(0.35)

            if not session:
                self.log_aggregation_message("❌ Не удалось получить сессию")
                logger.error("❌ Не удалось получить сессию для создания агрегации")
                return

            batch_limit = 99
            remaining = count
            batch_counts = []
            while remaining > 0:
                batch_size = min(batch_limit, remaining)
                batch_counts.append(batch_size)
                remaining -= batch_size

            total_batches = len(batch_counts)
            aggregate_ids = []

            for batch_index, batch_count in enumerate(batch_counts, start=1):
                self.log_aggregation_message(
                    f"📦 Запрос {batch_index}/{total_batches}: создаем {batch_count} кодов"
                )
                batch_ids = self.create_aggregate_codes(
                    session=session,
                    comment=comment,
                    count=batch_count
                )
                aggregate_ids.extend(batch_ids)
                self.log_aggregation_message(
                    f"✅ Запрос {batch_index}/{total_batches} выполнен: получено {len(batch_ids)} кодов"
                )

                progress = 0.35 + (0.45 * batch_index / total_batches)
                self.update_aggregation_progress(progress)

            created_count = len(aggregate_ids)
            self.log_aggregation_message(f"✅ Создано {created_count} агрегационных кодов")

            preview_ids = aggregate_ids[:5]
            for aggregate_id in preview_ids:
                self.log_aggregation_message(f"• {aggregate_id}")

            if created_count > len(preview_ids):
                self.log_aggregation_message(f"… и еще {created_count - len(preview_ids)} кодов")

            self.set_status_bar_threadsafe(f"Создано {created_count} кодов агрегации")
            self.update_aggregation_progress(1.0)

        except Exception as e:
            logger.error(f"❌ Ошибка создания кодов агрегации: {e}")
            self.log_aggregation_message(f"❌ Ошибка создания кодов агрегации: {str(e)}")
        finally:
            if self.create_agg_btn is not None:
                self.create_agg_btn.configure(state="normal", text="⚡ Генерировать")
            self.update_aggregation_progress(0)

    def _setup_create_frame(self):
        """Современный фрейм создания заказов с адаптивным расположением"""
        self.content_frames["create"] = CTkScrollableFrame(self.main_content, corner_radius=0)
        
        # Основной контейнер с уменьшенными отступами и смещением влево
        main_frame = ctk.CTkFrame(self.content_frames["create"], corner_radius=15)
        main_frame.pack(fill="both", expand=True, padx=(0, 5), pady=5)  # Сдвинут влево, уменьшены отступы
        
        # Заголовок с иконкой - компактный
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 10), padx=5)  # Уменьшены отступы
        
        ctk.CTkLabel(
            header_frame,
            text="📦",
            font=("Segoe UI", 28),  # Уменьшен размер иконки
            text_color=self._get_color("primary")
        ).pack(side="left", padx=(0, 5))
        
        title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_frame.pack(side="left", fill="y")
        
        ctk.CTkLabel(
            title_frame,
            text="Создание заказов",
            font=self.fonts["title"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w")
        
        ctk.CTkLabel(
            title_frame,
            text="Добавление и управление позициями заказов",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w")
        
        # Две колонки с адаптивным расположением
        columns_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        columns_frame.pack(fill="both", expand=True, padx=(0, 5))  # Сдвинут влево, уменьшены отступы
        
        # Настраиваем адаптивные колонки с уменьшенной минимальной шириной и разными весами
        columns_frame.grid_columnconfigure(0, weight=1, minsize=250)  # Левая колонка уже
        columns_frame.grid_columnconfigure(1, weight=3, minsize=300)  # Правая колонка шире
        columns_frame.grid_rowconfigure(0, weight=1)
        
        # Левая колонка - форма (фиксированной высоты, без ограничений)
        left_column = ctk.CTkFrame(columns_frame, corner_radius=12)
        left_column.grid(row=0, column=0, sticky="nsew", padx=(0, 3), pady=0)  # Сдвинут влево, уменьшены отступы
        
        # Правая колонка - таблица и лог (с прокруткой если нужно)
        right_column = ctk.CTkFrame(columns_frame, corner_radius=12)
        right_column.grid(row=0, column=1, sticky="nsew", padx=(3, 0), pady=0)  # Уменьшены отступы
        
        # === ЛЕВАЯ КОЛОНКА - ФОРМА (БЕЗ ПРОКРУТКИ, ВСЕГДА ВИДНА) ===
        ctk.CTkLabel(
            left_column, 
            text="Добавление позиции", 
            font=self.fonts["subheading"]
        ).pack(pady=(8, 5), padx=8, anchor="w")  # Уменьшены отступы
        
        # Основной контейнер формы БЕЗ прокрутки - все поля всегда видны
        form_container = ctk.CTkFrame(left_column, fg_color="transparent")
        form_container.pack(fill="both", expand=True, padx=5, pady=3)  # Уменьшены отступы
        
        # Настраиваем grid для form_container
        form_container.grid_columnconfigure(0, weight=0)  # Для лейблов - по содержимому
        form_container.grid_columnconfigure(1, weight=1)  # Для полей - расширяется
        
        row = 0
        
        # Заявка №
        ctk.CTkLabel(form_container, text="Заявка №:", font=self.fonts["normal"], anchor="w").grid(
            row=row, column=0, sticky="ew", padx=(0, 5), pady=5
        )
        self.order_entry = ctk.CTkEntry(
            form_container, 
            placeholder_text="Введите номер заявки", 
            font=self.fonts["normal"],
            width=150  # Уменьшена ширина
        )
        self.order_entry.grid(row=row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        row += 1
        
        # Режим поиска
        ctk.CTkLabel(form_container, text="Режим поиска:", font=self.fonts["normal"], anchor="w").grid(
            row=row, column=0, sticky="ew", padx=(0, 5), pady=5
        )
        
        mode_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        mode_frame.grid(row=row, column=1, sticky="w", padx=(5, 0), pady=5)
        
        self.gtin_var = ctk.StringVar(value="No")
        ctk.CTkRadioButton(
            mode_frame, 
            text="Поиск по GTIN", 
            variable=self.gtin_var, 
            value="Yes",
            command=self.gtin_toggle_mode, 
            font=self.fonts["small"]
        ).pack(side="left", padx=(0, 4))
        ctk.CTkRadioButton(
            mode_frame, 
            text="Выбор опций", 
            variable=self.gtin_var, 
            value="No",
            command=self.gtin_toggle_mode, 
            font=self.fonts["small"]
        ).pack(side="left")
        row += 1
        
        # GTIN frame (изначально скрыт)
        self.gtin_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        # Размещаем в grid, но изначально скрываем
        self.gtin_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=5)
        self.gtin_frame.grid_remove()  # Изначально скрыт
        
        ctk.CTkLabel(self.gtin_frame, text="GTIN:", font=self.fonts["normal"], anchor="w").grid(
            row=0, column=0, sticky="ew", padx=(0, 5)
        )
        self.gtin_entry = ctk.CTkEntry(
            self.gtin_frame, 
            placeholder_text="Введите GTIN", 
            font=self.fonts["normal"],
            width=150
        )
        self.gtin_entry.grid(row=0, column=1, sticky="w", padx=(5, 0))  # Изменено на sticky="w"
        self.gtin_entry.bind("<Return>", lambda e: self.search_by_gtin())
        self._add_entry_context_menu(self.gtin_entry)
        self.gtin_frame.grid_columnconfigure(0, weight=0)
        self.gtin_frame.grid_columnconfigure(1, weight=1)
        row += 1  # Резервируем row для gtin_frame
        
        # Select frame (группа полей для выбора опций)
        self.select_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        self.select_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=5)
        self.select_frame.grid_columnconfigure(0, weight=0)
        self.select_frame.grid_columnconfigure(1, weight=1)
        
        select_row = 0
        
        # Вид товара
        ctk.CTkLabel(self.select_frame, text="Вид товара:", font=self.fonts["normal"], anchor="w").grid(
            row=select_row, column=0, sticky="ew", padx=(0, 5), pady=5
        )
        self.simpl_combo = ctk.CTkComboBox(
            self.select_frame, 
            values=simplified_options,
            command=self.update_options, 
            font=self.fonts["normal"],
            width=150
        )
        self.simpl_combo.grid(row=select_row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        select_row += 1
        
        # Цвет
        self.color_label = ctk.CTkLabel(
            self.select_frame, 
            text="Цвет:", 
            font=self.fonts["normal"],
            anchor="w"
        )
        self.color_label.grid(row=select_row, column=0, sticky="ew", padx=(0, 5), pady=5)
        
        self.color_combo = ctk.CTkComboBox(
            self.select_frame, 
            values=color_options, 
            font=self.fonts["normal"],
            width=150
        )
        self.color_combo.grid(row=select_row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        select_row += 1
        
        # Венчик
        self.venchik_label = ctk.CTkLabel(
            self.select_frame, 
            text="Венчик:", 
            font=self.fonts["normal"],
            anchor="w"
        )
        self.venchik_label.grid(row=select_row, column=0, sticky="ew", padx=(0, 5), pady=5)
        
        self.venchik_combo = ctk.CTkComboBox(
            self.select_frame, 
            values=venchik_options, 
            font=self.fonts["normal"],
            width=150
        )
        self.venchik_combo.grid(row=select_row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        select_row += 1
        
        # Размер
        ctk.CTkLabel(self.select_frame, text="Размер:", font=self.fonts["normal"], anchor="w").grid(
            row=select_row, column=0, sticky="ew", padx=(0, 5), pady=5
        )
        self.size_combo = ctk.CTkComboBox(
            self.select_frame, 
            values=size_options, 
            font=self.fonts["normal"],
            width=150
        )
        self.size_combo.grid(row=select_row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        select_row += 1
        
        # Упаковка
        ctk.CTkLabel(self.select_frame, text="Единиц в упаковке:", font=self.fonts["normal"], anchor="w").grid(
            row=select_row, column=0, sticky="ew", padx=(0, 5), pady=5
        )
        self.units_combo = ctk.CTkComboBox(
            self.select_frame, 
            values=[str(u) for u in units_options], 
            font=self.fonts["normal"],
            width=150
        )
        self.units_combo.grid(row=select_row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        select_row += 1
        row += select_row  # Обновляем основной row
        
        # Количество кодов
        ctk.CTkLabel(form_container, text="Количество кодов:", font=self.fonts["normal"], anchor="w").grid(
            row=row, column=0, sticky="ew", padx=(0, 5), pady=5
        )
        self.codes_entry = ctk.CTkEntry(
            form_container, 
            placeholder_text="Введите количество", 
            font=self.fonts["normal"],
            width=150
        )
        self.codes_entry.grid(row=row, column=1, sticky="w", padx=(5, 0), pady=5)  # Изменено на sticky="w"
        row += 1
        
        # Кнопка добавления - ВСЕГДА ВИДНА ВНИЗУ
        add_btn = ctk.CTkButton(
            form_container, 
            text="➕ Добавить позицию", 
            command=self.add_item,
            height=28,  # Уменьшена высота
            fg_color=self._get_color("success"),
            hover_color="#228B69",
            font=self.fonts["button"],
            corner_radius=8
        )
        add_btn.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(10, 5))
        
        # Пустое пространство (spacer) не нужно, так как grid управляет
        
        # === ПРАВАЯ КОЛОНКА - ТАБЛИЦА И ЛОГ ===
        right_column.grid_rowconfigure(0, weight=2)
        right_column.grid_rowconfigure(1, weight=1)
        right_column.grid_columnconfigure(0, weight=1)
        
        # Таблица в прокручиваемом контейнере
        table_container = ctk.CTkFrame(right_column, corner_radius=8)
        table_container.grid(row=0, column=0, sticky="nsew", pady=(0, 3), padx=3)  # Уменьшены отступы
        
        ctk.CTkLabel(
            table_container, 
            text="Список позиций", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(8, 4), padx=8)  # Уменьшены отступы
        
        # Контейнер для таблицы с прокруткой
        table_scroll_frame = ctk.CTkFrame(table_container, fg_color="transparent")
        table_scroll_frame.pack(fill="both", expand=True, padx=5, pady=(0, 5))  # Уменьшены отступы
        
        # Создаем Treeview с прокруткой
        columns = ("idx", "full_name", "simpl_name", "size", "units_per_pack", "gtin", "codes_count", "order_name", "uid")
        self.tree = ttk.Treeview(table_scroll_frame, columns=columns, show="headings", height=6)
        
        # Настраиваем прокрутку для таблицы
        tree_scrollbar = ttk.Scrollbar(table_scroll_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scrollbar.set)
        
        # Заголовки
        headers = {
            "idx": "№", "full_name": "Наименование", "simpl_name": "Упрощенно",
            "size": "Размер", "units_per_pack": "Упаковка", "gtin": "GTIN",
            "codes_count": "Кодов", "order_name": "Заявка", "uid": "UID"
        }
        
        for col, text in headers.items():
            self.tree.heading(col, text=text)
            # Адаптивная ширина колонок (немного уменьшены)
            if col == "idx":
                self.tree.column(col, width=30, minwidth=30)
            elif col in ["size", "units_per_pack", "codes_count"]:
                self.tree.column(col, width=50, minwidth=40)
            else:
                self.tree.column(col, width=80, minwidth=60)
        
        # Размещаем таблицу и скроллбар
        self.tree.pack(side="left", fill="both", expand=True)
        tree_scrollbar.pack(side="right", fill="y")
        
        # Кнопки управления под таблицей
        btn_frame = ctk.CTkFrame(table_container, fg_color="transparent")
        btn_frame.pack(fill="x", padx=5, pady=5)  # Уменьшены отступы
        
        btn_frame.grid_columnconfigure(0, weight=1)
        btn_frame.grid_columnconfigure(1, weight=1)
        btn_frame.grid_columnconfigure(2, weight=1)
        
        delete_btn = ctk.CTkButton(
            btn_frame, 
            text="🗑️ Удалить", 
            command=self.delete_item, 
            height=28,  # Уменьшена высота
            font=self.fonts["button"],
            fg_color=self._get_color("error"),
            corner_radius=6
        )
        delete_btn.grid(row=0, column=0, sticky="ew", padx=1)
        
        self.execute_btn = ctk.CTkButton(
            btn_frame, 
            text="⚡ Выполнить", 
            command=self.execute_all,
            height=28,
            fg_color=self._get_color("primary"),
            hover_color="#2874A6",
            font=self.fonts["button"],
            corner_radius=6
        )
        self.execute_btn.grid(row=0, column=1, sticky="ew", padx=1)
        
        clear_btn = ctk.CTkButton(
            btn_frame, 
            text="🧹 Очистить", 
            command=self.clear_all, 
            height=28,
            font=self.fonts["button"],
            corner_radius=6
        )
        clear_btn.grid(row=0, column=2, sticky="ew", padx=1)
        
        # Лог в прокручиваемом контейнере
        log_container = ctk.CTkFrame(right_column, corner_radius=8)
        log_container.grid(row=1, column=0, sticky="nsew", pady=(3, 0), padx=3)  # Уменьшены отступы
        
        ctk.CTkLabel(
            log_container, 
            text="Лог операций", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(8, 4), padx=8)  # Уменьшены отступы

        self.log_text = ctk.CTkTextbox(log_container, font=self.fonts["normal"])
        self.log_text.pack(fill="both", expand=True, padx=5, pady=(0, 5))  # Уменьшены отступы
        self.log_text.configure(state="disabled")

        # Контекстное меню для лога
        self.log_text.bind("<Button-3>", self._show_log_context_menu)
        self.log_text.bind("<Control-c>", lambda e: self._copy_log_text())
        self.log_text.bind("<Control-C>", lambda e: self._copy_log_text())
        
        # Стиль для таблицы
        self._configure_treeview_style()

    def search_by_gtin(self):
        """Поиск товара по GTIN и заполнение полей"""
        gtin = self.gtin_entry.get().strip()
        
        if not gtin:
            self._log_message("❌ Введите GTIN для поиска", "error")
            return
            
        try:
            # Используем вашу существующую функцию из get_gtin.py
            full_name, simpl_name = lookup_by_gtin(self.df, gtin)
            
            if full_name and simpl_name:
                # Заполняем поле упрощенного названия
                self.simpl_combo.set(simpl_name)
                
                # Обновляем опции на основе выбранного товара
                self.update_options(simpl_name)
                
                self._log_message(f"✅ Найден товар: {full_name}", "success")
                
                # Автоматически переходим в режим выбора опций для уточнения характеристик
                self.gtin_var.set("No")
                self.gtin_toggle_mode()
                
            else:
                self._log_message(f"❌ Товар с GTIN {gtin} не найден в базе данных", "error")
                
        except Exception as e:
            self._log_message(f"❌ Ошибка поиска: {str(e)}", "error")

    def gtin_toggle_mode(self):
        """Переключение между режимом GTIN и выбором опций"""
        if self.gtin_var.get() == "Yes":
            # Показываем поле GTIN, скрываем выбор опций
            self.select_frame.grid_remove()
            self.gtin_frame.grid()
            self.gtin_entry.focus()
        else:
            # Показываем выбор опций, скрываем поле GTIN
            self.gtin_frame.grid_remove()
            self.select_frame.grid()
            self.simpl_combo.focus()

    

    
    def _setup_download_frame(self):
        """Современный фрейм загрузки кодов"""
        self.content_frames["download"] = CTkScrollableFrame(self.main_content, corner_radius=0)
        
        # Основной контейнер
        main_frame = ctk.CTkFrame(self.content_frames["download"], corner_radius=15)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Заголовок
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            header_frame,
            text="📥",
            font=("Segoe UI", 48),
            text_color=self._get_color("primary")
        ).pack(side="left", padx=(0, 15))
        
        title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_frame.pack(side="left", fill="y")
        
        ctk.CTkLabel(
            title_frame,
            text="Загрузка кодов",
            font=self.fonts["title"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w")
        
        ctk.CTkLabel(
            title_frame,
            text="Скачивание и управление кодами маркировки",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w")
        
        # Две колонки
        columns_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        columns_frame.pack(fill="both", expand=True)
        
        # Верхняя часть - таблица
        table_container = ctk.CTkFrame(columns_frame, corner_radius=8)
        table_container.pack(fill="both", expand=True, pady=(0, 10))
        
        ctk.CTkLabel(
            table_container, 
            text="Список заказов для скачивания", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        table_inner_frame = ctk.CTkFrame(table_container, fg_color="transparent")
        table_inner_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        download_columns = ("order_name", "document_id", "status", "filename")
        self.download_tree = ttk.Treeview(
            table_inner_frame,
            columns=download_columns,
            show="headings",
            height=12,
            selectmode="browse"
        )
        
        headers = {
            "order_name": "Заявка", "status": "Статус", 
            "filename": "Файл", "document_id": "ID заказа"
        }
        
        for col, text in headers.items():
            self.download_tree.heading(col, text=text)
            self.download_tree.column(col, width=150)
        
        scrollbar = ttk.Scrollbar(table_inner_frame, orient="vertical", command=self.download_tree.yview)
        self.download_tree.configure(yscrollcommand=scrollbar.set)
        self.download_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.download_tree.bind("<<TreeviewSelect>>", self._update_download_print_button_state)

        actions_frame = ctk.CTkFrame(columns_frame, corner_radius=8)
        actions_frame.pack(fill="x", pady=(0, 10))

        printer_select_frame = ctk.CTkFrame(actions_frame, fg_color="transparent")
        printer_select_frame.pack(side="left", padx=(15, 10), pady=12)

        ctk.CTkLabel(
            printer_select_frame,
            text="Принтер",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
        ).pack(side="left", padx=(0, 8))

        self.download_printer_combo = ctk.CTkComboBox(
            printer_select_frame,
            values=["Загрузка списка принтеров..."],
            width=320,
            state="readonly",
            command=lambda _: self._update_download_print_button_state(),
            font=self.fonts["normal"],
        )
        self.download_printer_combo.pack(side="left")

        self.download_printer_refresh_button = ctk.CTkButton(
            printer_select_frame,
            text="Обновить",
            command=lambda: self._refresh_download_printers(manual=True),
            width=110,
            fg_color=self._get_color("secondary"),
            hover_color=self._get_color("primary"),
            font=self.fonts["button"],
        )
        self.download_printer_refresh_button.pack(side="left", padx=(8, 0))

        self.download_print_button = ctk.CTkButton(
            actions_frame,
            text="Выполнить печать",
            command=self.print_selected_download_order,
            state="disabled",
            fg_color=self._get_color("primary"),
            hover_color=self._get_color("secondary"),
            font=self.fonts["button"],
        )
        self.download_print_button.pack(side="left", padx=15, pady=12)

        ctk.CTkLabel(
            actions_frame,
            text="Выберите принтер и заявку со скачанным CSV, чтобы отправить её в BarTender.",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(side="left", padx=(0, 15))
        
        # Нижняя часть - лог
        log_container = ctk.CTkFrame(columns_frame, corner_radius=8)
        log_container.pack(fill="both", expand=True, pady=(0, 0))
        
        ctk.CTkLabel(
            log_container, 
            text="Лог скачивания", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        self.download_log_text = ctk.CTkTextbox(log_container, height=150)
        self.download_log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._refresh_download_printers()

    def _setup_label_print_frame(self):
        """Экран печати крупных этикеток 100x180 через BarTender."""
        self.content_frames["label_print"] = CTkScrollableFrame(self.main_content, corner_radius=0)

        main_frame = ctk.CTkFrame(self.content_frames["label_print"], corner_radius=15)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)

        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 20))

        ctk.CTkLabel(
            header_frame,
            text="🖨️",
            font=("Segoe UI", 48),
            text_color=self._get_color("primary"),
        ).pack(side="left", padx=(0, 15))

        title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_frame.pack(side="left", fill="y", expand=True)

        ctk.CTkLabel(
            title_frame,
            text="Печать этикеток",
            font=self.fonts["title"],
            text_color=self._get_color("text_primary"),
        ).pack(anchor="w")

        ctk.CTkLabel(
            title_frame,
            text="Шаблоны 100x180, агрегированные коды и автозаполнение данных из заказа маркировки",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        ).pack(anchor="w")

        ctk.CTkButton(
            header_frame,
            text="Обновить данные",
            command=lambda: self._refresh_label_print_data(manual=True),
            fg_color=self._get_color("secondary"),
            hover_color=self._get_color("primary"),
            font=self.fonts["button"],
            width=170,
        ).pack(side="right")

        workspace_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        workspace_frame.pack(fill="both", expand=True)
        workspace_frame.grid_columnconfigure(0, weight=4)
        workspace_frame.grid_columnconfigure(1, weight=5)

        left_column = ctk.CTkFrame(workspace_frame, corner_radius=12)
        left_column.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        left_column.grid_rowconfigure(0, weight=1)
        left_column.grid_rowconfigure(1, weight=1)
        left_column.grid_columnconfigure(0, weight=1)

        agg_container = ctk.CTkFrame(left_column, corner_radius=10)
        agg_container.grid(row=0, column=0, sticky="nsew", padx=12, pady=(12, 6))
        agg_container.grid_rowconfigure(2, weight=1)
        agg_container.grid_columnconfigure(0, weight=1)

        self.label_print_source_title_label = ctk.CTkLabel(
            agg_container,
            text='Файлы АК из папки "Агрег коды км"',
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary"),
        )
        self.label_print_source_title_label.grid(row=0, column=0, sticky="w", padx=14, pady=(14, 4))

        self.label_print_source_hint_label = ctk.CTkLabel(
            agg_container,
            text="Источник переключается автоматически по выбранному шаблону.",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        )
        self.label_print_source_hint_label.grid(row=1, column=0, sticky="w", padx=14, pady=(0, 8))

        agg_tree_host = ctk.CTkFrame(agg_container, fg_color="transparent")
        agg_tree_host.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        agg_tree_host.grid_rowconfigure(0, weight=1)
        agg_tree_host.grid_columnconfigure(0, weight=1)

        agg_columns = ("name", "folder", "records", "modified")
        self.label_print_agg_tree = ttk.Treeview(
            agg_tree_host,
            columns=agg_columns,
            show="headings",
            height=10,
            selectmode="browse",
        )
        self.label_print_agg_tree.heading("name", text="CSV-файл")
        self.label_print_agg_tree.heading("folder", text="Папка")
        self.label_print_agg_tree.heading("records", text="АК")
        self.label_print_agg_tree.heading("modified", text="Изменен")
        self.label_print_agg_tree.column("name", width=220)
        self.label_print_agg_tree.column("folder", width=160)
        self.label_print_agg_tree.column("records", width=60, anchor="center")
        self.label_print_agg_tree.column("modified", width=125, anchor="center")

        agg_scrollbar = ttk.Scrollbar(agg_tree_host, orient="vertical", command=self.label_print_agg_tree.yview)
        self.label_print_agg_tree.configure(yscrollcommand=agg_scrollbar.set)
        self.label_print_agg_tree.grid(row=0, column=0, sticky="nsew")
        agg_scrollbar.grid(row=0, column=1, sticky="ns")
        self.label_print_agg_tree.bind("<<TreeviewSelect>>", self._update_label_print_summary)

        order_container = ctk.CTkFrame(left_column, corner_radius=10)
        order_container.grid(row=1, column=0, sticky="nsew", padx=12, pady=(6, 12))
        order_container.grid_rowconfigure(1, weight=1)
        order_container.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            order_container,
            text="Заказы кодов маркировки",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary"),
        ).grid(row=0, column=0, sticky="w", padx=14, pady=(14, 8))

        order_tree_host = ctk.CTkFrame(order_container, fg_color="transparent")
        order_tree_host.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
        order_tree_host.grid_rowconfigure(0, weight=1)
        order_tree_host.grid_columnconfigure(0, weight=1)

        order_columns = ("order_name", "document_id", "size", "units", "color")
        self.label_print_order_tree = ttk.Treeview(
            order_tree_host,
            columns=order_columns,
            show="headings",
            height=10,
            selectmode="browse",
        )
        self.label_print_order_tree.heading("order_name", text="Заявка")
        self.label_print_order_tree.heading("document_id", text="ID заказа")
        self.label_print_order_tree.heading("size", text="Размер")
        self.label_print_order_tree.heading("units", text="В упаковке")
        self.label_print_order_tree.heading("color", text="Цвет")
        self.label_print_order_tree.column("order_name", width=260)
        self.label_print_order_tree.column("document_id", width=160)
        self.label_print_order_tree.column("size", width=70, anchor="center")
        self.label_print_order_tree.column("units", width=90, anchor="center")
        self.label_print_order_tree.column("color", width=120)

        order_scrollbar = ttk.Scrollbar(order_tree_host, orient="vertical", command=self.label_print_order_tree.yview)
        self.label_print_order_tree.configure(yscrollcommand=order_scrollbar.set)
        self.label_print_order_tree.grid(row=0, column=0, sticky="nsew")
        order_scrollbar.grid(row=0, column=1, sticky="ns")
        self.label_print_order_tree.bind("<<TreeviewSelect>>", self._on_label_print_order_selected)

        right_column = ctk.CTkFrame(workspace_frame, corner_radius=12)
        right_column.grid(row=0, column=1, sticky="nsew")

        template_container = ctk.CTkFrame(right_column, corner_radius=10)
        template_container.pack(fill="both", expand=True, padx=12, pady=(12, 8))

        template_title = ctk.CTkFrame(template_container, fg_color="transparent")
        template_title.pack(fill="x", padx=14, pady=(14, 8))

        ctk.CTkLabel(
            template_title,
            text="Шаблоны BarTender 100x180",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary"),
        ).pack(side="left")

        ctk.CTkLabel(
            template_title,
            text="Латекс, Нитрил, HR / стерилка / Хирургия",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        ).pack(side="left", padx=(10, 0))

        self.label_print_templates_frame = CTkScrollableFrame(template_container, height=260, fg_color="transparent")
        self.label_print_templates_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        settings_container = ctk.CTkFrame(right_column, corner_radius=10)
        settings_container.pack(fill="x", padx=12, pady=(0, 8))
        settings_container.grid_columnconfigure(0, weight=1)
        settings_container.grid_columnconfigure(1, weight=1)
        settings_container.grid_columnconfigure(2, weight=1)

        ctk.CTkLabel(
            settings_container,
            text="Параметры печати",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary"),
        ).grid(row=0, column=0, columnspan=3, sticky="w", padx=14, pady=(14, 10))

        ctk.CTkLabel(settings_container, text="Дата изготовления", font=self.fonts["normal"]).grid(
            row=1, column=0, sticky="w", padx=(14, 8), pady=(0, 6)
        )
        ctk.CTkLabel(settings_container, text="Срок годности", font=self.fonts["normal"]).grid(
            row=1, column=1, sticky="w", padx=8, pady=(0, 6)
        )
        ctk.CTkLabel(settings_container, text="Количество", font=self.fonts["normal"]).grid(
            row=1, column=2, sticky="w", padx=(8, 14), pady=(0, 6)
        )

        self.label_print_mfg_entry = ctk.CTkEntry(
            settings_container,
            placeholder_text="2026-01",
            font=self.fonts["normal"],
        )
        self.label_print_mfg_entry.grid(row=2, column=0, sticky="ew", padx=(14, 8), pady=(0, 12))

        self.label_print_exp_entry = ctk.CTkEntry(
            settings_container,
            placeholder_text="2031-01",
            font=self.fonts["normal"],
        )
        self.label_print_exp_entry.grid(row=2, column=1, sticky="ew", padx=8, pady=(0, 12))

        self.label_print_quantity_entry = ctk.CTkEntry(
            settings_container,
            placeholder_text="Например, 500",
            font=self.fonts["normal"],
        )
        self.label_print_quantity_entry.grid(row=2, column=2, sticky="ew", padx=(8, 14), pady=(0, 12))

        ctk.CTkLabel(
            settings_container,
            text="Размер этикетки: 100x180. Для HR количество вводится вручную, для стерилки и хирургии берется из GTIN.",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        ).grid(row=3, column=0, columnspan=3, sticky="w", padx=14, pady=(0, 12))

        summary_container = ctk.CTkFrame(right_column, corner_radius=10)
        summary_container.pack(fill="x", padx=12, pady=(0, 8))

        ctk.CTkLabel(
            summary_container,
            text="Сводка автозаполнения",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary"),
        ).pack(anchor="w", padx=14, pady=(14, 8))

        self.label_print_summary_label = ctk.CTkLabel(
            summary_container,
            text="Выберите шаблон, CSV и заявку, затем заполните дату изготовления и срок годности.",
            justify="left",
            anchor="w",
            wraplength=620,
            font=self.fonts["normal"],
            text_color=self._get_color("text_secondary"),
        )
        self.label_print_summary_label.pack(fill="x", padx=14, pady=(0, 14))

        actions_container = ctk.CTkFrame(right_column, corner_radius=10)
        actions_container.pack(fill="x", padx=12, pady=(0, 8))

        printer_actions_frame = ctk.CTkFrame(actions_container, fg_color="transparent")
        printer_actions_frame.pack(side="left", padx=(14, 0), pady=14)

        ctk.CTkLabel(
            printer_actions_frame,
            text="Принтер",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary"),
        ).pack(side="left", padx=(0, 8))

        self.label_print_printer_combo = ctk.CTkComboBox(
            printer_actions_frame,
            values=["Загрузка списка принтеров..."],
            width=320,
            state="readonly",
            command=lambda _: self._update_label_print_button_state(),
            font=self.fonts["normal"],
        )
        self.label_print_printer_combo.pack(side="left")

        self.label_print_printer_refresh_button = ctk.CTkButton(
            printer_actions_frame,
            text="Обновить",
            command=lambda: self._refresh_label_print_printers(manual=True),
            width=110,
            fg_color=self._get_color("secondary"),
            hover_color=self._get_color("primary"),
            font=self.fonts["button"],
        )
        self.label_print_printer_refresh_button.pack(side="left", padx=(8, 14))

        self.label_print_button = ctk.CTkButton(
            actions_container,
            text="Выполнить печать",
            command=self.print_selected_100x180_labels,
            state="disabled",
            fg_color=self._get_color("primary"),
            hover_color=self._get_color("secondary"),
            font=self.fonts["button"],
            height=42,
        )
        self.label_print_button.pack(side="left", padx=14, pady=14)

        ctk.CTkLabel(
            actions_container,
            text="BarTender получит выбранный CSV как базу данных и отправит печать на выбранный принтер.",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        ).pack(side="left", padx=(0, 14))

        log_container = ctk.CTkFrame(right_column, corner_radius=10)
        log_container.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        ctk.CTkLabel(
            log_container,
            text="Лог печати этикеток",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary"),
        ).pack(anchor="w", padx=14, pady=(14, 8))

        self.label_print_log_text = ctk.CTkTextbox(log_container, height=120, font=self.fonts["normal"])
        self.label_print_log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.label_print_log_text.configure(state="disabled")

        for entry in (self.label_print_mfg_entry, self.label_print_exp_entry, self.label_print_quantity_entry):
            self._add_entry_context_menu(entry)
            entry.bind("<KeyRelease>", self._update_label_print_summary)

        self._set_default_date_range(self.label_print_mfg_entry, self.label_print_exp_entry)

    def _refresh_label_print_data(self, manual: bool = False):
        if self.label_print_refresh_in_progress:
            if manual:
                self.label_print_log_insert("Обновление вкладки печати этикеток уже выполняется.")
            return

        now = time.time()
        needs_refresh = manual or not self.label_print_data_loaded
        if not needs_refresh and now - self.label_print_last_refresh_at >= LABEL_PRINT_REFRESH_CACHE_SECONDS:
            needs_refresh = True

        if not needs_refresh:
            self._apply_label_print_loaded_data(manual=False)
            return

        self.label_print_refresh_in_progress = True
        self._update_label_print_button_state()
        if self.label_print_summary_label is not None:
            self.label_print_summary_label.configure(
                text="Загружаю данные для печати этикеток: шаблоны, CSV, историю заказов и список принтеров..."
            )

        threading.Thread(
            target=self._load_label_print_data_worker,
            args=(manual,),
            daemon=True,
            name="LabelPrintDataLoader",
        ).start()

    def _load_label_print_data_worker(self, manual: bool):
        try:
            printer_names, default_printer = list_installed_printers()
            templates = list_100x180_templates()
            aggregation_files = list_aggregation_csv_files()
            marking_files = list_marking_csv_files()
            history_orders = self.history_db.get_all_orders()
            orders = sorted(
                history_orders,
                key=lambda order: str(order.get("updated_at") or order.get("created_at") or ""),
                reverse=True,
            )

            metadata_cache: dict[str, Any] = {}
            display_cache: dict[str, dict[str, str]] = {}
            for order in orders:
                document_id = str(order.get("document_id") or "")
                try:
                    metadata = resolve_order_metadata(order, self.df)
                    metadata_cache[document_id] = metadata
                    display_cache[document_id] = {
                        "size": metadata.size or "-",
                        "units": str(metadata.units_per_pack or "-"),
                        "color": metadata.color or "-",
                    }
                except Exception as exc:
                    metadata_cache[document_id] = exc
                    display_cache[document_id] = {"size": "-", "units": "-", "color": "-"}

            payload = {
                "printers": printer_names,
                "default_printer": default_printer,
                "templates": templates,
                "aggregation_files": aggregation_files,
                "marking_files": marking_files,
                "orders": orders,
                "metadata_cache": metadata_cache,
                "display_cache": display_cache,
                "manual": manual,
            }
            self.after(0, lambda: self._apply_label_print_data_payload(payload))
        except Exception as exc:
            self.after(0, lambda err=str(exc), is_manual=manual: self._on_label_print_data_failed(err, is_manual))

    def _apply_label_print_data_payload(self, payload: dict[str, Any]):
        self.label_print_printer_names = payload["printers"]
        self.label_print_default_printer_name = payload["default_printer"]
        self.label_print_templates = payload["templates"]
        self.label_print_aggregation_files = payload["aggregation_files"]
        self.label_print_marking_files = payload["marking_files"]
        self.label_print_orders = payload["orders"]
        self.label_print_order_metadata_cache = payload["metadata_cache"]
        self.label_print_order_display_cache = payload["display_cache"]
        self.label_print_data_loaded = True
        self.label_print_last_refresh_at = time.time()
        self.label_print_refresh_in_progress = False
        self._apply_label_print_loaded_data(manual=bool(payload.get("manual")))

    def _on_label_print_data_failed(self, error_message: str, manual: bool):
        self.label_print_refresh_in_progress = False
        self._update_label_print_button_state()
        if self.label_print_summary_label is not None:
            self.label_print_summary_label.configure(
                text=f"Не удалось загрузить данные для печати этикеток: {error_message}"
            )
        self.label_print_log_insert(f"Ошибка обновления вкладки печати этикеток: {error_message}")

    def _apply_label_print_loaded_data(self, manual: bool):
        self._refresh_label_print_printers()
        self._refresh_label_print_templates()
        self._refresh_label_print_aggregation_files()
        self._refresh_label_print_orders()
        self._apply_label_print_template_mode()
        self._auto_select_label_print_source_for_order()
        self._update_label_print_summary()

        if manual:
            self.label_print_log_insert(
                f"Обновлены данные для печати: шаблоны {len(self.label_print_templates)}, "
                f"CSV АК {len(self.label_print_aggregation_files)}, CSV КМ {len(self.label_print_marking_files)}, "
                f"заказы {len(self.label_print_orders)}, принтеры {len(self.label_print_printer_names)}"
            )
    def _refresh_label_print_templates(self):
        current_path = self.label_print_selected_template_path

        if current_path and not any(template.path == current_path for template in self.label_print_templates):
            self.label_print_selected_template_path = None

        self._render_label_print_template_cards()
    def _render_label_print_template_cards(self):
        if self.label_print_templates_frame is None:
            return

        for child in self.label_print_templates_frame.winfo_children():
            child.destroy()

        self.label_print_template_cards = {}

        if not self.label_print_templates:
            ctk.CTkLabel(
                self.label_print_templates_frame,
                text='Шаблоны в папке "BarTender наклейки 100х180" не найдены.',
                justify="left",
                wraplength=580,
                text_color=self._get_color("warning"),
                font=self.fonts["normal"],
            ).pack(anchor="w", padx=10, pady=10)
            return

        grouped_templates: dict[str, list[LabelTemplateInfo]] = {}
        for template in self.label_print_templates:
            grouped_templates.setdefault(template.category, []).append(template)

        for category, templates in grouped_templates.items():
            ctk.CTkLabel(
                self.label_print_templates_frame,
                text=category,
                font=self.fonts["subheading"],
                text_color=self._get_color("text_primary"),
            ).pack(anchor="w", padx=8, pady=(8, 6))

            cards_grid = ctk.CTkFrame(self.label_print_templates_frame, fg_color="transparent")
            cards_grid.pack(fill="x", padx=4, pady=(0, 8))
            cards_grid.grid_columnconfigure(0, weight=1)
            cards_grid.grid_columnconfigure(1, weight=1)

            for index, template in enumerate(templates):
                is_selected = template.path == self.label_print_selected_template_path
                row = index // 2
                column = index % 2

                card = ctk.CTkFrame(
                    cards_grid,
                    corner_radius=12,
                    border_width=2,
                    border_color=self._get_color("accent") if is_selected else self._get_color("secondary"),
                    fg_color=self._get_color("primary") if is_selected else self._get_color("bg_secondary"),
                    cursor="hand2",
                )
                card.grid(row=row, column=column, sticky="nsew", padx=6, pady=6)

                title_color = "white" if is_selected else self._get_color("text_primary")
                secondary_color = "#EAF4FF" if is_selected else self._get_color("text_secondary")

                title_label = ctk.CTkLabel(
                    card,
                    text=template.name,
                    justify="left",
                    wraplength=250,
                    font=self.fonts["button"],
                    text_color=title_color,
                )
                title_label.pack(anchor="w", padx=12, pady=(12, 6))

                path_label = ctk.CTkLabel(
                    card,
                    text=template.relative_path,
                    justify="left",
                    wraplength=250,
                    font=self.fonts["small"],
                    text_color=secondary_color,
                )
                path_label.pack(anchor="w", padx=12, pady=(0, 10))

                source_label = ctk.CTkLabel(
                    card,
                    text="Источник: Коды км" if template.data_source_kind == MARKING_SOURCE_KIND else "Источник: Агрег коды км",
                    justify="left",
                    wraplength=250,
                    font=self.fonts["small"],
                    text_color=secondary_color,
                )
                source_label.pack(anchor="w", padx=12, pady=(0, 8))

                badge_label = ctk.CTkLabel(
                    card,
                    text="Выбран" if is_selected else "Нажмите для выбора",
                    font=self.fonts["small"],
                    text_color="white" if is_selected else self._get_color("primary"),
                    fg_color=self._get_color("accent") if is_selected else "transparent",
                    corner_radius=8,
                    padx=10,
                    pady=4,
                )
                badge_label.pack(anchor="w", padx=12, pady=(0, 12))

                for widget in (card, title_label, path_label, source_label, badge_label):
                    widget.bind("<Button-1>", lambda event, path=template.path: self._select_label_template(path))

                self.label_print_template_cards[template.path] = card

    def _select_label_template(self, template_path: str):
        self.label_print_selected_template_path = template_path
        self._render_label_print_template_cards()
        self._apply_label_print_template_mode()
        self._refresh_label_print_aggregation_files()
        self._auto_select_label_print_source_for_order()
        self._update_label_print_summary()

    def _get_selected_label_template(self) -> LabelTemplateInfo | None:
        if not self.label_print_selected_template_path:
            return None

        for template in self.label_print_templates:
            if template.path == self.label_print_selected_template_path:
                return template

        return None

    def _get_label_print_source_kind(self) -> str:
        selected_template = self._get_selected_label_template()
        if selected_template is None:
            return AGGREGATION_SOURCE_KIND
        return selected_template.data_source_kind

    def _apply_label_print_template_mode(self):
        source_kind = self._get_label_print_source_kind()
        if self.label_print_quantity_entry is None:
            return

        previous_state = self.label_print_quantity_entry.cget("state")

        if self.label_print_source_title_label is not None:
            if source_kind == MARKING_SOURCE_KIND:
                self.label_print_source_title_label.configure(text='Файлы КМ из папки "Коды км"')
            else:
                self.label_print_source_title_label.configure(text='Файлы АК из папки "Агрег коды км"')

        if self.label_print_source_hint_label is not None:
            if source_kind == MARKING_SOURCE_KIND:
                self.label_print_source_hint_label.configure(
                    text="Для шаблонов из 'стерилка' и 'Хирургия' используется CSV с кодами маркировки."
                )
            else:
                self.label_print_source_hint_label.configure(
                    text="Для шаблонов из 'Латекс, Нитрил, HR' используется CSV с агрегационными кодами."
                )

        if source_kind == MARKING_SOURCE_KIND:
            order_data, metadata_or_error = self._resolve_selected_label_order_metadata()
            quantity_text = ""
            if order_data and not isinstance(metadata_or_error, Exception) and metadata_or_error is not None:
                quantity_text = str(metadata_or_error.units_per_pack)

            self.label_print_quantity_entry.configure(state="normal")
            self.label_print_quantity_entry.delete(0, "end")
            if quantity_text:
                self.label_print_quantity_entry.insert(0, quantity_text)
            self.label_print_quantity_entry.configure(state="disabled", placeholder_text="Берется из GTIN")
        else:
            current_value = self.label_print_quantity_entry.get().strip()
            self.label_print_quantity_entry.configure(state="normal", placeholder_text="Например, 500")
            if previous_state == "disabled":
                current_value = ""
                self.label_print_quantity_entry.delete(0, "end")
            if current_value and not current_value.isdigit():
                self.label_print_quantity_entry.delete(0, "end")

    def _on_label_print_order_selected(self, event=None):
        self._apply_label_print_template_mode()
        self._auto_select_label_print_source_for_order()
        self._update_label_print_summary()

    def _auto_select_label_print_source_for_order(self):
        source_kind = self._get_label_print_source_kind()
        if source_kind != MARKING_SOURCE_KIND or self.label_print_agg_tree is None:
            return

        selected_order = self._get_selected_label_order()
        if selected_order is None:
            return

        resolved_path = self._resolve_order_csv_path(selected_order)
        if not resolved_path:
            return

        for item_iid, file_info in self.label_print_agg_by_iid.items():
            if os.path.normcase(file_info.path) == os.path.normcase(resolved_path):
                self.label_print_agg_tree.selection_set(item_iid)
                self.label_print_agg_tree.focus(item_iid)
                return
    def _refresh_label_print_aggregation_files(self):
        if self.label_print_agg_tree is None:
            return

        current_selection = self._get_selected_aggregation_csv()
        selected_path = current_selection.path if current_selection else None
        source_kind = self._get_label_print_source_kind()

        if source_kind == MARKING_SOURCE_KIND:
            files_to_show = self.label_print_marking_files
            self.label_print_agg_tree.heading("records", text="КМ")
        else:
            files_to_show = self.label_print_aggregation_files
            self.label_print_agg_tree.heading("records", text="АК")

        self.label_print_agg_by_iid = {}

        for item_id in self.label_print_agg_tree.get_children():
            self.label_print_agg_tree.delete(item_id)

        selected_iid = None
        for index, file_info in enumerate(files_to_show):
            item_iid = f"source_{index}"
            self.label_print_agg_by_iid[item_iid] = file_info
            modified_at = datetime.fromtimestamp(file_info.modified_timestamp).strftime("%d.%m.%Y %H:%M")
            self.label_print_agg_tree.insert(
                "",
                "end",
                iid=item_iid,
                values=(file_info.name, file_info.folder_name, file_info.record_count, modified_at),
            )
            if selected_path and file_info.path == selected_path:
                selected_iid = item_iid

        if selected_iid:
            self.label_print_agg_tree.selection_set(selected_iid)
            self.label_print_agg_tree.focus(selected_iid)
    def _refresh_label_print_orders(self):
        if self.label_print_order_tree is None:
            return

        current_selection = self._get_selected_label_order()
        selected_document_id = str(current_selection.get("document_id") or "") if current_selection else ""
        self.label_print_order_by_iid = {}

        for item_id in self.label_print_order_tree.get_children():
            self.label_print_order_tree.delete(item_id)

        selected_iid = None
        for index, order in enumerate(self.label_print_orders):
            item_iid = f"order_{index}"
            self.label_print_order_by_iid[item_iid] = order

            document_id = str(order.get("document_id") or "")
            display_data = self.label_print_order_display_cache.get(
                document_id,
                {"size": "-", "units": "-", "color": "-"},
            )
            self.label_print_order_tree.insert(
                "",
                "end",
                iid=item_iid,
                values=(
                    str(order.get("order_name") or ""),
                    document_id,
                    display_data.get("size", "-"),
                    display_data.get("units", "-"),
                    display_data.get("color", "-"),
                ),
            )
            if selected_document_id and document_id == selected_document_id:
                selected_iid = item_iid

        if selected_iid:
            self.label_print_order_tree.selection_set(selected_iid)
            self.label_print_order_tree.focus(selected_iid)

    def _get_selected_aggregation_csv(self) -> AggregationCsvInfo | None:
        if self.label_print_agg_tree is None:
            return None

        selected_items = self.label_print_agg_tree.selection()
        if not selected_items:
            return None

        return self.label_print_agg_by_iid.get(selected_items[0])

    def _get_selected_label_order(self) -> dict | None:
        if self.label_print_order_tree is None:
            return None

        selected_items = self.label_print_order_tree.selection()
        if not selected_items:
            return None

        return self.label_print_order_by_iid.get(selected_items[0])

    def _resolve_selected_label_order_metadata(self):
        order_data = self._get_selected_label_order()
        if not order_data:
            return None, None

        document_id = str(order_data.get("document_id") or "")
        if document_id in self.label_print_order_metadata_cache:
            return order_data, self.label_print_order_metadata_cache[document_id]

        try:
            metadata = resolve_order_metadata(order_data, self.df)
            self.label_print_order_metadata_cache[document_id] = metadata
            return order_data, metadata
        except Exception as exc:
            self.label_print_order_metadata_cache[document_id] = exc
            return order_data, exc

    def _update_label_print_summary(self, event=None):
        if self.label_print_summary_label is None:
            return

        selected_template = self._get_selected_label_template()
        source_file = self._get_selected_aggregation_csv()
        order_data, order_metadata_or_error = self._resolve_selected_label_order_metadata()
        source_kind = self._get_label_print_source_kind()
        source_title = "Коды км" if source_kind == MARKING_SOURCE_KIND else "Агрег коды км"
        record_label = "кодов маркировки" if source_kind == MARKING_SOURCE_KIND else "агрегационных кодов"

        summary_lines = [
            f"Шаблон: {selected_template.relative_path if selected_template else 'не выбран'}",
            f"Источник CSV: {source_title} | файл: {source_file.name if source_file else 'не выбран'}",
            f"Заявка: {str(order_data.get('order_name') or '') if order_data else 'не выбрана'}",
        ]

        if source_file:
            summary_lines.append(
                f"Записей в CSV: {source_file.record_count} {record_label} | папка: {source_file.folder_name}"
            )

        if order_data and isinstance(order_metadata_or_error, Exception):
            summary_lines.append(f"Ошибка чтения заявки: {order_metadata_or_error}")
        elif order_data and order_metadata_or_error is not None:
            metadata = order_metadata_or_error
            summary_lines.extend(
                [
                    f"Размер: {metadata.size} | Партия: {metadata.batch}",
                    f"Цвет: {metadata.color} | Единиц в упаковке: {metadata.units_per_pack}",
                    f"GTIN: {metadata.gtin}",
                ]
            )

        manufacture_date = self.label_print_mfg_entry.get().strip() if self.label_print_mfg_entry else ""
        expiration_date = self.label_print_exp_entry.get().strip() if self.label_print_exp_entry else ""
        quantity_text = self.label_print_quantity_entry.get().strip() if self.label_print_quantity_entry else ""

        summary_lines.append(
            f"Дата изготовления: {manufacture_date or 'не заполнена'} | Срок годности: {expiration_date or 'не заполнен'}"
        )

        if source_kind == MARKING_SOURCE_KIND:
            if order_data and not isinstance(order_metadata_or_error, Exception) and order_metadata_or_error is not None:
                quantity_value = order_metadata_or_error.units_per_pack
                summary_lines.append(
                    f"Количество: {quantity_value} "
                    f"{self._pluralize_label_print_ru(quantity_value, 'пара', 'пары', 'пар')} "
                    "(из GTIN / nomenclature.xlsx)"
                )
                summary_lines.append("Сериализация: номер этикетки начнется с 1 и увеличится для каждой строки CSV.")
            else:
                summary_lines.append("Количество: будет взято автоматически из GTIN / nomenclature.xlsx")
        else:
            if quantity_text:
                try:
                    quantity_value = int(quantity_text.replace(" ", ""))
                    quantity_line = (
                        f"Количество: {quantity_value} "
                        f"{self._pluralize_label_print_ru(quantity_value, 'пара', 'пары', 'пар')}"
                    )

                    if order_data and not isinstance(order_metadata_or_error, Exception) and order_metadata_or_error is not None:
                        units_per_pack = order_metadata_or_error.units_per_pack
                        if quantity_value > 0 and quantity_value % units_per_pack == 0:
                            dispenser_count = quantity_value // units_per_pack
                            quantity_line += (
                                f" | ({dispenser_count} "
                                f"{self._pluralize_label_print_ru(dispenser_count, 'диспенсер', 'диспенсера', 'диспенсеров')} "
                                f"по {units_per_pack} {self._pluralize_label_print_ru(units_per_pack, 'пара', 'пары', 'пар')})"
                            )
                        else:
                            quantity_line += f" | значение должно быть кратно {units_per_pack}"

                    summary_lines.append(quantity_line)
                except ValueError:
                    summary_lines.append("Количество: введите целое число")
            else:
                summary_lines.append("Количество: не заполнено")

        self.label_print_summary_label.configure(text="\n".join(summary_lines))
        self._update_label_print_button_state()

    def _pluralize_label_print_ru(self, value: int, singular: str, few: str, many: str) -> str:
        remainder10 = value % 10
        remainder100 = value % 100

        if remainder10 == 1 and remainder100 != 11:
            return singular
        if remainder10 in (2, 3, 4) and remainder100 not in (12, 13, 14):
            return few
        return many

    def _refresh_label_print_printers(self, manual: bool = False):
        current_printer = self._get_selected_label_print_printer()
        printer_names = self.label_print_printer_names
        default_printer = self.label_print_default_printer_name
        is_busy = bool(getattr(self, "label_print_in_progress", False))
        is_loading = bool(getattr(self, "label_print_refresh_in_progress", False))

        if self.label_print_printer_combo is None:
            return

        if not printer_names:
            self.label_print_printer_combo.configure(values=["Принтеры не найдены"], state="disabled")
            self.label_print_printer_combo.set("Принтеры не найдены")
        else:
            preferred_printer = current_printer
            if preferred_printer not in printer_names:
                preferred_printer = default_printer if default_printer in printer_names else printer_names[0]

            combo_state = "disabled" if is_busy or is_loading else "readonly"
            self.label_print_printer_combo.configure(values=printer_names, state=combo_state)
            self.label_print_printer_combo.set(preferred_printer)

        if self.label_print_printer_refresh_button is not None:
            self.label_print_printer_refresh_button.configure(
                state="disabled" if is_busy or is_loading else "normal"
            )

        self._update_label_print_button_state()

    def _get_selected_label_print_printer(self) -> str | None:
        if self.label_print_printer_combo is None:
            return None

        printer_name = str(self.label_print_printer_combo.get() or "").strip()
        if printer_name and printer_name in self.label_print_printer_names:
            return printer_name

        return None

    def _update_label_print_button_state(self, event=None):
        if self.label_print_button is None:
            return

        source_kind = self._get_label_print_source_kind()
        has_template = self._get_selected_label_template() is not None
        has_source = self._get_selected_aggregation_csv() is not None
        has_order = self._get_selected_label_order() is not None
        has_printer = self._get_selected_label_print_printer() is not None
        has_dates = bool(
            self.label_print_mfg_entry
            and self.label_print_exp_entry
            and self.label_print_mfg_entry.get().strip()
            and self.label_print_exp_entry.get().strip()
        )

        order_is_valid = False
        quantity_is_valid = False
        selected_order = self._get_selected_label_order()
        if selected_order is not None:
            document_id = str(selected_order.get("document_id") or "")
            metadata = self.label_print_order_metadata_cache.get(document_id)
            if metadata is not None and not isinstance(metadata, Exception):
                order_is_valid = True
                if source_kind == MARKING_SOURCE_KIND:
                    quantity_is_valid = metadata.units_per_pack > 0
                elif self.label_print_quantity_entry is not None:
                    raw_quantity = self.label_print_quantity_entry.get().strip().replace(" ", "")
                    if raw_quantity:
                        try:
                            parsed_quantity = int(raw_quantity)
                            quantity_is_valid = (
                                metadata.units_per_pack > 0
                                and parsed_quantity > 0
                                and parsed_quantity % metadata.units_per_pack == 0
                            )
                        except ValueError:
                            quantity_is_valid = False

        button_state = (
            "normal"
            if has_template
            and has_source
            and has_order
            and has_printer
            and has_dates
            and order_is_valid
            and quantity_is_valid
            and not self.label_print_in_progress
            and not self.label_print_refresh_in_progress
            else "disabled"
        )
        self.label_print_button.configure(state=button_state)

    def _set_label_print_busy(self, is_busy: bool):
        self.label_print_in_progress = is_busy
        if self.label_print_button is not None:
            self.label_print_button.configure(
                text="Печать..." if is_busy else "Выполнить печать"
            )
        if self.label_print_printer_combo is not None:
            combo_state = "disabled" if is_busy or not self.label_print_printer_names else "readonly"
            self.label_print_printer_combo.configure(state=combo_state)
        if self.label_print_printer_refresh_button is not None:
            self.label_print_printer_refresh_button.configure(
                state="disabled" if is_busy else "normal"
            )
        self._update_label_print_button_state()

    def _append_textbox_message(self, textbox, message: str):
        if textbox is None:
            return

        def append_message():
            try:
                previous_state = None
                try:
                    previous_state = str(textbox.cget("state"))
                except Exception:
                    previous_state = None

                if previous_state == "disabled":
                    textbox.configure(state="normal")

                textbox.insert("end", message)
                textbox.see("end")

                if previous_state == "disabled":
                    textbox.configure(state="disabled")
            except Exception as exc:
                logger.error(f"Ошибка при записи в текстовое поле: {exc}")

        if threading.current_thread() is threading.main_thread():
            append_message()
            return

        try:
            self.after(0, append_message)
        except Exception as exc:
            logger.error(f"Не удалось запланировать запись в текстовое поле: {exc}")

    def label_print_log_insert(self, message: str):
        if self.label_print_log_text is None:
            return

        timestamp = datetime.now().strftime("%H:%M:%S")
        self._append_textbox_message(self.label_print_log_text, f"[{timestamp}] {message}\n")

    def print_selected_100x180_labels(self):
        template = self._get_selected_label_template()
        template_path = self.label_print_selected_template_path
        if template is None or not template_path:
            mbox.showwarning("Выбор шаблона", "Сначала выберите шаблон BarTender 100x180.")
            return

        source_file = self._get_selected_aggregation_csv()
        source_kind = template.data_source_kind
        source_title = "кодами маркировки" if source_kind == MARKING_SOURCE_KIND else "агрегационными кодами"
        source_short = "КМ" if source_kind == MARKING_SOURCE_KIND else "АК"
        if source_file is None:
            mbox.showwarning("Выбор CSV", f"Сначала выберите CSV с {source_title}.")
            return

        printer_name = self._get_selected_label_print_printer()
        if not printer_name:
            mbox.showwarning("Выбор принтера", "Сначала выберите принтер для печати этикеток 100x180.")
            return

        order_data = self._get_selected_label_order()
        if order_data is None:
            mbox.showwarning("Выбор заявки", "Сначала выберите заказ кодов маркировки.")
            return

        try:
            context = build_100x180_label_print_context(
                df=self.df,
                order_data=order_data,
                template_path=template_path,
                aggregation_csv_path=source_file.path,
                printer_name=printer_name,
                manufacture_date=self.label_print_mfg_entry.get().strip() if self.label_print_mfg_entry else "",
                expiration_date=self.label_print_exp_entry.get().strip() if self.label_print_exp_entry else "",
                quantity_value=(
                    self.label_print_quantity_entry.get().strip()
                    if source_kind == AGGREGATION_SOURCE_KIND and self.label_print_quantity_entry
                    else None
                ),
            )
        except BarTenderLabel100x180Error as exc:
            mbox.showerror("Ошибка печати", str(exc))
            self.label_print_log_insert(f"Ошибка подготовки печати: {exc}")
            return

        self._set_label_print_busy(True)
        self.label_print_log_insert(
            f"Подготовка 100x180: {context.order_name} | принтер {context.printer_name} | шаблон {os.path.basename(context.template_path)} | "
            f"{source_short} {os.path.basename(context.aggregation_csv_path)} | записей {context.label_count}"
        )
        self.print_executor.submit(self._label_print_worker, context)

    def _label_print_worker(self, context):
        try:
            print_100x180_labels(context)
            self.after(0, lambda: self._on_label_print_completed(True, context, "Печать отправлена в BarTender"))
        except Exception as exc:
            self.after(0, lambda err=str(exc): self._on_label_print_completed(False, context, err))

    def _on_label_print_completed(self, success: bool, context, message: str):
        self._set_label_print_busy(False)
        source_short = "КМ" if getattr(context, "data_source_kind", "") == MARKING_SOURCE_KIND else "АК"

        if success:
            self.label_print_log_insert(
                f"Печать запущена: {context.order_name} | принтер {context.printer_name} | "
                f"{source_short} {os.path.basename(context.aggregation_csv_path)} | "
                f"{context.label_count} этикеток"
            )
            if hasattr(self, "status_bar") and self.status_bar:
                self.status_bar.configure(text=f"Печать 100x180 отправлена: {context.order_name} -> {context.printer_name}")
                self.after(3000, lambda: self._reset_status_bar())
            return

        self.label_print_log_insert(f"Ошибка печати {context.order_name}: {message}")
        mbox.showerror("Ошибка печати", message)


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

            source_items = list(self.collected)
            self.execute_btn.configure(state="disabled")
            self.log_insert("Подготавливаю выполнение заказов в фоне...")

            threading.Thread(
                target=self._execute_all_background,
                args=(source_items,),
                daemon=True,
                name="ExecuteAllStarter",
            ).start()

        except Exception as e:
            self.log_insert(f"❌ Ошибка при запуске выполнения: {e}")
            # В случае ошибки разблокируем кнопку
            self.execute_btn.configure(state="normal")

    def _execute_all_background(self, source_items):
        try:
            to_process = copy.deepcopy(source_items)
            save_snapshot(to_process)
            save_order_history(to_process)
            self.after(0, lambda count=len(to_process): self.log_insert(f"\nБудет выполнено {count} заказов."))

            futures = []
            for order_item in to_process:
                fut = self.execute_all_executor.submit(self._execute_worker, order_item)
                futures.append((fut, order_item))

            success_count = 0
            fail_count = 0
            results = []

            for fut, order_item in futures:
                try:
                    ok, msg = fut.result(timeout=60)
                    results.append((ok, msg, order_item))
                    self.after(0, self._on_execute_finished, order_item, ok, msg)

                    if ok:
                        success_count += 1
                    else:
                        fail_count += 1
                except Exception as exc:
                    error_msg = f"Таймаут или ошибка выполнения: {exc}"
                    results.append((False, error_msg, order_item))
                    self.after(0, self._on_execute_finished, order_item, False, error_msg)
                    fail_count += 1

            self.after(0, self._on_all_execute_finished, success_count, fail_count, results)
            self.after(0, self.start_auto_status_check)
        except Exception as exc:
            self.after(0, lambda err=str(exc): self._on_execute_all_failed(err))

    def _on_execute_all_failed(self, error_message: str):
        self.log_insert(f"❌ Ошибка подготовки выполнения: {error_message}")
        self.execute_btn.configure(state="normal")

    def _execute_worker(self, order_item):
        """Воркер для выполнения одного заказа в отдельном потоке"""
        try:
            self.log_insert(f"🎬 Запуск позиции: {order_item.simpl_name}  GTIN {order_item.gtin}  заявка № {order_item.order_name}")
            session = SessionManager.get_session()
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
                
                download_item = {
                    'order_name': order_item.order_name, 
                    'document_id': document_id,
                    'status': 'Ожидает',
                    'filename': None,
                    'csv_path': None,
                    'pdf_path': None,
                    'xls_path': None,
                    'simpl': order_item.simpl_name,
                    'full_name': order_item.full_name
                }
                self.download_list.append(download_item)

                #Сохраняем в историю
                history_item = download_item.copy()
                history_item['gtin'] = order_item.gtin
                self.history_db.add_order(history_item)

                self.update_download_tree()
            except Exception as e:
                self.log_insert(f"Не удалось извлечь document_id из: {msg} - {e}")
        else:
            self.log_insert(f"Ошибка: {order_item.simpl_name} | Заявка {order_item.order_name} => {msg}")

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
            logger.error(f"Ошибка при сбросе полей ввода: {e}")

    

    def log_insert(self, msg: str):
        """Выводит сообщение в лог (с ограничением доступа только для чтения)"""
        self._append_textbox_message(self.log_text, f"{msg}\n")

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

    def _set_default_date_range(self, production_entry, expiration_entry):
        production_date, expiration_date = get_default_production_window()

        if production_entry:
            production_entry.delete(0, "end")
            production_entry.insert(0, production_date)

        if expiration_entry:
            expiration_entry.delete(0, "end")
            expiration_entry.insert(0, expiration_date)

    def _should_auto_download_order(self, item: dict) -> bool:
        return (
            not item.get("from_history", False)
            and not item.get("downloading", False)
            and not item.get("filename")
            and item.get("document_id") not in self.sent_to_tsd_items
            and item.get("status") in {"Ожидает", "Генерируется"}
        )

    def _sync_history_from_download_item(self, item: dict):
        document_id = item.get("document_id")
        if not document_id:
            return

        self.history_db.add_order({
            "document_id": document_id,
            "order_name": item.get("order_name"),
            "status": item.get("status"),
            "filename": item.get("filename"),
            "csv_path": item.get("csv_path"),
            "pdf_path": item.get("pdf_path"),
            "xls_path": item.get("xls_path"),
            "simpl": item.get("simpl"),
            "full_name": item.get("full_name"),
            "gtin": item.get("gtin"),
        })


    def start_auto_status_check(self):
        """Запускает автоматическую проверку статусов заказов (только для новых заказов, не из истории)"""
        if self.auto_download_active:
            return
            
        self.auto_download_active = True
        self.download_log_insert("🔄 Автоматическая проверка статусов запущена")
        
        def status_check_worker():
            while self.auto_download_active:
                try:
                    # Проверяем каждые 10 секунд
                    time.sleep(10)
                    
                    # Получаем заказы, которые ожидают скачивания и НЕ являются заказами из истории
                    pending_orders = [
                        item for item in self.download_list
                        if self._should_auto_download_order(item)
                    ]
                    
                    if not pending_orders:
                        continue
                    
                    self.download_log_insert(f"🔍 Проверка статусов для {len(pending_orders)} заказов (исключая историю)")
                    
                    # Проверяем статусы и запускаем скачивание для готовых
                    for item in pending_orders:
                        if not self.auto_download_active:
                            break
                        
                        try:
                            # Проверяем статус заказа
                            status = self._check_order_status(item['document_id'])
                            
                            if status in ('released', 'received'):
                                self.download_log_insert(f"✅ Заказ {item['order_name']} готов к скачиванию")
                                # Устанавливаем флаг и статус синхронно
                                item['downloading'] = True
                                item['status'] = 'Скачивается'
                                # Обновляем UI в главном потоке
                                self.after(0, self.update_download_tree)
                                # Запускаем скачивание в отдельном потоке
                                self.download_executor.submit(self._download_order, item)
                            elif status in ('processing', 'created'):
                                item['status'] = 'Генерируется'
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
        # Убрали проверку downloading здесь, т.к. она теперь в worker
        try:
            # Обновляем статус в главном потоке (на случай, если не установлен)
            self.after(0, lambda: self._update_download_status(item, 'Скачивается'))
            
            session = SessionManager.get_session()
            
            # Скачиваем файлы
            paths = download_codes(session, item['document_id'], item['order_name'])
            
            # paths: tuple (pdf, csv, xls) или None (если early fail, e.g. status not ready)
            if paths is None:
                raise ValueError("download_codes вернул None (заказ не готов или ошибка подготовки)")
            
            item['pdf_path'], item['csv_path'], item['xls_path'] = paths
            non_none_paths = [p for p in paths if p is not None]
            if non_none_paths:
                filename = ', '.join(non_none_paths)  # Или просто paths[0] если нужен один
                self.after(0, lambda f=filename: self._finish_download(item, f))
            else:
                raise ValueError("Нет скачанных файлов (все пути None)")
            
        except Exception as e:
            logger.error(f"Ошибка скачивания {item['order_name']}: {e}", exc_info=True)
            self.after(0, lambda err=str(e): self._update_download_status(item, f"Ошибка: {err}"))
        finally:
            item['downloading'] = False
            self.after(0, self.update_idletasks)  # Принудительно обновить UI

    def _update_download_status(self, item, status):
        """Обновляет статус скачивания в UI"""
        try:
            item['status'] = status
            self.update_download_tree()
            self.download_log_insert(f"📦 {item['order_name']}: {status}")
            # Принудительно обновляем интерфейс
            self.update_idletasks()
        except Exception as e:
            logger.error(f"Ошибка обновления статуса: {e}")

    def _finish_download(self, item, filename):
        """Завершает скачивание"""
        try:
            item['status'] = 'Скачан'
            item['filename'] = filename
            self._sync_history_from_download_item(item)
            self.update_download_tree()
            self.download_log_insert(f"✅ Успешно скачан: {filename}")
            # Принудительно обновляем интерфейс
            self.update_idletasks()
        except Exception as e:
            logger.error(f"Ошибка завершения скачивания: {e}")


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
            'csv_path': None,
            'pdf_path': None,
            'xls_path': None,
            'simpl': order_item.simpl_name
        }
        
        self.download_list.append(new_item)
        self.update_download_tree()
        self.download_log_insert(f"📝 Добавлен в очередь скачивания: {order_item.order_name}")

    def update_download_tree(self):
        """Обновляет дерево заказов для скачивания"""
        # Очищаем дерево
        for i in self.download_tree.get_children():
            self.download_tree.delete(i)
        
        # Добавляем записи из download_list
        for item in self.download_list:
            status = item.get("status", "Неизвестно")
            
            # Добавляем иконку для заказов из истории
            if item.get('from_history'):
                status = "📜 " + status  # Добавляем иконку истории
            
            vals = (
                item.get("order_name"), 
                item.get("document_id"), 
                status, 
                item.get("filename") or ""
            )
            self.download_tree.insert("", "end", values=vals)
        self._update_download_print_button_state()

    def download_history_order_manual(self, history_tree_or_document_id):
        """Ручное скачивание заказа из истории"""
        try:
            # Определяем тип аргумента
            history_window = None
            if isinstance(history_tree_or_document_id, str):
                # Если передан document_id
                document_id = history_tree_or_document_id
            else:
                # Если передан history_tree
                history_tree = history_tree_or_document_id
                selected_items = history_tree.selection()
                if not selected_items:
                    tk.messagebox.showwarning("Выбор заказа", "Выберите заказ для ручного скачивания")
                    return
                
                if len(selected_items) > 1:
                    tk.messagebox.showwarning("Выбор заказа", "Выберите только один заказ для скачивания")
                    return
                
                item = selected_items[0]
                item_values = history_tree.item(item, 'values')
                document_id = item_values[1]
                
                # Сохраняем ссылку на окно истории для последующего закрытия
                history_window = history_tree.winfo_toplevel()
            
            # Дальше общая логика
            order_data = self.history_db.get_order_by_document_id(document_id)
            if not order_data:
                tk.messagebox.showerror("Ошибка", "Заказ не найден в истории")
                return
            
            # Проверяем, не добавлен ли уже заказ в download_list
            existing_order = None
            for item in self.download_list:
                if item.get("document_id") == document_id:
                    existing_order = item
                    break
            
            if not existing_order:
                # Добавляем в download_list
                new_order = {
                    "order_name": order_data.get("order_name"),
                    "document_id": document_id,
                    "status": "Из истории",
                    "filename": order_data.get("filename"),
                    "csv_path": order_data.get("csv_path"),
                    "pdf_path": order_data.get("pdf_path"),
                    "xls_path": order_data.get("xls_path"),
                    "simpl": order_data.get("simpl"),
                    "full_name": order_data.get("full_name"),
                    "gtin": order_data.get("gtin"),
                    "from_history": True,
                    "downloading": False,
                    "history_data": order_data  # Сохраняем полные данные
                }
                self.download_list.append(new_order)
                existing_order = new_order
                self.download_log_insert(f"✅ Добавлен заказ из истории: {order_data.get('order_name')}")
            
            # Проверяем, не скачивается ли уже
            if existing_order.get('downloading'):
                self.download_log_insert(f"⚠️ Заказ {existing_order.get('order_name')} уже скачивается")
                # Закрываем окно истории если нужно
                if history_window:
                    history_window.destroy()
                return
            
            # Меняем статус и запускаем скачивание
            existing_order['status'] = 'Скачивается'
            existing_order['downloading'] = True
            self.update_download_tree()
            
            order_name = existing_order.get('order_name', 'Unknown')
            self.download_log_insert(f"🔄 Ручное скачивание заказа из истории: {order_name}")
            
            # Запускаем скачивание
            self.download_executor.submit(self._download_order, existing_order)
            
            # Закрываем окно истории если нужно
            if history_window:
                history_window.destroy()
                
                # БЕЗОПАСНОЕ переключение на вкладку скачивания
                try:
                    # Проверяем существует ли tabview
                    if hasattr(self, 'tabview') and self.tabview:
                        # Используем after для безопасного вызова в главном потоке
                        self.after(100, lambda: self.tabview.set("📥 Скачивание кодов"))
                
                except Exception as e:
                    self.download_log_insert(f"⚠️ Ошибка переключения вкладки: {e}")
            
        except Exception as e:
            error_msg = f"❌ Ошибка ручного скачивания заказа из истории: {e}"
            self.download_log_insert(error_msg)
            tk.messagebox.showerror("Ошибка", error_msg)
            
            # Всегда пытаемся закрыть окно истории при ошибке
            try:
                if not isinstance(history_tree_or_document_id, str):
                    history_window = history_tree_or_document_id.winfo_toplevel()
                    history_window.destroy()
            except:
                pass

    def download_log_insert(self, msg: str):
        """Добавляет сообщение в лог скачиваний"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self._append_textbox_message(self.download_log_text, f"[{timestamp}] {msg}\n")

    def _update_download_print_button_state(self, event=None):
        if not hasattr(self, "download_print_button") or self.download_print_button is None:
            return

        is_busy = bool(getattr(self, "print_in_progress", False))
        has_selection = bool(self.download_tree.selection())
        has_printer = self._get_selected_download_printer() is not None
        button_state = "disabled" if is_busy or not has_selection or not has_printer else "normal"
        self.download_print_button.configure(state=button_state)

    def _refresh_download_printers(self, manual: bool = False):
        current_printer = self._get_selected_download_printer()
        printer_names, default_printer = list_installed_printers()
        self.download_printer_names = printer_names
        self.download_default_printer_name = default_printer
        is_busy = bool(getattr(self, "print_in_progress", False))

        if self.download_printer_combo is None:
            return

        if not printer_names:
            self.download_printer_combo.configure(values=["Принтеры не найдены"], state="disabled")
            self.download_printer_combo.set("Принтеры не найдены")
        else:
            preferred_printer = current_printer
            if preferred_printer not in printer_names:
                preferred_printer = default_printer if default_printer in printer_names else printer_names[0]

            combo_state = "disabled" if is_busy else "readonly"
            self.download_printer_combo.configure(values=printer_names, state=combo_state)
            self.download_printer_combo.set(preferred_printer)

        if self.download_printer_refresh_button is not None:
            self.download_printer_refresh_button.configure(
                state="disabled" if is_busy else "normal"
            )

        if manual:
            if printer_names:
                default_note = (
                    f" | по умолчанию: {default_printer}"
                    if default_printer and default_printer in printer_names
                    else ""
                )
                self.download_log_insert(
                    f"🖨️ Список принтеров обновлен: {len(printer_names)} шт.{default_note}"
                )
            else:
                self.download_log_insert("⚠️ Принтеры не найдены. Проверьте установленные устройства Windows.")

        self._update_download_print_button_state()

    def _get_selected_download_printer(self) -> str | None:
        if self.download_printer_combo is None:
            return None

        printer_name = str(self.download_printer_combo.get() or "").strip()
        if printer_name and printer_name in self.download_printer_names:
            return printer_name

        return None

    def _set_print_busy(self, is_busy: bool):
        self.print_in_progress = is_busy
        if hasattr(self, "download_print_button") and self.download_print_button is not None:
            self.download_print_button.configure(
                text="Печать..." if is_busy else "Выполнить печать"
            )
        if self.download_printer_combo is not None:
            combo_state = "disabled" if is_busy or not self.download_printer_names else "readonly"
            self.download_printer_combo.configure(state=combo_state)
        if self.download_printer_refresh_button is not None:
            self.download_printer_refresh_button.configure(
                state="disabled" if is_busy else "normal"
            )
        self._update_download_print_button_state()

    def _get_selected_download_item(self) -> dict | None:
        selected_items = self.download_tree.selection()
        if not selected_items:
            return None

        item_values = self.download_tree.item(selected_items[0], "values")
        if len(item_values) < 2:
            return None

        document_id = item_values[1]
        for item in self.download_list:
            if item.get("document_id") == document_id:
                return item

        return None

    def _resolve_order_csv_path(self, item: dict) -> str | None:
        candidate_paths = [
            item.get("csv_path"),
            (item.get("history_data") or {}).get("csv_path"),
        ]

        filename_value = item.get("filename") or (item.get("history_data") or {}).get("filename")
        if filename_value:
            for chunk in str(filename_value).split(","):
                normalized = chunk.strip()
                if normalized.lower().endswith(".csv"):
                    candidate_paths.append(normalized)

        for path in candidate_paths:
            if path and os.path.exists(path):
                return str(path)

        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        parent_dir = os.path.join(desktop, "Коды км")
        safe_order_name = "".join(
            char for char in str(item.get("order_name") or item.get("document_id") or "")
            if char.isalnum() or char in " -_"
        ).strip()

        if safe_order_name:
            order_dir = os.path.join(parent_dir, safe_order_name[:120])
            if os.path.isdir(order_dir):
                csv_files = sorted(
                    (
                        os.path.join(order_dir, file_name)
                        for file_name in os.listdir(order_dir)
                        if file_name.lower().endswith(".csv")
                    ),
                    key=os.path.getmtime,
                    reverse=True,
                )
                if csv_files:
                    return csv_files[0]

        return None

    def print_selected_download_order(self):
        selected_item = self._get_selected_download_item()
        if not selected_item:
            mbox.showwarning("Выбор заявки", "Сначала выберите заявку в таблице загрузок.")
            return

        printer_name = self._get_selected_download_printer()
        if not printer_name:
            mbox.showwarning("Выбор принтера", "Сначала выберите принтер для печати термоэтикеток.")
            return

        csv_path = self._resolve_order_csv_path(selected_item)
        if not csv_path:
            mbox.showwarning(
                "CSV не найден",
                "Для выбранной заявки не найден CSV с кодами маркировки. Дождитесь скачивания или скачайте заказ заново."
            )
            return

        try:
            context = build_print_context(
                order_name=str(selected_item.get("order_name") or ""),
                document_id=str(selected_item.get("document_id") or ""),
                csv_path=csv_path,
                printer_name=printer_name,
            )
        except BarTenderPrintError as exc:
            mbox.showerror("Ошибка печати", str(exc))
            return

        selected_item["csv_path"] = context.csv_path
        self._sync_history_from_download_item(selected_item)
        self._set_print_busy(True)
        self.download_log_insert(
            f"🖨️ Подготовка печати: {context.order_name} | принтер {context.printer_name} | "
            f"размер {context.size} | этикеток {context.label_count}"
        )
        self.print_executor.submit(self._print_order_worker, context)

    def _print_order_worker(self, context):
        try:
            print_labels(context)
            self.after(0, lambda: self._on_print_completed(True, context, "Печать отправлена в BarTender"))
        except Exception as exc:
            self.after(0, lambda err=str(exc): self._on_print_completed(False, context, err))

    def _on_print_completed(self, success: bool, context, message: str):
        self._set_print_busy(False)

        if success:
            self.download_log_insert(
                f"✅ Печать запущена: {context.order_name} | принтер: {context.printer_name} | CSV: {context.csv_path}"
            )
            if hasattr(self, "status_bar") and self.status_bar:
                self.status_bar.configure(text=f"Печать отправлена: {context.order_name} -> {context.printer_name}")
                self.after(3000, lambda: self._reset_status_bar())
            return

        self.download_log_insert(f"❌ Ошибка печати {context.order_name}: {message}")
        mbox.showerror("Ошибка печати", message)

    def on_closing(self):
        self.auto_download_active = False
        for executor in [self.download_executor, self.status_check_executor, self.print_executor,
                        self.execute_all_executor, self.intro_executor, self.intro_tsd_executor]:
            executor.shutdown(wait=False, cancel_futures=True)
        self.destroy()
        
    def _setup_introduction_frame(self):
        """Современный фрейм введения в оборот"""
        self.content_frames["intro"] = CTkScrollableFrame(self.main_content, corner_radius=0)
        
        # Основной контейнер
        main_frame = ctk.CTkFrame(self.content_frames["intro"], corner_radius=15)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Заголовок
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            header_frame,
            text="🚚",
            font=("Segoe UI", 48),
            text_color=self._get_color("primary")
        ).pack(side="left", padx=(0, 15))
        
        title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_frame.pack(side="left", fill="y")
        
        ctk.CTkLabel(
            title_frame,
            text="Ввод в оборот",
            font=self.fonts["title"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w")
        
        ctk.CTkLabel(
            title_frame,
            text="Управление вводом товаров в оборот",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w")
        
        # Две колонки
        columns_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        columns_frame.pack(fill="both", expand=True)
        columns_frame.grid_columnconfigure(0, weight=1)
        columns_frame.grid_columnconfigure(1, weight=1)
        columns_frame.grid_rowconfigure(0, weight=1)
        
        # Левая колонка - форма и таблица
        left_column = ctk.CTkFrame(columns_frame, corner_radius=12)
        left_column.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        
        # Правая колонка - лог
        right_column = ctk.CTkFrame(columns_frame, corner_radius=12)
        right_column.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        
        # Форма ввода
        form_container = ctk.CTkFrame(left_column, corner_radius=8)
        form_container.pack(fill="x", pady=(0, 10))
        
        ctk.CTkLabel(
            form_container, 
            text="Параметры ввода", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        form_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        form_frame.pack(fill="x", padx=15, pady=10)
        
        # Явно инициализируем поля ввода как None
        self.prod_date_intro_entry = None
        self.exp_date_intro_entry = None
        self.batch_intro_entry = None
        
        # Сетка для полей ввода с гарантированной инициализацией
        labels = [
            ("Дата производства (ДД-ММ-ГГГГ):", "prod_date_intro_entry"),
            ("Дата окончания (ДД-ММ-ГГГГ):", "exp_date_intro_entry"),
            ("Номер партии:", "batch_intro_entry")
        ]
        
        for i, (label_text, attr_name) in enumerate(labels):
            ctk.CTkLabel(form_frame, text=label_text, font=self.fonts["normal"]).grid(row=i, column=0, sticky="w", pady=8, padx=5)
            entry = ctk.CTkEntry(form_frame, width=200, font=self.fonts["normal"])
            entry.grid(row=i, column=1, pady=8, padx=5)
            setattr(self, attr_name, entry)
        
        self._set_default_date_range(self.prod_date_intro_entry, self.exp_date_intro_entry)
        
        # Кнопки
        btn_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        btn_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        self.intro_btn = ctk.CTkButton(
            btn_frame, 
            text="🔄 Ввести в оборот", 
            command=self.on_introduce_clicked,
            fg_color=self._get_color("success"),
            hover_color="#228B69",
            font=self.fonts["button"],
            corner_radius=6
        )
        self.intro_btn.pack(side="left", padx=5)
        
        self.intro_refresh_btn = ctk.CTkButton(
            btn_frame, 
            text="🔄 Обновить", 
            command=self.update_introduction_tree,
            font=self.fonts["button"],
            corner_radius=6
        )
        self.intro_refresh_btn.pack(side="left", padx=5)
        
        self.intro_clear_btn = ctk.CTkButton(
            btn_frame, 
            text="🧹 Очистить лог", 
            command=self.clear_intro_log,
            font=self.fonts["button"],
            corner_radius=6
        )
        self.intro_clear_btn.pack(side="left", padx=5)
        
        # Таблица заказов
        table_container = ctk.CTkFrame(left_column, corner_radius=8)
        table_container.pack(fill="both", expand=True, pady=(10, 0))
        
        ctk.CTkLabel(
            table_container, 
            text="Доступные заказы", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        table_inner_frame = ctk.CTkFrame(table_container, fg_color="transparent")
        table_inner_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        intro_columns = ("order_name", "document_id", "status", "filename")
        self.intro_tree = ttk.Treeview(table_inner_frame, columns=intro_columns, show="headings", 
                                    height=10, selectmode="extended")
        
        headers = {
            "order_name": "Заявка", "document_id": "ID заказа",
            "status": "Статус", "filename": "Файл"
        }
        
        for col, text in headers.items():
            self.intro_tree.heading(col, text=text)
            self.intro_tree.column(col, width=150)
        
        scrollbar = ttk.Scrollbar(table_inner_frame, orient="vertical", command=self.intro_tree.yview)
        self.intro_tree.configure(yscrollcommand=scrollbar.set)
        self.intro_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Лог
        log_container = ctk.CTkFrame(right_column, corner_radius=8)
        log_container.pack(fill="both", expand=True)
        
        ctk.CTkLabel(
            log_container, 
            text="Лог операций", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        self.intro_log_text = ctk.CTkTextbox(log_container, font=self.fonts["normal"])
        self.intro_log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.intro_log_text.configure(state="disabled")
        
        self.update_introduction_tree()

    def show_order_history(self):
        """Показывает диалог с историей всех заказов"""
        history_window = ctk.CTkToplevel(self)
        history_window.title("📚 История заказов")
        history_window.geometry("1000x600")
        history_window.transient(self)
        history_window.grab_set()

        main_frame = ctk.CTkFrame(history_window)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        ctk.CTkLabel(
            main_frame,
            text="История всех заказов",
            font=ctk.CTkFont(size=16, weight="bold")
        ).pack(pady=(0, 10))

        # Фрейм для поиска
        search_frame = ctk.CTkFrame(main_frame)
        search_frame.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(search_frame, text="Поиск:").pack(side="left", padx=5)
        
        search_var = ctk.StringVar()
        search_entry = ctk.CTkEntry(
            search_frame, 
            textvariable=search_var,
            placeholder_text="Введите номер заказа или название...",
            width=300
        )
        search_entry.pack(side="left", padx=5, fill="x", expand=True)
        
        ctk.CTkButton(
            search_frame,
            text="Найти",
            command=lambda: self._update_history_tree(history_tree, filter_var.get(), search_var.get())
        ).pack(side="left", padx=5)

        # Фрейм для фильтров
        filter_frame = ctk.CTkFrame(main_frame)
        filter_frame.pack(fill="x", pady=(0, 10))

        ctk.CTkLabel(filter_frame, text="Фильтры:").pack(side="left", padx=5)

        filter_var = ctk.StringVar(value="all")

        ctk.CTkRadioButton(
            filter_frame,
            text="Все заказы",
            variable=filter_var,
            value="all",
            command=lambda: self._update_history_tree(history_tree, filter_var.get(), search_var.get())
        ).pack(side="left", padx=10)

        ctk.CTkRadioButton(
            filter_frame,
            text="Не отправлено",
            variable=filter_var,
            value="without_tsd",
            command=lambda: self._update_history_tree(history_tree, filter_var.get(), search_var.get())
        ).pack(side="left", padx=10)

        ctk.CTkRadioButton(
            filter_frame,
            text="Отправлено",
            variable=filter_var,
            value="with_tsd",
            command=lambda: self._update_history_tree(history_tree, filter_var.get(), search_var.get())
        ).pack(side="left", padx=10)

        table_frame = ctk.CTkFrame(main_frame)
        table_frame.pack(fill="both", expand=True, pady=(0, 10))

        columns = ("order_name", "document_id", "status", "tsd_status", "created_at")
        history_tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=15)

        headers = {
            "order_name": "Заявка",
            "document_id": "ID заказа",
            "status": "Статус",
            "tsd_status": "Статус отправки на ТСД",
            "created_at": "Дата создания"
        }

        for col, text in headers.items():
            history_tree.heading(col, text=text)
            if col == "order_name":
                history_tree.column(col, width=200)
            elif col =="document_id":
                history_tree.column(col, width=150)
            elif col == "created_at": 
                history_tree.column(col, width=150)
            else:
                history_tree.column(col, width=120)
        
        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=history_tree.yview)
        history_tree.configure(yscrollcommand=scrollbar.set)
        history_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        button_frame = ctk.CTkFrame(main_frame)
        button_frame.pack(fill="x", pady=(0, 10))

        ctk.CTkButton(
            button_frame,
            text="🔄 Обновить",
            command=lambda: self._update_history_tree(history_tree, filter_var.get(), search_var.get())
        ).pack(side="left", padx=5)

        ctk.CTkButton(
            button_frame,
            text="📋 Добавить в ТСД",
            command=lambda: self._add_history_to_tsd(history_tree, history_window),
            fg_color="#E67E22",
            hover_color="#D35400"
        ).pack(side="left", padx=5)

        # НОВАЯ КНОПКА: Ручное скачивание
        ctk.CTkButton(
            button_frame,
            text="📥 Скачать вручную",
            command=lambda: self.download_history_order_manual(history_tree),
            fg_color="#27AE60",
            hover_color="#219A52"
        ).pack(side="left", padx=5)
        
        ctk.CTkButton(
            button_frame,
            text="❌ Закрыть",
            command=history_window.destroy
        ).pack(side="right", padx=5)
        
        # Обработка нажатия Enter в поле поиска
        search_entry.bind("<Return>", lambda event: self._update_history_tree(history_tree, filter_var.get(), search_var.get()))
        
        # Первоначальное заполнение таблицы
        self._update_history_tree(history_tree, "all", "")

    def _update_history_tree(self, history_tree, filter_type="all", search_query=""):
        """Обновляет дерево истории в диалоге согласно фильтру и поиску"""
        # Очищаем дерево
        for item in history_tree.get_children():
            history_tree.delete(item)
        
        # Загружаем заказы из истории БД (НЕ из download_list!)
        if filter_type == "all":
            history_orders = self.history_db.get_all_orders()
        elif filter_type == "without_tsd":
            history_orders = self.history_db.get_orders_without_tsd()
        elif filter_type == "with_tsd":
            history_orders = [order for order in self.history_db.get_all_orders() 
                            if order.get("tsd_created")]
        
        # СОРТИРОВКА: сначала новые заказы, потом старые
        try:
            history_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
        except:
            # Если сортировка не удалась, оставляем как есть
            pass
        
        # Применяем поиск, если есть поисковый запрос
        if search_query:
            search_lower = search_query.lower()
            history_orders = [order for order in history_orders 
                            if search_lower in order.get('document_id', '').lower() 
                            or search_lower in order.get('order_name', '').lower()]
        
        # Заполняем дерево
        for order in history_orders:
            # Форматируем дату
            created_at = order.get('created_at', '')
            if created_at:
                try:
                    created_at = datetime.fromisoformat(created_at).strftime("%d.%m.%Y %H:%M")
                except:
                    created_at = created_at
            
            # Определяем статус ТСД
            tsd_status = "✅ Отправлено" if order.get('tsd_created') else "⏳ Не отправлено"
            
            # Добавляем информацию о номере ТСД, если есть
            if order.get('tsd_created') and order.get('tsd_intro_number'):
                tsd_status += f" (№{order.get('tsd_intro_number')})"
            
            values = (
                order.get('order_name', 'Неизвестно'),
                order.get('document_id', 'Неизвестно'),
                order.get('status', 'Неизвестно'),
                tsd_status,
                created_at
            )
            
            history_tree.insert("", "end", values=values)

    def _add_history_to_tsd(self, history_tree, history_window):
        """Добавляет выбранные в истории заказы в download_list для ТСД"""
        try:
            selected_items = history_tree.selection()
            if not selected_items:
                tk.messagebox.showwarning("Выбор заказов", "Выберите заказы для добавления в ТСД")
                return
            
            # Получаем все заказы из истории для поиска по ID
            all_orders = self.history_db.get_all_orders()
            order_dict = {order['document_id']: order for order in all_orders}
            
            added_count = 0
            resent_count = 0
            
            # Сначала собираем информацию о уже отправленных заказах
            already_sent_orders = []
            orders_to_add = []
            
            for item_id in selected_items:
                try:
                    item_values = history_tree.item(item_id, 'values')
                    document_id = item_values[1]  # document_id находится во второй колонке
                    
                    order_data = order_dict.get(document_id)
                    if not order_data:
                        continue
                    
                    # ПРОВЕРЯЕМ, НЕ ОТПРАВЛЕН ЛИ УЖЕ ЗАКАЗ НА ТСД
                    if order_data.get('tsd_created'):
                        already_sent_orders.append(order_data)
                    else:
                        orders_to_add.append(order_data)
                        
                except Exception as e:
                    print(f"❌ DEBUG: Ошибка обработки элемента: {e}")
                    continue
            
            # Обрабатываем заказы, которые еще не отправлялись
            for order_data in orders_to_add:
                document_id = order_data.get('document_id')
                
                # Проверяем, не добавлен ли уже заказ в текущей сессии
                existing_item = next((item for item in self.download_list if item.get("document_id") == document_id), None)
                if not existing_item:
                    new_item = {
                        "order_name": order_data.get("order_name"),
                        "document_id": document_id,
                        "status": "Готов для ТСД",
                        "filename": order_data.get("filename"),
                        "csv_path": order_data.get("csv_path"),
                        "pdf_path": order_data.get("pdf_path"),
                        "xls_path": order_data.get("xls_path"),
                        "simpl": order_data.get("simpl"),
                        "full_name": order_data.get("full_name"),
                        "gtin": order_data.get("gtin"),
                        "from_history": True,
                        "downloading": False,
                        "history_data": order_data
                    }
                    self.download_list.append(new_item)
                    added_count += 1
                    print(f"✅ DEBUG: Добавлен заказ из истории с GTIN: {order_data.get('gtin')}")
                else:
                    # Обновляем существующий заказ
                    existing_item.update({
                        "status": "Готов для ТСД",
                        "from_history": True,
                        "gtin": order_data.get("gtin"),
                        "history_data": order_data
                    })
                    added_count += 1
                    print(f"✅ DEBUG: Обновлен заказ с GTIN: {order_data.get('gtin')}")
            
            # Обрабатываем уже отправленные заказы с запросом подтверждения
            if already_sent_orders:
                # Упрощенный диалог с использованием messagebox
                order_names = [order.get('order_name', 'Неизвестный заказ') for order in already_sent_orders[:3]]
                if len(already_sent_orders) > 3:
                    order_names.append(f"... и еще {len(already_sent_orders) - 3} заказов")
                
                message = (
                    f"Найдено {len(already_sent_orders)} заказов, которые уже отправлялись на ТСД.\n\n"
                    f"{chr(10).join(order_names)}\n\n"
                    f"Отправить эти заказы повторно?"
                )
                
                # Используем стандартный messagebox для упрощения
                response = tk.messagebox.askyesno(
                    "Повторная отправка на ТСД", 
                    message,
                    icon="warning"
                )
                
                if response:  # Если пользователь ответил "Да"
                    for order_data in already_sent_orders:
                        document_id = order_data.get('document_id')
                        
                        # Проверяем, не добавлен ли уже заказ в текущей сессии
                        existing_item = next((item for item in self.download_list if item.get("document_id") == document_id), None)
                        if not existing_item:
                            new_item = {
                                "order_name": order_data.get("order_name"),
                                "document_id": document_id,
                                "status": "Готов для ТСД",  # Используем тот же статус, что и для новых
                                "filename": order_data.get("filename"),
                                "csv_path": order_data.get("csv_path"),
                                "pdf_path": order_data.get("pdf_path"),
                                "xls_path": order_data.get("xls_path"),
                                "simpl": order_data.get("simpl"),
                                "full_name": order_data.get("full_name"),
                                "gtin": order_data.get("gtin"),
                                "from_history": True,
                                "downloading": False,
                                "history_data": order_data,
                                "resent": True  # Помечаем как повторно отправленный
                            }
                            self.download_list.append(new_item)
                            resent_count += 1
                            print(f"🔄 DEBUG: Повторно добавлен заказ с GTIN: {order_data.get('gtin')}")
                        else:
                            # Обновляем существующий заказ
                            existing_item.update({
                                "status": "Готов для ТСД",
                                "from_history": True,
                                "gtin": order_data.get("gtin"),
                                "history_data": order_data,
                                "resent": True
                            })
                            resent_count += 1
                            print(f"🔄 DEBUG: Обновлен заказ для повторной отправки с GTIN: {order_data.get('gtin')}")
            
            # Обновляем таблицу ТСД
            self.update_tsd_tree()
            
            # Закрываем окно истории только если оно еще существует
            if history_window and tk._default_root:
                try:
                    history_window.destroy()
                except:
                    pass
            
            # Показываем информативное сообщение
            message_parts = []
            if added_count > 0:
                message_parts.append(f"Добавлено новых заказов: {added_count}")
            if resent_count > 0:
                message_parts.append(f"Повторно отправлено заказов: {resent_count}")
            
            if message_parts:
                tk.messagebox.showinfo("Добавление в ТСД", "\n".join(message_parts))
            else:
                tk.messagebox.showwarning("Добавление в ТСД", "Не удалось добавить заказы.")
                
        except Exception as e:
            print(f"💥 DEBUG: Критическая ошибка в _add_history_to_tsd: {e}")
            import traceback
            print(f"🔍 DEBUG: Детали ошибки: {traceback.format_exc()}")
            tk.messagebox.showerror("Ошибка", f"Произошла ошибка при добавлении заказов: {str(e)}")

    def load_history_for_dialog(self):
        """Загружает заказы из истории ТОЛЬКО для отображения в диалоге истории"""
        try:
            return self.history_db.get_all_orders()
        except Exception as e:
            logger.error(f"Ошибка загрузки истории для диалога: {e}")
            return []

    def _show_error(self, message):
        """Вспомогательный метод для показа ошибок"""
        logger.error(f"❌ {message}")
        # Если лог уже инициализирован, пишем туда
        if hasattr(self, 'intro_log_text'):
            try:
                self.intro_log_text.configure(state="normal")
                self.intro_log_text.insert("end", f"❌ {message}\n")
                self.intro_log_text.configure(state="disabled")
            except:
                pass

    def on_introduce_clicked(self):
        """Обработчик кнопки — собирает данные, запускает threads для выбранных заказов."""
        try:
            # Улучшенная проверка инициализации полей
            field_checks = [
                (self.prod_date_intro_entry, "Дата производства"),
                (self.exp_date_intro_entry, "Дата окончания"), 
                (self.batch_intro_entry, "Номер партии")
            ]
            
            for field, name in field_checks:
                if field is None:
                    self.intro_log_insert(f"❌ Ошибка: поле '{name}' не инициализировано")
                    return
                if not hasattr(field, 'get'):
                    self.intro_log_insert(f"❌ Ошибка: поле '{name}' имеет неверный тип")
                    return

            selected_items = self.get_selected_intro_items()
            if not selected_items:
                self.intro_log_insert("❌ Не выбрано ни одного заказа.")
                return

            blocked_items = [item for item in selected_items if not is_order_ready_for_intro(item)]
            if blocked_items:
                blocked_names = ", ".join(
                    item.get("order_name", item.get("document_id", "Неизвестный заказ"))
                    for item in blocked_items
                )
                self.intro_log_insert(
                    f"❌ Обычный ввод в оборот доступен только после скачивания кодов. Недоступно: {blocked_names}"
                )
                return

            # Безопасное получение данных из полей ввода
            prod_date_text = self.prod_date_intro_entry.get().strip() if self.prod_date_intro_entry.get() else ""
            exp_date_text = self.exp_date_intro_entry.get().strip() if self.exp_date_intro_entry.get() else ""
            batch_num = self.batch_intro_entry.get().strip() if self.batch_intro_entry.get() else ""

            # Преобразование дат
            prod_date = self.convert_date_format(prod_date_text)
            exp_date = self.convert_date_format(exp_date_text)
            thumbprint = THUMBPRINT

            # Валидация
            errors = []
            
            if not prod_date:
                errors.append("Введите дату производства.")
            elif not self.validate_iso_date(prod_date):
                errors.append("Неверный формат даты производства. Используйте ДД-ММ-ГГГГ.")
                
            if not exp_date:
                errors.append("Введите дату окончания.")
            elif not self.validate_iso_date(exp_date):
                errors.append("Неверный формат даты окончания. Используйте ДД-ММ-ГГГГ.")
                
            if not batch_num:
                errors.append("Введите номер партии.")
                
            if errors:
                for error in errors:
                    self.intro_log_insert(f"❌ {error}")
                return

            # Отключаем кнопку пока выполняется
            self.intro_btn.configure(state="disabled")
            self.intro_log_insert(f"🚀 Запуск ввода в оборот для {len(selected_items)} заказа(ов)...")

            # Запускаем задачи
            futures = []
            for it in selected_items:
                if not it or 'document_id' not in it:
                    self.intro_log_insert("❌ Пропущен некорректный элемент заказа")
                    continue
                    
                order_name = it.get("order_name", "Unknown")
                simpl_name = it.get("simpl")
                tnved_code = get_tnved_code(simpl_name) if simpl_name else ""
                
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

            if not futures:
                self.intro_log_insert("❌ Нет валидных задач для выполнения")
                self.intro_btn.configure(state="normal")
                return

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
            self.intro_log_insert(f"❌ Критическая ошибка при запуске ввода в оборот: {e}")
            # Пытаемся разблокировать кнопку в случае ошибки
            try:
                self.intro_btn.configure(state="normal")
            except:
                pass

    # Остальные методы остаются без изменений
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
            logger.error(f"Ошибка очистки лога: {e}")

    def intro_log_insert(self, text: str):
        """Удобная функция логирования в таб 'Ввод' (вызовы только из GUI-потока)."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self._append_textbox_message(self.intro_log_text, f"{now} - {text}\n")

    def update_introduction_tree(self):
        """Наполнить дерево заказами, у которых status == 'Скачан'"""
        try:
            # Очистить дерево
            for item in self.intro_tree.get_children():
                self.intro_tree.delete(item)
            
            # Добавить записи из self.download_list
            for item in self.download_list:
                if is_order_ready_for_intro(item):
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

    def _intro_worker(self, item: dict, production_patch: dict, thumbprint: str | None):
        """
        Фоновая задача — производит ввод в оборот для одного заказа.
        Возвращает (ok, message).
        """
        try:
            document_id = item.get("document_id")
            if not document_id:
                return False, "Отсутствует document_id"
            
            session = SessionManager.get_session()
            
            # Импортируем функцию из api.py
            from api import put_into_circulation
            
            # Вызываем API функцию
            ok, result = put_into_circulation(
                session=session,
                codes_order_id=document_id,
                production_patch=production_patch,
                organization_id=os.getenv("ORGANIZATION_ID"),
                thumbprint=thumbprint,
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
                self.intro_log_insert(f"✅ Успешно: {order_name} (ID: {docid})")
                item["status"] = "Введен в оборот"
            else:
                self.intro_log_insert(f"❌ ОШИБКА: {order_name} (ID: {docid}) - {msg}")
                item["status"] = "Ошибка ввода"
            
            # Обновляем отображение
            self.update_introduction_tree()
            if hasattr(self, 'update_download_tree'):
                self.update_download_tree()
                
        except Exception as e:
            self.intro_log_insert(f"❌ Ошибка при обработке результата: {e}")

    def _setup_introduction_tsd_frame(self):
        """Современный фрейм введения TSD"""
        self.content_frames["intro_tsd"] = CTkScrollableFrame(self.main_content, corner_radius=0)
        
        # Основной контейнер
        main_frame = ctk.CTkFrame(self.content_frames["intro_tsd"], corner_radius=15)
        main_frame.pack(fill="both", expand=True, padx=20, pady=20)
        
        # Заголовок
        header_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        header_frame.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            header_frame,
            text="🏷️",
            font=("Segoe UI", 48),
            text_color=self._get_color("primary")
        ).pack(side="left", padx=(0, 15))
        
        title_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
        title_frame.pack(side="left", fill="y")
        
        ctk.CTkLabel(
            title_frame,
            text="Ввод в оборот (ТСД)",
            font=self.fonts["title"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w")
        
        ctk.CTkLabel(
            title_frame,
            text="Управление вводом товаров через ТСД",
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary")
        ).pack(anchor="w")
        
        # Две колонки
        columns_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        columns_frame.pack(fill="both", expand=True)
        columns_frame.grid_columnconfigure(0, weight=1)
        columns_frame.grid_columnconfigure(1, weight=1)
        columns_frame.grid_rowconfigure(0, weight=1)
        
        # Левая колонка - форма и таблица
        left_column = ctk.CTkFrame(columns_frame, corner_radius=12)
        left_column.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        
        # Правая колонка - лог
        right_column = ctk.CTkFrame(columns_frame, corner_radius=12)
        right_column.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        
        # Форма ввода TSD
        form_container = ctk.CTkFrame(left_column, corner_radius=8)
        form_container.pack(fill="x", pady=(0, 10))
        
        ctk.CTkLabel(
            form_container, 
            text="Параметры ТСД", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        form_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        form_frame.pack(fill="x", padx=15, pady=10)
        
        # Сетка для полей ввода
        tsd_labels = [
            ("Ввод в оборот №:", "tsd_intro_number_entry"),
            ("Дата производства (ДД-ММ-ГГГГ):", "tsd_prod_date_entry"),
            ("Дата окончания (ДД-ММ-ГГГГ):", "tsd_exp_date_entry"),
            ("Номер партии:", "tsd_batch_entry")
        ]
        
        for i, (label_text, attr_name) in enumerate(tsd_labels):
            ctk.CTkLabel(form_frame, text=label_text, font=self.fonts["normal"]).grid(row=i, column=0, sticky="w", pady=8, padx=5)
            entry = ctk.CTkEntry(form_frame, width=200, font=self.fonts["normal"])
            entry.grid(row=i, column=1, pady=8, padx=5)
            setattr(self, attr_name, entry)
        
        self._set_default_date_range(self.tsd_prod_date_entry, self.tsd_exp_date_entry)
        
        # Кнопки
        btn_frame = ctk.CTkFrame(form_container, fg_color="transparent")
        btn_frame.pack(fill="x", padx=15, pady=(0, 15))
        
        self.tsd_btn = ctk.CTkButton(
            btn_frame, 
            text="📱 Отправить на ТСД", 
            command=self.on_tsd_clicked,
            fg_color=self._get_color("warning"),
            hover_color="#D35400",
            font=self.fonts["button"],
            corner_radius=6
        )
        self.tsd_btn.pack(side="left", padx=5)
        
        self.tsd_refresh_btn = ctk.CTkButton(
            btn_frame, 
            text="🔄 Обновить", 
            command=self.update_tsd_tree,
            font=self.fonts["button"],
            corner_radius=6
        )
        self.tsd_refresh_btn.pack(side="left", padx=5)

        self.history_btn = ctk.CTkButton(
            btn_frame,
            text="📚 История заказов",
            command=self.show_order_history,
            fg_color=self._get_color("success"),
            hover_color="#219A52",
            font=self.fonts["button"],
            corner_radius=6
        )
        self.history_btn.pack(side="left", padx=5)
        
        self.tsd_clear_btn = ctk.CTkButton(
            btn_frame, 
            text="🧹 Очистить лог", 
            command=self.clear_tsd_log,
            font=self.fonts["button"],
            corner_radius=6
        )
        self.tsd_clear_btn.pack(side="left", padx=5)
        
        # Таблица
        table_container = ctk.CTkFrame(left_column, corner_radius=8)
        table_container.pack(fill="both", expand=True, pady=(10, 0))
        
        ctk.CTkLabel(
            table_container, 
            text="Доступные заказы", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        table_inner_frame = ctk.CTkFrame(table_container, fg_color="transparent")
        table_inner_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        tsd_columns = ("order_name", "document_id", "status", "filename")
        self.tsd_tree = ttk.Treeview(table_inner_frame, columns=tsd_columns, show="headings", 
                                height=12, selectmode="extended")
        
        headers = {
            "order_name": "Заявка", "document_id": "ID заказа",
            "status": "Статус", "filename": "Файл"
        }
        
        for col, text in headers.items():
            self.tsd_tree.heading(col, text=text)
            self.tsd_tree.column(col, width=150)
        
        scrollbar = ttk.Scrollbar(table_inner_frame, orient="vertical", command=self.tsd_tree.yview)
        self.tsd_tree.configure(yscrollcommand=scrollbar.set)
        self.tsd_tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        # Лог
        log_container = ctk.CTkFrame(right_column, corner_radius=8)
        log_container.pack(fill="both", expand=True)
        
        ctk.CTkLabel(
            log_container, 
            text="Лог ТСД", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        self.tsd_log_text = ctk.CTkTextbox(log_container, font=self.fonts["normal"])
        self.tsd_log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        
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
            logger.error(f"Ошибка при очистке лога ТСД: {e}")

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
        self._append_textbox_message(self.tsd_log_text, f"{now} - {text}\n")

    def update_tsd_tree(self):
        """Наполнить дерево заказами, которые готовы для отправки на ТСД"""
        # Очистить дерево
        for i in self.tsd_tree.get_children():
            self.tsd_tree.delete(i)
        
        # Добавить записи из self.download_list, которые не были отправлены на ТСД
        for item in self.download_list:
            document_id = item.get("document_id")
            
            if document_id not in self.sent_to_tsd_items and is_order_ready_for_tsd(item):
                
                vals = (
                    item.get("order_name"), 
                    document_id, 
                    item.get("status"), 
                    item.get("filename") or ""
                )
                self.tsd_tree.insert("", "end", iid=document_id, values=vals)
                print(f"✅ DEBUG: Заказ {document_id} добавлен в таблицу ТСД со статусом: {item.get('status')}")

    def get_selected_tsd_items(self):
        """Возвращает список объектов download_list, соответствующих выбранным строкам в tsd_tree."""
        try:
            sel = self.tsd_tree.selection()
            
            selected = []
            
            for iid in sel:
                # Получаем данные из дерева
                docid = iid  # или item_values[1] в зависимости от структуры
                
                # Ищем в download_list
                found_item = None
                for item in self.download_list:
                    if item.get("document_id") == docid:
                        found_item = item
                        break
                
                if found_item:
                    selected.append(found_item)
                else:
                    self.tsd_log_insert(f"❌ Заказ с ID {docid} не найден в download_list!")
            
            return selected
                
        except Exception as e:
            self.tsd_log_insert(f"❌ get_selected_tsd_items: Ошибка: {e}")
            return []
            
    def on_tsd_clicked(self):
        """Обработчик кнопки — собирает данные, запускает threads для выбранных заказов."""
        try:
            # Получаем выбранные элементы
            selected_items = self.get_selected_tsd_items()
            
            if not selected_items:
                self.tsd_log_insert("❌ ОШИБКА: Не выбрано ни одного заказа в таблице ТСД")
                return

            # Получаем данные из полей ввода
            intro_number = self.tsd_intro_number_entry.get().strip()
            prod_date_raw = self.tsd_prod_date_entry.get().strip()
            exp_date_raw = self.tsd_exp_date_entry.get().strip()
            batch_num = self.tsd_batch_entry.get().strip()
            
    

            # Преобразуем даты
            try:
                prod_date = self.convert_date_format(prod_date_raw)
                exp_date = self.convert_date_format(exp_date_raw)
            except Exception as e:
                self.tsd_log_insert(f"❌ ОШИБКА преобразования дат: {e}")
                return

            # Валидация полей формы
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
                    self.tsd_log_insert(f"❌ ОШИБКА валидации: {error}")
                return

            # Отключаем кнопку пока выполняется
            self.tsd_btn.configure(state="disabled")
            self.tsd_log_insert("🚀 Запуск создания заданий ТСД...")
            self.tsd_log_insert(f"📊 Будет обработано заказов: {len(selected_items)}")

            # Запускаем задачи
            futures = []
            skipped_items = []  # Для отслеживания пропущенных заказов
            
            for it in selected_items:
                try:
                    docid = it["document_id"]
                    order_name = it.get("order_name", "Unknown")
                    
                    simpl_name = it.get("simpl", "")
                    full_name = it.get("full_name", "Неизвестно")

                    # ПОЛУЧАЕМ GTIN - КРИТИЧЕСКИ ВАЖНЫЙ ЭТАП
                    gtin = None
                    
                    # Способ 1: Ищем напрямую в данных заказа
                    gtin = it.get("gtin")
                    
                    # Способ 2: Ищем через метод поиска по document_id
                    if not gtin:
                        gtin = self._get_gtin_for_order(docid)
                       
                    
                    # Способ 3: Извлекаем из структуры данных
                    if not gtin:
                        gtin = self._extract_gtin_from_order_data(it)
                       
                    
                    # Способ 4: Ищем в истории БД напрямую
                    if not gtin and hasattr(self, 'history_db'):
                        try:
                            history_order = self.history_db.get_order_by_document_id(docid)
                            if history_order and history_order.get('gtin'):
                                gtin = history_order.get('gtin')
                              
                        except Exception as e:
                            self.tsd_log_insert(f"⚠️ Ошибка при поиске GTIN в истории БД: {e}")

                    # КРИТИЧЕСКАЯ ПРОВЕРКА: если GTIN не найден, ПРЕКРАЩАЕМ обработку
                    if not gtin:
                        error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА: Не найден GTIN для заказа '{order_name}' (ID: {docid})"
                        self.tsd_log_insert(error_msg)
                        skipped_items.append({"item": it, "reason": "GTIN не найден"})
                        continue  # Пропускаем этот заказ

                    # Проверяем валидность GTIN
                    if not gtin.isdigit() or len(gtin) < 10:
                        error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА: Невалидный GTIN '{gtin}' для заказа '{order_name}'"
                        self.tsd_log_insert(error_msg)
                        skipped_items.append({"item": it, "reason": f"Невалидный GTIN: {gtin}"})
                        continue



                    # Получаем TNVED код
                    tnved_code = get_tnved_code(simpl_name)

                    # Формируем данные позиций
                    positions_data = [{
                        "name": full_name, 
                        "gtin": f"0{gtin}"  # Добавляем ведущий ноль
                    }]
              
                    
                    # Формируем production_patch
                    production_patch = {
                        "documentNumber": intro_number,
                        "productionDate": prod_date,
                        "expirationDate": exp_date,
                        "batchNumber": batch_num,
                        "TnvedCode": tnved_code
                    }
            
                    
             
                    session = SessionManager.get_session()
                    
                 
                    fut = self.intro_tsd_executor.submit(
                        self._tsd_worker, 
                        it, 
                        positions_data, 
                        production_patch, 
                        session
                    )
                    futures.append((fut, it))
                    
                except Exception as e:
                    error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА при подготовке заказа '{it.get('order_name', 'Unknown')}': {e}"
                    self.tsd_log_insert(error_msg)
                    import traceback
                    self.tsd_log_insert(f"🔍 Детали ошибки: {traceback.format_exc()}")
                    skipped_items.append({"item": it, "reason": f"Ошибка подготовки: {e}"})

            # Логируем информацию о пропущенных заказах
            if skipped_items:
                self.tsd_log_insert(f"⚠️ Пропущено заказов: {len(skipped_items)}")
                for skipped in skipped_items:
                    item = skipped["item"]
                    reason = skipped["reason"]
                    self.tsd_log_insert(f"   - '{item.get('order_name', 'Unknown')}' (ID: {item.get('document_id', 'Unknown')}): {reason}")

            # Проверяем, остались ли задачи для выполнения
            if not futures:
                error_msg = "❌ ОШИБКА: Нет задач для выполнения после подготовки"
                if skipped_items:
                    error_msg += f" (все {len(skipped_items)} заказов пропущены)"
                self.tsd_log_insert(error_msg)
                self.tsd_btn.configure(state="normal")
                
                # Показываем пользователю информативное сообщение
                if skipped_items:
                    reasons = "\n".join([f"- {s['item'].get('order_name', 'Unknown')}: {s['reason']}" for s in skipped_items])
                    tk.messagebox.showwarning(
                        "Не удалось отправить заказы", 
                        f"Не удалось отправить {len(skipped_items)} заказов:\n{reasons}"
                    )
                return

            # Создаём нитку-отслеживатель
            def tsd_monitor():
                try:
                    completed = 0
                    total = len(futures)
                    successful = 0
                    failed = 0
                    
                    for i, (fut, it) in enumerate(futures):
                        try:
                            # Ждем завершения задачи с таймаутом
                            ok, result = fut.result(timeout=300)  # 5 минут
                            
                            # Формируем сообщение
                            if ok:
                                intro_id = result.get('introduction_id', 'unknown')
                                msg = f"Успех: introduction_id = {intro_id}"
                                successful += 1
                            else:
                                errors = result.get('errors', ['unknown error'])
                                msg = f"Ошибка: {'; '.join(errors)}"
                                self.tsd_log_insert(f"❌ МОНИТОРИНГ: Задача {i+1}/{total} ОШИБКА: {msg}")
                                failed += 1
                            
                            self.after(0, self._on_tsd_finished, it, ok, msg)
                            completed += 1
                            
                        except Exception as e:
                            error_msg = f"Исключение при выполнении задачи: {e}"
                            self.tsd_log_insert(f"❌ МОНИТОРИНГ: Исключение в задаче {i+1}/{total}: {error_msg}")
                            import traceback
                            self.tsd_log_insert(f"🔍 Детали исключения: {traceback.format_exc()}")
                            
                            self.after(0, self._on_tsd_finished, it, False, error_msg)
                            completed += 1
                            failed += 1
                    
                    self.tsd_log_insert(f"📊 СТАТИСТИКА: Успешно: {successful}, Ошибки: {failed}, Всего: {total}")
                    
                except Exception as e:
                    self.tsd_log_insert(f"💥 КРИТИЧЕСКАЯ ОШИБКА в мониторе: {e}")
                    import traceback
                    self.tsd_log_insert(f"🔍 Детали критической ошибки: {traceback.format_exc()}")
                finally:
                    # Всегда разблокируем кнопку
                    self.after(0, lambda: self.tsd_btn.configure(state="normal"))
                    
                    # Показываем итоговое сообщение пользователю
                    if hasattr(self, 'successful') and hasattr(self, 'failed'):
                        success_count = getattr(self, 'successful', 0)
                        fail_count = getattr(self, 'failed', 0)
                        if success_count > 0 or fail_count > 0:
                            message = f"Обработка завершена:\nУспешно: {success_count}\nОшибки: {fail_count}"
                            self.after(0, lambda: tk.messagebox.showinfo("Результат", message))


            monitor_thread = threading.Thread(target=tsd_monitor, daemon=True)
            monitor_thread.start()

        except Exception as e:
            self.tsd_log_insert(f"💥 КРИТИЧЕСКАЯ ОШИБКА в on_tsd_clicked: {e}")
            import traceback
            self.tsd_log_insert(f"🔍 Детали критической ошибки: {traceback.format_exc()}")
            self.tsd_btn.configure(state="normal")

    def _tsd_worker(self, item: dict, positions_data: List[Dict[str, str]], production_patch: dict, session) -> Tuple[bool, Dict[str, Any]]:
        """
        Фоновая задача — производит ввод в оборот для одного заказа item.
        """
        try:
            document_id = item["document_id"]
            

            # ВЫЗОВ API
            try:
                
                ok, result = make_task_on_tsd(
                    session=session,
                    codes_order_id=document_id,
                    positions_data=positions_data,
                    production_patch=production_patch
                )
                
                
                if ok:
                    intro_id = result.get('introduction_id', 'unknown')
                    
                    # ПОМЕЧАЕМ ЗАКАЗ КАК ОБРАБОТАННЫЙ В ИСТОРИИ
                    from api import mark_order_as_tsd_created
                    mark_order_as_tsd_created(document_id, intro_id)
                else:
                    errors = result.get('errors', [])
                    self.tsd_log_insert(f"❌ _tsd_worker: ОШИБКА - {errors}")
                
                return ok, result
                
            except Exception as e:
                error_msg = f"Ошибка при вызове API: {e}"
                self.tsd_log_insert(f"❌ _tsd_worker: Исключение при вызове API: {error_msg}")
                import traceback
                self.tsd_log_insert(f"🔍 _tsd_worker: Детали исключения API: {traceback.format_exc()}")
                return False, {"errors": [error_msg]}
                
        except Exception as e:
            error_msg = f"Общая ошибка в _tsd_worker: {e}"
            self.tsd_log_insert(f"❌ _tsd_worker: Общая ошибка: {error_msg}")
            import traceback
            self.tsd_log_insert(f"🔍 _tsd_worker: Детали общей ошибки: {traceback.format_exc()}")
            return False, {"errors": [error_msg]}


    def clear_tsd_form(self):
        """Очищает поля формы ТСД после успешной отправки"""
        try:
            self.tsd_intro_number_entry.delete(0, 'end')
            self.tsd_batch_entry.delete(0, 'end')
            # Даты можно не очищать, так как они часто повторяются
        except Exception as e:
            logger.error(f"Ошибка при очистке формы ТСД: {e}")

    def show_info(self, message):
        """Показывает информационное сообщение"""
        tk.messagebox.showinfo("Информация", message)

    # И добавим вызов в _on_tsd_finished при успехе:
    def _on_tsd_finished(self, item: dict, ok: bool, msg: str):
        """Обновление GUI после завершения одного задания (в главном потоке)."""
        docid = item.get("document_id")
        order_name = item.get("order_name", "Unknown")
        
        if ok:
            self.tsd_log_insert("🎉 ЗАДАНИЕ УСПЕШНО СОЗДАНО!")
            self.sent_to_tsd_items.add(docid)
            item["status"] = "Отправлено на ТСД"
            self.show_info(f"Задание на ТСД для заказа '{order_name}' успешно создано!")
            
            # ВАЖНО: ДОБАВЛЯЕМ ПОМЕТКУ В ИСТОРИЮ
            try:
                # Извлекаем introduction_id из сообщения (пример: "Успех: introduction_id = 12345")
                if "introduction_id =" in msg:
                    intro_id = msg.split("introduction_id =")[1].strip()
                    self.history_db.mark_tsd_created(docid, intro_id)
            except Exception as e:
                self.tsd_log_insert(f"❌ Ошибка пометки заказа в истории: {e}")
            
            # ОЧИЩАЕМ ФОРМУ ПОСЛЕ УСПЕШНОЙ ОТПРАВКИ
            self.clear_tsd_form()
            remove_order_by_document_id(self.download_list, docid)
        else:
            self.tsd_log_insert(f"❌ [ОШИБКА] {order_name} (ID: {docid}) — {msg}")
            item["status"] = "Ошибка ТСД"

        self.update_tsd_tree()
        if hasattr(self, "update_download_tree"):
            self.update_download_tree()
        if hasattr(self, "update_introduction_tree"):
            self.update_introduction_tree()
    
        
    def _get_gtin_for_order(self, document_id):
        """Получает GTIN для заказа с детальной диагностикой"""
        try:
            self.tsd_log_insert(f"🔍 Поиск GTIN для document_id: {document_id}")
            
            # Сначала ищем в download_list
            for item in self.download_list:
                if item.get('document_id') == document_id:
                    gtin = item.get('gtin')
                    if gtin:
                        self.tsd_log_insert(f"✅ GTIN найден в download_list: {gtin}")
                        return gtin
                    else:
                        self.tsd_log_insert(f"❌ GTIN не найден в download_list для заказа {document_id}")
            
            # Если не нашли в download_list, ищем в истории БД
            try:
                history_order = self.history_db.get_order_by_document_id(document_id)
                if history_order and history_order.get('gtin'):
                    gtin = history_order.get('gtin')
                    self.tsd_log_insert(f"✅ GTIN найден в истории БД: {gtin}")
                    return gtin
            except Exception as e:
                self.tsd_log_insert(f"⚠️ Ошибка при поиске GTIN в истории БД: {e}")
            
            self.tsd_log_insert("❌ GTIN не найден ни в download_list, ни в истории БД")
            return None
            
        except Exception as e:
            self.tsd_log_insert(f"❌ Ошибка при поиске GTIN: {e}")
            return None

    def _extract_gtin_from_order_data(self, item):
        """Извлекает GTIN из данных заказа с детальной диагностикой"""
        try:
            self.tsd_log_insert(f"🔍 Попытка извлечь GTIN из данных заказа: {item.get('order_name', 'Unknown')}")
            
            # Проверяем различные возможные места хранения GTIN
            gtin = item.get('gtin')
            if gtin:
                self.tsd_log_insert(f"✅ GTIN найден непосредственно в item: {gtin}")
                return gtin
                
            # Проверяем history_data (данные из истории)
            if 'history_data' in item and item['history_data']:
                gtin = item['history_data'].get('gtin')
                if gtin:
                    self.tsd_log_insert(f"✅ GTIN найден в history_data: {gtin}")
                    return gtin
            
            # Проверяем вложенные структуры
            if 'history_entry' in item and item['history_entry']:
                gtin = item['history_entry'].get('gtin')
                if gtin:
                    self.tsd_log_insert(f"✅ GTIN найден в history_entry: {gtin}")
                    return gtin
            
            # Проверяем данные из API
            if 'api_data' in item:
                gtin = item['api_data'].get('gtin')
                if gtin:
                    self.tsd_log_insert(f"✅ GTIN найден в api_data: {gtin}")
                    return gtin
            
            self.tsd_log_insert("❌ Не удалось извлечь GTIN из данных заказа")
            return None
            
        except Exception as e:
            self.tsd_log_insert(f"❌ Ошибка при извлечении GTIN: {e}")
            return None

    def download_aggregate_codes(self, session, mode, target_value, status_filter="tsdProcessStart", limit=None):
        """Загружает aggregate codes в зависимости от выбранного режима"""
        base_url = "https://mk.kontur.ru/api/v1/aggregates"
        warehouse_id = "59739360-7d62-434b-ad13-4617c87a6d13"
        
        all_codes = []
        seen_codes = set()
        page_limit = 100
        offset = 0
        normalized_target = str(target_value or "").strip().lower()
        logger.info(
            "Агрегация: старт загрузки (mode=%s, target=%s, status=%s, limit=%s)",
            mode,
            target_value,
            status_filter,
            limit,
        )
        
        try:
            if mode == "comment" and not normalized_target:
                self.log_aggregation_message("❌ Ошибка: пустое наименование для поиска")
                logger.warning("Агрегация: пустое наименование для режима comment")
                return []

            while True:
                params = {
                    'warehouseId': warehouse_id,
                    'limit': page_limit,
                    'offset': offset,
                    'statuses': status_filter,
                    'sortField': 'createDate',
                    'sortOrder': 'descending'
                }
                
                try:
                    response = session.get(base_url, params=params, timeout=30)
                    response.raise_for_status()
                    
                    data = response.json()
                    items = data.get('items', [])
                    logger.info("Агрегация: получено %s записей (offset=%s)", len(items), offset)
                    
                    if not items:
                        break
                    
                    # Фильтрация в зависимости от режима
                    filtered_items = []
                    if mode == "comment":
                        filtered_items = [
                            item for item in items
                            if normalized_target in str(item.get('comment') or '').strip().lower()
                        ]
                    elif mode == "count":
                        filtered_items = items
                    logger.info("Агрегация: после фильтра mode=%s осталось %s", mode, len(filtered_items))
                    
                    # Добавляем отфильтрованные записи
                    for item in filtered_items:
                        aggregate_code = item.get('aggregateCode')
                        if aggregate_code and aggregate_code not in seen_codes:
                            seen_codes.add(aggregate_code)
                            all_codes.append({
                                'aggregateCode': aggregate_code,
                                'documentId': item.get('documentId'),
                                'createdDate': item.get('createdDate'),
                                'status': item.get('status'),
                                'updatedDate': item.get('updatedDate'),
                                'includesUnitsCount': item.get('includesUnitsCount'),
                                'comment': item.get('comment', ''),
                                'productGroup': item.get('productGroup'),
                                'aggregationType': item.get('aggregationType'),
                                'codesChecked': item.get('codesChecked'),
                                'codesCheckErrorsCount': item.get('codesCheckErrorsCount'),
                                'allowDelete': item.get('allowDelete')
                            })
                    
                    # Проверяем условия остановки
                    if mode == "count" and len(all_codes) >= int(target_value):
                        break
                    elif mode == "comment" and limit is not None and len(all_codes) >= limit:
                        break
                    
                    if len(items) < page_limit:
                        break
                    
                    offset += page_limit
                    time.sleep(0.3)
                    
                except Exception as e:
                    self.log_aggregation_message(f"❌ Ошибка при запросе: {str(e)}")
                    logger.exception("Агрегация: ошибка запроса (offset=%s)", offset)
                    break
            
            # Обрезаем до нужного количества
            if mode == "count" and len(all_codes) > int(target_value):
                all_codes = all_codes[:int(target_value)]
            elif mode == "comment" and limit is not None and len(all_codes) > limit:
                all_codes = all_codes[:limit]

            if mode == "comment" and not all_codes:
                self.log_aggregation_message("ℹ️ По указанному наименованию коды не найдены")
                logger.info("Агрегация: по наименованию '%s' совпадений не найдено", target_value)
            
            # СОРТИРОВКА ПО ВОЗРАСТАНИЮ НОМЕРА
            # Поскольку коды выглядят как "04650118042512020000000010" и "04650118042512010000000428",
            # сортируем по числовой части в конце строки
            all_codes.sort(key=lambda x: int(x['aggregateCode'][-10:]) if len(x['aggregateCode']) >= 10 else x['aggregateCode'])
            logger.info("Агрегация: успешно отобрано %s кодов (mode=%s)", len(all_codes), mode)
            
            return all_codes
            
        except Exception as e:
            self.log_aggregation_message(f"❌ Критическая ошибка при загрузке кодов: {str(e)}")
            logger.exception("Агрегация: критическая ошибка загрузки")
            return []

    def create_aggregate_codes(
        self,
        session,
        comment,
        count,
        extension_symbol="0",
        aggregation_type="gs1GlnAggregate",
    ):
        """Создает новые агрегационные коды"""
        base_url = "https://mk.kontur.ru/api/v1/aggregates"
        warehouse_id = "59739360-7d62-434b-ad13-4617c87a6d13"
        payload = {
            "extensionSymbol": extension_symbol,
            "comment": comment,
            "count": int(count),
            "productGroup": PRODUCT_GROUP or "wheelChairs",
            "aggregationType": aggregation_type,
        }

        logger.info(
            "Агрегация: создание кодов (warehouse_id=%s, comment=%s, count=%s)",
            warehouse_id,
            comment,
            count,
        )

        response = session.post(
            base_url,
            params={"warehouseId": warehouse_id},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Некорректный ответ сервиса создания агрегации")

        logger.info("Агрегация: сервис вернул %s идентификаторов", len(data))
        return data

    def save_simple_csv(self, codes, filename):
        """Сохраняет только коды в простом CSV с сортировкой по возрастанию"""
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        parent_dir = os.path.join(desktop, "Агрег коды км")
        target_dir = os.path.join(parent_dir, filename)
        os.makedirs(target_dir, exist_ok=True)

        target_path = os.path.join(target_dir, filename)
        if not codes:
            return None
        
        try:
            # СОРТИРУЕМ коды перед сохранением
            sorted_codes = sorted(codes, key=lambda x: int(x['aggregateCode'][-10:]) if len(x['aggregateCode']) >= 10 else x['aggregateCode'])
            
            with open(target_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                for code in sorted_codes:
                    writer.writerow([code['aggregateCode']])
            
            return target_dir
        except Exception as e:
            print(f"Ошибка при сохранении CSV: {e}")
            return None

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

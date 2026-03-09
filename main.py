import csv
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
from get_thumb import get_thumbprint
from history_db import OrderHistoryDB
import update
import customtkinter as ctk
from customtkinter import CTkScrollableFrame
import tkinter as tk
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
THUMBPRINT = get_thumbprint()
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
        
        repo_dir = os.path.abspath(os.path.dirname(__file__))
        update.check_for_updates(repo_dir=repo_dir, pre_update_cleanup=self.cleanup_before_update, auto_restart=True)
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
        self.auto_download_active = False
        self.execute_all_executor = ThreadPoolExecutor(max_workers=3)
        self.intro_executor = ThreadPoolExecutor(max_workers=3)
        self.intro_tsd_executor = ThreadPoolExecutor(max_workers=3)
        
        self.start_auto_status_check()
        
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
        self.download_agg_btn = None
        self.agg_progress = None
        self.agg_log_text = None
        
        # Атрибуты для навигации
        self.sidebar_frame = None
        self.main_content = None
        self.theme_button = None
        self.nav_buttons = {}
        self.content_frames = {}
        
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
            width=280,
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
            ("create", "📋 Заказ кодов", self.show_create_frame),
            ("download", "⏬ Загрузка кодов", self.show_download_frame),
            ("intro", "🔄 Ввод в оборот", self.show_intro_frame),
            ("intro_tsd", "📲 Задание на ТСД", self.show_intro_tsd_frame),
            ("aggregation", "📦 Коды агрегации", self.show_aggregation_frame)
        ]
        
        nav_font = ctk.CTkFont(family="Segoe UI", size=13, weight="normal")
        nav_font_bold = ctk.CTkFont(family="Segoe UI", size=13, weight="bold")
        
        for nav_id, text, command in nav_items:
            # Контейнер для кнопки навигации
            nav_item_frame = ctk.CTkFrame(nav_frame, fg_color="transparent", height=48)
            nav_item_frame.pack(fill="x", pady=2)
            nav_item_frame.pack_propagate(False)
            
            # Индикатор активного состояния (изначально скрыт)
            active_indicator = ctk.CTkFrame(
                nav_item_frame, 
                width=4, 
                fg_color="transparent",
                corner_radius=2
            )
            active_indicator.pack(side="left", fill="y", padx=(2, 0))
            
            # Кнопка навигации
            btn = ctk.CTkButton(
                nav_item_frame,
                text=text,
                command=command,
                anchor="w",
                height=44,
                font=nav_font,
                fg_color="transparent",
                hover_color=self._get_color("secondary"),
                text_color=self._get_color("text_primary"),
                corner_radius=8,
                border_spacing=15
            )
            btn.pack(side="left", fill="x", expand=True, padx=(8, 0))
            
            # Сохраняем ссылки на элементы для управления состоянием
            self.nav_buttons[nav_id] = {
                'button': btn,
                'indicator': active_indicator,
                'frame': nav_item_frame,
                'font_normal': nav_font,
                'font_bold': nav_font_bold
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
            text="⛶ Оконный режим",
            command=self.toggle_fullscreen,
            height=38,
            font=ctk.CTkFont(family="Segoe UI", size=11, weight="normal"),
            fg_color="transparent",
            hover_color=self._get_color("secondary"),
            text_color=self._get_color("text_secondary"),
            corner_radius=6
        )
        self.fullscreen_button.pack(fill="x")
    
    def _setup_navigation_animations(self):
        """Настройка анимаций для навигации"""
        for nav_id, elements in self.nav_buttons.items():
            elements['button'].bind('<Enter>', lambda e, btn=elements['button']: self._animate_nav_hover(btn, True))
            elements['button'].bind('<Leave>', lambda e, btn=elements['button']: self._animate_nav_hover(btn, False))

    def _animate_nav_hover(self, button, is_hover):
        """Анимация при наведении на элемент навигации"""
        if is_hover:
            # Плавное изменение цвета при наведении
            button.configure(fg_color=self._get_color("secondary"))
        else:
            # Возврат к исходному цвету
            current_bg = button.cget("fg_color")
            if current_bg != self._get_color("primary"):  # Если не активный элемент
                button.configure(fg_color="transparent")

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
            "aggregation": "aggregation"
        }
        
        active_nav_id = frame_to_nav_id.get(active_frame, "")
        
        for nav_id, elements in self.nav_buttons.items():
            if nav_id == active_nav_id:
                # Активное состояние
                elements['button'].configure(
                    fg_color=self._get_color("primary"),
                    text_color="white",
                    font=elements['font_bold'],
                    hover_color=self._get_color("primary")
                )
                elements['indicator'].configure(
                    fg_color=self._get_color("accent")
                )
            else:
                # Неактивное состояние
                elements['button'].configure(
                    fg_color="transparent",
                    text_color=self._get_color("text_primary"),
                    font=elements['font_normal'],
                    hover_color=self._get_color("secondary")
                )
                elements['indicator'].configure(
                    fg_color="transparent"
                )

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
        self._update_navigation_style(frame_name)

    def show_create_frame(self):
        """Показать раздел создания заказов"""
        self.show_content_frame("create")

    def show_download_frame(self):
        """Показать раздел загрузки кодов"""
        self.show_content_frame("download")

    def show_intro_frame(self):
        """Показать раздел введения в оборот"""
        self.show_content_frame("intro")

    def show_intro_tsd_frame(self):
        """Показать раздел введения TSD"""
        self.show_content_frame("intro_tsd")

    def show_aggregation_frame(self):
        """Показать раздел кодов агрегации"""
        self.show_content_frame("aggregation")

    

    def _get_color(self, color_name):
        """Получение цвета из текущей темы"""
        theme = self.color_themes[self.current_theme]
        return theme.get(color_name, "#FFFFFF")


    def _update_theme_colors(self):
        """Обновление цветов интерфейса при смене темы"""
        if hasattr(self, 'sidebar_frame') and self.sidebar_frame:
            self.sidebar_frame.configure(fg_color=self._get_color("bg_secondary"))
        
        if hasattr(self, 'status_bar') and self.status_bar:
            status_frame = self.status_bar.master
            if status_frame:
                status_frame.configure(fg_color=self._get_color("bg_secondary"))
            self.status_bar.configure(text_color=self._get_color("text_secondary"))
        
        # Обновляем навигацию с новой структурой
        if hasattr(self, 'nav_buttons') and self.nav_buttons:
            for nav_id, elements in self.nav_buttons.items():
                if 'button' in elements and elements['button']:
                    elements['button'].configure(
                        hover_color=self._get_color("secondary"),
                        text_color=self._get_color("text_primary")
                    )

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
        
        # Карточка с настройками
        settings_card = ctk.CTkFrame(main_frame, corner_radius=12)
        settings_card.pack(fill="x", pady=(0, 20))
        
        ctk.CTkLabel(
            settings_card,
            text="Настройки поиска",
            font=self.fonts["subheading"],
            text_color=self._get_color("text_primary")
        ).pack(anchor="w", padx=20, pady=(20, 10))
        
        # Переключатель режимов в современном стиле
        mode_frame = ctk.CTkFrame(settings_card, fg_color="transparent")
        mode_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(
            mode_frame,
            text="Режим поиска:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary")
        ).pack(side="left", padx=(0, 15))
        
        self.agg_mode_var = ctk.StringVar(value="count")
        
        # Стилизованные радиокнопки
        mode_options_frame = ctk.CTkFrame(mode_frame, fg_color="transparent")
        mode_options_frame.pack(side="left", fill="x", expand=True)
        
        count_radio = ctk.CTkRadioButton(
            mode_options_frame,
            text="🔢 По количеству",
            variable=self.agg_mode_var,
            value="count",
            command=self.toggle_aggregation_mode,
            font=self.fonts["normal"],
            border_color=self._get_color("primary"),
            hover_color=self._get_color("accent")
        )
        count_radio.pack(side="left", padx=(0, 20))
        
        comment_radio = ctk.CTkRadioButton(
            mode_options_frame,
            text="📝 По наименованию", 
            variable=self.agg_mode_var,
            value="comment",
            command=self.toggle_aggregation_mode,
            font=self.fonts["normal"],
            border_color=self._get_color("primary"),
            hover_color=self._get_color("accent")
        )
        comment_radio.pack(side="left")
        
        # Поля ввода в современном стиле
        input_frame = ctk.CTkFrame(settings_card, fg_color="transparent")
        input_frame.pack(fill="x", padx=20, pady=10)
        
        self.count_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        self.count_frame.pack(fill="x")
        
        ctk.CTkLabel(
            self.count_frame,
            text="Количество кодов:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary")
        ).pack(side="left", padx=(0, 15))
        
        self.count_entry = ctk.CTkEntry(
            self.count_frame,
            width=200,
            placeholder_text="Введите количество...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.count_entry.pack(side="left")
        
        # Поле комментария (изначально скрыто)
        self.comment_frame = ctk.CTkFrame(input_frame, fg_color="transparent")
        
        ctk.CTkLabel(
            self.comment_frame,
            text="Наименование товара:",
            font=self.fonts["normal"],
            text_color=self._get_color("text_primary")
        ).pack(side="left", padx=(0, 15))
        
        self.comment_entry = ctk.CTkEntry(
            self.comment_frame,
            width=300,
            placeholder_text="Введите наименование...",
            font=self.fonts["normal"],
            height=40,
            corner_radius=8,
            border_color=self._get_color("secondary")
        )
        self.comment_entry.pack(side="left")
        
        # Стилизованная кнопка загрузки
        self.download_agg_btn = ctk.CTkButton(
            settings_card,
            text="🚀 Начать загрузку кодов",
            command=self.start_aggregation_download,
            height=45,
            font=self.fonts["button"],
            fg_color=self._get_color("primary"),
            hover_color=self._get_color("accent"),
            corner_radius=8,
            border_width=0
        )
        self.download_agg_btn.pack(pady=20)
        
        # Прогресс-бар в современном стиле
        progress_frame = ctk.CTkFrame(settings_card, fg_color="transparent")
        progress_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        ctk.CTkLabel(
            progress_frame,
            text="Прогресс загрузки:",
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
            self.count_frame.pack(fill="x", padx=10, pady=10)
            self.comment_frame.pack_forget()
        else:
            self.count_frame.pack_forget()
            self.comment_frame.pack(fill="x", padx=10, pady=10)

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
            limit = int(target_value) if mode == "count" else 100
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
                self.status_bar.configure(text=f"Загружено {len(codes)} кодов агрегации")
            else:
                logger.error("❌ Не удалось загрузить данные")
                
        except Exception as e:
            logger.error(f"❌ Ошибка: {str(e)}")
        finally:
            # Разблокируем кнопку
            self.download_agg_btn.configure(state="normal", text="🚀 Загрузить коды агрегации")
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
        
        download_columns = ("order_name", "status", "filename", "document_id")
        self.download_tree = ttk.Treeview(table_inner_frame, columns=download_columns, show="headings", height=12)
        
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
        
        # Нижняя часть - лог
        log_container = ctk.CTkFrame(columns_frame, corner_radius=8)
        log_container.pack(fill="both", expand=True, pady=(10, 0))
        
        ctk.CTkLabel(
            log_container, 
            text="Лог скачивания", 
            font=self.fonts["subheading"]
        ).pack(anchor="w", pady=(15, 10), padx=15)
        
        self.download_log_text = ctk.CTkTextbox(log_container, height=150)
        self.download_log_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))


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
                
                download_item = {
                    'order_name': order_item.order_name, 
                    'document_id': document_id,
                    'status': 'Ожидает',
                    'filename': None,
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
        try:
            self.log_text.configure(state="normal")
            self.log_text.insert("end", f"{msg}\n")
            self.log_text.see("end")  # Автопрокрутка к новому сообщению
            self.log_text.configure(state="disabled")
        except Exception as e:
            logger.error(f"Ошибка при записи в лог: {e}")

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
                    pending_orders = [item for item in self.download_list 
                                    if item['status'] not in ['Скачивается', 'Скачан', 'Ошибка генерации', 'Из истории']
                                    and not item.get('from_history', False)]  # Исключаем заказы из истории
                    
                    if not pending_orders:
                        continue
                    
                    self.download_log_insert(f"🔍 Проверка статусов для {len(pending_orders)} заказов (исключая историю)")
                    
                    # Проверяем статусы и запускаем скачивание для готовых
                    for item in pending_orders:
                        if not self.auto_download_active:
                            break
                        
                        if item.get('downloading', False):
                            continue  # Пропускаем, если уже скачивается
                            
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
        self.download_log_text.insert("end", f"[{timestamp}] {msg}\n")
        self.download_log_text.see("end")

    def on_closing(self):
        self.auto_download_active = False
        for executor in [self.download_executor, self.status_check_executor,
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
        
        # Заполнение дат по умолчанию
        today = datetime.now().strftime("%d-%m-%Y")
        future_date = (datetime.now() + timedelta(days=1826)).strftime("%d-%m-%Y")
        
        if self.prod_date_intro_entry:
            self.prod_date_intro_entry.insert(0, today)
        if self.exp_date_intro_entry:
            self.exp_date_intro_entry.insert(0, future_date)
        
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
                
            if not thumbprint:
                errors.append("Введите отпечаток сертификата.")

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
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = f"{now} - {text}"
            
            self.intro_log_text.configure(state="normal")
            self.intro_log_text.insert("end", msg + "\n")
            self.intro_log_text.see("end")
        except Exception as e:
            logger.error(f"Ошибка записи в лог: {e}")

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

    def _intro_worker(self, item: dict, production_patch: dict, thumbprint: str):
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
        
        # Заполнение дат по умолчанию
        today = datetime.now().strftime("%d-%m-%Y")
        future_date = (datetime.now() + timedelta(days=1826)).strftime("%d-%m-%Y")
        if self.tsd_prod_date_entry:
            self.tsd_prod_date_entry.insert(0, today)
        if self.tsd_exp_date_entry:
            self.tsd_exp_date_entry.insert(0, future_date)
        
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
        msg = f"{now} - {text}\n"
        try:
            self.tsd_log_text.insert("end", msg)
            self.tsd_log_text.see("end")
        except Exception:
            pass

    def update_tsd_tree(self):
        """Наполнить дерево заказами, которые готовы для отправки на ТСД"""
        # Очистить дерево
        for i in self.tsd_tree.get_children():
            self.tsd_tree.delete(i)
        
        # Добавить записи из self.download_list, которые не были отправлены на ТСД
        for item in self.download_list:
            document_id = item.get("document_id")
            
            # ВАЖНО: Показываем заказы, которые готовы для ТСД (включая из истории)
            if (document_id not in self.sent_to_tsd_items and 
                item.get("status") in ("Скачан", "Downloaded", "Ожидает", "Скачивается", "Готов для ТСД") or 
                item.get("filename")):
                
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
        else:
            self.tsd_log_insert(f"❌ [ОШИБКА] {order_name} (ID: {docid}) — {msg}")
            item["status"] = "Ошибка ТСД"

        self.update_tsd_tree()
    
        
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

    def download_aggregate_codes(self, session, mode, target_value, status_filter="tsdProcessStart", limit=100):
        """Загружает aggregate codes в зависимости от выбранного режима"""
        base_url = "https://mk.kontur.ru/api/v1/aggregates"
        warehouse_id = "59739360-7d62-434b-ad13-4617c87a6d13"
        
        all_codes = []
        page_limit = 100
        offset = 0
        
        try:
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
                    response = session.get(base_url, params=params)
                    response.raise_for_status()
                    
                    data = response.json()
                    items = data.get('items', [])
                    
                    if not items:
                        break
                    
                    # Фильтрация в зависимости от режима
                    filtered_items = []
                    if mode == "comment":
                        filtered_items = [item for item in items if item.get('comment') == target_value]
                    elif mode == "count":
                        filtered_items = items
                    
                    # Добавляем отфильтрованные записи
                    for item in filtered_items:
                        aggregate_code = item.get('aggregateCode')
                        if aggregate_code and aggregate_code not in [c['aggregateCode'] for c in all_codes]:
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
                    elif mode == "comment" and len(all_codes) >= limit:
                        break
                    
                    if len(items) < page_limit:
                        break
                    
                    offset += page_limit
                    time.sleep(0.3)
                    
                except Exception as e:
                    self.log_aggregation_message(f"❌ Ошибка при запросе: {str(e)}")
                    break
            
            # Обрезаем до нужного количества
            if mode == "count" and len(all_codes) > int(target_value):
                all_codes = all_codes[:int(target_value)]
            elif mode == "comment" and len(all_codes) > limit:
                all_codes = all_codes[:limit]
            
            # СОРТИРОВКА ПО ВОЗРАСТАНИЮ НОМЕРА
            # Поскольку коды выглядят как "04650118042512020000000010" и "04650118042512010000000428",
            # сортируем по числовой части в конце строки
            all_codes.sort(key=lambda x: int(x['aggregateCode'][-10:]) if len(x['aggregateCode']) >= 10 else x['aggregateCode'])
            
            return all_codes
            
        except Exception as e:
            self.log_aggregation_message(f"❌ Критическая ошибка при загрузке кодов: {str(e)}")
            return []

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

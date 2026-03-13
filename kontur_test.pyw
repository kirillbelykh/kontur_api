from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import customtkinter as ctk
import pandas as pd  # type: ignore
import tkinter as tk
from tkinter import ttk

from desktop_shortcuts import ensure_kontur_test_shortcut
from logger import logger
from main import App, NOMENCLATURE_XLSX, SessionManager
from options import color_options, simplified_options, size_options, units_options, venchik_options


class KonturTestApp(App):
    def __init__(self, df):
        super().__init__(df)
        self.title("KonturTEST")
        self.is_fullscreen = False
        self.attributes("-fullscreen", False)
        self.geometry("1480x920")
        self.minsize(1360, 820)
        self.fullscreen_button.configure(text="Полный экран")
        self.center_window()
        self._bind_global_clipboard_shortcuts()
        self._apply_entry_context_menus()
        self._refresh_live_summary()
        self._start_summary_loop()
        self._announce("Новый интерфейс готов. Можно работать как обычно, но без технического шума.", "success")

    def _init_ui_attributes(self):
        super()._init_ui_attributes()
        self.sent_to_tsd_items = set()
        self.activity_feeds: dict[str, ctk.CTkScrollableFrame] = {}
        self.activity_items: dict[str, list[ctk.CTkFrame]] = {}
        self.activity_empty_labels: dict[str, ctk.CTkLabel] = {}
        self.metric_labels: dict[str, ctk.CTkLabel] = {}
        self.page_titles: dict[str, tuple[str, str]] = {}
        self.current_page = "create"
        self.status_banner = None
        self.status_bar = None
        self.page_title_label = None
        self.page_subtitle_label = None
        self.summary_loop_started = False

    def _setup_modern_fonts(self):
        title_family = "Bahnschrift SemiBold"
        body_family = "Segoe UI"
        code_family = "Cascadia Code"
        self.fonts = {
            "hero": ctk.CTkFont(family=title_family, size=34, weight="bold"),
            "title": ctk.CTkFont(family=title_family, size=26, weight="bold"),
            "section": ctk.CTkFont(family=title_family, size=18, weight="bold"),
            "subheading": ctk.CTkFont(family=title_family, size=15, weight="bold"),
            "normal": ctk.CTkFont(family=body_family, size=13, weight="normal"),
            "small": ctk.CTkFont(family=body_family, size=11, weight="normal"),
            "button": ctk.CTkFont(family=title_family, size=13, weight="bold"),
            "nav": ctk.CTkFont(family=title_family, size=13, weight="bold"),
            "mono": ctk.CTkFont(family=code_family, size=11, weight="normal"),
        }

    def _setup_modern_ui(self):
        self.current_theme = "atelier"
        self.color_themes = {
            "atelier": {
                "primary": "#1F4D4D",
                "secondary": "#DCE6E1",
                "accent": "#D46A4D",
                "success": "#3D7A57",
                "warning": "#C67A28",
                "error": "#B64A3F",
                "bg_primary": "#F5EFE6",
                "bg_secondary": "#FFF9F1",
                "bg_sidebar": "#132E2E",
                "surface_alt": "#E9E2D4",
                "text_primary": "#142121",
                "text_secondary": "#5F6B66",
                "text_inverse": "#FFF8F1",
                "line": "#D7D1C5",
                "chip": "#E7DED0",
            }
        }
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        self.configure(fg_color=self._get_color("bg_primary"))

        self.main_container = ctk.CTkFrame(self, fg_color=self._get_color("bg_primary"), corner_radius=0)
        self.main_container.pack(fill="both", expand=True)

        self._create_sidebar()
        self._create_app_shell()
        self.show_content_frame("create")

        self.bind("<Escape>", self.toggle_fullscreen)
        self.bind("<F11>", self.toggle_fullscreen)

    def _create_sidebar(self):
        self.sidebar_frame = ctk.CTkFrame(
            self.main_container,
            width=250,
            corner_radius=0,
            fg_color=self._get_color("bg_sidebar"),
        )
        self.sidebar_frame.pack(side="left", fill="y")
        self.sidebar_frame.pack_propagate(False)

        brand_wrap = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        brand_wrap.pack(fill="x", padx=24, pady=(28, 16))

        brand_mark = ctk.CTkFrame(
            brand_wrap,
            width=62,
            height=62,
            corner_radius=18,
            fg_color=self._get_color("accent"),
        )
        brand_mark.pack(anchor="w")
        brand_mark.pack_propagate(False)

        ctk.CTkLabel(
            brand_mark,
            text="KT",
            font=ctk.CTkFont(family="Bahnschrift SemiBold", size=20, weight="bold"),
            text_color=self._get_color("text_inverse"),
        ).pack(expand=True)

        ctk.CTkLabel(
            brand_wrap,
            text="KonturTEST",
            font=self.fonts["title"],
            text_color=self._get_color("text_inverse"),
        ).pack(anchor="w", pady=(14, 0))
        ctk.CTkLabel(
            brand_wrap,
            text="Новая рабочая станция для маркировки.\nТихий интерфейс, понятные шаги, быстрые действия.",
            font=self.fonts["small"],
            justify="left",
            text_color="#CFE0DA",
        ).pack(anchor="w", pady=(6, 0))

        quick_state = ctk.CTkFrame(
            self.sidebar_frame,
            corner_radius=18,
            fg_color="#1B3B3B",
            border_color="#2C5959",
            border_width=1,
        )
        quick_state.pack(fill="x", padx=20, pady=(4, 18))

        self.connection_indicator = ctk.CTkLabel(
            quick_state,
            text="Сессия в порядке",
            font=self.fonts["small"],
            text_color="#D8E6E1",
        )
        self.connection_indicator.pack(anchor="w", padx=16, pady=(14, 4))

        self.status_banner = ctk.CTkLabel(
            quick_state,
            text="Готово к работе",
            font=self.fonts["normal"],
            justify="left",
            wraplength=180,
            text_color=self._get_color("text_inverse"),
        )
        self.status_banner.pack(anchor="w", padx=16, pady=(0, 14))

        nav_items = [
            ("create", "Заказ кодов", "Собрать пакет", self.show_create_frame),
            ("download", "Загрузка", "Файлы и статусы", self.show_download_frame),
            ("intro", "Ввод", "Обычный ввод", self.show_intro_frame),
            ("intro_tsd", "ТСД", "Отправка на терминал", self.show_intro_tsd_frame),
            ("aggregation", "Агрегация", "Подбор кодов", self.show_aggregation_frame),
        ]

        self.nav_buttons = {}
        nav_wrap = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        nav_wrap.pack(fill="x", padx=16)

        for nav_id, title, subtitle, command in nav_items:
            item_frame = ctk.CTkFrame(nav_wrap, fg_color="transparent", corner_radius=14, height=58)
            item_frame.pack(fill="x", pady=4)
            item_frame.pack_propagate(False)

            button = ctk.CTkButton(
                item_frame,
                text=f"{title}\n{subtitle}",
                command=command,
                anchor="w",
                height=58,
                font=self.fonts["nav"],
                fg_color="transparent",
                hover_color="#214747",
                text_color="#EAF2EF",
                corner_radius=14,
                border_spacing=18,
            )
            button.pack(fill="both", expand=True)

            self.nav_buttons[nav_id] = {"button": button}

        spacer = ctk.CTkFrame(self.sidebar_frame, fg_color="transparent")
        spacer.pack(fill="both", expand=True)

        self.fullscreen_button = ctk.CTkButton(
            self.sidebar_frame,
            text="Полный экран",
            command=self.toggle_fullscreen,
            height=42,
            font=self.fonts["button"],
            fg_color="#1D4444",
            hover_color="#265353",
            text_color=self._get_color("text_inverse"),
            corner_radius=14,
        )
        self.fullscreen_button.pack(fill="x", padx=20, pady=(0, 24))

    def _create_app_shell(self):
        shell = ctk.CTkFrame(self.main_container, fg_color="transparent", corner_radius=0)
        shell.pack(side="right", fill="both", expand=True)

        header = ctk.CTkFrame(
            shell,
            fg_color=self._get_color("bg_secondary"),
            corner_radius=0,
            border_color=self._get_color("line"),
            border_width=1,
        )
        header.pack(fill="x", padx=20, pady=(20, 12))

        title_wrap = ctk.CTkFrame(header, fg_color="transparent")
        title_wrap.pack(side="left", fill="x", expand=True, padx=24, pady=20)

        ctk.CTkLabel(
            title_wrap,
            text="ATELIER WORKFLOW",
            font=self.fonts["small"],
            text_color=self._get_color("accent"),
        ).pack(anchor="w")

        self.page_title_label = ctk.CTkLabel(
            title_wrap,
            text="Заказ кодов",
            font=self.fonts["hero"],
            text_color=self._get_color("text_primary"),
        )
        self.page_title_label.pack(anchor="w", pady=(6, 0))

        self.page_subtitle_label = ctk.CTkLabel(
            title_wrap,
            text="Соберите понятный пакет позиций и отправьте его без лишнего шума.",
            font=self.fonts["normal"],
            text_color=self._get_color("text_secondary"),
        )
        self.page_subtitle_label.pack(anchor="w", pady=(6, 0))

        status_wrap = ctk.CTkFrame(header, fg_color="transparent")
        status_wrap.pack(side="right", padx=24, pady=20)

        now_label = ctk.CTkLabel(
            status_wrap,
            text=datetime.now().strftime("%d.%m.%Y"),
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        )
        now_label.pack(anchor="e")

        self.status_bar = ctk.CTkLabel(
            status_wrap,
            text="Готово к работе",
            font=self.fonts["subheading"],
            text_color=self._get_color("primary"),
        )
        self.status_bar.pack(anchor="e", pady=(8, 0))

        self.summary_strip = ctk.CTkFrame(shell, fg_color="transparent")
        self.summary_strip.pack(fill="x", padx=20, pady=(0, 12))
        self._create_summary_cards()

        self.main_content = ctk.CTkFrame(shell, fg_color="transparent", corner_radius=0)
        self.main_content.pack(fill="both", expand=True, padx=20, pady=(0, 20))

        self.content_frames = {}
        self._setup_create_frame()
        self._setup_download_frame()
        self._setup_introduction_frame()
        self._setup_introduction_tsd_frame()
        self._setup_aggregation_frame()

    def _create_summary_cards(self):
        cards = [
            ("bundle", "Позиции в наборе", "0"),
            ("queue", "Заказы в очереди", "0"),
            ("tsd_ready", "Готово к ТСД", "0"),
            ("tsd_sent", "Отправлено", "0"),
        ]

        for column, (key, title, value) in enumerate(cards):
            self.summary_strip.grid_columnconfigure(column, weight=1)
            card = ctk.CTkFrame(
                self.summary_strip,
                fg_color=self._get_color("bg_secondary"),
                corner_radius=18,
                border_color=self._get_color("line"),
                border_width=1,
            )
            card.grid(row=0, column=column, sticky="nsew", padx=6)

            ctk.CTkLabel(
                card,
                text=title,
                font=self.fonts["small"],
                text_color=self._get_color("text_secondary"),
            ).pack(anchor="w", padx=18, pady=(16, 6))

            value_label = ctk.CTkLabel(
                card,
                text=value,
                font=self.fonts["title"],
                text_color=self._get_color("text_primary"),
            )
            value_label.pack(anchor="w", padx=18, pady=(0, 4))

            self.metric_labels[key] = value_label

    def _create_page_shell(self, frame_name: str, eyebrow: str, title: str, subtitle: str):
        page = ctk.CTkScrollableFrame(self.main_content, fg_color="transparent", corner_radius=0)
        self.content_frames[frame_name] = page
        self.page_titles[frame_name] = (title, subtitle)

        hero = ctk.CTkFrame(
            page,
            fg_color=self._get_color("bg_secondary"),
            corner_radius=22,
            border_color=self._get_color("line"),
            border_width=1,
        )
        hero.pack(fill="x", pady=(0, 14))

        ctk.CTkLabel(
            hero,
            text=eyebrow,
            font=self.fonts["small"],
            text_color=self._get_color("accent"),
        ).pack(anchor="w", padx=24, pady=(22, 6))

        ctk.CTkLabel(
            hero,
            text=title,
            font=self.fonts["title"],
            text_color=self._get_color("text_primary"),
        ).pack(anchor="w", padx=24)

        ctk.CTkLabel(
            hero,
            text=subtitle,
            font=self.fonts["normal"],
            text_color=self._get_color("text_secondary"),
            justify="left",
            wraplength=860,
        ).pack(anchor="w", padx=24, pady=(8, 22))

        body = ctk.CTkFrame(page, fg_color="transparent")
        body.pack(fill="both", expand=True)
        return body

    def _create_card(self, parent, title: str, subtitle: str | None = None):
        card = ctk.CTkFrame(
            parent,
            fg_color=self._get_color("bg_secondary"),
            corner_radius=20,
            border_color=self._get_color("line"),
            border_width=1,
        )
        ctk.CTkLabel(
            card,
            text=title,
            font=self.fonts["section"],
            text_color=self._get_color("text_primary"),
        ).pack(anchor="w", padx=20, pady=(18, 0))

        if subtitle:
            ctk.CTkLabel(
                card,
                text=subtitle,
                font=self.fonts["small"],
                text_color=self._get_color("text_secondary"),
                justify="left",
                wraplength=540,
            ).pack(anchor="w", padx=20, pady=(6, 14))
        return card

    def _create_activity_panel(self, parent, key: str, title: str, subtitle: str):
        card = self._create_card(parent, title, subtitle)
        feed = ctk.CTkScrollableFrame(card, fg_color="transparent", corner_radius=0, height=240)
        feed.pack(fill="both", expand=True, padx=14, pady=(0, 14))

        empty_label = ctk.CTkLabel(
            feed,
            text="Здесь появятся понятные статусы, готовность и результаты.",
            font=self.fonts["normal"],
            text_color=self._get_color("text_secondary"),
            justify="left",
            wraplength=360,
        )
        empty_label.pack(anchor="w", padx=6, pady=8)

        self.activity_feeds[key] = feed
        self.activity_items[key] = []
        self.activity_empty_labels[key] = empty_label
        return card

    def _create_hidden_log_box(self, parent):
        widget = ctk.CTkTextbox(parent, width=1, height=1)
        widget.configure(state="disabled")
        return widget

    def _style_primary_button(self, widget):
        widget.configure(
            height=42,
            font=self.fonts["button"],
            fg_color=self._get_color("primary"),
            hover_color="#285B5B",
            text_color=self._get_color("text_inverse"),
            corner_radius=14,
        )

    def _style_secondary_button(self, widget):
        widget.configure(
            height=42,
            font=self.fonts["button"],
            fg_color=self._get_color("surface_alt"),
            hover_color=self._get_color("chip"),
            text_color=self._get_color("text_primary"),
            corner_radius=14,
        )

    def _style_alert_button(self, widget):
        widget.configure(
            height=42,
            font=self.fonts["button"],
            fg_color=self._get_color("accent"),
            hover_color="#C75B40",
            text_color=self._get_color("text_inverse"),
            corner_radius=14,
        )

    def _setup_create_frame(self):
        body = self._create_page_shell(
            "create",
            "СТАРТ ПАКЕТА",
            "Сборка заказа без лишних кликов",
            "Добавляйте позиции в понятной форме, следите за составом пакета справа и запускайте выполнение одной кнопкой.",
        )
        body.grid_columnconfigure(0, weight=4)
        body.grid_columnconfigure(1, weight=6)
        body.grid_rowconfigure(0, weight=1)
        body.grid_rowconfigure(1, weight=1)

        form_card = self._create_card(body, "Новая позиция", "Поля собраны в один тихий сценарий: сначала заявка, потом товар, затем количество.")
        form_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 8))

        form_inner = ctk.CTkFrame(form_card, fg_color="transparent")
        form_inner.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        form_inner.grid_columnconfigure(0, weight=0)
        form_inner.grid_columnconfigure(1, weight=1)

        row = 0
        ctk.CTkLabel(form_inner, text="Номер заявки", font=self.fonts["normal"]).grid(row=row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.order_entry = ctk.CTkEntry(form_inner, placeholder_text="Например, 1542 / март", height=38)
        self.order_entry.grid(row=row, column=1, sticky="ew", pady=6)
        row += 1

        ctk.CTkLabel(form_inner, text="Как искать товар", font=self.fonts["normal"]).grid(row=row, column=0, sticky="w", pady=6, padx=(0, 10))
        mode_frame = ctk.CTkFrame(form_inner, fg_color="transparent")
        mode_frame.grid(row=row, column=1, sticky="w", pady=6)
        self.gtin_var = ctk.StringVar(value="No")
        ctk.CTkRadioButton(mode_frame, text="По GTIN", variable=self.gtin_var, value="Yes", command=self.gtin_toggle_mode, font=self.fonts["small"]).pack(side="left", padx=(0, 16))
        ctk.CTkRadioButton(mode_frame, text="По параметрам", variable=self.gtin_var, value="No", command=self.gtin_toggle_mode, font=self.fonts["small"]).pack(side="left")
        row += 1

        self.gtin_frame = ctk.CTkFrame(form_inner, fg_color="transparent")
        self.gtin_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=2)
        self.gtin_frame.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(self.gtin_frame, text="GTIN", font=self.fonts["normal"]).grid(row=0, column=0, sticky="w", pady=6, padx=(0, 10))
        self.gtin_entry = ctk.CTkEntry(self.gtin_frame, placeholder_text="Вставьте GTIN и нажмите Enter", height=38)
        self.gtin_entry.grid(row=0, column=1, sticky="ew", pady=6)
        self.gtin_entry.bind("<Return>", lambda event: self.search_by_gtin())
        self.gtin_frame.grid_remove()
        row += 1

        self.select_frame = ctk.CTkFrame(form_inner, fg_color="transparent")
        self.select_frame.grid(row=row, column=0, columnspan=2, sticky="ew", pady=2)
        self.select_frame.grid_columnconfigure(1, weight=1)

        select_row = 0
        ctk.CTkLabel(self.select_frame, text="Вид товара", font=self.fonts["normal"]).grid(row=select_row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.simpl_combo = ctk.CTkComboBox(self.select_frame, values=simplified_options, command=self.update_options, height=38)
        self.simpl_combo.grid(row=select_row, column=1, sticky="ew", pady=6)
        select_row += 1

        self.color_label = ctk.CTkLabel(self.select_frame, text="Цвет", font=self.fonts["normal"])
        self.color_label.grid(row=select_row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.color_combo = ctk.CTkComboBox(self.select_frame, values=color_options, height=38)
        self.color_combo.grid(row=select_row, column=1, sticky="ew", pady=6)
        select_row += 1

        self.venchik_label = ctk.CTkLabel(self.select_frame, text="Венчик", font=self.fonts["normal"])
        self.venchik_label.grid(row=select_row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.venchik_combo = ctk.CTkComboBox(self.select_frame, values=venchik_options, height=38)
        self.venchik_combo.grid(row=select_row, column=1, sticky="ew", pady=6)
        select_row += 1

        ctk.CTkLabel(self.select_frame, text="Размер", font=self.fonts["normal"]).grid(row=select_row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.size_combo = ctk.CTkComboBox(self.select_frame, values=size_options, height=38)
        self.size_combo.grid(row=select_row, column=1, sticky="ew", pady=6)
        select_row += 1

        ctk.CTkLabel(self.select_frame, text="Единиц в упаковке", font=self.fonts["normal"]).grid(row=select_row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.units_combo = ctk.CTkComboBox(self.select_frame, values=[str(value) for value in units_options], height=38)
        self.units_combo.grid(row=select_row, column=1, sticky="ew", pady=6)
        row += 1

        ctk.CTkLabel(form_inner, text="Количество кодов", font=self.fonts["normal"]).grid(row=row, column=0, sticky="w", pady=6, padx=(0, 10))
        self.codes_entry = ctk.CTkEntry(form_inner, placeholder_text="Сколько кодов нужно", height=38)
        self.codes_entry.grid(row=row, column=1, sticky="ew", pady=6)
        row += 1

        add_btn = ctk.CTkButton(form_inner, text="Добавить позицию", command=self.add_item)
        self._style_alert_button(add_btn)
        add_btn.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(14, 0))

        queue_card = self._create_card(body, "Пакет на выполнение", "Справа всегда видно, что именно отправится в обработку.")
        queue_card.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 8))
        queue_card.grid_rowconfigure(1, weight=1)
        queue_card.grid_columnconfigure(0, weight=1)

        table_frame = ctk.CTkFrame(queue_card, fg_color="transparent")
        table_frame.pack(fill="both", expand=True, padx=16, pady=(0, 12))

        columns = ("idx", "full_name", "simpl_name", "size", "units_per_pack", "gtin", "codes_count", "order_name", "uid")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=12)
        for column, title in {
            "idx": "№",
            "full_name": "Товар",
            "simpl_name": "Вид",
            "size": "Размер",
            "units_per_pack": "Упаковка",
            "gtin": "GTIN",
            "codes_count": "Коды",
            "order_name": "Заявка",
            "uid": "UID",
        }.items():
            self.tree.heading(column, text=title)
            self.tree.column(column, width=100 if column != "full_name" else 220)

        tree_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=tree_scroll.set)
        self.tree.pack(side="left", fill="both", expand=True)
        tree_scroll.pack(side="right", fill="y")

        action_row = ctk.CTkFrame(queue_card, fg_color="transparent")
        action_row.pack(fill="x", padx=16, pady=(0, 16))
        action_row.grid_columnconfigure((0, 1, 2), weight=1)

        delete_btn = ctk.CTkButton(action_row, text="Удалить строку", command=self.delete_item)
        self._style_secondary_button(delete_btn)
        delete_btn.grid(row=0, column=0, sticky="ew", padx=(0, 6))

        self.execute_btn = ctk.CTkButton(action_row, text="Запустить пакет", command=self.execute_all)
        self._style_primary_button(self.execute_btn)
        self.execute_btn.grid(row=0, column=1, sticky="ew", padx=6)

        clear_btn = ctk.CTkButton(action_row, text="Очистить всё", command=self.clear_all)
        self._style_secondary_button(clear_btn)
        clear_btn.grid(row=0, column=2, sticky="ew", padx=(6, 0))

        activity_card = self._create_activity_panel(
            body,
            "create",
            "Понятная обратная связь",
            "Здесь не будет технических логов. Только понятные шаги: что добавилось, что ушло в работу и где нужна ваша реакция.",
        )
        activity_card.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))

        self.log_text = self._create_hidden_log_box(activity_card)
        self._configure_treeview_style()

    def _setup_download_frame(self):
        body = self._create_page_shell(
            "download",
            "КОНТРОЛЬ СКАЧИВАНИЯ",
            "Загрузка без ручного мониторинга",
            "Очередь показывает только то, что важно оператору: заказ, состояние и где лежат готовые файлы.",
        )
        body.grid_columnconfigure(0, weight=7)
        body.grid_columnconfigure(1, weight=5)
        body.grid_rowconfigure(0, weight=1)

        table_card = self._create_card(body, "Очередь файлов", "Когда заказ готов, система сама подхватывает его и сохраняет файлы.")
        table_card.grid(row=0, column=0, sticky="nsew", padx=(0, 8))

        table_frame = ctk.CTkFrame(table_card, fg_color="transparent")
        table_frame.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        columns = ("order_name", "status", "filename", "document_id")
        self.download_tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=14)
        for column, title in {
            "order_name": "Заявка",
            "status": "Состояние",
            "filename": "Сохранено",
            "document_id": "ID заказа",
        }.items():
            self.download_tree.heading(column, text=title)
            self.download_tree.column(column, width=160 if column != "filename" else 260)

        scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.download_tree.yview)
        self.download_tree.configure(yscrollcommand=scroll.set)
        self.download_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        right_stack = ctk.CTkFrame(body, fg_color="transparent")
        right_stack.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        right_stack.grid_rowconfigure(1, weight=1)
        right_stack.grid_columnconfigure(0, weight=1)

        hint_card = self._create_card(right_stack, "Что здесь происходит", "Ожидает: заказ создан. Генерируется: сервис готовит файлы. Скачан: можно идти дальше во ввод в оборот или на ТСД.")
        hint_card.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        activity_card = self._create_activity_panel(
            right_stack,
            "download",
            "Лента событий",
            "Каждый заказ сообщает только важное: готов ли он, началась ли загрузка, удалось ли сохранить файлы.",
        )
        activity_card.grid(row=1, column=0, sticky="nsew", pady=(8, 0))

        self.download_log_text = self._create_hidden_log_box(activity_card)

    def _setup_introduction_frame(self):
        body = self._create_page_shell(
            "intro",
            "ОБЫЧНЫЙ ВВОД",
            "Ввод в оборот в один спокойный шаг",
            "Берите готовые файлы, задавайте дату и партию, а результаты смотрите в понятной ленте действий.",
        )
        body.grid_columnconfigure(0, weight=5)
        body.grid_columnconfigure(1, weight=7)
        body.grid_rowconfigure(0, weight=1)

        left_stack = ctk.CTkFrame(body, fg_color="transparent")
        left_stack.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        left_stack.grid_rowconfigure(1, weight=1)
        left_stack.grid_columnconfigure(0, weight=1)

        form_card = self._create_card(left_stack, "Параметры ввода", "Даты уже предложены автоматически. Осталось выбрать партию и запустить ввод.")
        form_card.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        form_frame = ctk.CTkFrame(form_card, fg_color="transparent")
        form_frame.pack(fill="x", padx=20, pady=(0, 20))
        form_frame.grid_columnconfigure(1, weight=1)

        self.prod_date_intro_entry = None
        self.exp_date_intro_entry = None
        self.batch_intro_entry = None

        for row, (label_text, attr_name) in enumerate([
            ("Дата производства", "prod_date_intro_entry"),
            ("Дата окончания", "exp_date_intro_entry"),
            ("Номер партии", "batch_intro_entry"),
        ]):
            ctk.CTkLabel(form_frame, text=label_text, font=self.fonts["normal"]).grid(row=row, column=0, sticky="w", pady=6, padx=(0, 10))
            entry = ctk.CTkEntry(form_frame, height=38)
            entry.grid(row=row, column=1, sticky="ew", pady=6)
            setattr(self, attr_name, entry)

        self._set_default_date_range(self.prod_date_intro_entry, self.exp_date_intro_entry)

        btn_row = ctk.CTkFrame(form_card, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=(0, 20))
        btn_row.grid_columnconfigure((0, 1, 2), weight=1)

        self.intro_btn = ctk.CTkButton(btn_row, text="Запустить ввод", command=self.on_introduce_clicked)
        self._style_primary_button(self.intro_btn)
        self.intro_btn.grid(row=0, column=0, sticky="ew", padx=(0, 6))

        self.intro_refresh_btn = ctk.CTkButton(btn_row, text="Обновить список", command=self.update_introduction_tree)
        self._style_secondary_button(self.intro_refresh_btn)
        self.intro_refresh_btn.grid(row=0, column=1, sticky="ew", padx=6)

        self.intro_clear_btn = ctk.CTkButton(btn_row, text="Очистить события", command=self.clear_intro_log)
        self._style_secondary_button(self.intro_clear_btn)
        self.intro_clear_btn.grid(row=0, column=2, sticky="ew", padx=(6, 0))

        activity_card = self._create_activity_panel(
            left_stack,
            "intro",
            "Ход выполнения",
            "Система будет писать коротко и по делу: какой заказ обработан, какой требует внимания и когда пакет завершён.",
        )
        activity_card.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        self.intro_log_text = self._create_hidden_log_box(activity_card)

        right_card = self._create_card(body, "Готово к вводу", "Здесь видны только те заказы, которые уже можно брать в обычный ввод в оборот.")
        right_card.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        table_frame = ctk.CTkFrame(right_card, fg_color="transparent")
        table_frame.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        intro_columns = ("order_name", "document_id", "status", "filename")
        self.intro_tree = ttk.Treeview(table_frame, columns=intro_columns, show="headings", height=15, selectmode="extended")
        for column, title in {
            "order_name": "Заявка",
            "document_id": "ID заказа",
            "status": "Состояние",
            "filename": "Файл",
        }.items():
            self.intro_tree.heading(column, text=title)
            self.intro_tree.column(column, width=180 if column != "filename" else 240)

        scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.intro_tree.yview)
        self.intro_tree.configure(yscrollcommand=scroll.set)
        self.intro_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        self.update_introduction_tree()

    def _setup_introduction_tsd_frame(self):
        body = self._create_page_shell(
            "intro_tsd",
            "ТЕРМИНАЛ СБОРА ДАННЫХ",
            "ТСД-сценарий с понятной обратной связью",
            "Здесь только то, что важно оператору: номер ввода, даты, партия, список доступных заказов и история действий.",
        )
        body.grid_columnconfigure(0, weight=6)
        body.grid_columnconfigure(1, weight=5)
        body.grid_rowconfigure(0, weight=1)

        left_stack = ctk.CTkFrame(body, fg_color="transparent")
        left_stack.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        left_stack.grid_rowconfigure(1, weight=1)
        left_stack.grid_columnconfigure(0, weight=1)

        form_card = self._create_card(left_stack, "Параметры ТСД", "Все поля сверху, список заказов ниже. Можно быстро проверить и отправить пакет на терминал.")
        form_card.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        form_frame = ctk.CTkFrame(form_card, fg_color="transparent")
        form_frame.pack(fill="x", padx=20, pady=(0, 10))
        form_frame.grid_columnconfigure(1, weight=1)

        for row, (label_text, attr_name) in enumerate([
            ("Номер ввода в оборот", "tsd_intro_number_entry"),
            ("Дата производства", "tsd_prod_date_entry"),
            ("Дата окончания", "tsd_exp_date_entry"),
            ("Номер партии", "tsd_batch_entry"),
        ]):
            ctk.CTkLabel(form_frame, text=label_text, font=self.fonts["normal"]).grid(row=row, column=0, sticky="w", pady=6, padx=(0, 10))
            entry = ctk.CTkEntry(form_frame, height=38)
            entry.grid(row=row, column=1, sticky="ew", pady=6)
            setattr(self, attr_name, entry)

        self._set_default_date_range(self.tsd_prod_date_entry, self.tsd_exp_date_entry)

        btn_row = ctk.CTkFrame(form_card, fg_color="transparent")
        btn_row.pack(fill="x", padx=20, pady=(0, 20))
        btn_row.grid_columnconfigure((0, 1, 2, 3), weight=1)

        self.tsd_btn = ctk.CTkButton(btn_row, text="Отправить на ТСД", command=self.on_tsd_clicked)
        self._style_alert_button(self.tsd_btn)
        self.tsd_btn.grid(row=0, column=0, sticky="ew", padx=(0, 6))

        self.tsd_refresh_btn = ctk.CTkButton(btn_row, text="Обновить", command=self.update_tsd_tree)
        self._style_secondary_button(self.tsd_refresh_btn)
        self.tsd_refresh_btn.grid(row=0, column=1, sticky="ew", padx=6)

        self.history_btn = ctk.CTkButton(btn_row, text="История заказов", command=self.show_order_history)
        self._style_primary_button(self.history_btn)
        self.history_btn.grid(row=0, column=2, sticky="ew", padx=6)

        self.tsd_clear_btn = ctk.CTkButton(btn_row, text="Очистить события", command=self.clear_tsd_log)
        self._style_secondary_button(self.tsd_clear_btn)
        self.tsd_clear_btn.grid(row=0, column=3, sticky="ew", padx=(6, 0))

        table_card = self._create_card(left_stack, "Доступные заказы", "Показываются заказы, которые можно отправить на ТСД прямо сейчас.")
        table_card.grid(row=1, column=0, sticky="nsew", pady=(8, 0))

        table_frame = ctk.CTkFrame(table_card, fg_color="transparent")
        table_frame.pack(fill="both", expand=True, padx=16, pady=(0, 16))

        tsd_columns = ("order_name", "document_id", "status", "filename")
        self.tsd_tree = ttk.Treeview(table_frame, columns=tsd_columns, show="headings", height=15, selectmode="extended")
        for column, title in {
            "order_name": "Заявка",
            "document_id": "ID заказа",
            "status": "Состояние",
            "filename": "Файл",
        }.items():
            self.tsd_tree.heading(column, text=title)
            self.tsd_tree.column(column, width=180 if column != "filename" else 240)

        scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tsd_tree.yview)
        self.tsd_tree.configure(yscrollcommand=scroll.set)
        self.tsd_tree.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")

        activity_card = self._create_activity_panel(
            body,
            "tsd",
            "Что происходит по ТСД",
            "Вместо техничных логов показываются спокойные статусы: что найдено, что отправлено, где нужна проверка.",
        )
        activity_card.grid(row=0, column=1, sticky="nsew", padx=(8, 0))
        self.tsd_log_text = self._create_hidden_log_box(activity_card)

        self.update_tsd_tree()

    def _setup_aggregation_frame(self):
        body = self._create_page_shell(
            "aggregation",
            "АГРЕГАЦИЯ",
            "Быстрая выгрузка кодов агрегации",
            "Ниже можно выбрать режим поиска, увидеть прогресс и получить понятный результат без лишней технической переписки.",
        )
        body.grid_columnconfigure(0, weight=5)
        body.grid_columnconfigure(1, weight=5)
        body.grid_rowconfigure(1, weight=1)

        controls = self._create_card(body, "Параметры выгрузки", "Выберите удобный способ: по количеству кодов или по названию товара.")
        controls.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 8))

        inner = ctk.CTkFrame(controls, fg_color="transparent")
        inner.pack(fill="x", padx=20, pady=(0, 20))

        self.agg_mode_var = ctk.StringVar(value="count")
        switch_row = ctk.CTkFrame(inner, fg_color="transparent")
        switch_row.pack(fill="x", pady=(0, 10))
        ctk.CTkRadioButton(switch_row, text="По количеству", variable=self.agg_mode_var, value="count", command=self.toggle_aggregation_mode, font=self.fonts["small"]).pack(side="left", padx=(0, 20))
        ctk.CTkRadioButton(switch_row, text="По названию", variable=self.agg_mode_var, value="comment", command=self.toggle_aggregation_mode, font=self.fonts["small"]).pack(side="left")

        self.count_frame = ctk.CTkFrame(inner, fg_color="transparent")
        self.count_frame.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(self.count_frame, text="Количество кодов", font=self.fonts["normal"]).pack(anchor="w", pady=(0, 6))
        self.count_entry = ctk.CTkEntry(self.count_frame, placeholder_text="Введите количество", height=38)
        self.count_entry.pack(fill="x")

        self.comment_frame = ctk.CTkFrame(inner, fg_color="transparent")
        ctk.CTkLabel(self.comment_frame, text="Наименование товара", font=self.fonts["normal"]).pack(anchor="w", pady=(0, 6))
        self.comment_entry = ctk.CTkEntry(self.comment_frame, placeholder_text="Введите наименование товара", height=38)
        self.comment_entry.pack(fill="x")

        self.download_agg_btn = ctk.CTkButton(inner, text="Загрузить коды агрегации", command=self.start_aggregation_download)
        self._style_primary_button(self.download_agg_btn)
        self.download_agg_btn.pack(fill="x", pady=(6, 0))

        progress_card = self._create_card(body, "Прогресс", "Панель справа показывает ход работы и итог сохранения.")
        progress_card.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 8))

        progress_inner = ctk.CTkFrame(progress_card, fg_color="transparent")
        progress_inner.pack(fill="x", padx=20, pady=(0, 20))
        ctk.CTkLabel(progress_inner, text="Готовность выгрузки", font=self.fonts["small"], text_color=self._get_color("text_secondary")).pack(anchor="w")
        self.agg_progress = ctk.CTkProgressBar(progress_inner, height=10, corner_radius=10, progress_color=self._get_color("accent"))
        self.agg_progress.pack(fill="x", pady=(8, 0))
        self.agg_progress.set(0)

        activity_card = self._create_activity_panel(
            body,
            "aggregation",
            "Лента выгрузки",
            "Все важные этапы: старт, загрузка, сохранение и итоговое количество найденных кодов.",
        )
        activity_card.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        self.agg_log_text = self._create_hidden_log_box(activity_card)

        self.toggle_aggregation_mode()

    def _configure_treeview_style(self):
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "Treeview",
            background="#FFF9F1",
            foreground=self._get_color("text_primary"),
            fieldbackground="#FFF9F1",
            borderwidth=0,
            rowheight=32,
            font=("Segoe UI", 11),
        )
        style.configure(
            "Treeview.Heading",
            background=self._get_color("surface_alt"),
            foreground=self._get_color("text_primary"),
            relief="flat",
            font=("Bahnschrift SemiBold", 11),
        )
        style.map(
            "Treeview",
            background=[("selected", "#D9E7E1")],
            foreground=[("selected", self._get_color("text_primary"))],
        )

    def show_content_frame(self, frame_name):
        self.current_page = frame_name
        for name, frame in self.content_frames.items():
            if name == frame_name:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()

        title, subtitle = self.page_titles.get(frame_name, ("KonturTEST", ""))
        if self.page_title_label:
            self.page_title_label.configure(text=title)
        if self.page_subtitle_label:
            self.page_subtitle_label.configure(text=subtitle)
        self._update_navigation_style(frame_name)

    def _update_navigation_style(self, active_frame):
        for nav_id, payload in self.nav_buttons.items():
            button = payload["button"]
            if nav_id == active_frame:
                button.configure(fg_color=self._get_color("accent"), hover_color=self._get_color("accent"))
            else:
                button.configure(fg_color="transparent", hover_color="#214747")

    def _write_hidden_log(self, widget, message: str):
        try:
            widget.configure(state="normal")
            widget.insert("end", f"{message}\n")
            widget.see("end")
            widget.configure(state="disabled")
        except Exception as exc:
            logger.error(f"Ошибка записи в скрытый лог: {exc}")

    def _detect_tone(self, message: str) -> str:
        lowered = message.lower()
        if "ошиб" in lowered or "не найден" in lowered or lowered.startswith("❌"):
            return "error"
        if "вниман" in lowered or "пропущ" in lowered or lowered.startswith("⚠"):
            return "warning"
        if "успеш" in lowered or "готов" in lowered or lowered.startswith("✅") or lowered.startswith("🎉"):
            return "success"
        return "info"

    def _friendly_message(self, area: str, message: str) -> str:
        text = " ".join(message.replace("\n", " ").split())
        replacements = {
            "❌ ОШИБКА валидации:": "",
            "❌ КРИТИЧЕСКАЯ ОШИБКА:": "",
            "❌": "",
            "✅": "",
            "⚠️": "",
            "🎉": "",
            "МОНИТОРИНГ:": "",
            "_tsd_worker:": "",
            "download_codes вернул None (заказ не готов или ошибка подготовки)": "Заказ ещё не отдал файлы. Попробую снова, когда сервис подготовит выгрузку.",
            "Нет скачанных файлов (все пути None)": "Сервис не вернул ни одного файла. Проверьте состояние заказа.",
            "Выполнение отменено пользователем.": "Действие отменено.",
        }
        for old, new in replacements.items():
            text = text.replace(old, new)

        if area == "download":
            text = text.replace("Проверка статусов", "Проверяю готовность")
            text = text.replace("готов к скачиванию", "готов к загрузке")
            text = text.replace("Успешно скачан:", "Файлы сохранены:")
        elif area == "tsd":
            text = text.replace("ЗАДАНИЕ УСПЕШНО СОЗДАНО!", "Задание на ТСД успешно создано.")
            text = text.replace("Не найден GTIN", "Не удалось определить GTIN")
        elif area == "intro":
            text = text.replace("Все задачи завершены", "Пакет завершён")
        elif area == "aggregation":
            text = text.replace("Начинаем загрузку...", "Начинаю выгрузку кодов.")

        return text.strip() or message

    def _append_activity(self, area: str, message: str, tone: str | None = None):
        feed = self.activity_feeds.get(area)
        if not feed:
            return

        empty_label = self.activity_empty_labels.get(area)
        if empty_label:
            empty_label.pack_forget()

        tone = tone or self._detect_tone(message)
        palette = {
            "success": ("#E1F0E5", self._get_color("success")),
            "warning": ("#F8E8D5", self._get_color("warning")),
            "error": ("#F7E0DC", self._get_color("error")),
            "info": ("#E8EEEA", self._get_color("primary")),
        }
        bg_color, accent = palette.get(tone, palette["info"])

        card = ctk.CTkFrame(feed, fg_color=bg_color, corner_radius=14, border_color=accent, border_width=1)
        card.pack(fill="x", padx=6, pady=5)

        top_row = ctk.CTkFrame(card, fg_color="transparent")
        top_row.pack(fill="x", padx=12, pady=(10, 4))
        ctk.CTkLabel(
            top_row,
            text={"success": "Готово", "warning": "Проверьте", "error": "Нужно внимание", "info": "Шаг"}[tone],
            font=self.fonts["small"],
            text_color=accent,
        ).pack(side="left")
        ctk.CTkLabel(
            top_row,
            text=datetime.now().strftime("%H:%M"),
            font=self.fonts["small"],
            text_color=self._get_color("text_secondary"),
        ).pack(side="right")

        ctk.CTkLabel(
            card,
            text=self._friendly_message(area, message),
            font=self.fonts["normal"],
            justify="left",
            wraplength=520,
            text_color=self._get_color("text_primary"),
        ).pack(anchor="w", padx=12, pady=(0, 12))

        self.activity_items[area].append(card)
        while len(self.activity_items[area]) > 60:
            oldest = self.activity_items[area].pop(0)
            oldest.destroy()

    def _clear_activity(self, area: str):
        for item in self.activity_items.get(area, []):
            item.destroy()
        self.activity_items[area] = []
        empty_label = self.activity_empty_labels.get(area)
        if empty_label:
            empty_label.pack(anchor="w", padx=6, pady=8)

    def _announce(self, message: str, tone: str = "info"):
        if self.status_bar:
            colors = {
                "success": self._get_color("success"),
                "warning": self._get_color("warning"),
                "error": self._get_color("error"),
                "info": self._get_color("primary"),
            }
            self.status_bar.configure(text=message, text_color=colors.get(tone, self._get_color("primary")))
        if self.status_banner:
            self.status_banner.configure(text=message)

    def _refresh_live_summary(self):
        try:
            ready_for_tsd = sum(
                1
                for item in self.download_list
                if item.get("document_id") not in self.sent_to_tsd_items
                and (item.get("status") in ("Скачан", "Downloaded", "Ожидает", "Скачивается", "Готов для ТСД") or item.get("filename"))
            )
            self.metric_labels["bundle"].configure(text=str(len(self.collected)))
            self.metric_labels["queue"].configure(text=str(len(self.download_list)))
            self.metric_labels["tsd_ready"].configure(text=str(ready_for_tsd))
            self.metric_labels["tsd_sent"].configure(text=str(len(self.sent_to_tsd_items)))

            session_info = SessionManager.get_session_info()
            if session_info.get("has_session"):
                minutes = round(session_info.get("minutes_until_update", 0))
                self.connection_indicator.configure(text=f"Сессия активна · обновление ~ через {minutes} мин")
            else:
                self.connection_indicator.configure(text="Сессия ещё не инициализирована")
        except Exception:
            pass

    def _start_summary_loop(self):
        if self.summary_loop_started:
            return

        self.summary_loop_started = True

        def _tick():
            self._refresh_live_summary()
            self.after(4000, _tick)

        self.after(4000, _tick)

    def update_tree(self):
        super().update_tree()
        self._refresh_live_summary()

    def update_download_tree(self):
        super().update_download_tree()
        self._refresh_live_summary()

    def update_introduction_tree(self):
        super().update_introduction_tree()
        self._refresh_live_summary()

    def update_tsd_tree(self):
        if not hasattr(self, "tsd_tree"):
            return

        sent_items = getattr(self, "sent_to_tsd_items", set())
        for item_id in self.tsd_tree.get_children():
            self.tsd_tree.delete(item_id)

        for item in self.download_list:
            document_id = item.get("document_id")
            if document_id not in sent_items and (
                item.get("status") in ("Скачан", "Downloaded", "Ожидает", "Скачивается", "Готов для ТСД")
                or item.get("filename")
            ):
                self.tsd_tree.insert(
                    "",
                    "end",
                    iid=document_id,
                    values=(
                        item.get("order_name"),
                        document_id,
                        item.get("status"),
                        item.get("filename") or "",
                    ),
                )
        self._refresh_live_summary()

    def log_insert(self, msg: str):
        self._write_hidden_log(self.log_text, msg)
        self._append_activity("create", msg)
        self._announce(self._friendly_message("create", msg), self._detect_tone(msg))

    def download_log_insert(self, msg: str):
        self._write_hidden_log(self.download_log_text, f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        self._append_activity("download", msg)
        self._announce(self._friendly_message("download", msg), self._detect_tone(msg))

    def intro_log_insert(self, text: str):
        timestamped = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {text}"
        self._write_hidden_log(self.intro_log_text, timestamped)
        self._append_activity("intro", text)
        self._announce(self._friendly_message("intro", text), self._detect_tone(text))

    def tsd_log_insert(self, text: str):
        timestamped = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {text}"
        self._write_hidden_log(self.tsd_log_text, timestamped)
        self._append_activity("tsd", text)
        self._announce(self._friendly_message("tsd", text), self._detect_tone(text))

    def log_aggregation_message(self, message):
        self._write_hidden_log(self.agg_log_text, message)
        self._append_activity("aggregation", message)
        self._announce(self._friendly_message("aggregation", message), self._detect_tone(message))

    def _show_error(self, message):
        self.intro_log_insert(message)

    def clear_intro_log(self):
        self.intro_log_text.configure(state="normal")
        self.intro_log_text.delete("1.0", "end")
        self.intro_log_text.configure(state="disabled")
        self._clear_activity("intro")
        self._announce("Лента обычного ввода очищена.", "info")

    def clear_tsd_log(self):
        self.tsd_log_text.configure(state="normal")
        self.tsd_log_text.delete("1.0", "end")
        self.tsd_log_text.configure(state="disabled")
        self._clear_activity("tsd")
        self._announce("Лента ТСД очищена.", "info")

    def show_info(self, message):
        self._announce(message, "success")

    def _on_all_execute_finished(self, success_count, fail_count, results):
        super()._on_all_execute_finished(success_count, fail_count, results)
        tone = "warning" if fail_count else "success"
        self._announce(f"Пакет заказов завершён: успешно {success_count}, с ошибками {fail_count}.", tone)

    def _on_intro_finished(self, item: dict, ok: bool, msg: str):
        super()._on_intro_finished(item, ok, msg)
        self._announce(
            f"Обычный ввод: {'готово' if ok else 'нужно проверить'} для {item.get('order_name', 'заказа')}.",
            "success" if ok else "warning",
        )

    def _on_tsd_finished(self, item: dict, ok: bool, msg: str):
        super()._on_tsd_finished(item, ok, msg)
        self._announce(
            f"ТСД: {'отправка завершена' if ok else 'возникла ошибка'} для {item.get('order_name', 'заказа')}.",
            "success" if ok else "error",
        )

    def _finish_download(self, item, filename):
        super()._finish_download(item, filename)
        self._announce(f"Файлы заказа «{item.get('order_name', 'без названия')}» сохранены.", "success")

    def _handle_clipboard_shortcuts(self, event):
        key = ((event.keysym or "") or (event.char or "")).lower()
        focused = self.focus_get()
        if focused is None:
            return None

        if key in {"v", "м"}:
            focused.event_generate("<<Paste>>")
            return "break"
        if key in {"c", "с"}:
            focused.event_generate("<<Copy>>")
            return "break"
        if key in {"x", "ч"}:
            focused.event_generate("<<Cut>>")
            return "break"
        if key in {"a", "ф"}:
            focused.event_generate("<<SelectAll>>")
            return "break"
        return None

    def _bind_global_clipboard_shortcuts(self):
        self.bind_all("<Control-Key>", self._handle_clipboard_shortcuts, add="+")
        self.bind_all("<Control-Insert>", lambda event: (self.focus_get().event_generate("<<Copy>>"), "break")[1] if self.focus_get() else "break", add="+")
        self.bind_all("<Shift-Insert>", lambda event: (self.focus_get().event_generate("<<Paste>>"), "break")[1] if self.focus_get() else "break", add="+")
        self.bind_all("<Shift-Delete>", lambda event: (self.focus_get().event_generate("<<Cut>>"), "break")[1] if self.focus_get() else "break", add="+")

    def _iter_widgets(self, widget):
        yield widget
        for child in widget.winfo_children():
            yield from self._iter_widgets(child)

    def _apply_entry_context_menus(self):
        for widget in self._iter_widgets(self):
            if isinstance(widget, ctk.CTkEntry):
                self._add_entry_context_menu(widget)


if __name__ == "__main__":
    ensure_kontur_test_shortcut()

    if not os.path.exists(NOMENCLATURE_XLSX):
        logger.error(f"файл {NOMENCLATURE_XLSX} не найден.")
    else:
        df = pd.read_excel(NOMENCLATURE_XLSX)
        df.columns = df.columns.str.strip()
        app = KonturTestApp(df)
        app.mainloop()

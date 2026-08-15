/**
 * QA-мок моста pywebview для скриншот-прогона (только dev: `?qa=light|dark`).
 * В прод-сборку не попадает: импортируется динамически за import.meta.env.DEV.
 */

const SIMPL = ['лат диаг S', 'лат диаг M', 'нитрил синий L', 'винил прозр M', 'HR оранж XL']
const COLORS = ['Синий', 'Чёрный', 'Прозрачный', 'Оранжевый']
const SIZES = ['XS', 'S', 'M', 'L', 'XL']
const UNITS = ['1', '2', '10', '25', '50']
const STATUSES = ['Получен', 'Ожидает обработки', 'Ошибка подписи', 'Черновик', 'Скачан', 'Введён в оборот']
const TSD_STATUSES = ['Наполнен на ТСД', 'Создано', 'Ожидает наполнения', '']
const AK_STATUSES = [
  { value: 'ready', label: 'Готов к отправке' },
  { value: 'registered', label: 'Зарегистрирован' },
  { value: 'pending', label: 'Обрабатывается' },
  { value: 'error', label: 'Ошибка проверки' },
]

function gtin(index: number) {
  return `0460${String(1000000 + index * 37).slice(0, 7)}${String(100 + (index % 900))}`
}

function documentId(prefix: string, index: number) {
  return `${prefix}-${String(90000 + index)}`
}

function time(index: number) {
  const h = String(8 + (index % 12)).padStart(2, '0')
  const m = String((index * 7) % 60).padStart(2, '0')
  return `${h}:${m}`
}

function orderRow(index: number) {
  const simpl = SIMPL[index % SIMPL.length]
  return {
    document_id: documentId('doc', index),
    order_name: `Заявка ${1200 - index}`,
    status: STATUSES[index % STATUSES.length],
    status_summary: index % 4 === 0 ? 'Готово 12 из 24' : '',
    gtin: gtin(index),
    full_name: `Перчатки ${simpl} латексные неопудренные, размер ${SIZES[index % SIZES.length]}`,
    simpl,
    codes_count: 50 + (index % 20) * 25,
    created_at: `2026-08-${String(1 + (index % 28)).padStart(2, '0')}T${time(index)}:00`,
  }
}

const HISTORY = Array.from({ length: 120 }, (_, index) => orderRow(index))
const QUEUE = Array.from({ length: 5 }, (_, index) => ({
  uid: `q-${index}`,
  order_name: `Очередь ${index + 1}`,
  simpl_name: SIMPL[index % SIMPL.length],
  gtin: gtin(index + 300),
  codes_count: 100 + index * 50,
}))
const DELETED = Array.from({ length: 8 }, (_, index) => ({
  ...orderRow(index + 500),
  deleted_at: `10.08.2026 ${time(index)}`,
  deleted_by: index % 2 ? 'Оператор 1' : 'Оператор 2',
}))

const DOWNLOAD_ITEMS = Array.from({ length: 60 }, (_, index) => ({
  ...orderRow(index + 40),
  file_label: index % 3 === 0 ? `codes_${index}.csv` : '',
  from_history: index % 5 === 0,
}))

const INTRO_ITEMS = Array.from({ length: 40 }, (_, index) => ({
  ...orderRow(index + 60),
  can_intro: index % 2 === 0,
}))

const TSD_ITEMS = Array.from({ length: 35 }, (_, index) => ({
  ...orderRow(index + 80),
  tsd_status: TSD_STATUSES[index % TSD_STATUSES.length],
  tsd_intro_number: index % 3 === 0 ? `ВВ-${4000 + index}` : '',
  can_tsd: index % 3 !== 2,
}))

const AK_ITEMS = Array.from({ length: 220 }, (_, index) => {
  const status = AK_STATUSES[index % AK_STATUSES.length]
  return {
    document_id: documentId('ak', index),
    aggregate_code: `046000000${String(10000 + index)}`,
    comment: `лат диаг ${SIZES[index % SIZES.length]} 260316 (${200 + index}к)`,
    status: status.value,
    status_label: status.label,
    created_at_label: `0${1 + (index % 9)}.08.2026 ${time(index)}`,
    includes_units_count: (index % 25) * 10,
    codes_check_errors_count: index % 11 === 0 ? index % 7 : 0,
  }
})

const TEMPLATES = Array.from({ length: 7 }, (_, index) => ({
  name: `Шаблон ${['Латекс', 'Нитрил', 'HR', 'Винил', 'Латекс МИНИ', 'Нитрил КОРОБ', 'HR КОРОБ'][index]}`,
  category: index % 2 ? 'Коробки' : 'Пакеты',
  relative_path: `labels/100x180/template_${index}.btw`,
  path: `C:/labels/template_${index}.btw`,
  data_source_kind: index % 2 ? 'aggregation' : 'marking',
  source_label: index % 2 ? 'Коды агрегации' : 'Коды маркировки',
  sheet_format: index < 5 ? '100x180' : '100x136',
  sheet_format_label: index < 5 ? '100×180 мм' : '100×136 мм',
}))

const FILES = Array.from({ length: 30 }, (_, index) => ({
  name: `codes_${2600 + index}.csv`,
  folder_name: index % 2 ? 'Заявка 1187' : 'Заявка 1190',
  path: `C:/codes/file_${index}.csv`,
  record_count: 50 + index * 12,
}))

const LOG_LINES: Record<string, string[]> = {
  orders: [
    '[09:12:04] Заказ «Заявка 1200» создан (250 кодов)',
    '[09:14:31] Очередь: 3 позиции отправлены в Контур',
    '[10:02:11] История синхронизирована (git)',
  ],
  chz: ['[09:00:02] WMS: callback принят (ввод в оборот)', '[09:41:55] WMS: заказ 91240 передан'],
  download: ['[09:20:44] Скачаны коды: Заявка 1198 (500 шт.)', '[11:38:27] Статусы обновлены: 4 заказа готовы'],
  intro: ['[10:15:09] Ввод в оборот: документ ВВ-4012 подписан'],
  tsd: ['[08:55:13] Задание на ТСД создано: Заявка 1195', '[12:03:48] «Наполнен на ТСД»: Заявка 1195'],
  aggregation: ['[13:21:36] Создано 5 АК (лат диаг S)', '[13:44:02] Проведение АК: 3 успешно, 1 ошибка'],
  labels: ['[14:05:19] Печать 100×180: 24 этикетки → Zebra ZT230'],
}

/** Мок отвечает с микрозадержкой — ближе к реальному мосту. */
function reply<T>(value: T): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(structuredClone(value)), 30))
}

const QA_THEMES = ['light', 'ivory', 'dark', 'graphite', 'ocean', 'system']

export function installQaMock(theme: string) {
  if (QA_THEMES.includes(theme)) {
    window.localStorage.setItem('kontur_theme', theme)
  }
  // Детерминированное состояние панелей между прогонами одного chrome-профиля
  const params = new URLSearchParams(window.location.search)
  window.localStorage.setItem('kontur_journal_open_v1', params.get('journal') === '1' ? 'true' : 'false')
  window.localStorage.setItem('kontur_desktop_sidebar_open_v1', 'true')

  const api: Record<string, (...args: unknown[]) => Promise<unknown>> = {
    get_session_info: () => reply({ has_session: true, minutes_until_update: 42 }),
    get_auth_state: () =>
      reply({ has_session: true, ready: true, state: 'ready', minutes_until_update: 42 }),
    refresh_session: () => reply({ success: true, session: { has_session: true, minutes_until_update: 60 } }),
    get_app_version: () => reply({ version: '0.1.0', commit: 'a1b2c3d4e5' }),
    set_window_chrome: () => reply({ ok: true }),
    check_for_updates: () => reply({ update_available: false }),
    apply_update: () => reply({ success: true }),
    get_default_date_window: () => reply({ production_date: '01-03-2026', expiration_date: '01-03-2031' }),

    get_orders_view_state: () => reply({ queue: QUEUE, history: HISTORY, deleted_orders: DELETED }),
    get_options: () =>
      reply({
        simplified_options: SIMPL,
        color_options: COLORS,
        size_options: SIZES,
        units_options: UNITS,
        color_required: [SIMPL[2]],
        venchik_options: ['С венчиком', 'Без венчика'],
        venchik_required: [SIMPL[0]],
      }),
    lookup_gtin: () => reply({ gtin: gtin(1), full_name: 'Перчатки латексные', tnved_code: '4015120000' }),
    lookup_gtin_by_code: () => reply({ gtin: gtin(2), simpl_name: SIMPL[0] }),
    add_order_item: () => reply({ queue: QUEUE }),
    create_order: () => reply({ success: true }),
    submit_order_queue: () => reply({ state: { queue: [], history: HISTORY, deleted_orders: DELETED } }),
    clear_order_queue: () => reply({ queue: [] }),
    remove_order_item: () => reply({ queue: QUEUE.slice(1) }),
    delete_order: () => reply({ success: true }),
    restore_deleted_order: () => reply({ success: true }),
    export_order_history: () => reply({ state: { queue: QUEUE, history: HISTORY, deleted_orders: DELETED } }),
    add_history_orders_to_active: () => reply({ success: true }),
    get_order_details: () =>
      reply({
        document_id: 'doc-90001',
        fields: [
          { label: 'Заявка', value: 'Заявка 1199' },
          { label: 'GTIN', value: gtin(1) },
          { label: 'Статус', value: 'Получен' },
          { label: 'Кодов', value: '250' },
        ],
      }),

    get_download_state: () =>
      reply({ items: DOWNLOAD_ITEMS, printers: ['Zebra ZT230', 'Godex G500'], default_printer: 'Zebra ZT230' }),
    sync_download_statuses: () => reply({ success: true }),
    manual_download_order: () => reply({ success: true }),
    print_download_order: () => reply({ selection: { total_record_count: 250, selected_record_number: 1 } }),

    get_intro_state: () => reply({ items: INTRO_ITEMS }),
    introduce_orders: () => reply({ success: true, results: [], errors: [] }),

    get_tsd_state: () => reply({ items: TSD_ITEMS, live: false }),
    create_tsd_tasks: () => reply({ results: [], errors: [] }),
    sign_tsd_introduction: () => reply({ state: { items: TSD_ITEMS } }),

    get_aggregation_state: () =>
      reply({ items: AK_ITEMS, status_options: [{ value: '', label: 'Все статусы' }, ...AK_STATUSES], total_items: AK_ITEMS.length, cache_age_seconds: 92 }),
    create_aggregation_codes: () => reply({ success: true }),
    download_selected_aggregations: () => reply({ success: true }),
    approve_selected_aggregations: () => reply({ success: true }),
    archive_selected_aggregations: () => reply({ success: true }),
    introduce_selected_aggregations: () => reply({ success: true }),
    refill_aggregations: () => reply({ success: true }),

    get_labels_state: () =>
      reply({
        sheet_formats: [
          { key: '100x180', label: '100×180 мм' },
          { key: '100x136', label: '100×136 мм' },
        ],
        default_sheet_format: '100x180',
        templates: TEMPLATES,
        aggregation_files: FILES.slice(0, 15),
        marking_files: FILES.slice(15),
        orders: HISTORY.slice(0, 30).map((row) => ({
          document_id: row.document_id,
          order_name: row.order_name,
          status: row.status,
          gtin: row.gtin,
          full_name: row.full_name,
          size: SIZES[0],
          batch: '260316',
        })),
        printers: ['Zebra ZT230', 'Godex G500'],
        default_printer: 'Zebra ZT230',
      }),
    preview_100x180_label: () =>
      reply({ preview: { order_name: 'Заявка 1199', sheet_format: '100x180', label_count: 24 } }),
    print_100x180_label: () => reply({ preview: { order_name: 'Заявка 1199', label_count: 24 } }),

    get_logs: (channel: unknown) => reply(LOG_LINES[String(channel)] ?? []),
    clear_logs: () => reply({ success: true }),
  }

  window.pywebview = { api }
}

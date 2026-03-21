const ROUTES = {
  orders: {
    title: 'Заказ кодов',
    subtitle: 'Создание, очередь и история заказов кодов маркировки.',
  },
  download: {
    title: 'Загрузка кодов',
    subtitle: 'Синхронизация статусов, ручная загрузка и печать термоэтикеток.',
  },
  intro: {
    title: 'Ввод в оборот',
    subtitle: 'Проведение скачанных заказов в оборот через API.',
  },
  tsd: {
    title: 'Задание на ТСД',
    subtitle: 'Создание заданий на ТСД по активным и историческим заказам.',
  },
  aggregation: {
    title: 'Коды агрегации',
    subtitle: 'Создание, скачивание, проведение и повторное наполнение АК.',
  },
  labels: {
    title: 'Печать этикеток',
    subtitle: 'Шаблоны BarTender 100x180, контекст печати и запуск печати.',
  },
};

const state = {
  theme: localStorage.getItem('kontur-ui-v2-theme') || 'dark',
  route: 'orders',
  session: {},
  options: {
    simplified_options: [],
    color_options: [],
    size_options: [],
    units_options: [],
    color_required: [],
    venchik_options: [],
    venchik_required: [],
  },
  orders: {
    mode: 'params',
    queue: [],
    sessionOrders: [],
    history: [],
    deletedOrders: [],
    selectedHistoryId: '',
    selectedDeletedId: '',
    showDeleted: false,
  },
  download: {
    items: [],
    printers: [],
    defaultPrinter: '',
    selectedItemId: '',
    autoDownload: true,
  },
  intro: {
    items: [],
    selectedIds: new Set(),
  },
  tsd: {
    items: [],
    selectedIds: new Set(),
  },
  aggregation: {
    downloadMode: 'comment',
    csvFiles: [],
  },
  labels: {
    templates: [],
    aggregationFiles: [],
    markingFiles: [],
    orders: [],
    printers: [],
    defaultPrinter: '',
    selectedTemplatePath: '',
    selectedOrderId: '',
    selectedAggregationPath: '',
    selectedMarkingPath: '',
    preview: null,
    templatePage: 0,
    templatePageSize: 3,
  },
};

let appInitialized = false;

const $ = (selector) => document.querySelector(selector);

const API = {
  async call(method, ...args) {
    const target = window.pywebview?.api?.[method];
    if (!target) {
      throw new Error(`PyWebView API method not found: ${method}`);
    }
    const result = await target(...args);
    if (result?.error) {
      throw new Error(result.error);
    }
    return result;
  },
};

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function setStatusText(text, online = true) {
  $('#status-text').textContent = text;
  const connection = $('#status-connection');
  connection.textContent = online ? 'Онлайн' : 'Оффлайн';
  connection.classList.toggle('is-online', online);
}

function showToast(message, type = 'success') {
  const host = $('#toast-host');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  host.appendChild(toast);
  setTimeout(() => toast.remove(), 3800);
}

function statusPill(value) {
  const text = String(value ?? '—');
  const lower = text.toLowerCase();
  let cls = 'warning';
  if (
    lower.includes('скачан')
    || lower.includes('зарегистр')
    || lower.includes('введен')
    || lower.includes('доступен')
    || lower.includes('отправлен')
    || lower.includes('готов')
    || lower.includes('в обороте')
    || lower.includes('на тсд')
  ) {
    cls = 'success';
  } else if (
    lower.includes('ошибка')
    || lower.includes('failed')
    || lower.includes('error')
    || lower.includes('не зарегистр')
  ) {
    cls = 'danger';
  }
  return `<span class="pill ${cls}">${escapeHtml(text)}</span>`;
}

function createTable(container, columns, rows, options = {}) {
  const {
    rowId = (row) => row.id || row.document_id || row.uid,
    selectedIds = new Set(),
    single = false,
    onRowClick,
    compact = false,
    maxHeight = '',
  } = options;
  if (!rows?.length) {
    container.innerHTML = '<div class="table-empty">Данных пока нет.</div>';
    return;
  }

  const selectedSet = selectedIds instanceof Set ? selectedIds : new Set(selectedIds ? [selectedIds] : []);
  const html = `
    <div class="table-wrapper ${compact ? 'is-compact' : ''}" ${maxHeight ? `style="max-height:${escapeHtml(maxHeight)}"` : ''}>
      <table class="table ${compact ? 'table-compact' : ''}">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join('')}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => {
            const id = String(rowId(row) ?? '');
            const selected = selectedSet.has(id);
            return `
              <tr data-row-id="${escapeHtml(id)}" class="${selected ? 'is-selected' : ''}">
                ${columns.map((column) => {
                  const cell = typeof column.render === 'function' ? column.render(row) : escapeHtml(row[column.key] ?? '');
                  return `<td>${cell}</td>`;
                }).join('')}
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    </div>
  `;
  container.innerHTML = html;

  if (!onRowClick) {
    return;
  }

  container.querySelectorAll('tbody tr[data-row-id]').forEach((rowEl) => {
    rowEl.addEventListener('click', () => {
      const id = rowEl.dataset.rowId;
      if (single) {
        onRowClick(id);
      } else {
        onRowClick(id, rowEl.classList.contains('is-selected'));
      }
    });
  });
}

function renderStatusCell(row) {
  const summary = row?.status_summary ? `<span class="cell-note">${escapeHtml(row.status_summary)}</span>` : '';
  return `${statusPill(row?.status)}${summary}`;
}

function formatPreview(preview) {
  if (!preview) {
    return 'Выберите шаблон, файл и заказ, затем нажмите «Показать контекст».';
  }
  return [
    `Заказ: ${preview.order_name}`,
    `Шаблон: ${preview.template_category} / ${preview.data_source_kind}`,
    `Размер: ${preview.size}`,
    `Партия: ${preview.batch}`,
    `Цвет: ${preview.color || '—'}`,
    `Дата изготовления: ${preview.manufacture_date}`,
    `Срок годности: ${preview.expiration_date}`,
    `Количество: ${preview.quantity_pairs} ${preview.quantity_pairs_word}`,
    `Упаковка: ${preview.package_text || 'не используется'}`,
    `Этикеток к печати: ${preview.label_count}`,
  ].join('\n');
}

function setTheme(theme) {
  state.theme = theme;
  document.body.dataset.theme = theme;
  localStorage.setItem('kontur-ui-v2-theme', theme);
  $('#theme-toggle-btn').textContent = theme === 'dark' ? 'Светлая тема' : 'Тёмная тема';
}

function getDefaultDate(offsetYears = 0) {
  const now = new Date();
  now.setFullYear(now.getFullYear() + offsetYears);
  const year = now.getFullYear();
  const month = `${now.getMonth() + 1}`.padStart(2, '0');
  const day = `${now.getDate()}`.padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function fillSelect(select, values, placeholder = 'Выберите значение') {
  const current = select.value;
  select.innerHTML = [`<option value="">${escapeHtml(placeholder)}</option>`]
    .concat(values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`))
    .join('');
  if (values.includes(current)) {
    select.value = current;
  }
}

function fillDataList(id, values) {
  const list = $(id);
  list.innerHTML = values.map((value) => `<option value="${escapeHtml(value)}"></option>`).join('');
}

function readOrderForm() {
  return {
    order_name: $('#order-name-input').value.trim(),
    name: $('#product-name-input').value.trim(),
    gtin: $('#gtin-input').value.trim(),
    size: $('#size-select').value,
    color: $('#color-select').value,
    venchik: $('#venchik-select').value,
    units_per_pack: $('#units-select').value,
    codes_count: Number($('#codes-count-input').value || 0),
    mode: state.orders.mode,
  };
}

function updateLookupResult(result) {
  $('#lookup-gtin-result').textContent = result?.gtin || '—';
  $('#lookup-full-name-result').textContent = result?.full_name || '—';
  $('#lookup-tnved-result').textContent = result?.tnved_code || '—';
}

function applySessionInfo(info) {
  state.session = info || {};
  const hasSession = Boolean(info?.has_session);
  const text = hasSession
    ? `Сессия активна, обновление через ${info.minutes_until_update} мин`
    : 'Сессия не инициализирована';
  $('#sidebar-session-state').textContent = text;
  setStatusText(text, hasSession);
}

const Router = {
  go(route) {
    state.route = route;
    document.querySelectorAll('.nav-item').forEach((item) => {
      item.classList.toggle('is-active', item.dataset.route === route);
    });
    document.querySelectorAll('.view').forEach((view) => {
      view.classList.toggle('is-active', view.id === `view-${route}`);
    });
    $('#view-title').textContent = ROUTES[route].title;
    $('#view-subtitle').textContent = ROUTES[route].subtitle;
    refreshCurrentRouteState().catch(() => null);
  },
};

async function refreshSessionInfo(showToastOnSuccess = false) {
  try {
    const info = await API.call('get_session_info');
    applySessionInfo(info);
    if (showToastOnSuccess) {
      showToast('Сведения о сессии обновлены.');
    }
  } catch (error) {
    setStatusText(error.message, false);
    showToast(error.message, 'error');
  }
}

async function refreshLogs() {
  const mappings = [
    ['orders', '#orders-log'],
    ['download', '#download-log'],
    ['intro', '#intro-log'],
    ['tsd', '#tsd-log'],
    ['aggregation', '#aggregation-log'],
    ['labels', '#labels-log'],
  ];
  for (const [channel, selector] of mappings) {
    try {
      const lines = await API.call('get_logs', channel);
      $(selector).textContent = Array.isArray(lines) ? lines.join('\n') : '';
    } catch {
      $(selector).textContent = '';
    }
  }
}

const Views = {
  orders: {
    render() {
      const deletedPanel = $('#orders-deleted-panel');
      if (deletedPanel) {
        deletedPanel.classList.toggle('is-hidden', !state.orders.showDeleted);
      }
      const toggleDeletedBtn = $('#orders-toggle-deleted-btn');
      if (toggleDeletedBtn) {
        toggleDeletedBtn.textContent = state.orders.showDeleted ? 'Скрыть удаленные' : 'Удаленные';
      }

      createTable(
        $('#orders-queue-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Товар', key: 'simpl_name' },
          { label: 'GTIN', key: 'gtin' },
          { label: 'Кодов', key: 'codes_count' },
        ],
        state.orders.queue,
        { compact: true, maxHeight: '180px' },
      );

      createTable(
        $('#orders-session-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'ID документа', key: 'document_id' },
          { label: 'Статус', render: (row) => renderStatusCell(row) },
          { label: 'GTIN', key: 'gtin' },
        ],
        state.orders.sessionOrders,
        { compact: true, maxHeight: '180px' },
      );

      createTable(
        $('#orders-history-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус', render: (row) => renderStatusCell(row) },
          { label: 'GTIN', key: 'gtin' },
        ],
        state.orders.history,
        {
          single: true,
          compact: true,
          maxHeight: '360px',
          selectedIds: state.orders.selectedHistoryId,
          onRowClick: (id) => {
            state.orders.selectedHistoryId = id;
            Views.orders.render();
          },
        },
      );

      if (state.orders.showDeleted) {
        createTable(
          $('#orders-deleted-table'),
          [
            { label: 'Заявка', key: 'order_name' },
            { label: 'Полное наименование', key: 'full_name' },
            { label: 'Удален', key: 'deleted_at' },
            { label: 'Статус', render: (row) => renderStatusCell(row) },
          ],
          state.orders.deletedOrders,
          {
            single: true,
            compact: true,
            maxHeight: '260px',
            selectedIds: state.orders.selectedDeletedId,
            onRowClick: (id) => {
              state.orders.selectedDeletedId = id;
              Views.orders.render();
            },
          },
        );
      }
    },
  },
  download: {
    render() {
      fillSelect($('#download-printer-select'), state.download.printers, 'Выберите принтер');
      if (state.download.defaultPrinter) {
        $('#download-printer-select').value = state.download.defaultPrinter;
      }

      createTable(
        $('#download-items-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус', render: (row) => renderStatusCell(row) },
          { label: 'GTIN', key: 'gtin' },
          { label: 'Файлы', key: 'file_label' },
        ],
        state.download.items,
        {
          single: true,
          compact: true,
          maxHeight: '520px',
          selectedIds: state.download.selectedItemId,
          onRowClick: (id) => {
            state.download.selectedItemId = id;
            Views.download.render();
          },
        },
      );
    },
  },
  intro: {
    render() {
      createTable(
        $('#intro-items-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус', render: (row) => renderStatusCell(row) },
          { label: 'GTIN', key: 'gtin' },
        ],
        state.intro.items,
        {
          compact: true,
          maxHeight: '420px',
          selectedIds: state.intro.selectedIds,
          onRowClick: (id, isSelected) => {
            if (isSelected) {
              state.intro.selectedIds.delete(id);
            } else {
              state.intro.selectedIds.add(id);
            }
            Views.intro.render();
          },
        },
      );
    },
  },
  tsd: {
    render() {
      createTable(
        $('#tsd-items-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус ЧЗ', render: (row) => renderStatusCell(row) },
          { label: 'На ТСД', render: (row) => statusPill(row.tsd_status) },
          { label: 'GTIN', key: 'gtin' },
        ],
        state.tsd.items,
        {
          compact: true,
          maxHeight: '520px',
          selectedIds: state.tsd.selectedIds,
          onRowClick: (id, isSelected) => {
            if (isSelected) {
              state.tsd.selectedIds.delete(id);
            } else {
              state.tsd.selectedIds.add(id);
            }
            Views.tsd.render();
          },
        },
      );
    },
  },
  aggregation: {
    render() {
      createTable(
        $('#agg-csv-table'),
        [
          { label: 'Файл', key: 'name' },
          { label: 'Папка', key: 'folder_name' },
          { label: 'Строк', key: 'record_count' },
          { label: 'Путь', key: 'path' },
        ],
        state.aggregation.csvFiles,
      );
    },
  },
  labels: {
    render() {
      fillSelect($('#labels-printer-select'), state.labels.printers, 'Выберите принтер');
      if (state.labels.defaultPrinter) {
        $('#labels-printer-select').value = state.labels.defaultPrinter;
      }

      const templateHost = $('#labels-template-grid');
      const pageSize = state.labels.templatePageSize || 3;
      const totalPages = Math.max(1, Math.ceil(state.labels.templates.length / pageSize));
      state.labels.templatePage = Math.min(state.labels.templatePage, totalPages - 1);
      const pageStart = state.labels.templatePage * pageSize;
      const visibleTemplates = state.labels.templates.slice(pageStart, pageStart + pageSize);
      $('#labels-template-page').textContent = `${state.labels.templates.length ? pageStart + 1 : 0}-${Math.min(pageStart + visibleTemplates.length, state.labels.templates.length)} из ${state.labels.templates.length}`;
      $('#labels-template-prev').disabled = state.labels.templatePage <= 0;
      $('#labels-template-next').disabled = state.labels.templatePage >= totalPages - 1;

      templateHost.innerHTML = visibleTemplates.map((template) => `
        <button class="template-card ${state.labels.selectedTemplatePath === template.path ? 'is-selected' : ''}" data-template-path="${escapeHtml(template.path)}">
          <strong>${escapeHtml(template.name)}</strong>
          <small>${escapeHtml(template.category)}</small>
          <small>${escapeHtml(template.relative_path)}</small>
          <small>${escapeHtml(template.source_label || template.data_source_kind)}</small>
        </button>
      `).join('');
      templateHost.querySelectorAll('[data-template-path]').forEach((button) => {
        button.addEventListener('click', () => {
          state.labels.selectedTemplatePath = button.dataset.templatePath;
          Views.labels.render();
        });
      });

      createTable(
        $('#labels-orders-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'GTIN', key: 'gtin' },
          { label: 'Размер', key: 'size' },
          { label: 'Партия', key: 'batch' },
        ],
        state.labels.orders,
        {
          single: true,
          compact: true,
          maxHeight: '260px',
          selectedIds: state.labels.selectedOrderId,
          onRowClick: (id) => {
            state.labels.selectedOrderId = id;
            Views.labels.render();
          },
        },
      );

      createTable(
        $('#labels-aggregation-files-table'),
        [
          { label: 'Файл', key: 'name' },
          { label: 'Папка', key: 'folder_name' },
          { label: 'Строк', key: 'record_count' },
        ],
        state.labels.aggregationFiles,
        {
          single: true,
          compact: true,
          maxHeight: '260px',
          selectedIds: state.labels.selectedAggregationPath,
          rowId: (row) => row.path,
          onRowClick: (id) => {
            state.labels.selectedAggregationPath = id;
            Views.labels.render();
          },
        },
      );

      createTable(
        $('#labels-marking-files-table'),
        [
          { label: 'Файл', key: 'name' },
          { label: 'Папка', key: 'folder_name' },
          { label: 'Строк', key: 'record_count' },
        ],
        state.labels.markingFiles,
        {
          single: true,
          compact: true,
          maxHeight: '260px',
          selectedIds: state.labels.selectedMarkingPath,
          rowId: (row) => row.path,
          onRowClick: (id) => {
            state.labels.selectedMarkingPath = id;
            Views.labels.render();
          },
        },
      );

      $('#labels-preview-box').textContent = formatPreview(state.labels.preview);
    },
  },
};

async function loadOrdersState() {
  const result = await API.call('get_orders_view_state');
  state.orders.queue = result.queue || [];
  state.orders.sessionOrders = result.session_orders || [];
  state.orders.history = result.history || [];
  state.orders.deletedOrders = result.deleted_orders || [];
  if (!state.orders.history.some((item) => item.document_id === state.orders.selectedHistoryId)) {
    state.orders.selectedHistoryId = '';
  }
  if (!state.orders.deletedOrders.some((item) => item.document_id === state.orders.selectedDeletedId)) {
    state.orders.selectedDeletedId = '';
  }
  Views.orders.render();
}

async function loadDownloadState() {
  const result = await API.call('get_download_state');
  state.download.items = result.items || [];
  state.download.printers = result.printers || [];
  state.download.defaultPrinter = result.default_printer || state.download.defaultPrinter;
  if (!state.download.items.some((item) => item.document_id === state.download.selectedItemId)) {
    state.download.selectedItemId = state.download.items[0]?.document_id || '';
  }
  Views.download.render();
}

async function loadIntroState() {
  const result = await API.call('get_intro_state');
  state.intro.items = result.items || [];
  state.intro.selectedIds = new Set(
    [...state.intro.selectedIds].filter((id) => state.intro.items.some((item) => item.document_id === id)),
  );
  Views.intro.render();
}

async function loadTsdState() {
  const result = await API.call('get_tsd_state');
  state.tsd.items = result.items || [];
  state.tsd.selectedIds = new Set(
    [...state.tsd.selectedIds].filter((id) => state.tsd.items.some((item) => item.document_id === id)),
  );
  Views.tsd.render();
}

async function loadAggregationState() {
  const result = await API.call('get_aggregation_state');
  state.aggregation.csvFiles = result.csv_files || [];
  Views.aggregation.render();
}

async function loadLabelsState() {
  const result = await API.call('get_labels_state');
  state.labels.templates = result.templates || [];
  state.labels.aggregationFiles = result.aggregation_files || [];
  state.labels.markingFiles = result.marking_files || [];
  state.labels.orders = result.orders || [];
  state.labels.printers = result.printers || [];
  state.labels.defaultPrinter = result.default_printer || state.labels.defaultPrinter;
  if (!state.labels.templates.some((item) => item.path === state.labels.selectedTemplatePath)) {
    state.labels.selectedTemplatePath = state.labels.templates[0]?.path || '';
  }
  if (state.labels.selectedTemplatePath) {
    const currentIndex = state.labels.templates.findIndex((item) => item.path === state.labels.selectedTemplatePath);
    state.labels.templatePage = currentIndex >= 0 ? Math.floor(currentIndex / state.labels.templatePageSize) : 0;
  } else {
    state.labels.templatePage = 0;
  }
  if (!state.labels.orders.some((item) => item.document_id === state.labels.selectedOrderId)) {
    state.labels.selectedOrderId = state.labels.orders[0]?.document_id || '';
  }
  if (!state.labels.aggregationFiles.some((item) => item.path === state.labels.selectedAggregationPath)) {
    state.labels.selectedAggregationPath = state.labels.aggregationFiles[0]?.path || '';
  }
  if (!state.labels.markingFiles.some((item) => item.path === state.labels.selectedMarkingPath)) {
    state.labels.selectedMarkingPath = state.labels.markingFiles[0]?.path || '';
  }
  Views.labels.render();
}

function selectedTemplate() {
  return state.labels.templates.find((item) => item.path === state.labels.selectedTemplatePath) || null;
}

function selectedLabelCsvPath() {
  const template = selectedTemplate();
  if (!template) {
    return '';
  }
  if (template.data_source_kind === 'aggregation') {
    return state.labels.selectedAggregationPath;
  }
  return state.labels.selectedMarkingPath;
}

async function refreshCurrentRouteState() {
  switch (state.route) {
    case 'orders':
      await loadOrdersState();
      break;
    case 'download':
      await loadDownloadState();
      break;
    case 'intro':
      await loadIntroState();
      break;
    case 'tsd':
      await loadTsdState();
      break;
    case 'aggregation':
      await loadAggregationState();
      break;
    case 'labels':
      await loadLabelsState();
      break;
    default:
      break;
  }
}

async function runAction(label, action, successMessage = '') {
  setStatusText(label, true);
  try {
    const result = await action();
    if (successMessage) {
      showToast(successMessage);
    }
    return result;
  } catch (error) {
    setStatusText(error.message, false);
    showToast(error.message, 'error');
    throw error;
  } finally {
    await refreshLogs();
    await refreshSessionInfo(false);
  }
}

function applyOptions() {
  fillDataList('#product-options', state.options.simplified_options || []);
  fillSelect($('#size-select'), state.options.size_options || [], 'Выберите размер');
  fillSelect($('#color-select'), state.options.color_options || [], 'Цвет не выбран');
  fillSelect($('#venchik-select'), state.options.venchik_options || [], 'Без венчика');
  fillSelect($('#units-select'), (state.options.units_options || []).map(String), 'Выберите количество');
}

function updateOrderModeUi() {
  const gtinInput = $('#gtin-input');
  const productInput = $('#product-name-input');
  const sizeSelect = $('#size-select');
  const colorSelect = $('#color-select');
  const venchikSelect = $('#venchik-select');
  const unitsSelect = $('#units-select');
  const paramsMode = state.orders.mode === 'params';

  $('#orders-mode-toggle').querySelectorAll('.segment').forEach((button) => {
    button.classList.toggle('is-active', button.dataset.mode === state.orders.mode);
  });

  gtinInput.disabled = paramsMode;
  productInput.disabled = !paramsMode;
  sizeSelect.disabled = !paramsMode;
  colorSelect.disabled = !paramsMode;
  venchikSelect.disabled = !paramsMode;
  unitsSelect.disabled = !paramsMode;
}

async function bindEvents() {
  $('#sidebar-nav').addEventListener('click', (event) => {
    const button = event.target.closest('.nav-item');
    if (!button) return;
    Router.go(button.dataset.route);
  });

  $('#theme-toggle-btn').addEventListener('click', () => {
    setTheme(state.theme === 'dark' ? 'light' : 'dark');
  });

  $('#refresh-session-btn').addEventListener('click', async () => {
    await runAction('Обновляем сессию...', async () => {
      const result = await API.call('refresh_session');
      applySessionInfo(result.session || result);
      return result;
    }, 'Сессия обновлена.');
  });

  $('#orders-mode-toggle').addEventListener('click', (event) => {
    const button = event.target.closest('.segment');
    if (!button) return;
    state.orders.mode = button.dataset.mode;
    updateOrderModeUi();
  });

  $('#lookup-gtin-btn').addEventListener('click', async () => {
    await runAction('Ищем GTIN...', async () => {
      const result = await API.call('lookup_gtin', $('#product-name-input').value, $('#size-select').value, $('#units-select').value, $('#color-select').value, $('#venchik-select').value);
      updateLookupResult(result);
      $('#gtin-input').value = result.gtin || '';
      return result;
    });
  });

  $('#lookup-by-code-btn').addEventListener('click', async () => {
    await runAction('Ищем товар по GTIN...', async () => {
      const result = await API.call('lookup_gtin_by_code', $('#gtin-input').value);
      updateLookupResult(result);
      if (result.simpl_name) $('#product-name-input').value = result.simpl_name;
      return result;
    });
  });

  $('#add-order-btn').addEventListener('click', async () => {
    await runAction('Добавляем в очередь...', async () => {
      const result = await API.call('add_order_item', readOrderForm());
      await loadOrdersState();
      return result;
    }, 'Позиция добавлена в очередь.');
  });

  $('#create-order-now-btn').addEventListener('click', async () => {
    await runAction('Создаём заказ...', async () => {
      const result = await API.call('create_order', readOrderForm());
      await Promise.all([loadOrdersState(), loadDownloadState(), loadIntroState(), loadTsdState(), loadLabelsState()]);
      return result;
    }, 'Заказ создан.');
  });

  $('#submit-queue-btn').addEventListener('click', async () => {
    await runAction('Выполняем очередь заказов...', async () => {
      const result = await API.call('submit_order_queue');
      await Promise.all([loadOrdersState(), loadDownloadState(), loadIntroState(), loadTsdState(), loadLabelsState()]);
      return result;
    }, 'Очередь заказов выполнена.');
  });

  $('#clear-queue-btn').addEventListener('click', async () => {
    await runAction('Очищаем очередь...', async () => {
      const result = await API.call('clear_order_queue');
      await loadOrdersState();
      return result;
    }, 'Очередь очищена.');
  });

  $('#orders-delete-btn').addEventListener('click', async () => {
    await runAction('Удаляем заказ...', async () => {
      const result = await API.call('delete_order', state.orders.selectedHistoryId);
      await Promise.all([loadOrdersState(), loadDownloadState(), loadIntroState(), loadTsdState(), loadLabelsState()]);
      return result;
    }, 'Заказ перемещен в Удаленные.');
  });

  $('#orders-toggle-deleted-btn').addEventListener('click', () => {
    state.orders.showDeleted = !state.orders.showDeleted;
    Views.orders.render();
  });

  $('#orders-restore-deleted-btn').addEventListener('click', async () => {
    await runAction('Восстанавливаем заказ...', async () => {
      const result = await API.call('restore_deleted_order', state.orders.selectedDeletedId);
      await Promise.all([loadOrdersState(), loadDownloadState(), loadIntroState(), loadTsdState(), loadLabelsState()]);
      return result;
    }, 'Заказ восстановлен.');
  });

  $('#download-refresh-btn').addEventListener('click', async () => {
    state.download.autoDownload = $('#download-auto-checkbox').checked;
    await runAction('Синхронизируем статусы загрузки...', async () => {
      const result = await API.call('sync_download_statuses', state.download.autoDownload);
      await Promise.all([loadDownloadState(), loadIntroState(), loadTsdState()]);
      return result;
    }, 'Статусы загрузки обновлены.');
  });

  $('#download-manual-btn').addEventListener('click', async () => {
    await runAction('Скачиваем выбранный заказ...', async () => {
      const result = await API.call('manual_download_order', state.download.selectedItemId);
      await Promise.all([loadDownloadState(), loadIntroState(), loadTsdState()]);
      return result;
    }, 'Заказ скачан.');
  });

  $('#download-print-btn').addEventListener('click', async () => {
    await runAction('Запускаем печать термоэтикеток...', async () => {
      const result = await API.call('print_download_order', state.download.selectedItemId, $('#download-printer-select').value);
      return result;
    }, 'Печать термоэтикеток запущена.');
  });

  $('#intro-run-btn').addEventListener('click', async () => {
    await runAction('Выполняем ввод в оборот...', async () => {
      const result = await API.call(
        'introduce_orders',
        Array.from(state.intro.selectedIds),
        $('#intro-production-date').value,
        $('#intro-expiration-date').value,
        $('#intro-batch-number').value,
      );
      await Promise.all([loadDownloadState(), loadIntroState(), loadTsdState()]);
      return result;
    }, 'Ввод в оборот завершён.');
  });

  $('#tsd-run-btn').addEventListener('click', async () => {
    await runAction('Создаём задания на ТСД...', async () => {
      const result = await API.call(
        'create_tsd_tasks',
        Array.from(state.tsd.selectedIds),
        $('#tsd-intro-number').value,
        $('#tsd-production-date').value,
        $('#tsd-expiration-date').value,
        $('#tsd-batch-number').value,
      );
      await Promise.all([loadDownloadState(), loadIntroState(), loadTsdState()]);
      return result;
    }, 'Задания на ТСД созданы.');
  });

  $('#agg-download-mode-toggle').addEventListener('click', (event) => {
    const button = event.target.closest('.segment');
    if (!button) return;
    state.aggregation.downloadMode = button.dataset.mode;
    $('#agg-download-mode-toggle').querySelectorAll('.segment').forEach((item) => {
      item.classList.toggle('is-active', item.dataset.mode === state.aggregation.downloadMode);
    });
    $('#agg-download-target').placeholder = state.aggregation.downloadMode === 'comment'
      ? 'Название для поиска'
      : 'Количество кодов';
  });

  $('#agg-create-btn').addEventListener('click', async () => {
    await runAction('Создаём агрегационные коды...', async () => {
      const result = await API.call('create_aggregation_codes', $('#agg-create-comment').value, Number($('#agg-create-count').value || 0));
      await loadAggregationState();
      return result;
    }, 'Агрегационные коды созданы.');
  });

  $('#agg-download-btn').addEventListener('click', async () => {
    await runAction('Скачиваем агрегационные коды...', async () => {
      const result = await API.call(
        'download_aggregation_codes',
        state.aggregation.downloadMode,
        $('#agg-download-target').value,
        $('#agg-download-status').value || 'tsdProcessStart',
      );
      await Promise.all([loadAggregationState(), loadLabelsState()]);
      return result;
    }, 'Агрегационные коды скачаны.');
  });

  $('#agg-approve-btn').addEventListener('click', async () => {
    await runAction('Проводим АК...', async () => {
      const result = await API.call(
        'approve_aggregations',
        $('#agg-approve-filter').value,
        $('#agg-allow-disaggregate').checked,
      );
      await loadAggregationState();
      return result;
    }, 'Проведение АК завершено.');
  });

  $('#agg-refill-btn').addEventListener('click', async () => {
    await runAction('Запускаем повторное наполнение АК...', async () => {
      const result = await API.call(
        'refill_aggregations',
        $('#agg-approve-filter').value,
        $('#agg-refill-token').value,
      );
      await loadAggregationState();
      return result;
    }, 'Повторное наполнение АК завершено.');
  });

  $('#labels-template-prev').addEventListener('click', () => {
    state.labels.templatePage = Math.max(0, state.labels.templatePage - 1);
    Views.labels.render();
  });

  $('#labels-template-next').addEventListener('click', () => {
    state.labels.templatePage += 1;
    Views.labels.render();
  });

  $('#labels-preview-btn').addEventListener('click', async () => {
    await runAction('Собираем контекст печати...', async () => {
      const result = await API.call('preview_100x180_label', {
        document_id: state.labels.selectedOrderId,
        template_path: state.labels.selectedTemplatePath,
        csv_path: selectedLabelCsvPath(),
        printer_name: $('#labels-printer-select').value,
        manufacture_date: $('#labels-manufacture-date').value,
        expiration_date: $('#labels-expiration-date').value,
        quantity_value: $('#labels-quantity-value').value,
      });
      state.labels.preview = result.preview;
      Views.labels.render();
      return result;
    }, 'Контекст печати собран.');
  });

  $('#labels-print-btn').addEventListener('click', async () => {
    await runAction('Запускаем печать этикеток...', async () => {
      const result = await API.call('print_100x180_label', {
        document_id: state.labels.selectedOrderId,
        template_path: state.labels.selectedTemplatePath,
        csv_path: selectedLabelCsvPath(),
        printer_name: $('#labels-printer-select').value,
        manufacture_date: $('#labels-manufacture-date').value,
        expiration_date: $('#labels-expiration-date').value,
        quantity_value: $('#labels-quantity-value').value,
      });
      state.labels.preview = result.preview || state.labels.preview;
      Views.labels.render();
      return result;
    }, 'Печать этикеток запущена.');
  });

  document.querySelectorAll('[data-clear-log]').forEach((button) => {
    button.addEventListener('click', async () => {
      const channel = button.dataset.clearLog;
      await runAction('Очищаем лог...', async () => {
        const result = await API.call('clear_logs', channel);
        await refreshLogs();
        return result;
      }, 'Лог очищен.');
    });
  });
}

async function init() {
  if (appInitialized) {
    return;
  }
  appInitialized = true;
  setTheme(state.theme);
  $('#intro-production-date').value = getDefaultDate(0);
  $('#intro-expiration-date').value = getDefaultDate(5);
  $('#tsd-production-date').value = getDefaultDate(0);
  $('#tsd-expiration-date').value = getDefaultDate(5);
  $('#labels-manufacture-date').value = getDefaultDate(0).slice(0, 7);
  $('#labels-expiration-date').value = getDefaultDate(5).slice(0, 7);

  await bindEvents();

  try {
    setStatusText('Загружаем интерфейс...', true);
    state.options = await API.call('get_options');
    const session = await API.call('get_session_info');
    applySessionInfo(session || {});
    applyOptions();
    updateOrderModeUi();
    Router.go(state.route);
    await refreshLogs();
    setStatusText('Интерфейс готов к работе.', true);
  } catch (error) {
    setStatusText(error.message, false);
    showToast(error.message, 'error');
  }

  setInterval(() => {
    refreshSessionInfo(false).catch(() => null);
    refreshLogs().catch(() => null);
    refreshCurrentRouteState().catch(() => null);
  }, 45000);
}

window.addEventListener('pywebviewready', init);
window.addEventListener('DOMContentLoaded', () => {
  if (window.pywebview?.api) {
    init();
  } else {
    setTheme(state.theme);
    setStatusText('Ожидаем инициализацию PyWebView API...', false);
  }
});

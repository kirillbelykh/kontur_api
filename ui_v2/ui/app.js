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

const ROUTE_KEYS = Object.keys(ROUTES);
const UI_PERF = {
  sessionPollMs: 120000,
  logPollMs: 15000,
  autoRefreshTickMs: 5000,
  aggregationSearchDebounceMs: 180,
  routeNavFreshMs: {
    orders: 12000,
    download: 12000,
    intro: 12000,
    tsd: 12000,
    aggregation: 45000,
    labels: 20000,
  },
  routeAutoRefreshMs: {
    orders: 60000,
    download: 60000,
    intro: 75000,
    tsd: 75000,
    aggregation: 180000,
    labels: 120000,
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
    selectedPrinter: '',
    selectedItemId: '',
    selectedIds: new Set(),
    autoDownload: false,
    progress: {
      active: false,
      processed: 0,
      total: 0,
      label: '',
    },
  },
  intro: {
    items: [],
    selectedIds: new Set(),
  },
  tsd: {
    items: [],
    selectedIds: new Set(),
    liveLoadedAt: 0,
    liveRefreshRunning: false,
  },
  aggregation: {
    downloadMode: 'comment',
    items: [],
    filteredItems: [],
    statusOptions: [],
    statusFilter: '',
    searchQuery: '',
    itemsVersion: 0,
    filterCacheKey: '',
    selectedIds: new Set(),
    lastClickedIndex: -1,
    currentPage: 0,
    pageSize: 200,
    cacheAgeSeconds: 0,
    totalItems: 0,
  },
  labels: {
    templates: [],
    aggregationFiles: [],
    markingFiles: [],
    orders: [],
    printers: [],
    defaultPrinter: '',
    selectedPrinter: '',
    selectedTemplatePath: '',
    selectedOrderId: '',
    selectedAggregationPath: '',
    selectedMarkingPath: '',
    printScope: 'all',
    selectedRecordNumber: 1,
    preview: null,
    templatePage: 0,
    templatePageSize: 3,
  },
  ui: {
    sessionUpdatedAt: 0,
    routeUpdatedAt: Object.fromEntries(ROUTE_KEYS.map((route) => [route, 0])),
    routeDirty: Object.fromEntries(ROUTE_KEYS.map((route) => [route, true])),
    routeLoading: Object.fromEntries(ROUTE_KEYS.map((route) => [route, false])),
    logUpdatedAt: {},
    logLoading: {},
  },
};

let appInitialized = false;
let aggregationSearchTimer = null;
let buttonBusySequence = 0;

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

const LOG_SELECTORS = {
  orders: '#orders-log',
  download: '#download-log',
  intro: '#intro-log',
  tsd: '#tsd-log',
  aggregation: '#aggregation-log',
  labels: '#labels-log',
};

const ROUTE_LOG_CHANNEL = {
  orders: 'orders',
  download: 'download',
  intro: 'intro',
  tsd: 'tsd',
  aggregation: 'aggregation',
  labels: 'labels',
};

let lastActionButton = null;
let lastActionButtonAt = 0;
let uiAudioContext = null;

Object.values(ROUTE_LOG_CHANNEL).forEach((channel) => {
  state.ui.logUpdatedAt[channel] = 0;
  state.ui.logLoading[channel] = false;
});

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

function showToast(message, type = 'success', durationMs = null) {
  const host = $('#toast-host');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  host.appendChild(toast);
  const duration = durationMs ?? (type === 'error' ? 4200 : type === 'info' ? 1500 : 2800);
  setTimeout(() => toast.remove(), duration);
}

function getCurrentLogChannel() {
  return ROUTE_LOG_CHANNEL[state.route] || 'orders';
}

function appendUiLog(message, channel = getCurrentLogChannel()) {
  const selector = LOG_SELECTORS[channel];
  const element = selector ? $(selector) : null;
  if (!element) {
    return;
  }
  const timestamp = new Date().toLocaleTimeString('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const line = `[${timestamp}] ${message}`;
  const current = element.textContent.trim();
  element.textContent = current ? `${current}\n${line}` : line;
  element.scrollTop = element.scrollHeight;
}

function consumeLastActionButton() {
  if (!lastActionButton) {
    return null;
  }
  if ((Date.now() - lastActionButtonAt) > 1500 || !document.body.contains(lastActionButton)) {
    lastActionButton = null;
    return null;
  }
  const button = lastActionButton;
  lastActionButton = null;
  return button;
}

function setButtonBusy(button, busyText = 'Выполняется...') {
  if (!button) {
    return () => {};
  }
  buttonBusySequence += 1;
  const busyToken = `busy-${buttonBusySequence}`;
  const originalText = button.textContent;
  const originalDisabled = button.disabled;
  const originalMinWidth = button.style.minWidth;
  button.dataset.busyToken = busyToken;
  button.style.minWidth = `${button.offsetWidth}px`;
  button.textContent = busyText;
  button.disabled = true;
  button.classList.add('is-busy');
  return () => {
    if (button.dataset.busyToken !== busyToken) {
      return;
    }
    delete button.dataset.busyToken;
    button.textContent = originalText;
    button.disabled = originalDisabled;
    button.classList.remove('is-busy');
    button.style.minWidth = originalMinWidth;
  };
}

function schedulePostActionRefresh(channel) {
  window.setTimeout(() => {
    refreshLogs(channel, { force: true }).catch(() => null);
    refreshSessionInfo(false).catch(() => null);
  }, 0);
}

function getUiAudioContext() {
  const AudioContextCtor = window.AudioContext || window.webkitAudioContext;
  if (!AudioContextCtor) {
    return null;
  }
  if (!uiAudioContext) {
    uiAudioContext = new AudioContextCtor();
  }
  if (uiAudioContext.state === 'suspended') {
    uiAudioContext.resume().catch(() => null);
  }
  return uiAudioContext;
}

function playUiSound(kind = 'start') {
  const context = getUiAudioContext();
  if (!context) {
    return;
  }
  const patterns = {
    start: [
      { frequency: 720, duration: 0.045, delay: 0, gain: 0.018 },
    ],
    success: [
      { frequency: 720, duration: 0.04, delay: 0, gain: 0.018 },
      { frequency: 880, duration: 0.06, delay: 0.055, gain: 0.02 },
    ],
    error: [
      { frequency: 280, duration: 0.08, delay: 0, gain: 0.018 },
      { frequency: 220, duration: 0.09, delay: 0.07, gain: 0.015 },
    ],
  };
  const sequence = patterns[kind] || patterns.start;
  const now = context.currentTime;
  sequence.forEach((tone) => {
    const oscillator = context.createOscillator();
    const gainNode = context.createGain();
    oscillator.type = 'sine';
    oscillator.frequency.value = tone.frequency;
    gainNode.gain.setValueAtTime(0.0001, now + tone.delay);
    gainNode.gain.exponentialRampToValueAtTime(tone.gain, now + tone.delay + 0.01);
    gainNode.gain.exponentialRampToValueAtTime(0.0001, now + tone.delay + tone.duration);
    oscillator.connect(gainNode);
    gainNode.connect(context.destination);
    oscillator.start(now + tone.delay);
    oscillator.stop(now + tone.delay + tone.duration + 0.02);
  });
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

function hasActiveTextSelection() {
  const selection = window.getSelection?.();
  return Boolean(selection && String(selection).trim());
}

function updateDownloadSelectionMeta() {
  const selected = state.download.selectedIds.size;
  const total = state.download.items.length;
  const element = $('#download-selection-meta');
  if (!element) {
    return;
  }
  element.textContent = `Всего заказов: ${total} • Выбрано: ${selected}`;
}

function updateDownloadProgressUi() {
  const progress = state.download.progress || {};
  const host = $('#download-progress');
  const bar = $('#download-progress-bar');
  const label = $('#download-progress-label');
  if (!host || !bar || !label) {
    return;
  }
  const total = Math.max(0, Number(progress.total || 0));
  const processed = Math.max(0, Number(progress.processed || 0));
  const percent = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;
  host.classList.toggle('is-active', Boolean(progress.active));
  bar.style.width = `${percent}%`;
  label.textContent = progress.label || (progress.active ? `Прогресс: ${processed}/${total}` : 'Прогресс скачивания появится во время массовой загрузки.');
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
    rowEl.addEventListener('click', (event) => {
      if (event.target.closest('button, a, input, select, textarea, label')) {
        return;
      }
      if (hasActiveTextSelection()) {
        return;
      }
      const id = rowEl.dataset.rowId;
      if (single) {
        onRowClick(id, undefined, event);
      } else {
        onRowClick(id, rowEl.classList.contains('is-selected'), event);
      }
    });
  });
}

function fillSelectOptions(select, options, placeholder = '') {
  const current = select.value;
  const items = [];
  if (placeholder) {
    items.push(`<option value="">${escapeHtml(placeholder)}</option>`);
  }
  items.push(
    ...(options || []).map((option) => {
      const value = typeof option === 'object' ? option.value : option;
      const label = typeof option === 'object' ? option.label : option;
      return `<option value="${escapeHtml(value)}">${escapeHtml(label)}</option>`;
    }),
  );
  select.innerHTML = items.join('');
  if ([...select.options].some((option) => option.value === current)) {
    select.value = current;
  }
}

function nowMs() {
  return Date.now();
}

function isRouteActive(route) {
  return state.route === route;
}

function markRouteLoaded(route) {
  state.ui.routeUpdatedAt[route] = nowMs();
  state.ui.routeDirty[route] = false;
}

function markRoutesDirty(routes) {
  (routes || []).forEach((route) => {
    if (!route || !(route in state.ui.routeDirty)) {
      return;
    }
    state.ui.routeDirty[route] = true;
  });
}

function getRouteRefreshAge(route, fallbackMs = 0) {
  return UI_PERF.routeAutoRefreshMs[route] || fallbackMs;
}

function buildAggregationFilterCacheKey() {
  return [
    String(state.aggregation.itemsVersion || 0),
    String(state.aggregation.statusFilter || ''),
    String(state.aggregation.searchQuery || '').trim().toLowerCase(),
  ].join('|');
}

function invalidateAggregationFilterCache() {
  state.aggregation.filterCacheKey = '';
}

function getFilteredAggregationItems() {
  const cacheKey = buildAggregationFilterCacheKey();
  if (state.aggregation.filterCacheKey === cacheKey && Array.isArray(state.aggregation.filteredItems)) {
    return state.aggregation.filteredItems;
  }
  const query = String(state.aggregation.searchQuery || '').trim().toLowerCase();
  const statusFilter = String(state.aggregation.statusFilter || '').trim();
  const filteredItems = (state.aggregation.items || []).filter((item) => {
    const matchesStatus = !statusFilter || item.status === statusFilter;
    if (!matchesStatus) return false;
    if (!query) return true;
    const haystack = [
      item.aggregate_code,
      item.comment,
      item.status_label,
      item.created_at_label,
      item.document_id,
    ]
      .map((value) => String(value || '').toLowerCase())
      .join(' ');
    return haystack.includes(query);
  });
  state.aggregation.filterCacheKey = cacheKey;
  state.aggregation.filteredItems = filteredItems;
  return filteredItems;
}

function setAggregationSelection(ids) {
  state.aggregation.selectedIds = new Set((ids || []).map((id) => String(id)));
}

function getAggregationPageState(filteredRows) {
  const pageSize = Math.max(25, Number(state.aggregation.pageSize || 200));
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  state.aggregation.currentPage = Math.min(Math.max(0, state.aggregation.currentPage || 0), totalPages - 1);
  const start = state.aggregation.currentPage * pageSize;
  const end = Math.min(start + pageSize, filteredRows.length);
  return {
    pageSize,
    totalPages,
    currentPage: state.aggregation.currentPage,
    start,
    end,
    rows: filteredRows.slice(start, end),
  };
}

function updateAggregationSelectionMeta(filteredRows, pageState = null) {
  const total = state.aggregation.totalItems || (state.aggregation.items || []).length;
  const filtered = filteredRows.length;
  const selected = state.aggregation.selectedIds.size;
  const page = pageState ? `${pageState.currentPage + 1}/${pageState.totalPages}` : '1/1';
  const cacheAge = Number(state.aggregation.cacheAgeSeconds || 0);
  const freshness = cacheAge > 0 ? ` • Кэш: ${cacheAge} сек.` : '';
  $('#agg-selection-meta').textContent = `Всего АК: ${total} • Найдено: ${filtered} • Выбрано: ${selected} • Страница: ${page}${freshness}`;
}

function toggleAggregationSelection(documentId, index, event) {
  const id = String(documentId || '');
  if (!id) return;
  const filteredRows = getFilteredAggregationItems();
  const selected = state.aggregation.selectedIds.has(id);
  if (event?.shiftKey && state.aggregation.lastClickedIndex >= 0) {
    const start = Math.min(state.aggregation.lastClickedIndex, index);
    const end = Math.max(state.aggregation.lastClickedIndex, index);
    const shouldSelect = !selected;
    filteredRows.slice(start, end + 1).forEach((row) => {
      if (shouldSelect) {
        state.aggregation.selectedIds.add(row.document_id);
      } else {
        state.aggregation.selectedIds.delete(row.document_id);
      }
    });
  } else if (selected) {
    state.aggregation.selectedIds.delete(id);
  } else {
    state.aggregation.selectedIds.add(id);
  }
  state.aggregation.lastClickedIndex = index;
  Views.aggregation.render();
}

function createAggregationTable(container, filteredRows) {
  if (!filteredRows.length) {
    container.innerHTML = '<div class="table-empty">Агрегационные коды по текущему фильтру не найдены.</div>';
    updateAggregationSelectionMeta(filteredRows);
    return;
  }
  const pageState = getAggregationPageState(filteredRows);
  const rows = pageState.rows;

  const html = `
    <div class="table-wrapper" style="max-height: 520px">
      <table class="table table-compact aggregation-table">
        <thead>
          <tr>
            <th class="check-col">✓</th>
            <th>АК</th>
            <th>Название</th>
            <th>Статус</th>
            <th>Создан</th>
            <th>КМ</th>
            <th>Ошибки</th>
          </tr>
        </thead>
        <tbody>
          ${rows.map((row, localIndex) => {
            const id = String(row.document_id || '');
            const selected = state.aggregation.selectedIds.has(id);
            return `
              <tr data-row-id="${escapeHtml(id)}" data-row-index="${pageState.start + localIndex}" class="${selected ? 'is-selected' : ''}">
                <td class="check-col"><input type="checkbox" data-check-id="${escapeHtml(id)}" ${selected ? 'checked' : ''}></td>
                <td>
                  <strong>${escapeHtml(row.aggregate_code || '')}</strong>
                  <span class="cell-note">${escapeHtml(row.document_id || '')}</span>
                </td>
                <td>${escapeHtml(row.comment || '—')}</td>
                <td>
                  ${statusPill(row.status_label || row.status || '—')}
                  ${row.status === 'readyForSendAfterApproved'
                    ? '<span class="cell-note">Изменённый состав после прошлой регистрации</span>'
                    : ''}
                </td>
                <td>${escapeHtml(row.created_at_label || '—')}</td>
                <td>${escapeHtml(row.includes_units_count ?? '0')}</td>
                <td>${escapeHtml(row.codes_check_errors_count ?? '0')}</td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>
    </div>
    <div class="table-pagination">
      <div class="table-pagination__summary">
        Показано ${pageState.start + 1}-${pageState.end} из ${filteredRows.length}
      </div>
      <div class="table-pagination__actions">
        <button class="secondary-btn" data-agg-page="prev" ${pageState.currentPage <= 0 ? 'disabled' : ''}>Назад</button>
        <span class="table-pagination__page">Страница ${pageState.currentPage + 1} из ${pageState.totalPages}</span>
        <button class="secondary-btn" data-agg-page="next" ${pageState.currentPage >= pageState.totalPages - 1 ? 'disabled' : ''}>Вперёд</button>
      </div>
    </div>
  `;
  container.innerHTML = html;
  updateAggregationSelectionMeta(filteredRows, pageState);
  container.onclick = (event) => {
    const pageButton = event.target.closest('[data-agg-page]');
    if (pageButton) {
      if (pageButton.dataset.aggPage === 'prev') {
        state.aggregation.currentPage = Math.max(0, state.aggregation.currentPage - 1);
      } else {
        state.aggregation.currentPage += 1;
      }
      Views.aggregation.render();
      return;
    }

    const checkboxEl = event.target.closest('input[data-check-id]');
    if (checkboxEl) {
      const rowEl = checkboxEl.closest('tr[data-row-id]');
      const rowIndex = Number(rowEl?.dataset.rowIndex || 0);
      toggleAggregationSelection(checkboxEl.dataset.checkId, rowIndex, event);
      return;
    }

    const rowEl = event.target.closest('tbody tr[data-row-id]');
    if (!rowEl) {
      return;
    }
    if (hasActiveTextSelection()) {
      return;
    }
    const rowIndex = Number(rowEl.dataset.rowIndex || 0);
    toggleAggregationSelection(rowEl.dataset.rowId, rowIndex, event);
  };
}

function renderStatusCell(row) {
  const summary = row?.status_summary ? `<span class="cell-note">${escapeHtml(row.status_summary)}</span>` : '';
  return `${statusPill(row?.status)}${summary}`;
}

function formatPreview(preview) {
  if (!preview) {
    return 'Выберите шаблон, файл и заказ, затем нажмите «Показать контекст».';
  }
  const lines = [
    `Заказ: ${preview.order_name}` ,
    `Шаблон: ${preview.template_category} / ${preview.data_source_kind}` ,
    `Режим печати: ${preview.print_scope_label || 'Весь файл'}` ,
    `Размер: ${preview.size}` ,
    `Партия: ${preview.batch}` ,
    `Цвет: ${preview.color || '—'}` ,
    `Дата изготовления: ${preview.manufacture_date}` ,
    `Срок годности: ${preview.expiration_date}` ,
    `Количество: ${preview.quantity_pairs} ${preview.quantity_pairs_word}` ,
    `Упаковка: ${preview.package_text || 'не используется'}` ,
    `Этикеток к печати: ${preview.label_count}` ,
  ];
  if (preview.total_record_count) {
    lines.push(`Записей в файле: ${preview.total_record_count}`);
  }
  if (preview.selected_record_number) {
    lines.push(`Выбрана запись: ${preview.selected_record_number} из ${preview.total_record_count || preview.label_count}`);
  }
  if (preview.selected_code_label && preview.selected_code_value_short) {
    lines.push(`${preview.selected_code_label}: ${preview.selected_code_value_short}`);
  }
  if (preview.selected_code_gtin) {
    lines.push(`GTIN выбранной записи: ${preview.selected_code_gtin}`);
  }
  if (preview.selected_code_name) {
    lines.push(`Наименование выбранной записи: ${preview.selected_code_name}`);
  }
  return lines.join('\n');
}

function setTheme(theme) {
  state.theme = theme;
  document.body.dataset.theme = theme;
  localStorage.setItem('kontur-ui-v2-theme', theme);
  $('#theme-toggle-btn').textContent = theme === 'dark' ? 'Светлая тема' : 'Тёмная тема';
}

function applyDefaultDateWindow(dateWindow) {
  const productionDate = String(dateWindow?.production_date || '').trim();
  const expirationDate = String(dateWindow?.expiration_date || '').trim();
  if (!productionDate || !expirationDate) {
    return;
  }

  [
    '#intro-production-date',
    '#tsd-production-date',
    '#agg-intro-production-date',
    '#labels-manufacture-date',
  ].forEach((selector) => {
    const element = $(selector);
    if (element && !String(element.value || '').trim()) {
      element.value = productionDate;
    }
  });

  [
    '#intro-expiration-date',
    '#tsd-expiration-date',
    '#agg-intro-expiration-date',
    '#labels-expiration-date',
  ].forEach((selector) => {
    const element = $(selector);
    if (element && !String(element.value || '').trim()) {
      element.value = expirationDate;
    }
  });
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
  state.ui.sessionUpdatedAt = nowMs();
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
    Promise.all([
      refreshCurrentRouteState({ freshnessMs: UI_PERF.routeNavFreshMs[route] || 0 }),
      refreshLogs(getCurrentLogChannel(), { freshnessMs: 4000 }),
    ]).catch(() => null);
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

async function refreshLogs(channel = null, options = {}) {
  const channels = channel ? [String(channel)] : Object.keys(LOG_SELECTORS);
  const force = Boolean(options.force);
  const freshnessMs = Math.max(0, Number(options.freshnessMs || 0));
  for (const currentChannel of channels) {
    const selector = LOG_SELECTORS[currentChannel];
    if (!selector) {
      continue;
    }
    const lastUpdatedAt = Number(state.ui.logUpdatedAt[currentChannel] || 0);
    if (!force && lastUpdatedAt > 0 && nowMs() - lastUpdatedAt < freshnessMs) {
      continue;
    }
    if (state.ui.logLoading[currentChannel]) {
      continue;
    }
    state.ui.logLoading[currentChannel] = true;
    try {
      const lines = await API.call('get_logs', currentChannel);
      $(selector).textContent = Array.isArray(lines) ? lines.join('\n') : '';
      state.ui.logUpdatedAt[currentChannel] = nowMs();
    } catch {
      $(selector).textContent = '';
    } finally {
      state.ui.logLoading[currentChannel] = false;
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
      if (state.download.selectedPrinter) {
        $('#download-printer-select').value = state.download.selectedPrinter;
      }
      updateDownloadSelectionMeta();
      updateDownloadProgressUi();

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
          compact: true,
          maxHeight: '520px',
          selectedIds: state.download.selectedIds,
          onRowClick: (id, isSelected) => {
            state.download.selectedItemId = id;
            if (isSelected) {
              state.download.selectedIds.delete(id);
            } else {
              state.download.selectedIds.add(id);
            }
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
      fillSelectOptions($('#agg-status-filter'), state.aggregation.statusOptions || []);
      $('#agg-status-filter').value = state.aggregation.statusFilter || '';
      $('#agg-search-query').value = state.aggregation.searchQuery || '';
      state.aggregation.selectedIds = new Set(
        [...state.aggregation.selectedIds].filter((id) => state.aggregation.items.some((item) => item.document_id === id)),
      );
      createAggregationTable($('#agg-items-table'), getFilteredAggregationItems());
    },
  },
  labels: {
    render() {
      ensureLabelsSelectivePrintUi();
      fillSelect($("#labels-printer-select"), state.labels.printers, "Выберите принтер");
      if (state.labels.selectedPrinter) {
        $("#labels-printer-select").value = state.labels.selectedPrinter;
      }

      const templateHost = $("#labels-template-grid");
      const pageSize = state.labels.templatePageSize || 3;
      const totalPages = Math.max(1, Math.ceil(state.labels.templates.length / pageSize));
      state.labels.templatePage = Math.min(state.labels.templatePage, totalPages - 1);
      const pageStart = state.labels.templatePage * pageSize;
      const visibleTemplates = state.labels.templates.slice(pageStart, pageStart + pageSize);
      $("#labels-template-page").textContent = `${state.labels.templates.length ? pageStart + 1 : 0}-${Math.min(pageStart + visibleTemplates.length, state.labels.templates.length)} из ${state.labels.templates.length}`;
      $("#labels-template-prev").disabled = state.labels.templatePage <= 0;
      $("#labels-template-next").disabled = state.labels.templatePage >= totalPages - 1;

      templateHost.innerHTML = visibleTemplates.map((template) => `
        <button class="template-card ${state.labels.selectedTemplatePath === template.path ? 'is-selected' : ''}" data-template-path="${escapeHtml(template.path)}">
          <strong>${escapeHtml(template.name)}</strong>
          <small>${escapeHtml(template.category)}</small>
          <small>${escapeHtml(template.relative_path)}</small>
          <small>${escapeHtml(template.source_label || template.data_source_kind)}</small>
        </button>
      `).join('');
      templateHost.querySelectorAll("[data-template-path]").forEach((button) => {
        button.addEventListener("click", () => {
          state.labels.selectedTemplatePath = button.dataset.templatePath;
          invalidateLabelsPreview();
          Views.labels.render();
        });
      });

      createTable(
        $("#labels-orders-table"),
        [
          { label: "Заявка", key: "order_name" },
          { label: "Полное наименование", key: "full_name" },
          { label: "GTIN", key: "gtin" },
          { label: "Размер", key: "size" },
          { label: "Партия", key: "batch" },
        ],
        state.labels.orders,
        {
          single: true,
          compact: true,
          maxHeight: "260px",
          selectedIds: state.labels.selectedOrderId,
          onRowClick: (id) => {
            state.labels.selectedOrderId = id;
            invalidateLabelsPreview();
            Views.labels.render();
          },
        },
      );

      createTable(
        $("#labels-aggregation-files-table"),
        [
          { label: "Файл", key: "name" },
          { label: "Папка", key: "folder_name" },
          { label: "Строк", key: "record_count" },
        ],
        state.labels.aggregationFiles,
        {
          single: true,
          compact: true,
          maxHeight: "260px",
          selectedIds: state.labels.selectedAggregationPath,
          rowId: (row) => row.path,
          onRowClick: (id) => {
            state.labels.selectedAggregationPath = id;
            invalidateLabelsPreview();
            Views.labels.render();
          },
        },
      );

      createTable(
        $("#labels-marking-files-table"),
        [
          { label: "Файл", key: "name" },
          { label: "Папка", key: "folder_name" },
          { label: "Строк", key: "record_count" },
        ],
        state.labels.markingFiles,
        {
          single: true,
          compact: true,
          maxHeight: "260px",
          selectedIds: state.labels.selectedMarkingPath,
          rowId: (row) => row.path,
          onRowClick: (id) => {
            state.labels.selectedMarkingPath = id;
            invalidateLabelsPreview();
            Views.labels.render();
          },
        },
      );

      const { total, selectedRecordNumber } = normalizeLabelsRecordSelection();
      const printScopeSelect = $("#labels-print-scope");
      const recordInput = $("#labels-record-number");
      const prevButton = $("#labels-record-prev");
      const nextButton = $("#labels-record-next");
      const infoBox = $("#labels-record-info");
      if (printScopeSelect) {
        printScopeSelect.value = state.labels.printScope || "all";
      }
      if (recordInput) {
        recordInput.value = String(selectedRecordNumber || 1);
        recordInput.min = total > 0 ? "1" : "0";
        recordInput.max = total > 0 ? String(total) : "";
        recordInput.disabled = state.labels.printScope !== "single" || total <= 0;
      }
      if (prevButton) {
        prevButton.disabled = state.labels.printScope !== "single" || total <= 0 || selectedRecordNumber <= 1;
      }
      if (nextButton) {
        nextButton.disabled = state.labels.printScope !== "single" || total <= 0 || selectedRecordNumber >= total;
      }
      if (infoBox) {
        infoBox.textContent = labelsRecordInfoText();
      }

      $("#labels-preview-box").textContent = formatPreview(state.labels.preview);
    },
  },
};

function normalizeLoadOptions(options = {}) {
  if (typeof options === 'boolean') {
    return { force: Boolean(options) };
  }
  return options || {};
}

async function loadRouteState(route, options = {}) {
  switch (route) {
    case 'orders':
      return loadOrdersState(options);
    case 'download':
      return loadDownloadState(options);
    case 'intro':
      return loadIntroState(options);
    case 'tsd':
      return loadTsdState(options);
    case 'aggregation':
      return loadAggregationState(options);
    case 'labels':
      return loadLabelsState(options);
    default:
      return null;
  }
}

async function maybeRefreshRouteState(route, options = {}) {
  const { force = false, freshnessMs = null } = normalizeLoadOptions(options);
  const effectiveRoute = String(route || '').trim();
  if (!effectiveRoute || !(effectiveRoute in state.ui.routeUpdatedAt)) {
    return null;
  }
  if (state.ui.routeLoading[effectiveRoute]) {
    return null;
  }
  const maxAge = freshnessMs ?? getRouteRefreshAge(effectiveRoute, 60000);
  const lastUpdatedAt = Number(state.ui.routeUpdatedAt[effectiveRoute] || 0);
  if (!force && !state.ui.routeDirty[effectiveRoute] && lastUpdatedAt > 0 && nowMs() - lastUpdatedAt < maxAge) {
    return null;
  }
  return loadRouteState(effectiveRoute, { force, render: isRouteActive(effectiveRoute) });
}

async function loadOrdersState(options = {}) {
  const { render = isRouteActive('orders') } = normalizeLoadOptions(options);
  state.ui.routeLoading.orders = true;
  try {
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
    if (render) {
      Views.orders.render();
    }
    markRouteLoaded('orders');
  } finally {
    state.ui.routeLoading.orders = false;
  }
}

async function loadDownloadState(options = {}) {
  const { render = isRouteActive('download') } = normalizeLoadOptions(options);
  state.ui.routeLoading.download = true;
  try {
    const result = await API.call('get_download_state');
    state.download.items = result.items || [];
    state.download.printers = result.printers || [];
    state.download.defaultPrinter = result.default_printer || state.download.defaultPrinter;
    if (!state.download.printers.includes(state.download.selectedPrinter)) {
      if (state.download.defaultPrinter && state.download.printers.includes(state.download.defaultPrinter)) {
        state.download.selectedPrinter = state.download.defaultPrinter;
      } else {
        state.download.selectedPrinter = state.download.printers[0] || '';
      }
    }
    state.download.selectedIds = new Set(
      [...state.download.selectedIds].filter((id) => state.download.items.some((item) => item.document_id === id)),
    );
    if (!state.download.items.some((item) => item.document_id === state.download.selectedItemId)) {
      state.download.selectedItemId = [...state.download.selectedIds][0] || state.download.items[0]?.document_id || '';
    }
    if (!state.download.selectedIds.size && state.download.selectedItemId) {
      state.download.selectedIds.add(state.download.selectedItemId);
    }
    if (render) {
      Views.download.render();
    }
    markRouteLoaded('download');
  } finally {
    state.ui.routeLoading.download = false;
  }
}

async function loadIntroState(options = {}) {
  const { render = isRouteActive('intro') } = normalizeLoadOptions(options);
  state.ui.routeLoading.intro = true;
  try {
    const result = await API.call('get_intro_state');
    state.intro.items = result.items || [];
    state.intro.selectedIds = new Set(
      [...state.intro.selectedIds].filter((id) => state.intro.items.some((item) => item.document_id === id)),
    );
    if (render) {
      Views.intro.render();
    }
    markRouteLoaded('intro');
  } finally {
    state.ui.routeLoading.intro = false;
  }
}

async function loadTsdState(options = {}) {
  const { render = isRouteActive('tsd'), live = false } = normalizeLoadOptions(options);
  state.ui.routeLoading.tsd = true;
  try {
    const result = await API.call('get_tsd_state', Boolean(live));
    state.tsd.items = result.items || [];
    state.tsd.selectedIds = new Set(
      [...state.tsd.selectedIds].filter((id) => state.tsd.items.some((item) => item.document_id === id)),
    );
    if (result.live) {
      state.tsd.liveLoadedAt = nowMs();
    }
    if (render) {
      Views.tsd.render();
    }
    markRouteLoaded('tsd');
    if (
      !live
      && isRouteActive('tsd')
      && !state.tsd.liveRefreshRunning
      && (nowMs() - Number(state.tsd.liveLoadedAt || 0)) > 45000
    ) {
      state.tsd.liveRefreshRunning = true;
      window.setTimeout(() => {
        loadTsdState({ force: true, live: true, render: isRouteActive('tsd') })
          .catch(() => null)
          .finally(() => {
            state.tsd.liveRefreshRunning = false;
          });
      }, 0);
    }
  } finally {
    state.ui.routeLoading.tsd = false;
  }
}

async function loadAggregationState(options = {}) {
  const { force = false, render = isRouteActive('aggregation') } = normalizeLoadOptions(options);
  state.ui.routeLoading.aggregation = true;
  try {
    const result = await API.call('get_aggregation_state', Boolean(force));
    state.aggregation.items = result.items || [];
    state.aggregation.itemsVersion += 1;
    invalidateAggregationFilterCache();
    state.aggregation.statusOptions = result.status_options || [];
    state.aggregation.cacheAgeSeconds = Number(result.cache_age_seconds || 0);
    state.aggregation.totalItems = Number(result.total_items || state.aggregation.items.length || 0);
    state.aggregation.selectedIds = new Set(
      [...state.aggregation.selectedIds].filter((id) => state.aggregation.items.some((item) => item.document_id === id)),
    );
    state.aggregation.lastClickedIndex = -1;
    const filteredRows = getFilteredAggregationItems();
    const pageSize = Math.max(25, Number(state.aggregation.pageSize || 200));
    const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
    state.aggregation.currentPage = Math.min(state.aggregation.currentPage || 0, totalPages - 1);
    if (render) {
      Views.aggregation.render();
    }
    markRouteLoaded('aggregation');
  } finally {
    state.ui.routeLoading.aggregation = false;
  }
}

async function loadLabelsState(options = {}) {
  const { render = isRouteActive('labels') } = normalizeLoadOptions(options);
  state.ui.routeLoading.labels = true;
  try {
    const result = await API.call('get_labels_state');
    state.labels.templates = result.templates || [];
    state.labels.aggregationFiles = result.aggregation_files || [];
    state.labels.markingFiles = result.marking_files || [];
    state.labels.orders = result.orders || [];
    state.labels.printers = result.printers || [];
    state.labels.defaultPrinter = result.default_printer || state.labels.defaultPrinter;
    if (!state.labels.printers.includes(state.labels.selectedPrinter)) {
      if (state.labels.defaultPrinter && state.labels.printers.includes(state.labels.defaultPrinter)) {
        state.labels.selectedPrinter = state.labels.defaultPrinter;
      } else {
        state.labels.selectedPrinter = state.labels.printers[0] || '';
      }
    }
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
    if (render) {
      Views.labels.render();
    }
    markRouteLoaded('labels');
  } finally {
    state.ui.routeLoading.labels = false;
  }
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

function selectedLabelFileMeta() {
  const template = selectedTemplate();
  if (!template) {
    return null;
  }
  if (template.data_source_kind === 'aggregation') {
    return state.labels.aggregationFiles.find((item) => item.path === state.labels.selectedAggregationPath) || null;
  }
  return state.labels.markingFiles.find((item) => item.path === state.labels.selectedMarkingPath) || null;
}

function invalidateLabelsPreview() {
  state.labels.preview = null;
}

function normalizeLabelsRecordSelection() {
  const file = selectedLabelFileMeta();
  const total = Math.max(0, Number(file?.record_count || 0));
  let selectedRecordNumber = Math.max(1, Number.parseInt(state.labels.selectedRecordNumber || 1, 10) || 1);
  if (total > 0) {
    selectedRecordNumber = Math.min(selectedRecordNumber, total);
  }
  state.labels.selectedRecordNumber = selectedRecordNumber;
  return { file, total, selectedRecordNumber };
}

function labelsRecordInfoText() {
  const { total, selectedRecordNumber } = normalizeLabelsRecordSelection();
  if (!total) {
    return 'Сначала выберите файл с кодами для печати.';
  }
  if (state.labels.printScope !== 'single') {
    return `Сейчас на печать пойдёт весь файл: ${total} этикеток.`;
  }
  let text = `Выбрана запись №${selectedRecordNumber} из ${total}. Нажмите «Показать контекст», чтобы проверить код перед печатью.`;
  const preview = state.labels.preview;
  if (
    preview
    && preview.print_scope === 'single'
    && Number(preview.selected_record_number || 0) === selectedRecordNumber
    && preview.selected_code_value_short
  ) {
    text += ` ${preview.selected_code_label || 'Код'}: ${preview.selected_code_value_short}.`;
  }
  return text;
}

function ensureLabelsSelectivePrintUi() {
  const panel = document.querySelector('#view-labels .panel');
  const formGrid = panel?.querySelector('.form-grid');
  if (!panel || !formGrid) {
    return;
  }
  if (!$('#labels-selective-print-controls')) {
    formGrid.insertAdjacentHTML('afterend', `
      <div class="form-grid" id="labels-selective-print-controls">
        <label class="field">
          <span>Что печатать</span>
          <select id="labels-print-scope">
            <option value="all">Весь файл</option>
            <option value="single">Одну этикетку по порядку</option>
          </select>
        </label>
        <label class="field">
          <span>Номер этикетки по порядку</span>
          <input id="labels-record-number" type="number" min="1" step="1" placeholder="1">
        </label>
      </div>
      <div class="inline-actions compact wrap" id="labels-record-stepper">
        <button class="secondary-btn" type="button" id="labels-record-prev">Предыдущая</button>
        <button class="secondary-btn" type="button" id="labels-record-next">Следующая</button>
      </div>
      <div class="inline-note" id="labels-record-info">Сначала выберите файл с кодами для печати.</div>
    `);
  }

  const scopeSelect = $('#labels-print-scope');
  const recordInput = $('#labels-record-number');
  const prevButton = $('#labels-record-prev');
  const nextButton = $('#labels-record-next');
  if (!scopeSelect || scopeSelect.dataset.bound === '1') {
    return;
  }

  scopeSelect.dataset.bound = '1';
  scopeSelect.addEventListener('change', () => {
    state.labels.printScope = scopeSelect.value === 'single' ? 'single' : 'all';
    invalidateLabelsPreview();
    Views.labels.render();
  });

  recordInput.addEventListener('change', () => {
    state.labels.selectedRecordNumber = Number.parseInt(recordInput.value || '1', 10) || 1;
    invalidateLabelsPreview();
    Views.labels.render();
  });

  prevButton.addEventListener('click', () => {
    state.labels.printScope = 'single';
    state.labels.selectedRecordNumber = Math.max(1, (Number(state.labels.selectedRecordNumber || 1) || 1) - 1);
    invalidateLabelsPreview();
    Views.labels.render();
  });

  nextButton.addEventListener('click', () => {
    const { total } = normalizeLabelsRecordSelection();
    state.labels.printScope = 'single';
    if (total > 0) {
      state.labels.selectedRecordNumber = Math.min(total, (Number(state.labels.selectedRecordNumber || 1) || 1) + 1);
    } else {
      state.labels.selectedRecordNumber = (Number(state.labels.selectedRecordNumber || 1) || 1) + 1;
    }
    invalidateLabelsPreview();
    Views.labels.render();
  });
}

async function refreshCurrentRouteState(options = {}) {
  return maybeRefreshRouteState(state.route, options);
}

async function runAction(label, action, successMessage = '') {
  const channel = getCurrentLogChannel();
  const releaseButton = setButtonBusy(consumeLastActionButton());
  appendUiLog(label, channel);
  showToast(label, 'info');
  playUiSound('start');
  setStatusText(label, true);
  await new Promise((resolve) => window.requestAnimationFrame(resolve));
  try {
    const result = await action();
    if (successMessage) {
      setStatusText(successMessage, true);
      appendUiLog(successMessage, channel);
      showToast(successMessage);
      playUiSound('success');
    }
    return result;
  } catch (error) {
    setStatusText(error.message, false);
    appendUiLog(`Ошибка: ${error.message}`, channel);
    showToast(error.message, 'error');
    playUiSound('error');
    throw error;
  } finally {
    releaseButton();
    schedulePostActionRefresh(channel);
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
  document.addEventListener('pointerdown', (event) => {
    const button = event.target.closest('button');
    if (!button) {
      return;
    }
    lastActionButton = button;
    lastActionButtonAt = Date.now();
  });

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
      await loadOrdersState({ force: true });
      return result;
    }, 'Позиция добавлена в очередь.');
  });

  $('#create-order-now-btn').addEventListener('click', async () => {
    await runAction('Создаём заказ...', async () => {
      const result = await API.call('create_order', readOrderForm());
      await loadOrdersState({ force: true });
      markRoutesDirty(['download', 'intro', 'tsd', 'labels']);
      return result;
    }, 'Заказ создан.');
  });

  $('#submit-queue-btn').addEventListener('click', async () => {
    await runAction('Выполняем очередь заказов...', async () => {
      const result = await API.call('submit_order_queue');
      await loadOrdersState({ force: true });
      markRoutesDirty(['download', 'intro', 'tsd', 'labels']);
      return result;
    }, 'Очередь заказов выполнена.');
  });

  $('#clear-queue-btn').addEventListener('click', async () => {
    await runAction('Очищаем очередь...', async () => {
      const result = await API.call('clear_order_queue');
      await loadOrdersState({ force: true });
      return result;
    }, 'Очередь очищена.');
  });

  $('#orders-delete-btn').addEventListener('click', async () => {
    await runAction('Удаляем заказ...', async () => {
      const result = await API.call('delete_order', state.orders.selectedHistoryId);
      await loadOrdersState({ force: true });
      markRoutesDirty(['download', 'intro', 'tsd', 'labels']);
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
      await loadOrdersState({ force: true });
      markRoutesDirty(['download', 'intro', 'tsd', 'labels']);
      return result;
    }, 'Заказ восстановлен.');
  });

  $('#download-refresh-btn').addEventListener('click', async () => {
    state.download.autoDownload = $('#download-auto-checkbox').checked;
    await runAction('Синхронизируем статусы загрузки...', async () => {
      const result = await API.call('sync_download_statuses', state.download.autoDownload);
      await loadDownloadState({ force: true });
      markRoutesDirty(['intro', 'tsd', 'labels']);
      return result;
    }, 'Статусы загрузки обновлены.');
  });

  $('#download-manual-btn').addEventListener('click', async () => {
    await runAction('Скачиваем выбранный заказ...', async () => {
      const selectedIds = state.download.selectedIds.size
        ? Array.from(state.download.selectedIds)
        : [state.download.selectedItemId].filter(Boolean);
      if (!selectedIds.length) {
        throw new Error('Выберите хотя бы один заказ для скачивания.');
      }
      state.download.progress = {
        active: true,
        processed: 0,
        total: selectedIds.length,
        label: `Прогресс скачивания: 0/${selectedIds.length}`,
      };
      updateDownloadProgressUi();

      let successCount = 0;
      const errors = [];
      try {
        for (let index = 0; index < selectedIds.length; index += 1) {
          const documentId = selectedIds[index];
          const order = state.download.items.find((item) => item.document_id === documentId);
          appendUiLog(`Скачиваем: ${order?.order_name || documentId}`, 'download');
          try {
            await API.call('manual_download_order', documentId);
            successCount += 1;
          } catch (error) {
            errors.push(`${order?.order_name || documentId}: ${error.message}`);
            appendUiLog(`Ошибка скачивания: ${order?.order_name || documentId} - ${error.message}`, 'download');
          }
          state.download.progress = {
            active: true,
            processed: index + 1,
            total: selectedIds.length,
            label: `Прогресс скачивания: ${index + 1}/${selectedIds.length}`,
          };
          updateDownloadProgressUi();
        }
      } finally {
        state.download.progress = {
          active: false,
          processed: successCount,
          total: selectedIds.length,
          label: errors.length
            ? `Скачано ${successCount}/${selectedIds.length}, ошибок: ${errors.length}`
            : `Скачано ${successCount}/${selectedIds.length}`,
        };
        updateDownloadProgressUi();
      }
      await loadDownloadState({ force: true, render: isRouteActive('download') });
      markRoutesDirty(['intro', 'tsd', 'labels']);
      if (errors.length) {
        throw new Error(`Скачано ${successCount}/${selectedIds.length}. Подробности в логе.`);
      }
      return { success: true, processed: successCount, total: selectedIds.length };
    }, 'Заказ скачан.');
  });

  $('#download-print-btn').addEventListener('click', async () => {
    await runAction('Запускаем печать термоэтикеток...', async () => {
      const targetId = state.download.selectedItemId || Array.from(state.download.selectedIds)[0] || '';
      const result = await API.call('print_download_order', targetId, $('#download-printer-select').value);
      return result;
    }, 'Печать термоэтикеток запущена.');
  });

  $('#download-printer-select').addEventListener('change', () => {
    state.download.selectedPrinter = $('#download-printer-select').value;
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
      await loadIntroState({ force: true });
      markRoutesDirty(['download', 'tsd']);
      return result;
    }, 'Ввод в оборот завершён.');
  });

  $('#tsd-run-btn').addEventListener('click', async () => {
    await runAction('Создаём задания на ТСД...', async () => {
      const selectedIds = Array.from(state.tsd.selectedIds);
      const result = await API.call(
        'create_tsd_tasks',
        selectedIds,
        $('#tsd-intro-number').value,
        $('#tsd-production-date').value,
        $('#tsd-expiration-date').value,
        $('#tsd-batch-number').value,
      );
      const successfulIds = new Set((result.results || []).map((item) => item.document_id));
      const failedIds = new Set((result.errors || []).map((item) => item.document_id));
      state.tsd.selectedIds = new Set(
        selectedIds.filter((documentId) => failedIds.has(documentId)),
      );
      if (isRouteActive('tsd')) {
        Views.tsd.render();
      }
      markRoutesDirty(['download', 'intro']);
      window.setTimeout(() => {
        loadTsdState({ force: true }).catch(() => null);
        loadDownloadState({ force: true, render: isRouteActive('download') }).catch(() => null);
      }, 0);
      if (Array.isArray(result.errors) && result.errors.length) {
        throw new Error(`Создано ${result.results?.length || 0}/${selectedIds.length}. Подробности в логе.`);
      }
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
      await loadAggregationState({ force: true });
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
      Views.aggregation.render();
      markRoutesDirty(['labels']);
      return result;
    }, 'Агрегационные коды скачаны.');
  });

  $('#agg-status-filter').addEventListener('change', () => {
    state.aggregation.statusFilter = $('#agg-status-filter').value;
    state.aggregation.lastClickedIndex = -1;
    state.aggregation.currentPage = 0;
    invalidateAggregationFilterCache();
    Views.aggregation.render();
  });

  $('#agg-search-query').addEventListener('input', () => {
    const nextQuery = $('#agg-search-query').value;
    if (aggregationSearchTimer) {
      window.clearTimeout(aggregationSearchTimer);
    }
    aggregationSearchTimer = window.setTimeout(() => {
      state.aggregation.searchQuery = nextQuery;
      state.aggregation.lastClickedIndex = -1;
      state.aggregation.currentPage = 0;
      invalidateAggregationFilterCache();
      Views.aggregation.render();
    }, UI_PERF.aggregationSearchDebounceMs);
  });

  $('#agg-select-visible-btn').addEventListener('click', () => {
    setAggregationSelection(getFilteredAggregationItems().map((item) => item.document_id));
    Views.aggregation.render();
  });

  $('#agg-select-by-name-btn').addEventListener('click', () => {
    const selectedRow = state.aggregation.items.find((item) => state.aggregation.selectedIds.has(item.document_id));
    const fallbackRow = getFilteredAggregationItems()[0];
    const targetName = String(selectedRow?.comment || fallbackRow?.comment || '').trim();
    if (!targetName) {
      showToast('Сначала выберите АК или задайте поиск по наименованию.', 'error');
      return;
    }
    setAggregationSelection(
      state.aggregation.items
        .filter((item) => String(item.comment || '').trim() === targetName)
        .map((item) => item.document_id),
    );
    Views.aggregation.render();
  });

  $('#agg-clear-selection-btn').addEventListener('click', () => {
    state.aggregation.selectedIds = new Set();
    state.aggregation.lastClickedIndex = -1;
    Views.aggregation.render();
  });

  $('#agg-refresh-list-btn').addEventListener('click', async () => {
    await runAction('Обновляем список АК...', async () => {
      await loadAggregationState({ force: true });
      return { success: true };
    }, 'Список АК обновлён.');
  });

  $('#agg-download-selected-btn').addEventListener('click', async () => {
    await runAction('Скачиваем выбранные АК...', async () => {
      const selectedIds = Array.from(state.aggregation.selectedIds);
      if (!selectedIds.length) {
        throw new Error('Выберите хотя бы один АК.');
      }
      const result = await API.call('download_selected_aggregations', selectedIds);
      Views.aggregation.render();
      markRoutesDirty(['labels']);
      return result;
    }, 'Выбранные АК скачаны.');
  });

  $('#agg-approve-selected-btn').addEventListener('click', async () => {
    await runAction('Проводим выбранные АК...', async () => {
      const selectedIds = Array.from(state.aggregation.selectedIds);
      if (!selectedIds.length) {
        throw new Error('Выберите хотя бы один АК.');
      }
      const allowDisaggregate = $('#agg-allow-disaggregate').checked
        || window.confirm('Если среди выбранных АК есть коды, уже привязанные к другому АК, разрешить расформирование старого АК?');
      const result = await API.call(
        'approve_selected_aggregations',
        selectedIds,
        allowDisaggregate,
      );
      await loadAggregationState({ force: true });
      return result;
    }, 'Проведение выбранных АК завершено.');
  });

  $('#agg-intro-selected-btn').addEventListener('click', async () => {
    await runAction('Вводим в оборот коды из выбранных АК...', async () => {
      const selectedIds = Array.from(state.aggregation.selectedIds);
      if (!selectedIds.length) {
        throw new Error('Выберите хотя бы один АК.');
      }
      const result = await API.call(
        'introduce_selected_aggregations',
        selectedIds,
        $('#agg-intro-production-date').value,
        $('#agg-intro-expiration-date').value,
        $('#agg-intro-batch-number').value,
      );
      await loadAggregationState({ force: true });
      markRoutesDirty(['download', 'intro', 'tsd']);
      return result;
    }, 'Ввод в оборот по выбранным АК завершён.');
  });

  $('#agg-refill-btn').addEventListener('click', async () => {
    await runAction('Запускаем повторное наполнение АК...', async () => {
      const result = await API.call(
        'refill_aggregations',
        $('#agg-approve-filter').value,
        $('#agg-refill-token').value,
      );
      await loadAggregationState({ force: true });
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
        print_scope: state.labels.printScope,
        record_number: state.labels.printScope === 'single' ? state.labels.selectedRecordNumber : null,
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
        print_scope: state.labels.printScope,
        record_number: state.labels.printScope === 'single' ? state.labels.selectedRecordNumber : null,
      });
      state.labels.preview = result.preview || state.labels.preview;
      Views.labels.render();
      return result;
    }, 'Печать этикеток запущена.');
  });

  $('#labels-printer-select').addEventListener('change', () => {
    state.labels.selectedPrinter = $('#labels-printer-select').value;
  });

  document.querySelectorAll('[data-clear-log]').forEach((button) => {
    button.addEventListener('click', async () => {
      const channel = button.dataset.clearLog;
      await runAction('Очищаем лог...', async () => {
        const result = await API.call('clear_logs', channel);
        await refreshLogs(channel, { force: true });
        return result;
      }, 'Лог очищен.');
    });
  });
}

async function runAutoRefreshTick() {
  if (document.hidden) {
    return;
  }
  const currentRoute = state.route;
  const currentLogChannel = getCurrentLogChannel();
  const currentTime = nowMs();

  if ((currentTime - Number(state.ui.sessionUpdatedAt || 0)) >= UI_PERF.sessionPollMs) {
    refreshSessionInfo(false).catch(() => null);
  }

  if ((currentTime - Number(state.ui.logUpdatedAt[currentLogChannel] || 0)) >= UI_PERF.logPollMs) {
    refreshLogs(currentLogChannel, { freshnessMs: 0 }).catch(() => null);
  }

  const routeMaxAge = getRouteRefreshAge(currentRoute, 60000);
  const routeAge = currentTime - Number(state.ui.routeUpdatedAt[currentRoute] || 0);
  if (state.ui.routeDirty[currentRoute] || routeAge >= routeMaxAge) {
    refreshCurrentRouteState({ freshnessMs: 0 }).catch(() => null);
  }
}

function bindPerformanceEvents() {
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) {
      return;
    }
    refreshSessionInfo(false).catch(() => null);
    refreshLogs(getCurrentLogChannel(), { force: true }).catch(() => null);
    refreshCurrentRouteState({ freshnessMs: 3000 }).catch(() => null);
  });

  window.addEventListener('focus', () => {
    refreshLogs(getCurrentLogChannel(), { freshnessMs: 3000 }).catch(() => null);
  });
}

async function init() {
  if (appInitialized) {
    return;
  }
  appInitialized = true;
  setTheme(state.theme);

  await bindEvents();

  try {
    setStatusText('Загружаем интерфейс...', true);
    state.options = await API.call('get_options');
    const session = await API.call('get_session_info');
    const defaultDateWindow = await API.call('get_default_date_window');
    applySessionInfo(session || {});
    applyDefaultDateWindow(defaultDateWindow);
    applyOptions();
    updateOrderModeUi();
    bindPerformanceEvents();
    Router.go(state.route);
    await refreshLogs(getCurrentLogChannel(), { force: true });
    setStatusText('Интерфейс готов к работе.', true);
  } catch (error) {
    setStatusText(error.message, false);
    showToast(error.message, 'error');
  }

  setInterval(() => {
    runAutoRefreshTick().catch(() => null);
  }, UI_PERF.autoRefreshTickMs);
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

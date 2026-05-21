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
    subtitle: 'Шаблоны BarTender 100x180 и 100x136, контекст печати и запуск печати.',
  },
};

const CLIENT_CONFIG = {
  browserMode: Boolean(window.__KONTUR_CLIENT_CONFIG__?.browserMode),
  mobileMode: Boolean(window.__KONTUR_CLIENT_CONFIG__?.mobileMode),
  disableLabels: Boolean(window.__KONTUR_CLIENT_CONFIG__?.disableLabels),
  disablePrinting: Boolean(window.__KONTUR_CLIENT_CONFIG__?.disablePrinting),
  apiBase: String(window.__KONTUR_CLIENT_CONFIG__?.apiBase || '/api/call').replace(/\/+$/, ''),
  appTitle: String(window.__KONTUR_CLIENT_CONFIG__?.appTitle || '').trim(),
  brandTitle: String(window.__KONTUR_CLIENT_CONFIG__?.brandTitle || '').trim(),
  subtitleSuffix: String(window.__KONTUR_CLIENT_CONFIG__?.subtitleSuffix || '').trim(),
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
    orders: 15000,
    download: 15000,
    intro: 15000,
    tsd: 15000,
    aggregation: 180000,
    labels: 120000,
  },
};

const state = {
  theme: localStorage.getItem('kontur-ui-v2-theme-choice') || 'light',
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
    selectedQueueId: '',
    deletedOrders: [],
    selectedHistoryId: '',
    selectedDeletedId: '',
    showDeleted: false,
    historySearch: '',
    fullscreenTable: '',
  },
  download: {
    items: [],
    printers: [],
    defaultPrinter: '',
    selectedPrinter: '',
    recordNumber: '',
    selectedItemId: '',
    selectedIds: new Set(),
    searchQuery: '',
    lastClickedIndex: -1,
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
    searchQuery: '',
    statusFilter: '',
  },
  tsd: {
    items: [],
    selectedIds: new Set(),
    searchQuery: '',
    statusFilter: '',
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
    sheetFormats: [],
    defaultSheetFormat: '100x180',
    selectedSheetFormat: '100x180',
    templates: [],
    aggregationFiles: [],
    markingFiles: [],
    orders: [],
    printers: [],
    defaultPrinter: '',
    selectedPrinter: '',
    selectedTemplatePath: '',
    selectedOrderId: '',
    manualPrompt: '',
    manualFields: {
      gtin: '',
      size: '',
      batch: '',
      color: '',
      units_per_pack: '',
    },
    manualEnabled: false,
    selectedAggregationPath: '',
    selectedMarkingPath: '',
    printScope: 'all',
    selectedRecordNumber: 1,
    rangeStartNumber: 1,
    rangeEndNumber: 1,
    preview: null,
    templatePage: 0,
    templatePageSize: 3,
    tableSearch: {
      orders: '',
      aggregation: '',
      marking: '',
    },
    fullscreenTable: '',
  },
  ui: {
    sessionUpdatedAt: 0,
    routeUpdatedAt: Object.fromEntries(ROUTE_KEYS.map((route) => [route, 0])),
    routeDirty: Object.fromEntries(ROUTE_KEYS.map((route) => [route, true])),
    routeLoading: Object.fromEntries(ROUTE_KEYS.map((route) => [route, false])),
    logUpdatedAt: {},
    logLoading: {},
    findQuery: '',
    detailsLoading: false,
  },
};

let appInitialized = false;
let aggregationSearchTimer = null;
let buttonBusySequence = 0;
let interactionFallbacksInstalled = false;
let desktopContextMenu = null;
let desktopContextMenuTarget = null;
let desktopFindPanel = null;
let orderDetailsModal = null;

const $ = (selector) => document.querySelector(selector);

const API = {
  async call(method, ...args) {
    const target = window.pywebview?.api?.[method];
    if (target) {
      const result = await target(...args);
      if (result?.error) {
        throw new Error(result.error);
      }
      return result;
    }

    const response = await fetch(`${CLIENT_CONFIG.apiBase}/${encodeURIComponent(method)}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
      credentials: 'same-origin',
      body: JSON.stringify({ args }),
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch (error) {
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      throw new Error('Некорректный ответ сервера.');
    }
    if (!response.ok) {
      throw new Error(payload?.error || `HTTP ${response.status}`);
    }
    if (payload?.error) {
      throw new Error(payload.error);
    }
    return payload;
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

function repairMojibakeText(value) {
  const text = String(value ?? '');
  if (!text || !/(\u0420\xa0.|\u0420\u040e.|\u0421\u0402\u0421\u045f|\u0420\u0406\u0420\u040f|\u0420\u0406\u0420\u201a|\u0420\u0457\u0421\u2014\u0420\u2026)/.test(text)) {
    return text;
  }
  try {
    const bytes = Uint8Array.from(Array.from(text, (char) => char.charCodeAt(0) & 0xff));
    const decoded = new TextDecoder('utf-8', { fatal: false }).decode(bytes);
    return decoded && decoded !== text ? decoded : text;
  } catch (error) {
    return text;
  }
}

function normalizeMojibakeInDom(root) {
  if (!root) {
    return;
  }
  const textNodes = [];
  if (root.nodeType === Node.TEXT_NODE) {
    textNodes.push(root);
  } else {
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    let node = walker.nextNode();
    while (node) {
      textNodes.push(node);
      node = walker.nextNode();
    }
  }
  textNodes.forEach((node) => {
    const repaired = repairMojibakeText(node.nodeValue);
    if (repaired !== node.nodeValue) {
      node.nodeValue = repaired;
    }
  });

  const elements = [];
  if (root.nodeType === Node.ELEMENT_NODE) {
    elements.push(root, ...root.querySelectorAll('*'));
  }
  elements.forEach((element) => {
    ['placeholder', 'title', 'aria-label'].forEach((attr) => {
      const current = element.getAttribute?.(attr);
      if (!current) {
        return;
      }
      const repaired = repairMojibakeText(current);
      if (repaired !== current) {
        element.setAttribute(attr, repaired);
      }
    });
  });
  if (document.title) {
    document.title = repairMojibakeText(document.title);
  }
}

let mojibakeGuardInstalled = false;
let mojibakeObserver = null;
let mojibakeNormalizationQueued = false;
const mojibakePendingRoots = new Set();

function flushMojibakeNormalizationQueue() {
  mojibakeNormalizationQueued = false;
  const roots = Array.from(mojibakePendingRoots);
  mojibakePendingRoots.clear();
  if (!roots.length) {
    return;
  }
  if (mojibakeObserver) {
    mojibakeObserver.disconnect();
  }
  try {
    roots.forEach((root) => {
      if (root?.isConnected === false) {
        return;
      }
      normalizeMojibakeInDom(root || document.documentElement);
    });
  } finally {
    if (mojibakeObserver && document.documentElement) {
      mojibakeObserver.observe(document.documentElement, {
        childList: true,
        subtree: true,
      });
    }
  }
}

function scheduleMojibakeNormalization(root = document.documentElement) {
  if (!root) {
    return;
  }
  mojibakePendingRoots.add(root);
  if (mojibakeNormalizationQueued) {
    return;
  }
  mojibakeNormalizationQueued = true;
  const scheduler = typeof window.requestAnimationFrame === 'function'
    ? window.requestAnimationFrame.bind(window)
    : (callback) => window.setTimeout(callback, 0);
  scheduler(() => {
    flushMojibakeNormalizationQueue();
  });
}

function installMojibakeGuard() {
  if (mojibakeGuardInstalled || !document.documentElement) {
    return;
  }
  mojibakeGuardInstalled = true;
  scheduleMojibakeNormalization(document.documentElement);
  if (typeof MutationObserver !== 'function') {
    return;
  }
  mojibakeObserver = new MutationObserver((mutations) => {
    mutations.forEach((mutation) => {
      mutation.addedNodes.forEach((node) => {
        if (node?.nodeType === Node.ELEMENT_NODE || node?.nodeType === Node.TEXT_NODE) {
          scheduleMojibakeNormalization(node);
        }
      });
    });
  });
  mojibakeObserver.observe(document.documentElement, {
    childList: true,
    subtree: true,
  });
}

function setStatusText(text, online = true) {
  $('#status-text').textContent = repairMojibakeText(text);
  const connection = $('#status-connection');
  connection.textContent = repairMojibakeText(online ? 'Онлайн' : 'Оффлайн');
  connection.classList.toggle('is-online', online);
}

function applyClientConfig() {
  if (CLIENT_CONFIG.browserMode) {
    document.body.dataset.clientMode = CLIENT_CONFIG.mobileMode ? 'browser-mobile' : 'browser';
  }
  if (CLIENT_CONFIG.appTitle) {
    document.title = CLIENT_CONFIG.appTitle;
  }
  if (CLIENT_CONFIG.brandTitle) {
    const brandTitle = document.querySelector('.brand-block h1');
    if (brandTitle) {
      brandTitle.textContent = CLIENT_CONFIG.brandTitle;
    }
  }
  if (CLIENT_CONFIG.disableLabels) {
    document.querySelector('[data-route="labels"]')?.classList.add('is-hidden');
    document.querySelector('#view-labels')?.classList.add('is-hidden');
    if (state.route === 'labels') {
      state.route = 'orders';
    }
  }
  if (CLIENT_CONFIG.disablePrinting) {
    document.querySelector('#download-print-btn')?.classList.add('is-hidden');
    document.querySelector('#labels-preview-btn')?.classList.add('is-hidden');
    document.querySelector('#labels-print-btn')?.classList.add('is-hidden');
  }
}

let mobileViewportGuardInstalled = false;

function updateMobileViewportMetrics() {
  if (!CLIENT_CONFIG.mobileMode) {
    return;
  }
  const vv = window.visualViewport;
  const viewportHeight = Math.max(320, Math.round(vv?.height || window.innerHeight || 0));
  document.documentElement.style.setProperty('--app-height', `${viewportHeight}px`);
  const keyboardOpen = Boolean(vv && window.innerHeight && vv.height < window.innerHeight * 0.82);
  document.body.dataset.keyboardOpen = keyboardOpen ? 'true' : 'false';
}

function scrollFocusedFieldIntoView(target) {
  if (!CLIENT_CONFIG.mobileMode || !target) {
    return;
  }
  const focusHost = target.closest('.field, .panel, .table-host, .inline-actions') || target;
  const scrollAction = () => {
    try {
      focusHost.scrollIntoView({ behavior: 'smooth', block: 'center', inline: 'nearest' });
    } catch (error) {
      try {
        focusHost.scrollIntoView();
      } catch (_unused) {
        return;
      }
    }
  };
  window.setTimeout(scrollAction, 80);
  window.setTimeout(scrollAction, 260);
}

function installMobileViewportGuard() {
  if (!CLIENT_CONFIG.mobileMode || mobileViewportGuardInstalled) {
    return;
  }
  mobileViewportGuardInstalled = true;
  updateMobileViewportMetrics();

  const vv = window.visualViewport;
  if (vv) {
    vv.addEventListener('resize', updateMobileViewportMetrics);
    vv.addEventListener('scroll', updateMobileViewportMetrics);
  }
  window.addEventListener('resize', updateMobileViewportMetrics);
  window.addEventListener('orientationchange', () => {
    window.setTimeout(updateMobileViewportMetrics, 120);
  });

  document.addEventListener('focusin', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (!target.matches('input, select, textarea')) {
      return;
    }
    target.closest('.field')?.classList.add('is-active');
    scrollFocusedFieldIntoView(target);
  });

  document.addEventListener('focusout', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    target.closest('.field')?.classList.remove('is-active');
    window.setTimeout(updateMobileViewportMetrics, 120);
  });
}

function showToast(message, type = 'success', durationMs = null) {
  const host = $('#toast-host');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = repairMojibakeText(message);
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
  const line = `[${timestamp}] ${repairMojibakeText(message)}`;
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

function captureTableScrollState(container) {
  if (!container) {
    return null;
  }
  const scrollHost = container.querySelector('.table-wrapper') || container;
  return {
    top: scrollHost.scrollTop || 0,
    left: scrollHost.scrollLeft || 0,
  };
}

function restoreTableScrollState(container, snapshot) {
  if (!container || !snapshot) {
    return;
  }
  const scrollHost = container.querySelector('.table-wrapper') || container;
  scrollHost.scrollTop = snapshot.top || 0;
  scrollHost.scrollLeft = snapshot.left || 0;
}

function isEditableElement(target) {
  if (!target || !(target instanceof Element)) {
    return false;
  }
  return Boolean(target.closest('input, textarea, [contenteditable="true"], [contenteditable="plaintext-only"]'));
}

function matchesShortcutKey(event, code, fallbackKeys = []) {
  if (!event) {
    return false;
  }
  if (String(event.code || '').toLowerCase() === String(code || '').toLowerCase()) {
    return true;
  }
  const normalizedKey = String(event.key || '').toLowerCase();
  return fallbackKeys.some((key) => normalizedKey === String(key || '').toLowerCase());
}

function getEditableTarget(target) {
  if (!target || !(target instanceof Element)) {
    return null;
  }
  return target.closest('input, textarea, [contenteditable="true"], [contenteditable="plaintext-only"]');
}

function ensureDesktopContextMenu() {
  if (desktopContextMenu) {
    return desktopContextMenu;
  }

  desktopContextMenu = document.createElement('div');
  desktopContextMenu.className = 'desktop-context-menu is-hidden';
  desktopContextMenu.innerHTML = `
    <button type="button" data-action="details">Подробнее</button>
    <button type="button" data-action="sign-tsd-intro">Подписать и ввести в оборот</button>
    <button type="button" data-action="find">Найти</button>
    <button type="button" data-action="cut">Вырезать</button>
    <button type="button" data-action="copy">Копировать</button>
    <button type="button" data-action="paste">Вставить</button>
    <button type="button" data-action="select-all">Выделить всё</button>
  `;
  desktopContextMenu.addEventListener('click', async (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button || button.disabled) {
      return;
    }
    await runDesktopContextAction(button.dataset.action, desktopContextMenuTarget);
    hideDesktopContextMenu();
  });
  document.body.appendChild(desktopContextMenu);
  return desktopContextMenu;
}

function findKnownOrderRow(documentId) {
  const normalizedId = String(documentId || '').trim();
  if (!normalizedId) {
    return null;
  }
  const collections = [
    state.orders.sessionOrders,
    state.orders.history,
    state.orders.deletedOrders,
    state.download.items,
    state.intro.items,
    state.tsd.items,
    state.labels.orders,
  ];
  for (const collection of collections) {
    const found = (collection || []).find((item) => {
      return String(item?.document_id || '').trim() === normalizedId
        || String(item?.kontur_document_id || '').trim() === normalizedId
        || String(item?.introduction_document_id || '').trim() === normalizedId;
    });
    if (found) {
      return found;
    }
  }
  return null;
}

function getContextDocumentId(target) {
  if (!target || !(target instanceof Element)) {
    return '';
  }
  const row = target.closest('tr[data-row-id]');
  const rowId = String(row?.dataset?.rowId || '').trim();
  if (!rowId) {
    return '';
  }
  const knownRow = findKnownOrderRow(rowId);
  return String(knownRow?.document_id || rowId || '').trim();
}

function hideDesktopContextMenu() {
  if (!desktopContextMenu) {
    return;
  }
  desktopContextMenu.classList.add('is-hidden');
  desktopContextMenuTarget = null;
}

function showDesktopContextMenu(clientX, clientY, target) {
  const menu = ensureDesktopContextMenu();
  const editableTarget = getEditableTarget(target);
  const hasSelection = hasActiveTextSelection();
  const detailsDocumentId = getContextDocumentId(target);
  const knownRow = findKnownOrderRow(detailsDocumentId);
  desktopContextMenuTarget = editableTarget || target || null;

  menu.querySelectorAll('button[data-action]').forEach((button) => {
    const action = button.dataset.action;
    let enabled = false;
    if (action === 'details') {
      enabled = Boolean(detailsDocumentId);
      button.dataset.documentId = detailsDocumentId;
    } else if (action === 'sign-tsd-intro') {
      enabled = Boolean(detailsDocumentId) && state.route === 'tsd' && Boolean(knownRow);
      button.dataset.documentId = detailsDocumentId;
    } else if (action === 'find') {
      enabled = true;
    } else if (action === 'copy') {
      enabled = Boolean(editableTarget || hasSelection);
    } else if (action === 'select-all') {
      enabled = Boolean(editableTarget || document.body);
    } else if (action === 'paste') {
      enabled = Boolean(editableTarget);
    } else if (action === 'cut') {
      enabled = Boolean(editableTarget);
    }
    button.disabled = !enabled;
  });

  menu.classList.remove('is-hidden');
  const { innerWidth, innerHeight } = window;
  const menuRect = menu.getBoundingClientRect();
  const nextLeft = Math.max(8, Math.min(clientX, innerWidth - menuRect.width - 8));
  const nextTop = Math.max(8, Math.min(clientY, innerHeight - menuRect.height - 8));
  menu.style.left = `${nextLeft}px`;
  menu.style.top = `${nextTop}px`;
}

function dispatchEditableInput(target) {
  if (!target) {
    return;
  }
  target.dispatchEvent(new Event('input', { bubbles: true }));
  target.dispatchEvent(new Event('change', { bubbles: true }));
}

async function readClipboardTextValue() {
  if (window.pywebview?.api?.read_clipboard_text) {
    const result = await API.call('read_clipboard_text');
    return typeof result === 'string' ? result : String(result?.text || '');
  }
  if (navigator.clipboard?.readText) {
    return navigator.clipboard.readText();
  }
  throw new Error('Clipboard API unavailable');
}

async function writeClipboardTextValue(text) {
  const normalizedText = String(text ?? '');
  if (window.pywebview?.api?.write_clipboard_text) {
    await API.call('write_clipboard_text', normalizedText);
    return;
  }
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(normalizedText);
    return;
  }
  throw new Error('Clipboard API unavailable');
}

function getEditableSelectionText(target) {
  if (!target) {
    return '';
  }
  if (typeof target.selectionStart === 'number' && typeof target.selectionEnd === 'number') {
    return String(target.value || '').slice(target.selectionStart, target.selectionEnd);
  }
  if (target.isContentEditable) {
    const selection = window.getSelection?.();
    return selection ? String(selection.toString() || '') : '';
  }
  return '';
}

function deleteEditableSelection(target) {
  if (!target) {
    return;
  }
  if (typeof target.setRangeText === 'function' && Number.isInteger(target.selectionStart) && Number.isInteger(target.selectionEnd)) {
    target.setRangeText('', target.selectionStart, target.selectionEnd, 'start');
    dispatchEditableInput(target);
    return;
  }
  if (target.isContentEditable) {
    const selection = window.getSelection?.();
    if (selection?.rangeCount) {
      const range = selection.getRangeAt(0);
      range.deleteContents();
      selection.removeAllRanges();
      selection.addRange(range);
      dispatchEditableInput(target);
    }
  }
}

async function pasteIntoEditableTarget(target) {
  if (!target) {
    return;
  }
  let text = '';
  try {
    text = await readClipboardTextValue();
  } catch (_error) {
    const manualText = window.prompt('Вставьте текст');
    if (manualText == null) {
      return;
    }
    text = manualText;
  }

  target.focus({ preventScroll: true });
  if (typeof target.setRangeText === 'function' && Number.isInteger(target.selectionStart) && Number.isInteger(target.selectionEnd)) {
    target.setRangeText(text, target.selectionStart, target.selectionEnd, 'end');
    dispatchEditableInput(target);
    return;
  }

  document.execCommand('insertText', false, text);
  dispatchEditableInput(target);
}

async function copySelectionToClipboard(target) {
  const text = target ? getEditableSelectionText(target) : String(window.getSelection?.() || '');
  if (!text) {
    return;
  }
  await writeClipboardTextValue(text);
}

async function cutSelectionToClipboard(target) {
  if (!target) {
    return;
  }
  const text = getEditableSelectionText(target);
  if (!text) {
    return;
  }
  await writeClipboardTextValue(text);
  deleteEditableSelection(target);
}

function selectAllForTarget(target) {
  if (target?.select) {
    target.focus({ preventScroll: true });
    target.select();
    return;
  }
  document.execCommand('selectAll');
}

function ensureOrderDetailsModal() {
  if (orderDetailsModal) {
    return orderDetailsModal;
  }
  orderDetailsModal = document.createElement('div');
  orderDetailsModal.className = 'order-details-overlay is-hidden';
  orderDetailsModal.innerHTML = `
    <div class="order-details-dialog" role="dialog" aria-modal="true" aria-labelledby="order-details-title">
      <div class="order-details-header">
        <div>
          <h3 id="order-details-title">Подробнее о заказе</h3>
          <p id="order-details-subtitle">Метаданные из Контура и локальной истории</p>
        </div>
        <button class="secondary-btn icon-btn" type="button" data-details-close aria-label="Закрыть">×</button>
      </div>
      <div class="order-details-content" id="order-details-content"></div>
    </div>
  `;
  orderDetailsModal.addEventListener('click', (event) => {
    if (event.target === orderDetailsModal || event.target.closest('[data-details-close]')) {
      hideOrderDetailsModal();
    }
  });
  document.body.appendChild(orderDetailsModal);
  return orderDetailsModal;
}

function hideOrderDetailsModal() {
  if (orderDetailsModal) {
    orderDetailsModal.classList.add('is-hidden');
  }
}

function groupDetailsFields(fields) {
  const sections = [
    { title: 'Основное', fields: [] },
    { title: 'Позиции', fields: [] },
    { title: 'События', fields: [] },
    { title: 'Связанные документы', fields: [] },
    { title: 'Локальные файлы', fields: [] },
    { title: 'Остальное', fields: [] },
  ];
  const pickSection = (field) => {
    const label = String(field?.label || '').toLowerCase();
    const rawKey = String(field?.raw_key || '').toLowerCase();
    if (label.startsWith('позиция') || rawKey.startsWith('pos_')) return sections[1];
    if (label.startsWith('событие') || rawKey.startsWith('event_')) return sections[2];
    if (label.includes('связанный') || rawKey.startsWith('related')) return sections[3];
    if (rawKey.includes('path') || label.includes('путь') || label.includes('локальный')) return sections[4];
    if (sections[0].fields.length < 16) return sections[0];
    return sections[5];
  };
  (fields || []).forEach((field) => pickSection(field).fields.push(field));
  return sections.filter((section) => section.fields.length);
}

function renderOrderDetails(payload) {
  const modal = ensureOrderDetailsModal();
  const content = modal.querySelector('#order-details-content');
  const subtitle = modal.querySelector('#order-details-subtitle');
  const fields = Array.isArray(payload?.fields) ? payload.fields : [];
  subtitle.textContent = payload?.document_id ? `Документ ${payload.document_id}` : 'Метаданные из Контура и локальной истории';
  if (!fields.length) {
    content.innerHTML = '<div class="table-empty">Метаданные по заказу не найдены.</div>';
    return;
  }
  content.innerHTML = groupDetailsFields(fields).map((section) => `
    <section class="order-details-section">
      <h4>${escapeHtml(section.title)}</h4>
      <dl>
        ${section.fields.map((field) => `
          <div>
            <dt>${escapeHtml(field.label || '')}</dt>
            <dd>${escapeHtml(field.value || '—')}</dd>
          </div>
        `).join('')}
      </dl>
    </section>
  `).join('');
}

async function openOrderDetails(documentId) {
  const normalizedId = String(documentId || '').trim();
  if (!normalizedId || state.ui.detailsLoading) {
    return;
  }
  state.ui.detailsLoading = true;
  const modal = ensureOrderDetailsModal();
  modal.classList.remove('is-hidden');
  modal.querySelector('#order-details-content').innerHTML = '<div class="table-empty">Загружаем метаданные...</div>';
  try {
    const payload = await API.call('get_order_details', normalizedId);
    if (payload?.success === false) {
      throw new Error(payload.error || 'Не удалось получить метаданные заказа.');
    }
    renderOrderDetails(payload);
  } catch (error) {
    modal.querySelector('#order-details-content').innerHTML = `<div class="table-empty">${escapeHtml(error.message)}</div>`;
    showToast(error.message, 'error');
  } finally {
    state.ui.detailsLoading = false;
  }
}

function ensureDesktopFindPanel() {
  if (desktopFindPanel) {
    return desktopFindPanel;
  }

  desktopFindPanel = document.createElement('div');
  desktopFindPanel.className = 'desktop-find-panel is-hidden';
  desktopFindPanel.innerHTML = `
    <div class="desktop-find-panel__dialog">
      <strong class="desktop-find-panel__title">Поиск</strong>
      <input type="text" class="desktop-find-panel__input" data-find-input placeholder="Найти на странице">
      <div class="desktop-find-panel__actions">
        <button type="button" data-find-nav="prev" aria-label="Назад">↑</button>
        <button type="button" data-find-nav="next" aria-label="Вперёд">↓</button>
        <button type="button" data-find-nav="close" aria-label="Закрыть">×</button>
      </div>
    </div>
  `;
  desktopFindPanel.addEventListener('input', (event) => {
    const input = event.target.closest('[data-find-input]');
    if (!input) {
      return;
    }
    state.ui.findQuery = String(input.value || '').trim();
  });
  desktopFindPanel.addEventListener('keydown', (event) => {
    const input = event.target.closest('[data-find-input]');
    if (!input) {
      return;
    }
    if (event.key === 'Enter') {
      event.preventDefault();
      repeatDesktopFind(Boolean(event.shiftKey));
      return;
    }
    if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
      event.preventDefault();
      repeatDesktopFind(event.key === 'ArrowUp');
    }
  });
  desktopFindPanel.addEventListener('click', (event) => {
    const navButton = event.target.closest('button[data-find-nav]');
    if (!navButton) {
      return;
    }
    const action = navButton.dataset.findNav;
    if (action === 'close') {
      hideDesktopFindPanel();
      return;
    }
    repeatDesktopFind(action === 'prev');
  });
  document.body.appendChild(desktopFindPanel);
  return desktopFindPanel;
}

function hideDesktopFindPanel() {
  if (!desktopFindPanel) {
    return;
  }
  desktopFindPanel.classList.add('is-hidden');
}

function getDesktopFindInput() {
  return ensureDesktopFindPanel().querySelector('[data-find-input]');
}

function runBrowserFind(query, backwards = false) {
  const normalizedQuery = String(query || '').trim();
  if (!normalizedQuery) {
    return false;
  }
  if (typeof window.find !== 'function') {
    showToast('Поиск браузера в этом режиме недоступен.', 'error');
    return false;
  }
  state.ui.findQuery = normalizedQuery;
  return Boolean(window.find(normalizedQuery, false, backwards, true, false, false, false));
}

function openDesktopFind() {
  const panel = ensureDesktopFindPanel();
  const input = getDesktopFindInput();
  const initialValue = state.ui.findQuery || String(window.getSelection?.() || '').trim();
  panel.classList.remove('is-hidden');
  if (input) {
    input.value = initialValue;
    input.focus({ preventScroll: true });
    input.select();
  }
}

function repeatDesktopFind(backwards = false) {
  const input = getDesktopFindInput();
  const query = String(input?.value || state.ui.findQuery || '').trim();
  if (!query) {
    openDesktopFind();
    return;
  }
  const found = runBrowserFind(query, backwards);
  if (!found) {
    showToast(`Не найдено: ${query}`, 'info');
  }
}

async function runDesktopContextAction(action, target) {
  const editableTarget = getEditableTarget(target);
	  if (action === 'details') {
	    await openOrderDetails(getContextDocumentId(target));
	    return;
	  }
	  if (action === 'sign-tsd-intro') {
	    const documentId = getContextDocumentId(target);
	    if (!documentId) {
	      showToast('Выберите заказ для подписи.', 'error');
	      return;
	    }
	    if (!window.confirm('Подписать и ввести в оборот?')) {
	      return;
	    }
	    await runAction('Подписываем и вводим в оборот...', async () => {
	      const result = await API.call('sign_tsd_introduction', documentId);
	      if (result?.success === false) {
	        throw new Error(result.error || 'Не удалось подписать и ввести в оборот.');
	      }
	      state.tsd.selectedIds = new Set([documentId]);
	      if (result?.state?.items) {
	        state.tsd.items = result.state.items;
	      } else {
	        await loadTsdState({ force: true, live: true });
	      }
	      markRoutesDirty(['orders', 'download', 'intro']);
	      return result;
	    }, 'Документ подписан и отправлен в ГИС МТ.');
	    return;
	  }
	  if (action === 'find') {
    openDesktopFind();
    return;
  }
  if (action === 'paste') {
    await pasteIntoEditableTarget(editableTarget);
    return;
  }
  if (action === 'select-all') {
    selectAllForTarget(editableTarget);
    return;
  }
  if (editableTarget) {
    editableTarget.focus({ preventScroll: true });
  }
  if (action === 'copy') {
    await copySelectionToClipboard(editableTarget);
    return;
  }
  if (action === 'cut') {
    await cutSelectionToClipboard(editableTarget);
  }
}

function installDesktopInteractionFallbacks() {
  if (interactionFallbacksInstalled || CLIENT_CONFIG.browserMode || CLIENT_CONFIG.mobileMode) {
    return;
  }
  interactionFallbacksInstalled = true;

  document.addEventListener('keydown', (event) => {
    const editableTarget = getEditableTarget(event.target);
    const findPanelVisible = desktopFindPanel && !desktopFindPanel.classList.contains('is-hidden');

    if ((event.ctrlKey || event.metaKey) && matchesShortcutKey(event, 'KeyF', ['f', 'а'])) {
      event.preventDefault();
      openDesktopFind();
      return;
    }

    if ((event.ctrlKey || event.metaKey) && editableTarget) {
      if (matchesShortcutKey(event, 'KeyA', ['a', 'ф'])) {
        event.preventDefault();
        selectAllForTarget(editableTarget);
        return;
      }
      if (matchesShortcutKey(event, 'KeyC', ['c', 'с']) || matchesShortcutKey(event, 'KeyX', ['x', 'ч'])) {
        event.preventDefault();
        editableTarget.focus({ preventScroll: true });
        const command = matchesShortcutKey(event, 'KeyC', ['c', 'с']) ? 'copy' : 'cut';
        const action = command === 'copy' ? copySelectionToClipboard(editableTarget) : cutSelectionToClipboard(editableTarget);
        Promise.resolve(action).catch(() => null);
        return;
      }
      if (matchesShortcutKey(event, 'KeyV', ['v', 'м'])) {
        event.preventDefault();
        pasteIntoEditableTarget(editableTarget).catch(() => null);
        return;
      }
    }

    if (event.key === 'F3') {
      event.preventDefault();
      repeatDesktopFind(Boolean(event.shiftKey));
      return;
    }

    if (findPanelVisible && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
      event.preventDefault();
      repeatDesktopFind(event.key === 'ArrowUp');
      return;
    }

    if (findPanelVisible && event.key === 'Enter') {
      event.preventDefault();
      repeatDesktopFind(Boolean(event.shiftKey));
      return;
    }

    if (event.key === 'Escape') {
      if (state.labels.fullscreenTable) {
        state.labels.fullscreenTable = '';
        renderLabelsFullscreenTable();
      }
      if (state.orders.fullscreenTable) {
        state.orders.fullscreenTable = '';
        renderOrdersFullscreenTable();
      }
      hideOrderDetailsModal();
      hideDesktopFindPanel();
      hideDesktopContextMenu();
      return;
    }

    if (event.key === 'ContextMenu' || (event.shiftKey && event.key === 'F10')) {
      const activeElement = document.activeElement instanceof Element ? document.activeElement : document.body;
      const rect = activeElement.getBoundingClientRect?.() || { left: 24, top: 24, width: 0, height: 0 };
      event.preventDefault();
      showDesktopContextMenu(rect.left + Math.min(rect.width, 24), rect.top + Math.min(rect.height, 24), activeElement);
    }
  }, true);

  document.addEventListener('contextmenu', (event) => {
    showDesktopContextMenu(event.clientX, event.clientY, event.target);
    event.preventDefault();
  }, true);

  document.addEventListener('pointerdown', (event) => {
    if (!desktopContextMenu || desktopContextMenu.classList.contains('is-hidden')) {
      return;
    }
    if (event.target instanceof Element && event.target.closest('.desktop-context-menu')) {
      return;
    }
    hideDesktopContextMenu();
  }, true);
  document.addEventListener('pointerdown', (event) => {
    if (!desktopFindPanel || desktopFindPanel.classList.contains('is-hidden')) {
      return;
    }
    if (event.target === desktopFindPanel) {
      hideDesktopFindPanel();
    }
  }, true);
  window.addEventListener('blur', hideDesktopContextMenu);
  window.addEventListener('blur', hideDesktopFindPanel);
  window.addEventListener('scroll', hideDesktopContextMenu, true);
  window.addEventListener('scroll', hideDesktopFindPanel, true);
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

  const scrollState = captureTableScrollState(container);
  const selectedSet = selectedIds instanceof Set ? selectedIds : new Set(selectedIds ? [selectedIds] : []);
  const html = `
    <div class="table-wrapper ${compact ? 'is-compact' : ''}" ${maxHeight ? `style="max-height:${escapeHtml(maxHeight)}"` : ''}>
      <table class="table ${compact ? 'table-compact' : ''}">
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column.label)}</th>`).join('')}</tr>
        </thead>
        <tbody>
          ${rows.map((row, rowIndex) => {
            const id = String(rowId(row) ?? '');
            const selected = selectedSet.has(id);
            return `
              <tr data-row-id="${escapeHtml(id)}" data-row-index="${rowIndex}" class="${selected ? 'is-selected' : ''}">
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
  restoreTableScrollState(container, scrollState);

  if (!onRowClick) {
    return;
  }

  container.querySelectorAll('tbody tr[data-row-id]').forEach((rowEl) => {
    rowEl.addEventListener('click', (event) => {
      if (event.target.closest('button, a, input, select, textarea, label')) {
        return;
      }
      if (event.ctrlKey || event.metaKey || event.shiftKey) {
        event.preventDefault();
      }
      if (hasActiveTextSelection()) {
        return;
      }
      const id = rowEl.dataset.rowId;
      const rowIndex = Number(rowEl.dataset.rowIndex || 0);
      if (single) {
        onRowClick(id, undefined, event, rowIndex);
      } else {
        onRowClick(id, rowEl.classList.contains('is-selected'), event, rowIndex);
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
  const scrollState = captureTableScrollState(container);

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
  restoreTableScrollState(container, scrollState);
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

function renderOrderStatusWithIntro(row) {
  const baseStatus = renderStatusCell(row);
  const introRaw = String(row?.intro_status_raw || '').trim().toLowerCase();
  const introLabel = String(row?.intro_status || '').trim();
  const introduced = introRaw === 'introduced' || introLabel === 'Введены в оборот' || introLabel === 'Введен в оборот';
  return introduced
    ? `${baseStatus}<span class="cell-note cell-note-success">Введены в оборот</span>`
    : baseStatus;
}

function rowMatchesSearch(row, query) {
  const normalizedQuery = String(query || '').trim().toLowerCase();
  if (!normalizedQuery) {
    return true;
  }
  return Object.values(row || {})
    .map((value) => String(value ?? '').toLowerCase())
    .join(' ')
    .includes(normalizedQuery);
}

function getFilteredRows(rows, { query = '', status = '', statusKeys = ['status'] } = {}) {
  const normalizedStatus = String(status || '').trim();
  return (rows || []).filter((row) => {
    if (!rowMatchesSearch(row, query)) {
      return false;
    }
    if (!normalizedStatus) {
      return true;
    }
    return statusKeys.some((key) => String(row?.[key] || '').trim() === normalizedStatus);
  });
}

function getStatusFilterOptions(rows, { statusKey = 'status', placeholder = 'Все статусы' } = {}) {
  const values = [...new Set((rows || []).map((row) => String(row?.[statusKey] || '').trim()).filter(Boolean))];
  return [{ value: '', label: placeholder }].concat(values.map((value) => ({ value, label: value })));
}

function setInputValue(selector, value) {
  const element = $(selector);
  if (element && element.value !== String(value || '')) {
    element.value = String(value || '');
  }
}

function updateSelectOptions(selector, options, value) {
  const element = $(selector);
  if (!element) {
    return;
  }
  fillSelectOptions(element, options);
  element.value = [...element.options].some((option) => option.value === value) ? value : '';
}

function updateMultiSelectionFromClick(stateSlice, visibleRows, id, isSelected, event, rowIndex) {
  const normalizedId = String(id || '');
  if (!normalizedId) {
    return;
  }
  if (event?.shiftKey && Number.isInteger(stateSlice.lastClickedIndex) && stateSlice.lastClickedIndex >= 0) {
    const start = Math.min(stateSlice.lastClickedIndex, rowIndex);
    const end = Math.max(stateSlice.lastClickedIndex, rowIndex);
    visibleRows.slice(start, end + 1).forEach((row) => {
      if (row?.document_id) {
        stateSlice.selectedIds.add(row.document_id);
      }
    });
  } else if (event?.ctrlKey || event?.metaKey) {
    if (isSelected) {
      stateSlice.selectedIds.delete(normalizedId);
    } else {
      stateSlice.selectedIds.add(normalizedId);
    }
  } else {
    stateSlice.selectedIds = new Set([normalizedId]);
  }
  stateSlice.lastClickedIndex = rowIndex;
}

function formatPreview(preview) {
  if (!preview) {
    return 'Выберите шаблон, файл и заказ, затем нажмите «Показать контекст».';
  }
  const lines = [
    `Заказ: ${preview.order_name}` ,
    `Формат: ${preview.sheet_format_label || preview.sheet_format || '100x180'}` ,
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
  localStorage.setItem('kontur-ui-v2-theme-choice', theme);
  $('#theme-toggle-btn').textContent = theme === 'dark' ? 'Светлая тема' : 'Тёмная тема';
}

function ensureAggregationIntroDocumentTitleField() {
  const input = $('#agg-intro-document-title');
  const batchInput = $('#agg-intro-batch-number');
  if (!input || !batchInput) {
    return;
  }
  const field = input.closest('.field');
  const formGrid = batchInput.closest('.form-grid');
  const batchField = batchInput.closest('.field');
  if (!field || !formGrid || !batchField) {
    return;
  }
  const label = field.querySelector('span');
  if (label) {
    label.textContent = 'Название документа ввода в оборот';
  }
  input.placeholder = 'Можно оставить пустым для автоназвания';
  if (field.parentElement !== formGrid || field !== batchField.nextElementSibling) {
    batchField.insertAdjacentElement('afterend', field);
  }
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
    if (!(route in ROUTES) || (CLIENT_CONFIG.disableLabels && route === 'labels')) {
      route = 'orders';
    }
	    if (route !== 'labels') {
	      state.labels.fullscreenTable = '';
	    }
	    if (route !== 'orders') {
	      state.orders.fullscreenTable = '';
	    }
    state.route = route;
    document.querySelectorAll('.nav-item').forEach((item) => {
      item.classList.toggle('is-active', item.dataset.route === route);
    });
    document.querySelectorAll('.view').forEach((view) => {
      view.classList.toggle('is-active', view.id === `view-${route}`);
    });
    $('#view-title').textContent = ROUTES[route].title;
    $('#view-subtitle').textContent = CLIENT_CONFIG.subtitleSuffix
      ? `${ROUTES[route].subtitle} ${CLIENT_CONFIG.subtitleSuffix}`
      : ROUTES[route].subtitle;
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

	      setInputValue('#orders-history-search', state.orders.historySearch);
	      const historyRows = getFilteredRows(state.orders.history, { query: state.orders.historySearch });

	      createTable(
	        $('#orders-queue-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Товар', key: 'simpl_name' },
          { label: 'GTIN', key: 'gtin' },
          { label: 'Кодов', key: 'codes_count' },
        ],
        state.orders.queue,
        {
          single: true,
          compact: true,
          maxHeight: '180px',
          selectedIds: state.orders.selectedQueueId,
          onRowClick: (id) => {
            state.orders.selectedQueueId = id;
            Views.orders.render();
          },
        },
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
          { label: 'Статус', render: (row) => renderOrderStatusWithIntro(row) },
          { label: 'GTIN', key: 'gtin' },
        ],
        historyRows,
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
      renderOrdersFullscreenTable();

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
      if ($('#download-record-number')) {
        $('#download-record-number').value = state.download.recordNumber || '';
      }
	      updateDownloadSelectionMeta();
	      updateDownloadProgressUi();
	      setInputValue('#download-items-search', state.download.searchQuery);
	      const downloadRows = getFilteredRows(state.download.items, { query: state.download.searchQuery });

	      createTable(
        $('#download-items-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус', render: (row) => renderStatusCell(row) },
          { label: 'GTIN', key: 'gtin' },
          { label: 'Файлы', key: 'file_label' },
        ],
	        downloadRows,
        {
          compact: true,
          maxHeight: '520px',
          selectedIds: state.download.selectedIds,
	          onRowClick: (id, isSelected, event, rowIndex) => {
	            state.download.selectedItemId = id;
	            updateMultiSelectionFromClick(state.download, downloadRows, id, isSelected, event, rowIndex);
	            Views.download.render();
	          },
        },
      );
    },
  },
	  intro: {
	    render() {
	      setInputValue('#intro-items-search', state.intro.searchQuery);
	      updateSelectOptions(
	        '#intro-status-filter',
	        getStatusFilterOptions(state.intro.items, { statusKey: 'status', placeholder: 'Все статусы' }),
	        state.intro.statusFilter,
	      );
	      const introRows = getFilteredRows(
	        state.intro.items,
	        { query: state.intro.searchQuery, status: state.intro.statusFilter, statusKeys: ['status'] },
	      );
	      createTable(
        $('#intro-items-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус', render: (row) => renderStatusCell(row) },
          { label: 'GTIN', key: 'gtin' },
        ],
	        introRows,
        {
          single: true,
          compact: true,
          maxHeight: '420px',
          selectedIds: state.intro.selectedIds,
          onRowClick: (id) => {
            state.intro.selectedIds = new Set([id]);
            Views.intro.render();
          },
        },
      );
    },
  },
	  tsd: {
	    render() {
	      setInputValue('#tsd-items-search', state.tsd.searchQuery);
	      updateSelectOptions(
	        '#tsd-status-filter',
	        getStatusFilterOptions(state.tsd.items, { statusKey: 'tsd_status', placeholder: 'Все статусы' }),
	        state.tsd.statusFilter,
	      );
	      const tsdRows = getFilteredRows(
	        state.tsd.items,
	        { query: state.tsd.searchQuery, status: state.tsd.statusFilter, statusKeys: ['tsd_status'] },
	      );
	      createTable(
        $('#tsd-items-table'),
        [
          { label: 'Заявка', key: 'order_name' },
          { label: 'Полное наименование', key: 'full_name' },
          { label: 'Статус ЧЗ', render: (row) => renderStatusCell(row) },
          { label: 'На ТСД', render: (row) => statusPill(row.tsd_status) },
          { label: 'GTIN', key: 'gtin' },
        ],
	        tsdRows,
        {
          single: true,
          compact: true,
          maxHeight: '520px',
          selectedIds: state.tsd.selectedIds,
          onRowClick: (id) => {
            state.tsd.selectedIds = new Set([id]);
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
      fillSelectOptions(
        $("#labels-sheet-format-select"),
        state.labels.sheetFormats.map((item) => ({ value: item.key, label: item.label })),
        "Выберите формат",
      );
      if ($("#labels-sheet-format-select")) {
        $("#labels-sheet-format-select").value = state.labels.selectedSheetFormat || state.labels.defaultSheetFormat || '100x180';
      }
      const sheetFormatLabel = selectedLabelSheetFormatLabel();
      const heading = $("#labels-sheet-format-heading");
      if (heading) {
        heading.textContent = `Параметры печати ${sheetFormatLabel}`;
      }
      fillSelect($("#labels-printer-select"), state.labels.printers, "Выберите принтер");
      if (state.labels.selectedPrinter) {
        $("#labels-printer-select").value = state.labels.selectedPrinter;
      }
      const manualPanel = $('#labels-manual-panel');
      if (manualPanel) {
        manualPanel.classList.toggle('is-hidden', !state.labels.manualEnabled);
      }
      if ($('#labels-manual-note')) {
        $('#labels-manual-note').textContent = state.labels.manualPrompt
          || 'Если автоподбор не сработал, заполните поля вручную и повторите предпросмотр или печать.';
      }
      if ($('#labels-manual-gtin')) {
        $('#labels-manual-gtin').value = state.labels.manualFields.gtin || '';
      }
      if ($('#labels-manual-size')) {
        $('#labels-manual-size').value = state.labels.manualFields.size || '';
      }
      if ($('#labels-manual-batch')) {
        $('#labels-manual-batch').value = state.labels.manualFields.batch || '';
      }
      if ($('#labels-manual-color')) {
        $('#labels-manual-color').value = state.labels.manualFields.color || '';
      }
      if ($('#labels-manual-units')) {
        $('#labels-manual-units').value = state.labels.manualFields.units_per_pack || '';
      }

      const availableTemplates = visibleLabelTemplates();
      const templateHost = $("#labels-template-grid");
      const pageSize = state.labels.templatePageSize || 3;
      const totalPages = Math.max(1, Math.ceil(availableTemplates.length / pageSize));
      state.labels.templatePage = Math.min(state.labels.templatePage, totalPages - 1);
      const pageStart = state.labels.templatePage * pageSize;
      const visibleTemplates = availableTemplates.slice(pageStart, pageStart + pageSize);
      $("#labels-template-page").textContent = `${availableTemplates.length ? pageStart + 1 : 0}-${Math.min(pageStart + visibleTemplates.length, availableTemplates.length)} / ${availableTemplates.length}`;
      $("#labels-template-prev").disabled = state.labels.templatePage <= 0;
      $("#labels-template-next").disabled = state.labels.templatePage >= totalPages - 1;

      templateHost.innerHTML = visibleTemplates.map((template) => `
        <button class="template-card ${state.labels.selectedTemplatePath === template.path ? 'is-selected' : ''}" data-template-path="${escapeHtml(template.path)}">
          <strong>${escapeHtml(template.name)}</strong>
          <small>${escapeHtml(template.sheet_format_label || template.sheet_format || '')}</small>
          <small>${escapeHtml(template.category)}</small>
          <small>${escapeHtml(template.relative_path)}</small>
          <small>${escapeHtml(template.source_label || template.data_source_kind)}</small>
        </button>
      `).join('');
      templateHost.querySelectorAll("[data-template-path]").forEach((button) => {
        button.addEventListener("click", () => {
          state.labels.selectedTemplatePath = button.dataset.templatePath;
          resetLabelsManualState();
          invalidateLabelsPreview();
          Views.labels.render();
        });
      });

      if ($('#labels-orders-search')) {
        $('#labels-orders-search').value = state.labels.tableSearch.orders || '';
      }
      if ($('#labels-aggregation-search')) {
        $('#labels-aggregation-search').value = state.labels.tableSearch.aggregation || '';
      }
      if ($('#labels-marking-search')) {
        $('#labels-marking-search').value = state.labels.tableSearch.marking || '';
      }

      renderLabelsDataTable('orders', $("#labels-orders-table"));
      renderLabelsDataTable('aggregation', $("#labels-aggregation-files-table"));
      renderLabelsDataTable('marking', $("#labels-marking-files-table"));
      renderLabelsFullscreenTable();

      const {
        total,
        selectedRecordNumber,
        rangeStartNumber,
        rangeEndNumber,
      } = normalizeLabelsRecordSelection();
      const printScopeSelect = $("#labels-print-scope");
      const recordInput = $("#labels-record-number");
      const rangeStartInput = $("#labels-range-start");
      const rangeEndInput = $("#labels-range-end");
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
      if (rangeStartInput) {
        rangeStartInput.value = String(rangeStartNumber || 1);
        rangeStartInput.min = total > 0 ? "1" : "0";
        rangeStartInput.max = total > 0 ? String(total) : "";
        rangeStartInput.disabled = state.labels.printScope !== "range" || total <= 0;
      }
      if (rangeEndInput) {
        rangeEndInput.value = String(rangeEndNumber || 1);
        rangeEndInput.min = total > 0 ? "1" : "0";
        rangeEndInput.max = total > 0 ? String(total) : "";
        rangeEndInput.disabled = state.labels.printScope !== "range" || total <= 0;
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
    if (!state.orders.queue.some((item) => item.uid === state.orders.selectedQueueId)) {
      state.orders.selectedQueueId = '';
    }
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

function applyOrderQueueUpdate(result, options = {}) {
  if (Array.isArray(result?.queue)) {
    state.orders.queue = result.queue;
  } else if (result?.item) {
    state.orders.queue = [...state.orders.queue, result.item];
  }
  if (options.selectItem && result?.item?.uid) {
    state.orders.selectedQueueId = result.item.uid;
  }
  if (!state.orders.queue.some((item) => item.uid === state.orders.selectedQueueId)) {
    state.orders.selectedQueueId = '';
  }
  if (isRouteActive('orders')) {
    Views.orders.render();
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

function applyImmediateDownloadItem(downloadItem) {
  if (!downloadItem?.document_id) {
    return;
  }
  state.download.items = [
    downloadItem,
    ...state.download.items.filter((item) => item.document_id !== downloadItem.document_id),
  ];
  state.download.selectedItemId = downloadItem.document_id;
  state.download.selectedIds = new Set([downloadItem.document_id]);
  markRouteLoaded('download');
  if (isRouteActive('download')) {
    Views.download.render();
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
    state.labels.sheetFormats = result.sheet_formats || [];
    state.labels.defaultSheetFormat = result.default_sheet_format || state.labels.defaultSheetFormat || '100x180';
    state.labels.templates = result.templates || [];
    state.labels.aggregationFiles = result.aggregation_files || [];
    state.labels.markingFiles = result.marking_files || [];
    state.labels.orders = result.orders || [];
    state.labels.printers = result.printers || [];
    state.labels.defaultPrinter = result.default_printer || state.labels.defaultPrinter;
    const availableSheetFormatKeys = new Set(
      state.labels.sheetFormats.map((item) => String(item.key || '').trim()).filter(Boolean),
    );
    if (!availableSheetFormatKeys.has(state.labels.selectedSheetFormat)) {
      state.labels.selectedSheetFormat = availableSheetFormatKeys.has(state.labels.defaultSheetFormat)
        ? state.labels.defaultSheetFormat
        : (state.labels.sheetFormats[0]?.key || state.labels.defaultSheetFormat || '100x180');
    }
    if (!state.labels.printers.includes(state.labels.selectedPrinter)) {
      if (state.labels.defaultPrinter && state.labels.printers.includes(state.labels.defaultPrinter)) {
        state.labels.selectedPrinter = state.labels.defaultPrinter;
      } else {
        state.labels.selectedPrinter = state.labels.printers[0] || '';
      }
    }
    const availableTemplates = visibleLabelTemplates();
    if (!availableTemplates.some((item) => item.path === state.labels.selectedTemplatePath)) {
      state.labels.selectedTemplatePath = availableTemplates[0]?.path || '';
    }
    if (state.labels.selectedTemplatePath) {
      const currentIndex = availableTemplates.findIndex((item) => item.path === state.labels.selectedTemplatePath);
      state.labels.templatePage = currentIndex >= 0 ? Math.floor(currentIndex / state.labels.templatePageSize) : 0;
    } else {
      state.labels.templatePage = 0;
    }
    if (!state.labels.orders.some((item) => item.document_id === state.labels.selectedOrderId)) {
      state.labels.selectedOrderId = state.labels.orders[0]?.document_id || '';
      resetLabelsManualState();
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

function visibleLabelTemplates() {
  const selectedSheetFormat = String(state.labels.selectedSheetFormat || state.labels.defaultSheetFormat || '100x180').trim();
  return state.labels.templates.filter(
    (item) => String(item.sheet_format || state.labels.defaultSheetFormat || '100x180').trim() === selectedSheetFormat,
  );
}

function selectedLabelSheetFormatLabel() {
  const selectedSheetFormat = String(state.labels.selectedSheetFormat || state.labels.defaultSheetFormat || '100x180').trim();
  const selectedFormat = state.labels.sheetFormats.find((item) => String(item.key || '').trim() === selectedSheetFormat);
  return selectedFormat?.label || selectedSheetFormat || '100x180';
}

function selectedTemplate() {
  return visibleLabelTemplates().find((item) => item.path === state.labels.selectedTemplatePath) || null;
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

function selectedLabelsOrder() {
  return state.labels.orders.find((item) => item.document_id === state.labels.selectedOrderId) || null;
}

function getLabelsTableConfig(tableKey) {
  if (tableKey === 'orders') {
    return {
      title: 'Заказы',
      rows: state.labels.orders,
      columns: [
        { label: 'Заявка', key: 'order_name' },
        { label: 'Полное наименование', key: 'full_name' },
        { label: 'GTIN', key: 'gtin' },
        { label: 'Размер', key: 'size' },
        { label: 'Партия', key: 'batch' },
      ],
      rowId: (row) => row.document_id,
      selectedIds: state.labels.selectedOrderId,
      onRowClick: (id) => {
        state.labels.selectedOrderId = id;
        resetLabelsManualState();
        invalidateLabelsPreview();
        Views.labels.render();
      },
    };
  }

  if (tableKey === 'aggregation') {
    return {
      title: 'Агрег коды км',
      rows: state.labels.aggregationFiles,
      columns: [
        { label: 'Файл', key: 'name' },
        { label: 'Папка', key: 'folder_name' },
        { label: 'Строк', key: 'record_count' },
      ],
      rowId: (row) => row.path,
      selectedIds: state.labels.selectedAggregationPath,
      onRowClick: (id) => {
        state.labels.selectedAggregationPath = id;
        resetLabelsManualState();
        invalidateLabelsPreview();
        Views.labels.render();
      },
    };
  }

  return {
    title: 'Коды км',
    rows: state.labels.markingFiles,
    columns: [
      { label: 'Файл', key: 'name' },
      { label: 'Папка', key: 'folder_name' },
      { label: 'Строк', key: 'record_count' },
    ],
    rowId: (row) => row.path,
    selectedIds: state.labels.selectedMarkingPath,
    onRowClick: (id) => {
      state.labels.selectedMarkingPath = id;
      resetLabelsManualState();
      invalidateLabelsPreview();
      Views.labels.render();
    },
  };
}

function filterLabelsTableRows(tableKey, rows) {
  const query = String(state.labels.tableSearch?.[tableKey] || '').trim().toLowerCase();
  if (!query) {
    return rows;
  }
  return (rows || []).filter((row) => {
    const haystack = Object.values(row || {})
      .map((value) => String(value ?? '').toLowerCase())
      .join(' ');
    return haystack.includes(query);
  });
}

function renderLabelsDataTable(tableKey, container, { maxHeight = '260px' } = {}) {
  const config = getLabelsTableConfig(tableKey);
  const filteredRows = filterLabelsTableRows(tableKey, config.rows);
  createTable(
    container,
    config.columns,
    filteredRows,
    {
      single: true,
      compact: true,
      maxHeight,
      selectedIds: config.selectedIds,
      rowId: config.rowId,
      onRowClick: config.onRowClick,
    },
  );
}

function renderLabelsFullscreenTable() {
  const overlay = $('#table-fullscreen-overlay');
  const host = $('#table-fullscreen-host');
  const title = $('#table-fullscreen-title');
  const searchInput = $('#table-fullscreen-search');
  const tableKey = String(state.labels.fullscreenTable || '').trim();
  if (!overlay || !host || !title || !searchInput) {
    return;
  }

  if (!tableKey) {
    overlay.classList.add('is-hidden');
    return;
  }

  const config = getLabelsTableConfig(tableKey);
  title.textContent = config.title;
  searchInput.value = state.labels.tableSearch?.[tableKey] || '';
  overlay.classList.remove('is-hidden');
  renderLabelsDataTable(tableKey, host, { maxHeight: '72vh' });
}

function updateLabelsTableSearch(tableKey, value) {
  state.labels.tableSearch[tableKey] = String(value || '');
  Views.labels.render();
}

function renderOrdersFullscreenTable() {
  const overlay = $('#table-fullscreen-overlay');
  const host = $('#table-fullscreen-host');
  const title = $('#table-fullscreen-title');
  const searchInput = $('#table-fullscreen-search');
  if (!overlay || !host || !title || !searchInput) {
    return;
  }
  if (state.orders.fullscreenTable !== 'history') {
    if (!state.labels.fullscreenTable) {
      overlay.classList.add('is-hidden');
    }
    return;
  }
  title.textContent = 'История заказов';
  searchInput.value = state.orders.historySearch || '';
  overlay.classList.remove('is-hidden');
  const rows = getFilteredRows(state.orders.history, { query: state.orders.historySearch });
  createTable(
    host,
    [
      { label: 'Заявка', key: 'order_name' },
      { label: 'Полное наименование', key: 'full_name' },
      { label: 'Статус', render: (row) => renderOrderStatusWithIntro(row) },
      { label: 'GTIN', key: 'gtin' },
    ],
    rows,
    {
      single: true,
      compact: true,
      maxHeight: '72vh',
      selectedIds: state.orders.selectedHistoryId,
      onRowClick: (id) => {
        state.orders.selectedHistoryId = id;
        Views.orders.render();
      },
    },
  );
}

function resetLabelsManualState() {
  state.labels.manualPrompt = '';
  state.labels.manualEnabled = false;
  state.labels.manualFields = {
    gtin: '',
    size: '',
    batch: '',
    color: '',
    units_per_pack: '',
  };
}

function applyLabelsManualForm(payload = {}) {
  const fields = payload?.fields || {};
  state.labels.manualPrompt = payload?.prompt || 'Заполните форму вручную и повторите действие.';
  state.labels.manualEnabled = true;
  state.labels.manualFields = {
    gtin: String(fields.gtin || ''),
    size: String(fields.size || ''),
    batch: String(fields.batch || ''),
    color: String(fields.color || ''),
    units_per_pack: String(fields.units_per_pack || ''),
  };
}

function readLabelsManualOverride() {
  if (!state.labels.manualEnabled) {
    return null;
  }
  return {
    enabled: true,
    gtin: $('#labels-manual-gtin')?.value?.trim?.() || '',
    size: $('#labels-manual-size')?.value?.trim?.() || '',
    batch: $('#labels-manual-batch')?.value?.trim?.() || '',
    color: $('#labels-manual-color')?.value?.trim?.() || '',
    units_per_pack: $('#labels-manual-units')?.value?.trim?.() || '',
  };
}

function invalidateLabelsPreview() {
  state.labels.preview = null;
}

function formatPreview(preview) {
  if (!preview) {
    return 'Р’С‹Р±РµСЂРёС‚Рµ С€Р°Р±Р»РѕРЅ, С„Р°Р№Р» Рё Р·Р°РєР°Р·, Р·Р°С‚РµРј РЅР°Р¶РјРёС‚Рµ В«РџРѕРєР°Р·Р°С‚СЊ РєРѕРЅС‚РµРєСЃС‚В».';
  }
  const lines = [
    `Р—Р°РєР°Р·: ${preview.order_name}` ,
    `Р¤РѕСЂРјР°С‚: ${preview.sheet_format_label || preview.sheet_format || '100x180'}` ,
    `РЁР°Р±Р»РѕРЅ: ${preview.template_category} / ${preview.data_source_kind}` ,
    `Р РµР¶РёРј РїРµС‡Р°С‚Рё: ${preview.print_scope_label || 'Р’РµСЃСЊ С„Р°Р№Р»'}` ,
    `Р Р°Р·РјРµСЂ: ${preview.size}` ,
    `РџР°СЂС‚РёСЏ: ${preview.batch}` ,
    `Р¦РІРµС‚: ${preview.color || 'вЂ”'}` ,
    `Р”Р°С‚Р° РёР·РіРѕС‚РѕРІР»РµРЅРёСЏ: ${preview.manufacture_date}` ,
    `РЎСЂРѕРє РіРѕРґРЅРѕСЃС‚Рё: ${preview.expiration_date}` ,
    `РљРѕР»РёС‡РµСЃС‚РІРѕ: ${preview.quantity_pairs} ${preview.quantity_pairs_word}` ,
    `РЈРїР°РєРѕРІРєР°: ${preview.package_text || 'РЅРµ РёСЃРїРѕР»СЊР·СѓРµС‚СЃСЏ'}` ,
    `Р­С‚РёРєРµС‚РѕРє Рє РїРµС‡Р°С‚Рё: ${preview.label_count}` ,
  ];
  if (preview.total_record_count) {
    lines.push(`Р—Р°РїРёСЃРµР№ РІ С„Р°Р№Р»Рµ: ${preview.total_record_count}`);
  }
  if (
    preview.selected_record_number
    && preview.selected_record_end_number
    && preview.selected_record_end_number !== preview.selected_record_number
  ) {
    lines.push(`Р’С‹Р±СЂР°РЅ РґРёР°РїР°Р·РѕРЅ: ${preview.selected_record_number}вЂ“${preview.selected_record_end_number}`);
    lines.push(`Р—Р°РїРёСЃРµР№ РІ РґРёР°РїР°Р·РѕРЅРµ: ${preview.range_record_count || preview.label_count}`);
  } else if (preview.selected_record_number) {
    lines.push(`Р’С‹Р±СЂР°РЅР° Р·Р°РїРёСЃСЊ: ${preview.selected_record_number} РёР· ${preview.total_record_count || preview.label_count}`);
  }
  if (preview.selected_code_label && preview.selected_code_value_short) {
    lines.push(`${preview.selected_code_label}: ${preview.selected_code_value_short}`);
  }
  if (preview.selected_code_gtin) {
    lines.push(`GTIN РІС‹Р±СЂР°РЅРЅРѕР№ Р·Р°РїРёСЃРё: ${preview.selected_code_gtin}`);
  }
  if (preview.selected_code_name) {
    lines.push(`РќР°РёРјРµРЅРѕРІР°РЅРёРµ РІС‹Р±СЂР°РЅРЅРѕР№ Р·Р°РїРёСЃРё: ${preview.selected_code_name}`);
  }
  return lines.join('\n');
}

function normalizeLabelsRecordSelection() {
  const file = selectedLabelFileMeta();
  const total = Math.max(0, Number(file?.record_count || 0));
  let selectedRecordNumber = Math.max(1, Number.parseInt(state.labels.selectedRecordNumber || 1, 10) || 1);
  let rangeStartNumber = Math.max(1, Number.parseInt(state.labels.rangeStartNumber || 1, 10) || 1);
  let rangeEndNumber = Math.max(1, Number.parseInt(state.labels.rangeEndNumber || rangeStartNumber, 10) || rangeStartNumber);
  if (total > 0) {
    selectedRecordNumber = Math.min(selectedRecordNumber, total);
    rangeStartNumber = Math.min(rangeStartNumber, total);
    rangeEndNumber = Math.min(rangeEndNumber, total);
  }
  if (rangeEndNumber < rangeStartNumber) {
    rangeEndNumber = rangeStartNumber;
  }
  state.labels.selectedRecordNumber = selectedRecordNumber;
  state.labels.rangeStartNumber = rangeStartNumber;
  state.labels.rangeEndNumber = rangeEndNumber;
  return { file, total, selectedRecordNumber, rangeStartNumber, rangeEndNumber };
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

function normalizeLabelsRecordSelection() {
  const file = selectedLabelFileMeta();
  const total = Math.max(0, Number(file?.record_count || 0));
  let selectedRecordNumber = Math.max(1, Number.parseInt(state.labels.selectedRecordNumber || 1, 10) || 1);
  let rangeStartNumber = Math.max(1, Number.parseInt(state.labels.rangeStartNumber || 1, 10) || 1);
  let rangeEndNumber = Math.max(1, Number.parseInt(state.labels.rangeEndNumber || rangeStartNumber, 10) || rangeStartNumber);
  if (total > 0) {
    selectedRecordNumber = Math.min(selectedRecordNumber, total);
    rangeStartNumber = Math.min(rangeStartNumber, total);
    rangeEndNumber = Math.min(rangeEndNumber, total);
  }
  if (rangeEndNumber < rangeStartNumber) {
    rangeEndNumber = rangeStartNumber;
  }
  state.labels.selectedRecordNumber = selectedRecordNumber;
  state.labels.rangeStartNumber = rangeStartNumber;
  state.labels.rangeEndNumber = rangeEndNumber;
  return { file, total, selectedRecordNumber, rangeStartNumber, rangeEndNumber };
}

function labelsRecordInfoText() {
  const {
    total,
    selectedRecordNumber,
    rangeStartNumber,
    rangeEndNumber,
  } = normalizeLabelsRecordSelection();
  if (!total) {
    return 'РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРёС‚Рµ С„Р°Р№Р» СЃ РєРѕРґР°РјРё РґР»СЏ РїРµС‡Р°С‚Рё.';
  }
  if (state.labels.printScope === 'all') {
    return `РЎРµР№С‡Р°СЃ РЅР° РїРµС‡Р°С‚СЊ РїРѕР№РґС‘С‚ РІРµСЃСЊ С„Р°Р№Р»: ${total} СЌС‚РёРєРµС‚РѕРє.`;
  }
  if (state.labels.printScope === 'range') {
    let text = `РќР° РїРµС‡Р°С‚СЊ РїРѕР№РґС‘С‚ РґРёР°РїР°Р·РѕРЅ Р·Р°РїРёСЃРµР№ в„–${rangeStartNumber}вЂ“${rangeEndNumber} РёР· ${total}. Р’СЃРµРіРѕ СЌС‚РёРєРµС‚РѕРє: ${Math.max(0, rangeEndNumber - rangeStartNumber + 1)}.`;
    const rangePreview = state.labels.preview;
    if (
      rangePreview
      && rangePreview.print_scope === 'range'
      && Number(rangePreview.selected_record_number || 0) === rangeStartNumber
      && Number(rangePreview.selected_record_end_number || 0) === rangeEndNumber
      && rangePreview.selected_code_value_short
    ) {
      text += ` ${rangePreview.selected_code_label || 'РџРµСЂРІС‹Р№ РєРѕРґ'}: ${rangePreview.selected_code_value_short}.`;
    }
    return text;
  }
  let text = `Р’С‹Р±СЂР°РЅР° Р·Р°РїРёСЃСЊ в„–${selectedRecordNumber} РёР· ${total}. РќР°Р¶РјРёС‚Рµ В«РџРѕРєР°Р·Р°С‚СЊ РєРѕРЅС‚РµРєСЃС‚В», С‡С‚РѕР±С‹ РїСЂРѕРІРµСЂРёС‚СЊ РєРѕРґ РїРµСЂРµРґ РїРµС‡Р°С‚СЊСЋ.`;
  const preview = state.labels.preview;
  if (
    preview
    && preview.print_scope === 'single'
    && Number(preview.selected_record_number || 0) === selectedRecordNumber
    && preview.selected_code_value_short
  ) {
    text += ` ${preview.selected_code_label || 'РљРѕРґ'}: ${preview.selected_code_value_short}.`;
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
          <span>Р§С‚Рѕ РїРµС‡Р°С‚Р°С‚СЊ</span>
          <select id="labels-print-scope">
            <option value="all">Р’РµСЃСЊ С„Р°Р№Р»</option>
            <option value="single">РћРґРЅСѓ СЌС‚РёРєРµС‚РєСѓ РїРѕ РїРѕСЂСЏРґРєСѓ</option>
            <option value="range">Р”РёР°РїР°Р·РѕРЅ СЌС‚РёРєРµС‚РѕРє</option>
          </select>
        </label>
        <label class="field">
          <span>РќРѕРјРµСЂ СЌС‚РёРєРµС‚РєРё РїРѕ РїРѕСЂСЏРґРєСѓ</span>
          <input id="labels-record-number" type="number" min="1" step="1" placeholder="1">
        </label>
        <label class="field">
          <span>Р”РёР°РїР°Р·РѕРЅ Р·Р°РїРёСЃРµР№</span>
          <div class="inline-actions compact wrap">
            <input id="labels-range-start" type="number" min="1" step="1" placeholder="С 1">
            <input id="labels-range-end" type="number" min="1" step="1" placeholder="По 200">
          </div>
        </label>
      </div>
      <div class="inline-actions compact wrap" id="labels-record-stepper">
        <button class="secondary-btn" type="button" id="labels-record-prev">РџСЂРµРґС‹РґСѓС‰Р°СЏ</button>
        <button class="secondary-btn" type="button" id="labels-record-next">РЎР»РµРґСѓСЋС‰Р°СЏ</button>
      </div>
      <div class="inline-note" id="labels-record-info">РЎРЅР°С‡Р°Р»Р° РІС‹Р±РµСЂРёС‚Рµ С„Р°Р№Р» СЃ РєРѕРґР°РјРё РґР»СЏ РїРµС‡Р°С‚Рё.</div>
    `);
  }

  const scopeSelect = $('#labels-print-scope');
  const recordInput = $('#labels-record-number');
  const rangeStartInput = $('#labels-range-start');
  const rangeEndInput = $('#labels-range-end');
  const prevButton = $('#labels-record-prev');
  const nextButton = $('#labels-record-next');
  if (!scopeSelect || scopeSelect.dataset.bound === '1') {
    return;
  }

  scopeSelect.dataset.bound = '1';
  scopeSelect.addEventListener('change', () => {
    const nextScope = String(scopeSelect.value || 'all');
    state.labels.printScope = nextScope === 'single' || nextScope === 'range' ? nextScope : 'all';
    invalidateLabelsPreview();
    Views.labels.render();
  });

  recordInput.addEventListener('change', () => {
    state.labels.printScope = 'single';
    state.labels.selectedRecordNumber = Number.parseInt(recordInput.value || '1', 10) || 1;
    invalidateLabelsPreview();
    Views.labels.render();
  });

  rangeStartInput.addEventListener('change', () => {
    state.labels.printScope = 'range';
    state.labels.rangeStartNumber = Number.parseInt(rangeStartInput.value || '1', 10) || 1;
    invalidateLabelsPreview();
    Views.labels.render();
  });

  rangeEndInput.addEventListener('change', () => {
    state.labels.printScope = 'range';
    state.labels.rangeEndNumber = Number.parseInt(rangeEndInput.value || '1', 10) || 1;
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

function formatPreview(preview) {
  if (!preview) {
    return 'Выберите шаблон, файл и заказ, затем нажмите «Показать контекст».';
  }
  const lines = [
    `Заказ: ${preview.order_name}`,
    `Формат: ${preview.sheet_format_label || preview.sheet_format || '100x180'}`,
    `Шаблон: ${preview.template_category} / ${preview.data_source_kind}`,
    `Режим печати: ${preview.print_scope_label || 'Весь файл'}`,
    `Размер: ${preview.size}`,
    `Партия: ${preview.batch}`,
    `Цвет: ${preview.color || '—'}`,
    `Дата изготовления: ${preview.manufacture_date}`,
    `Срок годности: ${preview.expiration_date}`,
    `Количество: ${preview.quantity_pairs} ${preview.quantity_pairs_word}`,
    `Упаковка: ${preview.package_text || 'не используется'}`,
    `Этикеток к печати: ${preview.label_count}`,
  ];
  if (preview.total_record_count) {
    lines.push(`Записей в файле: ${preview.total_record_count}`);
  }
  if (
    preview.selected_record_number
    && preview.selected_record_end_number
    && preview.selected_record_end_number !== preview.selected_record_number
  ) {
    lines.push(`Выбран диапазон: ${preview.selected_record_number}-${preview.selected_record_end_number}`);
    lines.push(`Записей в диапазоне: ${preview.range_record_count || preview.label_count}`);
  } else if (preview.selected_record_number) {
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

function normalizeLabelsRecordSelection() {
  const file = selectedLabelFileMeta();
  const total = Math.max(0, Number(file?.record_count || 0));
  let selectedRecordNumber = Math.max(1, Number.parseInt(state.labels.selectedRecordNumber || 1, 10) || 1);
  let rangeStartNumber = Math.max(1, Number.parseInt(state.labels.rangeStartNumber || 1, 10) || 1);
  let rangeEndNumber = Math.max(1, Number.parseInt(state.labels.rangeEndNumber || rangeStartNumber, 10) || rangeStartNumber);
  if (total > 0) {
    selectedRecordNumber = Math.min(selectedRecordNumber, total);
    rangeStartNumber = Math.min(rangeStartNumber, total);
    rangeEndNumber = Math.min(rangeEndNumber, total);
  }
  if (rangeEndNumber < rangeStartNumber) {
    rangeEndNumber = rangeStartNumber;
  }
  state.labels.selectedRecordNumber = selectedRecordNumber;
  state.labels.rangeStartNumber = rangeStartNumber;
  state.labels.rangeEndNumber = rangeEndNumber;
  return { file, total, selectedRecordNumber, rangeStartNumber, rangeEndNumber };
}

function labelsRecordInfoText() {
  const {
    total,
    selectedRecordNumber,
    rangeStartNumber,
    rangeEndNumber,
  } = normalizeLabelsRecordSelection();
  if (!total) {
    return 'Сначала выберите файл с кодами для печати.';
  }
  if (state.labels.printScope === 'all') {
    return `Сейчас на печать пойдёт весь файл: ${total} этикеток.`;
  }
  if (state.labels.printScope === 'range') {
    let text = `На печать пойдёт диапазон записей №${rangeStartNumber}-${rangeEndNumber} из ${total}. Всего этикеток: ${Math.max(0, rangeEndNumber - rangeStartNumber + 1)}.`;
    const rangePreview = state.labels.preview;
    if (
      rangePreview
      && rangePreview.print_scope === 'range'
      && Number(rangePreview.selected_record_number || 0) === rangeStartNumber
      && Number(rangePreview.selected_record_end_number || 0) === rangeEndNumber
      && rangePreview.selected_code_value_short
    ) {
      text += ` ${rangePreview.selected_code_label || 'Первый код'}: ${rangePreview.selected_code_value_short}.`;
    }
    return text;
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
            <option value="range">Диапазон этикеток</option>
          </select>
        </label>
        <label class="field">
          <span>Номер этикетки по порядку</span>
          <input id="labels-record-number" type="number" min="1" step="1" placeholder="1">
        </label>
        <label class="field">
          <span>Диапазон записей</span>
          <div class="inline-actions compact wrap">
            <input id="labels-range-start" type="number" min="1" step="1" placeholder="С 1">
            <input id="labels-range-end" type="number" min="1" step="1" placeholder="По 200">
          </div>
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
  const rangeStartInput = $('#labels-range-start');
  const rangeEndInput = $('#labels-range-end');
  const prevButton = $('#labels-record-prev');
  const nextButton = $('#labels-record-next');
  if (!scopeSelect || scopeSelect.dataset.bound === '1') {
    return;
  }

  scopeSelect.dataset.bound = '1';
  scopeSelect.addEventListener('change', () => {
    const nextScope = String(scopeSelect.value || 'all');
    state.labels.printScope = nextScope === 'single' || nextScope === 'range' ? nextScope : 'all';
    invalidateLabelsPreview();
    Views.labels.render();
  });

  recordInput.addEventListener('change', () => {
    state.labels.printScope = 'single';
    state.labels.selectedRecordNumber = Number.parseInt(recordInput.value || '1', 10) || 1;
    invalidateLabelsPreview();
    Views.labels.render();
  });

  rangeStartInput.addEventListener('change', () => {
    state.labels.printScope = 'range';
    state.labels.rangeStartNumber = Number.parseInt(rangeStartInput.value || '1', 10) || 1;
    invalidateLabelsPreview();
    Views.labels.render();
  });

  rangeEndInput.addEventListener('change', () => {
    state.labels.printScope = 'range';
    state.labels.rangeEndNumber = Number.parseInt(rangeEndInput.value || '1', 10) || 1;
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
      applyOrderQueueUpdate(result, { selectItem: true });
      return result;
    }, 'Позиция добавлена в очередь.');
  });

  $('#create-order-now-btn').addEventListener('click', async () => {
    await runAction('Создаём заказ...', async () => {
      const result = await API.call('create_order', readOrderForm());
      if (result?.download_item) {
        applyImmediateDownloadItem(result.download_item);
      }
      await loadOrdersState({ force: true });
      markRoutesDirty(['download', 'intro', 'tsd', 'labels']);
      return result;
    }, 'Заказ создан.');
  });

  $('#submit-queue-btn').addEventListener('click', async () => {
    await runAction('Выполняем очередь заказов...', async () => {
      const result = await API.call('submit_order_queue');
      await loadOrdersState({ force: true });
      if (isRouteActive('download')) {
        await loadDownloadState({ force: true });
      }
      markRoutesDirty(['download', 'intro', 'tsd', 'labels']);
      return result;
    }, 'Очередь заказов выполнена.');
  });

  $('#clear-queue-btn').addEventListener('click', async () => {
    await runAction('Очищаем очередь...', async () => {
      const result = await API.call('clear_order_queue');
      applyOrderQueueUpdate(result);
      return result;
    }, 'Очередь очищена.');
  });

  $('#orders-remove-queue-btn').addEventListener('click', async () => {
    await runAction('Удаляем позицию из очереди...', async () => {
      if (!state.orders.selectedQueueId) {
        throw new Error('Выберите позицию в очереди заказов.');
      }
      const result = await API.call('remove_order_item', state.orders.selectedQueueId);
      applyOrderQueueUpdate(result);
      return result;
    }, 'Позиция удалена из очереди.');
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

	  $('#orders-history-search').addEventListener('input', (event) => {
	    state.orders.historySearch = event.target.value || '';
	    Views.orders.render();
	  });

	  $('#orders-history-fullscreen').addEventListener('click', () => {
	    state.orders.fullscreenTable = 'history';
	    renderOrdersFullscreenTable();
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

	  $('#download-items-search').addEventListener('input', (event) => {
	    state.download.searchQuery = event.target.value || '';
	    state.download.lastClickedIndex = -1;
	    Views.download.render();
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

  $('#download-record-number').addEventListener('change', () => {
    state.download.recordNumber = $('#download-record-number').value.trim();
  });

  $('#download-print-btn').addEventListener('click', async () => {
    await runAction('Запускаем печать термоэтикеток...', async () => {
      const targetId = state.download.selectedItemId || Array.from(state.download.selectedIds)[0] || '';
      const result = await API.call(
        'print_download_order',
        targetId,
        $('#download-printer-select').value,
        state.download.recordNumber || null,
      );
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

	  $('#intro-refresh-btn').addEventListener('click', async () => {
    await runAction('Обновляем список заказов для ввода в оборот...', async () => {
      const result = await loadIntroState({ force: true });
      return result || { success: true };
    }, 'Список заказов обновлён.');
	  });

	  $('#intro-items-search').addEventListener('input', (event) => {
	    state.intro.searchQuery = event.target.value || '';
	    Views.intro.render();
	  });

	  $('#intro-status-filter').addEventListener('change', (event) => {
	    state.intro.statusFilter = event.target.value || '';
	    Views.intro.render();
	  });

	  $('#tsd-items-search').addEventListener('input', (event) => {
	    state.tsd.searchQuery = event.target.value || '';
	    Views.tsd.render();
	  });

	  $('#tsd-status-filter').addEventListener('change', (event) => {
	    state.tsd.statusFilter = event.target.value || '';
	    Views.tsd.render();
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
        $('#agg-intro-document-title').value,
      );
      markRoutesDirty(['aggregation', 'download', 'intro', 'tsd']);
      window.setTimeout(() => {
        loadAggregationState({ force: true, render: isRouteActive('aggregation') }).catch(() => null);
      }, 0);
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

  $('#labels-orders-search').addEventListener('input', (event) => {
    updateLabelsTableSearch('orders', event.target.value);
  });

  $('#labels-aggregation-search').addEventListener('input', (event) => {
    updateLabelsTableSearch('aggregation', event.target.value);
  });

  $('#labels-marking-search').addEventListener('input', (event) => {
    updateLabelsTableSearch('marking', event.target.value);
  });

  $('#labels-orders-fullscreen').addEventListener('click', () => {
    state.labels.fullscreenTable = 'orders';
    renderLabelsFullscreenTable();
  });

  $('#labels-aggregation-fullscreen').addEventListener('click', () => {
    state.labels.fullscreenTable = 'aggregation';
    renderLabelsFullscreenTable();
  });

  $('#labels-marking-fullscreen').addEventListener('click', () => {
    state.labels.fullscreenTable = 'marking';
    renderLabelsFullscreenTable();
  });

	  $('#table-fullscreen-close').addEventListener('click', () => {
	    state.labels.fullscreenTable = '';
	    state.orders.fullscreenTable = '';
	    renderLabelsFullscreenTable();
	    renderOrdersFullscreenTable();
	  });

  $('#table-fullscreen-overlay').addEventListener('click', (event) => {
    if (event.target.id !== 'table-fullscreen-overlay') {
      return;
	    }
	    state.labels.fullscreenTable = '';
	    state.orders.fullscreenTable = '';
	    renderLabelsFullscreenTable();
	    renderOrdersFullscreenTable();
	  });

	  $('#table-fullscreen-search').addEventListener('input', (event) => {
	    if (state.orders.fullscreenTable === 'history') {
	      state.orders.historySearch = event.target.value || '';
	      Views.orders.render();
	      return;
	    }
	    const tableKey = String(state.labels.fullscreenTable || '').trim();
    if (!tableKey) {
      return;
    }
    updateLabelsTableSearch(tableKey, event.target.value);
  });

  $('#labels-preview-btn').addEventListener('click', async () => {
    await runAction('Собираем контекст печати...', async () => {
      const result = await API.call('preview_100x180_label', {
        sheet_format: state.labels.selectedSheetFormat,
        document_id: state.labels.selectedOrderId,
        template_path: state.labels.selectedTemplatePath,
        csv_path: selectedLabelCsvPath(),
        printer_name: $('#labels-printer-select').value,
        manufacture_date: $('#labels-manufacture-date').value,
        expiration_date: $('#labels-expiration-date').value,
        quantity_value: $('#labels-quantity-value').value,
        print_scope: state.labels.printScope,
        record_number: state.labels.printScope === 'single' ? state.labels.selectedRecordNumber : null,
        range_start: state.labels.printScope === 'range' ? state.labels.rangeStartNumber : null,
        range_end: state.labels.printScope === 'range' ? state.labels.rangeEndNumber : null,
        manual_override: readLabelsManualOverride(),
      });
      if (result.needs_manual_input) {
        applyLabelsManualForm(result.manual_form || {});
        state.labels.preview = null;
        Views.labels.render();
        setStatusText(result.prompt || 'Заполните форму вручную и повторите действие.', true);
        appendUiLog(result.prompt || 'Нужно заполнить поля вручную для печати этикетки.', 'labels');
        showToast(result.prompt || 'Заполните форму вручную.', 'info');
        return result;
      }
      if (!result.preview?.manual_override_used) {
        resetLabelsManualState();
      }
      state.labels.preview = result.preview;
      Views.labels.render();
      return result;
    });
  });

  $('#labels-print-btn').addEventListener('click', async () => {
    await runAction('Запускаем печать этикеток...', async () => {
      const result = await API.call('print_100x180_label', {
        sheet_format: state.labels.selectedSheetFormat,
        document_id: state.labels.selectedOrderId,
        template_path: state.labels.selectedTemplatePath,
        csv_path: selectedLabelCsvPath(),
        printer_name: $('#labels-printer-select').value,
        manufacture_date: $('#labels-manufacture-date').value,
        expiration_date: $('#labels-expiration-date').value,
        quantity_value: $('#labels-quantity-value').value,
        print_scope: state.labels.printScope,
        record_number: state.labels.printScope === 'single' ? state.labels.selectedRecordNumber : null,
        range_start: state.labels.printScope === 'range' ? state.labels.rangeStartNumber : null,
        range_end: state.labels.printScope === 'range' ? state.labels.rangeEndNumber : null,
        manual_override: readLabelsManualOverride(),
      });
      if (result.needs_manual_input) {
        applyLabelsManualForm(result.manual_form || {});
        state.labels.preview = null;
        Views.labels.render();
        setStatusText(result.prompt || 'Заполните форму вручную и повторите действие.', true);
        appendUiLog(result.prompt || 'Нужно заполнить поля вручную для печати этикетки.', 'labels');
        showToast(result.prompt || 'Заполните форму вручную.', 'info');
        return result;
      }
      if (!result.preview?.manual_override_used) {
        resetLabelsManualState();
      }
      state.labels.preview = result.preview || state.labels.preview;
      Views.labels.render();
      return result;
    });
  });

  $('#labels-printer-select').addEventListener('change', () => {
    state.labels.selectedPrinter = $('#labels-printer-select').value;
  });

  $('#labels-sheet-format-select').addEventListener('change', () => {
    state.labels.selectedSheetFormat = $('#labels-sheet-format-select').value || state.labels.defaultSheetFormat || '100x180';
    const availableTemplates = visibleLabelTemplates();
    if (!availableTemplates.some((item) => item.path === state.labels.selectedTemplatePath)) {
      state.labels.selectedTemplatePath = availableTemplates[0]?.path || '';
      state.labels.templatePage = 0;
    } else {
      const currentIndex = availableTemplates.findIndex((item) => item.path === state.labels.selectedTemplatePath);
      state.labels.templatePage = currentIndex >= 0 ? Math.floor(currentIndex / state.labels.templatePageSize) : 0;
    }
    resetLabelsManualState();
    invalidateLabelsPreview();
    Views.labels.render();
  });

  ['gtin', 'size', 'batch', 'color', 'units'].forEach((field) => {
    const element = $(`#labels-manual-${field}`);
    if (!element) {
      return;
    }
    element.addEventListener('input', () => {
      const key = field === 'units' ? 'units_per_pack' : field;
      state.labels.manualFields[key] = element.value;
    });
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
  applyClientConfig();
  setTheme(state.theme);
  installMobileViewportGuard();
  installMojibakeGuard();
  installDesktopInteractionFallbacks();
  ensureAggregationIntroDocumentTitleField();

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
  applyClientConfig();
  installMojibakeGuard();
  if (CLIENT_CONFIG.mobileMode) {
    setTheme(state.theme);
    installMobileViewportGuard();
  }
  if (window.pywebview?.api || CLIENT_CONFIG.browserMode) {
    init();
  } else {
    setTheme(state.theme);
    setStatusText('Ожидаем инициализацию PyWebView API...', false);
  }
});


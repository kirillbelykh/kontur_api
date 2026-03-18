import { startTransition, useDeferredValue, useEffect, useMemo, useState, type ReactNode } from 'react';

import {
  checkOrderStatus,
  createOrders,
  createTsdTask,
  downloadOrder,
  getAppState,
  listHistory,
  markTsdCreated,
  refreshSession,
  runIntroduction,
  searchAndExportAggregates,
  type AggregationExportResult,
  type AggregationQuery,
  type AppState,
  type DependencyStatus,
  type DownloadArtifact,
  type HistoryFilter,
  type IntroductionRequest,
  type OperationStatus,
  type OrderDraft,
  type OrderRecord,
  type PositionData,
  type ProductionPatch,
  type TSDRequest,
} from './api/backend';
import './styles/app.css';

type Section = 'overview' | 'orders' | 'introduction' | 'tsd' | 'aggregation' | 'history';

type OrderActionsState = {
  documentId: string;
};

type IntroductionFormState = {
  codesOrderId: string;
  organizationId: string;
  thumbprint: string;
  productionPatch: ProductionPatch;
};

type TsdFormState = {
  codesOrderId: string;
  positionsText: string;
  productionPatch: ProductionPatch;
};

type UserIssue = {
  key: string;
  title: string;
  text: string;
};

const sections: { id: Section; title: string; blurb: string }[] = [
  { id: 'overview', title: 'Главная', blurb: 'Общий обзор работы и быстрый доступ к основным действиям.' },
  { id: 'orders', title: 'Заказ кодов', blurb: 'Создание заказа, проверка статуса и скачивание файлов.' },
  { id: 'introduction', title: 'Ввод в оборот', blurb: 'Подготовка и отправка документа ввода в оборот.' },
  { id: 'tsd', title: 'ТСД', blurb: 'Создание задач на терминал сбора данных по выбранному заказу.' },
  { id: 'aggregation', title: 'Коды агрегации', blurb: 'Поиск, фильтрация и выгрузка кодов агрегации.' },
  { id: 'history', title: 'История', blurb: 'Общая история заказов и быстрые действия по строкам.' },
];

const initialDraft: OrderDraft = {
  orderName: '',
  simplifiedName: '',
  size: '',
  unitsPerPack: '',
  codesCount: 1,
  gtin: '',
  fullName: '',
  tnvedCode: '',
  cisType: 'unit',
};

const initialPatch: ProductionPatch = {
  documentNumber: '',
  productionDate: '',
  expirationDate: '',
  batchNumber: '',
  TnvedCode: '',
};

const initialIntroductionForm: IntroductionFormState = {
  codesOrderId: '',
  organizationId: '',
  thumbprint: '',
  productionPatch: { ...initialPatch },
};

const initialTsdForm: TsdFormState = {
  codesOrderId: '',
  positionsText: '',
  productionPatch: { ...initialPatch },
};

const initialAggregationQuery: AggregationQuery = {
  mode: 'comment',
  targetValue: '',
  statusFilter: 'tsdProcessStart',
  filename: '',
};

const initialOrderActions: OrderActionsState = {
  documentId: '',
};

function App() {
  const [section, setSection] = useState<Section>('overview');
  const [menuOpen, setMenuOpen] = useState(false);
  const [appState, setAppState] = useState<AppState | null>(null);
  const [history, setHistory] = useState<OrderRecord[]>([]);
  const [dependencies, setDependencies] = useState<DependencyStatus[]>([]);
  const [draft, setDraft] = useState<OrderDraft>(initialDraft);
  const [orderActions, setOrderActions] = useState<OrderActionsState>(initialOrderActions);
  const [introductionForm, setIntroductionForm] = useState<IntroductionFormState>(initialIntroductionForm);
  const [tsdForm, setTsdForm] = useState<TsdFormState>(initialTsdForm);
  const [aggregationQuery, setAggregationQuery] = useState<AggregationQuery>(initialAggregationQuery);
  const [historyFilter, setHistoryFilter] = useState<HistoryFilter>({ search: '', status: '', onlyWithoutTsd: false });
  const [selectedOrder, setSelectedOrder] = useState<OrderRecord | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: 'ok' | 'error' | 'info'; text: string } | null>(null);
  const [orderStatusResult, setOrderStatusResult] = useState<OperationStatus | null>(null);
  const [downloadResult, setDownloadResult] = useState<DownloadArtifact | null>(null);
  const [introductionResult, setIntroductionResult] = useState<OperationStatus | null>(null);
  const [tsdResult, setTsdResult] = useState<OperationStatus | null>(null);
  const [aggregationResult, setAggregationResult] = useState<AggregationExportResult | null>(null);

  const deferredSearch = useDeferredValue(historyFilter.search);
  const isBusy = busyAction !== null;

  useEffect(() => {
    void reloadDashboard();
  }, []);

  useEffect(() => {
    void reloadHistory(deferredSearch, historyFilter.status, historyFilter.onlyWithoutTsd);
  }, [deferredSearch, historyFilter.status, historyFilter.onlyWithoutTsd]);

  useEffect(() => {
    if (!notice) {
      return undefined;
    }
    const timeout = window.setTimeout(() => setNotice(null), 4500);
    return () => window.clearTimeout(timeout);
  }, [notice]);

  useEffect(() => {
    if (!menuOpen) {
      document.body.style.overflow = '';
      return undefined;
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setMenuOpen(false);
      }
    }

    window.addEventListener('keydown', handleKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [menuOpen]);

  useEffect(() => {
    if (!selectedOrder) {
      return;
    }
    const fresh = history.find((item) => item.document_id === selectedOrder.document_id);
    if (!fresh) {
      setSelectedOrder(null);
      return;
    }
    if (fresh !== selectedOrder) {
      setSelectedOrder(fresh);
    }
  }, [history, selectedOrder]);

  const currentSection = useMemo(
    () => sections.find((item) => item.id === section) ?? sections[0],
    [section],
  );

  const userIssues = useMemo(() => buildUserIssues(dependencies), [dependencies]);
  const selectedOrderTitle = selectedOrder?.order_name || selectedOrder?.document_id || 'Заказ не выбран';
  const sessionReady = Boolean(appState?.session.available);

  async function reloadDashboard() {
    const state = await getAppState();
    startTransition(() => {
      setAppState(state);
      setDependencies(state.dependencies ?? []);
    });
  }

  async function reloadHistory(search = '', status = '', onlyWithoutTsd = false) {
    const records = await listHistory({ search, status, onlyWithoutTsd });
    startTransition(() => setHistory(records));
  }

  function openSection(nextSection: Section) {
    setSection(nextSection);
    setMenuOpen(false);
  }

  function setInfo(text: string, tone: 'ok' | 'error' | 'info' = 'info') {
    setNotice({ tone, text: localizeMessage(text) });
  }

  function pickOrder(order: OrderRecord) {
    setSelectedOrder(order);
    setOrderActions({ documentId: order.document_id });
    setIntroductionForm((current) => ({
      ...current,
      codesOrderId: order.document_id,
      productionPatch: {
        ...current.productionPatch,
        documentNumber: current.productionPatch.documentNumber || order.order_name || '',
      },
    }));
    setTsdForm((current) => ({
      ...current,
      codesOrderId: order.document_id,
      positionsText: current.positionsText || orderToPositionsText(order),
      productionPatch: {
        ...current.productionPatch,
        documentNumber: current.productionPatch.documentNumber || order.order_name || '',
      },
    }));
    setInfo(`Выбран заказ ${order.document_id}`, 'info');
  }

  function clearSelection() {
    setSelectedOrder(null);
    setInfo('Выбор заказа очищен', 'info');
  }

  async function runAction<T>(label: string, job: () => Promise<T>, onSuccess: (result: T) => Promise<void> | void) {
    setBusyAction(label);
    try {
      const result = await job();
      await onSuccess(result);
    } catch (error) {
      setInfo(extractErrorMessage(error), 'error');
    } finally {
      setBusyAction(null);
    }
  }

  async function refreshAll() {
    await Promise.all([
      reloadDashboard(),
      reloadHistory(historyFilter.search, historyFilter.status, historyFilter.onlyWithoutTsd),
    ]);
  }

  async function handleRefreshSession() {
    await runAction('refresh-session', refreshSession, async (result) => {
      setInfo(result.message ?? 'Сессия обновлена', result.available ? 'ok' : 'info');
      await reloadDashboard();
    });
  }

  async function handleCreateDraft() {
    await runAction('create-order', () => createOrders([draft]), async (created) => {
      const first = created[0];
      setDraft(initialDraft);
      if (first) {
        setOrderActions({ documentId: first.document_id });
        pickOrder(first);
      }
      setInfo(`Создано записей: ${created.length}`, 'ok');
      await refreshAll();
    });
  }

  async function handleCheckOrderStatus(documentId = orderActions.documentId) {
    if (!documentId.trim()) {
      setInfo('Укажите ID документа для проверки статуса', 'error');
      return;
    }
    await runAction('check-order-status', () => checkOrderStatus(documentId.trim()), async (result) => {
      setOrderStatusResult(result);
      setInfo(result.message, result.ok ? 'ok' : 'info');
      await refreshAll();
    });
  }

  async function handleDownloadOrder(documentId = orderActions.documentId) {
    if (!documentId.trim()) {
      setInfo('Укажите ID документа для скачивания', 'error');
      return;
    }
    await runAction('download-order', () => downloadOrder(documentId.trim()), async (artifact) => {
      setDownloadResult(artifact);
      setInfo(artifact.directory ? `Файлы сохранены в ${artifact.directory}` : 'Файлы подготовлены', 'ok');
      await refreshAll();
    });
  }

  async function handleRunIntroduction() {
    if (!introductionForm.codesOrderId.trim()) {
      setInfo('Укажите ID заказа для ввода в оборот', 'error');
      return;
    }
    await runAction('run-introduction', () => runIntroduction(introductionForm as IntroductionRequest), async (result) => {
      setIntroductionResult(result);
      setInfo(result.message, result.ok ? 'ok' : 'info');
      await refreshAll();
    });
  }

  async function handleCreateTsdTask() {
    if (!tsdForm.codesOrderId.trim()) {
      setInfo('Укажите ID заказа для создания задачи на ТСД', 'error');
      return;
    }

    let positions: PositionData[];
    try {
      positions = parsePositions(tsdForm.positionsText);
    } catch (error) {
      setInfo(extractErrorMessage(error), 'error');
      return;
    }

    const request: TSDRequest = {
      codesOrderId: tsdForm.codesOrderId,
      positions,
      productionPatch: tsdForm.productionPatch,
    };

    await runAction('create-tsd-task', () => createTsdTask(request), async (result) => {
      setTsdResult(result);
      setInfo(result.message, result.ok ? 'ok' : 'info');
      await refreshAll();
    });
  }

  async function handleAggregationExport() {
    if (!aggregationQuery.targetValue.trim()) {
      setInfo('Укажите значение для поиска кодов агрегации', 'error');
      return;
    }
    await runAction('search-aggregation', () => searchAndExportAggregates(aggregationQuery), (result) => {
      setAggregationResult(result);
      setInfo(`Выгружено кодов агрегации: ${result.records.length}`, 'ok');
    });
  }

  async function handleMarkTsdCreated(order: OrderRecord) {
    await runAction('mark-tsd-created', () => markTsdCreated(order.document_id, `TSD-${Date.now()}`), async (updated) => {
      setInfo(`Заказ ${updated.document_id} отмечен как обработанный для ТСД`, 'ok');
      await refreshAll();
    });
  }

  return (
    <div className="app-shell">
      <button
        type="button"
        className={menuOpen ? 'drawer-backdrop visible' : 'drawer-backdrop'}
        aria-label="Закрыть меню"
        onClick={() => setMenuOpen(false)}
      />

      <aside className={menuOpen ? 'drawer open' : 'drawer'}>
        <div className="drawer-top">
          <div className="brand">
            <div className="brand-mark">КМ</div>
            <div>
              <h2>Kontur Go</h2>
              <p>рабочее приложение для маркировки</p>
            </div>
          </div>
          <button type="button" className="icon-button" aria-label="Закрыть меню" onClick={() => setMenuOpen(false)}>
            <span />
            <span />
          </button>
        </div>

        <nav className="drawer-nav">
          {sections.map((item) => (
            <button
              key={item.id}
              type="button"
              className={section === item.id ? 'drawer-link active' : 'drawer-link'}
              onClick={() => openSection(item.id)}
            >
              <strong>{item.title}</strong>
              <span>{item.blurb}</span>
            </button>
          ))}
        </nav>

        <div className="drawer-footer">
          <div className="session-card">
            <span className="label">Состояние</span>
            <strong>{sessionReady ? 'Сессия активна' : 'Нужен вход в Контур'}</strong>
            <p>{localizeMessage(appState?.session.message ?? 'Ожидание запуска приложения')}</p>
            <button type="button" onClick={handleRefreshSession} disabled={isBusy}>
              {busyAction === 'refresh-session' ? 'Обновление…' : 'Обновить сессию'}
            </button>
          </div>

          <div className="drawer-meta">
            <span className="label">Выбранный заказ</span>
            <strong>{selectedOrderTitle}</strong>
            {selectedOrder && <p>{selectedOrder.document_id}</p>}
          </div>
        </div>
      </aside>

      <main className="app-main">
        <header className="app-header">
          <div className="header-left">
            <button type="button" className="menu-toggle" aria-label="Открыть меню" onClick={() => setMenuOpen(true)}>
              <span />
              <span />
              <span />
            </button>
            <div>
              <p className="eyebrow">Контур.Маркировка</p>
              <h1>{currentSection.title}</h1>
              <p className="header-copy">{currentSection.blurb}</p>
            </div>
          </div>
          <div className="header-actions">
            <div className={sessionReady ? 'status-badge ok' : 'status-badge warn'}>
              {sessionReady ? 'Сессия активна' : 'Требуется вход'}
            </div>
            <button type="button" className="secondary" onClick={() => void refreshAll()} disabled={isBusy}>
              Обновить данные
            </button>
          </div>
        </header>

        {notice && <div className={`notice ${notice.tone}`}>{notice.text}</div>}

        <section className="summary-grid">
          <article className="summary-card emphasis">
            <span>Заказы в истории</span>
            <strong>{appState?.ordersTotal ?? 0}</strong>
            <p>Все записи, которые уже сохранены в общей истории.</p>
          </article>
          <article className="summary-card">
            <span>Без ТСД</span>
            <strong>{appState?.ordersWithoutTsd ?? 0}</strong>
            <p>Заказы, по которым еще не создана задача на терминал.</p>
          </article>
          <article className="summary-card">
            <span>Проблемы</span>
            <strong>{userIssues.length}</strong>
            <p>{userIssues.length ? 'Есть моменты, которые нужно исправить перед работой.' : 'Обязательные компоненты готовы.'}</p>
          </article>
        </section>

        {selectedOrder && (
          <section className="context-banner">
            <div>
              <span className="label">Текущий заказ</span>
              <strong>{selectedOrder.order_name || selectedOrder.document_id}</strong>
              <p>{selectedOrder.document_id}</p>
            </div>
            <div className="context-actions">
              <button type="button" className="secondary" onClick={() => handleCheckOrderStatus(selectedOrder.document_id)} disabled={isBusy}>
                Проверить статус
              </button>
              <button type="button" className="secondary" onClick={() => handleDownloadOrder(selectedOrder.document_id)} disabled={isBusy}>
                Скачать файлы
              </button>
              <button type="button" className="ghost-button" onClick={clearSelection} disabled={isBusy}>
                Снять выбор
              </button>
            </div>
          </section>
        )}

        {section === 'overview' && (
          <section className="section-grid">
            <article className="panel hero-panel panel-wide">
              <span className="panel-chip">Быстрый старт</span>
              <h3>Рабочее место для заказов, ТСД и агрегации</h3>
              <p className="panel-copy">
                Обновите сессию, выберите заказ из истории и переходите к нужному разделу. Основные действия собраны в одном месте.
              </p>
              <div className="action-row wrap">
                <button type="button" onClick={handleRefreshSession} disabled={isBusy}>
                  {busyAction === 'refresh-session' ? 'Обновление…' : 'Обновить сессию'}
                </button>
                <button type="button" className="secondary" onClick={() => openSection('orders')}>
                  Перейти к заказу кодов
                </button>
                <button type="button" className="secondary" onClick={() => openSection('history')}>
                  Открыть историю
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Что проверить перед работой</h3>
                <span className="panel-tag">Готовность</span>
              </div>
              {userIssues.length ? (
                <div className="issue-stack">
                  {userIssues.map((issue) => (
                    <div key={issue.key} className="issue-card">
                      <strong>{issue.title}</strong>
                      <p>{issue.text}</p>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-state">Все обязательные компоненты готовы. Можно переходить к работе.</div>
              )}
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Порядок работы</h3>
                <span className="panel-tag">Подсказка</span>
              </div>
              <div className="workflow-list">
                <div>
                  <strong>1. Обновите сессию</strong>
                  <span>Если приложение не подключено, сначала обновите сессию через Яндекс Браузер.</span>
                </div>
                <div>
                  <strong>2. Создайте или выберите заказ</strong>
                  <span>Новый заказ создается в разделе “Заказ кодов”, существующий можно выбрать в истории.</span>
                </div>
                <div>
                  <strong>3. Выполните нужный сценарий</strong>
                  <span>После выбора заказа переходите к вводу в оборот, ТСД или работе с агрегацией.</span>
                </div>
              </div>
            </article>

            <article className="panel panel-wide">
              <div className="panel-head">
                <h3>Текущий контекст</h3>
                <span className="panel-tag">Выбранный заказ</span>
              </div>
              {selectedOrder ? (
                <div className="context-grid">
                  <div className="context-item">
                    <span className="label">Название</span>
                    <strong>{selectedOrder.order_name || 'Без названия'}</strong>
                  </div>
                  <div className="context-item">
                    <span className="label">ID документа</span>
                    <strong>{selectedOrder.document_id}</strong>
                  </div>
                  <div className="context-item">
                    <span className="label">Статус</span>
                    <strong>{localizeStatus(selectedOrder.status)}</strong>
                  </div>
                  <div className="context-item">
                    <span className="label">Обновлен</span>
                    <strong>{formatDate(selectedOrder.updated_at) || 'Дата не указана'}</strong>
                  </div>
                </div>
              ) : (
                <div className="empty-state">Выберите заказ в разделе “История”, чтобы подставлять его в другие разделы автоматически.</div>
              )}
            </article>
          </section>
        )}

        {section === 'orders' && (
          <section className="section-grid">
            <article className="panel panel-wide">
              <div className="panel-head">
                <h3>Создание заказа</h3>
                <span className="panel-tag">Новый заказ</span>
              </div>
              <p className="panel-copy">Заполните данные по товару и количеству кодов, затем создайте заказ.</p>
              <div className="form-grid two-up">
                <Field label="Название заказа">
                  <input value={draft.orderName} onChange={(event) => setDraft({ ...draft, orderName: event.target.value })} />
                </Field>
                <Field label="Короткое название">
                  <input value={draft.simplifiedName} onChange={(event) => setDraft({ ...draft, simplifiedName: event.target.value })} />
                </Field>
                <Field label="Полное наименование">
                  <input value={draft.fullName} onChange={(event) => setDraft({ ...draft, fullName: event.target.value })} />
                </Field>
                <Field label="GTIN">
                  <input value={draft.gtin} onChange={(event) => setDraft({ ...draft, gtin: event.target.value })} />
                </Field>
                <Field label="ТН ВЭД">
                  <input value={draft.tnvedCode} onChange={(event) => setDraft({ ...draft, tnvedCode: event.target.value })} />
                </Field>
                <Field label="Количество кодов">
                  <input type="number" min={1} value={draft.codesCount} onChange={(event) => setDraft({ ...draft, codesCount: Number(event.target.value) || 1 })} />
                </Field>
                <Field label="Размер">
                  <input value={draft.size} onChange={(event) => setDraft({ ...draft, size: event.target.value })} />
                </Field>
                <Field label="Единиц в упаковке">
                  <input value={draft.unitsPerPack} onChange={(event) => setDraft({ ...draft, unitsPerPack: event.target.value })} />
                </Field>
              </div>
              <div className="action-row">
                <button type="button" onClick={handleCreateDraft} disabled={isBusy || !draft.orderName.trim()}>
                  {busyAction === 'create-order' ? 'Создание…' : 'Создать заказ'}
                </button>
                <button type="button" className="secondary" onClick={() => setDraft(initialDraft)} disabled={isBusy}>
                  Очистить поля
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Работа по ID документа</h3>
                <span className="panel-tag">Статус и скачивание</span>
              </div>
              <Field label="ID документа">
                <input value={orderActions.documentId} onChange={(event) => setOrderActions({ documentId: event.target.value })} />
              </Field>
              <div className="action-row wrap">
                <button type="button" onClick={() => handleCheckOrderStatus()} disabled={isBusy || !orderActions.documentId.trim()}>
                  {busyAction === 'check-order-status' ? 'Проверка…' : 'Проверить статус'}
                </button>
                <button type="button" className="secondary" onClick={() => handleDownloadOrder()} disabled={isBusy || !orderActions.documentId.trim()}>
                  {busyAction === 'download-order' ? 'Скачивание…' : 'Скачать файлы'}
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Подставить выбранный заказ
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Результат проверки</h3>
                <span className={orderStatusResult?.ok ? 'status-dot ok' : 'status-dot'} />
              </div>
              <ResultStatus result={orderStatusResult} emptyText="Проверка статуса еще не выполнялась." />
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Скачанные файлы</h3>
                <span className="panel-tag">Последний результат</span>
              </div>
              <ArtifactResult artifact={downloadResult} />
            </article>
          </section>
        )}

        {section === 'introduction' && (
          <section className="section-grid">
            <article className="panel panel-wide">
              <div className="panel-head">
                <h3>Ввод в оборот</h3>
                <span className="panel-tag">Документ</span>
              </div>
              <p className="panel-copy">Выберите заказ из истории или введите ID вручную, затем заполните сведения о документе.</p>
              <div className="form-grid three-up">
                <Field label="ID заказа">
                  <input value={introductionForm.codesOrderId} onChange={(event) => setIntroductionForm({ ...introductionForm, codesOrderId: event.target.value })} />
                </Field>
                <Field label="Номер документа">
                  <input value={introductionForm.productionPatch.documentNumber} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, documentNumber: event.target.value } })} />
                </Field>
                <Field label="ТН ВЭД">
                  <input value={introductionForm.productionPatch.TnvedCode} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, TnvedCode: event.target.value } })} />
                </Field>
                <Field label="Дата производства">
                  <input type="date" value={introductionForm.productionPatch.productionDate} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, productionDate: event.target.value } })} />
                </Field>
                <Field label="Срок годности">
                  <input type="date" value={introductionForm.productionPatch.expirationDate} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, expirationDate: event.target.value } })} />
                </Field>
                <Field label="Номер партии">
                  <input value={introductionForm.productionPatch.batchNumber} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, batchNumber: event.target.value } })} />
                </Field>
              </div>
              <div className="action-row wrap">
                <button type="button" onClick={handleRunIntroduction} disabled={isBusy || !introductionForm.codesOrderId.trim()}>
                  {busyAction === 'run-introduction' ? 'Отправка…' : 'Ввести в оборот'}
                </button>
                <button type="button" className="secondary" onClick={() => setIntroductionForm(initialIntroductionForm)} disabled={isBusy}>
                  Очистить поля
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Подставить выбранный заказ
                </button>
              </div>
            </article>

            <article className="panel panel-wide">
              <div className="panel-head">
                <h3>Результат ввода в оборот</h3>
                <span className={introductionResult?.ok ? 'status-dot ok' : 'status-dot'} />
              </div>
              <ResultStatus result={introductionResult} emptyText="Ввод в оборот еще не выполнялся." />
            </article>
          </section>
        )}

        {section === 'tsd' && (
          <section className="section-grid">
            <article className="panel panel-wide">
              <div className="panel-head">
                <h3>Задача на ТСД</h3>
                <span className="panel-tag">Позиции</span>
              </div>
              <p className="panel-copy">Позиции вводятся построчно в формате “Наименование|GTIN”.</p>
              <div className="form-grid three-up">
                <Field label="ID заказа">
                  <input value={tsdForm.codesOrderId} onChange={(event) => setTsdForm({ ...tsdForm, codesOrderId: event.target.value })} />
                </Field>
                <Field label="Номер документа">
                  <input value={tsdForm.productionPatch.documentNumber} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, documentNumber: event.target.value } })} />
                </Field>
                <Field label="ТН ВЭД">
                  <input value={tsdForm.productionPatch.TnvedCode} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, TnvedCode: event.target.value } })} />
                </Field>
                <Field label="Дата производства">
                  <input type="date" value={tsdForm.productionPatch.productionDate} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, productionDate: event.target.value } })} />
                </Field>
                <Field label="Срок годности">
                  <input type="date" value={tsdForm.productionPatch.expirationDate} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, expirationDate: event.target.value } })} />
                </Field>
                <Field label="Номер партии">
                  <input value={tsdForm.productionPatch.batchNumber} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, batchNumber: event.target.value } })} />
                </Field>
              </div>
              <Field label="Позиции">
                <textarea
                  rows={8}
                  value={tsdForm.positionsText}
                  onChange={(event) => setTsdForm({ ...tsdForm, positionsText: event.target.value })}
                  placeholder={'Кресло-коляска XL|04601234567890\nКресло-коляска S|04601234567891'}
                />
              </Field>
              <div className="action-row wrap">
                <button type="button" onClick={handleCreateTsdTask} disabled={isBusy || !tsdForm.codesOrderId.trim()}>
                  {busyAction === 'create-tsd-task' ? 'Создание…' : 'Создать задачу'}
                </button>
                <button type="button" className="secondary" onClick={() => setTsdForm(initialTsdForm)} disabled={isBusy}>
                  Очистить поля
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Подставить выбранный заказ
                </button>
              </div>
            </article>

            <article className="panel panel-wide">
              <div className="panel-head">
                <h3>Результат по ТСД</h3>
                <span className={tsdResult?.ok ? 'status-dot ok' : 'status-dot'} />
              </div>
              <ResultStatus result={tsdResult} emptyText="Задача на ТСД еще не создавалась." />
            </article>
          </section>
        )}

        {section === 'aggregation' && (
          <section className="section-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Поиск и выгрузка</h3>
                <span className="panel-tag">Коды агрегации</span>
              </div>
              <div className="form-grid two-up compact-grid">
                <Field label="Искать по">
                  <select value={aggregationQuery.mode} onChange={(event) => setAggregationQuery({ ...aggregationQuery, mode: event.target.value })}>
                    <option value="comment">Названию</option>
                    <option value="count">Количеству</option>
                  </select>
                </Field>
                <Field label="Значение">
                  <input value={aggregationQuery.targetValue} onChange={(event) => setAggregationQuery({ ...aggregationQuery, targetValue: event.target.value })} />
                </Field>
                <Field label="Статус">
                  <input value={aggregationQuery.statusFilter} onChange={(event) => setAggregationQuery({ ...aggregationQuery, statusFilter: event.target.value })} />
                </Field>
                <Field label="Имя файла">
                  <input value={aggregationQuery.filename} onChange={(event) => setAggregationQuery({ ...aggregationQuery, filename: event.target.value })} placeholder="необязательно.csv" />
                </Field>
              </div>
              <div className="action-row wrap">
                <button type="button" onClick={handleAggregationExport} disabled={isBusy || !aggregationQuery.targetValue.trim()}>
                  {busyAction === 'search-aggregation' ? 'Выгрузка…' : 'Найти и выгрузить'}
                </button>
                <button type="button" className="secondary" onClick={() => setAggregationQuery(initialAggregationQuery)} disabled={isBusy}>
                  Очистить поля
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Результат выгрузки</h3>
                <span className="panel-tag">{aggregationResult?.records.length ?? 0} строк</span>
              </div>
              {aggregationResult ? (
                <>
                  <div className="kv-list compact-list">
                    <div>
                      <span>Папка</span>
                      <strong>{aggregationResult.directory || 'Не указана'}</strong>
                    </div>
                    <div>
                      <span>Файл</span>
                      <strong>{aggregationResult.filename || 'Не указан'}</strong>
                    </div>
                  </div>
                  <div className="mini-table aggregation-table">
                    {aggregationResult.records.slice(0, 12).map((record) => (
                      <div className="mini-row" key={`${record.aggregateCode}-${record.documentId ?? ''}`}>
                        <div>
                          <strong>{record.aggregateCode}</strong>
                          <span>{record.comment || record.documentId || 'Без комментария'}</span>
                        </div>
                        <div>
                          <strong>{localizeStatus(record.status || 'Без статуса')}</strong>
                          <span>{formatDate(record.updatedDate || record.createdDate) || 'Дата не указана'}</span>
                        </div>
                      </div>
                    ))}
                    {!aggregationResult.records.length && <div className="empty-state">По текущему запросу ничего не найдено.</div>}
                  </div>
                </>
              ) : (
                <div className="empty-state">Выгрузка кодов агрегации еще не выполнялась.</div>
              )}
            </article>
          </section>
        )}

        {section === 'history' && (
          <section className="panel panel-wide history-panel">
            <div className="panel-head">
              <h3>История заказов</h3>
              <span className="panel-tag">{history.length} записей</span>
            </div>
            <div className="toolbar-row">
              <input
                value={historyFilter.search}
                onChange={(event) => setHistoryFilter((current) => ({ ...current, search: event.target.value }))}
                placeholder="Поиск по названию, GTIN или ID документа"
              />
              <input
                value={historyFilter.status}
                onChange={(event) => setHistoryFilter((current) => ({ ...current, status: event.target.value }))}
                placeholder="Фильтр по статусу"
              />
              <label className="check-row">
                <input
                  type="checkbox"
                  checked={historyFilter.onlyWithoutTsd}
                  onChange={(event) => setHistoryFilter((current) => ({ ...current, onlyWithoutTsd: event.target.checked }))}
                />
                <span>Только без ТСД</span>
              </label>
              <button type="button" className="secondary" onClick={() => reloadHistory(historyFilter.search, historyFilter.status, historyFilter.onlyWithoutTsd)} disabled={isBusy}>
                Обновить
              </button>
            </div>
            <div className="history-table">
              {history.map((item) => (
                <div className="history-row" key={item.document_id}>
                  <div>
                    <strong>{item.order_name || item.document_id}</strong>
                    <span>{item.full_name || item.gtin || 'Без описания'}</span>
                  </div>
                  <div>
                    <strong>{localizeStatus(item.status)}</strong>
                    <span>{formatDate(item.updated_at) || 'Дата не указана'}</span>
                  </div>
                  <div className="inline-actions">
                    <button type="button" className="ghost-button" onClick={() => pickOrder(item)} disabled={isBusy}>
                      Выбрать
                    </button>
                    <button type="button" className="secondary" onClick={() => handleCheckOrderStatus(item.document_id)} disabled={isBusy}>
                      Статус
                    </button>
                    <button type="button" className="secondary" onClick={() => handleDownloadOrder(item.document_id)} disabled={isBusy}>
                      Скачать
                    </button>
                    <button type="button" onClick={() => handleMarkTsdCreated(item)} disabled={isBusy}>
                      Отметить ТСД
                    </button>
                  </div>
                </div>
              ))}
              {!history.length && <div className="empty-state">По текущим фильтрам ничего не найдено.</div>}
            </div>
          </section>
        )}
      </main>
    </div>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
    </label>
  );
}

function ResultStatus({ result, emptyText }: { result: OperationStatus | null; emptyText: string }) {
  if (!result) {
    return <div className="empty-state">{emptyText}</div>;
  }

  return (
    <div className="result-stack">
      <div className={`result-banner ${result.ok ? 'ok' : 'info'}`}>
        <strong>{localizeMessage(result.message)}</strong>
      </div>
      {result.details && Object.keys(result.details).length > 0 && (
        <div className="kv-list compact-list">
          {Object.entries(result.details).map(([key, value]) => (
            <div key={key}>
              <span>{localizeDetailLabel(key)}</span>
              <strong>{value}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ArtifactResult({ artifact }: { artifact: DownloadArtifact | null }) {
  if (!artifact) {
    return <div className="empty-state">Файлы еще не скачивались.</div>;
  }

  return (
    <div className="kv-list compact-list">
      {artifact.directory && (
        <div>
          <span>Папка</span>
          <strong>{artifact.directory}</strong>
        </div>
      )}
      {artifact.pdfPath && (
        <div>
          <span>PDF</span>
          <strong>{artifact.pdfPath}</strong>
        </div>
      )}
      {artifact.csvPath && (
        <div>
          <span>CSV</span>
          <strong>{artifact.csvPath}</strong>
        </div>
      )}
      {artifact.xlsPath && (
        <div>
          <span>XLS</span>
          <strong>{artifact.xlsPath}</strong>
        </div>
      )}
      {!artifact.directory && !artifact.pdfPath && !artifact.csvPath && !artifact.xlsPath && (
        <div className="empty-state">Приложение подготовило результат, но пути к файлам пока не вернулись.</div>
      )}
    </div>
  );
}

function buildUserIssues(dependencies: DependencyStatus[]): UserIssue[] {
  const issues: UserIssue[] = [];

  for (const dependency of dependencies) {
    if (dependency.name === 'yandex-browser' && !dependency.available) {
      issues.push({
        key: dependency.name,
        title: 'Яндекс Браузер',
        text: 'Браузер не найден. Установите Яндекс Браузер и выполните в нем вход в Контур.Маркировку.',
      });
    }

    if (dependency.name === 'target-platform' && !dependency.available) {
      issues.push({
        key: dependency.name,
        title: 'Операционная система',
        text: 'Полноценная работа приложения поддерживается только на Windows.',
      });
    }

    if (dependency.name === 'cryptopro') {
      if (!dependency.available) {
        issues.push({
          key: dependency.name,
          title: 'CryptoPro',
          text: 'CryptoPro не найден. Установите CryptoPro CSP, чтобы подписывать документы и работать с вводом в оборот.',
        });
      } else if (dependency.status === 'installed-no-cert') {
        issues.push({
          key: `${dependency.name}-cert`,
          title: 'Сертификат подписи',
          text: 'CryptoPro установлен, но сертификат подписи не найден в хранилище текущего пользователя.',
        });
      }
    }

    if (!dependency.available && (dependency.name === 'base-url' || dependency.name === 'warehouse-id' || dependency.name === 'organization-id')) {
      issues.push({
        key: dependency.name,
        title: 'Конфигурация приложения',
        text: 'Приложение настроено не полностью. Нужно проверить служебные параметры подключения.',
      });
    }
  }

  return issues;
}

function parsePositions(text: string): PositionData[] {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    throw new Error('Добавьте хотя бы одну позицию. Формат строки: Наименование|GTIN');
  }

  return lines.map((line, index) => {
    const [namePart, gtinPart, ...rest] = line.split('|');
    const name = namePart?.trim() ?? '';
    const gtin = [gtinPart, ...rest].join('|').trim();
    if (!name || !gtin) {
      throw new Error(`Некорректная строка ${index + 1}. Ожидается формат: Наименование|GTIN`);
    }
    return { name, gtin };
  });
}

function orderToPositionsText(order: OrderRecord): string {
  if (order.positions && order.positions.length > 0) {
    return order.positions.map((position) => `${position.name}|${position.gtin}`).join('\n');
  }
  if (order.full_name && order.gtin) {
    return `${order.full_name}|${order.gtin}`;
  }
  return '';
}

function formatDate(value?: string): string {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString('ru-RU');
}

function localizeDetailLabel(key: string): string {
  const labels: Record<string, string> = {
    codesOrderId: 'ID заказа',
    documentId: 'ID документа',
    orderName: 'Заказ',
    documentNumber: 'Номер документа',
    positionsCount: 'Количество позиций',
    introductionId: 'ID документа ввода',
    status: 'Статус',
    thumbprint: 'Сертификат',
  };
  return labels[key] ?? key;
}

function localizeStatus(status?: string): string {
  if (!status) {
    return 'Статус не указан';
  }

  const normalized = status.trim();
  const map: Record<string, string> = {
    created: 'Создан',
    checked: 'Проверен',
    inProgress: 'В обработке',
    noErrors: 'Без ошибок',
    doesNotHaveErrors: 'Без ошибок',
    tsdProcessStart: 'Отправлен в ТСД',
    production: 'Ввод в оборот',
  };

  return map[normalized] ?? normalized;
}

function localizeMessage(message: string): string {
  const normalized = message.trim();
  const exact: Record<string, string> = {
    'Unknown error': 'Неизвестная ошибка',
    'Session refreshed': 'Сессия обновлена.',
    'Session not initialized yet.': 'Сессия еще не инициализирована.',
    'Captured cookies from browser profile successfully.': 'Сессия успешно получена из профиля браузера.',
    'Backend bindings are not connected in browser-only preview.': 'В режиме предпросмотра backend еще не подключен.',
    'Session refresh is available only from the Wails desktop runtime.': 'Обновление сессии доступно только в установленном desktop-приложении.',
    'Order status check is available only from the Wails desktop runtime.': 'Проверка статуса доступна только в установленном desktop-приложении.',
    'Introduction flow is available only from the Wails desktop runtime.': 'Ввод в оборот доступен только в установленном desktop-приложении.',
    'TSD flow is available only from the Wails desktop runtime.': 'Работа с ТСД доступна только в установленном desktop-приложении.',
    'Introduction flow completed': 'Ввод в оборот завершен.',
    'TSD task created': 'Задача на ТСД создана.',
    'order not found': 'Заказ не найден.',
    'codesOrderId is required': 'Не указан ID заказа.',
  };

  if (exact[normalized]) {
    return exact[normalized];
  }

  if (normalized.startsWith('Missing required cookies:')) {
    return `Не удалось получить обязательные cookies: ${normalized.replace('Missing required cookies:', '').trim()}`;
  }

  if (normalized.includes('is not registered in organization')) {
    return 'Сертификат не зарегистрирован в организации.';
  }

  if (normalized.includes('CAdESCOM') && normalized.includes('available only on Windows')) {
    return 'Подпись через CryptoPro доступна только на Windows.';
  }

  return normalized;
}

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return localizeMessage(error.message);
  }
  if (typeof error === 'string') {
    return localizeMessage(error);
  }
  return 'Неизвестная ошибка';
}

export default App;

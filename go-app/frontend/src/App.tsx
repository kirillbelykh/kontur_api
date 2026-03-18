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
  { id: 'overview', title: 'Главная', blurb: 'Состояние приложения и быстрый доступ к основным действиям' },
  { id: 'orders', title: 'Заказ кодов', blurb: 'Создание, проверка и скачивание заказов' },
  { id: 'introduction', title: 'Ввод в оборот', blurb: 'Подготовка и отправка документа ввода в оборот' },
  { id: 'tsd', title: 'ТСД', blurb: 'Создание задач на терминал сбора данных' },
  { id: 'aggregation', title: 'Коды агрегации', blurb: 'Поиск и выгрузка кодов агрегации' },
  { id: 'history', title: 'История', blurb: 'Общая история заказов и быстрые действия по строкам' },
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

  const currentSection = useMemo(
    () => sections.find((item) => item.id === section) ?? sections[0],
    [section],
  );

  const userIssues = useMemo(() => buildUserIssues(dependencies), [dependencies]);

  const selectedOrderMeta = useMemo(() => {
    if (!selectedOrder) {
      return 'Заказ не выбран';
    }
    return `${selectedOrder.order_name || 'Без названия'} · ${selectedOrder.document_id}`;
  }, [selectedOrder]);

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

  async function refreshDataAfterMutation() {
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
      }
      setInfo(`Создано записей: ${created.length}`, 'ok');
      await refreshDataAfterMutation();
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
      await refreshDataAfterMutation();
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
      await refreshDataAfterMutation();
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
      await refreshDataAfterMutation();
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
      await refreshDataAfterMutation();
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
      await refreshDataAfterMutation();
    });
  }

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">КМ</div>
          <div>
            <h1>Kontur Go</h1>
            <p>клиент для работы с Контур.Маркировкой</p>
          </div>
        </div>

        <nav className="nav">
          {sections.map((item) => (
            <button
              key={item.id}
              className={section === item.id ? 'nav-item active' : 'nav-item'}
              onClick={() => setSection(item.id)}
              type="button"
            >
              <span>{item.title}</span>
              <small>{item.blurb}</small>
            </button>
          ))}
        </nav>

        <div className="sidebar-card">
          <span className="label">Сессия</span>
          <strong>{appState?.session.available ? 'Подключена' : 'Требуется вход'}</strong>
          <p>{localizeMessage(appState?.session.message ?? 'Ожидание запуска приложения')}</p>
          {appState?.session.expiresAt && appState.session.available && <p>Действует до {formatDate(appState.session.expiresAt)}</p>}
          <button type="button" onClick={handleRefreshSession} disabled={isBusy}>
            {busyAction === 'refresh-session' ? 'Обновление…' : 'Обновить сессию'}
          </button>
        </div>

        {selectedOrder && (
          <div className="sidebar-card">
            <span className="label">Выбранный заказ</span>
            <strong>{selectedOrder.order_name || selectedOrder.document_id}</strong>
            <p>{selectedOrder.document_id}</p>
            <button type="button" className="secondary" onClick={clearSelection} disabled={isBusy}>
              Снять выбор
            </button>
          </div>
        )}
      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Контур.Маркировка</p>
            <h2>{currentSection.title}</h2>
            <p className="topbar-copy">{currentSection.blurb}</p>
          </div>
          <div className="topbar-actions">
            {section === 'history' && (
              <div className="command-bar">
                <input
                  value={historyFilter.search}
                  onChange={(event) => setHistoryFilter((current) => ({ ...current, search: event.target.value }))}
                  placeholder="Поиск по названию, GTIN или ID документа"
                />
              </div>
            )}
            <button type="button" className="secondary" onClick={() => void refreshDataAfterMutation()} disabled={isBusy}>
              Обновить данные
            </button>
          </div>
        </header>

        {notice && <div className={`notice ${notice.tone}`}>{notice.text}</div>}

        <section className="hero-grid">
          <article className="metric-card">
            <span>Заказы в истории</span>
            <strong>{appState?.ordersTotal ?? 0}</strong>
          </article>
          <article className="metric-card">
            <span>Без ТСД</span>
            <strong>{appState?.ordersWithoutTsd ?? 0}</strong>
          </article>
          <article className="metric-card accent">
            <span>Сессия</span>
            <strong>{appState?.session.available ? 'Активна' : 'Не подключена'}</strong>
          </article>
        </section>

        {section === 'overview' && (
          <section className="content-grid wide-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Быстрые действия</h3>
                <span className="pill ghost">Основное</span>
              </div>
              <div className="button-row wrap">
                <button type="button" onClick={handleRefreshSession} disabled={isBusy}>
                  {busyAction === 'refresh-session' ? 'Обновление…' : 'Обновить сессию'}
                </button>
                <button type="button" className="secondary" onClick={() => setSection('history')}>
                  Открыть историю
                </button>
                <button type="button" className="secondary" onClick={() => setSection('orders')}>
                  Заказать коды
                </button>
              </div>
              <div className="surface-block">
                <span className="label">Текущий выбор</span>
                <strong>{selectedOrderMeta}</strong>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Что можно сделать</h3>
                <span className="pill ghost">Сценарии</span>
              </div>
              <div className="task-rail">
                <div>
                  <strong>Заказ кодов</strong>
                  <span>Создайте заказ, проверьте его статус и скачайте готовые файлы.</span>
                </div>
                <div>
                  <strong>Ввод в оборот и ТСД</strong>
                  <span>Используйте выбранный заказ из истории, чтобы не вводить ID вручную.</span>
                </div>
                <div>
                  <strong>Коды агрегации</strong>
                  <span>Ищите коды по названию или количеству и сразу выгружайте результат в файл.</span>
                </div>
              </div>
            </article>

            <article className="panel span-two">
              <div className="panel-head">
                <h3>Что требует внимания</h3>
                <span className="pill ghost">{userIssues.length}</span>
              </div>
              {userIssues.length ? (
                <div className="dependency-grid two-up">
                  {userIssues.map((issue) => (
                    <div key={issue.key} className="dependency-card warn">
                      <strong>{issue.title}</strong>
                      <p>{issue.text}</p>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="empty-state">Все обязательные компоненты готовы к работе.</div>
              )}
            </article>
          </section>
        )}

        {section === 'orders' && (
          <section className="content-grid wide-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Новый заказ</h3>
                <span className="pill">Коды маркировки</span>
              </div>
              <div className="form-grid">
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
              <div className="button-row">
                <button type="button" onClick={handleCreateDraft} disabled={isBusy || !draft.orderName.trim()}>
                  {busyAction === 'create-order' ? 'Создание…' : 'Создать заказ'}
                </button>
                <button type="button" className="secondary" onClick={() => setDraft(initialDraft)} disabled={isBusy}>
                  Очистить
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Действия по заказу</h3>
                <span className="pill ghost">Статус и файлы</span>
              </div>
              <div className="form-grid single-column compact-grid">
                <Field label="ID документа">
                  <input value={orderActions.documentId} onChange={(event) => setOrderActions({ documentId: event.target.value })} />
                </Field>
              </div>
              <div className="button-row wrap">
                <button type="button" onClick={() => handleCheckOrderStatus()} disabled={isBusy || !orderActions.documentId.trim()}>
                  {busyAction === 'check-order-status' ? 'Проверка…' : 'Проверить статус'}
                </button>
                <button type="button" onClick={() => handleDownloadOrder()} disabled={isBusy || !orderActions.documentId.trim()}>
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
                <span className={`pill ${orderStatusResult?.ok ? '' : 'ghost'}`}>{orderStatusResult ? (orderStatusResult.ok ? 'Готово' : 'Статус') : 'Ожидание'}</span>
              </div>
              <ResultStatus result={orderStatusResult} emptyText="Проверка статуса еще не выполнялась." />
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Скачанные файлы</h3>
                <span className="pill ghost">Результат</span>
              </div>
              <ArtifactResult artifact={downloadResult} />
            </article>
          </section>
        )}

        {section === 'introduction' && (
          <section className="content-grid wide-grid">
            <article className="panel span-two">
              <div className="panel-head">
                <h3>Ввод в оборот</h3>
                <span className="pill">Документ</span>
              </div>
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
              <div className="button-row wrap">
                <button type="button" onClick={handleRunIntroduction} disabled={isBusy || !introductionForm.codesOrderId.trim()}>
                  {busyAction === 'run-introduction' ? 'Отправка…' : 'Ввести в оборот'}
                </button>
                <button type="button" className="secondary" onClick={() => setIntroductionForm(initialIntroductionForm)} disabled={isBusy}>
                  Очистить
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Подставить выбранный заказ
                </button>
              </div>
            </article>

            <article className="panel span-two">
              <div className="panel-head">
                <h3>Результат ввода в оборот</h3>
                <span className={`pill ${introductionResult?.ok ? '' : 'ghost'}`}>{introductionResult ? (introductionResult.ok ? 'Готово' : 'Статус') : 'Ожидание'}</span>
              </div>
              <ResultStatus result={introductionResult} emptyText="Ввод в оборот еще не выполнялся." />
            </article>
          </section>
        )}

        {section === 'tsd' && (
          <section className="content-grid wide-grid">
            <article className="panel span-two">
              <div className="panel-head">
                <h3>Задача на ТСД</h3>
                <span className="pill">Позиции</span>
              </div>
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
              <Field label="Позиции (каждая строка: Наименование|GTIN)">
                <textarea
                  rows={7}
                  value={tsdForm.positionsText}
                  onChange={(event) => setTsdForm({ ...tsdForm, positionsText: event.target.value })}
                  placeholder={'Кресло-коляска XL|04601234567890\nКресло-коляска S|04601234567891'}
                />
              </Field>
              <div className="button-row wrap">
                <button type="button" onClick={handleCreateTsdTask} disabled={isBusy || !tsdForm.codesOrderId.trim()}>
                  {busyAction === 'create-tsd-task' ? 'Создание…' : 'Создать задачу'}
                </button>
                <button type="button" className="secondary" onClick={() => setTsdForm(initialTsdForm)} disabled={isBusy}>
                  Очистить
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Подставить выбранный заказ
                </button>
              </div>
            </article>

            <article className="panel span-two">
              <div className="panel-head">
                <h3>Результат по ТСД</h3>
                <span className={`pill ${tsdResult?.ok ? '' : 'ghost'}`}>{tsdResult ? (tsdResult.ok ? 'Готово' : 'Статус') : 'Ожидание'}</span>
              </div>
              <ResultStatus result={tsdResult} emptyText="Задача на ТСД еще не создавалась." />
            </article>
          </section>
        )}

        {section === 'aggregation' && (
          <section className="content-grid wide-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Выгрузка кодов агрегации</h3>
                <span className="pill">Поиск</span>
              </div>
              <div className="form-grid compact-grid">
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
                <Field label="Имя файла выгрузки">
                  <input value={aggregationQuery.filename} onChange={(event) => setAggregationQuery({ ...aggregationQuery, filename: event.target.value })} placeholder="необязательно.csv" />
                </Field>
              </div>
              <div className="button-row wrap">
                <button type="button" onClick={handleAggregationExport} disabled={isBusy || !aggregationQuery.targetValue.trim()}>
                  {busyAction === 'search-aggregation' ? 'Выгрузка…' : 'Найти и выгрузить'}
                </button>
                <button type="button" className="secondary" onClick={() => setAggregationQuery(initialAggregationQuery)} disabled={isBusy}>
                  Очистить
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Результат выгрузки</h3>
                <span className="pill ghost">{aggregationResult?.records.length ?? 0}</span>
              </div>
              {aggregationResult ? (
                <>
                  <div className="kv-list compact-list">
                    <div><span>Папка</span><strong>{aggregationResult.directory || 'не указана'}</strong></div>
                    <div><span>Файл</span><strong>{aggregationResult.filename || 'не указан'}</strong></div>
                  </div>
                  <div className="mini-table aggregation-table">
                    {aggregationResult.records.slice(0, 12).map((record) => (
                      <div className="mini-row" key={`${record.aggregateCode}-${record.documentId ?? ''}`}>
                        <div>
                          <strong>{record.aggregateCode}</strong>
                          <span>{record.comment || record.documentId || 'без комментария'}</span>
                        </div>
                        <div>
                          <strong>{record.status || 'без статуса'}</strong>
                          <span>{record.updatedDate || record.createdDate || 'дата не указана'}</span>
                        </div>
                      </div>
                    ))}
                    {!aggregationResult.records.length && <div className="empty-state">По запросу ничего не найдено.</div>}
                  </div>
                </>
              ) : (
                <div className="empty-state">Выгрузка кодов агрегации еще не выполнялась.</div>
              )}
            </article>
          </section>
        )}

        {section === 'history' && (
          <section className="panel history-panel">
            <div className="panel-head">
              <h3>История заказов</h3>
              <span className="pill ghost">{history.length}</span>
            </div>
            <div className="toolbar-row">
              <label className="check-row">
                <input
                  type="checkbox"
                  checked={historyFilter.onlyWithoutTsd}
                  onChange={(event) => setHistoryFilter((current) => ({ ...current, onlyWithoutTsd: event.target.checked }))}
                />
                <span>Только без ТСД</span>
              </label>
              <input
                value={historyFilter.status}
                onChange={(event) => setHistoryFilter((current) => ({ ...current, status: event.target.value }))}
                placeholder="Фильтр по статусу"
              />
              <button type="button" className="secondary" onClick={() => reloadHistory(historyFilter.search, historyFilter.status, historyFilter.onlyWithoutTsd)} disabled={isBusy}>
                Обновить
              </button>
            </div>
            <div className="history-table detailed-history">
              {history.map((item) => (
                <div className="history-row expanded" key={item.document_id}>
                  <div>
                    <strong>{item.order_name || item.document_id}</strong>
                    <span>{item.full_name || item.gtin || 'без описания'}</span>
                  </div>
                  <div>
                    <strong>{item.status}</strong>
                    <span>{formatDate(item.updated_at) || 'дата не указана'}</span>
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
        text: 'Приложение настроено не полностью. Требуется проверить служебные параметры подключения.',
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

import { startTransition, useDeferredValue, useEffect, useMemo, useState, type ReactNode } from 'react';

import {
  checkDependencies,
  checkOrderStatus,
  createOrders,
  createTsdTask,
  downloadOrder,
  getAppState,
  listHistory,
  markTsdCreated,
  prepareArtifactDirectory,
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

type Section = 'overview' | 'orders' | 'introduction' | 'tsd' | 'aggregation' | 'history' | 'setup';

type OrderActionsState = {
  documentId: string;
  artifactOrderName: string;
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

const sections: { id: Section; title: string; blurb: string }[] = [
  { id: 'overview', title: 'Workbench', blurb: 'Runtime state and quick actions' },
  { id: 'orders', title: 'Orders', blurb: 'Create, check and download code orders' },
  { id: 'introduction', title: 'Introduction', blurb: 'Run production introduction flow' },
  { id: 'tsd', title: 'TSD', blurb: 'Create TSD tasks with positions' },
  { id: 'aggregation', title: 'Aggregation', blurb: 'Search and export aggregate codes' },
  { id: 'history', title: 'History', blurb: 'Shared records and row actions' },
  { id: 'setup', title: 'Setup', blurb: 'Dependencies and filesystem helpers' },
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
  artifactOrderName: '',
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
  const [logs, setLogs] = useState<string[]>([
    'go-app booted',
    'frontend is now wired to all available Wails backend operations',
  ]);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ tone: 'ok' | 'error' | 'info'; text: string } | null>(null);
  const [orderStatusResult, setOrderStatusResult] = useState<OperationStatus | null>(null);
  const [downloadResult, setDownloadResult] = useState<DownloadArtifact | null>(null);
  const [introductionResult, setIntroductionResult] = useState<OperationStatus | null>(null);
  const [tsdResult, setTsdResult] = useState<OperationStatus | null>(null);
  const [aggregationResult, setAggregationResult] = useState<AggregationExportResult | null>(null);
  const [preparedDirectory, setPreparedDirectory] = useState<string>('');

  const deferredSearch = useDeferredValue(historyFilter.search);
  const isBusy = busyAction !== null;

  useEffect(() => {
    void reloadDashboard();
  }, []);

  useEffect(() => {
    void reloadHistory(deferredSearch, historyFilter.status, historyFilter.onlyWithoutTsd);
  }, [deferredSearch, historyFilter.status, historyFilter.onlyWithoutTsd]);

  const highlightedDependencies = useMemo(
    () => dependencies.filter((item) => !item.available),
    [dependencies],
  );

  const selectedOrderMeta = useMemo(() => {
    if (!selectedOrder) {
      return 'No order selected';
    }
    return `${selectedOrder.order_name || 'Untitled'} · ${selectedOrder.document_id}`;
  }, [selectedOrder]);

  async function reloadDashboard() {
    const [state, deps] = await Promise.all([getAppState(), checkDependencies()]);
    startTransition(() => {
      setAppState(state);
      setDependencies(deps);
    });
  }

  async function reloadHistory(search = '', status = '', onlyWithoutTsd = false) {
    const records = await listHistory({ search, status, onlyWithoutTsd });
    startTransition(() => setHistory(records));
  }

  function appendLog(line: string) {
    startTransition(() => {
      setLogs((current) => [`${new Date().toLocaleTimeString()}  ${line}`, ...current].slice(0, 18));
    });
  }

  function setInfo(text: string, tone: 'ok' | 'error' | 'info' = 'info') {
    setNotice({ tone, text });
  }

  function pickOrder(order: OrderRecord) {
    setSelectedOrder(order);
    setOrderActions((current) => ({
      ...current,
      documentId: order.document_id,
      artifactOrderName: order.order_name || current.artifactOrderName,
    }));
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
    appendLog(`selected: ${order.document_id}`);
    setInfo(`Selected order ${order.document_id}`, 'info');
  }

  async function runAction<T>(label: string, job: () => Promise<T>, onSuccess: (result: T) => Promise<void> | void) {
    setBusyAction(label);
    try {
      const result = await job();
      await onSuccess(result);
    } catch (error) {
      const message = extractErrorMessage(error);
      appendLog(`${label}: error: ${message}`);
      setInfo(message, 'error');
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
      appendLog(`session: ${result.message ?? result.source}`);
      setInfo(result.message ?? 'Session refreshed', result.available ? 'ok' : 'info');
      await reloadDashboard();
    });
  }

  async function handleCreateDraft() {
    await runAction('create-order', () => createOrders([draft]), async (created) => {
      const first = created[0];
      setDraft(initialDraft);
      if (first) {
        setOrderActions((current) => ({
          ...current,
          documentId: first.document_id,
          artifactOrderName: first.order_name || current.artifactOrderName,
        }));
      }
      appendLog(`orders: created ${created.length} record(s)`);
      setInfo(`Created ${created.length} order record(s)`, 'ok');
      await refreshDataAfterMutation();
    });
  }

  async function handleCheckOrderStatus(documentId = orderActions.documentId) {
    if (!documentId.trim()) {
      setInfo('Document ID is required for status check', 'error');
      return;
    }
    await runAction('check-order-status', () => checkOrderStatus(documentId.trim()), async (result) => {
      setOrderStatusResult(result);
      appendLog(`orders: status for ${documentId} -> ${result.message}`);
      setInfo(result.message, result.ok ? 'ok' : 'info');
      await refreshDataAfterMutation();
    });
  }

  async function handleDownloadOrder(documentId = orderActions.documentId) {
    if (!documentId.trim()) {
      setInfo('Document ID is required for download', 'error');
      return;
    }
    await runAction('download-order', () => downloadOrder(documentId.trim()), async (artifact) => {
      setDownloadResult(artifact);
      appendLog(`orders: downloaded artifacts for ${documentId}`);
      setInfo(artifact.directory ? `Artifacts prepared in ${artifact.directory}` : 'Artifacts prepared', 'ok');
      await refreshDataAfterMutation();
    });
  }

  async function handlePrepareDirectory() {
    const orderName = orderActions.artifactOrderName.trim() || selectedOrder?.order_name || orderActions.documentId.trim();
    if (!orderName) {
      setInfo('Order name is required to prepare artifact directory', 'error');
      return;
    }
    await runAction('prepare-directory', () => prepareArtifactDirectory(orderName), (directory) => {
      setPreparedDirectory(directory);
      appendLog(`downloads: prepared directory ${directory}`);
      setInfo(`Prepared directory ${directory}`, 'ok');
    });
  }

  async function handleRunIntroduction() {
    if (!introductionForm.codesOrderId.trim()) {
      setInfo('Codes order ID is required for introduction flow', 'error');
      return;
    }
    await runAction('run-introduction', () => runIntroduction(introductionForm as IntroductionRequest), async (result) => {
      setIntroductionResult(result);
      appendLog(`introduction: ${result.message}`);
      setInfo(result.message, result.ok ? 'ok' : 'info');
      await refreshDataAfterMutation();
    });
  }

  async function handleCreateTsdTask() {
    if (!tsdForm.codesOrderId.trim()) {
      setInfo('Codes order ID is required for TSD flow', 'error');
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
      appendLog(`tsd: ${result.message}`);
      setInfo(result.message, result.ok ? 'ok' : 'info');
      await refreshDataAfterMutation();
    });
  }

  async function handleAggregationExport() {
    if (!aggregationQuery.targetValue.trim()) {
      setInfo('Target value is required for aggregation export', 'error');
      return;
    }
    await runAction('search-aggregation', () => searchAndExportAggregates(aggregationQuery), (result) => {
      setAggregationResult(result);
      appendLog(`aggregation: exported ${result.records.length} code(s) to ${result.filename}`);
      setInfo(`Exported ${result.records.length} aggregate code(s)`, 'ok');
    });
  }

  async function handleMarkTsdCreated(order: OrderRecord) {
    await runAction('mark-tsd-created', () => markTsdCreated(order.document_id, `INTRO-${Date.now()}`), async (updated) => {
      appendLog(`history: ${updated.document_id} marked as TSD created`);
      setInfo(`Marked ${updated.document_id} as TSD created`, 'ok');
      await refreshDataAfterMutation();
    });
  }

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">KG</div>
          <div>
            <h1>Kontur Go</h1>
            <p>desktop workbench with full backend wiring</p>
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
          <span className="label">Session</span>
          <strong>{appState?.session.available ? 'Ready' : 'Detached'}</strong>
          <p>{appState?.session.message ?? 'Waiting for backend runtime'}</p>
          <button type="button" onClick={handleRefreshSession} disabled={isBusy}>
            {busyAction === 'refresh-session' ? 'Refreshing…' : 'Refresh session'}
          </button>
        </div>
      </aside>

      <main className="workspace">
        <header className="topbar">
          <div>
            <p className="eyebrow">Go Alternative Desktop App</p>
            <h2>{sections.find((item) => item.id === section)?.title}</h2>
          </div>
          <div className="command-bar">
            <input
              value={historyFilter.search}
              onChange={(event) => setHistoryFilter((current) => ({ ...current, search: event.target.value }))}
              placeholder="History search / quick jump"
            />
          </div>
        </header>

        {notice && <div className={`notice ${notice.tone}`}>{notice.text}</div>}

        <section className="hero-grid">
          <article className="metric-card">
            <span>Orders in shared history</span>
            <strong>{appState?.ordersTotal ?? 0}</strong>
          </article>
          <article className="metric-card">
            <span>Orders without TSD</span>
            <strong>{appState?.ordersWithoutTsd ?? 0}</strong>
          </article>
          <article className="metric-card accent">
            <span>Host target</span>
            <strong>{dependencies.find((item) => item.name === 'target-platform')?.status ?? 'unknown'}</strong>
          </article>
        </section>

        {section === 'overview' && (
          <section className="content-grid wide-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Runtime Snapshot</h3>
                <span className="pill">{isBusy ? busyAction : 'idle'}</span>
              </div>
              <div className="kv-list">
                <div><span>Repo root</span><strong>{appState?.repoRoot ?? 'n/a'}</strong></div>
                <div><span>History path</span><strong>{appState?.historyPath ?? 'n/a'}</strong></div>
                <div><span>Last updated</span><strong>{appState?.lastUpdated ?? 'n/a'}</strong></div>
                <div><span>Selected order</span><strong>{selectedOrderMeta}</strong></div>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Exposed Backend APIs</h3>
                <span className="pill ghost">live wiring</span>
              </div>
              <div className="task-rail">
                <div>
                  <strong>AppState / Auth / System</strong>
                  <span>dashboard state, dependency probes, session refresh</span>
                </div>
                <div>
                  <strong>Orders / Downloads</strong>
                  <span>create, status check, artifact download, directory preparation</span>
                </div>
                <div>
                  <strong>Introduction / TSD / Aggregation</strong>
                  <span>remote transport forms are now available directly in the UI</span>
                </div>
                <div>
                  <strong>History</strong>
                  <span>shared list, selection context and TSD marking</span>
                </div>
              </div>
            </article>

            <article className="panel span-two">
              <div className="panel-head">
                <h3>Dependency Watch</h3>
                <span className="pill ghost">{highlightedDependencies.length} issue(s)</span>
              </div>
              <div className="dependency-grid two-up">
                {dependencies.map((item) => (
                  <div key={item.name} className={item.available ? 'dependency-card ok' : 'dependency-card warn'}>
                    <strong>{item.name}</strong>
                    <span>{item.status || 'missing'}</span>
                    <p>{item.hint}</p>
                  </div>
                ))}
              </div>
            </article>
          </section>
        )}

        {section === 'orders' && (
          <section className="content-grid wide-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Create Order</h3>
                <span className="pill">OrdersAPI.Create</span>
              </div>
              <div className="form-grid">
                <Field label="Order name">
                  <input value={draft.orderName} onChange={(event) => setDraft({ ...draft, orderName: event.target.value })} />
                </Field>
                <Field label="Simplified name">
                  <input value={draft.simplifiedName} onChange={(event) => setDraft({ ...draft, simplifiedName: event.target.value })} />
                </Field>
                <Field label="Full name">
                  <input value={draft.fullName} onChange={(event) => setDraft({ ...draft, fullName: event.target.value })} />
                </Field>
                <Field label="GTIN">
                  <input value={draft.gtin} onChange={(event) => setDraft({ ...draft, gtin: event.target.value })} />
                </Field>
                <Field label="TNVED">
                  <input value={draft.tnvedCode} onChange={(event) => setDraft({ ...draft, tnvedCode: event.target.value })} />
                </Field>
                <Field label="Codes count">
                  <input type="number" min={1} value={draft.codesCount} onChange={(event) => setDraft({ ...draft, codesCount: Number(event.target.value) || 1 })} />
                </Field>
                <Field label="Size">
                  <input value={draft.size} onChange={(event) => setDraft({ ...draft, size: event.target.value })} />
                </Field>
                <Field label="Units per pack">
                  <input value={draft.unitsPerPack} onChange={(event) => setDraft({ ...draft, unitsPerPack: event.target.value })} />
                </Field>
              </div>
              <div className="button-row">
                <button type="button" onClick={handleCreateDraft} disabled={isBusy || !draft.orderName.trim()}>
                  {busyAction === 'create-order' ? 'Creating…' : 'Create order'}
                </button>
                <button type="button" className="secondary" onClick={() => setDraft(initialDraft)} disabled={isBusy}>
                  Reset form
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Order Operations</h3>
                <span className="pill ghost">CheckStatus / Download / PrepareArtifactDirectory</span>
              </div>
              <div className="form-grid single-column compact-grid">
                <Field label="Document ID">
                  <input value={orderActions.documentId} onChange={(event) => setOrderActions({ ...orderActions, documentId: event.target.value })} />
                </Field>
                <Field label="Artifact order name">
                  <input value={orderActions.artifactOrderName} onChange={(event) => setOrderActions({ ...orderActions, artifactOrderName: event.target.value })} />
                </Field>
              </div>
              <div className="button-row wrap">
                <button type="button" onClick={() => handleCheckOrderStatus()} disabled={isBusy || !orderActions.documentId.trim()}>
                  {busyAction === 'check-order-status' ? 'Checking…' : 'Check status'}
                </button>
                <button type="button" onClick={() => handleDownloadOrder()} disabled={isBusy || !orderActions.documentId.trim()}>
                  {busyAction === 'download-order' ? 'Downloading…' : 'Download artifacts'}
                </button>
                <button type="button" className="secondary" onClick={handlePrepareDirectory} disabled={isBusy}>
                  {busyAction === 'prepare-directory' ? 'Preparing…' : 'Prepare directory'}
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Use selected order
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Latest Status Result</h3>
                <span className={`pill ${orderStatusResult?.ok ? '' : 'ghost'}`}>{orderStatusResult ? (orderStatusResult.ok ? 'ok' : 'info') : 'idle'}</span>
              </div>
              <ResultStatus result={orderStatusResult} emptyText="No status check executed yet." />
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Latest Download Result</h3>
                <span className="pill ghost">artifact paths</span>
              </div>
              <ArtifactResult artifact={downloadResult} preparedDirectory={preparedDirectory} />
            </article>
          </section>
        )}

        {section === 'introduction' && (
          <section className="content-grid wide-grid">
            <article className="panel span-two">
              <div className="panel-head">
                <h3>Run Introduction</h3>
                <span className="pill">IntroductionAPI.Run</span>
              </div>
              <div className="form-grid three-up">
                <Field label="Codes order ID">
                  <input value={introductionForm.codesOrderId} onChange={(event) => setIntroductionForm({ ...introductionForm, codesOrderId: event.target.value })} />
                </Field>
                <Field label="Organization ID">
                  <input value={introductionForm.organizationId} onChange={(event) => setIntroductionForm({ ...introductionForm, organizationId: event.target.value })} />
                </Field>
                <Field label="Thumbprint">
                  <input value={introductionForm.thumbprint} onChange={(event) => setIntroductionForm({ ...introductionForm, thumbprint: event.target.value })} />
                </Field>
                <Field label="Document number">
                  <input value={introductionForm.productionPatch.documentNumber} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, documentNumber: event.target.value } })} />
                </Field>
                <Field label="Production date">
                  <input type="date" value={introductionForm.productionPatch.productionDate} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, productionDate: event.target.value } })} />
                </Field>
                <Field label="Expiration date">
                  <input type="date" value={introductionForm.productionPatch.expirationDate} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, expirationDate: event.target.value } })} />
                </Field>
                <Field label="Batch number">
                  <input value={introductionForm.productionPatch.batchNumber} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, batchNumber: event.target.value } })} />
                </Field>
                <Field label="TNVED code">
                  <input value={introductionForm.productionPatch.TnvedCode} onChange={(event) => setIntroductionForm({ ...introductionForm, productionPatch: { ...introductionForm.productionPatch, TnvedCode: event.target.value } })} />
                </Field>
              </div>
              <div className="button-row wrap">
                <button type="button" onClick={handleRunIntroduction} disabled={isBusy || !introductionForm.codesOrderId.trim()}>
                  {busyAction === 'run-introduction' ? 'Running…' : 'Run introduction'}
                </button>
                <button type="button" className="secondary" onClick={() => setIntroductionForm(initialIntroductionForm)} disabled={isBusy}>
                  Reset form
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Use selected order
                </button>
              </div>
            </article>

            <article className="panel span-two">
              <div className="panel-head">
                <h3>Introduction Result</h3>
                <span className={`pill ${introductionResult?.ok ? '' : 'ghost'}`}>{introductionResult ? (introductionResult.ok ? 'ok' : 'info') : 'idle'}</span>
              </div>
              <ResultStatus result={introductionResult} emptyText="Introduction flow has not run yet." />
            </article>
          </section>
        )}

        {section === 'tsd' && (
          <section className="content-grid wide-grid">
            <article className="panel span-two">
              <div className="panel-head">
                <h3>Create TSD Task</h3>
                <span className="pill">TSDAPI.CreateTask</span>
              </div>
              <div className="form-grid three-up">
                <Field label="Codes order ID">
                  <input value={tsdForm.codesOrderId} onChange={(event) => setTsdForm({ ...tsdForm, codesOrderId: event.target.value })} />
                </Field>
                <Field label="Document number">
                  <input value={tsdForm.productionPatch.documentNumber} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, documentNumber: event.target.value } })} />
                </Field>
                <Field label="TNVED code">
                  <input value={tsdForm.productionPatch.TnvedCode} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, TnvedCode: event.target.value } })} />
                </Field>
                <Field label="Production date">
                  <input type="date" value={tsdForm.productionPatch.productionDate} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, productionDate: event.target.value } })} />
                </Field>
                <Field label="Expiration date">
                  <input type="date" value={tsdForm.productionPatch.expirationDate} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, expirationDate: event.target.value } })} />
                </Field>
                <Field label="Batch number">
                  <input value={tsdForm.productionPatch.batchNumber} onChange={(event) => setTsdForm({ ...tsdForm, productionPatch: { ...tsdForm.productionPatch, batchNumber: event.target.value } })} />
                </Field>
              </div>
              <Field label="Positions (one per line: Name|GTIN)">
                <textarea
                  rows={7}
                  value={tsdForm.positionsText}
                  onChange={(event) => setTsdForm({ ...tsdForm, positionsText: event.target.value })}
                  placeholder={'Wheel Chair XL|04601234567890\nWheel Chair S|04601234567891'}
                />
              </Field>
              <div className="button-row wrap">
                <button type="button" onClick={handleCreateTsdTask} disabled={isBusy || !tsdForm.codesOrderId.trim()}>
                  {busyAction === 'create-tsd-task' ? 'Creating…' : 'Create TSD task'}
                </button>
                <button type="button" className="secondary" onClick={() => setTsdForm(initialTsdForm)} disabled={isBusy}>
                  Reset form
                </button>
                <button type="button" className="ghost-button" onClick={() => selectedOrder && pickOrder(selectedOrder)} disabled={isBusy || !selectedOrder}>
                  Use selected order
                </button>
              </div>
            </article>

            <article className="panel span-two">
              <div className="panel-head">
                <h3>TSD Result</h3>
                <span className={`pill ${tsdResult?.ok ? '' : 'ghost'}`}>{tsdResult ? (tsdResult.ok ? 'ok' : 'info') : 'idle'}</span>
              </div>
              <ResultStatus result={tsdResult} emptyText="TSD flow has not run yet." />
            </article>
          </section>
        )}

        {section === 'aggregation' && (
          <section className="content-grid wide-grid">
            <article className="panel">
              <div className="panel-head">
                <h3>Aggregation Export</h3>
                <span className="pill">AggregationAPI.SearchAndExport</span>
              </div>
              <div className="form-grid compact-grid">
                <Field label="Mode">
                  <select value={aggregationQuery.mode} onChange={(event) => setAggregationQuery({ ...aggregationQuery, mode: event.target.value })}>
                    <option value="comment">comment</option>
                    <option value="count">count</option>
                  </select>
                </Field>
                <Field label="Target value">
                  <input value={aggregationQuery.targetValue} onChange={(event) => setAggregationQuery({ ...aggregationQuery, targetValue: event.target.value })} />
                </Field>
                <Field label="Status filter">
                  <input value={aggregationQuery.statusFilter} onChange={(event) => setAggregationQuery({ ...aggregationQuery, statusFilter: event.target.value })} />
                </Field>
                <Field label="Export filename">
                  <input value={aggregationQuery.filename} onChange={(event) => setAggregationQuery({ ...aggregationQuery, filename: event.target.value })} placeholder="optional.csv" />
                </Field>
              </div>
              <div className="button-row wrap">
                <button type="button" onClick={handleAggregationExport} disabled={isBusy || !aggregationQuery.targetValue.trim()}>
                  {busyAction === 'search-aggregation' ? 'Exporting…' : 'Search and export'}
                </button>
                <button type="button" className="secondary" onClick={() => setAggregationQuery(initialAggregationQuery)} disabled={isBusy}>
                  Reset form
                </button>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Aggregation Result</h3>
                <span className="pill ghost">{aggregationResult?.records.length ?? 0} rows</span>
              </div>
              {aggregationResult ? (
                <>
                  <div className="kv-list compact-list">
                    <div><span>Directory</span><strong>{aggregationResult.directory || 'n/a'}</strong></div>
                    <div><span>Filename</span><strong>{aggregationResult.filename || 'n/a'}</strong></div>
                  </div>
                  <div className="mini-table aggregation-table">
                    {aggregationResult.records.slice(0, 12).map((record) => (
                      <div className="mini-row" key={`${record.aggregateCode}-${record.documentId ?? ''}`}>
                        <div>
                          <strong>{record.aggregateCode}</strong>
                          <span>{record.comment || record.documentId || 'no comment'}</span>
                        </div>
                        <div>
                          <strong>{record.status || 'n/a'}</strong>
                          <span>{record.updatedDate || record.createdDate || 'n/a'}</span>
                        </div>
                      </div>
                    ))}
                    {!aggregationResult.records.length && <div className="empty-state">No aggregation records returned.</div>}
                  </div>
                </>
              ) : (
                <div className="empty-state">Aggregation export has not been executed yet.</div>
              )}
            </article>
          </section>
        )}

        {section === 'history' && (
          <section className="panel history-panel">
            <div className="panel-head">
              <h3>Shared Order History</h3>
              <span className="pill ghost">{history.length} rows</span>
            </div>
            <div className="toolbar-row">
              <label className="check-row">
                <input
                  type="checkbox"
                  checked={historyFilter.onlyWithoutTsd}
                  onChange={(event) => setHistoryFilter((current) => ({ ...current, onlyWithoutTsd: event.target.checked }))}
                />
                <span>Only without TSD</span>
              </label>
              <input
                value={historyFilter.status}
                onChange={(event) => setHistoryFilter((current) => ({ ...current, status: event.target.value }))}
                placeholder="Status filter"
              />
              <button type="button" className="secondary" onClick={() => reloadHistory(historyFilter.search, historyFilter.status, historyFilter.onlyWithoutTsd)} disabled={isBusy}>
                Reload
              </button>
            </div>
            <div className="history-table detailed-history">
              {history.map((item) => (
                <div className="history-row expanded" key={item.document_id}>
                  <div>
                    <strong>{item.order_name || item.document_id}</strong>
                    <span>{item.full_name || item.gtin || 'no metadata'}</span>
                  </div>
                  <div>
                    <strong>{item.status}</strong>
                    <span>{item.updated_at ?? 'n/a'}</span>
                  </div>
                  <div className="inline-actions">
                    <button type="button" className="ghost-button" onClick={() => pickOrder(item)} disabled={isBusy}>
                      Select
                    </button>
                    <button type="button" className="secondary" onClick={() => handleCheckOrderStatus(item.document_id)} disabled={isBusy}>
                      Status
                    </button>
                    <button type="button" className="secondary" onClick={() => handleDownloadOrder(item.document_id)} disabled={isBusy}>
                      Download
                    </button>
                    <button type="button" onClick={() => handleMarkTsdCreated(item)} disabled={isBusy}>
                      Mark TSD
                    </button>
                  </div>
                </div>
              ))}
              {!history.length && <div className="empty-state">No records matched the current filter.</div>}
            </div>
          </section>
        )}

        {section === 'setup' && (
          <section className="content-grid wide-grid">
            <article className="panel span-two">
              <div className="panel-head">
                <h3>Dependencies</h3>
                <span className="pill ghost">SystemAPI.CheckDependencies</span>
              </div>
              <div className="dependency-grid two-up">
                {dependencies.map((item) => (
                  <div key={item.name} className={item.available ? 'dependency-card ok' : 'dependency-card warn'}>
                    <strong>{item.name}</strong>
                    <span>{item.status || 'missing'}</span>
                    <p>{item.hint}</p>
                  </div>
                ))}
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Prepare Artifact Directory</h3>
                <span className="pill">DownloadsAPI.PrepareArtifactDirectory</span>
              </div>
              <Field label="Order name">
                <input
                  value={orderActions.artifactOrderName}
                  onChange={(event) => setOrderActions({ ...orderActions, artifactOrderName: event.target.value })}
                  placeholder="Folder name"
                />
              </Field>
              <div className="button-row wrap">
                <button type="button" onClick={handlePrepareDirectory} disabled={isBusy}>
                  {busyAction === 'prepare-directory' ? 'Preparing…' : 'Prepare'}
                </button>
              </div>
              <div className="surface-block">
                <span className="label">Directory</span>
                <strong>{preparedDirectory || 'not prepared yet'}</strong>
              </div>
            </article>

            <article className="panel">
              <div className="panel-head">
                <h3>Current Runtime</h3>
                <span className="pill ghost">AppState.Get</span>
              </div>
              <div className="kv-list compact-list">
                <div><span>Repo root</span><strong>{appState?.repoRoot ?? 'n/a'}</strong></div>
                <div><span>History path</span><strong>{appState?.historyPath ?? 'n/a'}</strong></div>
                <div><span>Session source</span><strong>{appState?.session.source ?? 'n/a'}</strong></div>
                <div><span>Cookies required</span><strong>{appState?.session.requiredCookies?.join(', ') || 'n/a'}</strong></div>
              </div>
            </article>
          </section>
        )}
      </main>

      <aside className="rail">
        <div className="rail-card">
          <span className="label">Live log</span>
          <div className="log-list">
            {logs.map((line, index) => (
              <div className="log-line" key={`${line}-${index}`}>
                {line}
              </div>
            ))}
          </div>
        </div>

        <div className="rail-card">
          <span className="label">Selection context</span>
          <strong>{selectedOrderMeta}</strong>
          <p>History row selection now feeds order status/download, introduction and TSD forms.</p>
        </div>
      </aside>
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
        <strong>{result.message}</strong>
      </div>
      {result.details && Object.keys(result.details).length > 0 && (
        <div className="kv-list compact-list">
          {Object.entries(result.details).map(([key, value]) => (
            <div key={key}>
              <span>{key}</span>
              <strong>{value}</strong>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function ArtifactResult({ artifact, preparedDirectory }: { artifact: DownloadArtifact | null; preparedDirectory: string }) {
  if (!artifact && !preparedDirectory) {
    return <div className="empty-state">No artifacts downloaded yet.</div>;
  }

  return (
    <div className="kv-list compact-list">
      {preparedDirectory && (
        <div>
          <span>Prepared directory</span>
          <strong>{preparedDirectory}</strong>
        </div>
      )}
      {artifact?.directory && (
        <div>
          <span>Artifact directory</span>
          <strong>{artifact.directory}</strong>
        </div>
      )}
      {artifact?.pdfPath && (
        <div>
          <span>PDF</span>
          <strong>{artifact.pdfPath}</strong>
        </div>
      )}
      {artifact?.csvPath && (
        <div>
          <span>CSV</span>
          <strong>{artifact.csvPath}</strong>
        </div>
      )}
      {artifact?.xlsPath && (
        <div>
          <span>XLS</span>
          <strong>{artifact.xlsPath}</strong>
        </div>
      )}
    </div>
  );
}

function parsePositions(text: string): PositionData[] {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (!lines.length) {
    throw new Error('At least one position is required. Use one line per item: Name|GTIN');
  }

  return lines.map((line, index) => {
    const [namePart, gtinPart, ...rest] = line.split('|');
    const name = namePart?.trim() ?? '';
    const gtin = [gtinPart, ...rest].join('|').trim();
    if (!name || !gtin) {
      throw new Error(`Invalid position line ${index + 1}. Expected format: Name|GTIN`);
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

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return 'Unknown error';
}

export default App;

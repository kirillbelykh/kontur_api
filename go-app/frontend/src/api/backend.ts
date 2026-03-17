import { dto } from '../../wailsjs/go/models';
import { SearchAndExport as WailsSearchAndExport } from '../../wailsjs/go/uiapi/AggregationAPI';
import { Get as WailsGetAppState } from '../../wailsjs/go/uiapi/AppStateAPI';
import { RefreshSession as WailsRefreshSession } from '../../wailsjs/go/uiapi/AuthAPI';
import { PrepareArtifactDirectory as WailsPrepareArtifactDirectory } from '../../wailsjs/go/uiapi/DownloadsAPI';
import { List as WailsListHistory, MarkTsdCreated as WailsMarkTsdCreated } from '../../wailsjs/go/uiapi/HistoryAPI';
import { Run as WailsRunIntroduction } from '../../wailsjs/go/uiapi/IntroductionAPI';
import {
  CheckStatus as WailsCheckOrderStatus,
  Create as WailsCreateOrders,
  Download as WailsDownloadOrder,
} from '../../wailsjs/go/uiapi/OrdersAPI';
import { CheckDependencies as WailsCheckDependencies } from '../../wailsjs/go/uiapi/SystemAPI';
import { CreateTask as WailsCreateTsdTask } from '../../wailsjs/go/uiapi/TSDAPI';

export type SessionState = {
  available: boolean;
  source: string;
  updatedAt?: string;
  expiresAt?: string;
  message?: string;
  requiredCookies?: string[];
};

export type DependencyStatus = {
  name: string;
  available: boolean;
  status: string;
  hint?: string;
  details?: string;
};

export type PositionData = {
  name: string;
  gtin: string;
};

export type OrderRecord = {
  document_id: string;
  order_name: string;
  status: string;
  filename?: string;
  simpl?: string;
  full_name?: string;
  gtin?: string;
  created_at?: string;
  updated_at?: string;
  tsd_created?: boolean;
  tsd_intro_number?: string;
  positions?: PositionData[];
  extra?: Record<string, string>;
};

export type AppState = {
  repoRoot: string;
  historyPath: string;
  ordersTotal: number;
  ordersWithoutTsd: number;
  session: SessionState;
  dependencies: DependencyStatus[];
  lastUpdated: string;
};

export type HistoryFilter = {
  search: string;
  status: string;
  onlyWithoutTsd: boolean;
};

export type OrderDraft = {
  orderName: string;
  simplifiedName: string;
  size: string;
  unitsPerPack: string;
  codesCount: number;
  gtin: string;
  fullName: string;
  tnvedCode: string;
  cisType: string;
};

export type ProductionPatch = {
  documentNumber: string;
  productionDate: string;
  expirationDate: string;
  batchNumber: string;
  TnvedCode: string;
};

export type IntroductionRequest = {
  codesOrderId: string;
  organizationId: string;
  thumbprint: string;
  productionPatch: ProductionPatch;
};

export type TSDRequest = {
  codesOrderId: string;
  positions: PositionData[];
  productionPatch: ProductionPatch;
};

export type AggregationQuery = {
  mode: string;
  targetValue: string;
  statusFilter: string;
  filename: string;
};

export type AggregationRecord = {
  aggregateCode: string;
  documentId?: string;
  createdDate?: string;
  status?: string;
  updatedDate?: string;
  includesUnitsCount?: number;
  comment?: string;
  productGroup?: string;
  aggregationType?: string;
  codesChecked?: boolean;
  codesCheckErrorsCount?: number;
  allowDelete?: boolean;
};

export type AggregationExportResult = {
  records: AggregationRecord[];
  directory: string;
  filename: string;
};

export type DownloadArtifact = {
  pdfPath?: string;
  csvPath?: string;
  xlsPath?: string;
  directory?: string;
};

export type OperationStatus = {
  ok: boolean;
  message: string;
  details?: Record<string, string>;
};

type WindowGo = {
  uiapi?: Record<string, Record<string, (...args: unknown[]) => Promise<unknown>>>;
};

declare global {
  interface Window {
    go?: WindowGo;
  }
}

const mockDependencies: DependencyStatus[] = [
  { name: 'go', available: true, status: 'detected', hint: 'Go backend is scaffolded.' },
  { name: 'git', available: true, status: 'detected', hint: 'History sync path is shared with Python.' },
  { name: 'windows-target', available: false, status: 'preview', hint: 'Windows-only integrations are unavailable in browser preview.' },
];

const previewOrderRecord = (draft: OrderDraft, index = 1): OrderRecord => ({
  document_id: `PREVIEW-${Date.now()}-${index}`,
  order_name: draft.orderName,
  status: 'Черновик Go',
  full_name: draft.fullName,
  gtin: draft.gtin,
  updated_at: new Date().toISOString(),
});

function hasBinding(api: string, method: string): boolean {
  return Boolean(window.go?.uiapi?.[api]?.[method]);
}

async function maybeCall<T>(api: string, method: string, call: () => Promise<T>, fallback: T): Promise<T> {
  if (!hasBinding(api, method)) {
    return fallback;
  }
  return call();
}

function previewOperation(message: string, details?: Record<string, string>): OperationStatus {
  return {
    ok: false,
    message,
    details,
  };
}

export async function getAppState(): Promise<AppState> {
  return maybeCall<AppState>('AppStateAPI', 'Get', () => WailsGetAppState() as unknown as Promise<AppState>, {
    repoRoot: 'go-app preview',
    historyPath: 'shared history not loaded in browser preview',
    ordersTotal: 0,
    ordersWithoutTsd: 0,
    session: {
      available: false,
      source: 'preview',
      message: 'Backend bindings are not connected in browser-only preview.',
    },
    dependencies: mockDependencies,
    lastUpdated: new Date().toISOString(),
  });
}

export async function listHistory(filter: HistoryFilter): Promise<OrderRecord[]> {
  return maybeCall<OrderRecord[]>(
    'HistoryAPI',
    'List',
    () => WailsListHistory(filter as unknown as dto.HistoryFilter) as unknown as Promise<OrderRecord[]>,
    [],
  );
}

export async function refreshSession(): Promise<SessionState> {
  return maybeCall<SessionState>('AuthAPI', 'RefreshSession', () => WailsRefreshSession() as unknown as Promise<SessionState>, {
    available: false,
    source: 'preview',
    message: 'Session refresh is available only from the Wails desktop runtime.',
  });
}

export async function createOrders(drafts: OrderDraft[]): Promise<OrderRecord[]> {
  return maybeCall<OrderRecord[]>(
    'OrdersAPI',
    'Create',
    () => WailsCreateOrders(drafts as unknown as dto.OrderDraft[]) as unknown as Promise<OrderRecord[]>,
    drafts.map((draft, index) => previewOrderRecord(draft, index + 1)),
  );
}

export async function checkOrderStatus(documentID: string): Promise<OperationStatus> {
  return maybeCall<OperationStatus>(
    'OrdersAPI',
    'CheckStatus',
    () => WailsCheckOrderStatus(documentID) as unknown as Promise<OperationStatus>,
    previewOperation('Order status check is available only from the Wails desktop runtime.', { documentId: documentID }),
  );
}

export async function downloadOrder(documentID: string): Promise<DownloadArtifact> {
  return maybeCall<DownloadArtifact>(
    'OrdersAPI',
    'Download',
    () => WailsDownloadOrder(documentID) as unknown as Promise<DownloadArtifact>,
    { directory: `preview/${documentID}` },
  );
}

export async function runIntroduction(request: IntroductionRequest): Promise<OperationStatus> {
  return maybeCall<OperationStatus>(
    'IntroductionAPI',
    'Run',
    () => WailsRunIntroduction(request as unknown as dto.IntroductionRequest) as unknown as Promise<OperationStatus>,
    previewOperation('Introduction flow is available only from the Wails desktop runtime.', {
      codesOrderId: request.codesOrderId,
      documentNumber: request.productionPatch.documentNumber,
    }),
  );
}

export async function createTsdTask(request: TSDRequest): Promise<OperationStatus> {
  return maybeCall<OperationStatus>(
    'TSDAPI',
    'CreateTask',
    () => WailsCreateTsdTask(request as unknown as dto.TSDRequest) as unknown as Promise<OperationStatus>,
    previewOperation('TSD flow is available only from the Wails desktop runtime.', {
      codesOrderId: request.codesOrderId,
      positionsCount: String(request.positions.length),
    }),
  );
}

export async function searchAndExportAggregates(query: AggregationQuery): Promise<AggregationExportResult> {
  return maybeCall<AggregationExportResult>(
    'AggregationAPI',
    'SearchAndExport',
    () => WailsSearchAndExport(query as unknown as dto.AggregationQuery) as unknown as Promise<AggregationExportResult>,
    {
      records: [],
      directory: 'preview/aggregation',
      filename: query.filename || 'preview.csv',
    },
  );
}

export async function markTsdCreated(documentID: string, introNumber: string): Promise<OrderRecord> {
  return maybeCall<OrderRecord>(
    'HistoryAPI',
    'MarkTsdCreated',
    () => WailsMarkTsdCreated(documentID, introNumber) as unknown as Promise<OrderRecord>,
    {
    document_id: documentID,
    order_name: 'preview',
    status: 'Черновик Go',
    tsd_created: true,
    tsd_intro_number: introNumber,
    updated_at: new Date().toISOString(),
    },
  );
}

export async function checkDependencies(): Promise<DependencyStatus[]> {
  return maybeCall<DependencyStatus[]>(
    'SystemAPI',
    'CheckDependencies',
    () => WailsCheckDependencies() as unknown as Promise<DependencyStatus[]>,
    mockDependencies,
  );
}

export async function prepareArtifactDirectory(orderName: string): Promise<string> {
  return maybeCall<string>(
    'DownloadsAPI',
    'PrepareArtifactDirectory',
    () => WailsPrepareArtifactDirectory(orderName) as unknown as Promise<string>,
    `preview/${orderName || 'untitled'}`,
  );
}

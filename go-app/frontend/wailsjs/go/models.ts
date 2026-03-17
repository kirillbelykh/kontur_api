export namespace dto {
	
	export class AggregationRecord {
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
	
	    static createFrom(source: any = {}) {
	        return new AggregationRecord(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.aggregateCode = source["aggregateCode"];
	        this.documentId = source["documentId"];
	        this.createdDate = source["createdDate"];
	        this.status = source["status"];
	        this.updatedDate = source["updatedDate"];
	        this.includesUnitsCount = source["includesUnitsCount"];
	        this.comment = source["comment"];
	        this.productGroup = source["productGroup"];
	        this.aggregationType = source["aggregationType"];
	        this.codesChecked = source["codesChecked"];
	        this.codesCheckErrorsCount = source["codesCheckErrorsCount"];
	        this.allowDelete = source["allowDelete"];
	    }
	}
	export class AggregationExportResult {
	    records: AggregationRecord[];
	    directory: string;
	    filename: string;
	
	    static createFrom(source: any = {}) {
	        return new AggregationExportResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.records = this.convertValues(source["records"], AggregationRecord);
	        this.directory = source["directory"];
	        this.filename = source["filename"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class AggregationQuery {
	    mode: string;
	    targetValue: string;
	    statusFilter: string;
	    filename: string;
	
	    static createFrom(source: any = {}) {
	        return new AggregationQuery(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.mode = source["mode"];
	        this.targetValue = source["targetValue"];
	        this.statusFilter = source["statusFilter"];
	        this.filename = source["filename"];
	    }
	}
	
	export class DependencyStatus {
	    name: string;
	    available: boolean;
	    status: string;
	    hint?: string;
	    details?: string;
	
	    static createFrom(source: any = {}) {
	        return new DependencyStatus(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.available = source["available"];
	        this.status = source["status"];
	        this.hint = source["hint"];
	        this.details = source["details"];
	    }
	}
	export class SessionState {
	    available: boolean;
	    source: string;
	    updatedAt?: string;
	    expiresAt?: string;
	    message?: string;
	    requiredCookies?: string[];
	
	    static createFrom(source: any = {}) {
	        return new SessionState(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.available = source["available"];
	        this.source = source["source"];
	        this.updatedAt = source["updatedAt"];
	        this.expiresAt = source["expiresAt"];
	        this.message = source["message"];
	        this.requiredCookies = source["requiredCookies"];
	    }
	}
	export class AppState {
	    repoRoot: string;
	    historyPath: string;
	    ordersTotal: number;
	    ordersWithoutTsd: number;
	    session: SessionState;
	    dependencies: DependencyStatus[];
	    lastUpdated: string;
	
	    static createFrom(source: any = {}) {
	        return new AppState(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.repoRoot = source["repoRoot"];
	        this.historyPath = source["historyPath"];
	        this.ordersTotal = source["ordersTotal"];
	        this.ordersWithoutTsd = source["ordersWithoutTsd"];
	        this.session = this.convertValues(source["session"], SessionState);
	        this.dependencies = this.convertValues(source["dependencies"], DependencyStatus);
	        this.lastUpdated = source["lastUpdated"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	export class DownloadArtifact {
	    pdfPath?: string;
	    csvPath?: string;
	    xlsPath?: string;
	    directory?: string;
	
	    static createFrom(source: any = {}) {
	        return new DownloadArtifact(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.pdfPath = source["pdfPath"];
	        this.csvPath = source["csvPath"];
	        this.xlsPath = source["xlsPath"];
	        this.directory = source["directory"];
	    }
	}
	export class HistoryFilter {
	    search: string;
	    status: string;
	    onlyWithoutTsd: boolean;
	
	    static createFrom(source: any = {}) {
	        return new HistoryFilter(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.search = source["search"];
	        this.status = source["status"];
	        this.onlyWithoutTsd = source["onlyWithoutTsd"];
	    }
	}
	export class ProductionPatch {
	    documentNumber: string;
	    productionDate: string;
	    expirationDate: string;
	    batchNumber: string;
	    TnvedCode: string;
	
	    static createFrom(source: any = {}) {
	        return new ProductionPatch(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.documentNumber = source["documentNumber"];
	        this.productionDate = source["productionDate"];
	        this.expirationDate = source["expirationDate"];
	        this.batchNumber = source["batchNumber"];
	        this.TnvedCode = source["TnvedCode"];
	    }
	}
	export class IntroductionRequest {
	    codesOrderId: string;
	    organizationId: string;
	    thumbprint: string;
	    productionPatch: ProductionPatch;
	
	    static createFrom(source: any = {}) {
	        return new IntroductionRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.codesOrderId = source["codesOrderId"];
	        this.organizationId = source["organizationId"];
	        this.thumbprint = source["thumbprint"];
	        this.productionPatch = this.convertValues(source["productionPatch"], ProductionPatch);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	export class OperationStatus {
	    ok: boolean;
	    message: string;
	    details?: Record<string, string>;
	
	    static createFrom(source: any = {}) {
	        return new OperationStatus(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.ok = source["ok"];
	        this.message = source["message"];
	        this.details = source["details"];
	    }
	}
	export class OrderDraft {
	    orderName: string;
	    simplifiedName: string;
	    size: string;
	    unitsPerPack: string;
	    codesCount: number;
	    gtin: string;
	    fullName: string;
	    tnvedCode: string;
	    cisType: string;
	
	    static createFrom(source: any = {}) {
	        return new OrderDraft(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.orderName = source["orderName"];
	        this.simplifiedName = source["simplifiedName"];
	        this.size = source["size"];
	        this.unitsPerPack = source["unitsPerPack"];
	        this.codesCount = source["codesCount"];
	        this.gtin = source["gtin"];
	        this.fullName = source["fullName"];
	        this.tnvedCode = source["tnvedCode"];
	        this.cisType = source["cisType"];
	    }
	}
	export class PositionData {
	    name: string;
	    gtin: string;
	
	    static createFrom(source: any = {}) {
	        return new PositionData(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.gtin = source["gtin"];
	    }
	}
	export class OrderRecord {
	    document_id: string;
	    order_name: string;
	    status: string;
	    filename?: string;
	    simpl?: string;
	    full_name?: string;
	    gtin?: string;
	    created_at?: string;
	    created_by?: string;
	    updated_at?: string;
	    updated_by?: string;
	    tsd_created?: boolean;
	    tsd_created_at?: string;
	    tsd_intro_number?: string;
	    tsd_created_by?: string;
	    positions?: PositionData[];
	    extra?: Record<string, string>;
	
	    static createFrom(source: any = {}) {
	        return new OrderRecord(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.document_id = source["document_id"];
	        this.order_name = source["order_name"];
	        this.status = source["status"];
	        this.filename = source["filename"];
	        this.simpl = source["simpl"];
	        this.full_name = source["full_name"];
	        this.gtin = source["gtin"];
	        this.created_at = source["created_at"];
	        this.created_by = source["created_by"];
	        this.updated_at = source["updated_at"];
	        this.updated_by = source["updated_by"];
	        this.tsd_created = source["tsd_created"];
	        this.tsd_created_at = source["tsd_created_at"];
	        this.tsd_intro_number = source["tsd_intro_number"];
	        this.tsd_created_by = source["tsd_created_by"];
	        this.positions = this.convertValues(source["positions"], PositionData);
	        this.extra = source["extra"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}
	
	
	
	export class TSDRequest {
	    codesOrderId: string;
	    positions: PositionData[];
	    productionPatch: ProductionPatch;
	
	    static createFrom(source: any = {}) {
	        return new TSDRequest(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.codesOrderId = source["codesOrderId"];
	        this.positions = this.convertValues(source["positions"], PositionData);
	        this.productionPatch = this.convertValues(source["productionPatch"], ProductionPatch);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}


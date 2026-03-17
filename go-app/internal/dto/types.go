package dto

type OrderDraft struct {
	OrderName      string `json:"orderName"`
	SimplifiedName string `json:"simplifiedName"`
	Size           string `json:"size"`
	UnitsPerPack   string `json:"unitsPerPack"`
	CodesCount     int    `json:"codesCount"`
	GTIN           string `json:"gtin"`
	FullName       string `json:"fullName"`
	TNVEDCode      string `json:"tnvedCode"`
	CISType        string `json:"cisType"`
}

type OrderRecord struct {
	DocumentID     string            `json:"document_id"`
	OrderName      string            `json:"order_name"`
	Status         string            `json:"status"`
	Filename       string            `json:"filename,omitempty"`
	Simpl          string            `json:"simpl,omitempty"`
	FullName       string            `json:"full_name,omitempty"`
	GTIN           string            `json:"gtin,omitempty"`
	CreatedAt      string            `json:"created_at,omitempty"`
	CreatedBy      string            `json:"created_by,omitempty"`
	UpdatedAt      string            `json:"updated_at,omitempty"`
	UpdatedBy      string            `json:"updated_by,omitempty"`
	TSDCreated     bool              `json:"tsd_created,omitempty"`
	TSDCreatedAt   string            `json:"tsd_created_at,omitempty"`
	TSDIntroNumber string            `json:"tsd_intro_number,omitempty"`
	TSDCreatedBy   string            `json:"tsd_created_by,omitempty"`
	Positions      []PositionData    `json:"positions,omitempty"`
	Extra          map[string]string `json:"extra,omitempty"`
}

type SessionState struct {
	Available       bool     `json:"available"`
	Source          string   `json:"source"`
	UpdatedAt       string   `json:"updatedAt,omitempty"`
	ExpiresAt       string   `json:"expiresAt,omitempty"`
	Message         string   `json:"message,omitempty"`
	RequiredCookies []string `json:"requiredCookies,omitempty"`
}

type ProductionPatch struct {
	DocumentNumber string `json:"documentNumber"`
	ProductionDate string `json:"productionDate"`
	ExpirationDate string `json:"expirationDate"`
	BatchNumber    string `json:"batchNumber"`
	TnvedCode      string `json:"TnvedCode"`
}

type PositionData struct {
	Name string `json:"name"`
	GTIN string `json:"gtin"`
}

type IntroductionRequest struct {
	CodesOrderID    string          `json:"codesOrderId"`
	OrganizationID  string          `json:"organizationId"`
	Thumbprint      string          `json:"thumbprint"`
	ProductionPatch ProductionPatch `json:"productionPatch"`
}

type TSDRequest struct {
	CodesOrderID    string          `json:"codesOrderId"`
	Positions       []PositionData  `json:"positions"`
	ProductionPatch ProductionPatch `json:"productionPatch"`
}

type AggregationQuery struct {
	Mode         string `json:"mode"`
	TargetValue  string `json:"targetValue"`
	StatusFilter string `json:"statusFilter"`
	Filename     string `json:"filename"`
}

type AggregationRecord struct {
	AggregateCode         string `json:"aggregateCode"`
	DocumentID            string `json:"documentId,omitempty"`
	CreatedDate           string `json:"createdDate,omitempty"`
	Status                string `json:"status,omitempty"`
	UpdatedDate           string `json:"updatedDate,omitempty"`
	IncludesUnitsCount    int    `json:"includesUnitsCount,omitempty"`
	Comment               string `json:"comment,omitempty"`
	ProductGroup          string `json:"productGroup,omitempty"`
	AggregationType       string `json:"aggregationType,omitempty"`
	CodesChecked          bool   `json:"codesChecked,omitempty"`
	CodesCheckErrorsCount int    `json:"codesCheckErrorsCount,omitempty"`
	AllowDelete           bool   `json:"allowDelete,omitempty"`
}

type AggregationExportResult struct {
	Records   []AggregationRecord `json:"records"`
	Directory string              `json:"directory"`
	Filename  string              `json:"filename"`
}

type DownloadArtifact struct {
	PDFPath   string `json:"pdfPath,omitempty"`
	CSVPath   string `json:"csvPath,omitempty"`
	XLSPath   string `json:"xlsPath,omitempty"`
	Directory string `json:"directory,omitempty"`
}

type DependencyStatus struct {
	Name      string `json:"name"`
	Available bool   `json:"available"`
	Status    string `json:"status"`
	Hint      string `json:"hint,omitempty"`
	Details   string `json:"details,omitempty"`
}

type HistoryFilter struct {
	Search         string `json:"search"`
	Status         string `json:"status"`
	OnlyWithoutTSD bool   `json:"onlyWithoutTsd"`
}

type AppState struct {
	RepoRoot         string             `json:"repoRoot"`
	HistoryPath      string             `json:"historyPath"`
	OrdersTotal      int                `json:"ordersTotal"`
	OrdersWithoutTSD int                `json:"ordersWithoutTsd"`
	Session          SessionState       `json:"session"`
	Dependencies     []DependencyStatus `json:"dependencies"`
	LastUpdated      string             `json:"lastUpdated"`
}

type OperationStatus struct {
	OK      bool              `json:"ok"`
	Message string            `json:"message"`
	Details map[string]string `json:"details,omitempty"`
}

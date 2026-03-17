package orders

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/history"
)

type noAuth struct{}

func (noAuth) ConfigureRequest(req *http.Request) error {
	return nil
}

type fakeSigner struct{}

func (fakeSigner) FindCertificateThumbprint(ctx context.Context) (string, error) {
	return "thumb-1", nil
}

func (fakeSigner) SignBase64(ctx context.Context, thumbprint, content string, detached bool) (string, error) {
	return "signed:" + content, nil
}

func TestCreateRemoteFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/codes-order":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "DOC-1"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-order/DOC-1/availability-status":
			_, _ = w.Write([]byte(`"available"`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/organizations/org-1/employees/has-certificate":
			_ = json.NewEncoder(w).Encode(true)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-order/DOC-1/orders-for-sign":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"id": "SIGN-1", "base64Content": "YmFzZTY0"}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/codes-order/DOC-1/send":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-order/DOC-1":
			_ = json.NewEncoder(w).Encode(map[string]string{"documentId": "DOC-1", "status": "released"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	historyService, err := history.NewService(config.Config{HistoryPath: t.TempDir() + "/history.json"})
	if err != nil {
		t.Fatalf("new history service: %v", err)
	}

	service := NewService(config.Config{
		BaseURL:           server.URL,
		OrganizationID:    "org-1",
		WarehouseID:       "warehouse-1",
		ProductGroup:      "wheelChairs",
		ReleaseMethodType: "production",
		FillingMethod:     "manual",
		CISType:           "unit",
	}, historyService, noAuth{}, fakeSigner{})

	created, err := service.Create([]dto.OrderDraft{{
		OrderName:      "ORDER-001",
		SimplifiedName: "chair",
		FullName:       "Wheel Chair",
		GTIN:           "1234567890123",
		TNVEDCode:      "4015120001",
		CodesCount:     25,
	}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(created) != 1 || created[0].DocumentID != "DOC-1" || created[0].Status != "released" {
		t.Fatalf("unexpected created records: %+v", created)
	}
}

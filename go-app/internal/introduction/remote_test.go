package introduction

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
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

func TestRunRemoteIntroduction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/codes-introduction/create-from-codes-order/CODE-1":
			_, _ = w.Write([]byte(`"INTRO-1"`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-introduction/INTRO-1":
			_ = json.NewEncoder(w).Encode(map[string]string{"id": "INTRO-1", "status": "created"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-checking/INTRO-1":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "noErrors"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-introduction/INTRO-1/production":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v1/codes-introduction/INTRO-1/production":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "patched"})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/codes-introduction/INTRO-1/positions/autocomplete":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/organizations/org-1/employees/has-certificate":
			_ = json.NewEncoder(w).Encode(true)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/codes-introduction/INTRO-1/generate-multiple":
			_ = json.NewEncoder(w).Encode([]map[string]string{{"documentId": "GEN-1", "base64Content": "YmFzZTY0"}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/codes-introduction/INTRO-1/send-multiple":
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "sent"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	service := NewService(config.Config{
		BaseURL:        server.URL,
		OrganizationID: "org-1",
		WarehouseID:    "warehouse-1",
		ProductGroup:   "wheelChairs",
	}, noAuth{}, fakeSigner{})

	status, err := service.Run(dto.IntroductionRequest{
		CodesOrderID:   "CODE-1",
		OrganizationID: "org-1",
		ProductionPatch: dto.ProductionPatch{
			DocumentNumber: "DOC-1",
			ProductionDate: "2025-10-05",
			ExpirationDate: "2025-11-05",
			BatchNumber:    "BATCH-1",
		},
	})
	if err != nil {
		t.Fatalf("run introduction: %v", err)
	}
	if !status.OK || status.Details["introductionId"] != "INTRO-1" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

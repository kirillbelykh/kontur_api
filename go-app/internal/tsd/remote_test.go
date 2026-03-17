package tsd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/introduction"
)

type noAuth struct{}

func (noAuth) ConfigureRequest(req *http.Request) error {
	return nil
}

func TestCreateTaskRemoteFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/codes-introduction":
			_, _ = w.Write([]byte(`"INTRO-TSD-1"`))
		case "/api/v1/codes-introduction/INTRO-TSD-1/production":
			_, _ = w.Write([]byte(`{"status":"patched"}`))
		case "/api/v1/codes-introduction/INTRO-TSD-1/positions":
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case "/api/v1/codes-introduction/INTRO-TSD-1/send-to-tsd":
			_, _ = w.Write([]byte(`{"status":"sent"}`))
		case "/api/v1/codes-introduction/INTRO-TSD-1":
			_, _ = w.Write([]byte(`{"id":"INTRO-TSD-1","status":"created"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	introService := introduction.NewService(config.Config{
		BaseURL:      server.URL,
		WarehouseID:  "warehouse-1",
		ProductGroup: "wheelChairs",
	}, noAuth{}, nil)

	service := NewService(config.Config{
		BaseURL:      server.URL,
		WarehouseID:  "warehouse-1",
		ProductGroup: "wheelChairs",
	}, noAuth{}, introService)

	status, err := service.CreateTask(dto.TSDRequest{
		CodesOrderID: "CODE-1",
		Positions: []dto.PositionData{
			{Name: "Product1", GTIN: "1234567890123"},
		},
		ProductionPatch: dto.ProductionPatch{
			DocumentNumber: "DOC-1",
			ProductionDate: "2025-10-05",
			ExpirationDate: "2025-11-05",
			BatchNumber:    "BATCH-1",
			TnvedCode:      "4015120001",
		},
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if !status.OK || status.Details["introductionId"] != "INTRO-TSD-1" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

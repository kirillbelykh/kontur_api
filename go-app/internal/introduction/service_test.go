package introduction

import (
	"testing"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

func TestDefaultProductionWindow(t *testing.T) {
	production, expiration := DefaultProductionWindow(time.Date(2026, 3, 13, 10, 0, 0, 0, time.UTC))
	if production != "01-01-2026" || expiration != "01-01-2031" {
		t.Fatalf("unexpected defaults: %s %s", production, expiration)
	}
}

func TestBuildProductionPayload(t *testing.T) {
	service := NewService(config.Config{WarehouseID: "warehouse", ProductGroup: "wheelChairs"}, nil, nil)
	payload := service.BuildProductionPayload(dto.ProductionPatch{
		DocumentNumber: "DOC-1",
		ProductionDate: "2025-10-05",
		ExpirationDate: "2025-11-05",
		BatchNumber:    "BATCH-1",
	}, "tsd")

	if payload.DocumentNumber != "DOC-1" || payload.FillingMethod != "tsd" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
	if payload.ProductionDate != "2025-10-05T00:00:00.000+03:00" {
		t.Fatalf("unexpected production date: %s", payload.ProductionDate)
	}
}

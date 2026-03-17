package history

import (
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

func TestMergePrefersNewerRecord(t *testing.T) {
	current := dto.OrderRecord{
		DocumentID: "DOC-1",
		Status:     "Ожидает",
		UpdatedAt:  "2026-03-01T10:00:00",
		UpdatedBy:  "pc-1",
	}
	incoming := dto.OrderRecord{
		DocumentID: "DOC-1",
		Status:     "Скачан",
		UpdatedAt:  "2026-03-01T11:00:00",
		UpdatedBy:  "pc-2",
	}

	merged := mergeOrders(current, incoming)
	if merged.Status != "Скачан" || merged.UpdatedBy != "pc-2" {
		t.Fatalf("unexpected merged record: %+v", merged)
	}
}

func TestMergePayloadsKeepsBothSources(t *testing.T) {
	merged := MergePayloads(
		[]dto.OrderRecord{{DocumentID: "REMOTE-1", Status: "Ожидает", UpdatedAt: "2026-03-01T09:00:00"}},
		[]dto.OrderRecord{{DocumentID: "LOCAL-1", Status: "Скачан", UpdatedAt: "2026-03-01T10:00:00"}},
	)

	if len(merged) != 2 {
		t.Fatalf("expected 2 merged records, got %d", len(merged))
	}
}

func TestMarkTsdCreated(t *testing.T) {
	tmp := t.TempDir()
	service, err := NewService(config.Config{HistoryPath: tmp + "/history.json"})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	record, err := service.Upsert(dto.OrderRecord{
		DocumentID: "DOC-2",
		OrderName:  "for tsd",
		Status:     "Скачан",
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	updated, err := service.MarkTsdCreated(record.DocumentID, "INTRO-77")
	if err != nil {
		t.Fatalf("mark tsd created: %v", err)
	}
	if !updated.TSDCreated || updated.TSDIntroNumber != "INTRO-77" {
		t.Fatalf("unexpected updated record: %+v", updated)
	}
}

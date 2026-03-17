package queue

import (
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

func TestIsOrderReadyForIntro(t *testing.T) {
	if !IsOrderReadyForIntro(dto.OrderRecord{DocumentID: "doc-1", Status: "Скачан"}) {
		t.Fatal("downloaded order should be ready for intro")
	}
	if IsOrderReadyForIntro(dto.OrderRecord{DocumentID: "doc-1", Status: "Ожидает"}) {
		t.Fatal("pending order should not be ready for intro")
	}
	if !IsOrderReadyForIntro(dto.OrderRecord{DocumentID: "doc-1", Status: "Из истории", Filename: "codes.csv"}) {
		t.Fatal("order with downloaded file should be ready for intro")
	}
}

func TestIsOrderReadyForTSD(t *testing.T) {
	if !IsOrderReadyForTSD(dto.OrderRecord{DocumentID: "doc-1", Status: "Ожидает"}) {
		t.Fatal("pending order should be ready for tsd")
	}
	if IsOrderReadyForTSD(dto.OrderRecord{DocumentID: "doc-1", Status: "Ошибка генерации"}) {
		t.Fatal("failed order should not be ready for tsd")
	}
}

func TestRemoveOrderByDocumentID(t *testing.T) {
	items := []dto.OrderRecord{
		{DocumentID: "doc-1", Status: "Скачан"},
		{DocumentID: "doc-2", Status: "Скачан"},
	}

	remaining, removed := RemoveOrderByDocumentID(items, "doc-1")
	if !removed {
		t.Fatal("expected order to be removed")
	}
	if len(remaining) != 1 || remaining[0].DocumentID != "doc-2" {
		t.Fatalf("unexpected remaining orders: %+v", remaining)
	}
}

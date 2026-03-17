package orders

import (
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

func TestBuildCreatePayload(t *testing.T) {
	service := NewService(config.Config{
		ProductGroup:      "wheelChairs",
		ReleaseMethodType: "production",
		FillingMethod:     "manual",
		CISType:           "unit",
	}, nil, nil, nil)

	payload := service.BuildCreatePayload(dto.OrderDraft{
		OrderName:      "ORDER-001",
		SimplifiedName: "chair",
		FullName:       "Wheel Chair",
		GTIN:           "1234567890123",
		TNVEDCode:      "4015120001",
		CodesCount:     25,
	})

	if payload.DocumentNumber != "ORDER-001" {
		t.Fatalf("unexpected document number: %+v", payload)
	}
	if payload.Positions[0].Quantity != 25 {
		t.Fatalf("unexpected quantity: %+v", payload.Positions[0])
	}
}

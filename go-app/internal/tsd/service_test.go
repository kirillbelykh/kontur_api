package tsd

import (
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

func TestBuildPositionsPayload(t *testing.T) {
	service := NewService(config.Config{ProductGroup: "wheelChairs"}, nil, nil)
	payload := service.BuildPositionsPayload(dto.TSDRequest{
		Positions: []dto.PositionData{
			{Name: "Product1", GTIN: "123"},
			{Name: "Product2", GTIN: "456"},
		},
		ProductionPatch: dto.ProductionPatch{TnvedCode: "4015120001"},
	})

	if len(payload) != 2 {
		t.Fatalf("unexpected positions len: %d", len(payload))
	}
	if payload[0].TNVEDCode != "4015120001" {
		t.Fatalf("unexpected tnved code: %+v", payload[0])
	}
}

package aggregation

import (
	"context"
	"strconv"
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

func TestSearchByCommentAcrossPages(t *testing.T) {
	service := NewService(config.Config{}, nil)
	service.SetFetcher(func(ctx context.Context, offset, limit int, statusFilter string) ([]dto.AggregationRecord, error) {
		switch offset {
		case 0:
			items := make([]dto.AggregationRecord, 0, 100)
			for i := 0; i < 100; i++ {
				items = append(items, dto.AggregationRecord{
					AggregateCode: "0465011804251202000000" + leftPad(i+1),
					Comment:       "other",
				})
			}
			return items, nil
		case 100:
			return []dto.AggregationRecord{
				{AggregateCode: "04650118042512020000000101", Comment: "target name"},
				{AggregateCode: "04650118042512020000000102", Comment: "target name"},
			}, nil
		default:
			return []dto.AggregationRecord{}, nil
		}
	})

	records, err := service.Search(context.Background(), dto.AggregationQuery{
		Mode:        "comment",
		TargetValue: "target name",
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestSearchByCountStopsAtLimit(t *testing.T) {
	service := NewService(config.Config{}, nil)
	service.SetFetcher(func(ctx context.Context, offset, limit int, statusFilter string) ([]dto.AggregationRecord, error) {
		items := make([]dto.AggregationRecord, 0, limit)
		for i := 0; i < limit; i++ {
			items = append(items, dto.AggregationRecord{
				AggregateCode: "0465011804251202000000" + leftPad(offset+i+1),
			})
		}
		return items, nil
	})

	records, err := service.Search(context.Background(), dto.AggregationQuery{
		Mode:        "count",
		TargetValue: "120",
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(records) != 120 {
		t.Fatalf("expected 120 records, got %d", len(records))
	}
}

func leftPad(value int) string {
	if value < 10 {
		return "000000000" + strconv.Itoa(value)
	}
	if value < 100 {
		return "00000000" + strconv.Itoa(value)
	}
	return "0000000" + strconv.Itoa(value)
}

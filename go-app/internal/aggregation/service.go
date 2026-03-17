package aggregation

import (
	"context"
	"encoding/csv"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/downloads"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/kontur"
)

type PageFetcher func(ctx context.Context, offset, limit int, statusFilter string) ([]dto.AggregationRecord, error)

type Service struct {
	cfg       config.Config
	downloads *downloads.Service
	fetcher   PageFetcher
	client    *kontur.Client
}

type aggregatesResponse struct {
	Items []dto.AggregationRecord `json:"items"`
}

func NewService(cfg config.Config, auth kontur.Authenticator) *Service {
	return &Service{
		cfg:       cfg,
		downloads: downloads.NewService(cfg, auth),
		client:    kontur.NewClient(cfg.BaseURL, auth),
	}
}

func (s *Service) SetFetcher(fetcher PageFetcher) {
	s.fetcher = fetcher
}

func (s *Service) Search(ctx context.Context, query dto.AggregationQuery) ([]dto.AggregationRecord, error) {
	fetch := s.fetcher
	if fetch == nil {
		if s.client == nil || s.cfg.BaseURL == "" || s.cfg.WarehouseID == "" {
			return nil, errors.New("aggregation fetcher is not configured yet")
		}
		fetch = s.fetchPage
	}

	pageLimit := 100
	offset := 0
	normalizedTarget := strings.ToLower(strings.TrimSpace(query.TargetValue))
	seen := make(map[string]struct{})
	records := make([]dto.AggregationRecord, 0)
	var wantedCount int
	if query.Mode == "count" && normalizedTarget != "" {
		wantedCount, _ = strconv.Atoi(normalizedTarget)
	}

	for {
		items, err := fetch(ctx, offset, pageLimit, firstNonEmpty(query.StatusFilter, "tsdProcessStart"))
		if err != nil {
			return nil, err
		}
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			if query.Mode == "comment" && normalizedTarget != "" {
				if !strings.Contains(strings.ToLower(strings.TrimSpace(item.Comment)), normalizedTarget) {
					continue
				}
			}

			if _, exists := seen[item.AggregateCode]; exists || item.AggregateCode == "" {
				continue
			}
			seen[item.AggregateCode] = struct{}{}
			records = append(records, item)
			if query.Mode == "count" && wantedCount > 0 && len(records) >= wantedCount {
				break
			}
		}

		if query.Mode == "count" && wantedCount > 0 && len(records) >= wantedCount {
			break
		}
		if len(items) < pageLimit {
			break
		}
		offset += pageLimit
	}

	if query.Mode == "count" && wantedCount > 0 && len(records) > wantedCount {
		records = records[:wantedCount]
	}

	sort.Slice(records, func(i, j int) bool {
		return suffixNumber(records[i].AggregateCode) < suffixNumber(records[j].AggregateCode)
	})
	return records, nil
}

func (s *Service) SearchAndExport(ctx context.Context, query dto.AggregationQuery) (dto.AggregationExportResult, error) {
	records, err := s.Search(ctx, query)
	if err != nil {
		return dto.AggregationExportResult{}, err
	}

	filename := query.Filename
	if filename == "" {
		switch query.Mode {
		case "count":
			filename = "Коды_агрегации_" + query.TargetValue + "_шт.csv"
		default:
			filename = downloads.SafeFilesystemName(query.TargetValue, 30) + "_" + strconv.Itoa(len(records)) + ".csv"
		}
	}
	dir, path, err := s.downloads.PrepareAggregationDirectory(filename)
	if err != nil {
		return dto.AggregationExportResult{}, err
	}

	file, err := os.Create(path)
	if err != nil {
		return dto.AggregationExportResult{}, err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, record := range records {
		if err := writer.Write([]string{record.AggregateCode}); err != nil {
			return dto.AggregationExportResult{}, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return dto.AggregationExportResult{}, err
	}

	return dto.AggregationExportResult{
		Records:   records,
		Directory: dir,
		Filename:  filename,
	}, nil
}

func (s *Service) fetchPage(ctx context.Context, offset, limit int, statusFilter string) ([]dto.AggregationRecord, error) {
	path := "/api/v1/aggregates?warehouseId=" + s.cfg.WarehouseID +
		"&limit=" + strconv.Itoa(limit) +
		"&offset=" + strconv.Itoa(offset) +
		"&statuses=" + statusFilter +
		"&sortField=createDate&sortOrder=descending"

	var response aggregatesResponse
	_, err := s.client.DoJSON(ctx, "GET", path, nil, &response)
	if err != nil {
		return nil, err
	}
	return response.Items, nil
}

func suffixNumber(code string) int {
	if len(code) >= 10 {
		if value, err := strconv.Atoi(code[len(code)-10:]); err == nil {
			return value
		}
	}
	if value, err := strconv.Atoi(code); err == nil {
		return value
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

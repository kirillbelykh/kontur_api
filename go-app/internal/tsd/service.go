package tsd

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/introduction"
	"github.com/kirillbelykh/kontur_api/go-app/internal/kontur"
)

type Service struct {
	cfg          config.Config
	introService *introduction.Service
	client       *kontur.Client
}

type PositionPayload struct {
	Name                      string `json:"name"`
	GTIN                      string `json:"gtin"`
	TNVEDCode                 string `json:"tnvedCode"`
	CertificateDocumentNumber string `json:"certificateDocumentNumber"`
	CertificateDocumentDate   string `json:"certificateDocumentDate"`
	CostInKopecksWithVAT      int    `json:"costInKopecksWithVat"`
	ExciseInKopecks           int    `json:"exciseInKopecks"`
	ProductGroup              string `json:"productGroup"`
}

type createResponse struct {
	ID string `json:"id"`
}

func NewService(cfg config.Config, auth kontur.Authenticator, introService *introduction.Service) *Service {
	if introService == nil {
		introService = introduction.NewService(cfg, auth, nil)
	}
	return &Service{
		cfg:          cfg,
		introService: introService,
		client:       kontur.NewClient(cfg.BaseURL, auth),
	}
}

func (s *Service) BuildPositionsPayload(request dto.TSDRequest) []PositionPayload {
	productGroup := s.cfg.ProductGroup
	if productGroup == "" {
		productGroup = "wheelChairs"
	}

	positions := make([]PositionPayload, 0, len(request.Positions))
	for _, pos := range request.Positions {
		positions = append(positions, PositionPayload{
			Name:                      pos.Name,
			GTIN:                      pos.GTIN,
			TNVEDCode:                 request.ProductionPatch.TnvedCode,
			CertificateDocumentNumber: "",
			CertificateDocumentDate:   "",
			CostInKopecksWithVAT:      0,
			ExciseInKopecks:           0,
			ProductGroup:              productGroup,
		})
	}
	return positions
}

func (s *Service) CreateTask(request dto.TSDRequest) (dto.OperationStatus, error) {
	if request.CodesOrderID == "" {
		return dto.OperationStatus{OK: false, Message: "codesOrderId is required"}, nil
	}
	if s.client == nil || s.cfg.BaseURL == "" || s.cfg.WarehouseID == "" {
		production := s.introService.BuildProductionPayload(request.ProductionPatch, "tsd")
		positions := s.BuildPositionsPayload(request)
		return dto.OperationStatus{
			OK:      true,
			Message: "TSD task prepared without remote transport because BASE_URL/session are unavailable.",
			Details: map[string]string{
				"codesOrderId":   request.CodesOrderID,
				"positionsCount": strconv.Itoa(len(positions)),
				"documentNumber": production.DocumentNumber,
			},
		}, nil
	}

	ctx := context.Background()
	reqPayload := map[string]string{
		"introductionType": "introduction",
		"productGroup":     firstNonEmpty(s.cfg.ProductGroup, "wheelChairs"),
	}
	createRaw, _, err := s.client.DoText(ctx, "POST", "/api/v1/codes-introduction?warehouseId="+s.cfg.WarehouseID, reqPayload)
	if err != nil {
		return dto.OperationStatus{}, err
	}
	documentID := strings.Trim(strings.TrimSpace(createRaw), `"`)
	if documentID == "" {
		return dto.OperationStatus{}, errors.New("codes-introduction create returned empty id")
	}

	production := s.introService.BuildProductionPayload(request.ProductionPatch, "tsd")
	_, err = s.client.DoJSON(ctx, "PATCH", "/api/v1/codes-introduction/"+documentID+"/production", production, &map[string]any{})
	if err != nil {
		return dto.OperationStatus{}, err
	}

	positions := s.BuildPositionsPayload(request)
	_, err = s.client.DoJSON(ctx, "POST", "/api/v1/codes-introduction/"+documentID+"/positions", map[string]any{
		"rows": positions,
	}, &map[string]any{})
	if err != nil {
		return dto.OperationStatus{}, err
	}

	_, _, err = s.client.DoText(ctx, "POST", "/api/v1/codes-introduction/"+documentID+"/send-to-tsd", nil)
	if err != nil {
		return dto.OperationStatus{}, err
	}

	var finalDoc map[string]any
	_, err = s.client.DoJSON(ctx, "GET", "/api/v1/codes-introduction/"+documentID, nil, &finalDoc)
	if err != nil {
		return dto.OperationStatus{}, err
	}

	return dto.OperationStatus{
		OK:      true,
		Message: "TSD task created",
		Details: map[string]string{
			"introductionId": documentID,
			"positionsCount": strconv.Itoa(len(positions)),
		},
	}, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

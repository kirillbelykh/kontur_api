package orders

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/downloads"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/history"
	"github.com/kirillbelykh/kontur_api/go-app/internal/kontur"
)

type signer interface {
	FindCertificateThumbprint(context.Context) (string, error)
	SignBase64(context.Context, string, string, bool) (string, error)
}

type Service struct {
	cfg       config.Config
	history   *history.Service
	downloads *downloads.Service
	client    *kontur.Client
	signer    signer
}

type PositionPayload struct {
	GTIN                string        `json:"gtin"`
	Name                string        `json:"name"`
	TNVEDCode           string        `json:"tnvedCode"`
	Quantity            int           `json:"quantity"`
	CertificateDocument interface{}   `json:"certificateDocument"`
	SetsGTINUnits       []interface{} `json:"setsGtinUnits"`
}

type CreatePayload struct {
	DocumentNumber    string            `json:"documentNumber"`
	Comment           string            `json:"comment"`
	ProductGroup      string            `json:"productGroup"`
	ReleaseMethodType string            `json:"releaseMethodType"`
	FillingMethod     string            `json:"fillingMethod"`
	CISType           string            `json:"cisType"`
	Positions         []PositionPayload `json:"positions"`
}

type createResponse struct {
	ID         string `json:"id"`
	DocumentID string `json:"documentId"`
	Status     string `json:"status"`
}

type signOrder struct {
	ID            string `json:"id"`
	Base64Content string `json:"base64Content"`
}

type finalOrderDocument struct {
	DocumentID string `json:"documentId"`
	ID         string `json:"id"`
	Status     string `json:"status"`
}

func NewService(cfg config.Config, historyService *history.Service, auth kontur.Authenticator, signing signer) *Service {
	return &Service{
		cfg:       cfg,
		history:   historyService,
		downloads: downloads.NewService(cfg, auth),
		client:    kontur.NewClient(cfg.BaseURL, auth),
		signer:    signing,
	}
}

func (s *Service) BuildCreatePayload(draft dto.OrderDraft) CreatePayload {
	return CreatePayload{
		DocumentNumber:    draft.OrderName,
		Comment:           "",
		ProductGroup:      s.cfg.ProductGroup,
		ReleaseMethodType: s.cfg.ReleaseMethodType,
		FillingMethod:     s.cfg.FillingMethod,
		CISType:           s.cfg.CISType,
		Positions: []PositionPayload{
			{
				GTIN:                draft.GTIN,
				Name:                firstNonEmpty(draft.FullName, draft.SimplifiedName),
				TNVEDCode:           draft.TNVEDCode,
				Quantity:            draft.CodesCount,
				CertificateDocument: nil,
				SetsGTINUnits:       []interface{}{},
			},
		},
	}
}

func (s *Service) Create(drafts []dto.OrderDraft) ([]dto.OrderRecord, error) {
	created := make([]dto.OrderRecord, 0, len(drafts))
	for idx, draft := range drafts {
		if s.canUseRemoteFlow() {
			record, err := s.createRemote(context.Background(), draft)
			if err != nil {
				return nil, err
			}
			created = append(created, record)
			continue
		}
		record, err := s.createDraftFallback(draft, idx)
		if err != nil {
			return nil, err
		}
		created = append(created, record)
	}
	return created, nil
}

func (s *Service) CheckStatus(documentID string) (dto.OperationStatus, error) {
	if s.canUseRemoteFlow() {
		var finalDoc finalOrderDocument
		_, err := s.client.DoJSON(context.Background(), "GET", "/api/v1/codes-order/"+url.PathEscape(documentID), nil, &finalDoc)
		if err != nil {
			return dto.OperationStatus{}, err
		}
		if s.history != nil {
			current, historyErr := s.history.GetByDocumentID(documentID)
			if historyErr == nil && current != nil {
				current.Status = firstNonEmpty(finalDoc.Status, current.Status)
				_, _ = s.history.Upsert(*current)
			}
		}
		return dto.OperationStatus{
			OK:      true,
			Message: finalDoc.Status,
			Details: map[string]string{"documentId": documentID},
		}, nil
	}

	record, err := s.history.GetByDocumentID(documentID)
	if err != nil {
		return dto.OperationStatus{}, err
	}
	if record == nil {
		return dto.OperationStatus{OK: false, Message: "order not found"}, nil
	}
	return dto.OperationStatus{
		OK:      true,
		Message: record.Status,
		Details: map[string]string{
			"documentId": documentID,
			"orderName":  record.OrderName,
		},
	}, nil
}

func (s *Service) Download(documentID string) (dto.DownloadArtifact, error) {
	record, err := s.history.GetByDocumentID(documentID)
	if err != nil {
		return dto.DownloadArtifact{}, err
	}
	if record == nil {
		return dto.DownloadArtifact{}, fmt.Errorf("order %s not found", documentID)
	}
	if !s.canUseRemoteFlow() {
		dir, prepErr := s.downloads.PrepareArtifactDirectory(firstNonEmpty(record.OrderName, documentID))
		if prepErr != nil {
			return dto.DownloadArtifact{}, prepErr
		}
		return dto.DownloadArtifact{Directory: dir, CSVPath: record.Filename}, nil
	}

	artifact, err := s.downloads.DownloadOrderArtifacts(context.Background(), documentID, firstNonEmpty(record.OrderName, documentID))
	if err != nil {
		return dto.DownloadArtifact{}, err
	}
	record.Status = "Скачан"
	record.Filename = firstNonEmpty(artifact.CSVPath, record.Filename)
	_, _ = s.history.Upsert(*record)
	return artifact, nil
}

func (s *Service) canUseRemoteFlow() bool {
	return s.client != nil && s.signer != nil && s.cfg.BaseURL != "" && s.cfg.WarehouseID != ""
}

func (s *Service) createRemote(ctx context.Context, draft dto.OrderDraft) (dto.OrderRecord, error) {
	payload := s.BuildCreatePayload(draft)

	var createdRaw json.RawMessage
	_, err := s.client.DoJSON(ctx, "POST", "/api/v1/codes-order?warehouseId="+url.QueryEscape(s.cfg.WarehouseID), payload, &createdRaw)
	if err != nil {
		return dto.OrderRecord{}, err
	}
	documentID, err := parseIDPayload(createdRaw)
	if err != nil {
		return dto.OrderRecord{}, err
	}

	availability, _, err := s.client.DoText(ctx, "GET", "/api/v1/codes-order/"+url.PathEscape(documentID)+"/availability-status", nil)
	if err != nil {
		return dto.OrderRecord{}, err
	}
	if strings.Trim(strings.TrimSpace(availability), `"`) != "available" {
		return dto.OrderRecord{}, fmt.Errorf("document %s is not available: %s", documentID, strings.TrimSpace(availability))
	}

	thumbprint, err := s.signer.FindCertificateThumbprint(ctx)
	if err != nil {
		return dto.OrderRecord{}, err
	}

	if s.cfg.OrganizationID != "" {
		var hasCertificate bool
		_, err = s.client.DoJSON(
			ctx,
			"GET",
			"/api/v1/organizations/"+url.PathEscape(s.cfg.OrganizationID)+"/employees/has-certificate?thumbprint="+url.QueryEscape(thumbprint),
			nil,
			&hasCertificate,
		)
		if err != nil {
			return dto.OrderRecord{}, err
		}
		if !hasCertificate {
			return dto.OrderRecord{}, fmt.Errorf("certificate %s is not registered in organization", thumbprint)
		}
	}

	var ordersToSign []signOrder
	_, err = s.client.DoJSON(ctx, "GET", "/api/v1/codes-order/"+url.PathEscape(documentID)+"/orders-for-sign", nil, &ordersToSign)
	if err != nil {
		return dto.OrderRecord{}, err
	}
	if len(ordersToSign) == 0 {
		return dto.OrderRecord{}, fmt.Errorf("orders-for-sign returned empty list for %s", documentID)
	}

	signedOrders := make([]map[string]string, 0, len(ordersToSign))
	for _, order := range ordersToSign {
		signature, signErr := s.signer.SignBase64(ctx, thumbprint, order.Base64Content, true)
		if signErr != nil {
			return dto.OrderRecord{}, signErr
		}
		signedOrders = append(signedOrders, map[string]string{
			"id":            order.ID,
			"base64Content": signature,
		})
	}

	_, err = s.client.DoJSON(ctx, "POST", "/api/v1/codes-order/"+url.PathEscape(documentID)+"/send", map[string]any{
		"signedOrders": signedOrders,
	}, &map[string]any{})
	if err != nil {
		return dto.OrderRecord{}, err
	}

	var finalDoc finalOrderDocument
	_, err = s.client.DoJSON(ctx, "GET", "/api/v1/codes-order/"+url.PathEscape(documentID), nil, &finalDoc)
	if err != nil {
		return dto.OrderRecord{}, err
	}

	record := dto.OrderRecord{
		DocumentID: firstNonEmpty(finalDoc.DocumentID, finalDoc.ID, documentID),
		OrderName:  draft.OrderName,
		Status:     firstNonEmpty(finalDoc.Status, "Выполнен"),
		Simpl:      draft.SimplifiedName,
		FullName:   firstNonEmpty(draft.FullName, draft.SimplifiedName),
		GTIN:       draft.GTIN,
		Positions: []dto.PositionData{
			{Name: firstNonEmpty(draft.FullName, draft.SimplifiedName), GTIN: draft.GTIN},
		},
		Extra: map[string]string{
			"source":         "go-remote",
			"thumbprint":     thumbprint,
			"codes_count":    fmt.Sprintf("%d", draft.CodesCount),
			"release_method": s.cfg.ReleaseMethodType,
			"filling_method": s.cfg.FillingMethod,
			"created_via":    "orders.Create",
		},
	}
	if s.history != nil {
		return s.history.Upsert(record)
	}
	return record, nil
}

func (s *Service) createDraftFallback(draft dto.OrderDraft, idx int) (dto.OrderRecord, error) {
	record := dto.OrderRecord{
		DocumentID: fmt.Sprintf("GO-DRAFT-%d-%d", time.Now().Unix(), idx+1),
		OrderName:  draft.OrderName,
		Status:     "Черновик Go",
		Simpl:      draft.SimplifiedName,
		FullName:   draft.FullName,
		GTIN:       draft.GTIN,
		Positions: []dto.PositionData{
			{Name: firstNonEmpty(draft.FullName, draft.SimplifiedName), GTIN: draft.GTIN},
		},
		Extra: map[string]string{
			"preview_document_number": draft.OrderName,
			"preview_quantity":        fmt.Sprintf("%d", draft.CodesCount),
		},
	}
	if s.history != nil {
		return s.history.Upsert(record)
	}
	return record, nil
}

func parseIDPayload(raw json.RawMessage) (string, error) {
	var obj createResponse
	if err := json.Unmarshal(raw, &obj); err == nil {
		if value := firstNonEmpty(obj.ID, obj.DocumentID); value != "" {
			return value, nil
		}
	}

	var text string
	if err := json.Unmarshal(raw, &text); err == nil && text != "" {
		return text, nil
	}
	return "", fmt.Errorf("unable to parse document id from payload: %s", string(raw))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

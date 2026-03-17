package introduction

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/kontur"
)

type signer interface {
	FindCertificateThumbprint(context.Context) (string, error)
	SignBase64(context.Context, string, string, bool) (string, error)
}

type Service struct {
	cfg    config.Config
	client *kontur.Client
	signer signer
}

type ProductionPayload struct {
	DocumentNumber                  string `json:"documentNumber"`
	ProducerInn                     string `json:"producerInn"`
	ProductionDate                  string `json:"productionDate"`
	ProductionType                  string `json:"productionType"`
	WarehouseID                     string `json:"warehouseId"`
	ExpirationType                  string `json:"expirationType"`
	ExpirationDate                  string `json:"expirationDate"`
	ContainsUtilisationReport       bool   `json:"containsUtilisationReport"`
	UsageType                       string `json:"usageType"`
	CISType                         string `json:"cisType"`
	FillingMethod                   string `json:"fillingMethod"`
	BatchNumber                     string `json:"batchNumber"`
	IsAutocompletePositionsDataNeed bool   `json:"isAutocompletePositionsDataNeeded"`
	ProductsHasSameDates            bool   `json:"productsHasSameDates"`
	ProductGroup                    string `json:"productGroup"`
}

type introductionDocument struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type checkingStatus struct {
	Status string `json:"status"`
}

type generatedSignatureItem struct {
	DocumentID    string `json:"documentId"`
	Base64Content string `json:"base64Content"`
}

func NewService(cfg config.Config, auth kontur.Authenticator, signing signer) *Service {
	return &Service{
		cfg:    cfg,
		client: kontur.NewClient(cfg.BaseURL, auth),
		signer: signing,
	}
}

func (s *Service) BuildProductionPayload(patch dto.ProductionPatch, fillingMethod string) ProductionPayload {
	productGroup := s.cfg.ProductGroup
	if productGroup == "" {
		productGroup = "wheelChairs"
	}
	return ProductionPayload{
		DocumentNumber:                  patch.DocumentNumber,
		ProducerInn:                     "",
		ProductionDate:                  patch.ProductionDate + "T00:00:00.000+03:00",
		ProductionType:                  "ownProduction",
		WarehouseID:                     s.cfg.WarehouseID,
		ExpirationType:                  "milkMoreThan72",
		ExpirationDate:                  patch.ExpirationDate + "T00:00:00.000+03:00",
		ContainsUtilisationReport:       true,
		UsageType:                       "verified",
		CISType:                         "unit",
		FillingMethod:                   fillingMethod,
		BatchNumber:                     patch.BatchNumber,
		IsAutocompletePositionsDataNeed: true,
		ProductsHasSameDates:            true,
		ProductGroup:                    productGroup,
	}
}

func (s *Service) Run(request dto.IntroductionRequest) (dto.OperationStatus, error) {
	if request.CodesOrderID == "" {
		return dto.OperationStatus{OK: false, Message: "codesOrderId is required"}, nil
	}
	if s.client == nil || s.cfg.BaseURL == "" {
		payload := s.BuildProductionPayload(request.ProductionPatch, "manual")
		return dto.OperationStatus{
			OK:      true,
			Message: "Introduction flow prepared without remote transport because BASE_URL/session are unavailable.",
			Details: map[string]string{
				"codesOrderId":   request.CodesOrderID,
				"documentNumber": payload.DocumentNumber,
			},
		}, nil
	}

	ctx := context.Background()
	createPath := s.createFromCodesOrderPath(request.CodesOrderID)
	introIDRaw, _, err := s.client.DoText(ctx, "POST", createPath, nil)
	if err != nil {
		return dto.OperationStatus{}, err
	}
	introID := strings.Trim(strings.TrimSpace(introIDRaw), `"`)
	if introID == "" {
		return dto.OperationStatus{}, fmt.Errorf("create-from-codes-order returned empty introduction id")
	}

	_, _ = s.client.DoJSON(ctx, "GET", "/api/v1/codes-introduction/"+url.PathEscape(introID), nil, &introductionDocument{})

	lastCheck, err := s.pollChecking(ctx, introID)
	if err != nil {
		return dto.OperationStatus{}, err
	}

	_, _ = s.client.DoJSON(ctx, "GET", "/api/v1/codes-introduction/"+url.PathEscape(introID)+"/production", nil, &map[string]any{})

	productionPayload := s.BuildProductionPayload(request.ProductionPatch, "manual")
	_, err = s.client.DoJSON(ctx, "PATCH", "/api/v1/codes-introduction/"+url.PathEscape(introID)+"/production", productionPayload, &map[string]any{})
	if err != nil {
		return dto.OperationStatus{}, err
	}

	_, _, _ = s.client.DoText(ctx, "POST", "/api/v1/codes-introduction/"+url.PathEscape(introID)+"/positions/autocomplete", nil)

	thumbprint := request.Thumbprint
	if thumbprint == "" && s.signer != nil {
		thumbprint, err = s.signer.FindCertificateThumbprint(ctx)
		if err != nil {
			return dto.OperationStatus{}, err
		}
	}

	organizationID := firstNonEmpty(request.OrganizationID, s.cfg.OrganizationID)
	if organizationID != "" && thumbprint != "" {
		var hasCertificate bool
		_, err = s.client.DoJSON(
			ctx,
			"GET",
			"/api/v1/organizations/"+url.PathEscape(organizationID)+"/employees/has-certificate?thumbprint="+url.QueryEscape(thumbprint),
			nil,
			&hasCertificate,
		)
		if err != nil {
			return dto.OperationStatus{}, err
		}
		if !hasCertificate {
			return dto.OperationStatus{}, fmt.Errorf("certificate %s is not registered in organization", thumbprint)
		}
	}

	var generated []generatedSignatureItem
	_, err = s.client.DoJSON(ctx, "GET", "/api/v1/codes-introduction/"+url.PathEscape(introID)+"/generate-multiple", nil, &generated)
	if err != nil {
		return dto.OperationStatus{}, err
	}
	if len(generated) == 0 {
		return dto.OperationStatus{}, fmt.Errorf("generate-multiple returned empty list")
	}

	signedPayload := make([]map[string]string, 0, len(generated))
	for _, item := range generated {
		signature, signErr := s.signer.SignBase64(ctx, thumbprint, item.Base64Content, true)
		if signErr != nil {
			return dto.OperationStatus{}, signErr
		}
		signedPayload = append(signedPayload, map[string]string{
			"documentId":    item.DocumentID,
			"signedContent": signature,
		})
	}

	_, err = s.client.DoJSON(ctx, "POST", "/api/v1/codes-introduction/"+url.PathEscape(introID)+"/send-multiple", signedPayload, &map[string]any{})
	if err != nil {
		return dto.OperationStatus{}, err
	}

	var finalIntro introductionDocument
	_, err = s.client.DoJSON(ctx, "GET", "/api/v1/codes-introduction/"+url.PathEscape(introID), nil, &finalIntro)
	if err != nil {
		return dto.OperationStatus{}, err
	}
	finalCheck, _ := s.pollChecking(ctx, introID)

	return dto.OperationStatus{
		OK:      true,
		Message: "Introduction flow completed",
		Details: map[string]string{
			"introductionId": introID,
			"status":         firstNonEmpty(finalIntro.Status, finalCheck.Status, lastCheck.Status),
			"thumbprint":     thumbprint,
		},
	}, nil
}

func (s *Service) createFromCodesOrderPath(codesOrderID string) string {
	path := "/api/v1/codes-introduction/create-from-codes-order/" + url.PathEscape(codesOrderID) + "?isImportFts=false&isAccompanyingDocumentNeeds=false"
	parsed, err := url.Parse(s.cfg.BaseURL)
	if err != nil {
		return path
	}
	if parsed.Hostname() == "mk.kontur.ru" {
		if _, lookupErr := net.LookupHost(parsed.Hostname()); lookupErr != nil {
			return strings.Replace(path, "/api/v1", "/api/v1", 1)
		}
	}
	return path
}

func (s *Service) pollChecking(ctx context.Context, introID string) (checkingStatus, error) {
	okStatuses := map[string]struct{}{
		"inProgress":        {},
		"doesNotHaveErrors": {},
		"created":           {},
		"checked":           {},
		"noErrors":          {},
	}
	var latest checkingStatus
	for attempt := 0; attempt < 24; attempt++ {
		_, err := s.client.DoJSON(ctx, "GET", "/api/v1/codes-checking/"+url.PathEscape(introID), nil, &latest)
		if err == nil {
			if _, ok := okStatuses[latest.Status]; ok {
				return latest, nil
			}
		}
		time.Sleep(5 * time.Second)
	}
	if latest.Status == "" {
		return checkingStatus{}, fmt.Errorf("codes-checking timed out for %s", introID)
	}
	return latest, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

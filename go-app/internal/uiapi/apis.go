package uiapi

import (
	"context"

	"github.com/kirillbelykh/kontur_api/go-app/internal/aggregation"
	"github.com/kirillbelykh/kontur_api/go-app/internal/appstate"
	"github.com/kirillbelykh/kontur_api/go-app/internal/downloads"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/history"
	"github.com/kirillbelykh/kontur_api/go-app/internal/introduction"
	"github.com/kirillbelykh/kontur_api/go-app/internal/orders"
	"github.com/kirillbelykh/kontur_api/go-app/internal/session"
	"github.com/kirillbelykh/kontur_api/go-app/internal/system"
	"github.com/kirillbelykh/kontur_api/go-app/internal/tsd"
)

type AppStateAPI struct {
	service *appstate.Service
}

func NewAppStateAPI(service *appstate.Service) *AppStateAPI {
	return &AppStateAPI{service: service}
}

func (a *AppStateAPI) Get() (dto.AppState, error) {
	return a.service.Get(context.Background())
}

type AuthAPI struct {
	service *session.Service
}

func NewAuthAPI(service *session.Service) *AuthAPI {
	return &AuthAPI{service: service}
}

func (a *AuthAPI) RefreshSession() (dto.SessionState, error) {
	return a.service.RefreshSession(context.Background())
}

type OrdersAPI struct {
	service *orders.Service
}

func NewOrdersAPI(service *orders.Service) *OrdersAPI {
	return &OrdersAPI{service: service}
}

func (a *OrdersAPI) Create(drafts []dto.OrderDraft) ([]dto.OrderRecord, error) {
	return a.service.Create(drafts)
}

func (a *OrdersAPI) CheckStatus(documentID string) (dto.OperationStatus, error) {
	return a.service.CheckStatus(documentID)
}

func (a *OrdersAPI) Download(documentID string) (dto.DownloadArtifact, error) {
	return a.service.Download(documentID)
}

type IntroductionAPI struct {
	service *introduction.Service
}

func NewIntroductionAPI(service *introduction.Service) *IntroductionAPI {
	return &IntroductionAPI{service: service}
}

func (a *IntroductionAPI) Run(request dto.IntroductionRequest) (dto.OperationStatus, error) {
	return a.service.Run(request)
}

type TSDAPI struct {
	service *tsd.Service
}

func NewTSDAPI(service *tsd.Service) *TSDAPI {
	return &TSDAPI{service: service}
}

func (a *TSDAPI) CreateTask(request dto.TSDRequest) (dto.OperationStatus, error) {
	return a.service.CreateTask(request)
}

type AggregationAPI struct {
	service *aggregation.Service
}

func NewAggregationAPI(service *aggregation.Service) *AggregationAPI {
	return &AggregationAPI{service: service}
}

func (a *AggregationAPI) SearchAndExport(query dto.AggregationQuery) (dto.AggregationExportResult, error) {
	return a.service.SearchAndExport(context.Background(), query)
}

type HistoryAPI struct {
	service *history.Service
}

func NewHistoryAPI(service *history.Service) *HistoryAPI {
	return &HistoryAPI{service: service}
}

func (a *HistoryAPI) List(filter dto.HistoryFilter) ([]dto.OrderRecord, error) {
	return a.service.List(filter)
}

func (a *HistoryAPI) MarkTsdCreated(documentID, introNumber string) (dto.OrderRecord, error) {
	return a.service.MarkTsdCreated(documentID, introNumber)
}

type SystemAPI struct {
	service *system.Service
}

func NewSystemAPI(service *system.Service) *SystemAPI {
	return &SystemAPI{service: service}
}

func (a *SystemAPI) CheckDependencies() ([]dto.DependencyStatus, error) {
	return a.service.CheckDependencies(context.Background())
}

type DownloadsAPI struct {
	service *downloads.Service
}

func NewDownloadsAPI(service *downloads.Service) *DownloadsAPI {
	return &DownloadsAPI{service: service}
}

func (a *DownloadsAPI) PrepareArtifactDirectory(orderName string) (string, error) {
	return a.service.PrepareArtifactDirectory(orderName)
}

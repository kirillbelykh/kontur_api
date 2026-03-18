package main

import (
	"context"

	"github.com/kirillbelykh/kontur_api/go-app/internal/aggregation"
	"github.com/kirillbelykh/kontur_api/go-app/internal/appstate"
	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/crypto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/downloads"
	"github.com/kirillbelykh/kontur_api/go-app/internal/history"
	"github.com/kirillbelykh/kontur_api/go-app/internal/introduction"
	"github.com/kirillbelykh/kontur_api/go-app/internal/orders"
	"github.com/kirillbelykh/kontur_api/go-app/internal/session"
	"github.com/kirillbelykh/kontur_api/go-app/internal/system"
	"github.com/kirillbelykh/kontur_api/go-app/internal/tsd"
	"github.com/kirillbelykh/kontur_api/go-app/internal/uiapi"
)

type desktopApp struct {
	ctx context.Context
}

func (d *desktopApp) startup(ctx context.Context) {
	d.ctx = ctx
}

func NewDesktopApp(cfg config.Config) (*desktopApp, []interface{}, error) {
	historyService, err := history.NewService(cfg)
	if err != nil {
		return nil, nil, err
	}

	sessionService := session.NewService(cfg)
	cryptoService := crypto.NewService(cfg)
	systemService := system.NewService(cfg, cryptoService)
	downloadsService := downloads.NewService(cfg, sessionService)
	introductionService := introduction.NewService(cfg, sessionService, cryptoService)
	tsdService := tsd.NewService(cfg, sessionService, introductionService)
	ordersService := orders.NewService(cfg, historyService, sessionService, cryptoService)
	aggregationService := aggregation.NewService(cfg, sessionService)
	appStateService := appstate.NewService(cfg, historyService, sessionService, systemService)

	bindings := []interface{}{
		uiapi.NewAppStateAPI(appStateService),
		uiapi.NewAuthAPI(sessionService),
		uiapi.NewOrdersAPI(ordersService),
		uiapi.NewIntroductionAPI(introductionService),
		uiapi.NewTSDAPI(tsdService),
		uiapi.NewAggregationAPI(aggregationService),
		uiapi.NewHistoryAPI(historyService),
		uiapi.NewSystemAPI(systemService),
		uiapi.NewDownloadsAPI(downloadsService),
	}

	return &desktopApp{}, bindings, nil
}

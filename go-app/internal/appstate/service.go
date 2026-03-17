package appstate

import (
	"context"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/history"
	"github.com/kirillbelykh/kontur_api/go-app/internal/session"
	"github.com/kirillbelykh/kontur_api/go-app/internal/system"
)

type Service struct {
	cfg     config.Config
	history *history.Service
	session *session.Service
	system  *system.Service
}

func NewService(cfg config.Config, historyService *history.Service, sessionService *session.Service, systemService *system.Service) *Service {
	return &Service{
		cfg:     cfg,
		history: historyService,
		session: sessionService,
		system:  systemService,
	}
}

func (s *Service) Get(ctx context.Context) (dto.AppState, error) {
	orders, err := s.history.List(dto.HistoryFilter{})
	if err != nil {
		return dto.AppState{}, err
	}
	withoutTSD, err := s.history.CountWithoutTSD()
	if err != nil {
		return dto.AppState{}, err
	}
	dependencies, err := s.system.CheckDependencies(ctx)
	if err != nil {
		return dto.AppState{}, err
	}

	return dto.AppState{
		RepoRoot:         s.cfg.RepoRoot,
		HistoryPath:      s.cfg.HistoryPath,
		OrdersTotal:      len(orders),
		OrdersWithoutTSD: withoutTSD,
		Session:          s.session.State(),
		Dependencies:     dependencies,
		LastUpdated:      time.Now().Format(time.RFC3339Nano),
	}, nil
}

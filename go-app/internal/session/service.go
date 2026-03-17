package session

import (
	"context"
	"net/http"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type provider interface {
	RefreshSession(context.Context) (dto.SessionState, error)
	ConfigureRequest(*http.Request) error
	State() dto.SessionState
}

type Service struct {
	provider provider
}

func NewService(cfg config.Config) *Service {
	return &Service{provider: newProvider(cfg)}
}

func (s *Service) RefreshSession(ctx context.Context) (dto.SessionState, error) {
	return s.provider.RefreshSession(ctx)
}

func (s *Service) ConfigureRequest(req *http.Request) error {
	return s.provider.ConfigureRequest(req)
}

func (s *Service) State() dto.SessionState {
	return s.provider.State()
}

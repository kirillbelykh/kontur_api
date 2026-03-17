//go:build !windows

package session

import (
	"context"
	"errors"
	"net/http"
	"runtime"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type stubProvider struct {
	state dto.SessionState
}

func newProvider(cfg config.Config) provider {
	_ = cfg
	return &stubProvider{
		state: dto.SessionState{
			Available: false,
			Source:    "stub",
			Message:   "Session refresh is enabled only on Windows target builds with Yandex Browser profile access.",
		},
	}
}

func (s *stubProvider) RefreshSession(ctx context.Context) (dto.SessionState, error) {
	_ = ctx
	s.state.Message = "Current environment is " + runtime.GOOS + ". Windows runtime is required for browser session capture."
	return s.state, nil
}

func (s *stubProvider) ConfigureRequest(req *http.Request) error {
	_ = req
	return errors.New("session provider is unavailable on non-Windows host")
}

func (s *stubProvider) State() dto.SessionState {
	return s.state
}

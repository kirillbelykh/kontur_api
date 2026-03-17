package crypto

import (
	"context"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type provider interface {
	FindCertificateThumbprint(context.Context) (string, error)
	SignBase64(context.Context, string, string, bool) (string, error)
	State() dto.DependencyStatus
}

type Service struct {
	provider provider
}

func NewService(cfg config.Config) *Service {
	return &Service{provider: newProvider(cfg)}
}

func (s *Service) FindCertificateThumbprint(ctx context.Context) (string, error) {
	return s.provider.FindCertificateThumbprint(ctx)
}

func (s *Service) SignBase64(ctx context.Context, thumbprint, content string, detached bool) (string, error) {
	return s.provider.SignBase64(ctx, thumbprint, content, detached)
}

func (s *Service) State() dto.DependencyStatus {
	return s.provider.State()
}

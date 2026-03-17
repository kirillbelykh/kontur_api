//go:build !windows

package crypto

import (
	"context"
	"errors"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type stubProvider struct{}

func newProvider(cfg config.Config) provider {
	_ = cfg
	return &stubProvider{}
}

func (s *stubProvider) FindCertificateThumbprint(ctx context.Context) (string, error) {
	_ = ctx
	return "", errors.New("CryptoPro COM integration is available only on Windows")
}

func (s *stubProvider) SignBase64(ctx context.Context, thumbprint, content string, detached bool) (string, error) {
	_ = ctx
	_ = thumbprint
	_ = content
	_ = detached
	return "", errors.New("CryptoPro COM integration is available only on Windows")
}

func (s *stubProvider) State() dto.DependencyStatus {
	return dto.DependencyStatus{
		Name:      "cryptopro",
		Available: false,
		Status:    "stub",
		Hint:      "CryptoPro signing is wired for Windows-only COM automation.",
	}
}

package system

import (
	"context"
	"os"
	"os/exec"
	"runtime"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/crypto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

type Service struct {
	cfg    config.Config
	crypto *crypto.Service
}

func NewService(cfg config.Config, cryptoService *crypto.Service) *Service {
	return &Service{
		cfg:    cfg,
		crypto: cryptoService,
	}
}

func (s *Service) CheckDependencies(ctx context.Context) ([]dto.DependencyStatus, error) {
	_ = ctx
	statuses := []dto.DependencyStatus{
		checkOptionalFile("yandex-browser", s.cfg.YandexBrowserPath, "Yandex Browser binary can be pinned explicitly via YANDEX_BROWSER_PATH."),
		checkOptionalFile("yandex-user-data", s.cfg.YandexUserDataDir, "Browser profile directory can be pinned explicitly via YANDEX_USER_DATA_DIR."),
		checkFile("history", s.cfg.HistoryPath, "Shared order history is stored here."),
		checkFile("runtime", s.cfg.RuntimeDir, "Go-specific runtime artifacts live here."),
		checkOptionalFile("env-file", s.cfg.EnvFilePath, "User configuration for the installed app is stored here."),
		checkOptionalValue("base-url", s.cfg.BaseURL, "Kontur API base URL must be present for remote flows."),
		checkOptionalValue("warehouse-id", s.cfg.WarehouseID, "Warehouse ID is required for orders, TSD and aggregation flows."),
		checkOptionalValue("organization-id", s.cfg.OrganizationID, "Organization ID is required for certificate checks and exports."),
		{
			Name:      "mode",
			Available: true,
			Status:    s.cfg.Mode,
			Hint:      "repo mode shares the Python project history, standalone mode uses installed app data.",
		},
		{
			Name:      "target-platform",
			Available: runtime.GOOS == "windows",
			Status:    runtime.GOOS,
			Hint:      "Windows is the primary runtime target for Yandex Browser and CryptoPro integrations.",
		},
	}
	if s.crypto != nil {
		statuses = append(statuses, s.crypto.StateWithContext(ctx))
	}

	if s.cfg.Mode == "repo" {
		statuses = append([]dto.DependencyStatus{
			checkExecutable("go", "Go toolchain is required to build the alternative desktop app."),
			checkExecutable("npm", "Node.js frontend dependencies are installed through npm."),
			checkExecutable("git", "Git is required for orders-history sync."),
			checkExecutable("wails", "Wails CLI is required for local desktop builds."),
		}, statuses...)
		statuses = append(statuses, checkFile("sync-cache", s.cfg.SyncCacheDir, "Dedicated git clone for orders-history sync lives here."))
	}

	return statuses, nil
}

func checkExecutable(name, hint string) dto.DependencyStatus {
	path, err := exec.LookPath(name)
	return dto.DependencyStatus{
		Name:      name,
		Available: err == nil,
		Status:    path,
		Hint:      hint,
	}
}

func checkFile(name, path, hint string) dto.DependencyStatus {
	_, err := os.Stat(path)
	return dto.DependencyStatus{
		Name:      name,
		Available: err == nil,
		Status:    path,
		Hint:      hint,
	}
}

func checkOptionalFile(name, path, hint string) dto.DependencyStatus {
	if path == "" {
		return dto.DependencyStatus{
			Name:      name,
			Available: false,
			Status:    "",
			Hint:      hint,
		}
	}
	return checkFile(name, path, hint)
}

func checkOptionalValue(name, value, hint string) dto.DependencyStatus {
	return dto.DependencyStatus{
		Name:      name,
		Available: value != "",
		Status:    value,
		Hint:      hint,
	}
}

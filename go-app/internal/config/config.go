package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	RepoRoot          string
	GoAppRoot         string
	RuntimeDir        string
	HistoryPath       string
	SyncEnabled       bool
	SyncBranch        string
	SyncCacheDir      string
	BaseURL           string
	OrganizationID    string
	WarehouseID       string
	ProductGroup      string
	ReleaseMethodType string
	CISType           string
	FillingMethod     string
	YandexProfileName string
	YandexBrowserPath string
	YandexUserDataDir string
	YandexTargetURL   string
	CookieTTL         time.Duration
}

func Discover() (Config, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return Config{}, err
	}

	repoRoot, err := findRepoRoot(cwd)
	if err != nil {
		return Config{}, err
	}

	_ = godotenv.Overload(filepath.Join(repoRoot, ".env"))

	goAppRoot := filepath.Join(repoRoot, "go-app")
	cfg := Config{
		RepoRoot:          repoRoot,
		GoAppRoot:         goAppRoot,
		RuntimeDir:        filepath.Join(goAppRoot, "runtime"),
		HistoryPath:       filepath.Join(repoRoot, "full_orders_history.json"),
		SyncEnabled:       resolveSyncEnabled(),
		SyncBranch:        envOrDefault("HISTORY_SYNC_BRANCH", "orders-history"),
		SyncCacheDir:      filepath.Join(goAppRoot, "runtime", "history-sync"),
		BaseURL:           os.Getenv("BASE_URL"),
		OrganizationID:    os.Getenv("ORGANIZATION_ID"),
		WarehouseID:       os.Getenv("WAREHOUSE_ID"),
		ProductGroup:      os.Getenv("PRODUCT_GROUP"),
		ReleaseMethodType: os.Getenv("RELEASE_METHOD_TYPE"),
		CISType:           os.Getenv("CIS_TYPE"),
		FillingMethod:     os.Getenv("FILLING_METHOD"),
		YandexProfileName: envOrDefault("YANDEX_PROFILE_NAME", "Vinsent O`neal"),
		YandexBrowserPath: os.Getenv("YANDEX_BROWSER_PATH"),
		YandexUserDataDir: os.Getenv("YANDEX_USER_DATA_DIR"),
		YandexTargetURL:   envOrDefault("YANDEX_TARGET_URL", "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"),
		CookieTTL:         13 * time.Minute,
	}

	if err := os.MkdirAll(cfg.RuntimeDir, 0o755); err != nil {
		return Config{}, err
	}
	if err := os.MkdirAll(cfg.SyncCacheDir, 0o755); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func resolveSyncEnabled() bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv("HISTORY_SYNC_ENABLED")))
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func findRepoRoot(start string) (string, error) {
	current := start
	for {
		mainPy := filepath.Join(current, "main.py")
		mainPyw := filepath.Join(current, "main.pyw")
		if fileExists(mainPy) && fileExists(mainPyw) {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("repo root not found from current working directory")
		}
		current = parent
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

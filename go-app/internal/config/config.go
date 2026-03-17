package config

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Mode              string
	RepoRoot          string
	GoAppRoot         string
	ExecutableDir     string
	DataRoot          string
	EnvFilePath       string
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
	cwd, _ := os.Getwd()
	execPath, err := os.Executable()
	if err != nil {
		return Config{}, err
	}
	execDir := filepath.Dir(execPath)

	repoRoot := discoverRepoRoot(cwd, execDir)
	overrideEnvPath := strings.TrimSpace(os.Getenv("GOAPP_ENV_PATH"))

	if repoRoot != "" {
		repoEnv := filepath.Join(repoRoot, ".env")
		loadEnvFiles(repoEnv, overrideEnvPath)

		goAppRoot := filepath.Join(repoRoot, "go-app")
		cfg := buildConfig(
			"repo",
			repoRoot,
			goAppRoot,
			execDir,
			goAppRoot,
			repoEnv,
			filepath.Join(goAppRoot, "runtime"),
			filepath.Join(repoRoot, "full_orders_history.json"),
			resolveSyncEnabled(),
			filepath.Join(goAppRoot, "runtime", "history-sync"),
		)
		if err := ensureDirs(cfg); err != nil {
			return Config{}, err
		}
		return cfg, nil
	}

	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return Config{}, err
	}
	dataRoot := filepath.Join(userConfigDir, "KonturGoWorkbench")
	appEnvPath := filepath.Join(dataRoot, ".env")
	defaultsEnvPath := filepath.Join(execDir, ".env.defaults")
	loadEnvFiles(defaultsEnvPath, appEnvPath, overrideEnvPath)

	cfg := buildConfig(
		"standalone",
		execDir,
		execDir,
		execDir,
		dataRoot,
		appEnvPath,
		filepath.Join(dataRoot, "runtime"),
		filepath.Join(dataRoot, "full_orders_history.json"),
		false,
		filepath.Join(dataRoot, "runtime", "history-sync"),
	)
	if err := ensureDirs(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func buildConfig(
	mode string,
	repoRoot string,
	goAppRoot string,
	executableDir string,
	dataRoot string,
	envFilePath string,
	runtimeDir string,
	historyPath string,
	syncEnabled bool,
	syncCacheDir string,
) Config {
	return Config{
		Mode:              mode,
		RepoRoot:          repoRoot,
		GoAppRoot:         goAppRoot,
		ExecutableDir:     executableDir,
		DataRoot:          dataRoot,
		EnvFilePath:       envFilePath,
		RuntimeDir:        runtimeDir,
		HistoryPath:       historyPath,
		SyncEnabled:       syncEnabled,
		SyncBranch:        envOrDefault("HISTORY_SYNC_BRANCH", "orders-history"),
		SyncCacheDir:      syncCacheDir,
		BaseURL:           envOrDefault("BASE_URL", "https://mk.kontur.ru"),
		OrganizationID:    envOrDefault("ORGANIZATION_ID", "5cda50fa-523f-4bb5-85b6-66d7241b23cd"),
		WarehouseID:       envOrDefault("WAREHOUSE_ID", "59739360-7d62-434b-ad13-4617c87a6d13"),
		ProductGroup:      envOrDefault("PRODUCT_GROUP", "wheelChairs"),
		ReleaseMethodType: envOrDefault("RELEASE_METHOD_TYPE", "production"),
		CISType:           envOrDefault("CIS_TYPE", "unit"),
		FillingMethod:     envOrDefault("FILLING_METHOD", "manual"),
		YandexProfileName: envOrDefault("YANDEX_PROFILE_NAME", "Default"),
		YandexBrowserPath: firstNonEmpty(os.Getenv("YANDEX_BROWSER_PATH"), defaultYandexBrowserPath()),
		YandexUserDataDir: firstNonEmpty(os.Getenv("YANDEX_USER_DATA_DIR"), defaultYandexUserDataDir()),
		YandexTargetURL:   envOrDefault("YANDEX_TARGET_URL", "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"),
		CookieTTL:         13 * time.Minute,
	}
}

func ensureDirs(cfg Config) error {
	if err := os.MkdirAll(cfg.DataRoot, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.RuntimeDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.SyncCacheDir, 0o755); err != nil {
		return err
	}
	return nil
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

func discoverRepoRoot(paths ...string) string {
	override := strings.TrimSpace(os.Getenv("KONTUR_REPO_ROOT"))
	if override != "" {
		if repoRoot, err := findRepoRoot(override); err == nil {
			return repoRoot
		}
	}
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}
		if repoRoot, err := findRepoRoot(path); err == nil {
			return repoRoot
		}
	}
	return ""
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

func loadEnvFiles(paths ...string) {
	for _, path := range paths {
		if strings.TrimSpace(path) == "" || !fileExists(path) {
			continue
		}
		_ = godotenv.Overload(path)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func defaultYandexUserDataDir() string {
	switch runtime.GOOS {
	case "windows":
		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData == "" {
			return ""
		}
		return filepath.Join(localAppData, "Yandex", "YandexBrowser", "User Data")
	default:
		return ""
	}
}

func defaultYandexBrowserPath() string {
	if runtime.GOOS != "windows" {
		return ""
	}
	candidates := []string{
		filepath.Join(os.Getenv("LOCALAPPDATA"), "Yandex", "YandexBrowser", "Application", "browser.exe"),
		filepath.Join(os.Getenv("ProgramFiles"), "Yandex", "YandexBrowser", "Application", "browser.exe"),
		filepath.Join(os.Getenv("ProgramFiles(x86)"), "Yandex", "YandexBrowser", "Application", "browser.exe"),
	}
	for _, candidate := range candidates {
		if fileExists(candidate) {
			return candidate
		}
	}
	return ""
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

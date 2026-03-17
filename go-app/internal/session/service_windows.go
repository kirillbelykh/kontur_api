//go:build windows

package session

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

var requiredCookies = []string{
	"auth.sid",
	"token",
	"portaluserid",
	"auth.check",
	"ngtoken",
	"device",
}

type windowsProvider struct {
	cfg     config.Config
	mu      sync.RWMutex
	cookies map[string]string
	state   dto.SessionState
}

func newProvider(cfg config.Config) provider {
	return &windowsProvider{
		cfg:     cfg,
		cookies: map[string]string{},
		state: dto.SessionState{
			Available:       false,
			Source:          "chromedp",
			Message:         "Session not initialized yet.",
			RequiredCookies: requiredCookies,
		},
	}
}

func (w *windowsProvider) RefreshSession(ctx context.Context) (dto.SessionState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	options := []chromedp.ExecAllocatorOption{
		chromedp.Flag("headless", false),
		chromedp.Flag("disable-gpu", false),
		chromedp.Flag("remote-debugging-port", 0),
	}
	if w.cfg.YandexBrowserPath != "" {
		options = append(options, chromedp.ExecPath(w.cfg.YandexBrowserPath))
	}
	if w.cfg.YandexUserDataDir != "" {
		options = append(options, chromedp.UserDataDir(w.cfg.YandexUserDataDir))
	}

	allocCtx, cancelAlloc := chromedp.NewExecAllocator(ctx, options...)
	defer cancelAlloc()

	taskCtx, cancelTask := chromedp.NewContext(allocCtx)
	defer cancelTask()

	targetURL := w.cfg.YandexTargetURL
	if targetURL == "" {
		targetURL = "https://mk.kontur.ru/organizations/5cda50fa-523f-4bb5-85b6-66d7241b23cd/warehouses"
	}

	if err := chromedp.Run(taskCtx,
		network.Enable(),
		chromedp.Navigate(targetURL),
		chromedp.Sleep(4*time.Second),
	); err != nil {
		w.state = dto.SessionState{
			Available:       false,
			Source:          "chromedp",
			Message:         err.Error(),
			RequiredCookies: requiredCookies,
		}
		return w.state, err
	}

	cookies, err := network.GetCookies().Do(taskCtx)
	if err != nil {
		return w.state, err
	}

	collected := make(map[string]string, len(cookies))
	for _, cookie := range cookies {
		collected[cookie.Name] = cookie.Value
	}

	var missing []string
	for _, field := range requiredCookies {
		if collected[field] == "" {
			missing = append(missing, field)
		}
	}
	if len(missing) != 0 {
		w.state = dto.SessionState{
			Available:       false,
			Source:          "chromedp",
			Message:         "Missing required cookies: " + strings.Join(missing, ", "),
			RequiredCookies: requiredCookies,
		}
		return w.state, fmt.Errorf("missing required cookies: %s", strings.Join(missing, ", "))
	}

	w.cookies = collected
	now := time.Now()
	w.state = dto.SessionState{
		Available:       true,
		Source:          "chromedp",
		UpdatedAt:       now.Format(time.RFC3339Nano),
		ExpiresAt:       now.Add(w.cfg.CookieTTL).Format(time.RFC3339Nano),
		Message:         "Captured cookies from browser profile successfully.",
		RequiredCookies: requiredCookies,
	}
	return w.state, nil
}

func (w *windowsProvider) ConfigureRequest(req *http.Request) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if len(w.cookies) == 0 {
		return fmt.Errorf("session cookies are empty")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	var cookiePairs []string
	for key, value := range w.cookies {
		req.AddCookie(&http.Cookie{
			Name:   key,
			Value:  value,
			Domain: "mk.kontur.ru",
			Path:   "/",
		})
		cookiePairs = append(cookiePairs, key+"="+value)
	}
	req.Header.Set("Cookie", strings.Join(cookiePairs, "; "))
	return nil
}

func (w *windowsProvider) State() dto.SessionState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.state
}

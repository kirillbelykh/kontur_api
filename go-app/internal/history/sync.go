package history

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
)

const (
	syncPullInterval = 20 * time.Second
	syncPushRetries  = 3
)

func (s *Service) initSync() {
	if !s.cfg.SyncEnabled {
		return
	}
	relPath, err := filepath.Rel(s.cfg.RepoRoot, s.cfg.HistoryPath)
	if err != nil || strings.HasPrefix(relPath, "..") {
		return
	}
	s.syncRelPath = filepath.ToSlash(relPath)

	originURL, err := s.runGit(s.cfg.RepoRoot, []string{"remote", "get-url", "origin"}, true)
	if err != nil {
		return
	}
	s.originURL = strings.TrimSpace(originURL)
	if s.originURL == "" {
		return
	}

	go s.syncWithGitHub(true, true, "startup")
}

func (s *Service) syncWithGitHub(force, push bool, reason string) bool {
	if !s.cfg.SyncEnabled || s.originURL == "" || s.syncRelPath == "" {
		return false
	}

	s.syncMu.Lock()
	defer s.syncMu.Unlock()

	if !force && !push && time.Since(s.lastSyncPullAt) < syncPullInterval {
		return false
	}

	repoDir, err := s.ensureSyncRepo()
	if err != nil {
		return false
	}

	s.mu.RLock()
	localData, err := s.load()
	s.mu.RUnlock()
	if err != nil {
		return false
	}
	mergedForLocal := localData

	for attempt := 0; attempt < syncPushRetries; attempt++ {
		if err := s.checkoutSyncBranch(repoDir); err != nil {
			break
		}

		syncFile := filepath.Join(repoDir, filepath.FromSlash(s.syncRelPath))
		remoteData, err := s.readPayload(syncFile)
		if err != nil {
			remoteData = filePayload{Orders: []dto.OrderRecord{}}
		}
		merged := remoteData
		merged.Orders = MergePayloads(remoteData.Orders, localData.Orders)
		mergedForLocal = merged

		committed := false
		if push && !reflect.DeepEqual(merged.Orders, remoteData.Orders) {
			if err := s.writePayload(syncFile, merged); err != nil {
				break
			}
			committed, err = s.stageAndCommit(repoDir, "Sync order history ("+reason+")")
			if err != nil {
				break
			}
		}

		if !push || !committed {
			break
		}

		pushed, retryable := s.pushSyncBranch(repoDir)
		if pushed {
			break
		}
		if !retryable {
			break
		}
	}

	if !reflect.DeepEqual(mergedForLocal.Orders, localData.Orders) {
		s.mu.Lock()
		_ = s.save(mergedForLocal)
		s.mu.Unlock()
	}
	s.lastSyncPullAt = time.Now()
	return true
}

func (s *Service) ensureSyncRepo() (string, error) {
	gitDir := filepath.Join(s.cfg.SyncCacheDir, ".git")
	if _, err := os.Stat(gitDir); err == nil {
		return s.cfg.SyncCacheDir, nil
	}

	if entries, err := os.ReadDir(s.cfg.SyncCacheDir); err == nil && len(entries) != 0 {
		if err := os.RemoveAll(s.cfg.SyncCacheDir); err != nil {
			return "", err
		}
		if err := os.MkdirAll(s.cfg.SyncCacheDir, 0o755); err != nil {
			return "", err
		}
	}

	if _, err := s.runGit(s.cfg.RepoRoot, []string{"clone", s.originURL, s.cfg.SyncCacheDir}, false); err != nil {
		return "", err
	}
	return s.cfg.SyncCacheDir, nil
}

func (s *Service) checkoutSyncBranch(repoDir string) error {
	if _, err := s.runGit(repoDir, []string{"fetch", "origin", "--prune"}, true); err != nil {
		return err
	}

	remoteExistsOutput, err := s.runGit(repoDir, []string{"ls-remote", "--heads", "origin", s.cfg.SyncBranch}, true)
	if err != nil {
		return err
	}
	if strings.TrimSpace(remoteExistsOutput) != "" {
		_, err = s.runGit(repoDir, []string{"checkout", "-B", s.cfg.SyncBranch, "origin/" + s.cfg.SyncBranch}, true)
		return err
	}
	_, err = s.runGit(repoDir, []string{"checkout", "-B", s.cfg.SyncBranch}, true)
	return err
}

func (s *Service) stageAndCommit(repoDir, message string) (bool, error) {
	relPath := filepath.ToSlash(s.syncRelPath)
	if _, err := s.runGit(repoDir, []string{"add", relPath}, true); err != nil {
		return false, err
	}

	status, err := s.runGit(repoDir, []string{"status", "--porcelain", "--", relPath}, true)
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(status) == "" {
		return false, nil
	}

	if _, err := s.runGit(repoDir, []string{"config", "user.name", currentUser()}, true); err != nil {
		return false, err
	}
	email := currentUser() + "@local"
	if _, err := s.runGit(repoDir, []string{"config", "user.email", email}, true); err != nil {
		return false, err
	}
	if _, err := s.runGit(repoDir, []string{"commit", "-m", message}, true); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Service) pushSyncBranch(repoDir string) (bool, bool) {
	output, err := s.runGit(repoDir, []string{"push", "origin", s.cfg.SyncBranch}, true)
	if err == nil {
		_ = output
		return true, false
	}
	lower := strings.ToLower(err.Error())
	return false, strings.Contains(lower, "non-fast-forward") || strings.Contains(lower, "rejected")
}

func (s *Service) runGit(cwd string, args []string, captureOutput bool) (string, error) {
	cmd := exec.Command("git", args...)
	cmd.Dir = cwd
	if captureOutput {
		output, err := cmd.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("git %s failed: %s", strings.Join(args, " "), strings.TrimSpace(string(output)))
		}
		return string(output), nil
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s failed: %s", strings.Join(args, " "), strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func (s *Service) readPayload(path string) (filePayload, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return filePayload{}, err
	}
	var payload filePayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return filePayload{}, err
	}
	if payload.Orders == nil {
		payload.Orders = []dto.OrderRecord{}
	}
	return payload, nil
}

func (s *Service) writePayload(path string, payload filePayload) error {
	payload.LastUpdate = nowISO()
	payload.UpdatedBy = currentUser()
	payload.Storage = path
	if payload.CreatedBy == "" {
		payload.CreatedBy = currentUser()
	}
	if payload.Orders == nil {
		payload.Orders = []dto.OrderRecord{}
	}

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

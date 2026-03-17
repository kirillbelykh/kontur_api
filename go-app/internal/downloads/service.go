package downloads

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
	"github.com/kirillbelykh/kontur_api/go-app/internal/dto"
	"github.com/kirillbelykh/kontur_api/go-app/internal/kontur"
)

type Service struct {
	cfg         config.Config
	desktopRoot string
	client      *kontur.Client
}

type printTemplate struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Size    string `json:"size"`
	DekkoID string `json:"dekkoId"`
}

type exportStartResponse struct {
	ResultID string `json:"resultId"`
}

type exportStatusResponse struct {
	Status    string `json:"status"`
	FileInfos []struct {
		FileID          string `json:"fileId"`
		FileURL         string `json:"fileUrl"`
		FileURLAbsolute string `json:"fileUrlAbsolute"`
	} `json:"fileInfos"`
}

type orderStatusResponse struct {
	Status string `json:"status"`
}

func NewService(cfg config.Config, auth kontur.Authenticator) *Service {
	desktopRoot := filepath.Join(userHomeDir(), "Desktop")
	return &Service{
		cfg:         cfg,
		desktopRoot: desktopRoot,
		client:      kontur.NewClient(cfg.BaseURL, auth),
	}
}

func (s *Service) PrepareArtifactDirectory(orderName string) (string, error) {
	parent := filepath.Join(s.desktopRoot, "Коды км")
	target := filepath.Join(parent, SafeFilesystemName(orderName, 120))
	if err := os.MkdirAll(target, 0o755); err != nil {
		return "", err
	}
	return target, nil
}

func (s *Service) PrepareAggregationDirectory(filename string) (string, string, error) {
	parent := filepath.Join(s.desktopRoot, "Агрег коды км")
	targetDir := filepath.Join(parent, filename)
	targetPath := filepath.Join(targetDir, filename)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return "", "", err
	}
	return targetDir, targetPath, nil
}

func (s *Service) DownloadOrderArtifacts(ctx context.Context, documentID, orderName string) (dto.DownloadArtifact, error) {
	if s.client == nil || s.cfg.BaseURL == "" {
		dir, err := s.PrepareArtifactDirectory(orderName)
		if err != nil {
			return dto.DownloadArtifact{}, err
		}
		return dto.DownloadArtifact{Directory: dir}, nil
	}

	if err := s.waitForReleased(ctx, documentID); err != nil {
		return dto.DownloadArtifact{}, err
	}

	targetDir, err := s.PrepareArtifactDirectory(orderName)
	if err != nil {
		return dto.DownloadArtifact{}, err
	}
	safeBase := SafeFilesystemName(orderName, 100)

	var artifact dto.DownloadArtifact
	artifact.Directory = targetDir

	if pdfPath, err := s.downloadPDF(ctx, documentID, targetDir, safeBase); err == nil {
		artifact.PDFPath = pdfPath
	}
	if csvPath, err := s.downloadExportFile(ctx, documentID, "csv", targetDir, safeBase+".csv", true); err == nil {
		artifact.CSVPath = csvPath
	}
	if xlsPath, err := s.downloadExportFile(ctx, documentID, "xls", targetDir, safeBase+".xls", false); err == nil {
		artifact.XLSPath = xlsPath
	}
	return artifact, nil
}

func (s *Service) ProcessCSVFile(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	processed := ProcessCSVContent(content)
	return os.WriteFile(path, processed, 0o644)
}

func ProcessCSVContent(content []byte) []byte {
	lines := strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n")
	var out bytes.Buffer

	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		if len(parts) >= 3 {
			first := strings.Trim(parts[0], "\"")
			first = strings.ReplaceAll(first, `""`, `"`)
			out.WriteString("^1")
			out.WriteString(first)
			out.WriteString("\t")
			out.WriteString(parts[1])
			out.WriteString("\t")
			out.WriteString(parts[2])
			out.WriteString("\n")
			continue
		}
		out.WriteString(line)
		out.WriteString("\n")
	}
	return out.Bytes()
}

func SafeFilesystemName(value string, maxLen int) string {
	if strings.TrimSpace(value) == "" {
		value = "untitled"
	}

	filtered := strings.Map(func(r rune) rune {
		switch {
		case r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r
		case r >= 'a' && r <= 'z':
			return r
		case r >= 0x0400 && r <= 0x04FF:
			return r
		case r == ' ' || r == '-' || r == '_':
			return r
		default:
			return -1
		}
	}, value)

	filtered = strings.TrimSpace(filtered)
	if filtered == "" {
		filtered = "untitled"
	}
	if len([]rune(filtered)) > maxLen {
		filtered = string([]rune(filtered)[:maxLen])
	}
	return filtered
}

func (s *Service) waitForReleased(ctx context.Context, documentID string) error {
	for attempt := 0; attempt < 10; attempt++ {
		var doc orderStatusResponse
		if _, err := s.client.DoJSON(ctx, "GET", "/api/v1/codes-order/"+documentID, nil, &doc); err != nil {
			return err
		}
		if doc.Status == "released" || doc.Status == "received" {
			return nil
		}
		time.Sleep(30 * time.Second)
	}
	return fmt.Errorf("order %s did not reach released state within timeout", documentID)
}

func (s *Service) downloadPDF(ctx context.Context, documentID, targetDir, safeBase string) (string, error) {
	if s.cfg.OrganizationID == "" {
		return "", fmt.Errorf("organization id is empty")
	}

	var templates []printTemplate
	_, err := s.client.DoJSON(
		ctx,
		"GET",
		"/api/v1/print-templates?organizationId="+s.cfg.OrganizationID+"&formTypes=codesOrder",
		nil,
		&templates,
	)
	if err != nil {
		return "", err
	}

	templateID := ""
	for _, tpl := range templates {
		if tpl.Name == "Этикетка 30x20" || tpl.Size == "30х20" || tpl.DekkoID == "30x20Template_v2" {
			templateID = tpl.ID
			break
		}
	}
	if templateID == "" {
		return "", fmt.Errorf("no matching print template found")
	}

	var exportStart exportStartResponse
	_, err = s.client.DoJSON(
		ctx,
		"POST",
		"/api/v1/codes-order/"+documentID+"/export/pdf?splitByGtins=false&templateId="+templateID,
		nil,
		&exportStart,
	)
	if err != nil {
		return "", err
	}

	status, err := s.waitExportStatus(ctx, documentID, "pdf", exportStart.ResultID, 12, 10*time.Second)
	if err != nil {
		return "", err
	}
	if len(status.FileInfos) == 0 {
		return "", fmt.Errorf("pdf export has no files")
	}
	downloadURL := firstNonEmpty(status.FileInfos[0].FileURLAbsolute, status.FileInfos[0].FileURL)
	if downloadURL == "" {
		return "", fmt.Errorf("pdf export returned empty file url")
	}

	path := filepath.Join(targetDir, safeBase+".pdf")
	if err := s.client.Download(ctx, downloadURL, path); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Service) downloadExportFile(ctx context.Context, documentID, format, targetDir, filename string, processCSV bool) (string, error) {
	var exportStart exportStartResponse
	_, err := s.client.DoJSON(
		ctx,
		"POST",
		"/api/v1/codes-order/"+documentID+"/export/"+format+"?splitByGtins=false",
		nil,
		&exportStart,
	)
	if err != nil {
		return "", err
	}

	status, err := s.waitExportStatus(ctx, documentID, format, exportStart.ResultID, 30, 10*time.Second)
	if err != nil {
		return "", err
	}
	if len(status.FileInfos) == 0 {
		return "", fmt.Errorf("%s export has no file infos", format)
	}

	fileID := status.FileInfos[0].FileID
	if fileID == "" {
		return "", fmt.Errorf("%s export returned empty file id", format)
	}
	targetPath := filepath.Join(targetDir, filename)
	downloadPath := fmt.Sprintf("/api/v1/codes-order/%s/export/%s/%s/download/%s", documentID, format, exportStart.ResultID, fileID)
	if err := s.client.Download(ctx, downloadPath, targetPath); err != nil {
		return "", err
	}
	if processCSV {
		if err := s.ProcessCSVFile(targetPath); err != nil {
			return "", err
		}
	}
	return targetPath, nil
}

func (s *Service) waitExportStatus(ctx context.Context, documentID, format, resultID string, attempts int, sleep time.Duration) (exportStatusResponse, error) {
	for attempt := 0; attempt < attempts; attempt++ {
		var status exportStatusResponse
		_, err := s.client.DoJSON(
			ctx,
			"GET",
			fmt.Sprintf("/api/v1/codes-order/%s/export/%s/%s", documentID, format, resultID),
			nil,
			&status,
		)
		if err != nil {
			return exportStatusResponse{}, err
		}
		if status.Status == "success" {
			return status, nil
		}
		time.Sleep(sleep)
	}
	return exportStatusResponse{}, fmt.Errorf("%s export for %s timed out", format, documentID)
}

func ParseIDPayload(raw json.RawMessage) (string, error) {
	var start exportStartResponse
	if err := json.Unmarshal(raw, &start); err == nil && start.ResultID != "" {
		return start.ResultID, nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil && text != "" {
		return text, nil
	}
	return "", fmt.Errorf("unable to parse id payload: %s", string(raw))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func userHomeDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return home
}

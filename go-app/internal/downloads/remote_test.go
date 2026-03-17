package downloads

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
)

type noAuth struct{}

func (noAuth) ConfigureRequest(req *http.Request) error {
	return nil
}

func TestDownloadOrderArtifacts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/codes-order/DOC-1":
			_, _ = w.Write([]byte(`{"status":"released"}`))
		case "/api/v1/print-templates":
			_, _ = w.Write([]byte(`[{"id":"tpl-1","name":"Этикетка 30x20","dekkoId":"30x20Template_v2"}]`))
		case "/api/v1/codes-order/DOC-1/export/pdf":
			_, _ = w.Write([]byte(`{"resultId":"pdf-1"}`))
		case "/api/v1/codes-order/DOC-1/export/pdf/pdf-1":
			_, _ = w.Write([]byte(`{"status":"success","fileInfos":[{"fileUrl":"/files/pdf"}]}`))
		case "/files/pdf":
			_, _ = w.Write([]byte("pdf"))
		case "/api/v1/codes-order/DOC-1/export/csv":
			_, _ = w.Write([]byte(`{"resultId":"csv-1"}`))
		case "/api/v1/codes-order/DOC-1/export/csv/csv-1":
			_, _ = w.Write([]byte(`{"status":"success","fileInfos":[{"fileId":"csv-file"}]}`))
		case "/api/v1/codes-order/DOC-1/export/csv/csv-1/download/csv-file":
			_, _ = w.Write([]byte("\"046501\"\tA\tB\n"))
		case "/api/v1/codes-order/DOC-1/export/xls":
			_, _ = w.Write([]byte(`{"resultId":"xls-1"}`))
		case "/api/v1/codes-order/DOC-1/export/xls/xls-1":
			_, _ = w.Write([]byte(`{"status":"success","fileInfos":[{"fileId":"xls-file"}]}`))
		case "/api/v1/codes-order/DOC-1/export/xls/xls-1/download/xls-file":
			_, _ = w.Write([]byte("xls"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	service := NewService(config.Config{
		BaseURL:        server.URL,
		OrganizationID: "org-1",
	}, noAuth{})
	service.desktopRoot = t.TempDir()

	artifact, err := service.DownloadOrderArtifacts(t.Context(), "DOC-1", "Order 1")
	if err != nil {
		t.Fatalf("download order artifacts: %v", err)
	}
	if artifact.CSVPath == "" || artifact.PDFPath == "" || artifact.XLSPath == "" {
		t.Fatalf("unexpected artifact: %+v", artifact)
	}

	content, err := os.ReadFile(artifact.CSVPath)
	if err != nil {
		t.Fatalf("read processed csv: %v", err)
	}
	if string(content) != "^1046501\tA\tB\n" {
		t.Fatalf("unexpected csv content: %q", string(content))
	}
}

package downloads

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
)

func TestProcessCSVContent(t *testing.T) {
	input := []byte("\"046501\"\tA\tB\nplain line\n")
	output := string(ProcessCSVContent(input))

	expected := "^1046501\tA\tB\nplain line\n"
	if output != expected {
		t.Fatalf("unexpected output: %q", output)
	}
}

func TestSafeFilesystemName(t *testing.T) {
	got := SafeFilesystemName("Товар / test * 01", 120)
	if got != "Товар  test  01" {
		t.Fatalf("unexpected safe name: %q", got)
	}
}

func TestPrepareAggregationDirectory(t *testing.T) {
	tmp := t.TempDir()
	service := NewService(config.Config{}, nil)
	service.desktopRoot = tmp

	dir, target, err := service.PrepareAggregationDirectory("codes.csv")
	if err != nil {
		t.Fatalf("prepare directory: %v", err)
	}
	if dir != filepath.Join(tmp, "Агрег коды км", "codes.csv") {
		t.Fatalf("unexpected dir: %s", dir)
	}
	if target != filepath.Join(dir, "codes.csv") {
		t.Fatalf("unexpected target: %s", target)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("expected directory to exist: %v", err)
	}
}

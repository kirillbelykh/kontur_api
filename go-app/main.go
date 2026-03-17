package main

import (
	"embed"
	"log"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"

	"github.com/kirillbelykh/kontur_api/go-app/internal/config"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	cfg, err := config.Discover()
	if err != nil {
		log.Fatalf("discover config: %v", err)
	}

	desktopApp, bindings, err := NewDesktopApp(cfg)
	if err != nil {
		log.Fatalf("create app: %v", err)
	}

	err = wails.Run(&options.App{
		Title:            "Kontur Go Workbench",
		Width:            1440,
		Height:           960,
		MinWidth:         1200,
		MinHeight:        780,
		Frameless:        false,
		DisableResize:    false,
		BackgroundColour: &options.RGBA{R: 14, G: 18, B: 27, A: 1},
		OnStartup:        desktopApp.startup,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		Bind: bindings,
	})
	if err != nil {
		log.Fatalf("run wails: %v", err)
	}
}

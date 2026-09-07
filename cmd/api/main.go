package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/UllasSG/Uptime-status-checker/internal/config"
	"github.com/UllasSG/Uptime-status-checker/internal/handler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var addr string
	var configPath string

	flag.StringVar(&addr, "addr", ":8080", "port the server should run on")
	flag.StringVar(&configPath, "configPath", "configs/dev.json", "port the server should run on")

	_, err := config.Load(configPath)
	if err != nil {
		log.Fatal("Cannot load config")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handler.Health)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Server started at :8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ListenAndServe: %s", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Shutdown: %s", err)
	}
}

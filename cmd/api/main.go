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
	"github.com/UllasSG/Uptime-status-checker/internal/scheduler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var addr string
	var configPath string

	flag.StringVar(&addr, "addr", ":8080", "port the server should run on")
	flag.StringVar(&configPath, "configPath", "configs/dev.json", "path to the config file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatal("Cannot load config")
	}

	srv := handler.NewServer(cfg)

	jobs := make(chan scheduler.Job, 100)
	worker := scheduler.NewWorkerPool(jobs)
	sched := scheduler.NewScheduler(cfg.Targets, jobs, worker)
	sched.Dispatch(ctx, cfg.Workers)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", srv.Health)
	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Printf("Server started at %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("ListenAndServe: %s", err)
		}
	}()

	<-ctx.Done()
	log.Println("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Shutdown: %s", err)
	}
}

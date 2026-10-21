package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"linkage-gateway/internal/config"
	"linkage-gateway/internal/linkage"
	"linkage-gateway/internal/store"
	"linkage-gateway/internal/web"
)

func main() {
	if err := run(); err != nil {
		slog.Error("linkage gateway stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "", "path to the YAML configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger := newLogger(cfg.Logging)
	slog.SetDefault(logger)

	location, err := time.LoadLocation(cfg.Protocol.EventTimezone)
	if err != nil {
		return fmt.Errorf("load event timezone: %w", err)
	}
	storage, err := store.Open(context.Background(), store.Options{
		Path:          cfg.Database.Path,
		BusyTimeoutMS: cfg.Database.BusyTimeoutMS,
		JournalMode:   cfg.Database.JournalMode,
	})
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			logger.Error("close database", "error", err)
		}
	}()

	webServer := web.NewServer(storage, cfg.Web, location, logger)
	defer webServer.Close()
	linkageService, err := linkage.NewService(
		storage,
		webServer.Publish,
		location,
		time.Duration(cfg.Protocol.DedupWindowMS)*time.Millisecond,
		logger,
	)
	if err != nil {
		return fmt.Errorf("create linkage service: %w", err)
	}

	server := &http.Server{
		Addr:              cfg.Web.Listen,
		Handler:           webServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	signalContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	runContext, cancel := context.WithCancel(signalContext)
	defer cancel()

	httpDone := make(chan error, 1)
	udpDone := make(chan error, 1)
	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		httpDone <- err
	}()
	go func() {
		udpDone <- linkage.ServeUDP(runContext, cfg.UDP.Listen, cfg.UDP.MaxPacketBytes, linkageService, logger)
	}()
	logger.Info("linkage gateway started", "udp_listen", cfg.UDP.Listen, "web_listen", cfg.Web.Listen, "database", cfg.Database.Path)

	var runErr error
	completedServers := 0
	select {
	case <-signalContext.Done():
	case err := <-httpDone:
		completedServers++
		if err != nil {
			runErr = fmt.Errorf("HTTP server: %w", err)
		}
	case err := <-udpDone:
		completedServers++
		if err != nil {
			runErr = fmt.Errorf("UDP server: %w", err)
		}
	}
	cancel()
	webServer.Close()

	shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownContext); err != nil && runErr == nil {
		runErr = fmt.Errorf("shut down HTTP server: %w", err)
	}
	for completedServers < 2 {
		select {
		case err := <-httpDone:
			completedServers++
			if err != nil && runErr == nil {
				runErr = fmt.Errorf("HTTP server: %w", err)
			}
		case err := <-udpDone:
			completedServers++
			if err != nil && runErr == nil {
				runErr = fmt.Errorf("UDP server: %w", err)
			}
		case <-shutdownContext.Done():
			if runErr == nil {
				runErr = fmt.Errorf("wait for servers to stop: %w", shutdownContext.Err())
			}
			return runErr
		}
	}
	if runErr != nil {
		return runErr
	}
	logger.Info("linkage gateway stopped")
	return nil
}

func newLogger(loggingConfig config.LoggingConfig) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToUpper(loggingConfig.Level) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}
	options := &slog.HandlerOptions{Level: level}
	if strings.EqualFold(loggingConfig.Format, "json") {
		return slog.New(slog.NewJSONHandler(os.Stderr, options))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, options))
}

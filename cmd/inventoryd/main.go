// Command inventoryd owns stock levels and reservations, and answers the orchestrator's reserve and release commands.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/config"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/service"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "inventoryd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.Inventory)
	if err != nil {
		return err
	}
	logger := service.NewLogger(os.Stdout, cfg.LogLevel, string(cfg.Service))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := service.New(logger, service.Options{
		Addr:            cfg.HTTPAddr,
		ShutdownDrain:   cfg.ShutdownDrain,
		ShutdownTimeout: cfg.ShutdownTimeout,
	})
	return srv.Run(ctx, httpapi.NewHandler(logger, httpapi.Options{Ready: srv.Ready}))
}

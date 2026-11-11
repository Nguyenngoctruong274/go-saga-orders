// Command paymentd owns payment authorizations, and answers the orchestrator's authorize and void commands.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/config"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/kafka"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/service"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "paymentd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.Payment)
	if err != nil {
		return err
	}
	logger := service.NewLogger(os.Stdout, cfg.LogLevel, string(cfg.Service))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kc, err := kafka.NewClient(kafka.Options{Brokers: cfg.KafkaBrokers, ClientID: string(cfg.Service), Logger: logger})
	if err != nil {
		return err
	}
	defer kc.Close()

	srv := service.New(logger, service.Options{
		Addr:            cfg.HTTPAddr,
		ShutdownDrain:   cfg.ShutdownDrain,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Check:           kc.Ping,
	})
	return srv.Run(ctx, httpapi.NewHandler(logger, httpapi.Options{Ready: srv.Ready}))
}

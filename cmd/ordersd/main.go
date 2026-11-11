// Command ordersd owns orders and runs the saga orchestrator that reserves stock and takes payment for each of them.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/config"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/kafka"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/message"
	"github.com/Nguyenngoctruong274/go-saga-orders/internal/service"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ordersd:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(config.Orders)
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

	// ordersd owns the topology: it creates the command, reply and
	// dead-letter topics, retrying until the broker is reachable.
	go func() {
		if err := kc.EnsureTopicsRetrying(ctx, message.Topics(), cfg.KafkaReplication, 2*time.Second); err == nil {
			logger.Info("kafka topics ready", slog.Int("topics", len(message.Topics())))
		}
	}()

	srv := service.New(logger, service.Options{
		Addr:            cfg.HTTPAddr,
		ShutdownDrain:   cfg.ShutdownDrain,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Check:           kc.Ping,
	})
	return srv.Run(ctx, httpapi.NewHandler(logger, httpapi.Options{Ready: srv.Ready}))
}

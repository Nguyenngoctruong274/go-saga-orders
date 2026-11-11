// Package config loads the runtime configuration of a go-saga-orders
// daemon from the environment. The three daemons (ordersd, inventoryd,
// paymentd) read the same variables; each runs with its own database and
// its own values, so the names carry no service prefix.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Service names one of the daemons in this repository.
type Service string

// The daemons. Each has its own default port so all three can run on one
// machine without configuration.
const (
	Orders    Service = "ordersd"
	Inventory Service = "inventoryd"
	Payment   Service = "paymentd"
)

var defaultAddr = map[Service]string{
	Orders:    ":8080",
	Inventory: ":8081",
	Payment:   ":8082",
}

// Config is the complete runtime configuration of one daemon.
type Config struct {
	// Service is the daemon this configuration was loaded for.
	Service  Service
	HTTPAddr string
	// DatabaseURL points at the PostgreSQL database owned by this daemon.
	// Services share nothing but Kafka: ordersd, inventoryd and paymentd
	// each have their own database.
	DatabaseURL string
	// DBMaxConns caps open connections to DatabaseURL.
	DBMaxConns int
	// KafkaBrokers are the bootstrap brokers, as host:port.
	KafkaBrokers []string
	// KafkaReplication is the replication factor of the topics ordersd
	// creates: 1 for a single local broker, 3 in production.
	KafkaReplication int16
	// ShutdownTimeout bounds the graceful shutdown of the HTTP server.
	ShutdownTimeout time.Duration
	// ShutdownDrain is how long to keep serving with /readyz failing before
	// closing the listener. 0 locally; a few seconds behind a load balancer.
	ShutdownDrain time.Duration
	LogLevel      slog.Level
}

// Load reads the configuration of svc from environment variables, applying
// defaults for optional values. It reports every invalid value at once so
// that a misconfigured deployment fails fast with a complete error message.
func Load(svc Service) (Config, error) {
	return load(svc, os.Getenv)
}

func load(svc Service, getenv func(string) string) (Config, error) {
	addr, ok := defaultAddr[svc]
	if !ok {
		return Config{}, fmt.Errorf("unknown service %q", svc)
	}
	cfg := Config{
		Service:          svc,
		HTTPAddr:         valueOr(getenv("SAGA_HTTP_ADDR"), addr),
		DatabaseURL:      getenv("SAGA_DATABASE_URL"),
		DBMaxConns:       20,
		KafkaReplication: 1,
		ShutdownTimeout:  15 * time.Second,
		LogLevel:         slog.LevelInfo,
	}

	var errs []error

	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("SAGA_DATABASE_URL: required"))
	} else if u, err := url.Parse(cfg.DatabaseURL); err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Host == "" {
		// The URL holds a password, so it is never echoed back.
		errs = append(errs, errors.New("SAGA_DATABASE_URL: must be a postgres:// or postgresql:// URL with a host"))
	}

	if raw := getenv("SAGA_DB_MAX_CONNS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			errs = append(errs, errors.New("SAGA_DB_MAX_CONNS: must be a positive integer"))
		} else {
			cfg.DBMaxConns = n
		}
	}

	brokers, err := parseBrokers(getenv("SAGA_KAFKA_BROKERS"))
	if err != nil {
		errs = append(errs, err)
	}
	cfg.KafkaBrokers = brokers

	if raw := getenv("SAGA_KAFKA_REPLICATION"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 16)
		if err != nil || n < 1 || n > 7 {
			errs = append(errs, errors.New("SAGA_KAFKA_REPLICATION: must be an integer from 1 to 7"))
		} else {
			cfg.KafkaReplication = int16(n) //nolint:gosec // parsed with bitSize 16 and checked to be 1..7
		}
	}

	if raw := getenv("SAGA_SHUTDOWN_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			errs = append(errs, errors.New("SAGA_SHUTDOWN_TIMEOUT: must be a positive duration"))
		} else {
			cfg.ShutdownTimeout = d
		}
	}

	if raw := getenv("SAGA_SHUTDOWN_DRAIN"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			errs = append(errs, errors.New("SAGA_SHUTDOWN_DRAIN: must be a non-negative duration"))
		} else {
			cfg.ShutdownDrain = d
		}
	}

	if raw := getenv("SAGA_LOG_LEVEL"); raw != "" {
		level, err := parseLevel(raw)
		if err != nil {
			errs = append(errs, err)
		} else {
			cfg.LogLevel = level
		}
	}

	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// parseBrokers splits a comma-separated broker list and checks that each
// entry is a host:port with a numeric port, the only form the Kafka client
// accepts without guessing. At least one broker is required.
func parseBrokers(raw string) ([]string, error) {
	var brokers []string
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		host, port, err := net.SplitHostPort(item)
		if err != nil || host == "" {
			return nil, fmt.Errorf("SAGA_KAFKA_BROKERS: %q is not a host:port", item)
		}
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("SAGA_KAFKA_BROKERS: %q has no valid port", item)
		}
		brokers = append(brokers, item)
	}
	if len(brokers) == 0 {
		return nil, errors.New("SAGA_KAFKA_BROKERS: required, a comma-separated list of host:port")
	}
	return brokers, nil
}

// parseLevel accepts the four slog levels by name, and nothing else: slog's
// own parser would also take "INFO+2", which no deployment means to write.
func parseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("SAGA_LOG_LEVEL: unsupported level %s (want debug, info, warn or error)", strconv.Quote(raw))
}

func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

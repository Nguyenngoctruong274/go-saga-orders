package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

var minimal = map[string]string{
	"SAGA_DATABASE_URL":  "postgres://saga:saga@localhost:5432/orders",
	"SAGA_KAFKA_BROKERS": "localhost:19092",
}

func TestLoadDefaults(t *testing.T) {
	for _, tc := range []struct {
		svc  Service
		addr string
	}{
		{Orders, ":8080"},
		{Inventory, ":8081"},
		{Payment, ":8082"},
	} {
		t.Run(string(tc.svc), func(t *testing.T) {
			cfg, err := load(tc.svc, envFrom(minimal))
			if err != nil {
				t.Fatal(err)
			}
			want := Config{
				Service:          tc.svc,
				HTTPAddr:         tc.addr,
				DatabaseURL:      "postgres://saga:saga@localhost:5432/orders",
				DBMaxConns:       20,
				KafkaBrokers:     []string{"localhost:19092"},
				KafkaReplication: 1,
				ShutdownTimeout:  15 * time.Second,
				LogLevel:         slog.LevelInfo,
			}
			if !reflect.DeepEqual(cfg, want) {
				t.Fatalf("got %+v, want %+v", cfg, want)
			}
		})
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := load(Payment, envFrom(map[string]string{
		"SAGA_HTTP_ADDR":         "127.0.0.1:9090",
		"SAGA_DATABASE_URL":      "postgresql://u:p@db.internal:5433/payment?sslmode=require",
		"SAGA_DB_MAX_CONNS":      "50",
		"SAGA_KAFKA_BROKERS":     " redpanda-0:9092, redpanda-1:9092 ,,[::1]:9092",
		"SAGA_KAFKA_REPLICATION": "3",
		"SAGA_SHUTDOWN_TIMEOUT":  "30s",
		"SAGA_SHUTDOWN_DRAIN":    "5s",
		"SAGA_LOG_LEVEL":         "DEBUG",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Service:          Payment,
		HTTPAddr:         "127.0.0.1:9090",
		DatabaseURL:      "postgresql://u:p@db.internal:5433/payment?sslmode=require",
		DBMaxConns:       50,
		KafkaBrokers:     []string{"redpanda-0:9092", "redpanda-1:9092", "[::1]:9092"},
		KafkaReplication: 3,
		ShutdownTimeout:  30 * time.Second,
		ShutdownDrain:    5 * time.Second,
		LogLevel:         slog.LevelDebug,
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("got %+v, want %+v", cfg, want)
	}
}

func TestLoadUnknownService(t *testing.T) {
	if _, err := load(Service("billingd"), envFrom(minimal)); err == nil || !strings.Contains(err.Error(), "billingd") {
		t.Fatalf("got %v, want an unknown-service error", err)
	}
}

func TestLoadInvalid(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string // substring of the error
	}{
		{"missing database url", map[string]string{"SAGA_KAFKA_BROKERS": "k:9092"}, "SAGA_DATABASE_URL: required"},
		{"database url scheme", with("SAGA_DATABASE_URL", "mysql://u:p@h/db"), "postgres://"},
		{"database url without host", with("SAGA_DATABASE_URL", "postgres:///db"), "with a host"},
		{"db max conns zero", with("SAGA_DB_MAX_CONNS", "0"), "SAGA_DB_MAX_CONNS"},
		{"db max conns text", with("SAGA_DB_MAX_CONNS", "many"), "SAGA_DB_MAX_CONNS"},
		{"missing brokers", map[string]string{"SAGA_DATABASE_URL": minimal["SAGA_DATABASE_URL"]}, "SAGA_KAFKA_BROKERS: required"},
		{"brokers only commas", with("SAGA_KAFKA_BROKERS", " , ,"), "SAGA_KAFKA_BROKERS: required"},
		{"broker without port", with("SAGA_KAFKA_BROKERS", "redpanda"), `"redpanda" is not a host:port`},
		{"broker without host", with("SAGA_KAFKA_BROKERS", ":9092"), `":9092" is not a host:port`},
		{"broker port text", with("SAGA_KAFKA_BROKERS", "redpanda:kafka"), "has no valid port"},
		{"broker port range", with("SAGA_KAFKA_BROKERS", "redpanda:70000"), "has no valid port"},
		{"one bad broker spoils the list", with("SAGA_KAFKA_BROKERS", "a:9092,b"), `"b" is not a host:port`},
		{"replication zero", with("SAGA_KAFKA_REPLICATION", "0"), "SAGA_KAFKA_REPLICATION"},
		{"replication too high", with("SAGA_KAFKA_REPLICATION", "40000"), "SAGA_KAFKA_REPLICATION"},
		{"shutdown timeout zero", with("SAGA_SHUTDOWN_TIMEOUT", "0"), "SAGA_SHUTDOWN_TIMEOUT"},
		{"shutdown timeout text", with("SAGA_SHUTDOWN_TIMEOUT", "soon"), "SAGA_SHUTDOWN_TIMEOUT"},
		{"shutdown drain negative", with("SAGA_SHUTDOWN_DRAIN", "-1s"), "SAGA_SHUTDOWN_DRAIN"},
		{"log level", with("SAGA_LOG_LEVEL", "verbose"), `SAGA_LOG_LEVEL: unsupported level "verbose"`},
		{"log level slog syntax", with("SAGA_LOG_LEVEL", "INFO+2"), "SAGA_LOG_LEVEL"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(Orders, envFrom(tc.env))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not mention %q", err, tc.want)
			}
			if !strings.HasPrefix(err.Error(), "invalid configuration:") {
				t.Fatalf("error %q lacks the invalid configuration prefix", err)
			}
		})
	}
}

// TestLoadReportsEveryError pins the fail-fast contract: an operator fixing
// a deployment sees all the problems at once, not one per restart.
func TestLoadReportsEveryError(t *testing.T) {
	_, err := load(Orders, envFrom(map[string]string{
		"SAGA_DATABASE_URL":  "",
		"SAGA_KAFKA_BROKERS": "nope",
		"SAGA_DB_MAX_CONNS":  "-1",
		"SAGA_LOG_LEVEL":     "loud",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"SAGA_DATABASE_URL", "SAGA_KAFKA_BROKERS", "SAGA_DB_MAX_CONNS", "SAGA_LOG_LEVEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestLoadDoesNotLeakThePassword(t *testing.T) {
	_, err := load(Orders, envFrom(with("SAGA_DATABASE_URL", "mysql://user:hunter2@db/orders")))
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("error %q echoes the password", err)
	}
}

func with(k, v string) map[string]string {
	m := make(map[string]string, len(minimal)+1)
	for key, val := range minimal {
		m[key] = val
	}
	m[k] = v
	return m
}

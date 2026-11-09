package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-saga-orders/internal/httpapi"
)

// start runs a Server with the probes mounted and returns its base URL, a
// cancel for the run context and a channel with Run's result.
func start(t *testing.T, opts Options) (string, context.CancelFunc, <-chan error, *Server) {
	t.Helper()
	opts.Addr = "127.0.0.1:0"
	srv := New(slog.New(slog.NewTextHandler(io.Discard, nil)), opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, httpapi.NewHandler(nil, httpapi.Options{Ready: srv.Ready})) }()
	select {
	case <-srv.Listening():
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start listening")
	}
	t.Cleanup(cancel)
	return "http://" + srv.Addr(), cancel, done, srv
}

func status(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body.Status
}

func TestServesProbesAndStopsCleanly(t *testing.T) {
	base, cancel, done, srv := start(t, Options{})
	if srv.Addr() == "" || strings.HasSuffix(srv.Addr(), ":0") {
		t.Fatalf("bound address %q not resolved", srv.Addr())
	}
	if code, s := status(t, base+"/healthz"); code != http.StatusOK || s != "ok" {
		t.Fatalf("healthz: %d %q", code, s)
	}
	if code, s := status(t, base+"/readyz"); code != http.StatusOK || s != "ready" {
		t.Fatalf("readyz: %d %q", code, s)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil after a clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	if err := srv.Ready(context.Background()); !errors.Is(err, ErrDraining) {
		t.Fatalf("Ready after shutdown = %v, want ErrDraining", err)
	}
}

func TestReadinessFailsDuringDrain(t *testing.T) {
	base, cancel, done, _ := start(t, Options{ShutdownDrain: 300 * time.Millisecond})
	cancel()

	// During the drain the process still answers, and says it is not ready:
	// that is what lets a load balancer move traffic away before the
	// listener closes.
	deadline := time.Now().Add(250 * time.Millisecond)
	var sawUnavailable bool
	for time.Now().Before(deadline) {
		code, s := status(t, base+"/readyz")
		if code == http.StatusServiceUnavailable && s == "unavailable" {
			sawUnavailable = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawUnavailable {
		t.Fatal("readyz never reported unavailable during the drain")
	}
	if code, _ := status(t, base+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz during drain = %d, want 200 so the pod is not restarted mid-drain", code)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the drain")
	}
}

func TestReadyDefersToTheDependencyCheck(t *testing.T) {
	var dbDown bool
	var mu sync.Mutex
	check := func(context.Context) error {
		mu.Lock()
		defer mu.Unlock()
		if dbDown {
			return errors.New("postgres unreachable")
		}
		return nil
	}
	base, _, _, _ := start(t, Options{Check: check})

	if code, _ := status(t, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("readyz with healthy dependency = %d", code)
	}
	mu.Lock()
	dbDown = true
	mu.Unlock()
	if code, _ := status(t, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz with failing dependency = %d, want 503", code)
	}
	if code, _ := status(t, base+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz with failing dependency = %d, want 200", code)
	}
}

func TestRunReportsListenFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	srv := New(nil, Options{Addr: ln.Addr().String()})
	err = srv.Run(context.Background(), http.NotFoundHandler())
	if err == nil || !strings.Contains(err.Error(), "listen") {
		t.Fatalf("Run on a busy port returned %v, want a listen error", err)
	}
	if srv.Addr() != "" {
		t.Fatalf("Addr after a failed listen = %q, want empty", srv.Addr())
	}
}

func TestInFlightRequestsFinishDuringShutdown(t *testing.T) {
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		fmt.Fprint(w, "done")
	})
	srv := New(slog.New(slog.NewTextHandler(io.Discard, nil)), Options{Addr: "127.0.0.1:0", ShutdownTimeout: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, handler) }()
	<-srv.Listening()

	type result struct {
		body string
		err  error
	}
	results := make(chan result, 1)
	go func() {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+srv.Addr()+"/slow", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			results <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		results <- result{body: string(b), err: err}
	}()

	// Let the request reach the handler, then begin shutdown while it is
	// still in flight.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned %v before the in-flight request finished", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)

	select {
	case r := <-results:
		if r.err != nil || r.body != "done" {
			t.Fatalf("in-flight request got %q, %v; want done", r.body, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestNewLoggerTagsTheService(t *testing.T) {
	var sb strings.Builder
	logger := NewLogger(&sb, slog.LevelInfo, "paymentd")
	logger.Debug("hidden")
	logger.Info("hello", slog.Int("n", 1))

	lines := strings.Split(strings.TrimSpace(sb.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d lines, want 1 (debug filtered): %q", len(lines), sb.String())
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("not JSON: %v", err)
	}
	if rec["service"] != "paymentd" || rec["msg"] != "hello" || rec["n"] != float64(1) {
		t.Fatalf("record %v lacks the service tag or fields", rec)
	}
}

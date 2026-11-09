package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestHandler(t *testing.T, ready func(context.Context) error) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	return NewHandler(logger, Options{Ready: ready})
}

func get(t *testing.T, h http.Handler, path string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeStatus(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q, want application/json", ct)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Status
}

func TestHealthz(t *testing.T) {
	// Liveness must not consult readiness: a failing dependency is not a
	// reason to restart the process.
	h := newTestHandler(t, func(context.Context) error { return errors.New("db down") })
	rec := get(t, h, "/healthz", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if s := decodeStatus(t, rec); s != "ok" {
		t.Fatalf("status %q, want ok", s)
	}
}

func TestReadyz(t *testing.T) {
	tests := []struct {
		name  string
		ready func(context.Context) error
		code  int
		body  string
	}{
		{"nil check is ready", nil, http.StatusOK, "ready"},
		{"passing check", func(context.Context) error { return nil }, http.StatusOK, "ready"},
		{"failing check", func(context.Context) error { return errors.New("draining") }, http.StatusServiceUnavailable, "unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := get(t, newTestHandler(t, tc.ready), "/readyz", nil)
			if rec.Code != tc.code {
				t.Fatalf("status %d, want %d", rec.Code, tc.code)
			}
			if s := decodeStatus(t, rec); s != tc.body {
				t.Fatalf("body status %q, want %q", s, tc.body)
			}
		})
	}
}

func TestReadyzCheckGetsADeadline(t *testing.T) {
	h := newTestHandler(t, func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("readiness check ran without a deadline")
		}
		return nil
	})
	get(t, h, "/readyz", nil)
}

func TestProbesAreGetOnly(t *testing.T) {
	h := newTestHandler(t, nil)
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", rec.Code)
	}
}

func TestUnknownRoute(t *testing.T) {
	if rec := get(t, newTestHandler(t, nil), "/nope", nil); rec.Code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", rec.Code)
	}
}

func TestRequestID(t *testing.T) {
	t.Run("generated when absent", func(t *testing.T) {
		rec := get(t, newTestHandler(t, nil), "/healthz", nil)
		id := rec.Header().Get(RequestIDHeader)
		if len(id) != 24 {
			t.Fatalf("generated id %q, want 24 hex chars", id)
		}
	})
	t.Run("echoed when given", func(t *testing.T) {
		rec := get(t, newTestHandler(t, nil), "/healthz", map[string]string{RequestIDHeader: "client-abc"})
		if id := rec.Header().Get(RequestIDHeader); id != "client-abc" {
			t.Fatalf("id %q, want client-abc", id)
		}
	})
	t.Run("replaced when oversized", func(t *testing.T) {
		huge := strings.Repeat("x", 129)
		rec := get(t, newTestHandler(t, nil), "/healthz", map[string]string{RequestIDHeader: huge})
		if id := rec.Header().Get(RequestIDHeader); id == huge || id == "" {
			t.Fatalf("oversized id was kept: %q", id)
		}
	})
	t.Run("visible to handlers through the context", func(t *testing.T) {
		var seen string
		h := newTestHandler(t, func(ctx context.Context) error {
			seen = RequestID(ctx)
			return nil
		})
		get(t, h, "/readyz", map[string]string{RequestIDHeader: "trace-1"})
		if seen != "trace-1" {
			t.Fatalf("handler saw request id %q, want trace-1", seen)
		}
	})
	t.Run("empty outside a request", func(t *testing.T) {
		if id := RequestID(context.Background()); id != "" {
			t.Fatalf("got %q, want empty", id)
		}
	})
}

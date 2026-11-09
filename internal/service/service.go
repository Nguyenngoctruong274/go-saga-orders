// Package service runs the lifecycle every daemon in this repository
// shares: a JSON logger tagged with the service name, an HTTP server, and
// a graceful shutdown that fails readiness first, keeps serving while the
// load balancer notices, then drains in-flight requests.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"
)

// ErrDraining is what Ready returns once shutdown has begun.
var ErrDraining = errors.New("shutting down")

// NewLogger returns a JSON logger at level whose every record carries the
// service name, so the three daemons' logs can be told apart in one stream.
func NewLogger(w io.Writer, level slog.Level, service string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})).
		With(slog.String("service", service))
}

// Options configures a Server.
type Options struct {
	// Addr is the listen address; ":0" picks a free port (tests).
	Addr string
	// ShutdownDrain is how long to keep serving with readiness failing
	// before the listener closes.
	ShutdownDrain time.Duration
	// ShutdownTimeout bounds how long in-flight requests get to finish once
	// the listener is closed. Zero means 15 seconds.
	ShutdownTimeout time.Duration
	// Check reports whether dependencies are reachable; nil means they are.
	// Ready combines it with the draining state.
	Check func(context.Context) error
}

// Server owns one HTTP listener and the readiness state of the process.
type Server struct {
	logger *slog.Logger
	opts   Options

	draining  atomic.Bool
	addr      atomic.Pointer[string]
	listening chan struct{}
}

// New returns a Server that has not started listening yet.
func New(logger *slog.Logger, opts Options) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if opts.ShutdownTimeout <= 0 {
		opts.ShutdownTimeout = 15 * time.Second
	}
	return &Server{logger: logger, opts: opts, listening: make(chan struct{})}
}

// Ready is the readiness check to hand to the HTTP layer: it fails as soon
// as shutdown starts and otherwise defers to the dependency check.
func (s *Server) Ready(ctx context.Context) error {
	if s.draining.Load() {
		return ErrDraining
	}
	if s.opts.Check != nil {
		return s.opts.Check(ctx)
	}
	return nil
}

// Listening is closed once the listener is bound, after which Addr is set.
func (s *Server) Listening() <-chan struct{} { return s.listening }

// Addr returns the bound address, or "" before Run has listened.
func (s *Server) Addr() string {
	if p := s.addr.Load(); p != nil {
		return *p
	}
	return ""
}

// Run serves handler until ctx is done, then shuts down: readiness turns
// false, the drain period elapses, and the listener closes with up to
// ShutdownTimeout for in-flight requests. It returns nil after a clean
// shutdown and the cause when listening or serving fails.
func (s *Server) Run(ctx context.Context, handler http.Handler) error {
	ln, err := net.Listen("tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.opts.Addr, err)
	}
	addr := ln.Addr().String()
	s.addr.Store(&addr)
	close(s.listening)

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelWarn),
	}

	errc := make(chan error, 1)
	go func() {
		s.logger.Info("listening", slog.String("addr", addr))
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
		close(errc)
	}()

	select {
	case err := <-errc:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	s.logger.Info("shutdown started", slog.Duration("drain", s.opts.ShutdownDrain))
	s.draining.Store(true)
	if s.opts.ShutdownDrain > 0 {
		// Readiness is already failing; this is the time a load balancer
		// needs to notice and stop routing to us.
		time.Sleep(s.opts.ShutdownDrain)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.opts.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	s.logger.Info("http server stopped")
	return nil
}

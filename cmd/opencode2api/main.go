// Command opencode2api starts the API gateway and its management interface.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	adminui "opencode2api/internal/admin"
	"opencode2api/internal/buildinfo"
	"opencode2api/internal/config"
	"opencode2api/internal/gateway"
	"opencode2api/internal/telemetry"
)

// version remains the linker injection point used by release builds.
var version = "dev"

func main() {
	startedAt := time.Now()
	buildinfo.Version = version
	configPath := flag.String("config", "config.json", "path to config.json")
	listen := flag.String("listen", "", "override the configured API listen address")
	webListen := flag.String("web-listen", "", "override the configured WebUI listen address")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("configuration error", "error", err)
		os.Exit(1)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *webListen != "" {
		cfg.WebUI.Listen = *webListen
	}

	// Signals cancel signalCtx, but the runtime keeps its own root: the
	// egress multiplexer must stay up while HTTP requests drain. Cancel the
	// runtime only after server.Shutdown returns. Canceling this signal
	// context directly used to stop sing-box first, leaving draining
	// anonymous requests to fall back to the abusive-blocked direct lanes.
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	runtimeCtx, stopRuntime := context.WithCancel(context.Background())
	defer stopRuntime()
	level := new(slog.LevelVar)
	telemetry.SetLogLevel(level, cfg.Logging.Level)
	hub := telemetry.NewLogHub(cfg.Logging.RingSize)
	redactor := config.NewSecretRedactor()
	redactor.Replace(cfg)
	logger := telemetry.NewStructuredLogger(level, hub, redactor)
	monitor := telemetry.NewMonitor()
	manager, err := gateway.NewRuntimeManager(runtimeCtx, *configPath, cfg, logger, monitor, hub, redactor, level)
	if err != nil {
		logger.Error("failed to initialize runtime", "component", "runtime", "event", "runtime_initialization_failed", "error", err)
		os.Exit(1)
	}
	defer manager.Shutdown()

	// A stop keeps the listener open and answers new requests with a retryable
	// 503 while in-flight streams finish. The previous behavior called
	// Shutdown immediately, which closes the listener, so clients hit a
	// refused connection for the whole drain budget; most clients do not retry
	// a reset and surfaced it as a hard "model unavailable" failure.
	var draining atomic.Bool
	var inflight atomic.Int64
	drain := &drainState{draining: &draining, inflight: &inflight}

	apiServer := &http.Server{
		Addr: cfg.Listen, Handler: drain.wrap(manager.Handler()), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second,
	}
	servers := []*http.Server{apiServer}
	go serveHTTP(stopSignals, logger, apiServer, "api", startedAt)

	if cfg.WebUI.Enabled {
		admin := adminui.New(manager, monitor, hub, logger)
		webServer := &http.Server{
			Addr: cfg.WebUI.Listen, Handler: drain.wrap(admin.Handler()), ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second,
		}
		servers = append(servers, webServer)
		go serveHTTP(stopSignals, logger, webServer, "webui", startedAt)
	}

	<-signalCtx.Done()
	draining.Store(true)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer drainCancel()
	// Wait out the in-flight streams while the listener still accepts the
	// departing clients' retries. The wait is bounded: a stream that outlives
	// the budget is cut when Shutdown runs.
	for inflight.Load() > 0 && drainCtx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	for _, server := range servers {
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed", "component", "server", "event", "shutdown_failed", "address", server.Addr, "error", err)
		}
	}
	// HTTP is drained (or its shutdown budget expired): only now stop the
	// egress multiplexer and background loops, so draining requests keep
	// their proxy lanes until the last one finishes.
	stopRuntime()
}

func serveHTTP(cancel context.CancelFunc, logger *slog.Logger, server *http.Server, component string, startedAt time.Time) {
	logger.Info("server listening", "component", component, "event", "server_started", "address", server.Addr, "version", buildinfo.Version, "startup_ms", time.Since(startedAt).Milliseconds())
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server stopped unexpectedly", "component", component, "event", "server_failed", "address", server.Addr, "error", err)
		cancel()
	}
}

// drainState fronts the request handlers so a stopping gateway keeps accepting
// connections, answering new requests with a retryable 503 while the stop
// waits out work already in flight. Refusing the connection instead made a
// deploy look like a hard failure to clients that do not retry a reset.
type drainState struct {
	draining *atomic.Bool
	inflight *atomic.Int64
}

func (d *drainState) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if d.draining.Load() {
			w.Header().Set("Retry-After", "5")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"type":"service_unavailable","message":"server is restarting; retry shortly"}}`))
			return
		}
		d.inflight.Add(1)
		defer d.inflight.Add(-1)
		next.ServeHTTP(w, r)
	})
}

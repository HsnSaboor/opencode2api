package proxypool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// ProxyPassword is the shared password every generated SOCKS5 user
// authenticates with; the username ("p<index>") selects the node.
const ProxyPassword = "p"

// MultiplexerConfig configures the sing-box subprocess that fronts all proxy
// nodes on one local SOCKS5 port.
type MultiplexerConfig struct {
	// Binary is the sing-box executable path (default "sing-box").
	Binary string
	// SocksPort is the loopback SOCKS5 port every node is multiplexed on.
	SocksPort int
	// LogLevel is sing-box's own log level (default "warn").
	LogLevel string
	// StateDir holds the generated config and log file. Empty means a temp
	// directory under the OS temp dir.
	StateDir string
}

// Multiplexer runs one sing-box process that exposes every profile node as a
// SOCKS5 user on a single loopback port. It mirrors the zen-proxy design:
// per-node auth_user routing rules, a direct fallback outbound, and a
// supervised process (restart on crash) so a dead multiplexer degrades the
// proxy lanes without killing the gateway.
type Multiplexer struct {
	cfg   MultiplexerConfig
	nodes []Node
	log   *slog.Logger

	mu      sync.Mutex
	cmd     *exec.Cmd
	stopped bool
	done    chan struct{}
	// readyAt is the last successful readiness probe. It gives Ready a short
	// trust window so one missed loopback dial under load cannot report the
	// pool as down while it is actively serving.
	readyAt time.Time
}

const (
	// readyProbeTimeout bounds the loopback dial Ready uses to prove the SOCKS
	// port accepts connections. The old 300ms budget was too tight under load.
	readyProbeTimeout = 1500 * time.Millisecond
	// readyTrustWindow keeps the multiplexer ready through a transient probe
	// failure: a listener that accepted a connection this recently is not dead.
	readyTrustWindow = 10 * time.Second
)

// NewMultiplexer prepares (but does not start) the multiplexer.
func NewMultiplexer(cfg MultiplexerConfig, nodes []Node, log *slog.Logger) *Multiplexer {
	if cfg.Binary == "" {
		cfg.Binary = "sing-box"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "warn"
	}
	return &Multiplexer{cfg: cfg, nodes: nodes, log: log}
}

// SocksAddr is the loopback address nodes are reachable on.
func (m *Multiplexer) SocksAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", m.cfg.SocksPort)
}

// BuildConfig renders the sing-box configuration that maps each node's
// SOCKS5 user to its outbound. Exposed for tests.
func (m *Multiplexer) BuildConfig(logPath string) ([]byte, error) {
	users := make([]map[string]any, 0, len(m.nodes))
	rules := make([]map[string]any, 0, len(m.nodes))
	outbounds := make([]map[string]any, 0, len(m.nodes)+1)
	for _, node := range m.nodes {
		tag := fmt.Sprintf("n%d", node.Index)
		users = append(users, map[string]any{"username": node.User, "password": ProxyPassword})
		rules = append(rules, map[string]any{"auth_user": []any{node.User}, "outbound": tag})
		outbound := make(map[string]any, len(node.Raw)+1)
		for key, value := range node.Raw {
			outbound[key] = value
		}
		// sing-box 1.12+ removed domain_strategy from outbounds; the
		// upstream profile stores it for older builds.
		delete(outbound, "domain_strategy")
		outbound["tag"] = tag
		outbounds = append(outbounds, outbound)
	}
	outbounds = append(outbounds, map[string]any{"type": "direct", "tag": "direct"})
	config := map[string]any{
		"log": map[string]any{"level": m.cfg.LogLevel, "output": logPath, "timestamp": true},
		"dns": map[string]any{
			"servers": []any{
				map[string]any{"tag": "dns-remote", "type": "udp", "server": "1.1.1.1"},
				map[string]any{"tag": "dns-local", "type": "local"},
			},
			"strategy": "prefer_ipv4",
		},
		"inbounds": []any{map[string]any{
			"type":        "socks",
			"tag":         "socks-in",
			"listen":      "127.0.0.1",
			"listen_port": m.cfg.SocksPort,
			"users":       users,
			"sniff":       false,
		}},
		"outbounds": outbounds,
		"route": map[string]any{
			"rules":                   rules,
			"auto_detect_interface":   true,
			"default_domain_resolver": "dns-remote",
			"final":                   "direct",
		},
	}
	return json.Marshal(config)
}

func (m *Multiplexer) configPath() string {
	return filepath.Join(m.stateDir(), "singbox.json")
}

func (m *Multiplexer) logPath() string {
	return filepath.Join(m.stateDir(), "singbox.log")
}

func (m *Multiplexer) stateDir() string {
	if m.cfg.StateDir != "" {
		return m.cfg.StateDir
	}
	return filepath.Join(os.TempDir(), "o2a-proxypool")
}

// Start writes the config, validates it with `sing-box check`, launches the
// process, and waits for the SOCKS port to accept connections. A validation
// or port failure stops the process and returns an error the caller can log
// while continuing with address-only egress.
func (m *Multiplexer) Start(ctx context.Context) error {
	if err := m.prepare(ctx); err != nil {
		return err
	}
	return m.spawn(ctx)
}

// prepare materializes the state directory, generated config, and validation
// result. It runs on every spawn so the supervisor can start the multiplexer
// without a prior Start call (the gateway relies on that path).
func (m *Multiplexer) prepare(ctx context.Context) error {
	if len(m.nodes) == 0 {
		return fmt.Errorf("proxypool: no nodes to multiplex")
	}
	if err := os.MkdirAll(m.stateDir(), 0o755); err != nil {
		return fmt.Errorf("proxypool: state dir: %w", err)
	}
	config, err := m.BuildConfig(m.logPath())
	if err != nil {
		return fmt.Errorf("proxypool: build config: %w", err)
	}
	if err := os.WriteFile(m.configPath(), config, 0o600); err != nil {
		return fmt.Errorf("proxypool: write config: %w", err)
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	check := exec.CommandContext(checkCtx, m.cfg.Binary, "check", "-c", m.configPath())
	if output, err := check.CombinedOutput(); err != nil {
		return fmt.Errorf("proxypool: sing-box check failed: %v: %s", err, truncate(string(output), 400))
	}
	return nil
}

func (m *Multiplexer) spawn(ctx context.Context) error {
	started := time.Now()
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return fmt.Errorf("proxypool: multiplexer stopped")
	}
	m.mu.Unlock()
	// Every spawn is self-preparing: the first supervised start and every
	// respawn after a crash or failed attempt need the state dir, the
	// generated config, and the validation result on disk.
	if err := m.prepare(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	if m.stopped {
		// Stop landed while prepare ran; without this second check the
		// process would launch unstoppable (Stop already returned).
		m.mu.Unlock()
		return fmt.Errorf("proxypool: multiplexer stopped")
	}
	logFile, err := os.OpenFile(m.logPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		m.mu.Unlock()
		return fmt.Errorf("proxypool: open log: %w", err)
	}
	cmd := exec.Command(m.cfg.Binary, "run", "-c", m.configPath())
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		m.mu.Unlock()
		return fmt.Errorf("proxypool: start sing-box: %w", err)
	}
	m.cmd = cmd
	m.done = make(chan struct{})
	done := m.done
	m.mu.Unlock()

	go func() {
		waitErr := cmd.Wait()
		_ = logFile.Close()
		m.mu.Lock()
		if m.done == done {
			close(m.done)
			if m.log != nil {
				if waitErr != nil {
					m.log.Warn("proxy multiplexer exited", "component", "egress", "event", "proxy_multiplexer_exited", "error", waitErr)
				} else {
					m.log.Warn("proxy multiplexer exited", "component", "egress", "event", "proxy_multiplexer_exited")
				}
			}
		}
		m.mu.Unlock()
	}()

	if err := m.waitPort(ctx); err != nil {
		m.terminate(cmd, done)
		return err
	}
	m.mu.Lock()
	m.readyAt = time.Now()
	m.mu.Unlock()
	if m.log != nil {
		m.log.Info("proxy multiplexer ready", "component", "egress", "event", "proxy_multiplexer_ready", "socks_port", m.cfg.SocksPort, "duration_ms", time.Since(started).Milliseconds())
	}
	return nil
}

// terminate ends one spawned process attempt and waits for its reaper
// goroutine. Unlike Stop it does not mark the multiplexer stopped, so the
// supervisor can retry a start that came up but never bound its port.
func (m *Multiplexer) terminate(cmd *exec.Cmd, done chan struct{}) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
	}
}

func (m *Multiplexer) waitPort(ctx context.Context) error {
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		conn, err := net.DialTimeout("tcp", m.SocksAddr(), 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		m.mu.Lock()
		done := m.done
		exited := false
		if done != nil {
			select {
			case <-done:
				exited = true
			default:
			}
		}
		m.mu.Unlock()
		if exited {
			return fmt.Errorf("proxypool: sing-box exited before binding %s (see %s)", m.SocksAddr(), m.logPath())
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("proxypool: sing-box did not bind %s in time (see %s)", m.SocksAddr(), m.logPath())
}

// Supervise keeps the multiplexer alive while ctx is live: it starts the
// process if a previous attempt failed, waits out the process's lifetime,
// and respawns it after a bounded exponential backoff. It blocks until ctx
// is done, then leaves the process stopped.
func (m *Multiplexer) Supervise(ctx context.Context) {
	backoff := time.Second
	initial := true
	for {
		select {
		case <-ctx.Done():
			m.Stop()
			return
		default:
		}
		m.mu.Lock()
		done := m.done
		m.mu.Unlock()
		if done == nil {
			// No process is running (initial start failed or the spawn is
			// pending). The first attempt is immediate: the old one-second
			// sleep was pure restart latency. Later attempts keep the
			// exponential backoff so a broken binary cannot spin.
			if !initial {
				select {
				case <-ctx.Done():
					m.Stop()
					return
				case <-time.After(backoff):
				}
			}
			initial = false
			if m.log != nil {
				m.log.Info("proxy multiplexer starting", "component", "egress", "event", "proxy_multiplexer_start")
			}
			if err := m.spawn(ctx); err != nil {
				if m.log != nil {
					m.log.Warn("proxy multiplexer start failed", "component", "egress", "event", "proxy_multiplexer_start_failed", "error", err)
				}
				backoff = min(backoff*2, 30*time.Second)
				continue
			}
			backoff = time.Second
			continue
		}
		select {
		case <-done:
		case <-ctx.Done():
			m.Stop()
			return
		}
		select {
		case <-ctx.Done():
			m.Stop()
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
		if m.log != nil {
			m.log.Info("proxy multiplexer restarting", "component", "egress", "event", "proxy_multiplexer_restart")
		}
		if err := m.spawn(ctx); err != nil {
			if m.log != nil {
				m.log.Warn("proxy multiplexer restart failed", "component", "egress", "event", "proxy_multiplexer_restart_failed", "error", err)
			}
			continue
		}
		backoff = time.Second
	}
}

// Ready reports whether a live process was spawned and the loopback SOCKS
// port is accepting connections. Probes consult it so a down multiplexer
// does not mark every candidate node dead.
func (m *Multiplexer) Ready() bool {
	m.mu.Lock()
	done := m.done
	lastReady := m.readyAt
	m.mu.Unlock()
	if done == nil {
		return false
	}
	select {
	case <-done:
		return false
	default:
	}
	conn, err := net.DialTimeout("tcp", m.SocksAddr(), readyProbeTimeout)
	if err != nil {
		// A single slow dial is not proof the listener is gone; only age the
		// multiplexer out once no probe has succeeded for the trust window.
		if !lastReady.IsZero() && time.Since(lastReady) < readyTrustWindow {
			return true
		}
		return false
	}
	_ = conn.Close()
	m.mu.Lock()
	m.readyAt = time.Now()
	m.mu.Unlock()
	return true
}

// Stop permanently terminates the multiplexer and its process. Safe to call
// multiple times.
func (m *Multiplexer) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	cmd := m.cmd
	done := m.done
	m.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	// The spawn reaper owns cmd.Wait(); a second Wait here would race it.
	// Signal, then bound the wait through the reaper's channel.
	_ = cmd.Process.Signal(os.Interrupt)
	if done == nil {
		_ = cmd.Process.Kill()
		return
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
	}
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit] + "..."
}

// Package rotation owns sticky, service-wide model selection. No network call
// runs under its lock. A generation prevents late requests undoing a switch.
package rotation

import (
	"encoding/json"
	"fmt"
	"opencode2api/internal/config"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type Attempt struct {
	Time    time.Time `json:"time"`
	Request string    `json:"request"`
	Alias   string    `json:"alias"`
	Model   string    `json:"model"`
	Number  int       `json:"number"`
	Outcome string    `json:"outcome"`
}
type Switch struct {
	Time   time.Time `json:"time"`
	From   string    `json:"from"`
	To     string    `json:"to"`
	Reason string    `json:"reason"`
}
type Group struct {
	ID         string               `json:"id"`
	Alias      string               `json:"alias"`
	Order      []string             `json:"order"`
	Current    string               `json:"current"`
	Generation uint64               `json:"generation"`
	Paused     map[string]time.Time `json:"paused"`
	Available  map[string]bool      `json:"available"`
	Switches   []Switch             `json:"switches"`
}
type Snapshot struct {
	Groups             []*Group  `json:"groups"`
	Attempts           []Attempt `json:"attempts"`
	PersistenceError   string    `json:"persistence_error,omitempty"`
	FirstOutputSeconds int       `json:"first_output_seconds"`
	// SOTASize is how many top-ranked catalog models the SOTA group holds. It
	// is reported so the management UI can move the line between the groups.
	SOTASize int `json:"sota_size"`
}
type Token struct {
	Group, Model string
	Generation   uint64
}
type Manager struct {
	mu    sync.Mutex
	path  string
	state Snapshot
	cfg   config.RotationConfig
}

func New(path string, cfg config.RotationConfig) (*Manager, error) {
	m := &Manager{path: path}
	if path != "" {
		b, e := os.ReadFile(path)
		if e == nil {
			if e = json.Unmarshal(b, &m.state); e != nil {
				return nil, fmt.Errorf("invalid rotation state: %w", e)
			}
		} else if !os.IsNotExist(e) {
			return nil, e
		}
	}
	if len(m.state.Groups) != 0 && (len(m.state.Groups) != 2 || m.state.Groups[0] == nil || m.state.Groups[1] == nil || m.state.Groups[0].ID != "sota" || m.state.Groups[1].ID != "sweet") {
		return nil, fmt.Errorf("invalid rotation groups")
	}
	m.Configure(cfg)
	if m.state.PersistenceError != "" {
		return nil, fmt.Errorf("cannot persist rotation state")
	}
	return m, nil
}
func (m *Manager) Configure(cfg config.RotationConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg.Defaults()
	for i, c := range []config.RotationGroup{cfg.SOTA, cfg.Sweet} {
		id := []string{"sota", "sweet"}[i]
		if len(m.state.Groups) <= i {
			m.state.Groups = append(m.state.Groups, &Group{ID: id})
		}
		g := m.state.Groups[i]
		previous := m.cfg.SOTA
		if i == 1 {
			previous = m.cfg.Sweet
		}
		order := slices.Clone(g.Order)
		if len(order) == 0 || !slices.Equal(previous.Order, c.Order) {
			order = slices.Clone(c.Order)
			for _, v := range g.Order {
				if !slices.Contains(order, v) {
					order = append(order, v)
				}
			}
		}
		if !slices.Equal(order, g.Order) || g.Alias != c.Alias {
			g.Generation++
		}
		g.Order = order
		g.Alias = c.Alias
		if g.Paused == nil {
			g.Paused = map[string]time.Time{}
		}
		if !slices.Contains(g.Order, g.Current) && len(g.Order) > 0 {
			g.Current = g.Order[0]
			g.Generation++
		}
	}
	m.cfg = cfg
	m.state.FirstOutputSeconds = cfg.FirstOutputSeconds
	m.state.SOTASize = cfg.SOTASize
	m.saveLocked()
}

// Membership is the ranked model list for each rotation group: position 0 is
// the highest-capability model. The rotation manager appends newly discovered
// models in this order and preserves whatever order the operator has saved.
// Which models belong to which group is decided by the caller from the
// upstream catalog, so this package holds no model-name knowledge.
type Membership struct {
	SOTA  []string
	Sweet []string
}

func (m *Manager) Sync(membership Membership) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ranked := [2][]string{membership.SOTA, membership.Sweet}
	sets := [2]map[string]bool{toSet(membership.SOTA), toSet(membership.Sweet)}
	changed := false
	for i, g := range m.state.Groups {
		g.Available = map[string]bool{}
		for _, model := range ranked[i] {
			g.Available[model] = true
		}
		added := make([]string, 0, len(ranked[i]))
		for _, model := range ranked[i] {
			if !slices.Contains(g.Order, model) {
				added = append(added, model)
			}
		}
		if len(added) > 0 {
			// Rank order, not alphabetical: a fresh installation starts with
			// the strongest model first.
			g.Order = append(g.Order, added...)
			changed = true
		}
		// A model the catalog now ranks into the other group must leave this
		// one, or the same ID would be served by both aliases. Models that are
		// merely unavailable stay listed: they remain part of this group and
		// return to the order when the catalog carries them again.
		other := sets[(i+1)%2]
		kept := make([]string, 0, len(g.Order))
		for _, model := range g.Order {
			if other[model] {
				changed = true
				continue
			}
			kept = append(kept, model)
		}
		g.Order = kept
		if g.Current == "" && len(g.Order) > 0 {
			g.Current = g.Order[0]
			g.Generation++
			changed = true
		}
	}
	if changed {
		m.saveLocked()
	}
}

func toSet(models []string) map[string]bool {
	result := make(map[string]bool, len(models))
	for _, model := range models {
		result[model] = true
	}
	return result
}
func (m *Manager) GroupID(alias string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, g := range m.state.Groups {
		if g.Alias == alias {
			return g.ID
		}
	}
	return ""
}
func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _ := json.Marshal(m.state)
	var s Snapshot
	_ = json.Unmarshal(b, &s)
	return s
}
func (m *Manager) group(id string) *Group {
	for _, g := range m.state.Groups {
		if g.ID == id {
			return g
		}
	}
	return nil
}
func usable(g *Group, model string) bool {
	return g.Available[model] && !time.Now().Before(g.Paused[model])
}
func (m *Manager) Select(id string, visited map[string]bool) (Token, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.group(id)
	if g == nil || len(g.Order) == 0 {
		return Token{}, false
	}
	start := max(slices.Index(g.Order, g.Current), 0)
	for n := 0; n < len(g.Order); n++ {
		model := g.Order[(start+n)%len(g.Order)]
		if visited[model] || !usable(g, model) {
			continue
		}
		// A request-local skip must not change a newer, healthy global selection.
		if !usable(g, g.Current) {
			m.switchLocked(g, model, "当前模型暂停或已下架")
			m.saveLocked()
		}
		return Token{g.ID, model, g.Generation}, true
	}
	return Token{}, false
}
func (m *Manager) switchLocked(g *Group, to, reason string) {
	from := g.Current
	g.Current = to
	g.Generation++
	g.Switches = append(g.Switches, Switch{time.Now().UTC(), from, to, reason})
	if len(g.Switches) > 30 {
		g.Switches = g.Switches[len(g.Switches)-30:]
	}
}
func (m *Manager) Fail(t Token, pause time.Duration, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.group(t.Group)
	if g == nil {
		return
	}
	until := time.Now().Add(pause).UTC()
	if until.After(g.Paused[t.Model]) {
		g.Paused[t.Model] = until
	}
	if g.Current == t.Model && g.Generation == t.Generation {
		start := slices.Index(g.Order, t.Model)
		next := t.Model
		for n := 1; n < len(g.Order); n++ {
			v := g.Order[(start+n)%len(g.Order)]
			if usable(g, v) {
				next = v
				break
			}
		}
		m.switchLocked(g, next, reason)
	}
	m.saveLocked()
}
func (m *Manager) Manual(id, model string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.group(id)
	if g == nil || !slices.Contains(g.Order, model) || !g.Available[model] {
		return fmt.Errorf("请选择该组当前可用的 Go 模型")
	}
	delete(g.Paused, model)
	m.switchLocked(g, model, "管理员手动切换")
	m.saveLocked()
	if m.state.PersistenceError != "" {
		return fmt.Errorf("轮转状态保存失败")
	}
	return nil
}
func (m *Manager) Record(a Attempt) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a.Time = time.Now().UTC()
	m.state.Attempts = append(m.state.Attempts, a)
	if len(m.state.Attempts) > 100 {
		m.state.Attempts = m.state.Attempts[len(m.state.Attempts)-100:]
	}
	m.saveLocked()
}
func (m *Manager) saveLocked() {
	if m.path == "" {
		return
	}
	m.state.PersistenceError = ""
	if err := m.writeLocked(); err != nil {
		m.state.PersistenceError = "轮转状态写入失败，请检查磁盘空间和文件权限"
	}
}
func (m *Manager) writeLocked() error {
	b, e := json.MarshalIndent(m.state, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(m.path), ".rotation-*.tmp")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), m.path)
}

package gateway

import (
	"net/http"
	"time"

	"opencode2api/internal/buildinfo"
	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	modelcatalog "opencode2api/internal/models"
	wire "opencode2api/internal/protocol"
)

type healthResponse struct {
	Status  string       `json:"status"`
	Ready   bool         `json:"ready"`
	Version string       `json:"version"`
	Models  healthModels `json:"models"`
	Keys    healthKeys   `json:"keys"`
	Egress  healthEgress `json:"egress"`
	Issues  []string     `json:"issues,omitempty"`
}

type healthModels struct {
	Status            string     `json:"status"`
	Total             int        `json:"total"`
	Exposed           int        `json:"exposed"`
	Zen               int        `json:"zen"`
	Go                int        `json:"go"`
	LastRefresh       *time.Time `json:"last_refresh,omitempty"`
	StaleAfterSeconds int        `json:"stale_after_seconds"`
	CacheSource       string     `json:"cache_source,omitempty"`
	Stale             bool       `json:"stale"`
}

type healthKeys struct {
	Zen       int  `json:"zen"`
	Go        int  `json:"go"`
	Total     int  `json:"total"`
	Anonymous bool `json:"anonymous"`
}

type healthEgress struct {
	Total     int `json:"total"`
	Healthy   int `json:"healthy"`
	Unhealthy int `json:"unhealthy"`
	// Parked counts lanes sitting out a FreeTier flag; Cooling counts
	// anonymous lanes in failure backoff. Both are selectable again on
	// expiry, unlike Unhealthy (transport-down). Healthy-transport with
	// zero selectable is the signature of a flag storm, not an outage.
	Parked  int `json:"parked,omitempty"`
	Cooling int `json:"cooling,omitempty"`
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	_, models := g.availableModels()
	egressTotal, egressHealthy, egressParked, egressCooling, egressSelectable := g.egressStatus()
	zenKeys, goKeys := g.zenNodes.Len(), g.goNodes.Len()
	staleAfter := max(2*time.Duration(g.cfg.Models.RefreshSeconds)*time.Second, time.Minute)

	modelStatus := "ready"
	var lastRefresh *time.Time
	issues := make([]string, 0, 3)
	if models.UpdatedAt.IsZero() {
		modelStatus = "pending"
		issues = append(issues, "model_catalog_pending")
	} else {
		updatedAt := models.UpdatedAt.UTC()
		lastRefresh = &updatedAt
		if models.Exposed == 0 {
			modelStatus = "empty"
			issues = append(issues, "model_catalog_empty")
		} else if models.Stale || time.Since(models.UpdatedAt) > staleAfter {
			modelStatus = "stale"
			issues = append(issues, "model_catalog_stale")
		}
	}
	if zenKeys+goKeys == 0 && !g.cfg.Anonymous {
		issues = append(issues, "no_upstream_keys")
	}
	if egressHealthy == 0 {
		issues = append(issues, "no_healthy_egress")
	}
	// A flag storm parks every lane while transports stay healthy: the pool
	// reports 100/100 and readiness stays green, but selection finds
	// nothing. Surface that state explicitly instead of the misleading
	// all-healthy line.
	if egressHealthy > 0 && egressSelectable == 0 && g.cfg.Anonymous {
		issues = append(issues, "no_selectable_egress")
	}

	status := "ok"
	if len(issues) > 0 {
		status = "degraded"
	}
	blocking := modelStatus == "pending" || modelStatus == "empty" || egressHealthy == 0
	httpStatus := http.StatusOK
	ready := !blocking
	// Readiness stays transport-based on purpose: a flag storm is
	// transient (parks expire, replenishment regrows) and must not flap
	// orchestrators into restarts. Operators watch no_selectable_egress
	// and the parked/cooling counts instead.
	if blocking {
		httpStatus = http.StatusServiceUnavailable
		if modelStatus == "pending" {
			status = "starting"
		}
	}
	httpx.WriteJSON(w, httpStatus, healthResponse{
		Status:  status,
		Ready:   ready,
		Version: buildinfo.Version,
		Models: healthModels{
			Status:            modelStatus,
			Total:             models.Total,
			Exposed:           models.Exposed,
			Zen:               models.Zen,
			Go:                models.Go,
			LastRefresh:       lastRefresh,
			StaleAfterSeconds: int(staleAfter / time.Second),
			CacheSource:       models.CacheSource,
			Stale:             models.Stale,
		},
		Keys: healthKeys{Zen: zenKeys, Go: goKeys, Total: zenKeys + goKeys, Anonymous: g.cfg.Anonymous},
		Egress: healthEgress{
			Total:     egressTotal,
			Healthy:   egressHealthy,
			Unhealthy: egressTotal - egressHealthy,
			Parked:    egressParked,
			Cooling:   egressCooling,
		},
		Issues: issues,
	})
}

// egressStatus snapshots the pool the way selection sees it: transport
// totals plus park/cooling pressure plus the anonymous selectable count.
// Selectable mirrors anonymousCursor.Next (healthy lane, no node cooldown,
// no freitier park) without consuming a cursor.
func (g *Gateway) egressStatus() (total, healthy, parked, cooling, selectable int) {
	nodes := g.anonymous.snapshotNodes()
	total, healthy, parked, cooling = g.transports.selectableCounts(nodes)
	if g.anonymous == nil {
		return total, healthy, parked, cooling, 0
	}
	now := time.Now().UnixNano()
	for _, node := range nodes {
		if node == nil || node.egress == nil {
			continue
		}
		if node.egress.healthy.Load() && node.cooldownUntil.Load() <= now && node.egress.freitierUntil.Load() <= now {
			selectable++
		}
	}
	return total, healthy, parked, cooling, selectable
}

func (g *Gateway) availableModels() ([]modelcatalog.Route, modelcatalog.CatalogSnapshot) {
	return g.catalog.AvailableModels(g.zenNodes.Len() > 0, g.goNodes.Len() > 0, g.cfg.Anonymous)
}

func (g *Gateway) handleModels(w http.ResponseWriter, _ *http.Request) {
	now := time.Now().Unix()
	routes, _ := g.availableModels()
	data := make([]map[string]any, 0, len(routes))
	for _, route := range routes {
		model := route.ID
		// Tier-scoped metadata: the advertised context window must match
		// the tier that will serve the request (anonymous ⇒ Zen).
		md := g.catalog.MetadataForTier(model, route.Tier)
		entry := map[string]any{
			"id": model, "object": "model", "created": now, "owned_by": "opencode",
			"metadata":        md,
			"native_protocol": route.Protocol,
		}
		if route.Protocol == wire.SystemOne {
			entry["supported_endpoints"] = []string{"/v1/systemone"}
		} else {
			entry["supported_endpoints"] = []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"}
		}
		// Top-level OpenAI-standard fields: discovery clients (jcode, Pi, …)
		// read context/reasoning at the top level of each model entry.
		if md.ContextWindow > 0 {
			entry["context_window"] = md.ContextWindow
			entry["context_length"] = md.ContextWindow
		}
		if md.MaxInput > 0 {
			entry["max_input"] = md.MaxInput
		}
		if md.MaxOutput > 0 {
			entry["max_output"] = md.MaxOutput
		}
		if md.Reasoning {
			entry["reasoning"] = true
			entry["supports_reasoning"] = true
		}
		if md.ToolCall {
			entry["tool_call"] = true
		}
		if md.StructuredOutput {
			entry["structured_output"] = true
		}
		data = append(data, entry)
	}
	for _, group := range g.rotation.Snapshot().Groups {
		if g.goNodes.Len() == 0 {
			continue
		}
		available := false
		for _, model := range group.Order {
			if route, err := g.catalog.RouteForTier(model, config.TierGo, false, true); err == nil && route.Protocol != wire.SystemOne {
				available = true
				break
			}
		}
		if !available {
			continue
		}
		data = append(data, map[string]any{"id": group.Alias, "object": "model", "created": now, "owned_by": "opencode-go-rotation"})
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

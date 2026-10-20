package admin

import (
	"fmt"
	"net/http"
	"opencode2api/internal/config"
	"opencode2api/internal/httpx"
	"slices"
)

func (a *Server) handleRotation(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	httpx.WriteJSON(w, 200, a.manager.RotationSnapshot())
}

// handleSaveRotation persists operator edits to the rotation groups. The
// submitted orders are reorderings of the live orders, and the SOTA size may
// move the line between the groups; membership itself always comes from the
// catalog, so an order that no longer matches live state is rejected with a
// refresh hint rather than silently applied.
func (a *Server) handleSaveRotation(w http.ResponseWriter, r *http.Request) {
	var input config.RotationConfig
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, 400, "invalid_request", err.Error())
		return
	}
	// A client that predates sota_size sends no value; keep the configured
	// one instead of resetting it to the default.
	requestedSize := input.SOTASize
	input.Defaults()
	_, err := a.manager.Update(func(cfg *config.Config) error {
		state := a.manager.RotationSnapshot()
		for i, g := range state.Groups {
			order := input.SOTA.Order
			if i == 1 {
				order = input.Sweet.Order
			}
			if len(order) != len(g.Order) {
				return fmt.Errorf("模型列表已更新，请刷新后重新排序")
			}
			for _, v := range order {
				if !slices.Contains(g.Order, v) {
					return fmt.Errorf("只能调整组内模型的顺序")
				}
			}
		}
		if requestedSize == 0 {
			requestedSize = cfg.Rotation.SOTASize
		}
		cfg.Rotation.SOTA.Alias = input.SOTA.Alias
		cfg.Rotation.Sweet.Alias = input.Sweet.Alias
		cfg.Rotation.SOTA.Order = input.SOTA.Order
		cfg.Rotation.Sweet.Order = input.Sweet.Order
		cfg.Rotation.SOTASize = requestedSize
		cfg.Rotation.FirstOutputSeconds = input.FirstOutputSeconds
		return cfg.Rotation.Validate(nil)
	})
	if err != nil {
		writeAdminError(w, 400, "rotation_rejected", a.manager.Redact(err.Error()))
		return
	}
	a.handleRotation(w, r)
}
func (a *Server) handleSelectRotation(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Group string `json:"group"`
		Model string `json:"model"`
	}
	if err := decodeAdminJSON(w, r, &input); err != nil {
		writeAdminError(w, 400, "invalid_request", err.Error())
		return
	}
	if err := a.manager.SelectRotation(input.Group, input.Model); err != nil {
		writeAdminError(w, 400, "rotation_rejected", a.manager.Redact(err.Error()))
		return
	}
	a.handleRotation(w, r)
}

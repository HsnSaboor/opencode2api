package config

import (
	"fmt"
	"regexp"
	"slices"
)

type RotationGroup struct {
	Alias string   `json:"alias"`
	Order []string `json:"order"`
}
type RotationConfig struct {
	SOTA  RotationGroup `json:"sota"`
	Sweet RotationGroup `json:"sweet"`
	// SOTASize is how many of the highest-ranked eligible Go models belong to
	// the SOTA group. Membership itself is derived from the upstream catalog's
	// published prices at every refresh, so a new flagship model joins SOTA on
	// its own and a model that gets repriced moves groups without a code
	// change. The operator's saved order is preserved; this only decides where
	// the line between the two groups falls.
	SOTASize           int `json:"sota_size"`
	FirstOutputSeconds int `json:"first_output_seconds"`
}

// DefaultSOTASize keeps the group split stable for configurations written
// before sota_size existed.
const DefaultSOTASize = 5

// MaxSOTASize bounds a typo that would otherwise ask for more SOTA slots than
// any plausible catalog holds.
const MaxSOTASize = 32

// Defaults fills the fields an operator is not required to state. Group
// membership stays empty on purpose: which models belong to which group is
// decided by the upstream catalog's price ranking at refresh time, and the
// rotation manager fills the order from it. Naming models here would freeze
// today's lineup into the configuration file.
func (c *RotationConfig) Defaults() {
	if c.SOTA.Alias == "" {
		c.SOTA.Alias = "sota"
	}
	if c.Sweet.Alias == "" {
		c.Sweet.Alias = "sweet"
	}
	if c.SOTASize == 0 {
		c.SOTASize = DefaultSOTASize
	}
	if c.FirstOutputSeconds == 0 {
		c.FirstOutputSeconds = 30
	}
}

// Validate checks the operator-authored shape of the rotation configuration.
// Membership needs no check here: the catalog is not loaded when a file is
// read, and a group order that names a since-retired model must not stop the
// gateway from starting. The management API validates submitted orders against
// live rotation state instead.
func (c RotationConfig) Validate(realModels []string) error {
	if c.FirstOutputSeconds < 1 || c.FirstOutputSeconds > 300 {
		return fmt.Errorf("首个有效输出超时需为 1–300 秒")
	}
	if c.SOTASize < 1 || c.SOTASize > MaxSOTASize {
		return fmt.Errorf("SOTA 组模型数量需为 1–%d", MaxSOTASize)
	}
	if c.SOTA.Alias == c.Sweet.Alias {
		return fmt.Errorf("两个分组的别名不能相同")
	}
	valid := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	for _, g := range []RotationGroup{c.SOTA, c.Sweet} {
		if !valid.MatchString(g.Alias) {
			return fmt.Errorf("别名需为 1–64 位英文字母、数字、点、短横线或下划线")
		}
		if slices.Contains(realModels, g.Alias) {
			return fmt.Errorf("别名不能与真实模型 ID 相同")
		}
		seen := map[string]bool{}
		for _, model := range g.Order {
			if model == "" || seen[model] {
				return fmt.Errorf("模型顺序包含重复或空值")
			}
			seen[model] = true
		}
	}
	for _, group := range []RotationGroup{c.SOTA, c.Sweet} {
		if slices.Contains(group.Order, c.SOTA.Alias) || slices.Contains(group.Order, c.Sweet.Alias) {
			return fmt.Errorf("别名不能与真实模型 ID 相同")
		}
	}
	for _, model := range c.Sweet.Order {
		if slices.Contains(c.SOTA.Order, model) {
			return fmt.Errorf("模型不能同时属于两个分组")
		}
	}
	return nil
}

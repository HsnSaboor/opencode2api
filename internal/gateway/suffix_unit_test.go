package gateway

import (
	"reflect"
	"testing"
)

func TestApplyModelSuffix(t *testing.T) {
	cases := []struct {
		name   string
		listed []string
		suffix string
		want   []string
	}{
		{
			name:   "appends to plain ids",
			listed: []string{"alpha", "beta"},
			suffix: ":free",
			want:   []string{"alpha:free", "beta:free"},
		},
		{
			name:   "keeps ids that already carry the suffix",
			listed: []string{"mimo-v2.5:free", "mimo-v2.5"},
			suffix: ":free",
			want:   []string{"mimo-v2.5:free"},
		},
		{
			name:   "collapses duplicates",
			listed: []string{"alpha", "alpha", "alpha:free"},
			suffix: ":free",
			want:   []string{"alpha:free"},
		},
		{
			name:   "preserves order",
			listed: []string{"b:free", "a", "c"},
			suffix: ":free",
			want:   []string{"b:free", "a:free", "c:free"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := applyModelSuffix(tc.listed, tc.suffix); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("applyModelSuffix(%v, %q) = %v, want %v", tc.listed, tc.suffix, got, tc.want)
			}
		})
	}
}

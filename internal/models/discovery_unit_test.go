package models

import (
	"encoding/json"
	"testing"
)

// The catalog encodes interleaved reasoning either as an object naming the
// wire field or as a legacy boolean meaning the default field. Both forms must
// decode; an undecodable form used to fail the whole capability refresh.
func TestCapabilityModelInterleavedEncodings(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{name: "boolean true selects the default field", body: `{"interleaved":true}`, field: "reasoning_content"},
		{name: "boolean false means no interleaving", body: `{"interleaved":false}`, field: ""},
		{name: "object names its own field", body: `{"interleaved":{"field":"reasoning_text"}}`, field: "reasoning_text"},
		{name: "absent entry stays absent", body: `{}`, field: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var model capabilityModel
			if err := json.Unmarshal([]byte(tc.body), &model); err != nil {
				t.Fatalf("decode %s: %v", tc.body, err)
			}
			if got := model.metadata().InterleavedField; got != tc.field {
				t.Fatalf("metadata InterleavedField = %q, want %q", got, tc.field)
			}
		})
	}
}

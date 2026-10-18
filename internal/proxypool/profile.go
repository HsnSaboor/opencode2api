// Package proxypool hosts the sing-box multiplexed SOCKS5 exit pool: profile
// loading, multiplexer process management, and the SOCKS5 dialer that proxy
// egress lanes use. Every profile node is one exit; the gateway treats one
// node the same way it treats one IPv6 source address: a single lane whose
// selection, parks, and rotation live in the shared egress pool.
package proxypool

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Node is one usable outbound from a sing-box profile: the raw outbound
// document plus the metadata the pool needs to route and label it. User is
// the SOCKS5 username ("p<Index>") the multiplexer maps to this outbound.
type Node struct {
	Index  int
	Tag    string
	Type   string
	Server string
	Port   int
	User   string
	Raw    map[string]any
}

// Profile is a loaded node list plus a content fingerprint of its source
// file, used to invalidate persisted pool state when the profile changes.
type Profile struct {
	Path        string
	Nodes       []Node
	Fingerprint string
}

// LoadProfile reads a sing-box outbounds document (the "expanded" profile
// shape written by the zen-proxy verification pipeline) and flattens it into
// nodes. Outbounds that cannot serve as a general exit are skipped: direct,
// block, and dns handlers; entries without a server endpoint; and transport
// types the installed sing-box cannot dial (xhttp).
func LoadProfile(path string) (*Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read proxy profile: %w", err)
	}
	var doc struct {
		Outbounds []map[string]any `json:"outbounds"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse proxy profile %s: %w", path, err)
	}
	nodes := make([]Node, 0, len(doc.Outbounds))
	for _, ob := range doc.Outbounds {
		outboundType, _ := ob["type"].(string)
		switch outboundType {
		case "", "direct", "block", "dns":
			continue
		}
		if transport, ok := ob["transport"].(map[string]any); ok {
			if transportType, _ := transport["type"].(string); transportType == "xhttp" {
				continue
			}
		}
		server, _ := ob["server"].(string)
		port := 0
		switch value := ob["server_port"].(type) {
		case float64:
			port = int(value)
		case json.Number:
			if parsed, err := value.Int64(); err == nil {
				port = int(parsed)
			}
		case string:
			// Some profile converters emit the port as a string; the
			// zen-proxy pipeline accepted both shapes.
			if parsed, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
				port = parsed
			}
		}
		if server == "" || port <= 0 || port > 65535 {
			continue
		}
		tag, _ := ob["tag"].(string)
		index := len(nodes)
		if tag == "" {
			tag = fmt.Sprintf("node-%d", index)
		}
		nodes = append(nodes, Node{
			Index:  index,
			Tag:    tag,
			Type:   outboundType,
			Server: server,
			Port:   port,
			User:   fmt.Sprintf("p%d", index),
			Raw:    ob,
		})
	}
	sum := sha256.Sum256(data)
	return &Profile{Path: path, Nodes: nodes, Fingerprint: hex.EncodeToString(sum[:])}, nil
}

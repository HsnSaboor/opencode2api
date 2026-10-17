package gateway

import (
	"encoding/json"

	"opencode2api/internal/jsonutil"
	wire "opencode2api/internal/protocol"
)

// unifiedExecToolDecls mirrors the unified exec declarations Codex sends for
// models it drives natively (exec_command/write_stdin). Codex's tool router
// dispatches both names in every session regardless of the toolset declared
// for the configured model, so they are a faithful stand-in for its freeform
// exec tool.
const unifiedExecToolDecls = `[
  {
    "type": "function",
    "name": "exec_command",
    "description": "Runs a command in a PTY, returning output or a session ID for ongoing interaction.",
    "strict": false,
    "parameters": {
      "type": "object",
      "properties": {
        "cmd": {"type": "string", "description": "Shell command to execute."},
        "justification": {"type": "string", "description": "User-facing approval question for 'require_escalated'; omit otherwise."},
        "login": {"type": "boolean", "description": "True runs the shell with -l/-i semantics; false disables them. Defaults to true."},
        "max_output_tokens": {"type": "number", "description": "Output token budget. Defaults to 10000 tokens; larger requests may be capped by policy."},
        "prefix_rule": {"type": "array", "description": "Reusable approval prefix for 'cmd', only with 'sandbox_permissions: require_escalated'; for example [\"git\", \"pull\"].", "items": {"type": "string"}},
        "sandbox_permissions": {"type": "string", "description": "Per-command sandbox override. Defaults to 'use_default'; use 'require_escalated' for unsandboxed execution.", "enum": ["use_default", "require_escalated"]},
        "shell": {"type": "string", "description": "Shell binary to launch. Defaults to the user's default shell."},
        "tty": {"type": "boolean", "description": "True allocates a PTY for the command; false or omitted uses plain pipes."},
        "workdir": {"type": "string", "description": "Working directory for the command. Defaults to the turn cwd."},
        "yield_time_ms": {"type": "number", "description": "Wait before yielding output. Defaults to 10000 ms; effective range is 250-30000 ms."}
      },
      "required": ["cmd"],
      "additionalProperties": false
    }
  },
  {
    "type": "function",
    "name": "write_stdin",
    "description": "Writes characters to an existing unified exec session and returns recent output.",
    "strict": false,
    "parameters": {
      "type": "object",
      "properties": {
        "chars": {"type": "string", "description": "Bytes to write to stdin. Defaults to empty, which polls without writing."},
        "max_output_tokens": {"type": "number", "description": "Output token budget. Defaults to 10000 tokens; larger requests may be capped by policy."},
        "session_id": {"type": "number", "description": "Identifier of the running unified exec session."},
        "yield_time_ms": {"type": "number", "description": "Wait before yielding output. Non-empty writes default to 250 ms and cap at 30000 ms; empty polls wait 5000-300000 ms by default."}
      },
      "required": ["session_id"],
      "additionalProperties": false
    }
  }
]`

// unifiedExecTools returns fresh copies of the unified exec declarations.
// Copies matter: upstream preparation may edit tool maps in place, and the
// shared constant must stay pristine across requests.
func unifiedExecTools() []any {
	var decls []any
	if err := json.Unmarshal([]byte(unifiedExecToolDecls), &decls); err != nil {
		return nil
	}
	return decls
}

// sanitizeToolsForUpstream rewrites Responses tool declarations that the Zen
// upstream rejects verbatim, returning the replacement list and whether
// anything changed. Codex declares two such shapes in every session:
//
//   - A freeform "custom" exec tool is rejected outright ("custom tools are
//     not supported on this endpoint"). It collapses into the unified exec
//     function pair, which Codex's router executes natively and every target
//     protocol can carry.
//   - A web_search declaration may not carry search_content_types. Responses
//     targets keep the tool without that field; Chat and Anthropic targets
//     have no search equivalent, so the declaration is dropped there.
//
// Namespace envelopes and custom tools this gateway cannot represent are
// dropped so the request still reaches the model. The input payload is not
// modified; callers own the replacement.
func sanitizeToolsForUpstream(input map[string]any, target wire.Protocol) ([]any, bool) {
	raw := jsonutil.SliceAt(input, "tools")
	if len(raw) == 0 {
		return nil, false
	}
	declared := make(map[string]bool, len(raw))
	for _, item := range raw {
		if tool, ok := item.(map[string]any); ok {
			declared[jsonutil.StringAt(tool, "name")] = true
		}
	}
	output := make([]any, 0, len(raw)+2)
	changed := false
	execCustom := false
	for _, item := range raw {
		tool, ok := item.(map[string]any)
		if !ok {
			output = append(output, item)
			continue
		}
		switch jsonutil.StringAt(tool, "type") {
		case "function":
			output = append(output, tool)
		case "custom":
			changed = true
			if jsonutil.StringAt(tool, "name") == "exec" {
				execCustom = true
			}
		case "web_search", "web_search_preview":
			if target != wire.Responses {
				changed = true
				continue
			}
			if _, present := tool["search_content_types"]; present {
				clone := make(map[string]any, len(tool))
				for key, value := range tool {
					clone[key] = value
				}
				delete(clone, "search_content_types")
				output = append(output, clone)
				changed = true
			} else {
				output = append(output, tool)
			}
		case "namespace":
			changed = true
		default:
			output = append(output, tool)
		}
	}
	if execCustom && target != wire.Anthropic {
		for _, decl := range unifiedExecTools() {
			tool, ok := decl.(map[string]any)
			if !ok {
				continue
			}
			name := jsonutil.StringAt(tool, "name")
			if !declared[name] {
				output = append(output, tool)
				declared[name] = true
			}
		}
	}
	if !changed {
		return nil, false
	}
	return output, true
}

// withReplacedTools returns a shallow copy of a request payload carrying the
// sanitized tool list, leaving the caller's payload untouched for other tiers.
func withReplacedTools(input map[string]any, tools []any) map[string]any {
	clone := make(map[string]any, len(input)+1)
	for key, value := range input {
		clone[key] = value
	}
	clone["tools"] = tools
	return clone
}

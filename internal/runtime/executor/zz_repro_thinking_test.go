package executor

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
)

// TestReproCodeBuddyKimiK3ThinkingPassThrough reproduces the production issue.
// The client (likely Claude Code via /v1/chat/completions bridge or similar)
// sends reasoning_effort=max; glm-5.3 supports max but kimi-k3-1 only has
// low/medium/high, and the thinking pipeline drops the effort entirely.
func TestReproCodeBuddyKimiK3ThinkingPassThrough(t *testing.T) {
	cases := []struct {
		name       string
		model      string
		effortBody string
	}{
		{"glm-5.3 with max", "glm-5.3", `"reasoning_effort": "max"`},
		{"kimi-k3-1 with max", "kimi-k3-1", `"reasoning_effort": "max"`},
		{"kimi-k3-1 with high", "kimi-k3-1", `"reasoning_effort": "high"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{
				"model": "` + tc.model + `",
				"max_tokens": 8192,
				` + tc.effortBody + `,
				"messages": [{"role": "user", "content": "hi"}]
			}`)
			// openai -> openai passthrough translation, then ApplyThinking (codebuddy provider key)
			out, err := thinking.ApplyThinkingWithSummary(body, tc.model, "openai", "openai", "codebuddy", thinking.SummaryConfig{})
			if err != nil {
				t.Logf("ApplyThinkingWithSummary returned error: %v", err)
			}
			recorded := thinking.ExtractTranslatedReasoningEffort(out, "codebuddy")
			var pretty map[string]any
			_ = json.Unmarshal(out, &pretty)
			if re, ok := pretty["reasoning_effort"]; ok {
				t.Logf("body reasoning_effort=%v, usage-recorded=%q", re, recorded)
			} else {
				t.Logf("body has NO reasoning_effort, usage-recorded=%q", recorded)
			}
			info := registry.LookupModelInfo(tc.model, "codebuddy")
			if info != nil && info.Thinking != nil {
				t.Logf("registry levels: %v", info.Thinking.Levels)
			}
		})
	}
}

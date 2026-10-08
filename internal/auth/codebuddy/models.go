package codebuddy

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

var modelDiscoveryDisabled atomic.Bool

// SetModelDiscoveryEnabled controls upstream discovery for local-model mode.
func SetModelDiscoveryEnabled(enabled bool) { modelDiscoveryDisabled.Store(!enabled) }

// ModelDiscoveryEnabled reports whether upstream model discovery is enabled.
func ModelDiscoveryEnabled() bool { return !modelDiscoveryDisabled.Load() }

type modelConfig struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	Disabled          bool   `json:"disabled"`
	MaxInputTokens    int    `json:"maxInputTokens"`
	MaxOutputTokens   int    `json:"maxOutputTokens"`
	SupportsImages    *bool  `json:"supportsImages"`
	SupportsReasoning *bool  `json:"supportsReasoning"`
	OnlyReasoning     bool   `json:"onlyReasoning"`
	Description       string `json:"descriptionEn"`
	Reasoning         struct {
		SupportedEfforts   []string `json:"supportedEfforts"`
		DefaultEffort      string   `json:"defaultEffort"`
		Effort             string   `json:"effort"`
		CanDisableThinking *bool    `json:"canDisableThinking"`
	} `json:"reasoning"`
}

// ParseModelConfig reads the same CLI catalog as CodeBuddy's /model picker.
// The product-wide model array also includes models unavailable to the CLI.
func ParseModelConfig(body []byte) ([]*registry.ModelInfo, error) {
	var response struct {
		Code int `json:"code"`
		Data struct {
			Models []modelConfig `json:"models"`
			Agents []struct {
				Name   string   `json:"name"`
				Models []string `json:"models"`
			} `json:"agents"`
		} `json:"data"`
	}
	if errDecode := json.Unmarshal(body, &response); errDecode != nil {
		return nil, fmt.Errorf("codebuddy: invalid model configuration JSON")
	}
	if response.Code != 0 {
		return nil, fmt.Errorf("codebuddy: model configuration rejected (code %d)", response.Code)
	}
	var cliModels []string
	for _, agent := range response.Data.Agents {
		if agent.Name == "cli" {
			cliModels = agent.Models
			break
		}
	}
	if len(cliModels) == 0 || len(response.Data.Models) == 0 {
		return nil, fmt.Errorf("codebuddy: model configuration has no CLI catalog")
	}
	byID := make(map[string]modelConfig, len(response.Data.Models))
	for _, model := range response.Data.Models {
		if id := strings.TrimSpace(model.ID); id != "" {
			byID[id] = model
		}
	}
	known := make(map[string]*registry.ModelInfo)
	for _, model := range registry.GetCodeBuddyModels() {
		if model != nil {
			known[model.ID] = model
		}
	}
	models := make([]*registry.ModelInfo, 0, len(cliModels))
	seen := make(map[string]bool, len(cliModels))
	for _, modelID := range cliModels {
		id := strings.TrimSpace(modelID)
		remote, exists := byID[id]
		if !exists || remote.Disabled || seen[id] {
			continue
		}
		seen[id] = true
		models = append(models, remote.modelInfo(id, known))
	}
	if len(models) == 0 {
		return nil, fmt.Errorf("codebuddy: model configuration has no usable CLI models")
	}
	return models, nil
}

func (m modelConfig) modelInfo(id string, known map[string]*registry.ModelInfo) *registry.ModelInfo {
	info := &registry.ModelInfo{ID: id, Object: "model", Type: ProviderName, OwnedBy: "tencent", DisplayName: id}
	baseline := known[id]
	if baseline == nil {
		baseline = known[strings.TrimSuffix(id, "-x")]
	}
	if baseline != nil {
		*info = *baseline
		info.ID = id
		if baseline.Thinking != nil {
			thinking := *baseline.Thinking
			thinking.Levels = append([]string(nil), baseline.Thinking.Levels...)
			info.Thinking = &thinking
		}
	}
	if m.Name != "" {
		info.DisplayName = m.Name
	}
	if m.Description != "" {
		info.Description = m.Description
	}
	if m.MaxInputTokens > 0 {
		info.ContextLength = m.MaxInputTokens
	}
	if m.MaxOutputTokens > 0 {
		info.MaxCompletionTokens = m.MaxOutputTokens
	}
	if m.SupportsImages != nil {
		info.SupportedInputModalities = []string{"text"}
		if *m.SupportsImages {
			info.SupportedInputModalities = append(info.SupportedInputModalities, "image")
		}
	}
	info.SupportedOutputModalities = []string{"text"}
	if m.SupportsReasoning != nil && !*m.SupportsReasoning {
		info.Thinking = nil
		return info
	}
	if len(m.Reasoning.SupportedEfforts) > 0 {
		info.Thinking = &registry.ThinkingSupport{Levels: append([]string(nil), m.Reasoning.SupportedEfforts...)}
	} else if info.Thinking == nil && m.SupportsReasoning != nil && *m.SupportsReasoning {
		effort := m.Reasoning.DefaultEffort
		if effort == "" {
			effort = m.Reasoning.Effort
		}
		if effort != "" {
			info.Thinking = &registry.ThinkingSupport{Levels: []string{effort}}
		}
	}
	if info.Thinking != nil {
		if m.Reasoning.CanDisableThinking != nil {
			info.Thinking.ZeroAllowed = *m.Reasoning.CanDisableThinking
		} else if m.OnlyReasoning {
			info.Thinking.ZeroAllowed = false
		}
	}
	return info
}

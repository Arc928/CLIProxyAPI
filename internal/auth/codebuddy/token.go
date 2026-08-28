// Package codebuddy provides authentication and token management for
// Tencent CodeBuddy / WorkBuddy subscription sessions.
package codebuddy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/misc"
	log "github.com/sirupsen/logrus"
)

const (
	// ProviderName is the CPA provider key for CodeBuddy.
	ProviderName = "codebuddy"
	// DefaultCNAPIBase is the China-site Copilot API host.
	DefaultCNAPIBase = "https://copilot.tencent.com"
	// DefaultIntlAPIBase is the international CodeBuddy API host.
	DefaultIntlAPIBase = "https://www.codebuddy.ai"
	// DefaultCNDomain is the domain stored in desktop login files for China accounts.
	DefaultCNDomain = "www.codebuddy.cn"
	// PluginPrefix is the authentication.attributes.prefixPath used by the CLI.
	PluginPrefix = "/plugin"
	// refreshThresholdSeconds is when to treat a token as expired for refresh.
	refreshThresholdSeconds = 300
)

// TokenStorage stores OAuth2 token information for CodeBuddy API authentication.
type TokenStorage struct {
	AccessToken  string         `json:"access_token"`
	RefreshToken string         `json:"refresh_token"`
	TokenType    string         `json:"token_type"`
	Scope        string         `json:"scope,omitempty"`
	Expired      string         `json:"expired,omitempty"`
	UID          string         `json:"uid,omitempty"`
	Nickname     string         `json:"nickname,omitempty"`
	EnterpriseID string         `json:"enterprise_id,omitempty"`
	Domain       string         `json:"domain,omitempty"`
	APIBaseURL   string         `json:"api_base_url,omitempty"`
	Type         string         `json:"type"`
	Metadata     map[string]any `json:"-"`
}

// TokenData holds a parsed OAuth token payload from CodeBuddy.
type TokenData struct {
	AccessToken  string
	RefreshToken string
	TokenType    string
	Scope        string
	ExpiresAt    int64 // unix seconds
	Domain       string
}

// AccountData holds the signed-in CodeBuddy account identity.
type AccountData struct {
	UID          string
	Nickname     string
	EnterpriseID string
}

// AuthBundle is the login result persisted into an auth file.
type AuthBundle struct {
	TokenData *TokenData
	Account   *AccountData
	APIBase   string
}

// AuthState is the browser login URL returned by /v2/plugin/auth/state.
type AuthState struct {
	AuthURL string
	State   string
}

// SetMetadata allows external callers to inject metadata into the storage before saving.
func (ts *TokenStorage) SetMetadata(meta map[string]any) {
	ts.Metadata = meta
}

// SaveTokenToFile serializes the CodeBuddy token storage to a JSON file.
func (ts *TokenStorage) SaveTokenToFile(authFilePath string) error {
	misc.LogSavingCredentials(authFilePath)
	ts.Type = ProviderName

	if err := os.MkdirAll(filepath.Dir(authFilePath), 0700); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	data, errMerge := misc.MergeMetadata(ts, ts.Metadata)
	if errMerge != nil {
		return fmt.Errorf("failed to merge metadata: %w", errMerge)
	}

	f, err := os.Create(authFilePath)
	if err != nil {
		return fmt.Errorf("failed to create token file: %w", err)
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			log.Errorf("codebuddy token storage: close token file error: %v", errClose)
		}
	}()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err = encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to write token to file: %w", err)
	}
	return nil
}

// IsExpired checks if the token has expired or is within the refresh threshold.
func (ts *TokenStorage) IsExpired() bool {
	if ts.Expired == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, ts.Expired)
	if err != nil {
		return true
	}
	return time.Now().Add(time.Duration(refreshThresholdSeconds) * time.Second).After(t)
}

// APIBaseURLForDomain maps a login domain to the Copilot API host.
func APIBaseURLForDomain(domain string) string {
	d := strings.ToLower(strings.TrimSpace(domain))
	d = strings.TrimPrefix(d, "https://")
	d = strings.TrimPrefix(d, "http://")
	if slash := strings.IndexByte(d, '/'); slash >= 0 {
		d = d[:slash]
	}
	switch {
	case d == "":
		return DefaultCNAPIBase
	case strings.Contains(d, "codebuddy.ai") && !strings.Contains(d, "codebuddy.cn"):
		return DefaultIntlAPIBase
	case strings.Contains(d, "sso.copilot.tencent.com") || strings.Contains(d, "sso.codebuddy"):
		return "https://tencent.sso.copilot.tencent.com"
	default:
		return DefaultCNAPIBase
	}
}

// ChatCompletionsURL returns the upstream chat completions endpoint.
func ChatCompletionsURL(apiBase string) string {
	return strings.TrimRight(strings.TrimSpace(apiBase), "/") + "/v2/chat/completions"
}

// PluginAuthURL returns a /v2/plugin/... path on apiBase.
func PluginAuthURL(apiBase, path string) string {
	base := strings.TrimRight(strings.TrimSpace(apiBase), "/")
	path = strings.TrimSpace(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return base + "/v2" + PluginPrefix + path
}

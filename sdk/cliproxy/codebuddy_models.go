package cliproxy

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	log "github.com/sirupsen/logrus"
)

const (
	codeBuddyModelsRefreshInterval = 8 * time.Minute
	codeBuddyModelsRetryInterval   = time.Minute
	// The official endpoint uses the CLI user agent and product to select its catalog.
	codeBuddyModelsUserAgent = "CLI/2.158.0 CodeBuddy/2.158.0"
)

type codeBuddyModelCacheEntry struct {
	sourceKey     string
	credentialKey string
	models        []*ModelInfo
	nextRefresh   time.Time
	inFlight      bool
}

func (s *Service) codeBuddyModelsForAuth(auth *coreauth.Auth) []*ModelInfo {
	if s == nil || !codebuddyauth.ModelDiscoveryEnabled() {
		return registry.GetCodeBuddyModels()
	}
	req, cfg, errRequest := s.codeBuddyModelRequest(context.Background(), auth)
	if errRequest != nil {
		return registry.GetCodeBuddyModels()
	}
	sourceKey, _ := codeBuddyModelCacheKeys(req, cfg, auth)
	s.codeBuddyModelsMu.Lock()
	entry := s.codeBuddyModelsCache[auth.ID]
	var models []*ModelInfo
	if entry != nil && entry.sourceKey == sourceKey {
		models = entry.models
	}
	s.codeBuddyModelsMu.Unlock()
	if models != nil {
		return models
	}
	return registry.GetCodeBuddyModels()
}

func (s *Service) codeBuddyModelRequest(ctx context.Context, auth *coreauth.Auth) (*http.Request, *config.Config, error) {
	if auth == nil || auth.ID == "" || auth.Disabled {
		return nil, nil, fmt.Errorf("codebuddy: no active credential")
	}
	baseURL := ""
	domain := ""
	if auth.Metadata != nil {
		baseURL, _ = auth.Metadata["api_base_url"].(string)
		domain, _ = auth.Metadata["domain"].(string)
	}
	if storage, ok := auth.Storage.(*codebuddyauth.TokenStorage); ok && storage != nil {
		if baseURL == "" {
			baseURL = storage.APIBaseURL
		}
		if domain == "" {
			domain = storage.Domain
		}
	}
	if strings.TrimSpace(baseURL) == "" {
		baseURL = codebuddyauth.APIBaseURLForDomain(domain)
	}
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(strings.TrimSpace(baseURL), "/")+"/v3/config", nil)
	if errRequest != nil {
		return nil, nil, fmt.Errorf("codebuddy: invalid model configuration URL")
	}
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if errPrepare := executor.NewCodeBuddyExecutor(cfg).PrepareRequest(req, auth); errPrepare != nil {
		return nil, nil, errPrepare
	}
	if req.Header.Get("Authorization") == "" {
		return nil, nil, fmt.Errorf("codebuddy: missing model discovery token")
	}
	req.Header.Set("User-Agent", codeBuddyModelsUserAgent)
	req.Header.Set("X-Product", "SaaS")
	return req, cfg, nil
}

func codeBuddyModelCacheKeys(req *http.Request, cfg *config.Config, auth *coreauth.Auth) (string, string) {
	headers := req.Header.Clone()
	credentialKey := fmt.Sprintf("%x", sha256.Sum256([]byte(headers.Get("Authorization"))))
	headers.Del("Authorization")
	headerJSON, _ := json.Marshal(headers)
	proxyURL := auth.ProxyURL
	if proxyURL == "" && cfg != nil {
		proxyURL = cfg.ProxyURL
	}
	sourceKey := fmt.Sprintf("%x", sha256.Sum256([]byte(req.URL.String()+"\n"+proxyURL+"\n"+string(headerJSON))))
	return sourceKey, credentialKey
}

func (s *Service) scheduleCodeBuddyModelRefresh(ctx context.Context, auth *coreauth.Auth) {
	if s == nil || !codebuddyauth.ModelDiscoveryEnabled() || s.coreManager == nil {
		return
	}
	s.codeBuddyModelsMu.Lock()
	if s.codeBuddyModelsContext != nil {
		ctx = s.codeBuddyModelsContext
	}
	s.codeBuddyModelsMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return
	}
	req, cfg, errRequest := s.codeBuddyModelRequest(ctx, auth)
	if errRequest != nil {
		return
	}
	sourceKey, credentialKey := codeBuddyModelCacheKeys(req, cfg, auth)
	s.codeBuddyModelsMu.Lock()
	if s.codeBuddyModelsCache == nil {
		s.codeBuddyModelsCache = make(map[string]*codeBuddyModelCacheEntry)
	}
	entry := s.codeBuddyModelsCache[auth.ID]
	if entry == nil || entry.sourceKey != sourceKey || entry.credentialKey != credentialKey {
		next := &codeBuddyModelCacheEntry{sourceKey: sourceKey, credentialKey: credentialKey}
		if entry != nil && entry.sourceKey == sourceKey {
			next.models = entry.models
		}
		entry = next
		s.codeBuddyModelsCache[auth.ID] = entry
	}
	if entry.inFlight || time.Now().Before(entry.nextRefresh) {
		s.codeBuddyModelsMu.Unlock()
		return
	}
	entry.inFlight = true
	s.codeBuddyModelsMu.Unlock()
	auth = auth.Clone()
	go s.refreshCodeBuddyModels(ctx, req, cfg, auth, entry)
}

func (s *Service) refreshCodeBuddyModels(ctx context.Context, req *http.Request, cfg *config.Config, auth *coreauth.Auth, entry *codeBuddyModelCacheEntry) {
	models, errFetch := fetchCodeBuddyModels(req, helps.NewProxyAwareHTTPClient(ctx, cfg, auth, 0))
	s.codeBuddyModelsMu.Lock()
	if s.codeBuddyModelsCache[auth.ID] != entry {
		s.codeBuddyModelsMu.Unlock()
		return
	}
	entry.inFlight = false
	entry.nextRefresh = time.Now().Add(codeBuddyModelsRetryInterval)
	if errFetch != nil {
		s.codeBuddyModelsMu.Unlock()
		if ctx.Err() == nil {
			log.WithError(errFetch).Warn("codebuddy: model discovery failed, keeping previous catalog")
		}
		return
	}
	changed := !reflect.DeepEqual(entry.models, models)
	entry.models = models
	entry.nextRefresh = time.Now().Add(codeBuddyModelsRefreshInterval)
	s.codeBuddyModelsMu.Unlock()
	if ctx.Err() != nil || !changed {
		return
	}
	current, exists := s.latestAuthForModelRegistration(auth.ID)
	if !exists || current.Disabled || current.Provider != codebuddyauth.ProviderName {
		return
	}
	if current.RegistrationEpoch != auth.RegistrationEpoch {
		return
	}
	currentRequest, currentConfig, errRequest := s.codeBuddyModelRequest(ctx, current)
	if errRequest != nil {
		return
	}
	sourceKey, credentialKey := codeBuddyModelCacheKeys(currentRequest, currentConfig, current)
	if sourceKey != entry.sourceKey || credentialKey != entry.credentialKey {
		return
	}
	s.refreshModelRegistrationForAuthWithContext(ctx, current, nil)
	log.WithField("model_count", len(models)).Info("codebuddy: synchronized upstream CLI model catalog")
}

func fetchCodeBuddyModels(req *http.Request, client *http.Client) ([]*ModelInfo, error) {
	resp, errFetch := client.Do(req)
	if errFetch != nil {
		return nil, fmt.Errorf("codebuddy: model configuration request failed: %w", errFetch)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Warn("codebuddy: close model configuration response")
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("codebuddy: model configuration HTTP status %d", resp.StatusCode)
	}
	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return nil, fmt.Errorf("codebuddy: read model configuration: %w", errRead)
	}
	return codebuddyauth.ParseModelConfig(body)
}

func (s *Service) startCodeBuddyModelsUpdater(ctx context.Context) {
	s.codeBuddyModelsMu.Lock()
	s.codeBuddyModelsContext = ctx
	s.codeBuddyModelsMu.Unlock()
	go func() {
		ticker := time.NewTicker(codeBuddyModelsRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if s.coreManager == nil {
					continue
				}
				for _, auth := range s.coreManager.List() {
					if auth != nil && auth.Provider == codebuddyauth.ProviderName && !auth.Disabled {
						s.scheduleCodeBuddyModelRefresh(ctx, auth)
					}
				}
			}
		}
	}()
}

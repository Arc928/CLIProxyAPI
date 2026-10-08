package cliproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codebuddy"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

const codeBuddyTestModels = `{"code":0,"data":{
	"agents":[{"name":"cli","models":["hy3-x","kimi-k2.8-preview"]}],
	"models":[{"id":"glm-5.0"},{"id":"hy3-x","name":"Hy3"},{"id":"kimi-k2.8-preview","name":"Kimi-K2.8-Preview"}]}}`

func newCodeBuddyModelTestService(t *testing.T, handler http.HandlerFunc) (*Service, *coreauth.Auth) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{}
	manager.SetConfig(cfg)
	s := &Service{cfg: cfg, coreManager: manager}
	auth, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID: t.Name(), Provider: "codebuddy", Status: coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "test-token", "uid": "test-user", "domain": "www.codebuddy.cn", "api_base_url": server.URL},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	return s, auth
}

func refreshCodeBuddyTestModels(t *testing.T, s *Service, auth *coreauth.Auth) {
	t.Helper()
	req, cfg, errRequest := s.codeBuddyModelRequest(context.Background(), auth)
	if errRequest != nil {
		t.Fatal(errRequest)
	}
	sourceKey, credentialKey := codeBuddyModelCacheKeys(req, cfg, auth)
	s.codeBuddyModelsMu.Lock()
	if s.codeBuddyModelsCache == nil {
		s.codeBuddyModelsCache = make(map[string]*codeBuddyModelCacheEntry)
	}
	entry := s.codeBuddyModelsCache[auth.ID]
	if entry == nil {
		entry = &codeBuddyModelCacheEntry{sourceKey: sourceKey, credentialKey: credentialKey}
		s.codeBuddyModelsCache[auth.ID] = entry
	}
	s.codeBuddyModelsMu.Unlock()
	s.refreshCodeBuddyModels(context.Background(), req, cfg, auth, entry)
}

func codeBuddyRegisteredIDs(authID string) []string {
	var ids []string
	for _, model := range GlobalModelRegistry().GetModelsForClient(authID) {
		ids = append(ids, model.ID)
	}
	sort.Strings(ids)
	return ids
}

func TestCodeBuddyModelDiscoveryRegistersOnlyCLICatalog(t *testing.T) {
	var chatReceived atomic.Bool
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v2/chat/completions" {
			var body struct {
				Model string `json:"model"`
			}
			if errDecode := json.NewDecoder(r.Body).Decode(&body); errDecode != nil || body.Model != "hy3-x" {
				t.Error("discovered model ID did not reach upstream unchanged")
			}
			chatReceived.Store(true)
			_, _ = w.Write([]byte(`{"id":"test","model":"hy3-x","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
			return
		}
		if r.URL.Path != "/v3/config" || r.Header.Get("Authorization") != "Bearer test-token" || r.Header.Get("X-User-Id") != "test-user" || r.Header.Get("X-Domain") != "www.codebuddy.cn" || r.Header.Get("X-Product") != "SaaS" || r.Header.Get("User-Agent") != codeBuddyModelsUserAgent {
			t.Errorf("unexpected discovery request path/identity headers")
		}
		_, _ = w.Write([]byte(codeBuddyTestModels))
	})
	refreshCodeBuddyTestModels(t, s, auth)
	if got := codeBuddyRegisteredIDs(auth.ID); !reflect.DeepEqual(got, []string{"hy3-x", "kimi-k2.8-preview"}) {
		t.Fatalf("registered models = %v", got)
	}
	if _, errExecute := s.coreManager.Execute(context.Background(), []string{"codebuddy"}, cliproxyexecutor.Request{
		Model: "hy3-x", Payload: []byte(`{"model":"hy3-x","messages":[{"role":"user","content":"test"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: "openai"}); errExecute != nil {
		t.Fatalf("discovered model was not routable: %v", errExecute)
	}
	if !chatReceived.Load() {
		t.Fatal("discovered model did not reach its upstream executor")
	}
}

func TestCodeBuddyModelDiscoveryRefreshRemovesOldModelsAndKeepsLastGoodOnFailure(t *testing.T) {
	var mode atomic.Int32
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load() {
		case 0:
			_, _ = w.Write([]byte(codeBuddyTestModels))
		case 1:
			_, _ = w.Write([]byte(`{"code":0,"data":{"agents":[{"name":"cli","models":["hy3"]}],"models":[{"id":"hy3"}]}}`))
		case 2:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 3:
			_, _ = w.Write([]byte(`{"code":0,"data":{"models":null}}`))
		}
	})
	refreshCodeBuddyTestModels(t, s, auth)
	mode.Store(1)
	refreshCodeBuddyTestModels(t, s, auth)
	for _, nextMode := range []int32{1, 2, 3} {
		mode.Store(nextMode)
		refreshCodeBuddyTestModels(t, s, auth)
		if got := codeBuddyRegisteredIDs(auth.ID); !reflect.DeepEqual(got, []string{"hy3"}) {
			t.Fatalf("mode %d registered models = %v", nextMode, got)
		}
		models := s.codeBuddyModelsForAuth(auth)
		if len(models) != 1 || models[0].ID != "hy3" {
			t.Fatalf("mode %d lost last good catalog: %+v", nextMode, models)
		}
	}
}

func TestCodeBuddyModelDiscoveryAppliesAliasesExclusionsAndPrefixes(t *testing.T) {
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(codeBuddyTestModels))
	})
	s.cfg.OAuthExcludedModels = map[string][]string{"codebuddy": {"hy3-x"}}
	s.cfg.OAuthModelAlias = map[string][]config.OAuthModelAlias{"codebuddy": {{Name: "kimi-k2.8-preview", Alias: "kimi-latest"}}}
	s.cfg.ForceModelPrefix = true
	auth.Prefix = "account"
	updated, errUpdate := s.coreManager.Update(context.Background(), auth)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
	refreshCodeBuddyTestModels(t, s, updated)
	if got := codeBuddyRegisteredIDs(auth.ID); !reflect.DeepEqual(got, []string{"account/kimi-latest"}) {
		t.Fatalf("registered models = %v", got)
	}
}

func TestCodeBuddyModelDiscoveryIsolatesAccounts(t *testing.T) {
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		id := "hy3"
		if r.Header.Get("Authorization") == "Bearer second-token" {
			id = "kimi-k3-1"
		}
		_, _ = fmt.Fprintf(w, `{"code":0,"data":{"agents":[{"name":"cli","models":[%q]}],"models":[{"id":%q}]}}`, id, id)
	})
	second := auth.Clone()
	second.ID += "-second"
	second.Metadata["access_token"] = "second-token"
	second.Metadata["uid"] = "second-user"
	second, errRegister := s.coreManager.Register(context.Background(), second)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(second.ID) })
	refreshCodeBuddyTestModels(t, s, auth)
	refreshCodeBuddyTestModels(t, s, second)
	if got := codeBuddyRegisteredIDs(auth.ID); !reflect.DeepEqual(got, []string{"hy3"}) {
		t.Fatalf("first account models = %v", got)
	}
	if got := codeBuddyRegisteredIDs(second.ID); !reflect.DeepEqual(got, []string{"kimi-k3-1"}) {
		t.Fatalf("second account models = %v", got)
	}
}

func TestCodeBuddyModelDiscoveryDoesNotBlockRegistration(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte(codeBuddyTestModels))
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.codeBuddyModelsContext = ctx
	registered := make(chan struct{})
	go func() {
		s.registerModelsForAuth(ctx, auth)
		close(registered)
	}()
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("registration blocked on upstream discovery")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("upstream discovery did not start")
	}
	s.applyCoreAuthRemoval(ctx, auth.ID)
	cancel()
	close(release)
	if got := codeBuddyRegisteredIDs(auth.ID); len(got) != 0 {
		t.Fatalf("removed auth still has registered models: %v", got)
	}
}

func TestCodeBuddyModelDiscoveryDoesNotRestoreRemovedAuth(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte(codeBuddyTestModels))
	})
	done := make(chan struct{})
	go func() {
		refreshCodeBuddyTestModels(t, s, auth)
		close(done)
	}()
	<-started
	s.applyCoreAuthRemoval(context.Background(), auth.ID)
	close(release)
	<-done
	if got := codeBuddyRegisteredIDs(auth.ID); len(got) != 0 {
		t.Fatalf("late model discovery restored removed auth: %v", got)
	}
}

func TestCodeBuddyModelDiscoveryHonorsCacheExpiryAndCredentialChanges(t *testing.T) {
	requests := make(chan string, 3)
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Header.Get("Authorization")
		_, _ = w.Write([]byte(codeBuddyTestModels))
	})
	refreshCodeBuddyTestModels(t, s, auth)
	<-requests
	s.scheduleCodeBuddyModelRefresh(context.Background(), auth)
	s.codeBuddyModelsMu.Lock()
	entry := s.codeBuddyModelsCache[auth.ID]
	if entry.inFlight {
		s.codeBuddyModelsMu.Unlock()
		t.Fatal("fresh cache triggered upstream discovery")
	}
	entry.nextRefresh = time.Time{}
	s.codeBuddyModelsMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.scheduleCodeBuddyModelRefresh(ctx, auth)
	select {
	case <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("expired cache did not refresh")
	}
	auth.Metadata["access_token"] = "refreshed-token"
	updated, errUpdate := s.coreManager.Update(context.Background(), auth)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
	s.scheduleCodeBuddyModelRefresh(ctx, updated)
	select {
	case token := <-requests:
		if token != "Bearer refreshed-token" {
			t.Fatalf("credential refresh reused the old token")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new credential did not trigger discovery")
	}
}

func TestCodeBuddyModelDiscoveryLocalModeUsesEmbeddedCatalog(t *testing.T) {
	codebuddyauth.SetModelDiscoveryEnabled(false)
	t.Cleanup(func() { codebuddyauth.SetModelDiscoveryEnabled(true) })
	s, auth := newCodeBuddyModelTestService(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("local-model mode contacted upstream")
	})
	s.registerModelsForAuth(context.Background(), auth)
	if got := codeBuddyRegisteredIDs(auth.ID); len(got) == 0 || !strings.Contains(strings.Join(got, ","), "auto") {
		t.Fatalf("local-model mode lost the embedded catalog: %v", got)
	}
}

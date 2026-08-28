package codebuddy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"golang.org/x/sync/singleflight"
)

func TestAPIBaseURLForDomain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		domain string
		want   string
	}{
		{domain: "", want: DefaultCNAPIBase},
		{domain: "www.codebuddy.cn", want: DefaultCNAPIBase},
		{domain: "https://codebuddy.cn/", want: DefaultCNAPIBase},
		{domain: "copilot.tencent.com", want: DefaultCNAPIBase},
		{domain: "www.codebuddy.ai", want: DefaultIntlAPIBase},
		{domain: "https://www.codebuddy.ai", want: DefaultIntlAPIBase},
		{domain: "tencent.sso.copilot.tencent.com", want: "https://tencent.sso.copilot.tencent.com"},
	}
	for _, tt := range tests {
		if got := APIBaseURLForDomain(tt.domain); got != tt.want {
			t.Errorf("APIBaseURLForDomain(%q) = %q, want %q", tt.domain, got, tt.want)
		}
	}
}

func TestParseDesktopAuthFile(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"account": {"uid": "user-1", "nickname": "Arc", "enterpriseId": "ent-9", "phoneNumber": "should-not-matter"},
		"auth": {
			"accessToken": "access-token",
			"refreshToken": "refresh-token",
			"tokenType": "Bearer",
			"expiresAt": 1793098372631,
			"domain": "www.codebuddy.cn"
		}
	}`)
	bundle, err := parseDesktopAuthFile(raw)
	if err != nil {
		t.Fatalf("parseDesktopAuthFile: %v", err)
	}
	if bundle.TokenData.AccessToken != "access-token" {
		t.Fatalf("access token = %q", bundle.TokenData.AccessToken)
	}
	if bundle.TokenData.RefreshToken != "refresh-token" {
		t.Fatalf("refresh token = %q", bundle.TokenData.RefreshToken)
	}
	if bundle.Account.UID != "user-1" || bundle.Account.Nickname != "Arc" {
		t.Fatalf("account = %+v", bundle.Account)
	}
	if bundle.Account.EnterpriseID != "ent-9" {
		t.Fatalf("enterprise id = %q", bundle.Account.EnterpriseID)
	}
	if bundle.APIBase != DefaultCNAPIBase {
		t.Fatalf("api base = %q", bundle.APIBase)
	}
	if bundle.TokenData.ExpiresAt != 1793098372 {
		t.Fatalf("expiresAt unix = %d, want 1793098372", bundle.TokenData.ExpiresAt)
	}
}

func TestImportLocalCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "Tencent-Cloud.coding-copilot.info")
	payload := map[string]any{
		"account": map[string]any{"uid": "uid-local", "nickname": "LocalUser"},
		"auth": map[string]any{
			"accessToken":  "local-access",
			"refreshToken": "local-refresh",
			"domain":       "www.codebuddy.cn",
			"expiresIn":    3600,
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if errWrite := os.WriteFile(path, data, 0600); errWrite != nil {
		t.Fatalf("write: %v", errWrite)
	}

	prev := localAuthDirFn
	localAuthDirFn = func() []string { return []string{dir} }
	t.Cleanup(func() { localAuthDirFn = prev })

	bundle, found, errImport := ImportLocalCredentials()
	if errImport != nil {
		t.Fatalf("ImportLocalCredentials: %v", errImport)
	}
	if found != path {
		t.Fatalf("found = %q, want %q", found, path)
	}
	if bundle.TokenData.AccessToken != "local-access" {
		t.Fatalf("access = %q", bundle.TokenData.AccessToken)
	}
	if bundle.Account.UID != "uid-local" {
		t.Fatalf("uid = %q", bundle.Account.UID)
	}
}

func TestFetchAuthStateAndRefresh(t *testing.T) {
	var refreshCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v2/plugin/auth/state"):
			if r.Header.Get("X-No-Authorization") != "true" {
				t.Errorf("expected X-No-Authorization on state request")
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"authUrl":"https://www.codebuddy.cn/login?x=1","state":"st-1"}}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/v2/plugin/auth/token"):
			_, _ = w.Write([]byte(`{"code":0,"data":{"accessToken":"new-access","refreshToken":"new-refresh","expiresIn":3600,"tokenType":"Bearer"}}`))
		case r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/v2/plugin/auth/token/refresh"):
			atomic.AddInt32(&refreshCalls, 1)
			if r.Header.Get("X-Refresh-Token") != "old-refresh" {
				t.Errorf("X-Refresh-Token = %q", r.Header.Get("X-Refresh-Token"))
			}
			if r.Header.Get("X-Auth-Refresh-Source") != "plugin" {
				t.Errorf("X-Auth-Refresh-Source = %q", r.Header.Get("X-Auth-Refresh-Source"))
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"accessToken":"rotated-access","refreshToken":"rotated-refresh","expiresIn":120}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	client := NewClient(&config.Config{}, server.URL, "direct")
	client.httpClient = server.Client()

	state, errState := client.FetchAuthState(context.Background())
	if errState != nil {
		t.Fatalf("FetchAuthState: %v", errState)
	}
	if state.State != "st-1" || !strings.Contains(state.AuthURL, "codebuddy.cn") {
		t.Fatalf("state = %+v", state)
	}

	token, errRefresh := client.RefreshToken(context.Background(), &TokenData{RefreshToken: "old-refresh", AccessToken: "old"}, &AccountData{UID: "u1"})
	if errRefresh != nil {
		t.Fatalf("RefreshToken: %v", errRefresh)
	}
	if token.AccessToken != "rotated-access" || token.RefreshToken != "rotated-refresh" {
		t.Fatalf("token = %+v", token)
	}
	if atomic.LoadInt32(&refreshCalls) != 1 {
		t.Fatalf("refresh calls = %d", refreshCalls)
	}
}

func TestRefreshTokenDeduplicatesConcurrentCalls(t *testing.T) {
	refreshGroup = singleflight.Group{}
	t.Cleanup(func() { refreshGroup = singleflight.Group{} })

	var calls int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/token/refresh") {
			http.NotFound(w, r)
			return
		}
		atomic.AddInt32(&calls, 1)
		once.Do(func() { close(started) })
		<-release
		_, _ = io.WriteString(w, `{"code":0,"data":{"accessToken":"a","refreshToken":"b","expiresIn":60}}`)
	}))
	t.Cleanup(server.Close)

	clientA := NewClient(&config.Config{}, server.URL, "direct")
	clientA.httpClient = server.Client()
	clientB := NewClient(&config.Config{}, server.URL, "direct")
	clientB.httpClient = server.Client()

	current := &TokenData{RefreshToken: "shared"}
	errs := make(chan error, 2)
	go func() {
		_, err := clientA.RefreshToken(context.Background(), current, nil)
		errs <- err
	}()
	<-started
	go func() {
		_, err := clientB.RefreshToken(context.Background(), current, nil)
		errs <- err
	}()
	time.Sleep(20 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("expected 1 refresh call, got %d", got)
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}
}

func TestCreateTokenStorage(t *testing.T) {
	t.Parallel()
	auth := NewAuth(&config.Config{})
	storage := auth.CreateTokenStorage(&AuthBundle{
		TokenData: &TokenData{
			AccessToken:  "a",
			RefreshToken: "r",
			TokenType:    "Bearer",
			ExpiresAt:    time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Unix(),
			Domain:       "www.codebuddy.cn",
		},
		Account: &AccountData{UID: "uid-2", Nickname: "N"},
		APIBase: DefaultCNAPIBase,
	})
	if storage.Type != ProviderName {
		t.Fatalf("type = %q", storage.Type)
	}
	if storage.UID != "uid-2" || storage.AccessToken != "a" {
		t.Fatalf("storage = %+v", storage)
	}
	if storage.Expired != "2026-01-02T03:04:05Z" {
		t.Fatalf("expired = %q", storage.Expired)
	}
}

func TestIsPendingEnvelopeLoginInProgress(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "login in progress", body: `{"code":11217,"msg":"11217:login ing...","requestId":"r-1"}`, want: true},
		{name: "login expired", body: `{"code":11200,"msg":"11200:login expired","requestId":"r-2"}`, want: false},
		{name: "empty data", body: `{"code":0,"data":{}}`, want: true},
	}
	for _, tt := range tests {
		if got := isPendingEnvelope([]byte(tt.body)); got != tt.want {
			t.Errorf("%s: isPendingEnvelope = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestPollTokenRetriesWhileLoginPending(t *testing.T) {
	var polls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/v2/plugin/auth/token") {
			http.NotFound(w, r)
			return
		}
		if atomic.AddInt32(&polls, 1) == 1 {
			_, _ = io.WriteString(w, `{"code":11217,"msg":"11217:login ing...","requestId":"r-1"}`)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"accessToken":"access-after-login","refreshToken":"refresh-after-login","expiresIn":3600,"tokenType":"Bearer"}}`)
	}))
	t.Cleanup(server.Close)

	client := NewClient(&config.Config{}, server.URL, "direct")
	client.httpClient = server.Client()

	token, errPoll := client.PollToken(context.Background(), "st-1")
	if errPoll != nil {
		t.Fatalf("PollToken: %v", errPoll)
	}
	if token.AccessToken != "access-after-login" {
		t.Fatalf("access token = %q", token.AccessToken)
	}
	if got := atomic.LoadInt32(&polls); got != 2 {
		t.Fatalf("polls = %d, want 2", got)
	}
}

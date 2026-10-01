package codebuddy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/singleflight"
)

const (
	defaultPollInterval = 2 * time.Second
	maxPollDuration     = 15 * time.Minute
	authHTTPTimeout     = 30 * time.Second
)

var refreshGroup singleflight.Group

// Auth handles CodeBuddy login and token refresh.
type Auth struct {
	client *Client
	cfg    *config.Config
}

// Client talks to CodeBuddy plugin auth endpoints.
type Client struct {
	httpClient *http.Client
	cfg        *config.Config
	baseURL    string
}

// NewAuth creates a CodeBuddy auth service.
func NewAuth(cfg *config.Config) *Auth {
	return &Auth{
		client: NewClient(cfg, "", ""),
		cfg:    cfg,
	}
}

// NewClient builds an HTTP client for CodeBuddy auth endpoints.
// proxyURL overrides cfg.ProxyURL when non-empty.
func NewClient(cfg *config.Config, baseURL, proxyURL string) *Client {
	httpClient := &http.Client{Timeout: authHTTPTimeout}
	effectiveProxyURL := strings.TrimSpace(proxyURL)
	var sdkCfg config.SDKConfig
	if cfg != nil {
		sdkCfg = cfg.SDKConfig
		if effectiveProxyURL == "" {
			effectiveProxyURL = strings.TrimSpace(cfg.ProxyURL)
		}
	}
	sdkCfg.ProxyURL = effectiveProxyURL
	httpClient = util.SetProxy(&sdkCfg, httpClient)

	resolvedBase := strings.TrimSpace(baseURL)
	if resolvedBase == "" {
		resolvedBase = DefaultCNAPIBase
	}
	return &Client{
		httpClient: httpClient,
		cfg:        cfg,
		baseURL:    strings.TrimRight(resolvedBase, "/"),
	}
}

// WithBaseURL returns a copy of the client pointed at apiBase.
func (c *Client) WithBaseURL(apiBase string) *Client {
	if c == nil {
		return NewClient(nil, apiBase, "")
	}
	next := *c
	if strings.TrimSpace(apiBase) != "" {
		next.baseURL = strings.TrimRight(strings.TrimSpace(apiBase), "/")
	}
	return &next
}

// CreateTokenStorage converts a login bundle into persistent token storage.
func (a *Auth) CreateTokenStorage(bundle *AuthBundle) *TokenStorage {
	if bundle == nil || bundle.TokenData == nil {
		return &TokenStorage{Type: ProviderName}
	}
	expired := ""
	if bundle.TokenData.ExpiresAt > 0 {
		expired = time.Unix(bundle.TokenData.ExpiresAt, 0).UTC().Format(time.RFC3339)
	}
	account := bundle.Account
	if account == nil {
		account = &AccountData{}
	}
	apiBase := strings.TrimSpace(bundle.APIBase)
	if apiBase == "" {
		apiBase = APIBaseURLForDomain(bundle.TokenData.Domain)
	}
	return &TokenStorage{
		AccessToken:  bundle.TokenData.AccessToken,
		RefreshToken: bundle.TokenData.RefreshToken,
		TokenType:    bundle.TokenData.TokenType,
		Scope:        bundle.TokenData.Scope,
		Expired:      expired,
		UID:          account.UID,
		Nickname:     account.Nickname,
		EnterpriseID: account.EnterpriseID,
		Domain:       bundle.TokenData.Domain,
		APIBaseURL:   apiBase,
		Type:         ProviderName,
	}
}

// StartLogin requests a browser authorization URL from CodeBuddy.
func (a *Auth) StartLogin(ctx context.Context) (*AuthState, error) {
	if a == nil || a.client == nil {
		return nil, fmt.Errorf("codebuddy: auth client is nil")
	}
	return a.client.FetchAuthState(ctx)
}

// WaitForLogin polls until the browser login produces a token, then loads the account.
func (a *Auth) WaitForLogin(ctx context.Context, state *AuthState) (*AuthBundle, error) {
	if a == nil || a.client == nil {
		return nil, fmt.Errorf("codebuddy: auth client is nil")
	}
	if state == nil || strings.TrimSpace(state.State) == "" {
		return nil, fmt.Errorf("codebuddy: auth state is required")
	}
	token, err := a.client.PollToken(ctx, state.State)
	if err != nil {
		return nil, err
	}
	account, errAccount := a.client.FetchAccount(ctx, state.State, token)
	if errAccount != nil {
		log.Warnf("codebuddy: fetch account after login failed: %v", errAccount)
		account = &AccountData{}
	}
	return &AuthBundle{
		TokenData: token,
		Account:   account,
		APIBase:   a.client.baseURL,
	}, nil
}

// RefreshToken exchanges a refresh token for a new access token.
func (c *Client) RefreshToken(ctx context.Context, current *TokenData, account *AccountData) (*TokenData, error) {
	if c == nil {
		return nil, fmt.Errorf("codebuddy: client is nil")
	}
	if current == nil || strings.TrimSpace(current.RefreshToken) == "" {
		return nil, fmt.Errorf("codebuddy: refresh token is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	refreshToken := strings.TrimSpace(current.RefreshToken)
	result, err, _ := refreshGroup.Do(refreshToken, func() (interface{}, error) {
		return c.refreshTokenSingleFlight(context.WithoutCancel(ctx), current, account)
	})
	if err != nil {
		return nil, err
	}
	tokenData, ok := result.(*TokenData)
	if !ok || tokenData == nil {
		return nil, fmt.Errorf("codebuddy: refresh token failed: invalid single-flight result")
	}
	return tokenData, nil
}

func (c *Client) refreshTokenSingleFlight(ctx context.Context, current *TokenData, account *AccountData) (*TokenData, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, PluginAuthURL(c.baseURL, "/auth/token/refresh"), bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, fmt.Errorf("codebuddy: failed to create refresh request: %w", err)
	}
	applyJSONHeaders(req)
	applyIdentityHeaders(req, current, account)
	req.Header.Set("X-Refresh-Token", strings.TrimSpace(current.RefreshToken))
	req.Header.Set("X-Auth-Refresh-Source", "plugin")

	body, status, errDo := c.do(req)
	if errDo != nil {
		return nil, fmt.Errorf("codebuddy: refresh request failed: %w", errDo)
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return nil, fmt.Errorf("codebuddy: refresh token rejected (status %d)", status)
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("codebuddy: refresh failed with status %d: %s", status, summarizeBody(body))
	}
	data, errData := unwrapData(body)
	if errData != nil {
		return nil, errData
	}
	token := parseTokenData(data)
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil, fmt.Errorf("codebuddy: empty access token in refresh response")
	}
	if token.RefreshToken == "" {
		token.RefreshToken = current.RefreshToken
	}
	if token.Domain == "" {
		token.Domain = current.Domain
	}
	if token.TokenType == "" {
		token.TokenType = current.TokenType
	}
	return token, nil
}

// FetchAuthState starts the CLI-external-link login flow.
func (c *Client) FetchAuthState(ctx context.Context) (*AuthState, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	endpoint := PluginAuthURL(c.baseURL, "/auth/state") + "?platform=" + url.QueryEscape(loginPlatform())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte("{}")))
	if err != nil {
		return nil, fmt.Errorf("codebuddy: failed to create auth state request: %w", err)
	}
	applyJSONHeaders(req)
	applyNoAuthHeaders(req)

	body, status, errDo := c.do(req)
	if errDo != nil {
		return nil, fmt.Errorf("codebuddy: auth state request failed: %w", errDo)
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("codebuddy: auth state failed with status %d: %s", status, summarizeBody(body))
	}
	data, errData := unwrapData(body)
	if errData != nil {
		return nil, errData
	}
	state := &AuthState{
		AuthURL: gjsonString(data, "authUrl", "auth_url", "url", "verificationUri", "verification_uri"),
		State:   gjsonString(data, "state"),
	}
	if state.AuthURL == "" || state.State == "" {
		return nil, fmt.Errorf("codebuddy: auth state response missing url or state")
	}
	return state, nil
}

// PollToken waits until /auth/token returns an access token for state.
func (c *Client) PollToken(ctx context.Context, state string) (*TokenData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	state = strings.TrimSpace(state)
	if state == "" {
		return nil, fmt.Errorf("codebuddy: state is required")
	}
	deadline := time.Now().Add(maxPollDuration)
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("codebuddy: context cancelled: %w", ctx.Err())
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("codebuddy: login timed out")
			}
			token, pending, errPoll := c.fetchToken(ctx, state)
			if errPoll != nil {
				return nil, errPoll
			}
			if pending {
				continue
			}
			if token == nil || strings.TrimSpace(token.AccessToken) == "" {
				continue
			}
			return token, nil
		}
	}
}

func (c *Client) fetchToken(ctx context.Context, state string) (*TokenData, bool, error) {
	endpoint := PluginAuthURL(c.baseURL, "/auth/token") + "?state=" + url.QueryEscape(state)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, false, fmt.Errorf("codebuddy: failed to create token poll request: %w", err)
	}
	applyJSONHeaders(req)
	applyNoAuthHeaders(req)

	body, status, errDo := c.do(req)
	if errDo != nil {
		return nil, false, fmt.Errorf("codebuddy: token poll failed: %w", errDo)
	}
	if status == http.StatusNotFound || status == http.StatusAccepted || status == http.StatusNoContent {
		return nil, true, nil
	}
	if status < 200 || status >= 300 {
		if isRetryableAuthStatus(status, body) {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("codebuddy: token poll failed with status %d: %s", status, summarizeBody(body))
	}
	data, errData := unwrapData(body)
	if errData != nil {
		if isPendingEnvelope(body) {
			return nil, true, nil
		}
		return nil, false, errData
	}
	if !data.Exists() || data.Type == gjson.Null {
		return nil, true, nil
	}
	token := parseTokenData(data)
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil, true, nil
	}
	return token, false, nil
}

// FetchAccount loads the signed-in account for a completed login state.
func (c *Client) FetchAccount(ctx context.Context, state string, token *TokenData) (*AccountData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	endpoint := PluginAuthURL(c.baseURL, "/login/account") + "?state=" + url.QueryEscape(state)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("codebuddy: failed to create account request: %w", err)
	}
	applyJSONHeaders(req)
	applyIdentityHeaders(req, token, nil)
	req.Header.Set("X-No-User-Id", "true")
	req.Header.Set("X-No-Enterprise-Id", "true")
	req.Header.Set("X-No-Department-Info", "true")

	body, status, errDo := c.do(req)
	if errDo != nil {
		return nil, fmt.Errorf("codebuddy: account request failed: %w", errDo)
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("codebuddy: account request failed with status %d: %s", status, summarizeBody(body))
	}
	data, errData := unwrapData(body)
	if errData != nil {
		return nil, errData
	}
	return parseAccountData(data), nil
}

func (c *Client) do(req *http.Request) ([]byte, int, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Errorf("codebuddy auth: close response body error: %v", errClose)
		}
	}()
	body, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response: %w", errRead)
	}
	return body, resp.StatusCode, nil
}

func applyJSONHeaders(req *http.Request) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "CLIProxyAPI")
}

func applyNoAuthHeaders(req *http.Request) {
	req.Header.Set("X-No-Authorization", "true")
	req.Header.Set("X-No-User-Id", "true")
	req.Header.Set("X-No-Enterprise-Id", "true")
	req.Header.Set("X-No-Department-Info", "true")
}

func applyIdentityHeaders(req *http.Request, token *TokenData, account *AccountData) {
	if token != nil && strings.TrimSpace(token.AccessToken) != "" {
		tokenType := strings.TrimSpace(token.TokenType)
		if tokenType == "" {
			tokenType = "Bearer"
		}
		req.Header.Set("Authorization", tokenType+" "+strings.TrimSpace(token.AccessToken))
	}
	if token != nil && strings.TrimSpace(token.Domain) != "" {
		req.Header.Set("X-Domain", strings.TrimSpace(token.Domain))
	}
	if account != nil && strings.TrimSpace(account.UID) != "" {
		req.Header.Set("X-User-Id", strings.TrimSpace(account.UID))
	}
	if account != nil && strings.TrimSpace(account.EnterpriseID) != "" {
		req.Header.Set("X-Enterprise-Id", strings.TrimSpace(account.EnterpriseID))
		req.Header.Set("X-Tenant-Id", strings.TrimSpace(account.EnterpriseID))
	}
}

func unwrapData(body []byte) (gjson.Result, error) {
	if !gjson.ValidBytes(body) {
		return gjson.Result{}, fmt.Errorf("codebuddy: invalid JSON response")
	}
	root := gjson.ParseBytes(body)
	if code := root.Get("code"); code.Exists() {
		if code.Int() != 0 {
			msg := firstNonEmpty(root.Get("msg").String(), root.Get("message").String(), summarizeBody(body))
			return gjson.Result{}, fmt.Errorf("codebuddy: %s", msg)
		}
		data := root.Get("data")
		if data.Exists() {
			return data, nil
		}
	}
	return root, nil
}

// authPendingCodes lists business codes that CodeBuddy returns with HTTP 200 while the
// browser login is still in progress. Polling must keep waiting instead of failing.
var authPendingCodes = map[int64]struct{}{
	11217: {}, // "11217:login ing..."
}

func isPendingEnvelope(body []byte) bool {
	if !gjson.ValidBytes(body) {
		return false
	}
	root := gjson.ParseBytes(body)
	code := root.Get("code")
	if !code.Exists() {
		return false
	}
	switch code.Int() {
	case 0:
		data := root.Get("data")
		return !data.Exists() || data.Type == gjson.Null || (data.IsObject() && len(data.Map()) == 0)
	default:
		if _, ok := authPendingCodes[code.Int()]; ok {
			return true
		}
		msg := strings.ToLower(firstNonEmpty(root.Get("msg").String(), root.Get("message").String()))
		return strings.Contains(msg, "pending") || strings.Contains(msg, "retry") || strings.Contains(msg, "not ready")
	}
}

func isRetryableAuthStatus(status int, body []byte) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	msg := strings.ToLower(summarizeBody(body))
	return strings.Contains(msg, "pending") || strings.Contains(msg, "retry") || strings.Contains(msg, "not ready")
}

func summarizeBody(body []byte) string {
	s := strings.TrimSpace(string(body))
	if len(s) > 300 {
		return s[:300]
	}
	return s
}

func loginPlatform() string {
	switch runtime.GOOS {
	case "darwin":
		return "macOS"
	case "windows":
		return "Windows"
	default:
		return "Linux"
	}
}

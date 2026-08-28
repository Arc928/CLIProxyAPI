package auth

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/browser"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var codebuddyRefreshLead = 5 * time.Minute

// CodeBuddyAuthenticator implements login for Tencent CodeBuddy subscription sessions.
type CodeBuddyAuthenticator struct{}

// NewCodeBuddyAuthenticator constructs a CodeBuddy authenticator.
func NewCodeBuddyAuthenticator() Authenticator {
	return &CodeBuddyAuthenticator{}
}

// Provider returns the provider key for CodeBuddy.
func (CodeBuddyAuthenticator) Provider() string {
	return codebuddy.ProviderName
}

// RefreshLead returns the duration before token expiry when refresh should occur.
func (CodeBuddyAuthenticator) RefreshLead() *time.Duration {
	return &codebuddyRefreshLead
}

// Login imports local desktop credentials when present, otherwise starts browser login.
func (a CodeBuddyAuthenticator) Login(ctx context.Context, cfg *config.Config, opts *LoginOptions) (*coreauth.Auth, error) {
	if cfg == nil {
		return nil, fmt.Errorf("cliproxy auth: configuration is required")
	}
	if opts == nil {
		opts = &LoginOptions{}
	}

	authSvc := codebuddy.NewAuth(cfg)
	forceOAuth := strings.EqualFold(strings.TrimSpace(opts.Metadata["oauth"]), "1") ||
		strings.EqualFold(strings.TrimSpace(opts.Metadata["oauth"]), "true")

	var bundle *codebuddy.AuthBundle
	if !forceOAuth {
		imported, sourcePath, errImport := codebuddy.ImportLocalCredentials()
		if errImport == nil && imported != nil && imported.TokenData != nil {
			bundle = imported
			fmt.Printf("Imported CodeBuddy desktop credentials from %s\n", sourcePath)
		} else if errImport != nil {
			log.Debugf("codebuddy: local credential import skipped: %v", errImport)
		}
	}

	if bundle == nil {
		fmt.Println("Starting CodeBuddy authentication...")
		state, errState := authSvc.StartLogin(ctx)
		if errState != nil {
			return nil, fmt.Errorf("codebuddy: failed to start login: %w", errState)
		}
		fmt.Printf("\nTo authenticate, please visit:\n%s\n\n", state.AuthURL)
		if !opts.NoBrowser {
			if browser.IsAvailable() {
				if errOpen := browser.OpenURL(state.AuthURL); errOpen != nil {
					log.Warnf("Failed to open browser automatically: %v", errOpen)
				} else {
					fmt.Println("Browser opened automatically.")
				}
			}
		}
		fmt.Println("Waiting for authorization...")
		var errWait error
		bundle, errWait = authSvc.WaitForLogin(ctx, state)
		if errWait != nil {
			return nil, fmt.Errorf("codebuddy: %w", errWait)
		}
	}

	tokenStorage := authSvc.CreateTokenStorage(bundle)
	metadata := codeBuddyMetadata(bundle)
	fileName := codeBuddyFileName(bundle)
	label := "CodeBuddy User"
	if bundle != nil && bundle.Account != nil && strings.TrimSpace(bundle.Account.Nickname) != "" {
		label = strings.TrimSpace(bundle.Account.Nickname)
	} else if bundle != nil && bundle.Account != nil && strings.TrimSpace(bundle.Account.UID) != "" {
		label = strings.TrimSpace(bundle.Account.UID)
	}

	fmt.Println("\nCodeBuddy authentication successful!")

	return &coreauth.Auth{
		ID:       fileName,
		Provider: a.Provider(),
		FileName: fileName,
		Label:    label,
		Storage:  tokenStorage,
		Metadata: metadata,
	}, nil
}

func codeBuddyMetadata(bundle *codebuddy.AuthBundle) map[string]any {
	metadata := map[string]any{
		"type":      codebuddy.ProviderName,
		"timestamp": time.Now().UnixMilli(),
	}
	if bundle == nil {
		return metadata
	}
	if bundle.TokenData != nil {
		metadata["access_token"] = bundle.TokenData.AccessToken
		metadata["refresh_token"] = bundle.TokenData.RefreshToken
		metadata["token_type"] = bundle.TokenData.TokenType
		metadata["scope"] = bundle.TokenData.Scope
		metadata["domain"] = bundle.TokenData.Domain
		if bundle.TokenData.ExpiresAt > 0 {
			metadata["expired"] = time.Unix(bundle.TokenData.ExpiresAt, 0).UTC().Format(time.RFC3339)
		}
	}
	if bundle.Account != nil {
		metadata["uid"] = bundle.Account.UID
		metadata["nickname"] = bundle.Account.Nickname
		metadata["enterprise_id"] = bundle.Account.EnterpriseID
	}
	apiBase := strings.TrimSpace(bundle.APIBase)
	if apiBase == "" && bundle.TokenData != nil {
		apiBase = codebuddy.APIBaseURLForDomain(bundle.TokenData.Domain)
	}
	if apiBase != "" {
		metadata["api_base_url"] = apiBase
	}
	return metadata
}

func codeBuddyFileName(bundle *codebuddy.AuthBundle) string {
	if bundle != nil && bundle.Account != nil {
		if uid := strings.TrimSpace(bundle.Account.UID); uid != "" {
			return fmt.Sprintf("codebuddy-%s.json", uid)
		}
	}
	return fmt.Sprintf("codebuddy-%d.json", time.Now().UnixMilli())
}

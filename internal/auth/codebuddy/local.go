package codebuddy

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

// localAuthDirFn is overridable in tests.
var localAuthDirFn = defaultLocalAuthDirs

func defaultLocalAuthDirs() []string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return nil
	}
	switch runtime.GOOS {
	case "darwin":
		return []string{
			filepath.Join(home, "Library", "Application Support", "CodeBuddyExtension", "Data", "Public", "auth"),
		}
	case "windows":
		localAppData := strings.TrimSpace(os.Getenv("LOCALAPPDATA"))
		if localAppData == "" {
			localAppData = filepath.Join(home, "AppData", "Local")
		}
		return []string{
			filepath.Join(localAppData, "CodeBuddyExtension", "Data", "Public", "auth"),
		}
	default:
		xdg := strings.TrimSpace(os.Getenv("XDG_DATA_HOME"))
		if xdg == "" {
			xdg = filepath.Join(home, ".local", "share")
		}
		return []string{
			filepath.Join(xdg, "CodeBuddyExtension", "Data", "Public", "auth"),
		}
	}
}

// ImportLocalCredentials loads a desktop CodeBuddy login file if one exists.
func ImportLocalCredentials() (*AuthBundle, string, error) {
	path, err := findLocalAuthFile()
	if err != nil {
		return nil, "", err
	}
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		return nil, path, fmt.Errorf("codebuddy: read local auth file: %w", errRead)
	}
	bundle, errParse := parseDesktopAuthFile(raw)
	if errParse != nil {
		return nil, path, errParse
	}
	return bundle, path, nil
}

func findLocalAuthFile() (string, error) {
	var candidates []string
	for _, dir := range localAuthDirFn() {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if !strings.HasSuffix(strings.ToLower(name), ".info") && !strings.HasSuffix(strings.ToLower(name), ".json") {
				continue
			}
			candidates = append(candidates, filepath.Join(dir, name))
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("codebuddy: no local desktop credentials found")
	}
	best := candidates[0]
	bestMTime := fileModTime(best)
	for _, path := range candidates[1:] {
		mt := fileModTime(path)
		if mt.After(bestMTime) {
			best = path
			bestMTime = mt
		}
	}
	return best, nil
}

func fileModTime(path string) time.Time {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return info.ModTime()
}

func parseDesktopAuthFile(raw []byte) (*AuthBundle, error) {
	if !gjson.ValidBytes(raw) {
		return nil, fmt.Errorf("codebuddy: local auth file is not valid JSON")
	}
	root := gjson.ParseBytes(raw)
	authNode := root.Get("auth")
	if !authNode.Exists() {
		authNode = root
	}
	token := parseTokenData(authNode)
	if strings.TrimSpace(token.AccessToken) == "" {
		return nil, fmt.Errorf("codebuddy: local auth file is missing access token")
	}
	accountNode := root.Get("account")
	account := parseAccountData(accountNode)
	if account.UID == "" {
		account.UID = firstNonEmpty(
			gjsonString(root, "uid", "user_id", "userId"),
		)
	}
	if account.Nickname == "" {
		account.Nickname = firstNonEmpty(
			gjsonString(root, "nickname", "name"),
		)
	}
	apiBase := APIBaseURLForDomain(token.Domain)
	return &AuthBundle{
		TokenData: token,
		Account:   account,
		APIBase:   apiBase,
	}, nil
}

func parseTokenData(node gjson.Result) *TokenData {
	token := &TokenData{
		AccessToken:  gjsonString(node, "accessToken", "access_token"),
		RefreshToken: gjsonString(node, "refreshToken", "refresh_token"),
		TokenType:    gjsonString(node, "tokenType", "token_type"),
		Scope:        gjsonString(node, "scope"),
		Domain:       gjsonString(node, "domain"),
	}
	if token.TokenType == "" {
		token.TokenType = "Bearer"
	}
	token.ExpiresAt = parseExpiresAtUnix(node)
	return token
}

func parseAccountData(node gjson.Result) *AccountData {
	if !node.Exists() {
		return &AccountData{}
	}
	return &AccountData{
		UID:          gjsonString(node, "uid", "userId", "user_id", "id"),
		Nickname:     gjsonString(node, "nickname", "name", "nickName"),
		EnterpriseID: gjsonString(node, "enterpriseId", "enterprise_id", "enterpriseID"),
	}
}

func parseExpiresAtUnix(node gjson.Result) int64 {
	if v := node.Get("expiresAt"); v.Exists() {
		return unixFromPossiblyMillis(v.Int())
	}
	if v := node.Get("expires_at"); v.Exists() {
		return unixFromPossiblyMillis(v.Int())
	}
	expiresIn := node.Get("expiresIn")
	if !expiresIn.Exists() {
		expiresIn = node.Get("expires_in")
	}
	if expiresIn.Exists() && expiresIn.Int() > 0 {
		return time.Now().Unix() + expiresIn.Int()
	}
	return 0
}

func unixFromPossiblyMillis(raw int64) int64 {
	if raw <= 0 {
		return 0
	}
	// Desktop files store expiresAt in milliseconds.
	if raw > 1_000_000_000_000 {
		return raw / 1000
	}
	return raw
}

func gjsonString(node gjson.Result, keys ...string) string {
	for _, key := range keys {
		v := node.Get(key)
		if v.Exists() {
			s := strings.TrimSpace(v.String())
			if s != "" && s != "null" {
				return s
			}
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

package helps

import (
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodeBuddy's gateway fingerprints Claude Code-derived prompts and rejects the
// request with business code 11128 ("Illegal API invocation from an unapproved
// channel"). Observed triggers (case-insensitive, system/developer/assistant
// only; user and tool content is not inspected by the gateway):
//
//  1. the gitStatus line "Main branch (you will usually use this for PRs"
//  2. "You are Claude Code, Anthropic's official CLI for Claude"
//
// Replacements are semantically equivalent so model behavior stays the same,
// and they are deterministic so upstream prompt-cache keys stay stable.
var codeBuddyPromptReplacements = []struct {
	old string
	new string
}{
	{
		old: "Main branch (you will usually use this for PRs)",
		new: "Main branch (used for PRs)",
	},
	{
		old: "You are Claude Code, Anthropic's official CLI for Claude",
		new: "You are Claude Code, the official CLI for Claude",
	},
}

// SanitizeCodeBuddySystemPrompts rewrites fingerprint-triggering phrases in
// system, developer, and assistant messages of an OpenAI chat payload.
func SanitizeCodeBuddySystemPrompts(payload []byte) []byte {
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return payload
	}
	messages := gjson.GetBytes(payload, "messages")
	if !messages.Exists() || !messages.IsArray() {
		return payload
	}
	out := payload
	changed := false
	messageIndex := 0
	messages.ForEach(func(_, message gjson.Result) bool {
		if codeBuddySkipSanitizeRole(message.Get("role").String()) {
			messageIndex++
			return true
		}
		var did bool
		out, did = rewriteCodeBuddyJSONStrings(out, fmt.Sprintf("messages.%d", messageIndex), message)
		if did {
			changed = true
		}
		messageIndex++
		return true
	})
	if !changed {
		return payload
	}
	return out
}

func codeBuddySkipSanitizeRole(role string) bool {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "user", "tool":
		return true
	default:
		return false
	}
}

// rewriteCodeBuddyJSONStrings rewrites every string leaf under node.
func rewriteCodeBuddyJSONStrings(out []byte, path string, node gjson.Result) ([]byte, bool) {
	switch {
	case node.Type == gjson.String:
		updated := rewriteCodeBuddyText(node.String())
		if updated == node.String() {
			return out, false
		}
		next, errSet := sjson.SetBytes(out, path, updated)
		if errSet != nil {
			return out, false
		}
		return next, true
	case node.IsArray():
		changed := false
		partIndex := 0
		node.ForEach(func(_, part gjson.Result) bool {
			var did bool
			out, did = rewriteCodeBuddyJSONStrings(out, fmt.Sprintf("%s.%d", path, partIndex), part)
			if did {
				changed = true
			}
			partIndex++
			return true
		})
		return out, changed
	case node.IsObject():
		changed := false
		node.ForEach(func(key, value gjson.Result) bool {
			var did bool
			out, did = rewriteCodeBuddyJSONStrings(out, path+"."+key.String(), value)
			if did {
				changed = true
			}
			return true
		})
		return out, changed
	default:
		return out, false
	}
}

// rewriteCodeBuddyText applies every replacement rule to one text value.
func rewriteCodeBuddyText(text string) string {
	for _, rule := range codeBuddyPromptReplacements {
		text = replaceFold(text, rule.old, rule.new)
	}
	return text
}

// replaceFold replaces every case-insensitive occurrence of old with new.
func replaceFold(text, old, new string) string {
	if text == "" || old == "" {
		return text
	}
	lower := strings.ToLower(text)
	oldLower := strings.ToLower(old)
	start := 0
	var b strings.Builder
	for {
		rel := strings.Index(lower[start:], oldLower)
		if rel < 0 {
			if start == 0 {
				return text
			}
			b.WriteString(text[start:])
			return b.String()
		}
		at := start + rel
		if start == 0 {
			b.Grow(len(text) + len(new) - len(old))
		}
		b.WriteString(text[start:at])
		b.WriteString(new)
		start = at + len(old)
	}
}

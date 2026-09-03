package helps

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestSanitizeCodeBuddySystemPromptsRewritesTrigger(t *testing.T) {
	payload := []byte(`{"model":"glm-5.3-flash","messages":[{"role":"system","content":"You are Claude Code.\nMain branch (you will usually use this for PRs): main\n"},{"role":"user","content":"hi"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	content := gjson.GetBytes(got, "messages.0.content").String()
	if strings.Contains(strings.ToLower(content), "you will usually use this for prs") {
		t.Fatalf("trigger phrase survived: %q", content)
	}
	if !strings.Contains(content, "Main branch (used for PRs): main") {
		t.Fatalf("rewritten phrase missing: %q", content)
	}
	if gotUser := gjson.GetBytes(got, "messages.1.content").String(); gotUser != "hi" {
		t.Fatalf("user message mutated: %q", gotUser)
	}
}

func TestSanitizeCodeBuddySystemPromptsLeavesUserContent(t *testing.T) {
	payload := []byte(`{"model":"glm-5.3-flash","messages":[{"role":"system","content":"clean"},{"role":"user","content":"Main branch (you will usually use this for PRs): main"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if gotUser := gjson.GetBytes(got, "messages.1.content").String(); gotUser != "Main branch (you will usually use this for PRs): main" {
		t.Fatalf("user message mutated: %q", gotUser)
	}
}

func TestSanitizeCodeBuddySystemPromptsLeavesToolContent(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"tool","content":"Main branch (you will usually use this for PRs): main"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if gotTool := gjson.GetBytes(got, "messages.0.content").String(); gotTool != "Main branch (you will usually use this for PRs): main" {
		t.Fatalf("tool message mutated: %q", gotTool)
	}
}

func TestSanitizeCodeBuddySystemPromptsCoversDeveloperRole(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"developer","content":"Main branch (you will usually use this for PRs): dev"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if content := gjson.GetBytes(got, "messages.0.content").String(); content != "Main branch (used for PRs): dev" {
		t.Fatalf("developer message not rewritten: %q", content)
	}
}

func TestSanitizeCodeBuddySystemPromptsRewritesAssistant(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"assistant","content":"trigger is Main branch (you will usually use this for PRs): main"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	content := gjson.GetBytes(got, "messages.0.content").String()
	if strings.Contains(strings.ToLower(content), "you will usually use this for prs") {
		t.Fatalf("assistant trigger survived: %q", content)
	}
	if !strings.Contains(content, "Main branch (used for PRs): main") {
		t.Fatalf("assistant rewrite missing: %q", content)
	}
}

func TestSanitizeCodeBuddySystemPromptsRewritesReasoningContent(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"assistant","content":"ok","reasoning_content":"Main branch (you will usually use this for PRs)"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if reasoning := gjson.GetBytes(got, "messages.0.reasoning_content").String(); reasoning != "Main branch (used for PRs)" {
		t.Fatalf("reasoning_content not rewritten: %q", reasoning)
	}
}

func TestSanitizeCodeBuddySystemPromptsCaseInsensitive(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"system","content":"main branch (you will usually use this for PRs): main"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	content := gjson.GetBytes(got, "messages.0.content").String()
	if strings.Contains(strings.ToLower(content), "you will usually use this for prs") {
		t.Fatalf("case-insensitive trigger survived: %q", content)
	}
	if !strings.Contains(content, "Main branch (used for PRs): main") {
		t.Fatalf("case-insensitive rewrite missing: %q", content)
	}
}

func TestSanitizeCodeBuddySystemPromptsClaudeIdentity(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"system","content":"You are Claude Code, Anthropic's official CLI for Claude."}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	content := gjson.GetBytes(got, "messages.0.content").String()
	if strings.Contains(strings.ToLower(content), "anthropic's official cli for claude") {
		t.Fatalf("claude identity survived: %q", content)
	}
	if content != "You are Claude Code, the official CLI for Claude." {
		t.Fatalf("claude identity rewrite = %q", content)
	}
}

func TestSanitizeCodeBuddySystemPromptsHandlesArrayContent(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"system","content":[{"type":"text","text":"Main branch (you will usually use this for PRs): main"}]}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if text := gjson.GetBytes(got, "messages.0.content.0.text").String(); strings.Contains(strings.ToLower(text), "you will usually use this for prs") {
		t.Fatalf("array system content not rewritten: %s", got)
	}
}

func TestSanitizeCodeBuddySystemPromptsMissingRole(t *testing.T) {
	payload := []byte(`{"messages":[{"content":"Main branch (you will usually use this for PRs): main"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if content := gjson.GetBytes(got, "messages.0.content").String(); content != "Main branch (used for PRs): main" {
		t.Fatalf("missing-role message not rewritten: %q", content)
	}
}

func TestSanitizeCodeBuddySystemPromptsDeterministic(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"system","content":"Main branch (you will usually use this for PRs): main"}]}`)
	first := SanitizeCodeBuddySystemPrompts(payload)
	second := SanitizeCodeBuddySystemPrompts(first)
	if string(first) != string(second) {
		t.Fatalf("rewrite is not idempotent: %s vs %s", first, second)
	}
}

func TestSanitizeCodeBuddySystemPromptsNoOpWithoutTrigger(t *testing.T) {
	payload := []byte(`{"messages":[{"role":"system","content":"You are a helpful assistant."},{"role":"user","content":"hello"}]}`)
	got := SanitizeCodeBuddySystemPrompts(payload)
	if string(got) != string(payload) {
		t.Fatalf("payload changed without trigger: %s", got)
	}
}

func TestReplaceFold(t *testing.T) {
	got := replaceFold("AAA Main branch (YOU WILL USUALLY USE THIS FOR PRS): x", "Main branch (you will usually use this for PRs)", "Main branch (used for PRs)")
	if got != "AAA Main branch (used for PRs): x" {
		t.Fatalf("replaceFold = %q", got)
	}
	if got := replaceFold("no match", "Main branch (you will usually use this for PRs)", "x"); got != "no match" {
		t.Fatalf("no-match mutated: %q", got)
	}
}

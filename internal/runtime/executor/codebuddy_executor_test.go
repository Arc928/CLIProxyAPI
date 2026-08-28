package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodeBuddyExecutorRequestToFormat(t *testing.T) {
	executor := NewCodeBuddyExecutor(&config.Config{})
	got := executor.RequestToFormat(cliproxyexecutor.Request{}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude})
	if got != sdktranslator.FormatOpenAI {
		t.Fatalf("RequestToFormat() = %q, want openai", got)
	}
}

func TestAggregateOpenAIChatSSE(t *testing.T) {
	sse := strings.Join([]string{
		`data: {"id":"chatcmpl-1","model":"glm-5.2","choices":[{"index":0,"delta":{"role":"assistant","content":"He"}}]}`,
		`data: {"choices":[{"index":0,"delta":{"content":"llo"}}]}`,
		`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		`data: [DONE]`,
		"",
	}, "\n")
	got, err := aggregateOpenAIChatSSE(strings.NewReader(sse))
	if err != nil {
		t.Fatalf("aggregateOpenAIChatSSE: %v", err)
	}
	if gjson.GetBytes(got, "choices.0.message.content").String() != "Hello" {
		t.Fatalf("content = %s", got)
	}
	if gjson.GetBytes(got, "choices.0.finish_reason").String() != "stop" {
		t.Fatalf("finish_reason = %s", got)
	}
	if gjson.GetBytes(got, "usage.total_tokens").Int() != 5 {
		t.Fatalf("usage = %s", got)
	}
}

func TestCodeBuddyReasoningCoalescer(t *testing.T) {
	var c codeBuddyReasoningCoalescer
	if got := c.Push([]byte(`data: {"choices":[{"delta":{"reasoning_content":"The "}}]}`)); len(got) != 0 {
		t.Fatalf("first reasoning chunk = %v, want buffered", got)
	}
	if got := c.Push([]byte(`data: {"choices":[{"delta":{"reasoning_content":"answer."}}]}`)); len(got) != 0 {
		t.Fatalf("second reasoning chunk = %v, want buffered", got)
	}
	got := c.Push([]byte(`data: {"choices":[{"delta":{"content":"done"}}]}`))
	if len(got) != 2 {
		t.Fatalf("flushed chunks = %d, want 2", len(got))
	}
	if gjson.GetBytes(bytes.TrimSpace(bytes.TrimPrefix(got[0], []byte("data:"))), "choices.0.delta.reasoning_content").String() != "The answer." {
		t.Fatalf("merged reasoning chunk = %s", got[0])
	}
	if !bytes.Contains(got[1], []byte(`"content":"done"`)) {
		t.Fatalf("content chunk = %s", got[1])
	}
}

// Hy4 emits an empty "tool_calls":[] in every delta; it must not block coalescing.
func TestCodeBuddyReasoningCoalescerIgnoresEmptyToolCalls(t *testing.T) {
	var c codeBuddyReasoningCoalescer
	chunk := func(text string) []byte {
		return []byte(`data: {"id":"cmb-1","model":"hy4-preview","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"","reasoning_content":"` + text + `","function_call":null,"refusal":"","tool_calls":[],"extra_fields":null},"logprobs":null,"finish_reason":""}],"usage":null}`)
	}
	if got := c.Push(chunk("We")); len(got) != 0 {
		t.Fatalf("first chunk = %v, want buffered", got)
	}
	if got := c.Push(chunk(" need")); len(got) != 0 {
		t.Fatalf("second chunk = %v, want buffered", got)
	}
	got := c.Flush()
	if len(got) != 1 {
		t.Fatalf("flushed = %d, want 1", len(got))
	}
	if merged := gjson.GetBytes(codeBuddySSEPayload(got[0]), "choices.0.delta.reasoning_content").String(); merged != "We need" {
		t.Fatalf("merged reasoning = %q, want %q", merged, "We need")
	}
}

func TestCodeBuddyReasoningCoalescerFlushesDone(t *testing.T) {
	var c codeBuddyReasoningCoalescer
	_ = c.Push([]byte(`data: {"choices":[{"delta":{"reasoning":"one"}}]}`))
	got := c.Push([]byte("data: [DONE]"))
	if len(got) != 2 || !bytes.Contains(got[0], []byte(`"reasoning":"one"`)) || !bytes.Equal(got[1], []byte("data: [DONE]")) {
		t.Fatalf("done flush = %v", got)
	}
	if got := c.Flush(); len(got) != 0 {
		t.Fatalf("buffer after done = %v", got)
	}
}

func TestCodeBuddyExecutorExecuteInjectsHeaders(t *testing.T) {
	var gotAuth, gotUser, gotDomain, gotURL string
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotUser = r.Header.Get("X-User-Id")
		gotDomain = r.Header.Get("X-Domain")
		body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"id\":\"c1\",\"model\":\"glm-5.2\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)

	auth := &cliproxyauth.Auth{
		ID:       "codebuddy-user-1.json",
		Provider: "codebuddy",
		Metadata: map[string]any{
			"type":         "codebuddy",
			"access_token": "tok-1",
			"uid":          "uid-1",
			"domain":       "www.codebuddy.cn",
			"api_base_url": server.URL,
		},
		Attributes: map[string]string{},
	}
	executor := NewCodeBuddyExecutor(&config.Config{})
	payload := []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`)
	resp, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{Model: "glm-5.2", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAI,
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotURL != "/v2/chat/completions" {
		t.Fatalf("url path = %q", gotURL)
	}
	if gotAuth != "Bearer tok-1" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotUser != "uid-1" {
		t.Fatalf("X-User-Id = %q", gotUser)
	}
	if gotDomain != "www.codebuddy.cn" {
		t.Fatalf("X-Domain = %q", gotDomain)
	}
	if !gjson.GetBytes(body, "stream").Bool() {
		t.Fatalf("expected stream=true, body=%s", body)
	}
	if !bytes.Contains(resp.Payload, []byte("ok")) && gjson.GetBytes(resp.Payload, "choices.0.message.content").String() != "ok" {
		// TranslateNonStream for openai->openai should keep content
		if gjson.GetBytes(resp.Payload, "choices.0.message.content").String() == "" && !strings.Contains(string(resp.Payload), "ok") {
			t.Fatalf("response payload = %s", resp.Payload)
		}
	}
}

func TestCodeBuddyExecutorRefresh(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/auth/token/refresh") {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"code":0,"data":{"accessToken":"new-tok","refreshToken":"new-ref","expiresIn":60}}`)
	}))
	t.Cleanup(server.Close)

	auth := &cliproxyauth.Auth{
		Provider: "codebuddy",
		Metadata: map[string]any{
			"type":          "codebuddy",
			"access_token":  "old",
			"refresh_token": "ref",
			"uid":           "u1",
			"api_base_url":  server.URL,
		},
	}
	executor := NewCodeBuddyExecutor(&config.Config{})
	got, err := executor.Refresh(context.Background(), auth)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got.Metadata["access_token"] != "new-tok" {
		t.Fatalf("access_token = %v", got.Metadata["access_token"])
	}
}

func TestChatCompletionsURL(t *testing.T) {
	if got := codebuddyauth.ChatCompletionsURL("https://copilot.tencent.com/"); got != "https://copilot.tencent.com/v2/chat/completions" {
		t.Fatalf("ChatCompletionsURL = %q", got)
	}
}

func TestCodeBuddyUpstreamError(t *testing.T) {
	err := codeBuddyUpstreamError([]byte(`{"code":10001,"msg":"sensitive content"}`))
	if err == nil || !strings.Contains(err.Error(), "sensitive content") {
		t.Fatalf("error = %v", err)
	}
	body, _ := sjson.SetBytes([]byte(`{}`), "choices.0.message.content", "ok")
	if err := codeBuddyUpstreamError(body); err != nil {
		t.Fatalf("choices payload should not be an error: %v", err)
	}
}

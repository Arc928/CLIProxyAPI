package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func codebuddyTestAuth(apiBaseURL string) *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		ID:       "codebuddy-test.json",
		Provider: "codebuddy",
		Metadata: map[string]any{
			"type":         "codebuddy",
			"access_token": "tok-test",
			"uid":          "uid-test",
			"domain":       "www.codebuddy.cn",
			"api_base_url": apiBaseURL,
		},
		Attributes: map[string]string{},
	}
}

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

// Reasoning deltas are forwarded one-to-one without coalescing.
func TestCodeBuddyExecutorStreamLeavesPlainDeltasAlone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, text := range []string{"你", "好", "世", "界"} {
			_, _ = io.WriteString(w, `data: {"id":"plain","model":"glm-5.2","choices":[{"index":0,"delta":{"content":"","reasoning_content":"`+text+`"}}]}`+"\n\n")
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)

	executor := NewCodeBuddyExecutor(&config.Config{})
	result, err := executor.ExecuteStream(context.Background(), codebuddyTestAuth(server.URL), cliproxyexecutor.Request{
		Model:   "glm-5.2",
		Payload: []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAI,
		ResponseFormat: sdktranslator.FormatOpenAI,
		Stream:         true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	var reasoning []string
	for streamChunk := range result.Chunks {
		if streamChunk.Err != nil {
			t.Fatalf("stream chunk error: %v", streamChunk.Err)
		}
		if !gjson.ValidBytes(streamChunk.Payload) {
			continue
		}
		if value := gjson.GetBytes(streamChunk.Payload, "choices.0.delta.reasoning_content"); value.Exists() {
			reasoning = append(reasoning, value.String())
		}
	}
	want := []string{"你", "好", "世", "界"}
	if len(reasoning) != len(want) {
		t.Fatalf("reasoning deltas = %q, want %q untouched", reasoning, want)
	}
	for i := range want {
		if reasoning[i] != want[i] {
			t.Fatalf("reasoning deltas = %q, want %q untouched", reasoning, want)
		}
	}
}

func TestCodeBuddyExecutorStreamMovesThinkingTagsOutOfContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"tagged","model":"glm-5.2","choices":[{"index":0,"delta":{"content":"<thinking>plan"}}]}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"choices":[{"index":0,"delta":{"content":" step</thinking>answer"}}]}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)

	executor := NewCodeBuddyExecutor(&config.Config{})
	result, err := executor.ExecuteStream(context.Background(), codebuddyTestAuth(server.URL), cliproxyexecutor.Request{
		Model:   "glm-5.2",
		Payload: []byte(`{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAI,
		ResponseFormat: sdktranslator.FormatOpenAI,
		Stream:         true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	var content, reasoning string
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		content += gjson.GetBytes(chunk.Payload, "choices.0.delta.content").String()
		reasoning += gjson.GetBytes(chunk.Payload, "choices.0.delta.reasoning_content").String()
		if bytes.Contains(chunk.Payload, []byte("<thinking>")) || bytes.Contains(chunk.Payload, []byte("</thinking>")) {
			t.Fatalf("thinking tags leaked in downstream chunk: %s", chunk.Payload)
		}
	}
	if content != "answer" {
		t.Fatalf("visible content = %q, want answer", content)
	}
	if reasoning != "plan step" {
		t.Fatalf("reasoning content = %q, want plan step", reasoning)
	}
}

// Deltas carrying an empty "tool_calls":[] must not split reasoning into
// several items for Responses clients.
func TestCodeBuddyExecutorStreamResponsesKeepsSingleReasoningItem(t *testing.T) {
	hy4Reasoning := func(text string) string {
		return `data: {"id":"cmb-t1","model":"hy4-preview","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"content":"","reasoning_content":"` + text + `","function_call":null,"refusal":"","tool_calls":[],"extra_fields":null},"logprobs":null,"finish_reason":""}],"usage":null}` + "\n\n"
	}
	hy4Chunk := func(delta string) string {
		return `data: {"id":"cmb-t1","model":"hy4-preview","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":` + delta + `,"logprobs":null,"finish_reason":""}],"usage":null}` + "\n\n"
	}
	r1 := "Step one " + strings.Repeat("a", 600)
	r2 := strings.Repeat("b", 600)
	r3 := strings.Repeat("c", 300) + " end"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, hy4Reasoning(r1))
		_, _ = io.WriteString(w, hy4Reasoning(r2))
		_, _ = io.WriteString(w, hy4Reasoning(r3))
		_, _ = io.WriteString(w, hy4Chunk(`{"content":"Answer.","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[],"extra_fields":null}`))
		_, _ = io.WriteString(w, hy4Chunk(`{"content":"","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"grep","arguments":""},"index":0}],"extra_fields":null}`))
		_, _ = io.WriteString(w, hy4Chunk(`{"content":"","reasoning_content":"","function_call":null,"refusal":"","tool_calls":[{"function":{"name":"","arguments":"{\"q\":\"x\"}"},"index":0}],"extra_fields":null}`))
		_, _ = io.WriteString(w, `data: {"id":"cmb-t1","model":"hy4-preview","object":"chat.completion.chunk","created":1,"choices":[{"index":0,"delta":{"role":"assistant","content":"","reasoning_content":"","function_call":{"name":"","arguments":""},"refusal":"","tool_calls":[],"extra_fields":null},"logprobs":null,"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)

	auth := &cliproxyauth.Auth{
		ID:       "codebuddy-stream.json",
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
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "hy4-preview",
		Payload: []byte(`{"model":"hy4-preview","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAIResponse,
		ResponseFormat: sdktranslator.FormatOpenAIResponse,
		Stream:         true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	var raw []byte
	for streamChunk := range result.Chunks {
		if streamChunk.Err != nil {
			t.Fatalf("stream chunk error: %v", streamChunk.Err)
		}
		raw = append(raw, streamChunk.Payload...)
	}

	reasoningAdded, reasoningDone := 0, 0
	reasoningText := ""
	funcArgsDone := ""
	completed := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := []byte(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		switch gjson.GetBytes(payload, "type").String() {
		case "response.output_item.added":
			if gjson.GetBytes(payload, "item.type").String() == "reasoning" {
				reasoningAdded++
			}
		case "response.output_item.done":
			if gjson.GetBytes(payload, "item.type").String() == "reasoning" {
				reasoningDone++
			}
		case "response.reasoning_summary_text.delta":
			reasoningText += gjson.GetBytes(payload, "delta").String()
		case "response.function_call_arguments.done":
			funcArgsDone = gjson.GetBytes(payload, "arguments").String()
		case "response.completed":
			completed = true
		}
	}
	if reasoningAdded != 1 || reasoningDone != 1 {
		t.Fatalf("reasoning item lifecycle = %d added / %d done, want exactly 1/1\n%s", reasoningAdded, reasoningDone, raw)
	}
	if want := r1 + r2 + r3; reasoningText != want {
		t.Fatalf("reasoning text = %q..., want concatenation of three reasoning chunks", reasoningText[:60])
	}
	if funcArgsDone != `{"q":"x"}` {
		t.Fatalf("function_call_arguments.done = %q", funcArgsDone)
	}
	if !completed {
		t.Fatalf("missing response.completed event")
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

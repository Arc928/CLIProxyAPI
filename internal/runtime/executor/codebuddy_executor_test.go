package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
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

// Zero-information chunks (empty reasoning deltas, usage-only frames) must be
// forwarded without splitting the buffered reasoning stream.
func TestCodeBuddyReasoningCoalescerIgnoresControlChunks(t *testing.T) {
	var c codeBuddyReasoningCoalescer
	if got := c.Push([]byte(`data: {"choices":[{"index":0,"delta":{"content":"","reasoning_content":"你","tool_calls":[]},"finish_reason":""}]}`)); len(got) != 0 {
		t.Fatalf("reasoning chunk = %v, want buffered", got)
	}
	got := c.Push([]byte(`data: {"choices":[{"index":0,"delta":{"content":"","reasoning_content":"","tool_calls":[]},"finish_reason":""}],"usage":null}`))
	if len(got) != 1 || !bytes.Contains(got[0], []byte(`"reasoning_content":""`)) {
		t.Fatalf("empty reasoning delta = %v, want forwarded without flush", got)
	}
	got = c.Push([]byte(`data: {"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`))
	if len(got) != 1 || !bytes.Contains(got[0], []byte(`"usage"`)) {
		t.Fatalf("usage-only chunk = %v, want forwarded without flush", got)
	}
	if got := c.Push([]byte(`data: {"choices":[{"index":0,"delta":{"content":"","reasoning_content":"好","tool_calls":[]},"finish_reason":""}]}`)); len(got) != 0 {
		t.Fatalf("reasoning after control chunks = %v, want buffered", got)
	}
	flushed := c.Flush()
	if len(flushed) != 1 {
		t.Fatalf("flushed = %d, want 1", len(flushed))
	}
	if merged := gjson.GetBytes(codeBuddySSEPayload(flushed[0]), "choices.0.delta.reasoning_content").String(); merged != "你好" {
		t.Fatalf("merged reasoning = %q, want %q", merged, "你好")
	}
}

func TestCodeBuddyReasoningCoalescerFlushesOnFinishReason(t *testing.T) {
	var c codeBuddyReasoningCoalescer
	if got := c.Push([]byte(`data: {"choices":[{"index":0,"delta":{"reasoning_content":"tail"}}]}`)); len(got) != 0 {
		t.Fatalf("reasoning chunk = %v, want buffered", got)
	}
	got := c.Push([]byte(`data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	if len(got) != 2 || !bytes.Contains(got[0], []byte(`"reasoning_content":"tail"`)) || !bytes.Contains(got[1], []byte(`"finish_reason":"stop"`)) {
		t.Fatalf("finish_reason flush = %v", got)
	}
}

// Buffers below the byte threshold must reach the client while the upstream is
// still silent, instead of waiting for the stream to end.
func TestCodeBuddyExecutorStreamFlushesReasoningOnIdle(t *testing.T) {
	stall := 20 * codeBuddyReasoningIdleFlush
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `data: {"id":"idle","model":"hy4-preview","choices":[{"index":0,"delta":{"content":"","reasoning_content":"thinking","tool_calls":[]}}]}`+"\n\n")
		flusher.Flush()
		time.Sleep(stall)
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	t.Cleanup(server.Close)

	executor := NewCodeBuddyExecutor(&config.Config{})
	result, err := executor.ExecuteStream(context.Background(), codebuddyTestAuth(server.URL), cliproxyexecutor.Request{
		Model:   "hy4-preview",
		Payload: []byte(`{"model":"hy4-preview","messages":[{"role":"user","content":"hi"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAI,
		ResponseFormat: sdktranslator.FormatOpenAI,
		Stream:         true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream: %v", err)
	}
	start := time.Now()
	first, open := <-result.Chunks
	elapsed := time.Since(start)
	if !open {
		t.Fatalf("stream closed without chunks")
	}
	if first.Err != nil {
		t.Fatalf("first chunk error: %v", first.Err)
	}
	if elapsed > stall/2 {
		t.Fatalf("first chunk arrived after %v, want it released once the upstream idled", elapsed)
	}
	if got := gjson.GetBytes(first.Payload, "choices.0.delta.reasoning_content").String(); got != "thinking" {
		t.Fatalf("first chunk = %s, want the idle reasoning batch", first.Payload)
	}
	for streamChunk := range result.Chunks {
		if streamChunk.Err != nil {
			t.Fatalf("stream chunk error: %v", streamChunk.Err)
		}
	}
}

// Upstreams that do not stamp an empty tool_calls array on their deltas must be
// forwarded untouched: no stripping and no reasoning coalescing.
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

func TestCodeBuddyDeltaShape(t *testing.T) {
	if hasDelta, empty := codeBuddyDeltaShape([]byte(`data: {"choices":[{"delta":{"content":"x","tool_calls":[]}}]}`)); !hasDelta || !empty {
		t.Fatalf("empty tool_calls delta = %v/%v, want true/true", hasDelta, empty)
	}
	if hasDelta, empty := codeBuddyDeltaShape([]byte(`data: {"choices":[{"delta":{"content":"x","tool_calls":[{"index":0}]}}]}`)); !hasDelta || empty {
		t.Fatalf("populated tool_calls delta = %v/%v, want true/false", hasDelta, empty)
	}
	if hasDelta, empty := codeBuddyDeltaShape([]byte(`data: {"choices":[{"delta":{"content":"x"}}]}`)); !hasDelta || empty {
		t.Fatalf("plain delta = %v/%v, want true/false", hasDelta, empty)
	}
	if hasDelta, _ := codeBuddyDeltaShape([]byte("data: [DONE]")); hasDelta {
		t.Fatalf("[DONE] should not count as a delta")
	}
	if hasDelta, _ := codeBuddyDeltaShape([]byte(": ping")); hasDelta {
		t.Fatalf("SSE comment should not count as a delta")
	}
}

func TestCodeBuddyStripEmptyToolCalls(t *testing.T) {
	raw := []byte(`data: {"id":"c1","choices":[{"index":0,"delta":{"content":"","reasoning_content":"t","function_call":null,"tool_calls":[]},"finish_reason":""}],"usage":null}`)
	got := codeBuddyStripEmptyToolCalls(raw)
	if bytes.Contains(got, []byte("tool_calls")) {
		t.Fatalf("empty tool_calls not stripped: %s", got)
	}
	if !bytes.HasPrefix(got, []byte("data: ")) {
		t.Fatalf("SSE framing lost: %s", got)
	}
	if g := gjson.GetBytes(codeBuddySSEPayload(got), "choices.0.delta.reasoning_content").String(); g != "t" {
		t.Fatalf("reasoning_content = %q, want t", g)
	}
	keep := []byte(`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"id":"c1","type":"function","function":{"name":"bash","arguments":"x"},"index":0}]}}]}`)
	if out := codeBuddyStripEmptyToolCalls(keep); !bytes.Contains(out, []byte(`"tool_calls"`)) {
		t.Fatalf("non-empty tool_calls must be preserved: %s", out)
	}
	done := []byte("data: [DONE]")
	if got := codeBuddyStripEmptyToolCalls(done); !bytes.Equal(got, done) {
		t.Fatalf("[DONE] line changed: %s", got)
	}
}

// Hy4 attaches an empty "tool_calls":[] to every delta; Responses clients must
// still see a single reasoning item lifecycle rather than one per coalesced
// chunk. Reasoning text exceeds the 512-byte coalescing threshold twice so
// multiple reasoning chunks actually flow downstream.
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

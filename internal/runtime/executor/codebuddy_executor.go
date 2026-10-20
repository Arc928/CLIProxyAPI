package executor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/buildinfo"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// CodeBuddyExecutor is a stateless executor for CodeBuddy's OpenAI-compatible chat API.
type CodeBuddyExecutor struct {
	cfg *config.Config
}

// NewCodeBuddyExecutor creates a CodeBuddy executor.
func NewCodeBuddyExecutor(cfg *config.Config) *CodeBuddyExecutor {
	return &CodeBuddyExecutor{cfg: cfg}
}

// Identifier returns the executor identifier.
func (e *CodeBuddyExecutor) Identifier() string { return codebuddyauth.ProviderName }

// RequestToFormat reports the upstream request format used after auth selection.
func (e *CodeBuddyExecutor) RequestToFormat(_ cliproxyexecutor.Request, _ cliproxyexecutor.Options) sdktranslator.Format {
	return sdktranslator.FormatOpenAI
}

// PrepareRequest injects CodeBuddy credentials into the outgoing HTTP request.
func (e *CodeBuddyExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	applyCodeBuddyHeaders(req, auth, strings.Contains(req.Header.Get("Accept"), "text/event-stream"))
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(req, attrs)
	return nil
}

// HttpRequest injects CodeBuddy credentials into the request and executes it.
func (e *CodeBuddyExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("codebuddy executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	httpReq := req.WithContext(ctx)
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	return httpClient.Do(httpReq)
}

// Execute performs a non-streaming chat completion by aggregating the upstream SSE stream.
func (e *CodeBuddyExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	to := sdktranslator.FromString("openai")
	body, err := e.buildChatBody(ctx, req, opts, from, to, baseModel, true)
	if err != nil {
		return resp, err
	}
	reporter.SetTranslatedReasoningEffort(body, e.Identifier())

	httpResp, _, errDo := e.doChat(ctx, auth, body, true, reporter)
	if errDo != nil {
		return resp, errDo
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codebuddy executor: close response body error: %v", errClose)
		}
	}()
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		helps.AppendAPIResponseChunk(ctx, e.cfg, b)
		helps.LogWithRequestID(ctx).Debugf("request error, error status: %d, error message: %s", httpResp.StatusCode, helps.SummarizeErrorBody(httpResp.Header.Get("Content-Type"), b))
		err = statusErr{code: httpResp.StatusCode, msg: string(b)}
		return resp, err
	}

	data, errRead := readCodeBuddyChatBody(httpResp)
	if errRead != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errRead)
		return resp, errRead
	}
	helps.AppendAPIResponseChunk(ctx, e.cfg, data)
	if errUpstream := codeBuddyUpstreamError(data); errUpstream != nil {
		err = errUpstream
		return resp, err
	}
	data = helps.NormalizeOpenAICompatNonStreamPayload(data, helps.NewOpenAICompatStreamState())
	reporter.Publish(ctx, helps.ParseOpenAIUsage(data))
	reporter.EnsurePublished(ctx)

	var param any
	out := sdktranslator.TranslateNonStream(ctx, to, responseFormat, req.Model, opts.OriginalRequest, body, data, &param)
	if responseFormat == sdktranslator.FormatOpenAIResponse {
		out = helps.EnsureResponsesUsageDetails(out)
	}
	resp = cliproxyexecutor.Response{Payload: out, Headers: httpResp.Header.Clone()}
	return resp, nil
}

// ExecuteStream performs a streaming chat completion request to CodeBuddy.
func (e *CodeBuddyExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	reporter := helps.NewExecutorUsageReporter(ctx, e, baseModel, auth)
	defer reporter.TrackFailure(ctx, &err)

	to := sdktranslator.FromString("openai")
	body, err := e.buildChatBody(ctx, req, opts, from, to, baseModel, true)
	if err != nil {
		return nil, err
	}
	reporter.SetTranslatedReasoningEffort(body, e.Identifier())

	httpResp, _, errDo := e.doChat(ctx, auth, body, true, reporter)
	if errDo != nil {
		return nil, errDo
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		b, _ := io.ReadAll(httpResp.Body)
		helps.AppendAPIResponseChunk(ctx, e.cfg, b)
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codebuddy executor: close response body error: %v", errClose)
		}
		err = statusErr{code: httpResp.StatusCode, msg: string(b)}
		return nil, err
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer func() {
			if errClose := httpResp.Body.Close(); errClose != nil {
				log.Errorf("codebuddy executor: close response body error: %v", errClose)
			}
		}()
		scanner := bufio.NewScanner(httpResp.Body)
		scanner.Buffer(nil, 1_048_576)
		originalPayload := req.Payload
		if len(opts.OriginalRequest) > 0 {
			originalPayload = opts.OriginalRequest
		}
		claudeInputTokens := helps.NewClaudeInputTokenState(from, to, responseFormat, originalPayload)
		var param any
		var streamUsage helps.StreamUsageBuffer
		streamState := helps.NewOpenAICompatStreamState()
		var reasoningCoalescer codeBuddyReasoningCoalescer
		defer streamUsage.Publish(ctx, reporter)

		// Upstreams that stamp an empty "tool_calls" array on every delta need
		// the array stripped and the resulting tiny reasoning deltas merged.
		// The shape is detected on the first delta instead of being assumed for
		// every model behind this provider.
		normalizeStream := func(line []byte) []byte { return line }
		coalesce := false
		shapeDecided := false

		// Reading the SSE body blocks until upstream produces the next line, so
		// it runs on its own goroutine: the coalescer needs a ticking clock to
		// release buffered reasoning while the upstream stays silent.
		lines := make(chan []byte)
		readErr := make(chan error, 1)
		go func() {
			defer close(lines)
			for scanner.Scan() {
				select {
				case lines <- append([]byte(nil), scanner.Bytes()...):
				case <-ctx.Done():
					return
				}
			}
			readErr <- scanner.Err()
		}()

		// idle fires when the upstream stalls, releasing whatever reasoning the
		// batch is holding. It is only running while a batch is open.
		idle := time.NewTimer(codeBuddyReasoningIdleFlush)
		idle.Stop()
		defer idle.Stop()

		emit := func(line []byte) bool {
			normalized := helps.NormalizeOpenAICompatStreamLine(normalizeStream(line), streamState)
			chunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, opts.OriginalRequest, body, normalized, &param, claudeInputTokens)
			for i := range chunks {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
				case <-ctx.Done():
					return false
				}
			}
			return true
		}

		// flush releases buffered reasoning, reporting false if the context is
		// done.
		flush := func() bool {
			idle.Stop()
			for _, coalesced := range reasoningCoalescer.Flush() {
				if !emit(coalesced) {
					return false
				}
			}
			return true
		}

	stream:
		for {
			var line []byte
			var open bool
			select {
			case line, open = <-lines:
				if !open {
					break stream
				}
			case <-idle.C:
				if !flush() {
					return
				}
				continue
			case <-ctx.Done():
				return
			}

			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			streamUsage.ObserveOpenAIStream(line)
			if !shapeDecided {
				if hasDelta, emptyToolCalls := codeBuddyDeltaShape(line); hasDelta {
					shapeDecided = true
					coalesce = emptyToolCalls
					if emptyToolCalls {
						normalizeStream = codeBuddyStripEmptyToolCalls
					}
				}
			}
			if !coalesce {
				if !emit(line) {
					return
				}
				continue
			}
			for _, coalesced := range reasoningCoalescer.Push(line) {
				if !emit(coalesced) {
					return
				}
			}
			// Re-arm on every line so idle measures the gap since the last
			// delta rather than the age of the batch.
			idle.Stop()
			if reasoningCoalescer.Pending() {
				idle.Reset(codeBuddyReasoningIdleFlush)
			}
		}
		if !flush() {
			return
		}
		var scanErr error
		select {
		case scanErr = <-readErr:
		case <-ctx.Done():
			return
		}
		doneLine := helps.NormalizeOpenAICompatStreamLine([]byte("data: [DONE]"), streamState)
		doneChunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, opts.OriginalRequest, body, doneLine, &param, claudeInputTokens)
		for i := range doneChunks {
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: doneChunks[i]}:
			case <-ctx.Done():
				return
			}
		}
		if scanErr != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, scanErr)
			reporter.PublishFailure(ctx, scanErr)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: scanErr}:
			case <-ctx.Done():
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: httpResp.Header.Clone(), Chunks: out}, nil
}

// CountTokens estimates token count for CodeBuddy requests.
func (e *CodeBuddyExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	_ = auth
	baseModel := thinking.ParseSuffix(req.Model).ModelName
	from := opts.SourceFormat
	responseFormat := cliproxyexecutor.ResponseFormatOrSource(opts)
	to := sdktranslator.FromString("openai")
	translated, err := e.buildChatBody(ctx, req, opts, from, to, baseModel, false)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	enc, errTok := helps.TokenizerForModel(baseModel)
	if errTok != nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codebuddy executor: tokenizer init failed: %w", errTok)
	}
	count, errCount := helps.CountOpenAIChatTokens(enc, translated)
	if errCount != nil {
		return cliproxyexecutor.Response{}, fmt.Errorf("codebuddy executor: token counting failed: %w", errCount)
	}
	usageJSON := helps.BuildOpenAIUsageJSON(count)
	translatedUsage := sdktranslator.TranslateTokenCount(ctx, to, responseFormat, count, usageJSON)
	return cliproxyexecutor.Response{Payload: translatedUsage}, nil
}

// Refresh refreshes the CodeBuddy token using the refresh token.
func (e *CodeBuddyExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	log.Debugf("codebuddy executor: refresh called")
	if refreshed, handled, err := helps.RefreshAuthViaHome(ctx, e.cfg, auth); handled {
		return refreshed, err
	}
	if auth == nil {
		return nil, fmt.Errorf("codebuddy executor: auth is nil")
	}
	current := codeBuddyTokenFromAuth(auth)
	if strings.TrimSpace(current.RefreshToken) == "" {
		return auth, nil
	}
	account := codeBuddyAccountFromAuth(auth)
	client := codebuddyauth.NewClient(e.cfg, codeBuddyAPIBase(auth), auth.ProxyURL)
	td, err := client.RefreshToken(ctx, current, account)
	if err != nil {
		return nil, err
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["access_token"] = td.AccessToken
	if td.RefreshToken != "" {
		auth.Metadata["refresh_token"] = td.RefreshToken
	}
	if td.TokenType != "" {
		auth.Metadata["token_type"] = td.TokenType
	}
	if td.Domain != "" {
		auth.Metadata["domain"] = td.Domain
	}
	if td.ExpiresAt > 0 {
		auth.Metadata["expired"] = time.Unix(td.ExpiresAt, 0).UTC().Format(time.RFC3339)
	}
	auth.Metadata["type"] = codebuddyauth.ProviderName
	auth.Metadata["last_refresh"] = time.Now().Format(time.RFC3339)
	return auth, nil
}

func (e *CodeBuddyExecutor) buildChatBody(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, from, to sdktranslator.Format, baseModel string, stream bool) ([]byte, error) {
	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalTranslated := helps.TranslateRequestWithCodexMultiAgentV2(ctx, opts.Headers, e.cfg, from, to, baseModel, bytes.Clone(originalPayloadSource), stream)
	body := helps.TranslateRequestWithCodexMultiAgentV2(ctx, opts.Headers, e.cfg, from, to, baseModel, bytes.Clone(req.Payload), stream)

	upstreamModel := strings.TrimSpace(baseModel)
	if upstreamModel == "" {
		upstreamModel = "auto"
	}
	var err error
	body, err = sjson.SetBytes(body, "model", upstreamModel)
	if err != nil {
		return nil, fmt.Errorf("codebuddy executor: failed to set model in payload: %w", err)
	}
	body, err = helps.ApplyThinkingWithSourcePayload(body, req.Payload, originalPayloadSource, req.Model, from.String(), "openai", e.Identifier())
	if err != nil {
		return nil, err
	}
	if stream {
		body, err = sjson.SetBytes(body, "stream", true)
		if err != nil {
			return nil, fmt.Errorf("codebuddy executor: failed to set stream: %w", err)
		}
		body, err = sjson.SetBytes(body, "stream_options.include_usage", true)
		if err != nil {
			return nil, fmt.Errorf("codebuddy executor: failed to set stream_options: %w", err)
		}
	}
	requestedModel := helps.PayloadRequestedModel(opts, req.Model)
	requestPath := helps.PayloadRequestPath(opts)
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, baseModel, to.String(), from.String(), "", body, originalTranslated, requestedModel, requestPath, opts.Headers)
	body = helps.SanitizeCodeBuddySystemPrompts(body)
	return body, nil
}

func (e *CodeBuddyExecutor) doChat(ctx context.Context, auth *cliproxyauth.Auth, body []byte, stream bool, reporter *helps.UsageReporter) (*http.Response, *http.Request, error) {
	url := codebuddyauth.ChatCompletionsURL(codeBuddyAPIBase(auth))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	applyCodeBuddyHeaders(httpReq, auth, stream)
	var attrs map[string]string
	if auth != nil {
		attrs = auth.Attributes
	}
	util.ApplyCustomHeadersFromAttrs(httpReq, attrs)
	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      body,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})
	httpClient := helps.NewProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpClient = reporter.TrackHTTPClient(httpClient)
	httpResp, errDo := httpClient.Do(httpReq)
	if errDo != nil {
		helps.RecordAPIResponseError(ctx, e.cfg, errDo)
		return nil, httpReq, errDo
	}
	return httpResp, httpReq, nil
}

func readCodeBuddyChatBody(httpResp *http.Response) ([]byte, error) {
	if httpResp == nil {
		return nil, fmt.Errorf("codebuddy executor: response is nil")
	}
	ct := strings.ToLower(httpResp.Header.Get("Content-Type"))
	if strings.Contains(ct, "text/event-stream") {
		return aggregateOpenAIChatSSE(httpResp.Body)
	}
	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(data)
	if bytes.HasPrefix(trimmed, []byte("data:")) || bytes.Contains(trimmed, []byte("\ndata:")) {
		return aggregateOpenAIChatSSE(bytes.NewReader(data))
	}
	return data, nil
}

func aggregateOpenAIChatSSE(r io.Reader) ([]byte, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(nil, 1_048_576)

	id := ""
	model := ""
	finishReason := ""
	var content strings.Builder
	var reasoning strings.Builder
	usageRaw := ""
	toolCalls := map[int]map[string]string{}

	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if bytes.Equal(payload, []byte("[DONE]")) {
			break
		}
		if !gjson.ValidBytes(payload) {
			continue
		}
		root := gjson.ParseBytes(payload)
		if id == "" {
			id = root.Get("id").String()
		}
		if model == "" {
			model = root.Get("model").String()
		}
		if u := root.Get("usage"); u.Exists() {
			usageRaw = u.Raw
		}
		for _, choice := range root.Get("choices").Array() {
			if fr := choice.Get("finish_reason"); fr.Exists() && fr.String() != "" && fr.String() != "null" {
				finishReason = fr.String()
			}
			delta := choice.Get("delta")
			if !delta.Exists() {
				delta = choice.Get("message")
			}
			if text := delta.Get("content"); text.Exists() && text.Type == gjson.String {
				content.WriteString(text.String())
			}
			if rc := delta.Get("reasoning_content"); rc.Exists() && rc.Type == gjson.String {
				reasoning.WriteString(rc.String())
			}
			if rc := delta.Get("reasoning"); rc.Exists() && rc.Type == gjson.String {
				reasoning.WriteString(rc.String())
			}
			for _, tool := range delta.Get("tool_calls").Array() {
				idx := int(tool.Get("index").Int())
				entry := toolCalls[idx]
				if entry == nil {
					entry = map[string]string{"id": "", "type": "function", "name": "", "arguments": ""}
					toolCalls[idx] = entry
				}
				if v := tool.Get("id").String(); v != "" {
					entry["id"] = v
				}
				if v := tool.Get("type").String(); v != "" {
					entry["type"] = v
				}
				if v := tool.Get("function.name").String(); v != "" {
					entry["name"] += v
				}
				if v := tool.Get("function.arguments").String(); v != "" {
					entry["arguments"] += v
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	out := []byte(`{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant"},"finish_reason":"stop"}]}`)
	if id != "" {
		out, _ = sjson.SetBytes(out, "id", id)
	}
	if model != "" {
		out, _ = sjson.SetBytes(out, "model", model)
	}
	out, _ = sjson.SetBytes(out, "choices.0.message.content", content.String())
	if reasoning.Len() > 0 {
		out, _ = sjson.SetBytes(out, "choices.0.message.reasoning_content", reasoning.String())
	}
	if finishReason != "" {
		out, _ = sjson.SetBytes(out, "choices.0.finish_reason", finishReason)
	}
	if len(toolCalls) > 0 {
		rawCalls := make([]string, 0, len(toolCalls))
		for i := 0; i < len(toolCalls); i++ {
			entry := toolCalls[i]
			if entry == nil {
				continue
			}
			call := []byte(`{"type":"function","function":{}}`)
			if entry["id"] != "" {
				call, _ = sjson.SetBytes(call, "id", entry["id"])
			}
			if entry["type"] != "" {
				call, _ = sjson.SetBytes(call, "type", entry["type"])
			}
			call, _ = sjson.SetBytes(call, "function.name", entry["name"])
			call, _ = sjson.SetBytes(call, "function.arguments", entry["arguments"])
			rawCalls = append(rawCalls, string(call))
		}
		if len(rawCalls) > 0 {
			out, _ = sjson.SetRawBytes(out, "choices.0.message.tool_calls", []byte("["+strings.Join(rawCalls, ",")+"]"))
			if finishReason == "" {
				out, _ = sjson.SetBytes(out, "choices.0.finish_reason", "tool_calls")
			}
		}
	}
	if usageRaw != "" {
		out, _ = sjson.SetRawBytes(out, "usage", []byte(usageRaw))
	}
	return out, nil
}

const (
	// codeBuddyReasoningBatchBytes is the amount of reasoning text released to
	// the downstream translators in one chunk.
	codeBuddyReasoningBatchBytes = 512
	// codeBuddyReasoningIdleFlush bounds how long buffered reasoning waits for
	// a full batch, so an upstream that stalls mid-thought does not hold text
	// back until the stream ends.
	codeBuddyReasoningIdleFlush = 50 * time.Millisecond
)

// Chunk classification returned by codeBuddyClassifyDelta.
const (
	// codeBuddyChunkBoundary carries meaningful content (answer text, tool
	// calls, refusal, or a terminal signal) and flushes pending reasoning.
	codeBuddyChunkBoundary = iota
	// codeBuddyChunkReasoning carries reasoning text that can be merged.
	codeBuddyChunkReasoning
	// codeBuddyChunkNoop carries no content (empty deltas, usage-only chunks)
	// and is forwarded without disturbing pending reasoning.
	codeBuddyChunkNoop
)

// codeBuddyReasoningCoalescer combines the very small reasoning deltas emitted
// by Hy4 into usable stream chunks while preserving normal OpenAI chunk order.
// A batch is released once it holds codeBuddyReasoningBatchBytes of text or
// once the upstream stays silent for codeBuddyReasoningIdleFlush.
type codeBuddyReasoningCoalescer struct {
	// template is the first delta of the batch, kept so Flush can serialize the
	// merged text once instead of rewriting the payload on every delta.
	template []byte
	key      string
	text     strings.Builder
}

// Pending reports whether a batch of reasoning is being held back.
func (c *codeBuddyReasoningCoalescer) Pending() bool { return len(c.template) > 0 }

// Push buffers one upstream SSE data line and returns the lines that are ready
// to travel downstream, in order.
func (c *codeBuddyReasoningCoalescer) Push(line []byte) [][]byte {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	text, key, kind := codeBuddyClassifyDelta(line)
	switch kind {
	case codeBuddyChunkNoop:
		return [][]byte{bytes.Clone(line)}
	case codeBuddyChunkBoundary:
		return c.flushWith(line)
	}
	if len(c.template) > 0 && c.key != key {
		// Reasoning moved to another field; close the batch and open a new one.
		out := c.Flush()
		c.begin(line, key, text)
		return out
	}
	if len(c.template) == 0 {
		c.begin(line, key, text)
	} else {
		c.text.WriteString(text)
	}
	if c.text.Len() >= codeBuddyReasoningBatchBytes {
		return c.Flush()
	}
	return nil
}

// begin opens a batch seeded by line, whose reasoning text lives under key.
func (c *codeBuddyReasoningCoalescer) begin(line []byte, key, text string) {
	c.template = bytes.Clone(codeBuddySSEPayload(line))
	c.key = key
	c.text.Reset()
	c.text.WriteString(text)
}

func (c *codeBuddyReasoningCoalescer) flushWith(line []byte) [][]byte {
	out := c.Flush()
	if len(bytes.TrimSpace(line)) > 0 {
		out = append(out, bytes.Clone(line))
	}
	return out
}

// Flush releases the buffered reasoning as a single line shaped like the delta
// that opened the batch.
func (c *codeBuddyReasoningCoalescer) Flush() [][]byte {
	if len(c.template) == 0 {
		return nil
	}
	// The template is validated JSON that was classified as carrying this key,
	// so the path always resolves and the set cannot fail.
	merged, _ := sjson.SetBytes(c.template, "choices.0.delta."+c.key, c.text.String())
	c.template = nil
	c.key = ""
	c.text.Reset()
	return [][]byte{append([]byte("data: "), merged...)}
}

func codeBuddySSEPayload(line []byte) []byte {
	trimmed := bytes.TrimSpace(line)
	if bytes.HasPrefix(trimmed, []byte("data:")) {
		return bytes.TrimSpace(trimmed[len("data:"):])
	}
	return trimmed
}

// codeBuddyStripEmptyToolCalls removes empty "tool_calls":[] arrays from Hy4
// stream chunks. Hy4 attaches an empty tool_calls array to every delta,
// including pure reasoning chunks; downstream translators would otherwise
// treat each chunk as a tool-call boundary and restart reasoning/message
// items on every delta.
func codeBuddyStripEmptyToolCalls(line []byte) []byte {
	payload := codeBuddySSEPayload(line)
	if !gjson.ValidBytes(payload) {
		return line
	}
	tcs := gjson.GetBytes(payload, "choices.0.delta.tool_calls")
	if !tcs.IsArray() || len(tcs.Array()) != 0 {
		return line
	}
	updated, err := sjson.DeleteBytes(payload, "choices.0.delta.tool_calls")
	if err != nil {
		return line
	}
	if bytes.HasPrefix(bytes.TrimSpace(line), []byte("data:")) {
		return append([]byte("data: "), updated...)
	}
	return updated
}

// codeBuddyDeltaShape inspects one upstream SSE data line and reports whether
// it carries a chat delta and, if so, whether the delta holds an empty
// "tool_calls" array — the shape Hy4 attaches to every delta.
func codeBuddyDeltaShape(line []byte) (hasDelta bool, emptyToolCalls bool) {
	payload := codeBuddySSEPayload(line)
	if !gjson.ValidBytes(payload) || bytes.Equal(payload, []byte("[DONE]")) {
		return false, false
	}
	delta := gjson.GetBytes(payload, "choices.0.delta")
	if !delta.Exists() {
		return false, false
	}
	toolCalls := delta.Get("tool_calls")
	return true, toolCalls.IsArray() && len(toolCalls.Array()) == 0
}

// codeBuddyClassifyDelta inspects one upstream SSE data line and reports
// whether it is a mergeable reasoning delta, a real content boundary, or a
// zero-information chunk that must not interrupt reasoning coalescing.
func codeBuddyClassifyDelta(line []byte) (text string, key string, kind int) {
	payload := codeBuddySSEPayload(line)
	if !gjson.ValidBytes(payload) || bytes.Equal(payload, []byte("[DONE]")) {
		return "", "", codeBuddyChunkBoundary
	}
	choices := gjson.GetBytes(payload, "choices")
	if !choices.IsArray() || len(choices.Array()) > 1 {
		return "", "", codeBuddyChunkBoundary
	}
	if len(choices.Array()) == 0 {
		return "", "", codeBuddyChunkNoop
	}
	choice := choices.Get("0")
	if finish := choice.Get("finish_reason"); finish.Type == gjson.String && finish.String() != "" {
		return "", "", codeBuddyChunkBoundary
	}
	delta := choice.Get("delta")
	if !delta.Exists() {
		return "", "", codeBuddyChunkNoop
	}
	if value := delta.Get("content"); value.Type == gjson.String && value.String() != "" {
		return "", "", codeBuddyChunkBoundary
	}
	if toolCalls := delta.Get("tool_calls"); toolCalls.Exists() && len(toolCalls.Array()) > 0 {
		return "", "", codeBuddyChunkBoundary
	}
	if value := delta.Get("refusal"); value.Type == gjson.String && value.String() != "" {
		return "", "", codeBuddyChunkBoundary
	}
	if fc := delta.Get("function_call"); fc.IsObject() && (fc.Get("name").String() != "" || fc.Get("arguments").String() != "") {
		return "", "", codeBuddyChunkBoundary
	}
	for _, reasoningKey := range []string{"reasoning_content", "reasoning"} {
		if value := delta.Get(reasoningKey); value.Type == gjson.String && value.String() != "" {
			return value.String(), reasoningKey, codeBuddyChunkReasoning
		}
	}
	return "", "", codeBuddyChunkNoop
}

func codeBuddyUpstreamError(data []byte) error {
	if !gjson.ValidBytes(data) {
		return nil
	}
	root := gjson.ParseBytes(data)
	if root.Get("choices").Exists() {
		return nil
	}
	code := root.Get("code")
	if code.Exists() && code.Type != gjson.Null && code.Int() != 0 {
		msg := strings.TrimSpace(root.Get("msg").String())
		if msg == "" {
			msg = strings.TrimSpace(root.Get("message").String())
		}
		if msg == "" {
			msg = string(data)
		}
		return statusErr{code: http.StatusBadRequest, msg: msg}
	}
	if errObj := root.Get("error"); errObj.Exists() {
		msg := strings.TrimSpace(errObj.Get("message").String())
		if msg == "" {
			msg = errObj.Raw
		}
		return statusErr{code: http.StatusBadRequest, msg: msg}
	}
	return nil
}

func applyCodeBuddyHeaders(req *http.Request, auth *cliproxyauth.Auth, stream bool) {
	if req == nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "CLIProxyAPI/"+buildinfo.Version)
	token := codeBuddyTokenFromAuth(auth)
	if strings.TrimSpace(token.AccessToken) != "" {
		tokenType := strings.TrimSpace(token.TokenType)
		if tokenType == "" {
			tokenType = "Bearer"
		}
		req.Header.Set("Authorization", tokenType+" "+strings.TrimSpace(token.AccessToken))
	}
	account := codeBuddyAccountFromAuth(auth)
	if strings.TrimSpace(account.UID) != "" {
		req.Header.Set("X-User-Id", strings.TrimSpace(account.UID))
	}
	if strings.TrimSpace(account.EnterpriseID) != "" {
		req.Header.Set("X-Enterprise-Id", strings.TrimSpace(account.EnterpriseID))
		req.Header.Set("X-Tenant-Id", strings.TrimSpace(account.EnterpriseID))
	}
	if domain := strings.TrimSpace(token.Domain); domain != "" {
		req.Header.Set("X-Domain", domain)
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
		return
	}
	req.Header.Set("Accept", "application/json")
}

func codeBuddyAPIBase(auth *cliproxyauth.Auth) string {
	if auth != nil && auth.Metadata != nil {
		if v, ok := auth.Metadata["api_base_url"].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		if v, ok := auth.Metadata["domain"].(string); ok {
			return codebuddyauth.APIBaseURLForDomain(v)
		}
	}
	return codebuddyauth.DefaultCNAPIBase
}

func codeBuddyTokenFromAuth(auth *cliproxyauth.Auth) *codebuddyauth.TokenData {
	token := &codebuddyauth.TokenData{TokenType: "Bearer"}
	if auth == nil {
		return token
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["access_token"].(string); ok {
			token.AccessToken = v
		}
		if v, ok := auth.Metadata["refresh_token"].(string); ok {
			token.RefreshToken = v
		}
		if v, ok := auth.Metadata["token_type"].(string); ok && strings.TrimSpace(v) != "" {
			token.TokenType = v
		}
		if v, ok := auth.Metadata["domain"].(string); ok {
			token.Domain = v
		}
	}
	if token.AccessToken == "" && auth.Attributes != nil {
		if v := auth.Attributes["access_token"]; v != "" {
			token.AccessToken = v
		}
	}
	if storage, ok := auth.Storage.(*codebuddyauth.TokenStorage); ok && storage != nil {
		if token.AccessToken == "" {
			token.AccessToken = storage.AccessToken
		}
		if token.RefreshToken == "" {
			token.RefreshToken = storage.RefreshToken
		}
		if token.Domain == "" {
			token.Domain = storage.Domain
		}
	}
	return token
}

func codeBuddyAccountFromAuth(auth *cliproxyauth.Auth) *codebuddyauth.AccountData {
	account := &codebuddyauth.AccountData{}
	if auth == nil {
		return account
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata["uid"].(string); ok {
			account.UID = v
		}
		if v, ok := auth.Metadata["nickname"].(string); ok {
			account.Nickname = v
		}
		if v, ok := auth.Metadata["enterprise_id"].(string); ok {
			account.EnterpriseID = v
		}
	}
	if storage, ok := auth.Storage.(*codebuddyauth.TokenStorage); ok && storage != nil {
		if account.UID == "" {
			account.UID = storage.UID
		}
		if account.EnterpriseID == "" {
			account.EnterpriseID = storage.EnterpriseID
		}
	}
	return account
}

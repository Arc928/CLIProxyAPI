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

	codebuddyauth "github.com/router-for-me/CLIProxyAPI/v7/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/buildinfo"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
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

	httpResp, _, errDo := e.doChat(ctx, auth, body, true)
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

	httpResp, _, errDo := e.doChat(ctx, auth, body, true)
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
		var reasoningCoalescer codeBuddyReasoningCoalescer
		defer streamUsage.Publish(ctx, reporter)
		for scanner.Scan() {
			line := scanner.Bytes()
			helps.AppendAPIResponseChunk(ctx, e.cfg, line)
			streamUsage.ObserveOpenAIStream(line)
			for _, coalesced := range reasoningCoalescer.Push(line) {
				chunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, opts.OriginalRequest, body, coalesced, &param, claudeInputTokens)
				for i := range chunks {
					select {
					case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
		for _, coalesced := range reasoningCoalescer.Flush() {
			chunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, opts.OriginalRequest, body, coalesced, &param, claudeInputTokens)
			for i := range chunks {
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: chunks[i]}:
				case <-ctx.Done():
					return
				}
			}
		}
		doneChunks := helps.TranslateStreamWithClaudeInputTokens(ctx, to, responseFormat, req.Model, opts.OriginalRequest, body, []byte("[DONE]"), &param, claudeInputTokens)
		for i := range doneChunks {
			select {
			case out <- cliproxyexecutor.StreamChunk{Payload: doneChunks[i]}:
			case <-ctx.Done():
				return
			}
		}
		if errScan := scanner.Err(); errScan != nil {
			helps.RecordAPIResponseError(ctx, e.cfg, errScan)
			reporter.PublishFailure(ctx, errScan)
			select {
			case out <- cliproxyexecutor.StreamChunk{Err: errScan}:
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
	return body, nil
}

func (e *CodeBuddyExecutor) doChat(ctx context.Context, auth *cliproxyauth.Auth, body []byte, stream bool) (*http.Response, *http.Request, error) {
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

const codeBuddyReasoningBatchBytes = 512

// codeBuddyReasoningCoalescer combines the very small reasoning deltas emitted
// by Hy4 into usable stream chunks while preserving normal OpenAI chunk order.
type codeBuddyReasoningCoalescer struct {
	pending      []byte
	reasoningKey string
	length       int
}

func (c *codeBuddyReasoningCoalescer) Push(line []byte) [][]byte {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}
	text, key, ok := codeBuddyReasoningDelta(line)
	if !ok {
		return c.flushWith(line)
	}
	if len(c.pending) == 0 {
		c.pending = bytes.Clone(line)
		c.reasoningKey = key
		c.length = len(text)
	} else if c.reasoningKey == key {
		payload := codeBuddySSEPayload(c.pending)
		updated, err := sjson.SetBytes(payload, "choices.0.delta."+key, gjson.GetBytes(payload, "choices.0.delta."+key).String()+text)
		if err != nil {
			return c.flushWith(line)
		}
		c.pending = append([]byte("data: "), updated...)
		c.length += len(text)
	} else {
		out := c.Flush()
		c.pending = bytes.Clone(line)
		c.reasoningKey = key
		c.length = len(text)
		return out
	}
	if c.length >= codeBuddyReasoningBatchBytes {
		return c.Flush()
	}
	return nil
}

func (c *codeBuddyReasoningCoalescer) flushWith(line []byte) [][]byte {
	out := c.Flush()
	if len(bytes.TrimSpace(line)) > 0 {
		out = append(out, bytes.Clone(line))
	}
	return out
}

func (c *codeBuddyReasoningCoalescer) Flush() [][]byte {
	if len(c.pending) == 0 {
		return nil
	}
	out := [][]byte{c.pending}
	c.pending = nil
	c.reasoningKey = ""
	c.length = 0
	return out
}

func codeBuddySSEPayload(line []byte) []byte {
	trimmed := bytes.TrimSpace(line)
	if bytes.HasPrefix(trimmed, []byte("data:")) {
		return bytes.TrimSpace(trimmed[len("data:"):])
	}
	return trimmed
}

func codeBuddyReasoningDelta(line []byte) (string, string, bool) {
	payload := codeBuddySSEPayload(line)
	if !gjson.ValidBytes(payload) || bytes.Equal(payload, []byte("[DONE]")) {
		return "", "", false
	}
	choices := gjson.GetBytes(payload, "choices")
	if !choices.IsArray() || len(choices.Array()) != 1 {
		return "", "", false
	}
	delta := choices.Get("0.delta")
	toolCalls := delta.Get("tool_calls")
	if !delta.Exists() ||
		(delta.Get("content").Type == gjson.String && delta.Get("content").String() != "") ||
		(toolCalls.Exists() && len(toolCalls.Array()) > 0) {
		return "", "", false
	}
	for _, key := range []string{"reasoning_content", "reasoning"} {
		value := delta.Get(key)
		if value.Type == gjson.String && value.String() != "" {
			return value.String(), key, true
		}
	}
	return "", "", false
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

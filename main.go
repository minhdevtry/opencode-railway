package main

import (
	"bytes"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ======================== CẤU HÌNH & HỒ MÔ HÌNH ========================

const (
	defaultPort = "8080"
	baseZenURL  = "https://opencode.ai/zen/v1"
	baseInfURL  = "https://opencode.ai/zen/v1"
)

var (
	poolMu        sync.RWMutex
	defaultSolidModels = []string{
		"glm-5.3-flash-free",
		"deepseek-v4.1-flash-free",
		"nemotron-3.5-lightning-free",
		"big-pickle",
		"mimo-v2.6-flash-free",
		"beam-free",
	}
	freeModelPool = []string{
		"glm-5.3-flash-free",
		"deepseek-v4.1-flash-free",
		"nemotron-3.5-lightning-free",
		"big-pickle",
		"mimo-v2.6-flash-free",
		"beam-free",
	}
)

func initFreeModelPool(custom string) []string {
	if custom != "" {
		list := splitList(custom)
		if len(list) > 0 {
			var normalized []string
			for _, m := range list {
				m = strings.ToLower(strings.TrimSpace(m))
				if !strings.HasSuffix(m, "-free") && m != "big-pickle" {
					m += "-free"
				}
				normalized = append(normalized, m)
			}
			return normalized
		}
	}
	cp := make([]string, len(defaultSolidModels))
	copy(cp, defaultSolidModels)
	return cp
}

// reconcilePool giao pool hiện tại với danh sách model Free đang sống ở upstream.
//
// custom != "" ⇒ pool ĐÓNG: chỉ giữ đúng các model người dùng liệt kê (và còn
// sống). Không gộp thêm model khác — nếu gộp thì AUTO_MODELS trở nên vô nghĩa và
// các model phế quay lại pool sau mỗi lần đồng bộ (đo 24/10: pool mặc định 6
// model bị phình thành 7 vì gộp nemotron-3.5-super-vl-free).
//
// custom == "" ⇒ pool MỞ: giữ các model đang có và gộp thêm model Free mới của
// upstream (trừ jev-1.13-free và flint-40b-free).
func reconcilePool(current []string, active map[string]bool, custom string) []string {
	var live []string
	for _, m := range current {
		if active[m] {
			live = append(live, m)
		}
	}
	if custom != "" {
		return live
	}
	for m := range active {
		if m == "jev-1.13-free" || m == "flint-40b-free" {
			continue
		}
		found := false
		for _, p := range live {
			if p == m {
				found = true
				break
			}
		}
		if !found {
			live = append(live, m)
		}
	}
	return live
}

func prunePool(model string) {
	poolMu.Lock()
	defer poolMu.Unlock()
	var next []string
	deadBase := strings.TrimSuffix(model, "-free")
	for _, m := range freeModelPool {
		if m != model && strings.TrimSuffix(m, "-free") != deadBase {
			next = append(next, m)
		}
	}
	if len(next) > 0 {
		freeModelPool = next
	}
}
func startCatalogSync(ctx context.Context) {
	// Đồng bộ ngay khi khởi động
	syncCatalog()
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			syncCatalog()
		}
	}
}

func syncCatalog() {
	req, err := http.NewRequest("GET", "https://opencode.ai/zen/v1/models", nil)
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer public")
	req.Header.Set("User-Agent", "opencode/1.19.4")
	var client *http.Client
	if mgr != nil {
		snap := mgr.snapshot()
		if len(snap) > 0 {
			client = snap[0].client.Load()
		}
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		return
	}
	defer resp.Body.Close()
	var env struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if json.NewDecoder(resp.Body).Decode(&env) != nil || len(env.Data) == 0 {
		return
	}
	active := map[string]bool{}
	for _, m := range env.Data {
		if strings.HasSuffix(m.ID, "-free") || m.ID == "big-pickle" {
			active[m.ID] = true
		}
	}
	poolMu.Lock()
	defer poolMu.Unlock()
	live := reconcilePool(freeModelPool, active, cfg.AutoModels)
	if len(live) > 0 {
		freeModelPool = live
		log.Printf("[CatalogSync] Tự động đồng bộ pool theo upstream: %v", live)
	}
}


var (
	cfg            Config
	mgr            *Manager
	grd            *guard
	poolIndex      uint32
	concurrencySem chan struct{}
	inflightTotal  atomic.Int32

	// có thể ghi đè trong test
	infURL  = baseInfURL
	zenURL  = baseZenURL
	started = time.Now()
)

func isAutoModel(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "auto", "auto-free", "free", "pool", "free-pool", "fast-free":
		return true
	}
	return false
}

func resolveModelName(name string) (resolved string, isPool bool) {
	clean := strings.ToLower(strings.TrimSpace(name))
	if rep := getDynamicAlias(clean); rep != "" {
		clean = rep
	}
	poolMu.RLock()
	defer poolMu.RUnlock()
	if isAutoModel(clean) {
		return freeModelPool[int(atomic.AddUint32(&poolIndex, 1))%len(freeModelPool)], true
	}
	switch clean {
	case "big-pickle":
		return "big-pickle", false
	case "nemotron", "nemotron-3.5-lightning":
		return "nemotron-3.5-lightning-free", false
	case "cat-jam":
		return "cat-jam-free", false
	case "space-bunny":
		return "space-bunny-free", false
	case "minimax", "minimax-m3.1", "minimax-m3.1-flash", "minimax-m3.1-flash-free":
		return "deepseek-v4.1-flash-free", false
	case "mimo", "mimo-v2.6-flash":
		return "mimo-v2.6-flash-free", false
	case "deepseek", "deepseek-v4.1-flash", "deepseek-v4.1", "deepseek-flash", "deepseek-chat", "deepseek-reasoner":
		return "deepseek-v4.1-flash-free", false
	case "jev", "jev-1.13":
		return "jev-1.13-free", false
	case "step-5", "step-5-preview":
		return "step-5-preview-free", false
	case "longcat", "longcat-2.5", "longcat-2.5-preview":
		return "longcat-2.5-preview-free", false
	}
	if !strings.HasSuffix(clean, "-free") {
		for _, p := range freeModelPool {
			if strings.TrimSuffix(p, "-free") == clean {
				return p, false
			}
		}
	}
	// Hỗ trợ alias linh hoạt: claude-*, gpt-*, o1*, o3*, sonnet*, opus*, haiku* -> trỏ về defaultModel (deepseek)
	if strings.HasPrefix(clean, "claude") || strings.HasPrefix(clean, "gpt-") ||
		strings.HasPrefix(clean, "o1") || strings.HasPrefix(clean, "o3") ||
		strings.HasPrefix(clean, "sonnet") || strings.HasPrefix(clean, "opus") || strings.HasPrefix(clean, "haiku") {
		def := cfg.DefaultModel
		if def == "" {
			def = "deepseek"
		}
		if def == "deepseek" || def == "deepseek-v4.1-flash" {
			return "deepseek-v4.1-flash-free", false
		}
		return def, false
	}
	return clean, false
}

// applyFingerprint ép stream=true (upstream chỉ nhận stream) và bổ sung bộ tool bắt buộc của opencode
// nếu client chưa khai báo. inject=false thì giữ nguyên tools của client.
func applyFingerprint(body map[string]any, model string, inject bool, stubDesc string) {
	body["stream"] = true
	if !inject {
		if strings.HasPrefix(model, "cat-jam") || strings.HasPrefix(model, "beam") || strings.HasPrefix(model, "space-bunny") || strings.HasPrefix(model, "glm") {
			delete(body, "cache_control")
		}
		return
	}

	d := func(name string) string {
		if stubDesc != "" {
			return stubDesc
		}
		return name
	}
	mandatory := []map[string]any{
		{"type": "function", "function": map[string]any{"name": "bash", "description": d("bash"), "strict": false, "parameters": map[string]any{"type": "object", "properties": map[string]any{"command": map[string]any{"type": "string"}}, "required": []string{"command"}}}},
		{"type": "function", "function": map[string]any{"name": "glob", "description": d("glob"), "strict": false, "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
		{"type": "function", "function": map[string]any{"name": "grep", "description": d("grep"), "strict": false, "parameters": map[string]any{"type": "object", "properties": map[string]any{"pattern": map[string]any{"type": "string"}}, "required": []string{"pattern"}}}},
		{"type": "function", "function": map[string]any{"name": "read", "description": d("read"), "strict": false, "parameters": map[string]any{"type": "object", "properties": map[string]any{"filePath": map[string]any{"type": "string"}}, "required": []string{"filePath"}}}},
	}

	raw, has := body["tools"]
	if !has || raw == nil {
		body["tools"] = mandatory
	} else {
		var combined []any
		names := make(map[string]bool)
		if s, ok := raw.([]any); ok {
			for _, t := range s {
				if tm, ok := t.(map[string]any); ok {
					if fn, ok := tm["function"].(map[string]any); ok {
						if n, ok := fn["name"].(string); ok {
							names[n] = true
						}
					}
					combined = append(combined, t)
				}
			}
		}
		for _, mt := range mandatory {
			if n := mt["function"].(map[string]any)["name"].(string); !names[n] {
				combined = append(combined, mt)
			}
		}
		body["tools"] = combined
	}
	if strings.HasPrefix(model, "cat-jam") || strings.HasPrefix(model, "beam") || strings.HasPrefix(model, "space-bunny") || strings.HasPrefix(model, "glm") {
		delete(body, "cache_control")
	}
}
func jsonMarshalNoEscape(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}


func setCORS(w http.ResponseWriter) {
	if cfg.CORSOrigin == "" {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", cfg.CORSOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, X-Session-Id, x-api-key, anthropic-version, anthropic-beta")
}

// ======================== SSE PARSE / AGGREGATE ========================

type oaToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaChunk struct {
	ID      string `json:"id"`
	Choices []struct {
		Delta struct {
			Content          string       `json:"content"`
			ReasoningContent string       `json:"reasoning_content"`
			ToolCalls        []oaToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens        int `json:"prompt_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails struct {
			CachedTokens int `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

// readSSE gọi fn cho từng payload "data:" (bỏ [DONE]); không giới hạn độ dài dòng.
func readSSE(r io.Reader, fn func(payload string) error) error {
	br := bufio.NewReaderSize(r, 32*1024)
	for {
		line, err := br.ReadString('\n')
		text := strings.TrimSpace(line)
		if strings.HasPrefix(text, "data:") {
			payload := strings.TrimSpace(strings.TrimPrefix(text, "data:"))
			if payload == "[DONE]" {
				return nil
			}
			if payload != "" {
				if ferr := fn(payload); ferr != nil {
					return ferr
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

type aggTool struct {
	id, name string
	args     strings.Builder
}

type aggregate struct {
	id           string
	content      strings.Builder
	reasoning    strings.Builder
	tools        map[int]*aggTool
	finishReason string
	promptTok    int
	complTok     int
	cachedTok    int
	chunks       int
}

func aggregateSSE(r io.Reader) (*aggregate, error) {
	agg := &aggregate{tools: map[int]*aggTool{}}
	err := readSSE(r, func(payload string) error {
		var ch oaChunk
		if json.Unmarshal([]byte(payload), &ch) != nil {
			return nil
		}
		agg.chunks++
		if ch.ID != "" {
			agg.id = ch.ID
		}
		if ch.Usage != nil {
			agg.promptTok, agg.complTok = ch.Usage.PromptTokens, ch.Usage.CompletionTokens
			agg.cachedTok = ch.Usage.PromptTokensDetails.CachedTokens
		}
		if len(ch.Choices) == 0 {
			return nil
		}
		c := ch.Choices[0]
		agg.content.WriteString(c.Delta.Content)
		agg.reasoning.WriteString(c.Delta.ReasoningContent)
		if c.FinishReason != nil && *c.FinishReason != "" {
			agg.finishReason = *c.FinishReason
		}
		for _, tc := range c.Delta.ToolCalls {
			t := agg.tools[tc.Index]
			if t == nil {
				t = &aggTool{}
				agg.tools[tc.Index] = t
			}
			if tc.ID != "" {
				t.id = tc.ID
			}
			if tc.Function.Name != "" {
				t.name = tc.Function.Name
			}
			t.args.WriteString(tc.Function.Arguments)
		}
		return nil
	})
	return agg, err
}

func (a *aggregate) orderedTools() []*aggTool {
	var out []*aggTool
	for i := 0; i < len(a.tools); i++ {
		if t, ok := a.tools[i]; ok {
			out = append(out, t)
		}
	}
	return out
}

// ======================== HTTP HELPERS ========================

func writeGWError(w http.ResponseWriter, e *gwError) {
	if e.retryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprint(int(e.retryAfter.Seconds())+1))
	}
	writeJSONError(w, e.status, e.typ, e.msg)
}

func setGWHeaders(w http.ResponseWriter, up *upstream) {
	w.Header().Set("X-Gateway-Egress", up.egress.Name)
	w.Header().Set("X-Gateway-Model", up.model)
}

func readBody(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, cfg.MaxBodyBytes))
	if err != nil {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "Request body too large or unreadable")
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "Invalid JSON")
		return nil, false
	}
	return m, true
}

// sessionKey nhận diện hội thoại của client để giữ session ID/egress ổn định (cache prefix):
// Claude Code gửi X-Claude-Code-Session-Id và metadata.user_id.session_id; client OpenAI có thể gửi X-Session-Id hoặc user.
func sessionKey(r *http.Request, body map[string]any) string {
	for _, h := range []string{"X-Claude-Code-Session-Id", "X-Session-Id", "X-Opencode-Session"} {
		if v := strings.TrimSpace(r.Header.Get(h)); v != "" {
			return v
		}
	}
	if md, ok := body["metadata"].(map[string]any); ok {
		if uid, ok := md["user_id"].(string); ok && uid != "" {
			var inner struct {
				SessionID string `json:"session_id"`
			}
			if json.Unmarshal([]byte(uid), &inner) == nil && inner.SessionID != "" {
				return inner.SessionID
			}
		}
	}
	if u, ok := body["user"].(string); ok {
		return strings.TrimSpace(u)
	}
	return ""
}

func acquireSlot(w http.ResponseWriter, r *http.Request) (release func(), ok bool) {
	t := time.NewTimer(cfg.QueueWait)
	defer t.Stop()
	select {
	case concurrencySem <- struct{}{}:
		inflightTotal.Add(1)
		return func() { <-concurrencySem; inflightTotal.Add(-1) }, true
	case <-t.C:
		w.Header().Set("Retry-After", "5")
		writeJSONError(w, http.StatusServiceUnavailable, "overloaded_error", "Gateway is at capacity, retry shortly")
		return nil, false
	case <-r.Context().Done():
		return nil, false
	}
}

func passthroughError(w http.ResponseWriter, up *upstream) {
	b, _ := io.ReadAll(io.LimitReader(up.resp.Body, 1<<20))
	setGWHeaders(w, up)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(up.resp.StatusCode)
	_, _ = w.Write(b)
}

func preflight(w http.ResponseWriter, r *http.Request) bool {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "invalid_request_error", "Method not allowed")
		return true
	}
	return false
}

// ======================== HANDLERS ========================

func chatHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	reqData, ok := readBody(w, r)
	if !ok {
		return
	}
	if ms, _ := reqData["messages"].([]any); len(ms) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "messages: at least one message is required")
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	modelIn, _ := reqData["model"].(string)
	clientStream, _ := reqData["stream"].(bool)
	target, isPool := resolveModelName(modelIn)
	t0 := time.Now()

	up, gerr := forward(r.Context(), target, isPool, infURL+"/chat/completions", "*/*", sessionKey(r, reqData), func(m string) []byte {
		reqData["model"] = m
		applyFingerprint(reqData, m, true, stubDescription)
		b, _ := jsonMarshalNoEscape(reqData)
		return b
	})
	if gerr != nil {
		writeGWError(w, gerr)
		return
	}
	defer up.close()
	if up.resp.StatusCode != http.StatusOK {
		passthroughError(w, up)
		return
	}
	setGWHeaders(w, up)

	if clientStream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, _ := w.(http.Flusher)
		br := bufio.NewReaderSize(up.resp.Body, 32*1024)
		var firstByte time.Duration
		for {
			line, err := br.ReadBytes('\n')
			if len(line) > 0 {
				if firstByte == 0 {
					firstByte = time.Since(t0)
				}
				if _, werr := w.Write(line); werr != nil {
					break
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if err != nil {
				break
			}
		}
		logReq("chat", "stream", up, t0, firstByte)
		return
	}

	agg, err := aggregateSSE(up.resp.Body)
	if err != nil && agg.chunks == 0 {
		writeJSONError(w, http.StatusBadGateway, "upstream_error", "Upstream stream interrupted")
		return
	}
	msg := map[string]any{
		"role":              "assistant",
		"content":           agg.content.String(),
		"reasoning_content": agg.reasoning.String(),
	}
	finish := "stop"
	if tools := agg.orderedTools(); len(tools) > 0 {
		var list []any
		for i, t := range tools {
			list = append(list, map[string]any{
				"index": i, "id": t.id, "type": "function",
				"function": map[string]any{"name": t.name, "arguments": t.args.String()},
			})
		}
		msg["tool_calls"] = list
		finish = "tool_calls"
	}
	out, _ := json.Marshal(map[string]any{
		"id": agg.id, "object": "chat.completion", "created": time.Now().Unix(), "model": up.model,
		"choices": []map[string]any{{"index": 0, "message": msg, "finish_reason": finish}},
		"usage": map[string]any{"prompt_tokens": agg.promptTok, "completion_tokens": agg.complTok, "total_tokens": agg.promptTok + agg.complTok,
			"prompt_tokens_details": map[string]any{"cached_tokens": agg.cachedTok}},
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
	logReqTok("chat", "json", up, t0, up.ttfb, agg.promptTok, agg.cachedTok)
}

func logReq(ep, mode string, up *upstream, t0 time.Time, first time.Duration) {
	logReqTok(ep, mode, up, t0, first, 0, 0)
}

// logReqTok ghi thêm số token prompt và token trúng cache (nếu upstream báo) để đo hiệu quả prompt cache.
func logReqTok(ep, mode string, up *upstream, t0 time.Time, first time.Duration, prompt, cached int) {
	log.Printf("[req] %s %s model=%s egress=%s ttfb=%v first=%v total=%v prompt=%d cached=%d",
		ep, mode, up.model, up.egress.Name, up.ttfb.Round(time.Millisecond), first.Round(time.Millisecond), time.Since(t0).Round(time.Millisecond), prompt, cached)
}

func systemoneHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	reqData, ok := readBody(w, r)
	if !ok {
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	model, _ := reqData["model"].(string)
	resolved, _ := resolveModelName(model)
	if resolved == "" || resolved == "jev" || resolved == "jev-1.13" {
		resolved = "jev-1.13-free"
	}
	reqData["model"] = resolved
	body, _ := json.Marshal(reqData)
	t0 := time.Now()

	up, gerr := forward(r.Context(), resolved, false, zenURL+"/systemone", "application/json", sessionKey(r, reqData), func(string) []byte { return body })
	if gerr != nil {
		writeGWError(w, gerr)
		return
	}
	defer up.close()
	setGWHeaders(w, up)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(up.resp.StatusCode)
	_, _ = io.Copy(w, up.resp.Body)
	logReq("systemone", "json", up, t0, up.ttfb)
}
func capacityHandler(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	snap := mgr.snapshot()
	proxiesCount := len(snap) - 1
	if proxiesCount < 0 {
		proxiesCount = 0
	}
	out, _ := json.Marshal(map[string]any{
		"object":        "systemone_capacity",
		"endpoint":      "/v1/systemone",
		"measured_env":  "Cloudflare WARP 16x cluster -> opencode.ai/zen",
		"measured_on":   "2026-10-24",
		"gate_capacity": cfg.MaxConcurrency,
		"state": map[string]any{
			"egress_pool_size": proxiesCount,
			"gate_capacity":    cfg.MaxConcurrency,
			"in_flight":        inflightTotal.Load(),
		},
		"usage": map[string]any{
			"recommended_in_flight": cfg.MaxConcurrency,
			"why":                   "Được điều phối tự động bởi scoring load balancer trên 16 proxy WARP độc lập.",
		},
	})
	_, _ = w.Write(out)
}


var modelList = []struct{ id, owner string }{
	{"auto-free", "auto-dispatcher"},
	{"auto", "auto-dispatcher"},
	{"deepseek", "deepseek"},
	{"deepseek-v4.1-flash", "deepseek"},
	{"glm-5.3-flash", "zhipu"},
	{"glm", "zhipu"},
	{"nemotron-3.5-lightning", "nvidia"},
	{"nemotron", "nvidia"},
	{"big-pickle", "opencode"},
	{"mimo-v2.6-flash", "xiaomi"},
	{"mimo", "xiaomi"},
	{"beam", "opencode"},
	{"nemotron-3.5-super-vl", "nvidia"},
	{"jev-1.13", "typesafe"},
}

func modelObj(id, owner string) map[string]any {
	clean := strings.ToLower(strings.TrimSpace(id))
	clean = strings.TrimSuffix(clean, "-free")

	name := id
	desc := "OpenCode catalog model " + id
	contextWin := 128000
	maxOutput := 8192
	inputMods := []string{"text"}
	vision := false
	reasoning := true
	efforts := []string{"minimal", "low", "medium", "high", "max"}

	switch clean {
	case "deepseek", "deepseek-v4.1", "deepseek-v4.1-flash":
		name = "DeepSeek V4.1 Flash"
		desc = "Top-tier code generation & reasoning model with 1M context window and adaptive thinking"
		contextWin = 1048576
		maxOutput = 65536
	case "glm", "glm-5.3", "glm-5.3-flash":
		name = "GLM 5.3 Flash"
		desc = "Fastest response time and highest concurrency throughput among free models"
		contextWin = 128000
		maxOutput = 65536
	case "nemotron", "nemotron-3.5-lightning":
		name = "Nemotron 3.5 Lightning"
		desc = "Low-latency lightweight reasoning model with ultra-consistent tail latency"
		contextWin = 128000
		maxOutput = 32000
	case "big-pickle":
		name = "Big Pickle"
		desc = "OpenCode internal zero-cost general-purpose model with high concurrency resilience"
		contextWin = 200000
		maxOutput = 64000
	case "mimo", "mimo-v2.6", "mimo-v2.6-flash":
		name = "Mimo V2.6 Flash"
		desc = "1M long-context flash reasoning model"
		contextWin = 1000000
		maxOutput = 65536
	case "beam":
		name = "Beam"
		desc = "Focused logical deduction and analytical processing model"
		contextWin = 128000
		maxOutput = 32000
		efforts = []string{"low", "medium", "high", "max"}
	case "nemotron-3.5-super-vl":
		name = "Nemotron 3.5 Super VL"
		desc = "Vision-Language multimodal model supporting image analysis and optical understanding"
		contextWin = 128000
		maxOutput = 32000
		inputMods = []string{"text", "image"}
		vision = true
	case "jev", "jev-1.13":
		name = "TypeSafe JEV 1.13"
		desc = "Specialized model for TypeSafe System One binary classification (/v1/systemone)"
		contextWin = 32768
		maxOutput = 8192
		reasoning = false
		efforts = nil
	case "auto", "auto-free":
		name = "Auto Free Dispatcher"
		desc = "Automated load-balanced proxy routing across the best available free models"
		contextWin = 1048576
		maxOutput = 65536
	default:
		if strings.Contains(clean, "vision") || strings.Contains(clean, "vl") || strings.Contains(clean, "image") {
			inputMods = []string{"text", "image"}
			vision = true
		}
	}

	return map[string]any{
		"id":                 id,
		"object":             "model",
		"created":            1735689600,
		"created_at":         "2025-01-01T00:00:00Z",
		"owned_by":           owner,
		"type":               "model",
		"display_name":       name,
		"name":               name,
		"description":        desc,
		"context_window":     contextWin,
		"max_output_tokens":  maxOutput,
		"input_modalities":   inputMods,
		"output_modalities":  []string{"text"},
		"capabilities": map[string]any{
			"vision":    vision,
			"tools":     clean != "jev" && clean != "jev-1.13",
			"reasoning": reasoning,
		},
		"thinking": map[string]any{
			"supported":     reasoning,
			"mode":          "anthropic-adaptive",
			"default_level": "high",
			"efforts":       efforts,
		},
		"pricing": map[string]any{
			"input":  0,
			"output": 0,
			"free":   true,
		},
	}
}
func modelsHandler(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if id := strings.TrimPrefix(r.URL.Path, "/v1/models/"); id != "" && id != r.URL.Path {
		want := anthropicModel(id)
		for _, m := range modelList {
			if m.id == want || m.id == strings.ToLower(id) {
				out, _ := json.Marshal(modelObj(m.id, m.owner))
				_, _ = w.Write(out)
				return
			}
		}
		writeAnthropicError(w, &gwError{status: http.StatusNotFound, typ: "not_found_error", msg: "model not found: " + id})
		return
	}
	var models []map[string]any
	for _, m := range modelList {
		models = append(models, modelObj(m.id, m.owner))
	}
	out, _ := json.Marshal(map[string]any{"object": "list", "data": models, "has_more": false,
		"first_id": modelList[0].id, "last_id": modelList[len(modelList)-1].id})
	_, _ = w.Write(out)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" && r.URL.Path != "/health" {
		writeJSONError(w, http.StatusNotFound, "not_found_error", "Unknown endpoint: "+r.URL.Path)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok","gateway":"opencode-railway"}`))
}

func statusHandler(w http.ResponseWriter, r *http.Request) {
	type egStat struct {
		Name      string             `json:"name"`
		Kind      string             `json:"kind"`
		Ready     bool               `json:"ready"`
		Inflight  int32              `json:"inflight"`
		Requests  int64              `json:"requests"`
		OK        int64              `json:"ok"`
		RateLimit int64              `json:"rate_limited"`
		Errors    int64              `json:"errors"`
		Cooldown  map[string]float64 `json:"cooldown_seconds,omitempty"`
	}
	var list []egStat
	for _, e := range mgr.snapshot() {
		s := egStat{Name: e.Name, Kind: e.Kind, Ready: e.ready.Load(), Inflight: e.inflight.Load(),
			Requests: e.reqs.Load(), OK: e.oks.Load(), RateLimit: e.r429.Load(), Errors: e.errs.Load()}
		e.mu.Lock()
		for model, c := range e.cool {
			if d := time.Until(c.until); d > 0 {
				if s.Cooldown == nil {
					s.Cooldown = map[string]float64{}
				}
				s.Cooldown[model] = d.Round(time.Second).Seconds()
			}
		}
		e.mu.Unlock()
		list = append(list, s)
	}
	out, _ := json.Marshal(map[string]any{
		"uptime_seconds": int(time.Since(started).Seconds()),
		"inflight":       inflightTotal.Load(),
		"max_inflight":   cfg.MaxConcurrency,
		"egress":         list,
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}

// ======================== MAIN ========================

func buildMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", grd.wrap(chatHandler, true))
	mux.HandleFunc("/v1/messages", grd.wrap(messagesHandler, true))
	mux.HandleFunc("/v1/responses", grd.wrap(responsesHandler, true))
	mux.HandleFunc("/v1/completions", grd.wrap(completionsHandler, true))
	mux.HandleFunc("/v1/systemone", grd.wrap(systemoneHandler, true))
	mux.HandleFunc("/v1/systemone/capacity", capacityHandler)
	mux.HandleFunc("/v1/messages/count_tokens", grd.wrap(countTokensHandler, false))
	mux.HandleFunc("/v1/models", grd.wrap(modelsHandler, false))
	mux.HandleFunc("/v1/models/", grd.wrap(modelsHandler, false))
	mux.HandleFunc("/status", grd.wrap(statusHandler, false))
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", healthHandler)
	return mux
}

func main() {
	cfg = loadConfig()
	// Dọn dẹp các thư mục tạm tor cũ từ phiên trước nếu có
	if matches, err := filepath.Glob(filepath.Join(os.TempDir(), "tor-gw-*")); err == nil {
		for _, d := range matches {
			_ = os.RemoveAll(d)
		}
	}
	concurrencySem = make(chan struct{}, cfg.MaxConcurrency)

	allow, err := parseAllowlist(cfg.AllowedIPs)
	if err != nil {
		log.Fatalf("Cấu hình sai: %v", err)
	}
	grd = &guard{allow: allow, limiter: newIPLimiter(cfg.RateRPM, cfg.RateBurst), header: cfg.ClientIPHeader}
	switch {
	case allow.all:
		log.Printf("[Auth] ALLOWED_IPS=* => KHÔNG giới hạn IP (chỉ nên dùng khi đã có lớp bảo vệ khác)")
	case allow.empty():
		log.Printf("[Auth] ALLOWED_IPS trống => chỉ cho loopback và mạng nội bộ. Đặt ALLOWED_IPS=ip1,ip2,cidr để mở cho client khác")
	default:
		log.Printf("[Auth] ALLOWED_IPS: %d mục", len(allow.prefixes))
	}
	if cfg.ClientIPHeader != "" {
		log.Printf("[Auth] Lấy IP client từ header %s", cfg.ClientIPHeader)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	poolMu.Lock()
	freeModelPool = initFreeModelPool(cfg.AutoModels)
	poolMu.Unlock()
	log.Printf("[AutoModels] Pool model mặc định: %v", freeModelPool)

	mgr = newManager(ctx)
	mgr.addStatic(os.Getenv("TOR_INSTANCES"), os.Getenv("PROXY_POOL"), cfg.ResidentialProxies, cfg.ResidentialRefreshURLs)
	torAvailable = checkTorBinary()
	mode := "Tor trước (nhiều identity/daemon), direct dự phòng"
	if cfg.PreferDirect {
		mode = "direct trước, Tor khi bị 429"
	}
	switch {
	case cfg.DisableTor:
		mode = fmt.Sprintf("%d Proxies WARP (Tor đã vô hiệu hóa)", len(mgr.snapshot())-1)
	case cfg.ForceTor:
		mode = "chỉ Tor (FORCE_TOR)"
	case !torAvailable && len(mgr.snapshot()) == 1:
		mode = "chỉ direct"
	}
	if cfg.DisableTor {
		log.Printf("Egress: %s", mode)
	} else {
		log.Printf("Egress: %s | TOR_WARM=%d TOR_MAX=%d (tự thêm daemon khi tải > %d/daemon, tắt sau %v nhàn rỗi)", mode, cfg.TorWarm, cfg.TorMax, cfg.TorLoad, cfg.TorIdleStop)
	}
	go mgr.warmDirect()
	if torAvailable {
		go mgr.warmTor()
		go mgr.reapIdleTor()
		go mgr.autoscaleTor()
	}
	go startCatalogSync(ctx)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           buildMux(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	log.Printf("Opencode Railway Gateway listening on :%s (in-flight tối đa %d)", cfg.Port, cfg.MaxConcurrency)

	select {
	case err := <-errCh:
		log.Fatalf("Server error: %v", err)
	case <-ctx.Done():
		log.Printf("Nhận tín hiệu dừng, đang đóng...")
	}
	shutCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	mgr.wg.Wait()
}

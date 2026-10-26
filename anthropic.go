package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Endpoint tương thích Anthropic Messages API để Claude Code (và SDK Anthropic) dùng được trực tiếp:
//   POST /v1/messages                (stream + non-stream, tool_use, thinking, ảnh)
//   POST /v1/messages/count_tokens   (ước lượng)
//   GET  /v1/models, /v1/models/{id} (đồng thời đúng định dạng OpenAI và Anthropic)

const stubDescription = "Internal compatibility stub. Never call this tool; use the other tools provided instead."

var modelSuffixRe = regexp.MustCompile(`\[[^\]]*\]$`)
var totalTokensRe = regexp.MustCompile(`(?s)<total_tokens>.*?</total_tokens>\s*`)

// anthropicModel chuẩn hóa tên model từ Claude Code: "deepseek[1m]" -> "deepseek",
// "cline-free/deepseek-v4.1-flash" -> "deepseek-v4.1-flash", tên claude-* -> model mặc định.
func anthropicModel(name string) string {
	n := strings.ToLower(strings.TrimSpace(modelSuffixRe.ReplaceAllString(strings.TrimSpace(name), "")))
	if i := strings.LastIndex(n, "/"); i >= 0 {
		n = n[i+1:]
	}
	if n == "" || strings.HasPrefix(n, "claude") || strings.HasPrefix(n, "sonnet") ||
		strings.HasPrefix(n, "opus") || strings.HasPrefix(n, "haiku") || strings.HasPrefix(n, "gpt-") {
		if cfg.DefaultModel != "" {
			return cfg.DefaultModel
		}
		return envStr("DEFAULT_MODEL", "deepseek")
	}
	return n
}

func writeAnthropicError(w http.ResponseWriter, e *gwError) {
	if e.retryAfter > 0 {
		w.Header().Set("Retry-After", fmt.Sprint(int(e.retryAfter.Seconds())+1))
	}
	typ := e.typ
	switch e.status {
	case http.StatusTooManyRequests:
		typ = "rate_limit_error"
	case http.StatusBadGateway, http.StatusServiceUnavailable:
		typ = "overloaded_error"
	case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
		typ = "invalid_request_error"
	case http.StatusForbidden:
		typ = "permission_error"
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	b, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]any{"type": typ, "message": e.msg}})
	_, _ = w.Write(b)
}

// ---------- Anthropic request -> OpenAI chat ----------

// blockText gom text từ string hoặc mảng block (text); block khác được thay bằng ghi chú ngắn.
func blockText(v any) string {
	switch c := v.(type) {
	case string:
		return c
	case []any:
		var parts []string
		for _, b := range c {
			bm, _ := b.(map[string]any)
			switch bm["type"] {
			case "text":
				if s, _ := bm["text"].(string); s != "" {
					parts = append(parts, s)
				}
			case "image":
				parts = append(parts, "[image]")
			case "document":
				parts = append(parts, "[document]")
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// systemText gom system prompt, bỏ block x-anthropic-billing-header và thẻ <total_tokens>...
// (Claude Code đổi các giá trị này mỗi request, làm hỏng hoàn toàn cache prefix của KV cache upstream).
func systemText(v any) string {
	blocks, ok := v.([]any)
	if !ok {
		s := blockText(v)
		if strings.Contains(s, "<total_tokens>") {
			s = strings.TrimSpace(totalTokensRe.ReplaceAllString(s, ""))
		}
		return s
	}
	var kept []any
	for _, b := range blocks {
		if bm, _ := b.(map[string]any); bm != nil {
			t, _ := bm["text"].(string)
			trimmed := strings.TrimSpace(t)
			if strings.HasPrefix(trimmed, "x-anthropic-billing-header") {
				continue
			}
			if strings.HasPrefix(trimmed, "<total_tokens>") && strings.HasSuffix(trimmed, "</total_tokens>") {
				continue
			}
			if strings.Contains(t, "<total_tokens>") {
				t = strings.TrimSpace(totalTokensRe.ReplaceAllString(t, ""))
				if t == "" {
					continue
				}
				b = map[string]any{"type": "text", "text": t}
			}
		}
		kept = append(kept, b)
	}
	return blockText(kept)
}

func imageURL(bm map[string]any) string {
	src, _ := bm["source"].(map[string]any)
	switch src["type"] {
	case "base64":
		mt, _ := src["media_type"].(string)
		data, _ := src["data"].(string)
		return "data:" + mt + ";base64," + data
	case "url":
		u, _ := src["url"].(string)
		return u
	}
	return ""
}

func anthropicToOpenAI(req map[string]any, model string) map[string]any {
	out := map[string]any{"model": model}
	var msgs []any

	if sys := systemText(req["system"]); sys != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": sys})
	}

	rawMsgs, _ := req["messages"].([]any)
	for _, rm := range rawMsgs {
		m, _ := rm.(map[string]any)
		role, _ := m["role"].(string)
		if s, ok := m["content"].(string); ok {
			msgs = append(msgs, map[string]any{"role": role, "content": s})
			continue
		}
		blocks, _ := m["content"].([]any)

		if role == "assistant" {
			var text, thinking []string
			var calls []any
			for _, b := range blocks {
				bm, _ := b.(map[string]any)
				switch bm["type"] {
				case "text":
					if s, _ := bm["text"].(string); s != "" {
						text = append(text, s)
					}
				case "thinking":
					if s, _ := bm["thinking"].(string); s != "" {
						thinking = append(thinking, s)
					}
				case "tool_use":
					args, _ := json.Marshal(bm["input"])
					if len(args) == 0 || string(args) == "null" {
						args = []byte("{}")
					}
					calls = append(calls, map[string]any{
						"id": bm["id"], "type": "function",
						"function": map[string]any{"name": bm["name"], "arguments": string(args)},
					})
				}
			}
			if len(text) == 0 && len(calls) == 0 { // chỉ có thinking: upstream không nhận assistant rỗng
				continue
			}
			am := map[string]any{"role": "assistant", "content": strings.Join(text, "\n")}
			if len(calls) > 0 {
				am["tool_calls"] = calls
			}
			if len(thinking) > 0 {
				am["reasoning_content"] = strings.Join(thinking, "\n")
			}
			msgs = append(msgs, am)
			continue
		}

		// user: tool_result thành message role=tool (phải đứng ngay sau assistant có tool_calls), phần còn lại thành 1 message user.
		var userParts []any
		var userText []string
		hasImage := false
		for _, b := range blocks {
			bm, _ := b.(map[string]any)
			switch bm["type"] {
			case "tool_result":
				txt := blockText(bm["content"])
				if isErr, _ := bm["is_error"].(bool); isErr && txt != "" {
					txt = "Error: " + txt
				}
				if txt == "" {
					txt = "(no output)"
				}
				msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": bm["tool_use_id"], "content": txt})
			case "text":
				if s, _ := bm["text"].(string); s != "" {
					userText = append(userText, s)
					userParts = append(userParts, map[string]any{"type": "text", "text": s})
				}
			case "image":
				if cfg.ImageModels[model] {
					if u := imageURL(bm); u != "" {
						hasImage = true
						userParts = append(userParts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}})
					}
				} else { // model chỉ nhận text: thay bằng ghi chú thay vì để upstream trả 400
					userText = append(userText, "[image attached]")
					userParts = append(userParts, map[string]any{"type": "text", "text": "[image attached]"})
				}
			case "document":
				userText = append(userText, "[document omitted]")
				userParts = append(userParts, map[string]any{"type": "text", "text": "[document omitted]"})
			}
		}
		if hasImage {
			msgs = append(msgs, map[string]any{"role": "user", "content": userParts})
		} else if len(userText) > 0 {
			msgs = append(msgs, map[string]any{"role": "user", "content": strings.Join(userText, "\n")})
		}
	}
	out["messages"] = normalizeToolPairs(msgs)

	if tools, ok := req["tools"].([]any); ok {
		var list []any
		for _, t := range tools {
			tm, _ := t.(map[string]any)
			name, _ := tm["name"].(string)
			schema, ok := tm["input_schema"]
			if name == "" || !ok { // server tool (web_search...) không map được
				continue
			}
			list = append(list, map[string]any{"type": "function", "function": map[string]any{
				"name": name, "description": tm["description"], "parameters": schema,
			}})
		}
		if len(list) > 0 {
			out["tools"] = list
		}
	}
	if tc, ok := req["tool_choice"].(map[string]any); ok && out["tools"] != nil {
		switch tc["type"] {
		case "auto":
			out["tool_choice"] = "auto"
		case "any":
			out["tool_choice"] = "required"
		case "tool":
			out["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": tc["name"]}}
		case "none":
			out["tool_choice"] = "none"
		}
	}
	for _, k := range []string{"max_tokens", "temperature", "top_p"} {
		if v, ok := req[k]; ok {
			out[k] = v
		}
	}
	if s, ok := req["stop_sequences"]; ok {
		out["stop"] = s
	}
	// Suy luận: client Anthropic (Claude Code, omp) gửi mức suy luận ở
	// output_config.effort (kèm thinking.effort) chứ không phải
	// reasoning_effort của Chat Completions. Map sang reasoning_effort để
	// upstream thực sự điều chỉnh độ sâu suy luận.
	if effort := claudeReasoningEffort(req); effort != "" {
		out["reasoning_effort"] = effort
	}
	return out
}

// claudeReasoningEffort trích mức suy luận từ body Anthropic Messages:
// ưu tiên output_config.effort (nơi Claude Code/omp đặt --effort), sau đó
// thinking.effort. Trả "" khi thinking bị tắt hoặc không có effort hợp lệ.
func claudeReasoningEffort(req map[string]any) string {
	th, _ := req["thinking"].(map[string]any)
	if th != nil {
		if t, _ := th["type"].(string); t == "disabled" {
			return ""
		}
	}
	effort := ""
	if oc, ok := req["output_config"].(map[string]any); ok {
		effort, _ = oc["effort"].(string)
	}
	if effort == "" && th != nil {
		effort, _ = th["effort"].(string)
	}
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "minimal", "low", "medium", "high", "xhigh", "max":
		return strings.ToLower(strings.TrimSpace(effort))
	}
	return ""
}

func estimateTokens(v any) int {
	b, _ := json.Marshal(v)
	return len(b)/4 + 1
}

func mapStopReason(finish string, sawTool bool) string {
	switch finish {
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call":
		return "tool_use"
	}
	if sawTool {
		return "tool_use"
	}
	return "end_turn"
}

// normalizeToolPairs bỏ tool_call không có kết quả và kết quả mồ côi (hội thoại bị ngắt/compact) vì upstream trả 400 với cặp lệch.
func normalizeToolPairs(msgs []any) []any {
	results := map[string]bool{}
	for _, m := range msgs {
		if mm, _ := m.(map[string]any); mm["role"] == "tool" {
			if id, _ := mm["tool_call_id"].(string); id != "" {
				results[id] = true
			}
		}
	}
	kept := map[string]bool{}
	var out []any
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		if mm["role"] == "assistant" {
			if calls, ok := mm["tool_calls"].([]any); ok {
				var ok2 []any
				for _, c := range calls {
					cm, _ := c.(map[string]any)
					if id, _ := cm["id"].(string); results[id] {
						ok2 = append(ok2, c)
						kept[id] = true
					}
				}
				if len(ok2) == 0 {
					delete(mm, "tool_calls")
					if s, _ := mm["content"].(string); s == "" {
						continue
					}
				} else {
					mm["tool_calls"] = ok2
				}
			}
		}
		if mm["role"] == "tool" {
			if id, _ := mm["tool_call_id"].(string); !kept[id] {
				continue
			}
		}
		out = append(out, m)
	}
	return out
}

// ---------- Dịch ngược tool stub -> tool của client ----------

type clientTool struct {
	name  string
	props map[string]bool
}

func collectClientTools(req map[string]any) map[string]clientTool {
	out := map[string]clientTool{}
	tools, _ := req["tools"].([]any)
	for _, t := range tools {
		tm, _ := t.(map[string]any)
		name, _ := tm["name"].(string)
		if name == "" {
			continue
		}
		props := map[string]bool{}
		if sch, ok := tm["input_schema"].(map[string]any); ok {
			if pm, ok := sch["properties"].(map[string]any); ok {
				for k := range pm {
					props[k] = true
				}
			}
		}
		out[name] = clientTool{name: name, props: props}
	}
	return out
}

// remapStub: model lỡ gọi tool stub (bash/read/grep/glob) trong khi client có tool tương ứng viết hoa khác
// (Bash/Read/...) thì đổi tên về tool của client. rewrite != nil nếu cần sửa lại JSON tham số.
func remapStub(name string, tools map[string]clientTool) (string, func(string) string) {
	if _, exact := tools[name]; exact {
		return name, nil
	}
	switch name {
	case "bash", "read", "grep", "glob":
	default:
		return name, nil
	}
	for _, ct := range tools {
		if strings.EqualFold(ct.name, name) {
			if name == "read" && ct.props["file_path"] && !ct.props["filePath"] {
				return ct.name, renameArgKey("filePath", "file_path")
			}
			return ct.name, nil
		}
	}
	return name, nil
}

func renameArgKey(from, to string) func(string) string {
	return func(args string) string {
		var m map[string]any
		if json.Unmarshal([]byte(args), &m) != nil {
			return args
		}
		if v, ok := m[from]; ok {
			delete(m, from)
			m[to] = v
		}
		b, _ := json.Marshal(m)
		return string(b)
	}
}

// ---------- Handlers ----------

func countTokensHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	req, ok := readBody(w, r)
	if !ok {
		return
	}
	n := estimateTokens(map[string]any{"s": req["system"], "m": req["messages"], "t": req["tools"]})
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"input_tokens":%d}`, n)
}

func messagesHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	req, ok := readBody(w, r)
	if !ok {
		return
	}
	if ms, _ := req["messages"].([]any); len(ms) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "messages: at least one message is required")
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	modelIn, _ := req["model"].(string)
	clientStream, _ := req["stream"].(bool)
	target, isPool := resolveModelName(anthropicModel(modelIn))
	oa := anthropicToOpenAI(req, target)
	clientTools := collectClientTools(req)
	inputEst := estimateTokens(oa["messages"]) + estimateTokens(oa["tools"])
	t0 := time.Now()

	up, gerr := forward(withMaxWait(withFallback(withProfile(r.Context(), anthropicModel(modelIn)), r), r), target, isPool, infURL+"/chat/completions", "*/*", sessionKey(r, req), func(m string) []byte {
		oa["model"] = m
		oa["stream_options"] = map[string]any{"include_usage": true}
		applyFingerprint(oa, m, cfg.InjectToolsAnthropic, stubDescription)
		b, _ := jsonMarshalNoEscape(oa)
		return b
	})
	if gerr != nil {
		writeAnthropicError(w, gerr)
		return
	}
	defer up.close()
	if up.resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(up.resp.Body, 1<<20))
		msg := strings.TrimSpace(string(b))
		var env struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &env) == nil && env.Error.Message != "" {
			msg = env.Error.Message
		}
		log.Printf("[req] messages upstream HTTP %d egress=%s model=%s body=%q", up.resp.StatusCode, up.egress.Name, up.model, truncate(string(b), 300))
		setGWHeaders(w, up)
		writeAnthropicError(w, &gwError{status: up.resp.StatusCode, typ: "api_error", msg: msg})
		return
	}
	setGWHeaders(w, up)
	msgID := "msg_" + randomHex(24)

	if !clientStream {
		agg, err := aggregateSSE(up.resp.Body)
		// Không có finish_reason (stream đứt giữa chừng, kể cả EOF "sạch" không có [DONE]) => nội dung có thể bị cụt:
		// báo lỗi để client thử lại, KHÔNG trả 200 với câu trả lời cụt.
		if agg.chunks == 0 || agg.finishReason == "" {
			if err != nil {
				log.Printf("[messages] stream đứt: %v", err)
			}
			writeAnthropicError(w, &gwError{status: http.StatusBadGateway, typ: "overloaded_error", msg: "Upstream stream interrupted"})
			return
		}
		var content []any
		if t := agg.reasoning.String(); t != "" {
			content = append(content, map[string]any{"type": "thinking", "thinking": t, "signature": ""})
		}
		if t := agg.content.String(); t != "" || len(agg.tools) == 0 {
			content = append(content, map[string]any{"type": "text", "text": t})
		}
		tools := agg.orderedTools()
		for _, t := range tools {
			var input any = map[string]any{}
			if a := strings.TrimSpace(t.args.String()); a != "" {
				if json.Unmarshal([]byte(a), &input) != nil {
					input = map[string]any{}
				}
			}
			id := t.id
			if id == "" {
				id = "toolu_" + randomHex(12)
			}
			name := t.name
			if nn, rw := remapStub(name, clientTools); nn != name {
				name = nn
				if rw != nil {
					if m, ok := input.(map[string]any); ok {
						if b, err := json.Marshal(m); err == nil {
							_ = json.Unmarshal([]byte(rw(string(b))), &input)
						}
					}
				}
			}
			content = append(content, map[string]any{"type": "tool_use", "id": id, "name": name, "input": input})
		}
		in, outTok := agg.promptTok, agg.complTok
		if in == 0 {
			in = inputEst
		}
		if outTok == 0 {
			outTok = len(agg.content.String())/4 + len(agg.reasoning.String())/4 + 1
		}
		b, _ := json.Marshal(map[string]any{
			"id": msgID, "type": "message", "role": "assistant", "model": modelIn, "content": content,
			"stop_reason": mapStopReason(agg.finishReason, len(tools) > 0), "stop_sequence": nil,
			"usage": map[string]any{"input_tokens": in - agg.cachedTok, "output_tokens": outTok, "cache_read_input_tokens": agg.cachedTok, "cache_creation_input_tokens": 0},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
		logReqTok("messages", "json", up, t0, up.ttfb, in, agg.cachedTok)
		return
	}

	// Stream: dịch từng chunk OpenAI thành sự kiện Anthropic.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)
	st := &anthStream{w: w, fl: flusher, model: modelIn, id: msgID, idx: -1, toolBlock: map[int]int{}, inputEst: inputEst, clientTools: clientTools}
	st.start()
	var firstByte time.Duration
	err := readSSE(up.resp.Body, func(payload string) error {
		var ch oaChunk
		if json.Unmarshal([]byte(payload), &ch) != nil {
			return nil
		}
		if firstByte == 0 {
			firstByte = time.Since(t0)
		}
		return st.chunk(&ch)
	})
	if st.finish_ == "" {
		// upstream đứt giữa chừng (chưa có finish_reason): báo lỗi để client retry, không giả làm câu trả lời hoàn chỉnh.
		// Điều kiện KHÔNG phụ thuộc err: EOF "sạch" không có [DONE] cũng là stream đứt.
		if err != nil {
			log.Printf("[messages] stream đứt: %v", err)
		}
		st.fail("Upstream stream interrupted")
	} else {
		st.finish()
	}
	logReqTok("messages", "stream", up, t0, firstByte, st.inTok, st.cachedTok)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------- Stream translator ----------

type anthStream struct {
	w     http.ResponseWriter
	fl    http.Flusher
	model string
	id    string

	idx       int    // index block hiện tại
	open      string // "", "text", "thinking", "tool"
	curTool   int    // chỉ số tool OpenAI của block tool đang mở
	toolBlock map[int]int
	sawTool   bool
	text      int // số byte đã sinh, để ước lượng token khi upstream không trả usage
	finish_   string
	inTok     int
	outTok    int
	cachedTok int
	inputEst  int
	done      bool

	clientTools map[string]clientTool
	rewrite     func(string) string // sửa JSON tham số của tool đang mở (chỉ gửi 1 lần khi đóng block)
	pending     strings.Builder
}

func (s *anthStream) send(event string, v any) {
	b, _ := json.Marshal(v)
	fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", event, b)
	if s.fl != nil {
		s.fl.Flush()
	}
}

func (s *anthStream) start() {
	s.send("message_start", map[string]any{"type": "message_start", "message": map[string]any{
		"id": s.id, "type": "message", "role": "assistant", "model": s.model, "content": []any{},
		"stop_reason": nil, "stop_sequence": nil,
		"usage": map[string]any{"input_tokens": s.inputEst, "output_tokens": 1},
	}})
	s.send("ping", map[string]any{"type": "ping"})
}

func (s *anthStream) closeBlock() {
	if s.open == "" {
		return
	}
	if s.open == "tool" && s.rewrite != nil {
		s.send("content_block_delta", map[string]any{"type": "content_block_delta", "index": s.idx,
			"delta": map[string]any{"type": "input_json_delta", "partial_json": s.rewrite(s.pending.String())}})
		s.rewrite = nil
		s.pending.Reset()
	}
	if s.open == "thinking" {
		s.send("content_block_delta", map[string]any{"type": "content_block_delta", "index": s.idx,
			"delta": map[string]any{"type": "signature_delta", "signature": ""}})
	}
	s.send("content_block_stop", map[string]any{"type": "content_block_stop", "index": s.idx})
	s.open = ""
}

func (s *anthStream) openBlock(kind string, block map[string]any) {
	s.closeBlock()
	s.idx++
	s.open = kind
	s.send("content_block_start", map[string]any{"type": "content_block_start", "index": s.idx, "content_block": block})
}

func (s *anthStream) sawContent() bool { return s.idx >= 0 }

func (s *anthStream) chunk(ch *oaChunk) error {
	if ch.Usage != nil {
		s.inTok, s.outTok = ch.Usage.PromptTokens, ch.Usage.CompletionTokens
		s.cachedTok = ch.Usage.PromptTokensDetails.CachedTokens
	}
	if len(ch.Choices) == 0 {
		return nil
	}
	c := ch.Choices[0]
	if r := c.Delta.reasoningText(); r != "" {
		if s.open != "thinking" {
			s.openBlock("thinking", map[string]any{"type": "thinking", "thinking": ""})
		}
		s.text += len(r)
		s.send("content_block_delta", map[string]any{"type": "content_block_delta", "index": s.idx,
			"delta": map[string]any{"type": "thinking_delta", "thinking": r}})
	}
	if t := c.Delta.Content; t != "" {
		if s.open != "text" {
			s.openBlock("text", map[string]any{"type": "text", "text": ""})
		}
		s.text += len(t)
		s.send("content_block_delta", map[string]any{"type": "content_block_delta", "index": s.idx,
			"delta": map[string]any{"type": "text_delta", "text": t}})
	}
	for _, tc := range c.Delta.ToolCalls {
		if _, seen := s.toolBlock[tc.Index]; !seen {
			id := tc.ID
			if id == "" {
				id = "toolu_" + randomHex(12)
			}
			name, rw := remapStub(tc.Function.Name, s.clientTools)
			s.openBlock("tool", map[string]any{"type": "tool_use", "id": id, "name": name, "input": map[string]any{}})
			s.rewrite = rw
			s.toolBlock[tc.Index] = s.idx
			s.curTool = tc.Index
			s.sawTool = true
		}
		if a := tc.Function.Arguments; a != "" && s.open == "tool" && s.curTool == tc.Index {
			s.text += len(a)
			if s.rewrite != nil {
				s.pending.WriteString(a)
			} else {
				s.send("content_block_delta", map[string]any{"type": "content_block_delta", "index": s.idx,
					"delta": map[string]any{"type": "input_json_delta", "partial_json": a}})
			}
		}
	}
	if c.FinishReason != nil && *c.FinishReason != "" {
		s.finish_ = *c.FinishReason
	}
	return nil
}

func (s *anthStream) finish() {
	if s.done {
		return
	}
	s.done = true
	if s.idx < 0 { // upstream không trả gì: vẫn phải có 1 block để client không lỗi
		s.openBlock("text", map[string]any{"type": "text", "text": ""})
	}
	s.closeBlock()
	out := s.outTok
	if out == 0 {
		out = s.text/4 + 1
	}
	s.send("message_delta", map[string]any{"type": "message_delta",
		"delta": map[string]any{"stop_reason": mapStopReason(s.finish_, s.sawTool), "stop_sequence": nil},
		"usage": map[string]any{"input_tokens": s.inTok - s.cachedTok, "output_tokens": out, "cache_read_input_tokens": s.cachedTok, "cache_creation_input_tokens": 0}})
	s.send("message_stop", map[string]any{"type": "message_stop"})
}

func (s *anthStream) fail(msg string) {
	if s.done {
		return
	}
	s.done = true
	s.closeBlock()
	s.send("error", map[string]any{"type": "error", "error": map[string]any{"type": "overloaded_error", "message": msg}})
}

var _ = bufio.NewReader

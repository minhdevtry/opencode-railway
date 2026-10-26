package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

// OpenAI Responses API (/v1/responses, dùng bởi Codex CLI và SDK mới) và Completions cũ (/v1/completions),
// dịch qua chat/completions của upstream.

func str(v any) string { s, _ := v.(string); return s }

// responsesContentToChat đổi phần content của 1 message Responses thành content kiểu chat.
func responsesContentToChat(c any) any {
	switch v := c.(type) {
	case string:
		return v
	case []any:
		var parts []any
		allText := true
		var sb strings.Builder
		for _, p := range v {
			pm, _ := p.(map[string]any)
			switch str(pm["type"]) {
			case "input_text", "output_text", "text":
				parts = append(parts, map[string]any{"type": "text", "text": str(pm["text"])})
				sb.WriteString(str(pm["text"]))
			case "input_image":
				allText = false
				url := str(pm["image_url"])
				if u, ok := pm["image_url"].(map[string]any); ok {
					url = str(u["url"])
				}
				if url != "" {
					parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": url}})
				}
			}
		}
		if allText {
			return sb.String()
		}
		return parts
	}
	return ""
}

func toolOutputText(o any) string {
	switch v := o.(type) {
	case string:
		return v
	case []any:
		var sb strings.Builder
		for _, p := range v {
			if pm, ok := p.(map[string]any); ok {
				sb.WriteString(str(pm["text"]))
			}
		}
		return sb.String()
	case nil:
		return ""
	}
	b, _ := json.Marshal(o)
	return string(b)
}

// responsesToChat dịch body /v1/responses sang body chat/completions.
func responsesToChat(req map[string]any) map[string]any {
	chat := map[string]any{"model": req["model"]}
	var msgs []any
	if ins := str(req["instructions"]); ins != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": ins})
	}
	switch in := req["input"].(type) {
	case string:
		msgs = append(msgs, map[string]any{"role": "user", "content": in})
	case []any:
		for _, it := range in {
			im, _ := it.(map[string]any)
			switch typ := str(im["type"]); {
			case typ == "function_call":
				call := map[string]any{"id": str(im["call_id"]), "type": "function",
					"function": map[string]any{"name": str(im["name"]), "arguments": str(im["arguments"])}}
				if n := len(msgs); n > 0 {
					if last, ok := msgs[n-1].(map[string]any); ok && last["role"] == "assistant" && last["tool_calls"] != nil {
						last["tool_calls"] = append(last["tool_calls"].([]any), call)
						continue
					}
				}
				msgs = append(msgs, map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{call}})
			case typ == "function_call_output":
				msgs = append(msgs, map[string]any{"role": "tool", "tool_call_id": str(im["call_id"]), "content": toolOutputText(im["output"])})
			case typ == "" || typ == "message":
				role := str(im["role"])
				if role == "developer" {
					role = "system"
				}
				if role == "" {
					role = "user"
				}
				msgs = append(msgs, map[string]any{"role": role, "content": responsesContentToChat(im["content"])})
			}
		}
	}
	chat["messages"] = msgs
	if ts, ok := req["tools"].([]any); ok {
		var tools []any
		for _, t := range ts {
			tm, _ := t.(map[string]any)
			if str(tm["type"]) != "function" || str(tm["name"]) == "" {
				continue
			}
			fn := map[string]any{"name": tm["name"]}
			if d, ok := tm["description"]; ok {
				fn["description"] = d
			}
			if p, ok := tm["parameters"]; ok && p != nil {
				fn["parameters"] = p
			} else {
				fn["parameters"] = map[string]any{"type": "object", "properties": map[string]any{}}
			}
			tools = append(tools, map[string]any{"type": "function", "function": fn})
		}
		if len(tools) > 0 {
			chat["tools"] = tools
		}
	}
	switch tc := req["tool_choice"].(type) {
	case string:
		chat["tool_choice"] = tc
	case map[string]any:
		if str(tc["type"]) == "function" && str(tc["name"]) != "" {
			chat["tool_choice"] = map[string]any{"type": "function", "function": map[string]any{"name": tc["name"]}}
		}
	}
	for _, k := range []string{"temperature", "top_p", "parallel_tool_calls", "user"} {
		if v, ok := req[k]; ok {
			chat[k] = v
		}
	}
	if v, ok := req["max_output_tokens"]; ok {
		chat["max_tokens"] = v
	}
	if tf, ok := req["text"].(map[string]any); ok {
		if f, ok := tf["format"].(map[string]any); ok && (str(f["type"]) == "json_object" || str(f["type"]) == "json_schema") {
			chat["response_format"] = map[string]any{"type": "json_object"}
		}
	}
	return chat
}

type respItem struct {
	kind     string // "message" | "function_call"
	id       string
	callID   string
	name     string
	text     strings.Builder
	args     strings.Builder
	chatIdx  int
	outIndex int
}

func (it *respItem) json(done bool) map[string]any {
	status := "in_progress"
	if done {
		status = "completed"
	}
	if it.kind == "message" {
		content := []any{}
		if done {
			content = append(content, map[string]any{"type": "output_text", "text": it.text.String(), "annotations": []any{}})
		}
		return map[string]any{"id": it.id, "type": "message", "role": "assistant", "status": status, "content": content}
	}
	a := ""
	if done {
		a = it.args.String()
	}
	return map[string]any{"id": it.id, "type": "function_call", "call_id": it.callID, "name": it.name, "arguments": a, "status": status}
}

func responseObject(id, model, status string, items []*respItem, a *aggregate) map[string]any {
	out := []any{}
	for _, it := range items {
		out = append(out, it.json(status == "completed"))
	}
	o := map[string]any{"id": id, "object": "response", "created_at": time.Now().Unix(), "status": status, "model": model,
		"output": out, "error": nil, "incomplete_details": nil, "parallel_tool_calls": true}
	if a != nil {
		o["usage"] = map[string]any{"input_tokens": a.promptTok, "output_tokens": a.complTok, "total_tokens": a.promptTok + a.complTok,
			"input_tokens_details": map[string]any{"cached_tokens": a.cachedTok}, "output_tokens_details": map[string]any{"reasoning_tokens": 0}}
	}
	return o
}

func responsesHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	req, ok := readBody(w, r)
	if !ok {
		return
	}
	if req["input"] == nil && req["instructions"] == nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "input: field required")
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	modelIn := str(req["model"])
	clientStream, _ := req["stream"].(bool)
	chat := responsesToChat(req)
	target, isPool := resolveModelName(modelIn)
	t0 := time.Now()
	up, gerr := forward(withMaxWait(withFallback(withProfile(r.Context(), modelIn), r), r), target, isPool, infURL+"/chat/completions", "*/*", sessionKey(r, req), func(m string) []byte {
		chat["model"] = m
		applyFingerprint(chat, m, true, stubDescription)
		b, _ := json.Marshal(chat)
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
	respID := "resp_" + randomHex(12)
	echoModel := modelIn
	if echoModel == "" {
		echoModel = up.model
	}

	if !clientStream {
		agg, err := aggregateSSE(up.resp.Body)
		// Không có finish_reason => nội dung có thể bị cụt: báo lỗi, không trả 200 với status "completed".
		if agg.chunks == 0 || agg.finishReason == "" {
			if err != nil {
				log.Printf("[responses] stream đứt: %v", err)
			}
			writeJSONError(w, http.StatusBadGateway, "upstream_error", "Upstream stream interrupted")
			return
		}
		var items []*respItem
		if t := agg.content.String(); t != "" || len(agg.orderedTools()) == 0 {
			it := &respItem{kind: "message", id: "msg_" + randomHex(10)}
			it.text.WriteString(t)
			items = append(items, it)
		}
		for _, t := range agg.orderedTools() {
			it := &respItem{kind: "function_call", id: "fc_" + randomHex(10), callID: t.id, name: t.name}
			it.args.WriteString(t.args.String())
			items = append(items, it)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(responseObject(respID, echoModel, "completed", items, agg))
		logReqTok("responses", "json", up, t0, up.ttfb, agg.promptTok, agg.cachedTok)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher, _ := w.(http.Flusher)
	seq := 0
	emit := func(typ string, data map[string]any) {
		data["type"] = typ
		data["sequence_number"] = seq
		seq++
		b, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", typ, b)
		if flusher != nil {
			flusher.Flush()
		}
	}
	emit("response.created", map[string]any{"response": responseObject(respID, echoModel, "in_progress", nil, nil)})
	emit("response.in_progress", map[string]any{"response": responseObject(respID, echoModel, "in_progress", nil, nil)})

	var items []*respItem
	var msg *respItem
	tools := map[int]*respItem{}
	upFinish := "" // finish_reason của upstream: rỗng = stream bị cụt
	agg := &aggregate{tools: map[int]*aggTool{}}
	var first time.Duration
	closeMsg := func() {
		if msg == nil {
			return
		}
		emit("response.output_text.done", map[string]any{"item_id": msg.id, "output_index": msg.outIndex, "content_index": 0, "text": msg.text.String()})
		emit("response.content_part.done", map[string]any{"item_id": msg.id, "output_index": msg.outIndex, "content_index": 0,
			"part": map[string]any{"type": "output_text", "text": msg.text.String(), "annotations": []any{}}})
		emit("response.output_item.done", map[string]any{"output_index": msg.outIndex, "item": msg.json(true)})
		msg = nil
	}
	err := readSSE(up.resp.Body, func(payload string) error {
		var ch oaChunk
		if json.Unmarshal([]byte(payload), &ch) != nil {
			return nil
		}
		if first == 0 {
			first = time.Since(t0)
		}
		if ch.Usage != nil {
			agg.promptTok, agg.complTok = ch.Usage.PromptTokens, ch.Usage.CompletionTokens
			agg.cachedTok = ch.Usage.PromptTokensDetails.CachedTokens
		}
		if len(ch.Choices) == 0 {
			return nil
		}
		if fr := ch.Choices[0].FinishReason; fr != nil && *fr != "" {
			upFinish = *fr // upstream đã kết thúc stream đúng cách
		}
		d := ch.Choices[0].Delta
		if d.Content != "" {
			if msg == nil {
				msg = &respItem{kind: "message", id: "msg_" + randomHex(10), outIndex: len(items)}
				items = append(items, msg)
				emit("response.output_item.added", map[string]any{"output_index": msg.outIndex, "item": msg.json(false)})
				emit("response.content_part.added", map[string]any{"item_id": msg.id, "output_index": msg.outIndex, "content_index": 0,
					"part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
			}
			msg.text.WriteString(d.Content)
			emit("response.output_text.delta", map[string]any{"item_id": msg.id, "output_index": msg.outIndex, "content_index": 0, "delta": d.Content})
		}
		for _, tc := range d.ToolCalls {
			it := tools[tc.Index]
			if it == nil {
				closeMsg()
				it = &respItem{kind: "function_call", id: "fc_" + randomHex(10), callID: tc.ID, name: tc.Function.Name, outIndex: len(items)}
				tools[tc.Index] = it
				items = append(items, it)
				emit("response.output_item.added", map[string]any{"output_index": it.outIndex, "item": it.json(false)})
			}
			if tc.ID != "" && it.callID == "" {
				it.callID = tc.ID
			}
			if tc.Function.Name != "" && it.name == "" {
				it.name = tc.Function.Name
			}
			if tc.Function.Arguments != "" {
				it.args.WriteString(tc.Function.Arguments)
				emit("response.function_call_arguments.delta", map[string]any{"item_id": it.id, "output_index": it.outIndex, "delta": tc.Function.Arguments})
			}
		}
		return nil
	})
	closeMsg()
	for _, it := range items {
		if it.kind == "function_call" {
			emit("response.function_call_arguments.done", map[string]any{"item_id": it.id, "output_index": it.outIndex, "arguments": it.args.String()})
			emit("response.output_item.done", map[string]any{"output_index": it.outIndex, "item": it.json(true)})
		}
	}
	if err != nil || upFinish == "" {
		// Không có finish_reason (kể cả EOF "sạch" không có [DONE]) => stream bị cụt: báo failed, không báo completed.
		if err != nil {
			log.Printf("[responses] stream đứt: %v", err)
		}
		emit("error", map[string]any{"code": "upstream_error", "message": "Upstream stream interrupted"})
		emit("response.failed", map[string]any{"response": responseObject(respID, echoModel, "failed", items, agg)})
	} else {
		emit("response.completed", map[string]any{"response": responseObject(respID, echoModel, "completed", items, agg)})
	}
	logReqTok("responses", "stream", up, t0, first, agg.promptTok, agg.cachedTok)
}

// completionsHandler: /v1/completions cũ (prompt thuần), dịch thành 1 lượt chat.
func completionsHandler(w http.ResponseWriter, r *http.Request) {
	if preflight(w, r) {
		return
	}
	req, ok := readBody(w, r)
	if !ok {
		return
	}
	var prompt string
	switch p := req["prompt"].(type) {
	case string:
		prompt = p
	case []any:
		var parts []string
		for _, x := range p {
			parts = append(parts, str(x))
		}
		prompt = strings.Join(parts, "\n")
	}
	if prompt == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "prompt: field required")
		return
	}
	release, ok := acquireSlot(w, r)
	if !ok {
		return
	}
	defer release()

	chat := map[string]any{"messages": []any{map[string]any{"role": "user", "content": prompt}}}
	for _, k := range []string{"temperature", "top_p", "stop", "user"} {
		if v, ok := req[k]; ok {
			chat[k] = v
		}
	}
	if v, ok := req["max_tokens"]; ok {
		chat["max_tokens"] = v
	}
	modelIn := str(req["model"])
	clientStream, _ := req["stream"].(bool)
	target, isPool := resolveModelName(modelIn)
	t0 := time.Now()
	up, gerr := forward(withMaxWait(withFallback(withProfile(r.Context(), modelIn), r), r), target, isPool, infURL+"/chat/completions", "*/*", sessionKey(r, req), func(m string) []byte {
		chat["model"] = m
		applyFingerprint(chat, m, true, stubDescription)
		b, _ := json.Marshal(chat)
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
	agg, err := aggregateSSE(up.resp.Body)
	// Không có finish_reason => nội dung có thể bị cụt: báo lỗi thay vì trả 200 với finish_reason "stop".
	if agg.chunks == 0 || agg.finishReason == "" {
		if err != nil {
			log.Printf("[completions] stream đứt: %v", err)
		}
		writeJSONError(w, http.StatusBadGateway, "upstream_error", "Upstream stream interrupted")
		return
	}
	finish := "stop"
	if agg.finishReason == "length" {
		finish = "length"
	}
	id := "cmpl-" + randomHex(12)
	choice := map[string]any{"index": 0, "text": agg.content.String(), "finish_reason": finish, "logprobs": nil}
	if clientStream {
		w.Header().Set("Content-Type", "text/event-stream")
		b, _ := json.Marshal(map[string]any{"id": id, "object": "text_completion", "created": time.Now().Unix(), "model": up.model, "choices": []any{choice}})
		fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", b)
	} else {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"id": id, "object": "text_completion", "created": time.Now().Unix(), "model": up.model,
			"choices": []any{choice},
			"usage":   map[string]any{"prompt_tokens": agg.promptTok, "completion_tokens": agg.complTok, "total_tokens": agg.promptTok + agg.complTok}})
	}
	logReqTok("completions", "json", up, t0, up.ttfb, agg.promptTok, agg.cachedTok)
}

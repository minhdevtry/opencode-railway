package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ======================== BẢO VỆ CHẤT LƯỢNG CHO MODEL AUTO ========================
//
// Client gọi "auto" chỉ cần "đưa vào, nhận về". Đo 2026-10-24 trên workload gán nhãn: vì upstream bắt buộc có tool
// (stub bash/glob/grep/read), vài model thỉnh thoảng gọi tool thay vì trả lời; model reasoning (nemotron, flint)
// có lúc đốt hết max_tokens vào suy nghĩ và trả content rỗng. Nên với auto (client không gửi tool):
//   - đặt tool_choice=none và tắt reasoning ở model hỗ trợ (nemotron 12s -> 1.3s, cùng chất lượng JSON);
//   - đọc trước stream tới khi thấy nội dung thật; rỗng hoặc gọi tool => bỏ, đổi model khác, ghi nhận model kém.

// autoModelExtras: tham số thêm cho từng họ model khi chạy auto (client đã tự đặt thì không ghi đè).
var autoModelExtras = map[string]map[string]any{
	// deepseek bỏ qua reasoning.enabled; thinking.type=disabled mới tắt được (đo 2026-10-25: reasoning 0, nhanh hơn ~20%)
	"deepseek": {"thinking": map[string]any{"type": "disabled"}},
	"nemotron": {"reasoning": map[string]any{"enabled": false}},
	"glm":      {"reasoning": map[string]any{"enabled": false}},
	"beam":     {"reasoning": map[string]any{"enabled": false}},
}

func applyAutoExtras(body map[string]any, model string) {
	if _, ok := body["tool_choice"]; !ok && !paramUnsupported(model, "tool_choice") {
		body["tool_choice"] = "none"
	}
	if _, ok := body["reasoning"]; ok {
		return
	}
	if _, ok := body["reasoning_effort"]; ok {
		return
	}
	if _, ok := body["thinking"]; ok {
		return
	}
	for prefix, extra := range autoModelExtras {
		if strings.HasPrefix(model, prefix) {
			for k, v := range extra {
				if !paramUnsupported(model, k) {
					body[k] = v
				}
			}
		}
	}
}

// ---- tham số bị nhà cung cấp phía sau từ chối ----
// Upstream định tuyến mỗi request tới 1 trong nhiều nhà cung cấp; có nhà không nhận reasoning/tool_choice (400
// unsupported_parameter). Gặp thì ngừng gửi tham số đó cho model ấy 10 phút rồi thử lại.

var (
	upMu        sync.Mutex
	unsupported = map[string]time.Time{}
)

func autoParamSet(p string) bool {
	return p == "reasoning" || p == "tool_choice" || p == "thinking" || p == "reasoning_effort"
}

// thinkingOff: công tắc tắt suy nghĩ có tác dụng với từng họ model (đo 2026-10-25, research/results/thinking_matrix.txt).
var thinkingOff = map[string]map[string]any{
	"deepseek":   {"thinking": map[string]any{"type": "disabled"}},
	"big-pickle": {"thinking": map[string]any{"type": "disabled"}},
	"mimo":       {"reasoning": map[string]any{"enabled": false}},
	"nemotron":   {"reasoning": map[string]any{"enabled": false}},
	"glm":        {"reasoning": map[string]any{"enabled": false}}, // chỉ giảm, glm vẫn nghĩ chút ít
}

// normalizeThinking: chuẩn chung reasoning_effort = none|low|medium|high. "none"/"off" được dịch sang đúng công tắc tắt
// của model; low/medium/high giữ nguyên (mọi model đều nhận). Client gửi tham số gốc (thinking/reasoning) thì giữ nguyên.
func normalizeThinking(body map[string]any, model string) {
	eff, _ := body["reasoning_effort"].(string)
	switch strings.ToLower(eff) {
	case "none", "off", "disabled":
		delete(body, "reasoning_effort")
		for prefix, sw := range thinkingOff {
			if strings.HasPrefix(model, prefix) {
				for k, v := range sw {
					if _, set := body[k]; !set {
						body[k] = v
					}
				}
			}
		}
	}
}

// dropUnsupported gỡ các tham số mà nhà cung cấp phía sau của model đang từ chối; trả về tên tham số đã gỡ.
func dropUnsupported(body map[string]any, model string) []string {
	var dropped []string
	for _, p := range []string{"reasoning", "thinking", "tool_choice", "reasoning_effort"} {
		if _, ok := body[p]; ok && paramUnsupported(model, p) {
			delete(body, p)
			dropped = append(dropped, p)
		}
	}
	return dropped
}

func markParamUnsupported(model, p string) {
	upMu.Lock()
	unsupported[model+"|"+p] = time.Now().Add(10 * time.Minute)
	upMu.Unlock()
}

func paramUnsupported(model, p string) bool {
	upMu.Lock()
	defer upMu.Unlock()
	return time.Now().Before(unsupported[model+"|"+p])
}

// unsupportedParam đọc tên tham số từ lỗi 400 kiểu {"error":{"param":"reasoning","message":"...unsupported_parameter..."}}.
// Dò bằng regex vì body lỗi chỉ đọc 512 byte đầu (có thể bị cắt, không parse JSON được).
var paramRe = regexp.MustCompile(`"param"\s*:\s*"([A-Za-z_]+)"`)

func unsupportedParam(body []byte) string {
	if !bytes.Contains(body, []byte("unsupported_parameter")) && !bytes.Contains(body, []byte("Unrecognized request argument")) &&
		!bytes.Contains(body, []byte("unknown_parameter")) && !bytes.Contains(body, []byte("unknown field")) {
		return ""
	}
	if m := paramRe.FindSubmatch(body); m != nil {
		return string(m[1])
	}
	return ""
}

func hasClientTools(body map[string]any) bool {
	t, _ := body["tools"].([]any)
	return len(t) > 0
}

// sniffContent đọc stream SSE cho tới chunk đầu tiên có content (good=true) hoặc tới khi stream kết thúc / có
// tool_calls (good=false). Trả về toàn bộ byte đã đọc để phát lại cho client.
// Quá `limit` mà chưa có content thì đóng body (rc) và coi như hỏng (upstream treo stream).
//
// anyProgress=true (model cụ thể): token suy nghĩ, tool_call hay finish_reason đều tính là "đang chạy tốt"; chỉ hỏng
// khi treo quá hạn hoặc đứt mà chưa có gì.
func sniffContent(rc io.ReadCloser, limit time.Duration, anyProgress ...bool) (consumed []byte, br *bufio.Reader, good bool) {
	t := time.AfterFunc(limit, func() { rc.Close() })
	consumed, br, good = sniffLines(rc, len(anyProgress) > 0 && anyProgress[0])
	if !t.Stop() { // timer đã bắn (body đã bị đóng) dù có thể đúng lúc vừa đọc được content: coi như hỏng
		return consumed, br, false
	}
	return consumed, br, good
}

func sniffLines(r io.Reader, anyProgress bool) (consumed []byte, br *bufio.Reader, good bool) {
	br = bufio.NewReaderSize(r, 32*1024)
	var buf bytes.Buffer
	for {
		line, err := br.ReadBytes('\n')
		buf.Write(line)
		if s := bytes.TrimSpace(line); bytes.HasPrefix(s, []byte("data:")) {
			payload := bytes.TrimSpace(s[5:])
			var c oaChunk
			if !bytes.Equal(payload, []byte("[DONE]")) && json.Unmarshal(payload, &c) == nil {
				for _, ch := range c.Choices {
					if anyProgress && (len(ch.Delta.ToolCalls) > 0 || ch.Delta.reasoningText() != "" || ch.Delta.Content != "" ||
						(ch.FinishReason != nil && *ch.FinishReason != "")) {
						return buf.Bytes(), br, true
					}
					if len(ch.Delta.ToolCalls) > 0 {
						return buf.Bytes(), br, false
					}
					if strings.TrimSpace(ch.Delta.Content) != "" {
						return buf.Bytes(), br, true
					}
				}
			}
		}
		if err != nil {
			return buf.Bytes(), br, false
		}
	}
}

// replayBody: phần đã đọc trước + phần còn lại của stream, đóng body gốc khi Close.
type replayBody struct {
	io.Reader
	orig io.Closer
}

func (b *replayBody) Close() error { return b.orig.Close() }

// ---- chất lượng model (cho vòng xoay auto) ----

type modelQ struct{ good, bad float64 }

var (
	mqMu sync.Mutex
	mq   = map[string]*modelQ{}
)

// noteModel ghi nhận 1 kết quả của model (trung bình trượt, mẫu cũ phai dần).
func noteModel(model string, good bool) {
	mqMu.Lock()
	defer mqMu.Unlock()
	q := mq[model]
	if q == nil {
		q = &modelQ{}
		mq[model] = q
	}
	q.good *= 0.98
	q.bad *= 0.98
	if good {
		q.good++
	} else {
		q.bad++
	}
}

// modelPoor: model đang trả kết quả hỏng nhiều (>40% trên đủ mẫu) thì vòng xoay auto tạm bỏ qua.
func modelPoor(model string) bool {
	mqMu.Lock()
	defer mqMu.Unlock()
	q := mq[model]
	if q == nil || q.good+q.bad < 8 {
		return false
	}
	return q.bad/(q.good+q.bad) > 0.4
}

func modelQualitySnapshot() map[string]map[string]float64 {
	mqMu.Lock()
	defer mqMu.Unlock()
	out := map[string]map[string]float64{}
	for m, q := range mq {
		out[m] = map[string]float64{"good": float64(int(q.good*10)) / 10, "bad": float64(int(q.bad*10)) / 10}
	}
	return out
}

// Danh sách model cần tránh cho 1 request (đã trả kết quả hỏng ở lần thử trước), truyền qua context tới pickAny.
type excludeKey struct{}

func withExclude(ctx context.Context, models map[string]bool) context.Context {
	return context.WithValue(ctx, excludeKey{}, models)
}

func excludedModels(ctx context.Context) map[string]bool {
	m, _ := ctx.Value(excludeKey{}).(map[string]bool)
	return m
}

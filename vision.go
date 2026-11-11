package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
)

// ======================== KHẢ NĂNG VISION CỦA MODEL ========================
//
// Upstream (opencode.ai/zen) trả modalities.input cho mỗi model trong catalog.
// Model mà modalities.input KHÔNG có image/* là TEXT-ONLY: gửi ảnh cho nó sẽ bị
// upstream trả 400 "model X not supported for vision".
//
// Gateway đồng bộ bảng này từ catalog upstream (syncCatalog) + seed tĩnh bên dưới,
// rồi dùng ở 4 chỗ:
//   1) failover ưu tiên model nhận ảnh (request có ảnh không được rơi vào model text-only)
//   2) hạ ảnh xuống text khi model được chọn chắc chắn không nhận ảnh (thay vì để upstream trả 400)
//   3) tự học: upstream báo "not supported for vision" -> ghi nhớ model đó là text-only
//   4) /v1/models quảng bá capability đúng với upstream
//
// Key trong bảng là id upstream (có hậu tố "-free"), ví dụ "glm-5.3-flash-free".

var (
	visionMu sync.RWMutex
	// visionModels: id -> model đó nhận ảnh hay không.
	visionModels = map[string]bool{}
	// visionContext: id -> context window upstream (0 = chưa biết).
	visionContext = map[string]int{}
)

// seedVision: năng lực đo từ catalog upstream (2026-11-11). Dùng làm mặc định trước
// lần syncCatalog đầu tiên và khi upstream không trả catalog.
var seedVision = map[string]bool{
	// Nhận ảnh
	"glm-5.3-flash-free":         true,
	"deepseek-v4.1-flash-free":   true,
	"mimo-v2.6-flash-free":       true,
	"nemotron-3.5-super-vl-free": true,
	"inkling-2-small-free":       true,
	"minimax-m3.1-flash-free":    true,
	"longcat-2.5-free":           true,
	// Chỉ text
	"big-pickle":                   false,
	"beam-free":                    false,
	"flint-40b-free":               false,
	"nemotron-3.5-lightning-free": false,
	"iceman-free":                  false,
}

func init() {
	for id, v := range seedVision {
		visionModels[id] = v
	}
}

// visionState trả (model nhận ảnh?, đã biết chắc chưa).
// Model chưa biết -> (true, false): coi như CÓ THỂ nhận ảnh, để thử; nếu upstream
// từ chối thì ta học được (markNoVision) và hạ ảnh ở lần sau.
func visionState(model string) (supports bool, known bool) {
	id := strings.ToLower(strings.TrimSpace(model))
	if id == "" {
		return true, false
	}
	visionMu.RLock()
	defer visionMu.RUnlock()
	// Ưu tiên bản "-free": đây là bản gateway THỰC SỰ phục vụ (pool chỉ chứa model free).
	// Bản trả phí cùng họ có thể khác hẳn (vd glm-5.3-flash: 1M context, còn glm-5.3-flash-free: 200k).
	if v, ok := visionModels[id+"-free"]; ok {
		return v, true
	}
	if v, ok := visionModels[id]; ok {
		return v, true
	}
	if v, ok := visionModels[strings.TrimSuffix(id, "-free")]; ok {
		return v, true
	}
	return true, false
}

// modelRejectsImages: model CHẮC CHẮN không nhận ảnh -> phải hạ ảnh xuống text.
func modelRejectsImages(model string) bool {
	supports, known := visionState(model)
	return known && !supports
}

// modelSupportsImages: model có nhận ảnh không.
//
// Nếu người dùng đặt IMAGE_MODELS: giữ đúng ngữ nghĩa allowlist cũ (chỉ model trong
// danh sách mới nhận ảnh, model khác bị hạ ảnh xuống text).
// Nếu KHÔNG đặt (mặc định): dùng bảng vision đồng bộ từ catalog upstream.
// Model chưa biết -> coi như nhận ảnh (thử trước; nếu upstream từ chối thì tự học).
func modelSupportsImages(model string) bool {
	key := strings.ToLower(strings.TrimSpace(model))
	if len(cfg.ImageModels) > 0 {
		if v, ok := cfg.ImageModels[key]; ok {
			return v
		}
		if v, ok := cfg.ImageModels[strings.TrimSuffix(key, "-free")]; ok {
			return v
		}
		return false
	}
	supports, _ := visionState(model)
	return supports
}

// markNoVision ghi nhớ model là text-only (upstream vừa báo không nhận ảnh).
func markNoVision(model string) {
	id := strings.ToLower(strings.TrimSpace(model))
	if id == "" {
		return
	}
	visionMu.Lock()
	defer visionMu.Unlock()
	if v, ok := visionModels[id]; ok && !v {
		return
	}
	visionModels[id] = false
	log.Printf("[Vision] ghi nhớ model %s KHÔNG nhận ảnh (upstream báo lỗi)", id)
}

// setVisionFromCatalog cập nhật bảng năng lực + context từ catalog upstream.
func setVisionFromCatalog(hasImage map[string]bool, ctxLimit map[string]int) {
	visionMu.Lock()
	defer visionMu.Unlock()
	for id, img := range hasImage {
		visionModels[strings.ToLower(id)] = img
	}
	for id, c := range ctxLimit {
		visionContext[strings.ToLower(id)] = c
	}
}

// visionContextOf: context window upstream của model (0 = chưa biết).
func visionContextOf(model string) int {
	id := strings.ToLower(strings.TrimSpace(model))
	visionMu.RLock()
	defer visionMu.RUnlock()
	// Ưu tiên bản "-free" (xem visionState).
	if c, ok := visionContext[id+"-free"]; ok {
		return c
	}
	if c, ok := visionContext[id]; ok {
		return c
	}
	if c, ok := visionContext[strings.TrimSuffix(id, "-free")]; ok {
		return c
	}
	return 0
}

// servedModelID: id client gõ -> id model gateway thực sự phục vụ upstream.
// "deepseek" -> "deepseek-v4.1-flash-free", "glm-5.3-flash" -> "glm-5.3-flash-free"...
// Trả "" nếu là model auto (pool) vì khi đó model chọn lúc chạy, không cố định.
func servedModelID(id string) string {
	resolved, isPool := resolveModelName(id)
	if isPool || resolved == "" {
		return ""
	}
	return resolved
}

// ======================== PHÂN LOẠI LỖI UPSTREAM ========================

// isVisionUnsupported: upstream từ chối vì model không nhận ảnh.
func isVisionUnsupported(body []byte) bool {
	return bytes.Contains(body, []byte("not supported for vision")) ||
		bytes.Contains(body, []byte("does not support vision"))
}

// isEndpointUnavailable: lỗi hạ tầng của nhà cung cấp, KHÔNG phải hạn mức
// (xem research/limit-shape/probe/main.go: "Endpoint is unavailable" là sự cố provider).
func isEndpointUnavailable(body []byte) bool {
	return bytes.Contains(body, []byte("Endpoint is unavailable"))
}

// isModelUnavailable: upstream không còn phục vụ model này ("Model is unavailable").
// Model bị gỡ khỏi catalog hoặc backend tạm hạ -> phải failover, không phải lỗi của client.
func isModelUnavailable(body []byte) bool {
	return bytes.Contains(body, []byte("Model is unavailable"))
}

// ======================== CONTEXT: REQUEST CÓ ẢNH KHÔNG ========================

type visionKey struct{}

// withVision đánh dấu request mang ảnh -> failover ưu tiên model nhận ảnh.
func withVision(ctx context.Context, has bool) context.Context {
	return context.WithValue(ctx, visionKey{}, has)
}

// needsVision: request này có ảnh không.
func needsVision(ctx context.Context) bool {
	v, _ := ctx.Value(visionKey{}).(bool)
	return v
}

// ======================== HẠ ẢNH KHI MODEL KHÔNG NHẬN ẢNH ========================

// stripImagesChat trả về bản sao của messages với mọi phần ảnh bị thay bằng ghi chú
// text. Không sửa input (copy-on-write) vì body gốc được dùng lại cho lần thử sau.
func stripImagesChat(msgs []any) []any {
	out := make([]any, len(msgs))
	for i, raw := range msgs {
		m, ok := raw.(map[string]any)
		if !ok {
			out[i] = raw
			continue
		}
		content, ok := m["content"].([]any)
		if !ok {
			out[i] = raw
			continue
		}
		newContent := make([]any, len(content))
		changed := false
		for j, praw := range content {
			pm, ok := praw.(map[string]any)
			if ok {
				switch str(pm["type"]) {
				case "image_url", "input_image", "image":
					newContent[j] = map[string]any{"type": "text", "text": "[image attached]"}
					changed = true
					continue
				}
			}
			newContent[j] = praw
		}
		if !changed {
			out[i] = raw
			continue
		}
		nm := make(map[string]any, len(m))
		for k, v := range m {
			nm[k] = v
		}
		nm["content"] = newContent
		out[i] = nm
	}
	return out
}

// bodyHasImage: body chat/completions (đã quy đổi) có phần ảnh không.
func bodyHasImage(body map[string]any) bool {
	msgs, _ := body["messages"].([]any)
	for _, raw := range msgs {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		content, ok := m["content"].([]any)
		if !ok {
			continue
		}
		for _, praw := range content {
			pm, ok := praw.(map[string]any)
			if !ok {
				continue
			}
			switch str(pm["type"]) {
			case "image_url", "input_image", "image":
				return true
			}
		}
	}
	return false
}

// bodyForModel: nếu model chắc chắn không nhận ảnh, hạ ảnh trong body xuống text để
// upstream không trả 400 "not supported for vision". Body gốc không bị sửa.
func bodyForModel(body map[string]any, model string) map[string]any {
	if modelSupportsImages(model) {
		return body
	}
	msgs, _ := body["messages"].([]any)
	if !bodyHasImage(body) {
		return body
	}
	out := make(map[string]any, len(body)+1)
	for k, v := range body {
		out[k] = v
	}
	out["messages"] = stripImagesChat(msgs)
	log.Printf("[Vision] model=%s không nhận ảnh -> hạ ảnh xuống text", model)
	return out
}

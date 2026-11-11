package main

import (
	"testing"
)

// isVisionUnsupported phải nhận ra đúng lỗi upstream "model X not supported for vision".
func TestIsVisionUnsupported(t *testing.T) {
	body := []byte(`{"error":{"type":"invalid_request_error","message":"Upstream request failed: [invalid_request_error] model \"iceman-free\" not supported for vision"}}`)
	if !isVisionUnsupported(body) {
		t.Error("phải nhận ra lỗi not supported for vision")
	}
	if isVisionUnsupported([]byte(`{"error":{"message":"Your request resulted in more tokens than this model's maximum context length allows"}}`)) {
		t.Error("lỗi context length không phải lỗi vision")
	}
}

// isModelUnavailable phải nhận ra model upstream đã bị gỡ.
func TestIsModelUnavailable(t *testing.T) {
	if !isModelUnavailable([]byte(`{"error":{"type":"server_error","message":"Upstream request failed: Model is unavailable."}}`)) {
		t.Error("phải nhận ra lỗi Model is unavailable")
	}
	if isModelUnavailable([]byte(`{"error":{"message":"model not found for vision"}}`)) {
		t.Error("lỗi vision không phải Model is unavailable")
	}
}

// Bảng vision phải phân biệt được model nhận ảnh và model chỉ text.
func TestVisionState(t *testing.T) {
	visionMu.Lock()
	old := visionModels
	visionModels = map[string]bool{
		"glm-5.3-flash-free": true,
		"iceman-free":        false,
	}
	visionMu.Unlock()
	defer func() {
		visionMu.Lock()
		visionModels = old
		visionMu.Unlock()
	}()

	if s, known := visionState("glm-5.3-flash-free"); !known || !s {
		t.Errorf("glm-5.3-flash-free phải nhận ảnh, got (%v,%v)", s, known)
	}
	// Tra bằng id không hậu tố "-free" (client thường dùng dạng này).
	if s, known := visionState("glm-5.3-flash"); !known || !s {
		t.Errorf("tra theo id không hậu tố phải ra nhận ảnh, got (%v,%v)", s, known)
	}
	if s, known := visionState("iceman-free"); !known || s {
		t.Errorf("iceman-free phải text-only, got (%v,%v)", s, known)
	}
	if modelRejectsImages("iceman-free") != true {
		t.Error("iceman-free phải bị đánh dấu text-only")
	}
	if modelRejectsImages("glm-5.3-flash-free") != false {
		t.Error("glm-5.3-flash-free không được đánh dấu text-only")
	}
	// Model chưa biết: chưa chắc chắn -> coi như nhận ảnh (để thử, upstream từ chối thì tự học).
	if _, known := visionState("model-la-quen"); known {
		t.Error("model chưa biết phải trả known=false")
	}
	if modelRejectsImages("model-la-quen") {
		t.Error("model chưa biết không được đánh dấu text-only")
	}
}

// markNoVision phải ghi nhớ model text-only sau khi upstream báo lỗi.
func TestMarkNoVision(t *testing.T) {
	visionMu.Lock()
	old := visionModels
	visionModels = map[string]bool{"model-x": true}
	visionMu.Unlock()
	defer func() {
		visionMu.Lock()
		visionModels = old
		visionMu.Unlock()
	}()

	markNoVision("model-x")
	if !modelRejectsImages("model-x") {
		t.Error("sau markNoVision, model phải được coi là text-only")
	}
}

// Hạ ảnh xuống text khi model không nhận ảnh; body gốc không được sửa.
func TestBodyForModelStripsImages(t *testing.T) {
	visionMu.Lock()
	old := visionModels
	visionModels = map[string]bool{"iceman-free": false, "glm-5.3-flash-free": true}
	visionMu.Unlock()
	oldCfg := cfg
	cfg.ImageModels = map[string]bool{} // không đặt IMAGE_MODELS -> dùng bảng catalog
	defer func() {
		visionMu.Lock()
		visionModels = old
		visionMu.Unlock()
		cfg = oldCfg
	}()

	body := map[string]any{"messages": []any{
		map[string]any{"role": "user", "content": []any{
			map[string]any{"type": "text", "text": "look"},
			map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:image/png;base64,AAA"}},
		}},
	}}
	out := bodyForModel(body, "iceman-free")
	content := out["messages"].([]any)[0].(map[string]any)["content"].([]any)
	if content[0].(map[string]any)["text"] != "look" {
		t.Errorf("phần text gốc phải được giữ nguyên: %v", content[0])
	}
	if content[1].(map[string]any)["text"] != "[image attached]" {
		t.Errorf("ảnh phải bị hạ thành ghi chú [image attached]: %v", content[1])
	}
	for _, p := range content {
		if p.(map[string]any)["type"] == "image_url" {
			t.Error("model text-only không được giữ image_url")
		}
	}
	// Body gốc không được sửa (dùng lại cho lần thử sau với model khác).
	orig := body["messages"].([]any)[0].(map[string]any)["content"].([]any)
	if orig[1].(map[string]any)["type"] != "image_url" {
		t.Error("body gốc không được bị sửa khi hạ ảnh")
	}
	// Model nhận ảnh -> giữ nguyên.
	out = bodyForModel(body, "glm-5.3-flash-free")
	content = out["messages"].([]any)[0].(map[string]any)["content"].([]any)
	if content[1].(map[string]any)["type"] != "image_url" {
		t.Error("model có vision phải giữ image_url")
	}
}

// nextModelInPool phải ưu tiên model nhận ảnh khi request mang ảnh.
func TestNextModelInPoolPrefersVision(t *testing.T) {
	poolMu.Lock()
	oldPool := freeModelPool
	freeModelPool = []string{"glm-5.3-flash-free", "iceman-free", "big-pickle", "deepseek-v4.1-flash-free"}
	poolMu.Unlock()
	defer func() {
		poolMu.Lock()
		freeModelPool = oldPool
		poolMu.Unlock()
	}()

	visionMu.Lock()
	old := visionModels
	visionModels = map[string]bool{
		"glm-5.3-flash-free":       true,
		"iceman-free":              false,
		"big-pickle":               false,
		"deepseek-v4.1-flash-free": true,
	}
	visionMu.Unlock()
	defer func() {
		visionMu.Lock()
		visionModels = old
		visionMu.Unlock()
	}()

	// Request có ảnh: không được chọn model text-only nếu còn model nhận ảnh.
	for i := 0; i < 8; i++ {
		next := nextModelInPool("iceman-free", true)
		if modelRejectsImages(next) {
			t.Errorf("failover request có ảnh chọn model text-only: %s", next)
		}
	}
	// Request không ảnh: chọn model kế tiếp trong pool như cũ (kể cả model text-only).
	if next := nextModelInPool("glm-5.3-flash-free", false); next != "iceman-free" {
		t.Errorf("không có ảnh thì phải lấy model kế tiếp trong pool, got %s", next)
	}
}

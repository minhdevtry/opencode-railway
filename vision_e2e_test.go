package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// setupVisionFixture: dựng gateway với pool 3 model (2 nhận ảnh, 1 text-only) và
// bảng vision tương ứng. Trả về gateway test + manager.
func setupVisionFixture(t *testing.T, up http.HandlerFunc) (*httptest.Server, *Manager) {
	t.Helper()
	visionMu.Lock()
	old := visionModels
	visionModels = map[string]bool{
		"glm-5.3-flash-free":       true,
		"iceman-free":              false,
		"deepseek-v4.1-flash-free": true,
	}
	visionMu.Unlock()
	oldCfg := cfg
	cfg.ImageModels = map[string]bool{}

	poolMu.Lock()
	oldPool := freeModelPool
	freeModelPool = []string{"glm-5.3-flash-free", "iceman-free", "deepseek-v4.1-flash-free"}
	poolMu.Unlock()

	t.Cleanup(func() {
		visionMu.Lock()
		visionModels = old
		visionMu.Unlock()
		poolMu.Lock()
		freeModelPool = oldPool
		poolMu.Unlock()
		cfg = oldCfg
	})

	gw, m := setup(t, up, true) // 1 direct + 1 tor: đủ đường để failover
	cfg.AutoModelFallback = true // production đặt AUTO_MODEL_FALLBACK=true
	return gw, m
}

const imgBody = `{"model":"glm-5.3-flash","messages":[{"role":"user","content":[` +
	`{"type":"text","text":"x"},` +
	`{"type":"image_url","image_url":{"url":"data:image/png;base64,AAA"}}]}]}`

// TestVision400FailsOverToVisionModel: upstream trả 400 "not supported for vision" cho
// model đang dùng -> gateway phải chuyển sang model khác thay vì trả 400 cho client.
// Đây chính là lỗi gốc: client nhận 400 và cả lượt agent bị chết.
func TestVision400FailsOverToVisionModel(t *testing.T) {
	var served atomic.Value
	gw, _ := setupVisionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		m, _ := body["model"].(string)
		served.Store(m)
		// Model ghim đã hết quota trên mọi egress -> gateway failover sang model khác.
		if m == "glm-5.3-flash-free" {
			w.Header().Set("Retry-After", "21600")
			w.WriteHeader(429)
			w.Write([]byte(`{"type":"error","error":{"type":"FreeUsageLimitError"}}`))
			return
		}
		// Model text-only: upstream từ chối ảnh.
		if m == "iceman-free" {
			w.WriteHeader(400)
			w.Write([]byte(`{"error":{"type":"invalid_request_error","message":"Upstream request failed: [invalid_request_error] model \"iceman-free\" not supported for vision"}}`))
			return
		}
		sse(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
	})

	resp, out := post(t, gw.URL+"/v1/chat/completions", imgBody)
	if resp.StatusCode != 200 {
		t.Fatalf("request có ảnh không được chết với %d: %s", resp.StatusCode, out)
	}
	if !strings.Contains(out, "ok") {
		t.Errorf("phải nhận được câu trả lời, got %s", out)
	}
	if m, _ := served.Load().(string); m == "iceman-free" {
		t.Error("không được phục vụ bằng model text-only")
	}
}

// TestImageNotSentToTextOnlyModel: khi model phục vụ là text-only, ảnh phải được hạ
// xuống text để upstream không trả 400.
func TestImageNotSentToTextOnlyModel(t *testing.T) {
	var gotImage atomic.Bool
	gw, _ := setupVisionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		msgs, _ := body["messages"].([]any)
		for _, raw := range msgs {
			m, _ := raw.(map[string]any)
			content, _ := m["content"].([]any)
			for _, p := range content {
				pm, _ := p.(map[string]any)
				if pm["type"] == "image_url" {
					gotImage.Store(true)
				}
			}
		}
		sse(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
	})

	// Yêu cầu trực tiếp model text-only.
	resp, out := post(t, gw.URL+"/v1/chat/completions",
		`{"model":"iceman-free","messages":[{"role":"user","content":[`+
			`{"type":"text","text":"x"},`+
			`{"type":"image_url","image_url":{"url":"data:image/png;base64,AAA"}}]}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("phải thành công, got %d: %s", resp.StatusCode, out)
	}
	if gotImage.Load() {
		t.Error("ảnh không được gửi lên model text-only")
	}
}

// TestResponsesPathVision: đường /v1/responses (omp dùng đường này) cũng phải hạ ảnh
// khi model text-only.
func TestResponsesPathVision(t *testing.T) {
	var gotImage atomic.Bool
	gw, _ := setupVisionFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		msgs, _ := body["messages"].([]any)
		for _, raw := range msgs {
			m, _ := raw.(map[string]any)
			content, _ := m["content"].([]any)
			for _, p := range content {
				pm, _ := p.(map[string]any)
				if pm["type"] == "image_url" {
					gotImage.Store(true)
				}
			}
		}
		sse(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
	})

	resp, out := post(t, gw.URL+"/v1/responses",
		`{"model":"iceman-free","input":[{"role":"user","content":[`+
			`{"type":"input_text","text":"x"},`+
			`{"type":"input_image","image_url":"data:image/png;base64,AAA"}]}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("phải thành công, got %d: %s", resp.StatusCode, out)
	}
	if gotImage.Load() {
		t.Error("/v1/responses: ảnh không được gửi lên model text-only")
	}
}

package main

import (
	"net/http"
	"strings"
	"testing"
)

// Upstream đứt giữa chừng (stream kết thúc mà KHÔNG có finish_reason) => mọi endpoint đều phải báo lỗi, tuyệt đối
// không trả 200 kèm nội dung cụt (client xử lý văn bản dài sẽ tưởng là câu trả lời đầy đủ).
func TestTruncatedUpstreamIsNot200(t *testing.T) {
	truncated := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		// chỉ có nội dung, KHÔNG có finish_reason, KHÔNG có [DONE] -> stream bị cụt
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"câu trả lời bị cụt\"}}]}\n\n"))
	}

	gw, _ := setup(t, truncated, false)

	cases := []struct {
		name string
		path string
		body string
	}{
		{"chat non-stream", "/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`},
		{"messages non-stream", "/v1/messages", `{"model":"deepseek","max_tokens":5,"messages":[{"role":"user","content":"x"}]}`},
		{"responses non-stream", "/v1/responses", `{"model":"deepseek","input":"x"}`},
		{"completions non-stream", "/v1/completions", `{"model":"deepseek","prompt":"x"}`},
	}
	for _, c := range cases {
		resp, body := post(t, gw.URL+c.path, c.body)
		if resp.StatusCode == http.StatusOK {
			t.Errorf("%s: stream bị cụt mà vẫn trả 200 (body=%q)", c.name, body)
			continue
		}
		if resp.StatusCode != http.StatusBadGateway {
			t.Errorf("%s: phải trả 502 khi stream bị cụt, got %d (body=%q)", c.name, resp.StatusCode, body)
		}
	}
}

// Chế độ stream: client đã nhận status 200 nên lỗi phải được báo qua frame SSE (không có cách nào khác).
func TestTruncatedUpstreamStreamSignalsError(t *testing.T) {
	truncated := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"cụt\"}}]}\n\n"))
	}
	gw, _ := setup(t, truncated, false)

	// chat stream: phải có frame lỗi
	resp, body := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","stream":true,"messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream phải trả 200 (đã gửi header), got %d", resp.StatusCode)
	}
	if !strings.Contains(body, "Upstream stream interrupted") {
		t.Errorf("chat stream phải báo lỗi qua frame SSE, body=%q", body)
	}

	// messages stream: phải có event lỗi
	_, body = post(t, gw.URL+"/v1/messages", `{"model":"deepseek","max_tokens":5,"stream":true,"messages":[{"role":"user","content":"x"}]}`)
	if !strings.Contains(body, "error") {
		t.Errorf("messages stream phải báo lỗi, body=%q", body)
	}
	if strings.Contains(body, "message_stop") {
		t.Errorf("messages stream không được kết thúc bằng message_stop khi upstream bị cụt, body=%q", body)
	}

	// responses stream: phải có response.failed, không có response.completed
	_, body = post(t, gw.URL+"/v1/responses", `{"model":"deepseek","input":"x","stream":true}`)
	if !strings.Contains(body, "response.failed") {
		t.Errorf("responses stream phải báo response.failed, body=%q", body)
	}
	if strings.Contains(body, "response.completed") {
		t.Errorf("responses stream không được báo response.completed khi upstream bị cụt, body=%q", body)
	}
}

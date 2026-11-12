package main

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// X-Max-Wait phải có tác dụng thật: client đặt 1s mà upstream chậm 3s thì client phải nhận lỗi trong khoảng 1s,
// không ngồi chờ 3s. Trước đây hạn chỉ áp cho hàng đợi, thời gian chờ upstream nằm ngoài hạn.
func TestMaxWaitBoundsUpstreamWait(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second) // upstream chậm
		w.Write([]byte("data: {}\n\n"))
	}, false)

	req, _ := http.NewRequest("POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Max-Wait", "1s")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	el := time.Since(start)
	if err != nil {
		t.Fatalf("lỗi HTTP: %v", err)
	}
	resp.Body.Close()
	if el > 1800*time.Millisecond {
		t.Fatalf("phải trả lỗi trong hạn X-Max-Wait (1s), mất %v", el)
	}
	if resp.StatusCode == http.StatusOK {
		t.Fatal("không được trả 200 khi hết hạn chờ")
	}
}

// Khi quota tài khoản đang cạn (breaker chặn), mốc QUOTA HỒI phải được trả về client - nhưng KHÔNG được
// dùng làm header Retry-After: client SDK chỉ honor tới retry.maxDelayMs (mặc định 5 phút), header hàng
// giờ làm client ABORT chứ không retry (đo 2026-11-10: retry-after-ms=32391000 > 300000 -> chết cứng).
// Nên: Retry-After = giá trị nhỏ để client retry được, mốc quota hồi THẬT nằm ở header X-Quota-Reset.
func TestRetryAfterUsesQuotaResetWhenCapped(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7200") // 2 giờ
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":{"message":"FreeUsageLimitError","type":"rate_limit_error"}}`))
	}, false)

	// dựng trạng thái "quota cạn cấp tài khoản": 3 egress, có IP mới, 2 họ đường mạng
	g := newGate()
	g.noteFreshCap("proxy1", 2*time.Hour, true, "m1", "")
	g.noteFreshCap("proxy2", 2*time.Hour, true, "m1", "")
	g.noteFreshCap("res1", 2*time.Hour, true, "m2", "")
	old := acctGate
	acctGate = g
	defer func() { acctGate = old }()

	resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("phải trả 429, got %d", resp.StatusCode)
	}
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil {
		t.Fatalf("phải có header Retry-After, got %q", resp.Header.Get("Retry-After"))
	}
	// Retry-After phải NHỎ để client SDK honor được (nếu không client abort, không retry).
	limit := cfg.RetryAfterMax
	if limit <= 0 {
		limit = 60 * time.Second
	}
	if time.Duration(ra)*time.Second > limit+time.Second {
		t.Fatalf("Retry-After phải được giới hạn để client honor được (<=%v), got %ds", limit, ra)
	}
	// Mốc quota hồi thật vẫn phải có, ở header phụ.
	reset, err := strconv.Atoi(resp.Header.Get("X-Quota-Reset"))
	if err != nil {
		t.Fatalf("phải có header X-Quota-Reset (mốc quota hồi thật), got %q", resp.Header.Get("X-Quota-Reset"))
	}
	if time.Until(time.Unix(int64(reset), 0)) < time.Hour {
		t.Fatalf("X-Quota-Reset phải là mốc quota hồi (>=1h), got %s", time.Unix(int64(reset), 0))
	}
}

// Kịch bản đúng sự cố 2026-11-10: model đã cạn quota trên MỌI egress -> bộ điều phối trả 429. Header
// Retry-After phải ngắn (client retry được) chứ không phải mốc nửa đêm UTC (9h) làm client abort.
func TestRateLimitedHeaderIsClientFriendly(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: [DONE]\n\n"))
	}, false)

	// model đã cạn trên egress duy nhất, hạn tới ~9h (mốc quota hồi của upstream)
	noteModelExhausted("deepseek-v4.1-flash-free", "direct", time.Now().Add(9*time.Hour), 1)

	resp, body := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("phải trả 429, got %d (%s)", resp.StatusCode, body)
	}
	if !strings.Contains(body, "rate limited") {
		t.Fatalf("body phải nói rõ bị rate limit, got %s", body)
	}
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil {
		t.Fatalf("phải có header Retry-After, got %q", resp.Header.Get("Retry-After"))
	}
	limit := cfg.RetryAfterMax
	if limit <= 0 {
		limit = 60 * time.Second
	}
	if time.Duration(ra)*time.Second > limit+time.Second {
		t.Fatalf("Retry-After phải ngắn để client retry được (<=%v), got %ds", limit, ra)
	}
	reset, err := strconv.Atoi(resp.Header.Get("X-Quota-Reset"))
	if err != nil {
		t.Fatalf("phải có header X-Quota-Reset, got %q", resp.Header.Get("X-Quota-Reset"))
	}
	if time.Until(time.Unix(int64(reset), 0)) < time.Hour {
		t.Fatalf("X-Quota-Reset phải là mốc quota hồi thật (>=1h), got %s", time.Unix(int64(reset), 0))
	}
}

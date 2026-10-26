package main

import (
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
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

// Hết hạn rồi thì KHÔNG được gửi thêm request lên upstream (trước đây attempt đầu tiên không bị chặn).
func TestNoUpstreamCallAfterDeadline(t *testing.T) {
	var hits atomic.Int32
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		time.Sleep(2 * time.Second)
		w.Write([]byte("data: {}\n\n"))
	}, false)

	old := cfg.MaxWait
	cfg.MaxWait = 200 * time.Millisecond
	defer func() { cfg.MaxWait = old }()

	req, _ := http.NewRequest("POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
	if n := hits.Load(); n > 1 {
		t.Fatalf("hết hạn rồi mà còn gửi %d request lên upstream", n)
	}
}

// Khi quota tài khoản đang cạn (breaker chặn), Retry-After phải là mốc QUOTA HỒI chứ không phải cooldown
// của egress (vài chục giây) - client retry theo hạn cooldown sẽ lại nhận 429 ngay.
func TestRetryAfterUsesQuotaResetWhenCapped(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7200") // 2 giờ
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"error":{"message":"FreeUsageLimitError","type":"rate_limit_error"}}`))
	}, false)

	// dựng trạng thái "quota cạn cấp tài khoản": 3 egress, có IP mới, 2 họ đường mạng
	g := newGate()
	g.noteFreshCap("proxy1", 2*time.Hour, true)
	g.noteFreshCap("proxy2", 2*time.Hour, true)
	g.noteFreshCap("res1", 2*time.Hour, true)
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
	if ra < 3600 {
		t.Fatalf("Retry-After phải là mốc quota hồi (>=1h), got %ds", ra)
	}
}

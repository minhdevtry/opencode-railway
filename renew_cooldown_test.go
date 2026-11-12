package main

import (
	"net/http"
	"strconv"
	"testing"
	"time"
)

// Xoay danh tính cho 1 model đã cạn ở mọi đường gần như vô ích nhưng làm mất sức chứng của cả pool
// (đo 2026-11-12: 7 lần xoay/3 phút, mỗi lần chặn 1 egress 10-15s -> request xếp hàng rồi 429).
// Vì vậy mỗi model chỉ được xoay 1 lần trong MODEL_RENEW_COOLDOWN.
func TestModelRenewThrottle(t *testing.T) {
	gw, mgr := setup(t, func(w http.ResponseWriter, r *http.Request) {
		sse(w, "data: [DONE]\n\n")
	}, false)
	_ = gw
	cfg.ModelRenewCooldown = 50 * time.Millisecond

	const model = "deepseek-v4.1-flash-free"
	if !mgr.allowModelRenew(model) {
		t.Fatal("lần đầu phải được xoay danh tính")
	}
	mgr.noteModelRenew(model)
	if mgr.allowModelRenew(model) {
		t.Error("vừa xoay xong thì không được xoay tiếp ngay")
	}
	time.Sleep(60 * time.Millisecond)
	if !mgr.allowModelRenew(model) {
		t.Error("hết cooldown thì phải xoay được lại")
	}
}

// Model đã cạn ở MỌI đường và lần xoay danh tính gần nhất không cứu được -> trả 429 NGAY kèm mốc hồi,
// không bắt client ngồi hết MAX_WAIT rồi mới nhận 429.
func TestFailFastWhenModelExhaustedOnEveryPath(t *testing.T) {
	gw, mgr := setup(t, func(w http.ResponseWriter, r *http.Request) {
		sse(w, "data: [DONE]\n\n")
	}, true) // 2 egress (direct + tor-1): nếu không fail-fast thì request phải chờ hết MAX_WAIT
	torAvailable = true // giả lập prod: có pool WARP để xoay danh tính -> request được giữ trong hàng đợi
	cfg.TorMax = 4
	cfg.ModelRenewCooldown = time.Minute
	cfg.MaxWait = 5 * time.Second

	const model = "deepseek-v4.1-flash-free"
	// Model cạn ở MỌI đường (2 egress) -> không còn đường nào phục vụ được.
	noteModelExhausted(model, "direct", time.Now().Add(2*time.Hour), 2)
	noteModelExhausted(model, "tor-1", time.Now().Add(2*time.Hour), 2)
	mgr.noteModelRenew(model) // đã xoay danh tính cho model này rồi, IP mới vẫn cạn

	start := time.Now()
	resp, body := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("phải trả 429, got %d (%s)", resp.StatusCode, body)
	}
	if elapsed > time.Second {
		t.Errorf("phải trả 429 ngay, không chờ hết MAX_WAIT (mất %v)", elapsed)
	}
	ra, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err != nil || ra <= 0 {
		t.Errorf("phải có Retry-After dương (mốc quota hồi), got %q", resp.Header.Get("Retry-After"))
	}
}

package main

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// X-Max-Wait phải là TỔNG ngân sách chờ của request, dùng chung cho mọi lần forward() (kể cả retry).
// Nếu forward() tính lại hạn từ cfg.MaxWait thì client đặt 1s vẫn chờ tới MAX_WAIT (5s trong test này).
func TestMaxWaitIsTotalBudgetAcrossRetries(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		// 429 + Retry-After ngắn: gateway thử lại liên tục nhưng KHÔNG làm egress nghỉ lâu (nếu cooldown dài
		// thì request fast-fail ngay và không phân biệt được hạn nào được dùng).
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}, false)

	old, oldAtt := cfg.MaxWait, cfg.MaxAttempts
	cfg.MaxWait = 5 * time.Second
	cfg.MaxAttempts = 1000 // để vòng retry chạy tới hết hạn (nếu không, hết MaxAttempts là hết luôn -> không phân biệt được)
	defer func() { cfg.MaxWait, cfg.MaxAttempts = old, oldAtt }()

	req, _ := http.NewRequest("POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Max-Wait", "1s")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	el := time.Since(start)
	if err == nil {
		resp.Body.Close()
	}
	if el > 2500*time.Millisecond {
		t.Fatalf("client đặt X-Max-Wait=1s nhưng chờ %v (>2.5s) -> hạn bị tính lại", el)
	}
}

// Hết hạn thì KHÔNG được gửi thêm request lên upstream. Upstream báo lỗi context (loại model rồi thử model khác)
// nên gateway cứ thử lại; nếu không có check deadline thì nó thử cả pool.
func TestNoUpstreamCallAfterDeadline(t *testing.T) {
	var hits atomic.Int32
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"This model's maximum context length is 200000 tokens","type":"invalid_request_error"}}`))
	}, false)

	old := cfg.MaxWait
	cfg.MaxWait = 100 * time.Millisecond // ngắn hơn cả khoảng nghỉ 150ms giữa 2 lần thử -> attempt sau chắc chắn bị chặn
	defer func() { cfg.MaxWait = old }()

	resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"x"}]}`)
	resp.Body.Close()
	if n := hits.Load(); n > 1 {
		t.Fatalf("hết hạn rồi mà còn gửi %d request lên upstream (tối đa 1)", n)
	}
}

// Client huỷ request giữa chừng KHÔNG được phạt egress: lỗi do phía client, không phải do đường mạng.
func TestClientAbortDoesNotCoolEgress(t *testing.T) {
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"}}]}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done() // chờ client huỷ
	}, false)

	e := m.egresses[0]
	req, _ := http.NewRequest("POST", gw.URL+"/v1/chat/completions", strings.NewReader(`{"model":"deepseek","stream":true,"messages":[{"role":"user","content":"x"}]}`))
	req.Header.Set("Content-Type", "application/json")
	c := &http.Client{Timeout: 300 * time.Millisecond}
	resp, err := c.Do(req)
	if err == nil {
		b := resp.Body
		b.Read(make([]byte, 16))
		b.Close()
	}
	time.Sleep(300 * time.Millisecond)
	if e.downUntil.Load() != 0 {
		t.Fatal("client huỷ request không được làm egress nghỉ (downUntil)")
	}
}

// 1 request 200 ở egress đang bị cooldown lỗi TCP không được xoá bộ đếm lỗi (nếu xoá thì backoff không bao giờ leo).
// Kịch bản thật: 1 request đang chạy dở thì egress gặp lỗi TCP ở request khác, rồi request cũ trả 200.
func TestSuccessDoesNotResetErrorCountDuringCooldown(t *testing.T) {
	release := make(chan struct{})
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}, false)

	e := m.egresses[0]
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
		resp.Body.Close()
	}()

	// chờ request đã vào upstream rồi giả lập lỗi TCP ở request khác
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.inflight.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.errRun.Store(3)
	e.coolEgress(time.Minute)
	close(release)

	<-done // chờ request xong trước khi test kết thúc (cfg/defer đổi sau, không được race với request đang chạy)
	if e.errRun.Load() == 0 {
		t.Fatal("egress đang cooldown mà bộ đếm lỗi bị xoá bởi 1 request 200 -> backoff không bao giờ leo")
	}
}

// DATA RACE: probeAccount đọc freeModelPool phải dưới poolMu (prunePool ghi pool đó ở goroutine khác).
// Chạy với -race để bắt.
func TestProbeAccountRaceOnFreeModelPool(t *testing.T) {
	_, m := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			prunePool("deepseek-v4.1-flash-free")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.probeAccount()
		}
	}()
	wg.Wait()
}

// 200 của session thuộc IP CŨ (request đang chạy dở khi IP được đổi) KHÔNG được mở breaker.
// Chỉ 200 của cặp (IP, session) MỚI mới chứng minh quota đã hồi.
func TestOldSessionSuccessDoesNotOpenBreaker(t *testing.T) {
	release := make(chan struct{})
	// SessionIDMode phải đặt TRƯỚC khi có request nào chạy (đổi cfg giữa chừng là data race).
	oldMode := cfg.SessionIDMode
	cfg.SessionIDMode = "egress" // cần session để có es != nil

	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}, false)

	// breaker đang chặn
	g := newGate()
	g.noteFreshCap("proxy1", time.Hour, true, "m1")
	g.noteFreshCap("proxy2", time.Hour, true, "m1")
	g.noteFreshCap("proxy3", time.Hour, true, "m1")
	g.noteFreshCap("res1", time.Hour, true, "m2")
	oldGate := acctGate
	acctGate = g
	defer func() { acctGate = oldGate }()
	if !g.capped() {
		t.Fatal("phải trip trước")
	}

	e := m.egresses[0]
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
		resp.Body.Close()
	}()

	// chờ request đã vào upstream, rồi đổi IP (session của request đang chạy thuộc thế hệ CŨ)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.inflight.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.sess.reset()
	e.markIPChanged()
	close(release)

	<-done // chờ request xong rồi mới đụng lại cfg (tránh race)
	cfg.SessionIDMode = oldMode
	if !acctGate.capped() {
		t.Fatal("200 của session IP cũ không được mở breaker")
	}
}

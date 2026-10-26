package main

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// Lỗi TCP của 1 egress phải cho CẢ egress đó nghỉ (mọi model), không chỉ model đang gọi: nếu chỉ nghỉ theo model thì
// model khác lại chọn lại đúng proxy hỏng và lại reset (đây là lỗi client báo: res3 reset liên tục, gateway trả 502).
func TestTransportErrorCoolsWholeEgress(t *testing.T) {
	var calls atomic.Int32
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			panic("upstream chết giữa chừng") // httptest đóng connection -> client thấy lỗi transport
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}, false)

	// thêm 1 egress dự phòng để request vẫn thành công sau khi egress hỏng bị nghỉ
	ok := newEgress("ok", kindProxy, nil)
	ok.tier = 2
	ok.daemon = "ok"
	tagged(ok)
	ok.ready.Store(true)
	m.add(ok)

	direct := m.egresses[0]
	resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"hi"}]}`)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("request phải thành công qua egress dự phòng, got %d", resp.StatusCode)
	}
	if direct.downUntil.Load() == 0 {
		t.Fatal("lỗi transport phải khiến egress nghỉ (downUntil > 0)")
	}
	// nghỉ cho MỌI model, không chỉ model vừa gọi
	if direct.available("model-khac") {
		t.Fatal("egress đang nghỉ phải chặn mọi model")
	}
}

// 1 request thành công KHÔNG được xoá cooldown của egress đó: request khác (đang chạy song song) có thể thành công
// trong khi egress vẫn đang reset với request kia. Xoá cooldown lúc đó là proxy hỏng được chọn lại ngay -> lại 502.
func TestSuccessDoesNotClearEgressCooldown(t *testing.T) {
	release := make(chan struct{})
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		<-release // giữ request treo để ta kịp đặt cooldown giữa chừng
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}, false)

	e := m.egresses[0]
	var status atomic.Int32
	go func() {
		resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"hi"}]}`)
		status.Store(int32(resp.StatusCode))
		resp.Body.Close()
	}()

	// chờ request đã vào upstream rồi đặt cooldown (giống 1 request khác vừa gặp lỗi transport)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if e.inflight.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	e.coolEgress(time.Minute)
	close(release)

	for i := 0; i < 100 && status.Load() == 0; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if status.Load() != http.StatusOK {
		t.Fatalf("request treo phải trả 200, got %d", status.Load())
	}
	if e.downUntil.Load() == 0 {
		t.Fatal("1 request thành công không được xoá cooldown của egress đang lỗi")
	}
}

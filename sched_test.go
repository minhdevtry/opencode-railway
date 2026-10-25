package main

import (
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 2000 request đồng thời qua 4 egress có trần in-flight: tất cả phải 200, không egress nào vượt trần, slot trả đủ.
func TestSchedulerBurstNoLeak(t *testing.T) {
	var cur, peak atomic.Int32
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		n := cur.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}, false)
	cfg.MaxConcurrency = 5000
	concurrencySem = make(chan struct{}, cfg.MaxConcurrency)
	cfg.QueueWait = time.Minute
	cfg.MaxWait = time.Minute
	for _, name := range []string{"p1", "p2", "p3"} {
		e := newEgress(name, kindProxy, nil)
		e.tier = 2
		e.daemon = name
		tagged(e)
		e.ready.Store(true)
		m.add(e)
	}
	for _, e := range m.snapshot() {
		e.limit.Store(5)
	}
	var wg sync.WaitGroup
	var bad atomic.Int32
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
				strings.NewReader(`{"model":"deepseek","messages":[{"role":"user","content":"hi"}]}`))
			if err != nil || resp.StatusCode != 200 {
				bad.Add(1)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d request lỗi", bad.Load())
	}
	if p := peak.Load(); p > 20 {
		t.Errorf("vượt trần: %d request đồng thời tới upstream (trần 4 egress x 5)", p)
	}
	time.Sleep(100 * time.Millisecond)
	for _, e := range m.snapshot() {
		if n := e.inflight.Load(); n != 0 {
			t.Errorf("%s còn inflight=%d sau khi xong (rò slot)", e.Name, n)
		}
	}
	if queuedHigh.Load() != 0 {
		t.Errorf("hàng đợi còn %d", queuedHigh.Load())
	}
}

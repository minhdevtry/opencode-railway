package main

import (
	"net/http"
	"sync"
	"time"
)

// ======================== SỨC CHỨA SỐNG (cho client tự chỉnh concurrency) ========================
//
// GET /v1/capacity: client gọi "auto" chỉ cần giữ số request đang gửi quanh recommended_concurrency (gửi nhiều hơn vẫn
// được: phần dư xếp hàng ở gateway, không bị lỗi, chỉ chờ lâu hơn). Lô rất lớn thì dùng /v1/batches.

var okRate = &rateCounter{}

// rateCounter đếm sự kiện theo từng giây trong 60 giây gần nhất.
type rateCounter struct {
	mu   sync.Mutex
	secs [60]int64
	cnt  [60]int
}

func (r *rateCounter) add() {
	now := time.Now().Unix()
	i := now % 60
	r.mu.Lock()
	if r.secs[i] != now {
		r.secs[i], r.cnt[i] = now, 0
	}
	r.cnt[i]++
	r.mu.Unlock()
}

func (r *rateCounter) perSecond() float64 {
	now := time.Now().Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for i := range r.secs {
		if now-r.secs[i] < 60 {
			total += r.cnt[i]
		}
	}
	return float64(total) / 60
}

func liveCapacityHandler(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	usable, slots, inflight := 0, 0, 0
	budgetLeft, budgetKnown := 0, 0
	for _, e := range mgr.snapshot() {
		inflight += int(e.inflight.Load())
		if !e.ready.Load() || e.expired.Load() || (cfg.SessionIDMode == "egress" && !e.sess.usable()) {
			continue
		}
		if time.Now().UnixNano() < e.downUntil.Load() {
			continue // egress đang nghỉ cả egress (lỗi TCP) -> không tính vào sức chứa dùng được
		}
		usable++
		slots += int(e.limit.Load())
		if b := e.sess.budgetLeft(); b >= 0 {
			budgetKnown++
			budgetLeft += b
		}
	}
	// Giữ hàng chờ ngắn: đủ để mọi slot luôn có việc ngay khi trống (x1.5) nhưng không dồn quá nhiều chờ lâu.
	rec := slots * 3 / 2
	if rec < 1 {
		rec = 1
	}
	writeJSON(w, map[string]any{
		"object":                  "capacity",
		"usable_egress":           usable,
		"upstream_slots":          slots,
		"upstream_inflight":       inflight,
		"queued_sync":             queuedHigh.Load(),
		"queued_jobs":             queuedLow.Load(),
		"ok_per_second_60s":       float64(int(okRate.perSecond()*100)) / 100,
		"recommended_concurrency": rec,
		"model":                   "auto",
		"session_budget_left":     budgetLeft,  // tổng số session MỚI còn mở được hôm nay trên các IP đang dùng
		"session_budget_known":    budgetKnown, // số egress có ngân sách hữu hạn (không tính vào tổng)
		"bulk_endpoint":           "/v1/batches",
		"note":                    "Gửi vượt recommended_concurrency vẫn không lỗi (xếp hàng tối đa MAX_WAIT); lô lớn nên dùng /v1/batches.",
	})
}

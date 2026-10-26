package main

import (
	"log"
	"sync"
	"time"
)

// ======================== NGÂN SÁCH THEO MODEL ========================
//
// Đo 2026-10-26 (research/limit-shape): hạn mức free của upstream KHÔNG tính chung cho mọi model, mà TÍCH RIÊNG
// TỪNG MODEL. Cùng một IP, cùng session mới:
//   - glm-5.3-flash-free      -> 429 FreeUsageLimitError (đã cạn)
//   - deepseek-v4.1-flash-free-> 429 FreeUsageLimitError (đã cạn)
//   - mimo-v2.6-flash-free     -> 429 FreeUsageLimitError (đã cạn)
//   - nemotron-3.5-lightning-free -> vẫn 200
//   - beam-free                -> vẫn 200
// Ngoài ra hạn mức cũng không bám theo IP: sau khi một IP cạn, đổi sang IPv6 mới (kể cả khác /48) thì model đó vẫn
// cạn -> ngân sách là của MODEL, dùng chung cho mọi IP.
//
// Hệ quả: tổng sức chứa free = tổng ngân sách của các model đang có. Gateway phải:
//   1. Biết model nào đã cạn và cạn tới lúc nào (theo Retry-After của upstream) -> KHÔNG gửi request vào model đã cạn.
//   2. Xoay vòng qua các model còn ngân sách.
//   3. Tự mở lại model khi tới mốc reset (upstream luôn trả Retry-After tới 00:00 UTC).
//
// Trước khi có phần này, gateway chỉ biết "model này vừa lỗi" theo từng request: mỗi model đã cạn tốn thêm vài
// request 429 vô ích, và request của client phải chờ qua các lần thử 429 đó.

var (
	budgetMu sync.Mutex
	budgetEx map[string]time.Time // model -> mốc hết hạn (sau đó model dùng lại được)
)

func init() { budgetEx = map[string]time.Time{} }

// noteModelExhausted: model đã hết ngân sách tới mốc until (thường là 00:00 UTC).
func noteModelExhausted(model string, until time.Time) {
	if model == "" || until.IsZero() {
		return
	}
	budgetMu.Lock()
	defer budgetMu.Unlock()
	if old, ok := budgetEx[model]; ok && old.After(until) {
		return // đã ghi hạn dài hơn rồi
	}
	budgetEx[model] = until
	log.Printf("[ModelBudget] %s đã hết ngân sách free tới %s (không gửi request vào model này nữa)",
		model, until.UTC().Format("15:04 UTC"))
}

// modelExhausted: model đã hết ngân sách chưa (tới mốc reset)?
func modelExhausted(model string) bool {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	until, ok := budgetEx[model]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(budgetEx, model) // hết hạn -> dùng lại được, không cần thăm dò
		return false
	}
	return true
}

// modelBudgetSnapshot: trạng thái ngân sách từng model (để hiện trong /status và /v1/capacity).
func modelBudgetSnapshot() map[string]string {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	out := map[string]string{}
	now := time.Now()
	for m, until := range budgetEx {
		if now.After(until) {
			continue
		}
		out[m] = until.UTC().Format(time.RFC3339)
	}
	return out
}

// usableModels: các model trong pool còn ngân sách (bỏ model đã cạn). Nếu TẤT CẢ đều cạn thì trả về nguyên pool
// (để vẫn thử, biết đâu upstream đã reset mà ta chưa biết).
func usableModels(pool []string) []string {
	out := make([]string, 0, len(pool))
	for _, m := range pool {
		if !modelExhausted(m) {
			out = append(out, m)
		}
	}
	if len(out) == 0 {
		return pool
	}
	return out
}

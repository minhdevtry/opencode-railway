package main

import (
	"log"
	"sync"
	"time"
)

// ======================== NGÂN SÁCH THEO (MODEL, IP) ========================
//
// Đo 2026-10-26 (research/limit-shape):
//
//  1. Hạn mức KHÔNG tính chung cho mọi model: cùng IP, cùng session mới -> glm/deepseek/mimo trả 429
//     FreeUsageLimitError còn nemotron/beam vẫn 200 (nemotron 250/250 request).
//  2. Hạn mức cũng không bám theo IP một cách đơn giản: sau khi 1 IP cạn, đổi sang IPv6 mới (kể cả khác /48)
//     thì model đó vẫn cạn -> các block IPv6 của Cloudflare WARP dùng chung với rất nhiều người dùng WARP
//     khác, nên chúng thường đã cạn sẵn. Cùng lúc đó, IP của proxy dân cư (riêng) vẫn còn ngân sách cho model đó.
//
// Kết luận: đơn vị tiêu thụ là cặp (model, IP). Sức chứa free = số cặp (model, IP) còn ngân sách.
//
// Gateway phải biết cặp nào đã cạn để không gửi request vào nó (mỗi request 429 là vừa lãng phí vừa làm
// client chờ), nhưng KHÔNG được loại model ở mức toàn cục khi chỉ 1 IP đã cạn - các IP khác vẫn phục vụ được
// model đó. Vì vậy registry ở mức (model, egress), kèm 1 lối tắt ngắn: khi model đã cạn trên hầu hết các
// egress thì bỏ nó khỏi routing toàn cục cho tới mốc reset.

var (
	budgetMu    sync.Mutex
	budgetPairs map[string]map[string]time.Time // model -> egress -> mốc hết hạn
	budgetAll   map[string]time.Time            // model -> mốc hết hạn (model cạn trên HẦU HẾT các egress)
)

func init() {
	budgetPairs = map[string]map[string]time.Time{}
	budgetAll = map[string]time.Time{}
}

// noteModelExhausted: model đã hết ngân sách trên egress này, tới mốc until (thường 00:00 UTC).
func noteModelExhausted(model, egress string, until time.Time) {
	if model == "" || until.IsZero() {
		return
	}
	budgetMu.Lock()
	defer budgetMu.Unlock()
	if budgetPairs[model] == nil {
		budgetPairs[model] = map[string]time.Time{}
	}
	if old, ok := budgetPairs[model][egress]; ok && old.After(until) {
		return
	}
	budgetPairs[model][egress] = until
	// Nếu model đã cạn trên hầu hết các egress -> bỏ model khỏi routing toàn cục (lối tắt).
	poolMu.RLock()
	total := len(freeModelPool)
	poolMu.RUnlock()
	dead := 0
	for _, u := range budgetPairs[model] {
		if time.Now().Before(u) {
			dead++
		}
	}
	if total > 0 && dead > total/2 { // đa số rõ ràng, không phải "đúng một nửa"
		if old, ok := budgetAll[model]; !ok || until.After(old) {
			budgetAll[model] = until
			log.Printf("[ModelBudget] %s đã hết ngân sách trên %d/%d đường -> bỏ khỏi routing tới %s",
				model, dead, total, until.UTC().Format("15:04 UTC"))
		}
	}
}

// modelExhaustedOn: cặp (model, egress) này đã hết ngân sách chưa?
func modelExhaustedOn(model, egress string) bool {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	if until, ok := budgetAll[model]; ok {
		if time.Now().Before(until) {
			return true
		}
		delete(budgetAll, model)
	}
	m, ok := budgetPairs[model]
	if !ok {
		return false
	}
	until, ok := m[egress]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(m, egress)
		return false
	}
	return true
}

// modelBudgetSnapshot: trạng thái ngân sách (để hiện trong /status và /v1/capacity).
func modelBudgetSnapshot() map[string]any {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	now := time.Now()
	out := map[string]any{}
	for model, byEgress := range budgetPairs {
		dead := map[string]string{}
		for e, u := range byEgress {
			if now.Before(u) {
				dead[e] = u.UTC().Format(time.RFC3339)
			}
		}
		if len(dead) > 0 {
			out[model] = dead
		}
	}
	return out
}

// modelResetAt: mốc hết hạn sớm nhất của model trên bất kỳ egress nào (dùng cho Retry-After).
func modelResetAt(model string) (time.Time, bool) {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	now := time.Now()
	var best time.Time
	if until, ok := budgetAll[model]; ok && now.Before(until) {
		best = until
	}
	for _, u := range budgetPairs[model] {
		if now.Before(u) && (best.IsZero() || u.Before(best)) {
			best = u
		}
	}
	if best.IsZero() {
		return time.Time{}, false
	}
	return best, true
}

// resetModelBudget (chỉ dùng trong test).
func resetModelBudget() {
	budgetMu.Lock()
	budgetPairs = map[string]map[string]time.Time{}
	budgetAll = map[string]time.Time{}
	budgetMu.Unlock()
}

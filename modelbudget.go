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
// model đó. Vì vậy registry ở mức (model, egress), kèm 1 lối tắt ngắn: chỉ khi model đã cạn ở MỌI egress
// còn dùng được thì mới bỏ nó khỏi routing toàn cục cho tới mốc reset.
//
// BUG ĐÃ SỬA 2026-10-26: ngưỡng lối tắt trước đây so "số egress đã cạn" với len(freeModelPool) - tức là
// so với SỐ MODEL trong pool (7), không phải số egress (32). Hệ quả: model chỉ cần cạn trên 4/32 egress là
// bị bỏ khỏi routing tới 00:00 UTC, trong khi 24 egress còn tốt vẫn phục vụ được -> gateway trả 429
// "All egress paths are rate limited for this model" cho cả model đó.

var (
	budgetMu    sync.Mutex
	budgetPairs map[string]map[string]time.Time // model -> egress -> mốc hết hạn
	budgetAll   map[string]time.Time            // model -> mốc hết hạn (model cạn ở MỌI egress còn dùng được)
)

func init() {
	budgetPairs = map[string]map[string]time.Time{}
	budgetAll = map[string]time.Time{}
}

// noteModelExhausted: model đã hết ngân sách trên egress này, tới mốc until (thường 00:00 UTC).
// totalEgresses: số egress CÒN DÙNG ĐƯỢC trong pool - dùng để biết khi nào model đã cạn ở mọi nơi.
func noteModelExhausted(model, egress string, until time.Time, totalEgresses int) {
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
	// Lối tắt toàn cục: chỉ khi model đã cạn ở MỌI egress còn dùng được thì mới bỏ khỏi routing
	// (không có đường nào phục vụ được nữa -> báo 429 ngay thay vì xếp hàng tới MAX_WAIT).
	// So sánh với SỐ EGRESS, không phải số model trong pool (xem chú thích đầu file).
	dead := 0
	for _, u := range budgetPairs[model] {
		if time.Now().Before(u) {
			dead++
		}
	}
	if totalEgresses > 0 && dead >= totalEgresses {
		if old, ok := budgetAll[model]; !ok || until.After(old) {
			budgetAll[model] = until
			log.Printf("[ModelBudget] %s đã hết ngân sách ở MỌI đường (%d/%d) -> bỏ khỏi routing tới %s",
				model, dead, totalEgresses, until.UTC().Format("15:04 UTC"))
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

// modelUsableEgresses: số egress còn ngân sách cho model này. Nếu = 0 thì model này đã cạn trên MỌI IP ->
// cần một IP mới (xoay danh tính) thì mới phục vụ lại được.
func modelUsableEgresses(model string, total int) int {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	now := time.Now()
	dead := 0
	for _, u := range budgetPairs[model] {
		if now.Before(u) {
			dead++
		}
	}
	if total <= 0 {
		return 0
	}
	if n := total - dead; n > 0 {
		return n
	}
	return 0
}

// clearBudgetForEgress: IP của egress này vừa đổi -> cặp (model, IP) cũ không còn ý nghĩa, xoá đi để model
// được dùng lại với IP mới.
func clearBudgetForEgress(egress string) {
	budgetMu.Lock()
	defer budgetMu.Unlock()
	for model, byEgress := range budgetPairs {
		if _, ok := byEgress[egress]; ok {
			delete(byEgress, egress)
			// Egress này có IP mới -> model không còn cạn ở mọi nơi nữa -> bỏ lệnh cấm toàn cục.
			delete(budgetAll, model)
		}
		if len(byEgress) == 0 {
			delete(budgetPairs, model)
		}
	}
	if len(budgetPairs) == 0 {
		budgetAll = map[string]time.Time{}
	}
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

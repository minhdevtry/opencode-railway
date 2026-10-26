package main

import (
	"testing"
	"time"
)

// Hạn mức free là của cặp (model, IP): model đã cạn trên IP này thì KHÔNG được loại model đó ở mức toàn cục -
// các IP khác vẫn còn ngân sách cho model đó.
func TestModelBudgetIsPerModelAndEgress(t *testing.T) {
	resetModelBudget()
	defer resetModelBudget()

	noteModelExhausted("glm-5.3-flash-free", "proxy1", time.Now().Add(2*time.Hour))
	if !modelExhaustedOn("glm-5.3-flash-free", "proxy1") {
		t.Fatal("cặp (model, egress) vừa ghi phải bị coi là hết ngân sách")
	}
	if modelExhaustedOn("glm-5.3-flash-free", "proxy2") {
		t.Fatal("IP khác phải còn ngân sách cho model đó")
	}
	if modelExhaustedOn("nemotron-3.5-lightning-free", "proxy1") {
		t.Fatal("model khác không được coi là hết ngân sách")
	}
}

// Hết hạn (tới mốc reset) thì cặp đó dùng lại được, không cần thăm dò.
func TestModelBudgetExpires(t *testing.T) {
	resetModelBudget()
	defer resetModelBudget()

	noteModelExhausted("glm-5.3-flash-free", "proxy1", time.Now().Add(50*time.Millisecond))
	if !modelExhaustedOn("glm-5.3-flash-free", "proxy1") {
		t.Fatal("phải còn hết ngân sách trước mốc reset")
	}
	time.Sleep(80 * time.Millisecond)
	if modelExhaustedOn("glm-5.3-flash-free", "proxy1") {
		t.Fatal("tới mốc reset thì cặp phải dùng lại được")
	}
}

// Khi model đã cạn trên HẦU HẾT các egress -> bỏ model khỏi routing toàn cục (lối tắt để khỏi thử mãi).
func TestModelBudgetGlobalShortcutWhenMostEgressesDead(t *testing.T) {
	resetModelBudget()
	defer resetModelBudget()

	// 2 egress trong pool -> cạn trên cả 2 thì model bị loại toàn cục
	poolMu.Lock()
	old := freeModelPool
	freeModelPool = []string{"proxy1", "proxy2"}
	poolMu.Unlock()
	defer func() { poolMu.Lock(); freeModelPool = old; poolMu.Unlock() }()

	noteModelExhausted("glm-5.3-flash-free", "proxy1", time.Now().Add(time.Hour))
	if modelExhaustedOn("glm-5.3-flash-free", "proxy2") {
		t.Fatal("mới cạn 1/2 egress thì chưa được loại toàn cục")
	}
	noteModelExhausted("glm-5.3-flash-free", "proxy2", time.Now().Add(time.Hour))
	if !modelExhaustedOn("glm-5.3-flash-free", "proxy3") {
		t.Fatal("cạn trên hết các egress thì model phải bị loại toàn cục")
	}
}

// Ghi hạn dài hơn thì không được ghi đè bằng hạn ngắn hơn.
func TestModelBudgetKeepsLongestExpiry(t *testing.T) {
	resetModelBudget()
	defer resetModelBudget()

	noteModelExhausted("m1", "proxy1", time.Now().Add(2*time.Hour))
	noteModelExhausted("m1", "proxy1", time.Now().Add(30*time.Minute))
	snap := modelBudgetSnapshot()
	byEgress, _ := snap["m1"].(map[string]string)
	until, err := time.Parse(time.RFC3339, byEgress["proxy1"])
	if err != nil {
		t.Fatalf("mốc reset không đúng định dạng: %v", err)
	}
	if time.Until(until) < 90*time.Minute {
		t.Fatalf("phải giữ mốc hạn dài nhất, còn %v", time.Until(until))
	}
}

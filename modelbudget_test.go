package main

import (
	"testing"
	"time"
)

// Hạn mức free tính RIÊNG từng model (đo 2026-10-26): model đã cạn phải bị bỏ qua ngay, không gửi thêm request
// vào nó (mỗi request 429 là vừa lãng phí vừa làm client chờ).
func TestModelBudgetExcludesExhaustedModel(t *testing.T) {
	resetBudgetState()
	defer resetBudgetState()

	noteModelExhausted("glm-5.3-flash-free", time.Now().Add(2*time.Hour))
	if !modelExhausted("glm-5.3-flash-free") {
		t.Fatal("model vừa ghi phải bị coi là hết ngân sách")
	}
	if modelExhausted("nemotron-3.5-lightning-free") {
		t.Fatal("model khác không được coi là hết ngân sách")
	}
	// Bộ điều phối phải bỏ qua model đã cạn
	pool := []string{"glm-5.3-flash-free", "nemotron-3.5-lightning-free", "beam-free"}
	got := usableModels(pool)
	if len(got) != 2 {
		t.Fatalf("phải bỏ model đã cạn, còn lại %v", got)
	}
	for _, m := range got {
		if m == "glm-5.3-flash-free" {
			t.Fatal("model đã cạn vẫn còn trong danh sách dùng")
		}
	}
}

// Hết hạn (tới mốc reset) thì model dùng lại được, không cần thăm dò.
func TestModelBudgetExpires(t *testing.T) {
	resetBudgetState()
	defer resetBudgetState()

	noteModelExhausted("glm-5.3-flash-free", time.Now().Add(50*time.Millisecond))
	if !modelExhausted("glm-5.3-flash-free") {
		t.Fatal("phải còn hết ngân sách trước mốc reset")
	}
	time.Sleep(80 * time.Millisecond)
	if modelExhausted("glm-5.3-flash-free") {
		t.Fatal("tới mốc reset thì model phải dùng lại được")
	}
}

// Nếu TẤT CẢ model đều hết ngân sách thì vẫn phải thử (có thể upstream đã reset mà ta chưa biết) - không xếp hàng
// tới MAX_WAIT rồi trả 429.
func TestModelBudgetAllExhaustedStillTries(t *testing.T) {
	resetBudgetState()
	defer resetBudgetState()

	pool := []string{"a", "b"}
	for _, m := range pool {
		noteModelExhausted(m, time.Now().Add(time.Hour))
	}
	got := usableModels(pool)
	if len(got) != len(pool) {
		t.Fatalf("khi mọi model đều cạn thì vẫn phải thử, got %v", got)
	}
}

// Ghi hạn dài hơn thì không được ghi đè bằng hạn ngắn hơn.
func TestModelBudgetKeepsLongestExpiry(t *testing.T) {
	resetBudgetState()
	defer resetBudgetState()

	noteModelExhausted("m1", time.Now().Add(2*time.Hour))
	noteModelExhausted("m1", time.Now().Add(30*time.Minute))
	snap := modelBudgetSnapshot()
	if snap["m1"] == "" {
		t.Fatal("phải còn ghi hạn")
	}
	until, err := time.Parse(time.RFC3339, snap["m1"])
	if err != nil {
		t.Fatalf("mốc reset không đúng định dạng: %v", err)
	}
	if time.Until(until) < 90*time.Minute {
		t.Fatalf("phải giữ mốc hạn dài nhất, còn %v", time.Until(until))
	}
}

func resetBudgetState() {
	budgetMu.Lock()
	budgetEx = map[string]time.Time{}
	budgetMu.Unlock()
}

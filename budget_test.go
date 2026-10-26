package main

import (
	"testing"
)

// Ngân sách session mới trên 1 IP/ngày: hết thì KHÔNG mở thêm (chỉ để nhận 429), IP phải được đổi.
// Session ĐANG dùng vẫn được dùng lại bình thường (giới hạn của upstream là trên cặp (IP, session) MỚI).
func TestSessionDailyBudgetStopsOpeningSessions(t *testing.T) {
	old := cfg.SessionDailyBudget
	cfg.SessionDailyBudget = 2
	defer func() { cfg.SessionDailyBudget = old }()

	var p sessionPool
	if s := p.acquire(8, 1); s == nil { // perSession=1 -> session đầy ngay -> lần sau phải mở session mới
		t.Fatal("session đầu tiên phải được mở")
	}
	if s := p.acquire(8, 1); s == nil {
		t.Fatal("session thứ 2 phải được mở")
	}
	if s := p.acquire(8, 1); s != nil {
		t.Fatal("hết ngân sách session thì không được mở thêm")
	}
	if left := p.budgetLeft(); left != 0 {
		t.Fatalf("ngân sách còn lại phải = 0, got %d", left)
	}
	// Khi cả 2 session đã chết (không còn dùng được) và hết ngân sách thì IP không còn công suất -> phải đổi IP.
	p.mu.Lock()
	for _, s := range p.list {
		s.dead = true
	}
	p.mu.Unlock()
	if p.usable() {
		t.Fatal("IP đã hết ngân sách và không còn session sống thì không được coi là dùng được")
	}
}

// Đổi IP = ngân sách mới (đếm lại từ 0 cho IP mới).
func TestSessionDailyBudgetResetsOnIPChange(t *testing.T) {
	old := cfg.SessionDailyBudget
	cfg.SessionDailyBudget = 1
	defer func() { cfg.SessionDailyBudget = old }()

	var p sessionPool
	if p.acquire(8, 1) == nil {
		t.Fatal("session đầu tiên phải được mở")
	}
	if p.acquire(8, 1) != nil {
		t.Fatal("hết ngân sách thì không mở thêm")
	}
	p.reset() // đổi IP
	if p.acquire(8, 1) == nil {
		t.Fatal("IP mới phải có ngân sách mới")
	}
	if left := p.budgetLeft(); left != 0 {
		t.Fatalf("sau khi đổi IP, ngân sách còn lại = 0, got %d", left)
	}
}

// Ngân sách = 0 nghĩa là không giới hạn (mặc định cũ, để không đổi hành vi khi chưa bật).
func TestSessionDailyBudgetZeroMeansUnlimited(t *testing.T) {
	old := cfg.SessionDailyBudget
	cfg.SessionDailyBudget = 0
	defer func() { cfg.SessionDailyBudget = old }()

	var p sessionPool
	for i := 0; i < 20; i++ {
		if p.acquire(8, 1) == nil {
			t.Fatalf("phải mở được session thứ %d khi không giới hạn", i+1)
		}
	}
	if left := p.budgetLeft(); left != -1 {
		t.Fatalf("budgetLeft phải = -1 (không giới hạn), got %d", left)
	}
}

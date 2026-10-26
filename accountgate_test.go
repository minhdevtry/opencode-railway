package main

import (
	"testing"
	"time"
)

func TestAccountGateTripsAndClears(t *testing.T) {
	g := &accountGate{recent: map[string]time.Time{}}
	// retry-after ngắn (cửa sổ tốc độ) không được trip
	g.noteFreshCap("res1", time.Minute, true)
	if g.tripped() {
		t.Fatal("retry-after ngắn không được trip breaker")
	}
	// 1-2 egress chưa đủ
	g.noteFreshCap("proxy1", time.Hour, true)
	g.noteFreshCap("proxy2", time.Hour, true)
	if g.tripped() {
		t.Fatal("dưới ngưỡng distinct chưa được trip")
	}
	// đủ 3 egress khác nhau với retry-after dài -> trip
	g.noteFreshCap("proxy3", time.Hour, true)
	if !g.tripped() {
		t.Fatal("đủ 3 egress cạn cấp tài khoản phải trip")
	}
	// chặn tối đa acctGateMax dù retry-after báo 1h
	if d := time.Until(g.untilAt()); d > acctGateMax+time.Second {
		t.Fatalf("thời gian chặn phải <= acctGateMax, got %v", d)
	}
	// prober thấy quota hồi -> mở lại
	g.clear()
	if g.tripped() {
		t.Fatal("clear() phải mở lại breaker")
	}
	if g.capped() {
		t.Fatal("clear() phải mở lại cả trạng thái capped")
	}
}

// Cạn từng IP (mọi session đều ĐÃ từng chạy) thì đổi IP vẫn có ích -> không được trip.
func TestAccountGateNeedsFreshIPEvidence(t *testing.T) {
	g := &accountGate{recent: map[string]time.Time{}}
	g.noteFreshCap("proxy1", time.Hour, false)
	g.noteFreshCap("proxy2", time.Hour, false)
	g.noteFreshCap("proxy3", time.Hour, false)
	g.noteFreshCap("proxy4", time.Hour, false)
	if g.tripped() {
		t.Fatal("cạn từng IP không phải cạn cấp tài khoản: không được trip")
	}
	// thêm bằng chứng IP mới (session chưa từng chạy) -> trip
	g.noteFreshCap("proxy5", time.Hour, true)
	if !g.tripped() {
		t.Fatal("có bằng chứng IP mới cũng cạn thì phải trip")
	}
}

// Gate đang chặn thì bằng chứng mới phải gia hạn được (prober thăm dò vẫn thấy cạn).
func TestAccountGateRefreshWhileBlocked(t *testing.T) {
	g := &accountGate{recent: map[string]time.Time{}}
	g.noteFreshCap("proxy1", time.Hour, true)
	g.noteFreshCap("proxy2", time.Hour, true)
	g.noteFreshCap("proxy3", time.Hour, true)
	if !g.tripped() {
		t.Fatal("phải trip")
	}
	first := g.untilAt()
	// prober thăm dò lại sau 1s, vẫn cạn -> gia hạn
	time.Sleep(1100 * time.Millisecond)
	g.noteFreshCap("proxy1", time.Hour, true)
	if !g.tripped() {
		t.Fatal("gate phải vẫn chặn sau khi gia hạn")
	}
	if !g.untilAt().After(first) {
		t.Fatal("phải gia hạn mốc chặn")
	}
}

// capped() lâu hơn tripped(): quota cạn thì dừng xoay IP cho tới khi quota hồi, còn fast-fail thì chỉ trong cửa sổ chặn.
func TestAccountGateCappedLongerThanTripped(t *testing.T) {
	g := &accountGate{recent: map[string]time.Time{}}
	g.noteFreshCap("proxy1", 2*time.Hour, true)
	g.noteFreshCap("proxy2", 2*time.Hour, true)
	g.noteFreshCap("proxy3", 2*time.Hour, true)
	if !g.tripped() || !g.capped() {
		t.Fatal("phải vừa chặn vừa capped")
	}
	if d := time.Until(g.resetTime()); d <= acctGateMax {
		t.Fatalf("mốc quota hồi phải xa hơn cửa sổ chặn, got %v", d)
	}
}

func TestEgressWideCooldown(t *testing.T) {
	e := newEgress("p", kindProxy, nil)
	e.ready.Store(true)
	if !e.available("m") {
		t.Fatal("egress mới phải dùng được")
	}
	e.coolEgress(time.Minute)
	if e.available("m") || e.available("khac") {
		t.Fatal("coolEgress phải chặn MỌI model, không chỉ 1 model")
	}
}

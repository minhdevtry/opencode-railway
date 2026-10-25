package main

import (
	"testing"
	"time"
)

func TestAccountGateTripsAndClears(t *testing.T) {
	g := &accountGate{recent: map[string]time.Time{}}
	// retry-after ngắn (cửa sổ tốc độ) không được trip
	g.noteFreshCap("res1", time.Minute)
	if g.tripped() {
		t.Fatal("retry-after ngắn không được trip breaker")
	}
	// 1-2 egress chưa đủ
	g.noteFreshCap("proxy1", time.Hour)
	g.noteFreshCap("proxy2", time.Hour)
	if g.tripped() {
		t.Fatal("dưới ngưỡng distinct chưa được trip")
	}
	// đủ 3 egress khác nhau với retry-after dài -> trip
	g.noteFreshCap("proxy3", time.Hour)
	if !g.tripped() {
		t.Fatal("đủ 3 egress cạn cấp tài khoản phải trip")
	}
	// chặn tối đa acctGateMax dù retry-after báo 1h
	if d := time.Until(g.until_()); d > acctGateMax+time.Second {
		t.Fatalf("thời gian chặn phải <= acctGateMax, got %v", d)
	}
	// có request thành công -> mở lại ngay
	g.clear()
	if g.tripped() {
		t.Fatal("clear() phải mở lại breaker")
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

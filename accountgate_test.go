package main

import (
	"testing"
	"time"
)

func newGate() *accountGate { return &accountGate{recent: map[string]capEvidence{}} }

func TestAccountGateTripsAndClears(t *testing.T) {
	g := newGate()
	// retry-after ngắn (cửa sổ tốc độ) không được trip
	g.noteFreshCap("res1", time.Minute, true)
	if g.capped() {
		t.Fatal("retry-after ngắn không được trip breaker")
	}
	// 1-2 egress chưa đủ
	g.noteFreshCap("proxy1", time.Hour, true)
	g.noteFreshCap("proxy2", time.Hour, true)
	if g.capped() {
		t.Fatal("dưới ngưỡng distinct chưa được trip")
	}
	// đủ 3 egress khác nhau, có IP mới, đủ 2 họ đường mạng -> trip
	g.noteFreshCap("proxy3", time.Hour, true)
	g.noteFreshCap("res2", time.Hour, true)
	if !g.capped() {
		t.Fatal("đủ 3 egress cạn cấp tài khoản phải trip")
	}
	// chặn tối đa acctGateMax dù retry-after báo 1h
	if d := time.Until(g.untilAt()); d > acctGateMax+time.Second {
		t.Fatalf("thời gian chặn phải <= acctGateMax, got %v", d)
	}
	// IP mới trả 200 -> mở lại
	g.clear()
	if g.capped() {
		t.Fatal("clear() phải mở lại breaker")
	}
}

// Cạn từng IP (không có IP mới nào bị cạn) thì đổi IP vẫn có ích -> không được trip.
func TestAccountGateNeedsFreshIPEvidence(t *testing.T) {
	g := newGate()
	g.noteFreshCap("proxy1", time.Hour, false)
	g.noteFreshCap("proxy2", time.Hour, false)
	g.noteFreshCap("proxy3", time.Hour, false)
	g.noteFreshCap("proxy4", time.Hour, false)
	if g.capped() {
		t.Fatal("cạn từng IP không phải cạn cấp tài khoản: không được trip")
	}
}

// 16 container WARP dùng CHUNG IPv4 Cloudflare: 4 tên egress "proxy*" có thể là 1 IP. Bằng chứng phải đa dạng họ
// đường mạng, nếu không thì cạn từng IP bị coi nhầm là cạn cấp tài khoản (lúc đó đổi IP vẫn có ích).
func TestAccountGateNeedsMultipleFamilies(t *testing.T) {
	g := newGate()
	for _, e := range []string{"proxy1", "proxy2", "proxy3", "proxy4"} {
		g.noteFreshCap(e, time.Hour, true)
	}
	if g.capped() {
		t.Fatal("4 egress cùng họ WARP (có thể chung 1 IPv4) không được trip breaker tài khoản")
	}
	g.noteFreshCap("res1", time.Hour, true) // họ đường mạng thứ 2
	if !g.capped() {
		t.Fatal("có bằng chứng từ 2 họ đường mạng thì phải trip")
	}
}

// Bằng chứng "IP mới" cũ không được cộng dồn vào cụm mới: nó phải nằm trong cửa sổ thời gian.
func TestAccountGateFreshEvidenceExpires(t *testing.T) {
	g := newGate()
	g.noteFreshCap("proxy1", time.Hour, true) // bằng chứng IP mới, 1 egress
	// 3 egress khác bị cạn nhưng KHÔNG có IP mới, và bằng chứng cũ đã ngoài cửa sổ -> không trip
	g.recent["proxy1"] = capEvidence{at: time.Now().Add(-2 * acctGateWindow), fresh: true}
	g.noteFreshCap("proxy2", time.Hour, false)
	g.noteFreshCap("proxy3", time.Hour, false)
	g.noteFreshCap("res1", time.Hour, false)
	if g.capped() {
		t.Fatal("bằng chứng IP mới đã hết hạn thì không được trip")
	}
}

// Gate đang chặn thì bằng chứng mới phải gia hạn được (prober thăm dò vẫn thấy cạn).
func TestAccountGateRefreshWhileBlocked(t *testing.T) {
	g := newGate()
	g.noteFreshCap("proxy1", time.Hour, true)
	g.noteFreshCap("proxy2", time.Hour, true)
	g.noteFreshCap("proxy3", time.Hour, true)
	g.noteFreshCap("res1", time.Hour, true)
	if !g.capped() {
		t.Fatal("phải trip")
	}
	first := g.untilAt()
	// prober thăm dò lại sau 1s, vẫn cạn -> gia hạn
	time.Sleep(1100 * time.Millisecond)
	g.noteFreshCap("proxy1", time.Hour, true)
	if !g.capped() {
		t.Fatal("gate phải vẫn chặn sau khi gia hạn")
	}
	if !g.untilAt().After(first) {
		t.Fatal("phải gia hạn mốc chặn")
	}
}

// Mốc "quota hồi" cũ không được sót sang lần cap mới: client phải được báo "2 giờ" chứ không "24 giờ" cũ.
func TestAccountGateResetTimeOnlyExtends(t *testing.T) {
	g := newGate()
	g.noteFreshCap("proxy1", 4*time.Hour, true)
	g.noteFreshCap("proxy2", 4*time.Hour, true)
	g.noteFreshCap("proxy3", 4*time.Hour, true)
	g.noteFreshCap("res1", 4*time.Hour, true)
	first := g.resetTime()
	g.noteFreshCap("proxy4", time.Hour, true) // báo cáo RA ngắn hơn, trong lúc vẫn đang chặn -> gia hạn, không lùi
	if g.resetTime().Before(first) {
		t.Fatal("mốc quota hồi không được lùi về sớm hơn")
	}
}

// Lần cap MỚI (cửa sổ chặn cũ đã hết hạn) phải thay mốc quota hồi cũ.
func TestAccountGateNewEpisodeReplacesStaleResetTime(t *testing.T) {
	g := newGate()
	g.noteFreshCap("proxy1", 24*time.Hour, true)
	g.noteFreshCap("proxy2", 24*time.Hour, true)
	g.noteFreshCap("proxy3", 24*time.Hour, true)
	g.noteFreshCap("res1", 24*time.Hour, true)
	if !g.capped() {
		t.Fatal("phải trip")
	}
	// giả lập cửa sổ chặn cũ đã hết hạn
	g.until = time.Now().Add(-time.Second)
	// lần cap mới, upstream nói còn 2 giờ
	g.noteFreshCap("proxy1", 2*time.Hour, true)
	if d := time.Until(g.resetTime()); d > 3*time.Hour {
		t.Fatalf("lần cap mới phải thay mốc quota hồi cũ, còn %v", d)
	}
}

// Mốc "quota hồi" (resetTime) phải xa hơn cửa sổ chặn: nó dùng cho Retry-After của client, còn cửa sổ chặn thì ngắn
// (được gia hạn bằng bằng chứng quota mới) để không kẹt cứng khi prober hỏng vì lý do mạng.
func TestAccountGateResetTimeBeyondBlockWindow(t *testing.T) {
	g := newGate()
	g.noteFreshCap("proxy1", 2*time.Hour, true)
	g.noteFreshCap("proxy2", 2*time.Hour, true)
	g.noteFreshCap("proxy3", 2*time.Hour, true)
	g.noteFreshCap("res1", 2*time.Hour, true)
	if !g.capped() {
		t.Fatal("phải capped")
	}
	if d := time.Until(g.untilAt()); d > acctGateMax+time.Second {
		t.Fatalf("cửa sổ chặn phải <= acctGateMax, got %v", d)
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

// IP mới (vừa đổi) là bằng chứng "cạn cấp tài khoản"; IP cũ thì không.
func TestEgressIPFreshness(t *testing.T) {
	e := newEgress("p", kindProxy, nil)
	if e.ipIsFresh() {
		t.Fatal("egress chưa đổi IP thì không được coi là IP mới")
	}
	e.markIPChanged()
	if !e.ipIsFresh() {
		t.Fatal("sau khi đổi IP thì phải coi là IP mới")
	}
}

// Session của IP CŨ phải khác thế hệ với pool hiện tại (sau khi đổi IP) -> dùng để biết 200/429 thuộc IP nào.
func TestSessionGenerationDistinguishesOldIP(t *testing.T) {
	var p sessionPool
	es := &egSession{id: "s1"}
	p.list = []*egSession{es}
	if es.gen != p.genNow() {
		t.Fatal("session mới phải cùng thế hệ với pool")
	}
	p.reset() // đổi IP -> thế hệ tăng, session cũ không còn hiệu lực
	if es.gen == p.genNow() {
		t.Fatal("session của IP cũ phải khác thế hệ sau khi đổi IP")
	}
}

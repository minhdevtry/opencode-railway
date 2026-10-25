package main

import (
	"log"
	"sync"
	"time"
)

// ======================== CIRCUIT BREAKER CẤP TÀI KHOẢN ========================
//
// Đo 2026-10-24 và 25: về cuối ngày UTC, hạn mức free cạn ở mức CAO HƠN từng IP — một IP WARP vừa đổi mới tinh, IP nhà,
// và proxy dân cư đều đồng loạt trả FreeUsageLimitError với retry-after đếm tới nửa đêm UTC. Lúc đó thêm/đổi IP vô ích.
//
// Trước khi có breaker: gateway cứ đổi danh tính WARP (đo được 349-422 lần/3h) và fan-out mỗi request qua mọi egress —
// tốn tài nguyên, kéo độ trễ lên. Breaker phát hiện trạng thái này (nhiều egress khác nhau cùng báo "session mới cũng
// bị cạn, retry-after dài") và: (1) ngừng đổi IP WARP, (2) để request fast-fail 429 thay vì xếp hàng tới MAX_WAIT,
// (3) một luồng nền thăm dò lại định kỳ để tự mở lại khi quota hồi (retry-after của upstream hay báo dài hơn thực tế).

type accountGate struct {
	mu     sync.Mutex
	until  time.Time            // đang chặn tới lúc này
	recent map[string]time.Time // egress -> lần gần nhất báo "session mới cũng cạn"
}

var acctGate = &accountGate{recent: map[string]time.Time{}}

const (
	acctGateDistinct = 3                // cần bao nhiêu egress khác nhau cùng báo thì mới trip
	acctGateWindow   = 2 * time.Minute  // trong khoảng thời gian này
	acctGateMax      = 10 * time.Minute // chặn tối đa bấy nhiêu rồi thăm dò lại (retry-after của upstream không đáng tin)
)

// noteFreshCap: 1 egress báo session CHƯA từng thành công cũng bị FreeUsageLimit với retry-after dài (daily cap, không
// phải cửa sổ tốc độ ngắn). Gom theo egress; đủ số egress khác nhau trong cửa sổ thì trip breaker.
func (g *accountGate) noteFreshCap(egress string, retryAfter time.Duration) {
	if retryAfter < 30*time.Minute {
		return // cửa sổ tốc độ ngắn của 1 session, không phải cạn cả ngày
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.recent[egress] = now
	n := 0
	for name, t := range g.recent {
		if now.Sub(t) <= acctGateWindow {
			n++
		} else {
			delete(g.recent, name)
		}
	}
	if n >= acctGateDistinct && now.After(g.until) {
		d := retryAfter
		if d > acctGateMax {
			d = acctGateMax
		}
		g.until = now.Add(d)
		log.Printf("[AccountGate] %d egress báo quota cạn cấp tài khoản (retry-after ~%v) -> tạm ngừng đổi IP & fan-out trong %v", n, retryAfter.Round(time.Minute), d)
	}
}

// clear: có 1 request thành công -> quota còn, mở lại ngay.
func (g *accountGate) clear() {
	g.mu.Lock()
	if !g.until.IsZero() {
		g.until = time.Time{}
		g.recent = map[string]time.Time{}
		log.Printf("[AccountGate] có request thành công, mở lại bình thường")
	}
	g.mu.Unlock()
}

func (g *accountGate) tripped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.until)
}

func (g *accountGate) until_() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.until
}

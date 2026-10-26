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
// tốn tài nguyên, kéo độ trễ lên. Breaker phát hiện trạng thái này và: (1) ngừng đổi IP WARP, (2) để request fast-fail 429
// thay vì xếp hàng tới MAX_WAIT, (3) một luồng nền thăm dò lại định kỳ để tự mở lại khi quota hồi.
//
// Hai mốc thời gian:
//   - until:   đang CHẶN (fast-fail 429 + dừng xoay IP). Tối đa acctGateMax; sau đó prober thăm dò lại.
//   - resetAt: lúc upstream nói quota hồi (retry-after thật). Dùng làm Retry-After trả về client.
//
// BẰNG CHỨNG để trip: nhiều egress khác nhau cùng bị cạn với retry-after dài, VÀ trong đó có ít nhất 1 session CHƯA từng
// chạy được (tức IP vừa đổi mới tinh cũng cạn) — đó mới là bằng chứng "cạn ở mức cao hơn từng IP". Nếu chỉ cần nhiều
// egress cùng cạn thì có thể chỉ là cạn từng IP (đổi IP vẫn có ích), nên không trip.
//
// QUAN TRỌNG: breaker KHÔNG được mở lại chỉ vì có 1 request thành công. Lúc quota cạn một phần, các session cũ vẫn
// chạy được và trả 200 -> mở gate lúc đó là quay lại đúng cái bão xoay IP mà breaker sinh ra để chặn. Chỉ PROBER
// (thăm dò có chủ đích) mới được mở lại, vì nó là bằng chứng quota đã hồi thật.

type accountGate struct {
	mu      sync.Mutex
	until   time.Time            // đang chặn tới lúc này
	resetAt time.Time            // upstream nói quota hồi lúc này
	fresh   bool                 // đã thấy IP MỚI (session chưa từng chạy) cũng bị cạn dài => cạn cấp tài khoản
	recent  map[string]time.Time // egress -> lần gần nhất bị cạn dài
}

var acctGate = &accountGate{recent: map[string]time.Time{}}

const (
	acctGateDistinct = 3                // cần bao nhiêu egress khác nhau cùng báo thì mới trip
	acctGateWindow   = 2 * time.Minute  // trong khoảng thời gian này
	acctGateMinCap   = 30 * time.Minute // retry-after từ ngần này trở lên mới tính là "cạn cả ngày" (cửa sổ tốc độ ngắn thì không)
	acctGateMax      = 10 * time.Minute // chặn tối đa bấy nhiêu rồi thăm dò lại (retry-after của upstream hay báo dài hơn thực tế)
	acctGateResetMax = 24 * time.Hour   // trần cho mốc "quota hồi" (retry-after upstream đếm tới nửa đêm UTC)
)

// noteFreshCap: 1 egress bị FreeUsageLimit với retry-after dài.
//
//	egress     - tên egress (phải là duy nhất theo đường mạng, không phải theo model)
//	retryAfter - Retry-After của upstream
//	fresh      - session của egress CHƯA từng chạy được (IP mới tinh) => bằng chứng cạn cấp tài khoản
func (g *accountGate) noteFreshCap(egress string, retryAfter time.Duration, fresh bool) {
	if retryAfter < acctGateMinCap {
		return // cửa sổ tốc độ ngắn của 1 session, không phải cạn cả ngày
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.recent[egress] = now
	if fresh {
		g.fresh = true
	}
	n := 0
	for name, t := range g.recent {
		if now.Sub(t) <= acctGateWindow {
			n++
		} else {
			delete(g.recent, name)
		}
	}
	blocked := now.Before(g.until)
	proven := g.fresh && n >= acctGateDistinct
	if !proven && !blocked {
		return // chưa đủ bằng chứng: đừng chặn (cạn từng IP thì đổi IP vẫn có ích)
	}
	block := retryAfter
	if block > acctGateMax {
		block = acctGateMax
	}
	g.until = now.Add(block)
	reset := retryAfter
	if reset > acctGateResetMax {
		reset = acctGateResetMax
	}
	if reset > 0 {
		g.resetAt = now.Add(reset)
	}
	if proven && !blocked {
		log.Printf("[AccountGate] %d egress báo quota cạn cấp tài khoản (IP mới cũng cạn, retry-after ~%v) -> dừng xoay IP & fast-fail trong %v (quota hồi ~%v)",
			n, retryAfter.Round(time.Minute), block.Round(time.Minute), g.resetAt.UTC().Format("15:04 UTC"))
	}
}

// clear: PROBER thăm dò thành công -> quota đã hồi, mở lại bình thường.
func (g *accountGate) clear() {
	g.mu.Lock()
	if !g.until.IsZero() || g.fresh {
		g.until = time.Time{}
		g.resetAt = time.Time{}
		g.fresh = false
		g.recent = map[string]time.Time{}
		log.Printf("[AccountGate] thăm dò thành công, mở lại bình thường")
	}
	g.mu.Unlock()
}

// tripped: đang trong cửa sổ chặn (fast-fail 429 + dừng xoay IP).
func (g *accountGate) tripped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.until)
}

// capped: quota tài khoản vẫn bị coi là cạn -> KHÔNG đổi IP (đổi cũng vô ích) cho tới khi prober thấy quota hồi.
// Lâu hơn cửa sổ chặn (till) vì upstream nói quota hồi lúc nửa đêm UTC.
func (g *accountGate) capped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.resetAt)
}

func (g *accountGate) untilAt() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.until
}

func (g *accountGate) resetTime() time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.resetAt
}

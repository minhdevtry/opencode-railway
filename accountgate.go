package main

import (
	"log"
	"strings"
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
// thay vì xếp hàng tới MAX_WAIT, (3) tự mở lại khi quota hồi.
//
// Hai mốc thời gian:
//   - until:   đang CHẶN (fast-fail 429 + dừng xoay IP). Tối đa acctGateMax; được gia hạn mỗi khi có bằng chứng mới.
//   - resetAt: lúc upstream nói quota hồi (retry-after thật). Dùng làm Retry-After trả về client.
//
// BẰNG CHỨNG để trip — cần đủ 3 điều kiện, nếu không sẽ chặn nhầm:
//   1. ≥3 egress KHÁC NHAU bị FreeUsageLimit với retry-after dài, trong cửa sổ acctGateWindow.
//   2. Trong số đó có ≥1 egress vừa đổi IP (IP mới tinh) — đó mới là bằng chứng "cạn ở mức CAO HƠN từng IP".
//      Nếu chỉ cần nhiều egress cùng cạn thì có thể chỉ là cạn từng IP, lúc đó đổi IP VẪN CÓ ÍCH.
//   3. Bằng chứng phải đến từ ≥2 họ đường mạng khác nhau (WARP / dân cư / direct): 3 container WARP có thể chung
//      một IPv4 dùng chung, đếm 3 egress đó không khác gì 1 IP.
//
// MỖI BẰNG CHỨNG có mốc thời gian riêng và tự hết hạn sau acctGateWindow: bằng chứng cũ không được "cho" bằng chứng
// mới của một cụm khác (nếu không thì 1 lần cạn cách đây cả ngày vẫn trip được breaker).
//
// QUAN TRỌNG: breaker KHÔNG được mở lại chỉ vì có 1 request thành công. Lúc quota cạn một phần, các session cũ vẫn
// chạy được và trả 200 -> mở gate lúc đó là quay lại đúng cái bão xoay IP mà breaker sinh ra để chặn. Chỉ một request
// thành công trên IP MỚI (vừa đổi) mới chứng minh "cặp (IP, session) mới vẫn được chấp nhận" => quota đã hồi.

type capEvidence struct {
	at    time.Time // lúc egress này bị cạn dài
	fresh bool      // IP của egress này vừa được đổi (mới tinh) tại thời điểm đó
}

type accountGate struct {
	mu      sync.Mutex
	until   time.Time              // đang chặn tới lúc này
	resetAt time.Time              // upstream nói quota hồi lúc này
	recent  map[string]capEvidence // egress -> bằng chứng cạn dài gần nhất
}

var acctGate = &accountGate{recent: map[string]capEvidence{}}

const (
	acctGateDistinct = 3                // cần bao nhiêu egress khác nhau cùng báo thì mới trip
	acctGateFamilies = 2                // bằng chứng phải đến từ tối thiểu 2 họ đường mạng (WARP / dân cư / direct)
	acctGateWindow   = 2 * time.Minute  // trong khoảng thời gian này
	acctGateMinCap   = 30 * time.Minute // retry-after từ ngần này trở lên mới tính là "cạn cả ngày" (cửa sổ tốc độ ngắn thì không)
	acctGateMax      = 10 * time.Minute // chặn tối đa bấy nhiêu rồi thăm dò lại (retry-after của upstream hay báo dài hơn thực tế)
	acctGateResetMax = 24 * time.Hour   // trần cho mốc "quota hồi" (retry-after upstream đếm tới nửa đêm UTC)
)

// noteFreshCap: 1 egress bị FreeUsageLimit với retry-after dài.
//
//	egress     - tên egress (phải là duy nhất theo đường mạng, không phải theo model)
//	retryAfter - Retry-After của upstream
//	freshIP    - IP của egress này vừa được đổi (mới tinh) => bằng chứng cạn cấp tài khoản
func (g *accountGate) noteFreshCap(egress string, retryAfter time.Duration, freshIP bool) {
	if retryAfter < acctGateMinCap {
		return // cửa sổ tốc độ ngắn của 1 session, không phải cạn cả ngày
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.recent[egress] = capEvidence{at: now, fresh: freshIP}
	n, freshSeen := 0, false
	fams := map[string]bool{}
	for name, ev := range g.recent {
		if now.Sub(ev.at) > acctGateWindow {
			delete(g.recent, name)
			continue
		}
		n++
		if ev.fresh {
			freshSeen = true
		}
		if gr := egressGroupOf(name); gr != "" {
			fams[gr] = true
		}
	}
	blocked := now.Before(g.until)
	// Cần đủ 3 egress + có bằng chứng IP mới. Thêm: bằng chứng phải đa dạng (≥2 họ đường mạng) - trừ khi có
	// đủ 4 egress, vì 3 container WARP có thể dùng chung 1 IPv4 (đếm 3 egress đó không khác gì 1 IP).
	proven := freshSeen && n >= acctGateDistinct && (len(fams) >= acctGateFamilies || n >= acctGateDistinct+1)
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
	// CHỈ giới hạn dài, không lùi: mốc quota hồi phải là mốc LỚN nhất từng thấy.
	if reset > 0 && now.Add(reset).After(g.resetAt) {
		g.resetAt = now.Add(reset)
	}
	if proven && !blocked {
		log.Printf("[AccountGate] %d egress (%d họ đường mạng) báo quota cạn cấp tài khoản (IP mới cũng cạn, retry-after ~%v) -> dừng xoay IP & fast-fail trong %v (quota hồi ~%v)",
			n, len(fams), retryAfter.Round(time.Minute), block.Round(time.Minute), g.resetAt.UTC().Format("15:04 UTC"))
	}
}

// clear: một request thành công trên IP MỚI => cặp (IP, session) mới vẫn được chấp nhận => quota đã hồi, mở lại.
func (g *accountGate) clear() {
	g.mu.Lock()
	if !g.until.IsZero() {
		g.until = time.Time{}
		g.resetAt = time.Time{}
		g.recent = map[string]capEvidence{}
		log.Printf("[AccountGate] IP mới đã trả 200 => quota hồi, mở lại bình thường")
	}
	g.mu.Unlock()
}

// tripped: đang trong cửa sổ chặn (fast-fail 429 + dừng xoay IP).
func (g *accountGate) tripped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.until)
}

// capped: quota tài khoản vẫn bị coi là cạn -> KHÔNG đổi IP (đổi cũng vô ích).
// Cửa sổ này được GIA HẠN mỗi khi có bằng chứng quota mới (prober thăm dò hoặc request thật), nên nó kéo dài suốt
// thời gian quota cạn. Cố tình KHÔNG dùng resetAt (mốc quota hồi, có thể tới 24 giờ) làm mốc: nếu prober hỏng vì
// lý do mạng chứ không phải vì quota, thì giữ "cạn" suốt 24 giờ là kẹt cứng - không egress nào được đổi IP nữa.
func (g *accountGate) capped() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return time.Now().Before(g.until)
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

// egressGroupOf: họ đường mạng của 1 tên egress (warp / res / direct / tor). Dùng để đòi bằng chứng đa dạng:
// 3 container WARP có thể dùng chung 1 IPv4, đếm 3 egress đó không khác gì 1 IP.
func egressGroupOf(egress string) string {
	switch {
	case egress == "direct":
		return "direct"
	case strings.HasPrefix(egress, "res"):
		return "res"
	case strings.HasPrefix(egress, "proxy"):
		return "warp"
	case strings.HasPrefix(egress, "tor"):
		return "tor"
	}
	return ""
}

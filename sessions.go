package main

import (
	"sync"
	"time"
)

// ======================== SESSION THEO EGRESS ========================
//
// Đo 2026-10-24 (research/README.md): quota free của upstream KHÔNG đếm theo request mà theo số cặp (IP, session)
// khác nhau mỗi ngày UTC: 1 IP chỉ mở được ~50 session mới/ngày (mỗi request 1 session mới => 429 sau ~42 request),
// còn 1 session dùng lại thì chạy tiếp được rất nhiều request. Session đem sang IP khác cũng tính là cặp mới.
// Vì vậy mỗi egress giữ vài session sống lâu, dùng lại cho mọi request đi qua nó; chỉ mở session mới khi session cũ
// bị upstream từ chối, và không bao giờ dùng session của egress này cho egress khác.

type egSession struct {
	id        string
	uses      int64
	ok        bool // đã từng thành công (để phân biệt "session hết hạn mức" với "IP hết lượt mở session mới")
	dead      bool
	coolUntil time.Time // session chạm cửa sổ tốc độ (~80 request) thì nghỉ theo Retry-After rồi dùng tiếp
	inflight  int
	gen       uint64 // thế hệ pool lúc mở; khác thế hệ hiện tại (IP đã đổi) thì kết quả của nó không còn ý nghĩa
}

type sessionPool struct {
	mu        sync.Mutex
	day       string
	list      []*egSession
	rr        int
	newDenied bool      // IP này hiện không mở được session mới
	deniedAt  time.Time // lúc bị từ chối mở session mới (sau SessionReprobe thì thử lại 1 lần)
	opened    int       // số session đã mở hôm nay
	gen       uint64
}

func utcDay() string { return time.Now().UTC().Format("2006-01-02") }

func (p *sessionPool) rollover() {
	if d := utcDay(); d != p.day {
		p.day, p.list, p.rr, p.newDenied, p.opened = d, nil, 0, false, 0
		p.gen++
	}
	// Retry-After của upstream luôn chỉ tới nửa đêm UTC nhưng đo thấy có lúc hồi sớm (cửa sổ trượt): định kỳ cho thử lại.
	if p.newDenied && cfg.SessionReprobe > 0 && time.Since(p.deniedAt) > cfg.SessionReprobe {
		p.newDenied = false
	}
}

// acquire trả session cho 1 request (nil nếu egress hết đường). Chọn session sống, không đang nghỉ, ít request
// đang chạy nhất; mở session mới (tốn 1 lượt trong ngân sách ~50 lượt/IP/ngày) chỉ khi chưa có session nào rảnh
// (mọi session đang nghỉ hoặc đã có >= perSession request đang chạy) và chưa chạm `want`.
func (p *sessionPool) acquire(want, perSession int) *egSession {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	now := time.Now()
	var best *egSession
	alive := 0
	for _, s := range p.list {
		if s.dead {
			continue
		}
		alive++
		if now.Before(s.coolUntil) {
			continue
		}
		if best == nil || s.inflight < best.inflight {
			best = s
		}
	}
	if (best == nil || best.inflight >= perSession) && alive < want && !p.newDenied {
		s := &egSession{id: newSessionID(), inflight: 1, gen: p.gen}
		p.list = append(p.list, s)
		p.opened++
		return s
	}
	if best == nil {
		return nil
	}
	best.inflight++
	return best
}

// done trả "chỗ" của request trên session.
func (p *sessionPool) done(s *egSession) {
	if s == nil {
		return
	}
	p.mu.Lock()
	if s.inflight > 0 {
		s.inflight--
	}
	p.mu.Unlock()
}

// cool: session chạm cửa sổ tốc độ, nghỉ d rồi dùng tiếp.
func (p *sessionPool) cool(s *egSession, d time.Duration) {
	if s == nil {
		return
	}
	p.mu.Lock()
	s.coolUntil = time.Now().Add(d)
	p.mu.Unlock()
}

// nextReady: thời điểm sớm nhất có session hết nghỉ (0 nếu đang có session dùng được ngay).
func (p *sessionPool) nextReady() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	now := time.Now()
	var soon time.Duration = -1
	for _, s := range p.list {
		if s.dead {
			continue
		}
		d := s.coolUntil.Sub(now)
		if d <= 0 {
			return 0
		}
		if soon < 0 || d < soon {
			soon = d
		}
	}
	if !p.newDenied {
		return 0
	}
	return soon
}

// usable: acquire() lúc này có trả được session không (cùng điều kiện với acquire).
func (p *sessionPool) usable() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	now := time.Now()
	alive := 0
	for _, s := range p.list {
		if s.dead {
			continue
		}
		alive++
		if !now.Before(s.coolUntil) {
			return true
		}
	}
	return !p.newDenied && alive < max(cfg.SessionsPerEgress, 1)
}

// readyWithin: trong khoảng d tới có thể dùng lại không (hết nghỉ, hoặc tới lúc thăm dò lại lượt mở session).
func (p *sessionPool) readyWithin(d time.Duration) bool {
	if p.usable() {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for _, s := range p.list {
		if !s.dead && s.coolUntil.Sub(now) <= d {
			return true
		}
	}
	return p.newDenied && cfg.SessionReprobe > 0 && time.Until(p.deniedAt.Add(cfg.SessionReprobe)) <= d
}

// exhausted: hết hẳn hôm nay (không mở được session mới và không còn session sống nào, kể cả đang nghỉ).
func (p *sessionPool) exhausted() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	if !p.newDenied {
		return false
	}
	for _, s := range p.list {
		if !s.dead {
			return false
		}
	}
	return true
}

func (p *sessionPool) success(s *egSession) {
	if s == nil {
		return
	}
	p.mu.Lock()
	s.ok = true
	s.uses++
	p.mu.Unlock()
}

// freshSession: session này CHƯA từng chạy thành công (IP mới tinh). Đọc dưới khoá vì success() ghi s.ok từ goroutine khác.
func (p *sessionPool) freshSession(s *egSession) bool {
	if s == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return !s.ok
}

// blocked: upstream trả FreeUsageLimitError cho session s. Session đã từng chạy => nghỉ (Retry-After ngắn thì theo nó,
// dài thì thăm dò lại sau SessionReprobe, vì đo thấy có lúc hồi sớm); chưa chạy lần nào => IP hết lượt mở session.
// Session của thế hệ cũ (IP đã đổi) thì bỏ qua. Trả về true nếu là session hiện hành.
func (p *sessionPool) blocked(s *egSession, ra time.Duration) bool {
	if s == nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.gen != p.gen {
		return false
	}
	if s.ok {
		d := ra
		if d <= 0 || d > cfg.SessionCoolMax {
			d = cfg.SessionReprobe
		}
		s.coolUntil = time.Now().Add(d)
		return true
	}
	p.newDenied = true
	p.deniedAt = time.Now()
	s.dead = true
	return true
}

// denied: upstream trả FreeUsageLimitError cho session s. Session đã từng chạy => session cạn hạn mức (bỏ, mở cái khác);
// session chưa chạy lần nào => IP hết lượt mở session mới hôm nay.
func (p *sessionPool) denied(s *egSession) {
	if s == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s.gen != p.gen {
		return
	}
	if !s.ok {
		p.newDenied = true
		p.deniedAt = time.Now()
	}
	s.dead = true
}

// reset khi egress đổi sang IP mới (WARP đổi danh tính, proxy xoay đổi IP): hạn mức mới.
func (p *sessionPool) reset() {
	p.mu.Lock()
	p.day, p.list, p.rr, p.newDenied, p.opened = utcDay(), nil, 0, false, 0
	p.gen++
	p.mu.Unlock()
}

func (p *sessionPool) stats() (alive, opened int, newDenied bool, uses int64) { // alive gồm cả session đang nghỉ
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	for _, s := range p.list {
		if !s.dead {
			alive++
		}
		uses += s.uses
	}
	return alive, p.opened, p.newDenied, uses
}

package main

import (
	"context"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// ======================== BỘ ĐIỀU PHỐI (HÀNG ĐỢI FIFO + GIỮ CHỖ) ========================
//
// Mọi request cần 1 slot egress đi qua acquire():
//   - Còn slot và không ai xếp hàng trước => chọn egress và giữ chỗ (inflight++) ngay trong cùng khoá, không có
//     khoảng hở để 2 request giành 1 slot.
//   - Hết slot => xếp hàng FIFO (2 mức: request đồng bộ trước, job nền sau). Goroutine dispatcher phân slot cho
//     người đứng đầu mỗi khi có slot trả về (release), cooldown hết hạn hoặc egress đổi IP xong; không polling.
// Trần in-flight của từng egress tự điều chỉnh (AIMD): chạy ổn thì nới dần tới LIMIT_MAX, gặp áp lực (lỗi mạng,
// 5xx, rate limit ngắn) thì giảm 25%.

type pickRes struct {
	model string
	e     *Egress
}

type waiter struct {
	model  string
	isPool bool
	avoid  []string
	excl   map[string]bool
	fb     bool // model cụ thể được phép đổi sang model khác khi hết đường
	res    chan pickRes
	gone   bool
	key    string // lớp (tính 1 lần): cùng lớp mà người trước không có slot thì người sau cũng không
}

// class: avoid không tính vào lớp vì pickAvoid tự bỏ qua avoid khi không còn egress nào khác.
func (w *waiter) class() string {
	if w.key == "" {
		k := w.model
		if w.isPool {
			k = "*"
		} else if w.fb {
			k += "|fb"
		}
		if len(w.excl) > 0 {
			k += "|x" + strings.Join(sortedKeys(w.excl), ",")
		}
		w.key = k
	}
	return w.key
}

var queuedHigh, queuedLow atomic.Int32

// kickSched báo dispatcher có thay đổi về sức chứa.
func (m *Manager) kickSched() {
	select {
	case m.kick <- struct{}{}:
	default:
	}
}

// release trả slot của egress (gọi đúng 1 lần cho mỗi lần giữ chỗ).
func (e *Egress) release() {
	e.inflight.Add(-1)
	if mgr != nil {
		mgr.kickSched()
	}
}

// adapt điều chỉnh trần in-flight của egress theo kết quả 1 request.
func (e *Egress) adapt(ok bool) {
	lim := e.limit.Load()
	if ok {
		if run := e.okRun.Add(1); run >= 2*lim && lim < int32(max(cfg.LimitMax, 1)) {
			if e.limit.CompareAndSwap(lim, lim+1) {
				e.okRun.Store(0)
			}
		}
		return
	}
	e.okRun.Store(0)
	if cfg.LimitMax <= 0 {
		return // chưa cấu hình AIMD
	}
	if n := lim * 3 / 4; n >= int32(max(cfg.LimitMin, 1)) {
		e.limit.CompareAndSwap(lim, n)
	} else {
		e.limit.Store(int32(max(cfg.LimitMin, 1)))
	}
}

func initLimit(e *Egress) {
	l := cfg.LimitWarp
	switch {
	case e.Kind == kindDirect:
		l = cfg.LimitDirect
	case e.Kind == kindTor:
		l = cfg.TorMaxInflight
	case e.tier == 1:
		l = cfg.LimitRes
	}
	if l <= 0 { // cấu hình thiếu (vd test): không giới hạn thực tế
		l = 1 << 20
	}
	e.limit.Store(int32(l))
}

// pickAny chọn (model, egress) cho 1 yêu cầu và giữ chỗ. Gọi khi đang giữ m.wmu.
func (m *Manager) pickAny(model string, isPool bool, avoid []string, excl map[string]bool, fallback bool) (string, *Egress) {
	try := func(cand string) *Egress {
		if e := m.pickAvoid(cand, avoid...); e != nil {
			e.inflight.Add(1)
			return e
		}
		return nil
	}
	pool := usableModels(poolSnapshot())
	if isPool {
		n := len(pool)
		if n == 0 {
			return "", nil
		}
		start := int(atomic.AddUint32(&poolIndex, 1))
		allPoor := true
		for _, c := range pool {
			if !excl[c] && !modelPoor(c) {
				allPoor = false
				break
			}
		}
		for i := range n {
			cand := pool[(start+i)%n]
			if excl[cand] || (!allPoor && modelPoor(cand)) {
				continue
			}
			if e := try(cand); e != nil {
				return cand, e
			}
		}
		return "", nil
	}
	if e := try(model); e != nil {
		return model, e
	}
	if fallback && isFreeModelInPool(model) && len(pool) > 0 {
		n := len(pool)
		start := int(atomic.AddUint32(&poolIndex, 1))
		for i := range n {
			if cand := pool[(start+i)%n]; cand != model {
				if e := try(cand); e != nil {
					return cand, e
				}
			}
		}
	}
	return "", nil
}

// fastPick: không ai xếp hàng trước thì lấy slot ngay (khoá được trả kể cả khi pick panic).
func (m *Manager) fastPick(model string, isPool bool, avoid []string, excl map[string]bool, fb, low bool) (string, *Egress, bool) {
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if len(m.waitHigh) == 0 && (!low || len(m.waitLow) == 0) {
		if mod, e := m.pickAny(model, isPool, avoid, excl, fb); e != nil {
			return mod, e, true
		}
	}
	return "", nil, false
}

// acquire: lấy slot egress (đã giữ chỗ) hoặc lỗi khi hết hạn chờ / client huỷ / không có đường nào có thể phục vụ.
func (m *Manager) acquire(ctx context.Context, model string, isPool bool, deadline time.Time, avoid ...string) (string, *Egress, *gwError) {
	low := isLowPriority(ctx)
	excl := excludedModels(ctx)
	fb := modelFallback(ctx)
	if mod, e, ok := m.fastPick(model, isPool, avoid, excl, fb, low); ok {
		return mod, e, nil
	}
	m.wmu.Lock()
	models := []string{model}
	if isPool || (fb && isFreeModelInPool(model)) {
		models = poolSnapshot()
	}
	if !m.canWaitForTor() && !m.anyUsableWithin(models, time.Until(deadline)) && (!m.anyRenewing() || acctGate.capped()) {
		m.wmu.Unlock()
		return "", nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
			msg: "All egress paths are rate limited for this model. Retry later.", retryAfter: m.soonest(models)}
	}
	w := &waiter{model: model, isPool: isPool, avoid: append([]string(nil), avoid...), excl: excl, fb: fb, res: make(chan pickRes, 1)}
	if low {
		m.waitLow = append(m.waitLow, w)
		queuedLow.Add(1)
	} else {
		m.waitHigh = append(m.waitHigh, w)
		queuedHigh.Add(1)
	}
	m.wmu.Unlock()
	m.kickSched()

	leave := func() *gwError {
		m.wmu.Lock()
		w.gone = true
		m.wmu.Unlock()
		select { // dispatcher có thể vừa giao slot ngay lúc rời hàng: trả lại
		case r := <-w.res:
			r.e.release()
		default:
		}
		return nil
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case r := <-w.res:
			return r.model, r.e, nil
		case <-ctx.Done():
			leave()
			return "", nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
		case <-timer.C:
			leave()
			return "", nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
				msg: "All egress paths are busy or rate limited. Max wait exceeded.", retryAfter: m.soonest(models)}
		case <-tick.C:
			m.growTor()
			// Không đường nào có thể phục vụ trong thời gian còn lại (vd chỉ có direct đã cạn): báo sớm thay vì giữ.
			if !m.canWaitForTor() && !m.anyUsableWithin(models, time.Until(deadline)) && (!m.anyRenewing() || acctGate.capped()) {
				leave()
				return "", nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
					msg: "All egress paths are rate limited for this model. Retry later.", retryAfter: m.soonest(models)}
			}
		}
	}
}

func (m *Manager) anyRenewing() bool {
	for _, e := range m.snapshot() {
		if e.renewing.Load() {
			return true
		}
	}
	return false
}

// dispatcher phân slot cho hàng chờ theo thứ tự. Yêu cầu cùng "lớp" (cùng model/pool + avoid) mà người trước đã
// không có slot thì người sau cũng không: bỏ qua cả lớp để mỗi vòng chỉ tốn vài lần pick dù hàng chờ hàng nghìn.
func (m *Manager) dispatcher(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.kick:
		case <-t.C:
		}
		m.servePass()
	}
}

// servePass: hàng đồng bộ được chọn trước, slot còn dư mới tới hàng job (job không bị bỏ đói khi có request đồng
// bộ đang chờ model/egress khác). Lỗi bất ngờ khi pick không được làm chết dispatcher hay kẹt khoá.
func (m *Manager) servePass() {
	m.wmu.Lock()
	defer m.wmu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Sched] lỗi khi phân slot: %v", r)
		}
	}()
	m.waitHigh = m.serve(m.waitHigh, &queuedHigh)
	m.waitLow = m.serve(m.waitLow, &queuedLow)
}

func (m *Manager) serve(q []*waiter, counter *atomic.Int32) []*waiter {
	failed := map[string]bool{}
	out := q[:0]
	for i, w := range q {
		if w.gone {
			counter.Add(-1)
			continue
		}
		if i > 20000 { // giới hạn công việc mỗi vòng; phần còn lại giữ nguyên thứ tự
			out = append(out, w)
			continue
		}
		k := w.class()
		if failed[k] {
			out = append(out, w)
			continue
		}
		if mod, e := m.pickAny(w.model, w.isPool, w.avoid, w.excl, w.fb); e != nil {
			w.res <- pickRes{model: mod, e: e}
			counter.Add(-1)
			continue
		}
		failed[k] = true
		out = append(out, w)
	}
	for i := len(out); i < len(q); i++ {
		q[i] = nil
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---- chế độ cố định model ----
// Gọi đích danh 1 model thì mặc định KHÔNG đổi sang model khác (nhất quán kết quả, đúng tham số thinking/output
// của model đó). Cho phép đổi: header X-Model-Fallback: allow, hoặc env AUTO_MODEL_FALLBACK=true.

type fallbackKey struct{}

func withFallback(ctx context.Context, r *http.Request) context.Context {
	allow := cfg.AutoModelFallback
	switch strings.ToLower(strings.TrimSpace(r.Header.Get("X-Model-Fallback"))) {
	case "allow", "1", "true", "yes":
		allow = true
	case "deny", "0", "false", "no", "strict":
		allow = false
	}
	return context.WithValue(ctx, fallbackKey{}, allow)
}

func modelFallback(ctx context.Context) bool {
	if v, ok := ctx.Value(fallbackKey{}).(bool); ok {
		return v
	}
	return cfg.AutoModelFallback
}

// X-Max-Wait: client tự chọn chờ tối đa bao lâu trong hàng đợi (vd "30s") trước khi nhận 429, thay vì mặc định MAX_WAIT.
// Dùng khi client thà fail nhanh và tự điều tiết còn hơn chờ lâu lúc hệ thống gần cạn sức.
//
// Hạn phải là MỐC TUYỆT ĐỐI, không phải "chờ thêm bao lâu": handler có thể gọi forward() nhiều lần (retry 429/502,
// guard retry stream bị cụt). Nếu mỗi lần forward() tính lại hạn từ đầu thì client chờ N × X-Max-Wait, tức client
// yêu cầu 15s nhưng thực tế chờ 8 × 15s = 2 phút - đúng cái điều client muốn tránh.
type maxWaitKey struct{}

func withMaxWait(ctx context.Context, r *http.Request) context.Context {
	d := cfg.MaxWait
	if v := strings.TrimSpace(r.Header.Get("X-Max-Wait")); v != "" {
		if pd, err := time.ParseDuration(v); err == nil && pd > 0 {
			// Client chỉ được RÚT NGẮN hạn, không được dài hơn MAX_WAIT của server.
			if pd < d {
				d = pd
			}
		}
	}
	// Luôn gắn mốc tuyệt đối, kể cả khi client không đặt header: nếu không, mỗi lần forward() trong vòng retry lại
	// lấy một hạn MỚI (MAX_WAIT) => 1 HTTP request có thể chờ N x MAX_WAIT.
	return context.WithValue(ctx, maxWaitKey{}, time.Now().Add(d))
}

// deadlineFrom: hạn chờ tuyệt đối của client (nếu client gửi X-Max-Wait).
func deadlineFrom(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(maxWaitKey{}).(time.Time)
	return t, ok
}

// waitBudget: ngân sách chờ còn lại của request (giây). Mặc định là MAX_WAIT; nếu ctx có mốc tuyệt đối (do handler
// gắn: X-Max-Wait của client hoặc MAX_WAIT) thì lấy phần còn lại của mốc đó - nhờ vậy các lần retry trong cùng
// 1 request không cộng dồn thời gian chờ.
func waitBudget(ctx context.Context) time.Duration {
	wait := cfg.MaxWait
	if dl, ok := deadlineFrom(ctx); ok {
		if d := time.Until(dl); d < wait {
			wait = d
		}
	}
	if wait < 0 {
		wait = 0
	}
	return wait
}

// Egress cần tránh ngay từ lần thử đầu (lần gửi lại sau khi stream trên egress đó bị treo/đứt).
type avoidKey struct{}

func withAvoid(ctx context.Context, daemons []string) context.Context {
	return context.WithValue(ctx, avoidKey{}, append([]string(nil), daemons...))
}

func avoidFrom(ctx context.Context) []string {
	v, _ := ctx.Value(avoidKey{}).([]string)
	return v
}

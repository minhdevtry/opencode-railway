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
	vision bool // request mang ảnh -> ưu tiên model nhận ảnh
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
	e.limitCutAt.Store(time.Now().UnixNano())
}

// recoverLimits: nới trần AIMD +1 cho các egress chạy ổn định sau LIMIT_RECOVER kể từ lần giảm
// trần gần nhất. Không có bước này, trần bị giảm vĩnh viễn khi egress chập chờn rồi hồi: đường
// thành công cần 2*lim request mới nhích +1, mà traffic thấp thì không bao giờ đủ (PRIVATE-BRANCH.md).
// Chỉ nới khi egress sẵn sàng, không bị cooldown toàn đường, và không có lỗi truyền tải gần đây.
func (m *Manager) recoverLimits() {
	if cfg.LimitMax <= 0 || cfg.LimitRecover <= 0 {
		return
	}
	now := time.Now()
	for _, e := range m.snapshot() {
		if !e.ready.Load() || e.expired.Load() || e.downUntil.Load() > now.UnixNano() || e.errRun.Load() > 0 {
			continue
		}
		cut := e.limitCutAt.Load()
		if cut == 0 || now.Sub(time.Unix(0, cut)) < cfg.LimitRecover {
			continue
		}
		lim := e.limit.Load()
		if lim < int32(cfg.LimitMax) && e.limit.CompareAndSwap(lim, lim+1) {
			e.limitCutAt.Store(now.UnixNano())
		}
	}
}

// limitRecoverLoop chạy recoverLimits định kỳ cho tới khi ctx kết thúc.
func (m *Manager) limitRecoverLoop(ctx context.Context) {
	if cfg.LimitMax <= 0 || cfg.LimitRecover <= 0 {
		return
	}
	iv := cfg.LimitRecover / 2
	if iv < time.Second {
		iv = time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.recoverLimits()
		}
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
func (m *Manager) pickAny(model string, isPool bool, avoid []string, excl map[string]bool, fallback bool, vision bool) (string, *Egress) {
	try := func(cand string, auto bool) *Egress {
		if e := m.pickAvoid(cand, auto, avoid...); e != nil {
			e.inflight.Add(1)
			return e
		}
		return nil
	}
	pool := poolSnapshot()
	if isPool {
		n := len(pool)
		if n == 0 {
			return "", nil
		}
		// Auto: ưu tiên model còn nhiều sức chứng (sức chứng = tổng ngân sách còn lại trên các IP).
		ordered := m.orderModelsByCapacity(pool, true)
		allPoor := true
		for _, c := range pool {
			if !excl[c] && !modelPoor(c) {
				allPoor = false
				break
			}
		}
		// Auto: ưu tiên model còn nhiều sức chứng. Vòng 1 chỉ model nhận ảnh, vòng 2 mới tới model text-only
		// (model text-only bị upstream trả 400 "not supported for vision").
		for pass := 0; pass < 2; pass++ {
			for _, cand := range ordered {
				if excl[cand] || (!allPoor && modelPoor(cand)) {
					continue
				}
				if pass == 0 && vision && modelRejectsImages(cand) {
					continue
				}
				if e := try(cand, true); e != nil {
					return cand, e
				}
			}
		}
		return "", nil
	}
	if e := try(model, false); e != nil {
		return model, e
	}
	if fallback && isFreeModelInPool(model) && len(pool) > 0 {
		// Failover sang model khác: gọi thẳng của client -> không giới hạn share của auto.
		// Vòng 1 chỉ model nhận ảnh, vòng 2 mới tới model text-only.
		for pass := 0; pass < 2; pass++ {
			for _, cand := range m.orderModelsByCapacity(pool, false) {
				if cand == model {
					continue
				}
				if pass == 0 && vision && modelRejectsImages(cand) {
					continue
				}
				if e := try(cand, false); e != nil {
					return cand, e
				}
			}
		}
	}
	return "", nil
}

// fastPick: không ai xếp hàng trước thì lấy slot ngay (khoá được trả kể cả khi pick panic).
func (m *Manager) fastPick(model string, isPool bool, avoid []string, excl map[string]bool, fb, low, vision bool) (string, *Egress, bool) {
	m.wmu.Lock()
	defer m.wmu.Unlock()
	if len(m.waitHigh) == 0 && (!low || len(m.waitLow) == 0) {
		if mod, e := m.pickAny(model, isPool, avoid, excl, fb, vision); e != nil {
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
	vision := needsVision(ctx)
	if mod, e, ok := m.fastPick(model, isPool, avoid, excl, fb, low, vision); ok {
		return mod, e, nil
	}
	m.wmu.Lock()
	models := []string{model}
	if isPool || (fb && isFreeModelInPool(model)) {
		models = poolSnapshot()
	}
	// Hạn chờ để có 1 IP mới (đổi danh tính WARP) thay cho đường đã cạn quota.
	freshUntil := time.Now().Add(cfg.FreshPathWait)
	// Model đã cạn ở MỌI đường mà lần xoay danh tính gần nhất không cứu được (IP mới vẫn cạn) -> xoay tiếp
	// gần như vô ích. Trả 429 NGAY kèm mốc hồi thay vì giữ request trong hàng đợi tới hết MAX_WAIT
	// (đo 2026-11-12: request treo 60s rồi mới nhận 429 -> client tưởng gateway treo).
	if m.hopelessModels(models) {
		m.wmu.Unlock()
		return "", nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
			msg: "All egress paths are rate limited for this model. Retry at quota reset.", retryAfter: m.soonest(models)}
	}
	if !m.canWaitForTor() && !m.anyUsableWithin(models, time.Until(deadline)) && (!m.anyRenewing() || acctGate.capped()) {
		// Không còn đường nào phục vụ được model này. TRƯỚC khi trả 429: kích hoạt đổi danh tính WARP ngay
		// để lấy IP mới rồi xếp hàng chờ (8-25s). Trả 429 luôn kèm Retry-After tới nửa đêm UTC là sai:
		// client SDK chỉ honor Retry-After tới retry.maxDelayMs (5 phút) nên sẽ ABORT chứ không retry ->
		// request chết cứng (đo 2026-11-10: proxy1 đổi danh tính xong 3s sau là glm phục vụ lại).
		if !m.triggerRenewFor(models) {
			m.wmu.Unlock()
			return "", nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
				msg: "All egress paths are rate limited for this model. Retry later.", retryAfter: m.soonest(models)}
		}
	}
	w := &waiter{model: model, isPool: isPool, avoid: append([]string(nil), avoid...), excl: excl, fb: fb, vision: vision, res: make(chan pickRes, 1)}
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
			// Model cạn ở mọi đường và đã hết cửa xoay danh tính -> trả 429 ngay (không giữ tới hết MAX_WAIT).
			if m.hopelessModels(models) {
				leave()
				return "", nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
					msg: "All egress paths are rate limited for this model. Retry at quota reset.", retryAfter: m.soonest(models)}
			}
			// Không đường nào có thể phục vụ trong thời gian còn lại (vd chỉ có direct đã cạn): báo sớm thay vì giữ.
			if !m.canWaitForTor() && !m.anyUsableWithin(models, time.Until(deadline)) && (!m.anyRenewing() || acctGate.capped()) {
				// Vẫn chưa có đường: xoay tiếp (IP mới có thể còn quota) cho tới khi hết hạn chờ.
				if time.Now().Before(freshUntil) && m.triggerRenewFor(models) {
					continue
				}
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

// renewCooldown: tối thiểu bao lâu giữa 2 lần đổi danh tính của cùng 1 egress (mỗi lần xoay tốn 1 IP mới).
const renewCooldown = 20 * time.Second

// triggerRenewFor: model đã cạn quota trên mọi đường hiện có -> kích hoạt đổi danh tính WARP để lấy
// IP mới (IP mới = ngân sách mới cho model đó). Trả true nếu request nên CHỜ (đã kích hoạt xoay, hoặc
// đang có egress xoay sẵn), false nếu không xoay được thì phải trả 429.
//
// Vì sao phải làm: nhánh 429 cũ trả ngay kèm Retry-After tới nửa đêm UTC (hàng giờ) -> client SDK không
// honor được (retry.maxDelayMs mặc định 5 phút) nên request chết cứng. Một lần đổi danh tính chỉ mất
// 8-25s và thường cứu được model (đo 2026-11-10: sau khi proxy1/proxy2 đổi danh tính thì glm phục vụ lại).
func (m *Manager) triggerRenewFor(models []string) bool {
	if acctGate.capped() {
		return false // quota cạn cấp tài khoản: đổi IP không giúp
	}
	if m.anyRenewing() {
		return true // đã có egress đang xoay -> chờ nó xong
	}
	pool := len(models) > 1
	// Giới hạn tần suất xoay THEO MODEL (tính trên mọi egress): model đã cạn ở mọi đường mà xoay liên
	// tục thì mỗi lần xoay lại làm mất sức chứng của cả pool (đo 2026-11-12: 7 lần/3 phút).
	renewKey := modelRenewKey(models)
	if !m.allowModelRenew(renewKey) {
		return false
	}
	for _, e := range m.snapshot() {
		if e.container == "" || !e.ready.Load() || e.renewing.Load() {
			continue
		}
		if time.Since(time.Unix(0, e.lastIdentityChange.Load())) < renewCooldown {
			continue // giới hạn tần suất xoay
		}
		// Model cụ thể: chỉ xoay egress đã chết với model đó (IP mới có thể còn quota cho model này).
		// Pool: không còn đường nào cho bất kỳ model nào -> xoay bất kỳ egress nào.
		if !pool && !modelExhaustedOn(models[0], e.Name) {
			continue
		}
		log.Printf("[Sched] %s không còn đường nào phục vụ được -> xoay danh tính %s để lấy IP mới",
			strings.Join(models, ","), e.Name)
		m.noteModelRenew(renewKey)
		m.renewIdentity(e)
		return true
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
		if mod, e := m.pickAny(w.model, w.isPool, w.avoid, w.excl, w.fb, w.vision); e != nil {
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

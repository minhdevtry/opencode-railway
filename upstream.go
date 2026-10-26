package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ======================== FINGERPRINT OPENCODE 1.19.0 ========================

const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

var (
	idLock    sync.Mutex
	lastMs    int64
	idCounter int64
)

func makeID(descending bool) string {
	now := time.Now().UnixMilli()
	idLock.Lock()
	if now != lastMs {
		lastMs = now
		idCounter = 0
	}
	idCounter++
	c := idCounter & 0xFFF
	idLock.Unlock()

	val := (uint64(now)*0x1000 + uint64(c)) & 0xFFFFFFFFFFFF
	if descending {
		val = (^val) & 0xFFFFFFFFFFFF
	}

	var randBytes [14]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		var seed [8]byte
		binary.BigEndian.PutUint64(seed[:], uint64(time.Now().UnixNano()))
		for i := range randBytes {
			randBytes[i] = seed[i%8] ^ byte(i*37)
		}
	}
	var suffix [14]byte
	for i, b := range randBytes {
		suffix[i] = alphabet[b%byte(len(alphabet))]
	}
	return fmt.Sprintf("%012x%s", val, string(suffix[:]))
}

func newSessionID() string { return "ses_" + makeID(true) }
func newRequestID() string { return "msg_" + makeID(false) }
func newProjectID() string { return randomHex(40) }

func randomHex(n int) string {
	b := make([]byte, n/2)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// ID dùng khi SESSION_ID_MODE=stable. Mặc định (egress) session lấy từ pool của từng egress (sessions.go) vì quota
// upstream tính theo cặp (IP, session). Cache prompt nằm ở phía server chạy model nên đổi ID không mất cache.
var (
	stableSession = newSessionID()
	stableProject = newProjectID()
)

// sessionClock nhớ thời điểm lần đầu thấy mỗi hội thoại. Gate của upstream giải mã 12 hex đầu của session ID thành
// timestamp (TypeID-descending: ~(ms*0x1000+counter)) và từ chối giá trị không hợp lý, nên ID ổn định phải mang
// timestamp thật chứ không phải hash ngẫu nhiên. Dùng thời điểm bắt đầu hội thoại giống client opencode thật.
var sessionClock = struct {
	sync.Mutex
	first map[string]int64
}{first: map[string]int64{}}

func firstSeenMs(key string) int64 {
	sessionClock.Lock()
	defer sessionClock.Unlock()
	if ts, ok := sessionClock.first[key]; ok {
		return ts
	}
	now := time.Now().UnixMilli()
	if len(sessionClock.first) >= 8192 {
		cutoff := now - 24*3600*1000
		for k, v := range sessionClock.first {
			if v < cutoff {
				delete(sessionClock.first, k)
			}
		}
		if len(sessionClock.first) >= 6000 {
			count := 0
			for k := range sessionClock.first {
				delete(sessionClock.first, k)
				count++
				if count >= 3000 {
					break
				}
			}
		}
	}
	sessionClock.first[key] = now
	return now
}

// sessionIDAt dựng session ID hợp lệ (ses_ + 12 hex TypeID-descending + 14 base62) từ timestamp và hash của khoá.
// Đo trực tiếp với gate (đã thử): timestamp cũ tới 1 ngày vẫn qua; hex ngẫu nhiên không timestamp hoặc counter lớn thì 403.
func sessionIDAt(tsMs int64, h [32]byte) string {
	// Counter phải nhỏ như client thật (đã đo: counter ngẫu nhiên 12-bit bị 403, 1..8 thì qua).
	counter := 1 + uint64(h[0])%8
	val := (^((uint64(tsMs)*0x1000 + counter) & 0xFFFFFFFFFFFF)) & 0xFFFFFFFFFFFF
	var suffix [14]byte
	for i := range suffix {
		suffix[i] = alphabet[int(h[6+i])%len(alphabet)]
	}
	return fmt.Sprintf("ses_%012x%s", val, string(suffix[:]))
}

// idsFromKey suy ra session/project ID ổn định từ khoá hội thoại của client.
func idsFromKey(key string) (ses, proj string) {
	h := sha256.Sum256([]byte("orw-ses|" + key))
	p := sha256.Sum256([]byte("orw-prj|" + key))
	return sessionIDAt(firstSeenMs(key), h), hex.EncodeToString(p[:20])
}

// setUpstreamHeaders đặt bộ header giả danh opencode theo cfg.ClientProfile. Header chỉ là 1 phần của cổng phân loại:
// TLS fingerprint (tlsfp.go) và khối <env> trong system prompt (withEnvBlock) mới quyết định request vào quota
// "client thật" hay quota chung. Mặc định mỗi hội thoại của client có 1 session ID ổn định để giữ prompt cache.
func setUpstreamHeaders(req *http.Request, accept, key string, es ...*egSession) {
	ses, proj := newSessionID(), newProjectID()
	ks, kp := idsFromKey(key)
	switch cfg.SessionIDMode {
	case "stable":
		ses = stableSession
	case "client":
		if key != "" {
			ses = ks
		}
	case "egress":
		if len(es) > 0 && es[0] != nil {
			ses = es[0].id
		}
	}
	switch cfg.ProjectIDMode {
	case "stable":
		proj = stableProject
	case "client":
		if key != "" {
			proj = kp
		}
	}
	req.Header.Set("Authorization", "Bearer public")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-opencode-client", "cli")
	req.Header.Set("x-opencode-session", ses)
	req.Header.Set("x-opencode-session-id", ses)
	switch cfg.ClientProfile {
	case "2.1":
		// opencode 2.1.4 (bắt bằng mitmproxy): UA 3 đoạn, không có x-opencode-request, thêm affinity + trace.
		tid, sid := randomHex(32), randomHex(16)
		req.Header.Set("User-Agent", "opencode/latest/2.1.4/cli")
		req.Header.Set("x-opencode-project", proj)
		req.Header.Set("x-session-affinity", ses)
		req.Header.Set("x-session-id", ses)
		req.Header.Set("traceparent", "00-"+tid+"-"+sid+"-01")
		req.Header.Set("b3", tid+"-"+sid+"-1-"+randomHex(16))
	case "legacy":
		req.Header.Set("User-Agent", "opencode/2.1.1")
		req.Header.Set("x-opencode-client", "CLI")
		req.Header.Set("x-opencode-project", proj)
		req.Header.Set("x-opencode-request", newRequestID())
		req.Header.Set("x-session-id", ses)
	default:
		// opencode 1.18.35 (bắt bằng mitmproxy, đã đo 200 qua direct/WARP/dân cư khi đi kèm TLS Bun + khối <env>).
		req.Header.Set("User-Agent", "opencode/1.18.35 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14")
		req.Header.Set("x-opencode-project", "global")
		req.Header.Set("x-opencode-request", newRequestID())
	}
	req.Header.Set("Accept", accept)
}

// ======================== FORWARD ========================

type gwError struct {
	status     int
	typ        string
	msg        string
	retryAfter time.Duration
}

func (e *gwError) Error() string { return e.msg }

// upstream là 1 response đang mở; luôn gọi close() để trả slot egress.
type upstream struct {
	resp   *http.Response
	egress *Egress
	ses    *egSession
	model  string
	ttfb   time.Duration
	cancel context.CancelFunc
	once   sync.Once
}

func (u *upstream) close() {
	u.once.Do(func() {
		u.resp.Body.Close()
		u.cancel()
		u.egress.sess.done(u.ses)
		u.egress.release()
	})
}

// idleBody hủy request nếu upstream im lặng quá lâu giữa chừng (tránh treo slot vĩnh viễn khi stream đứng).
type idleBody struct {
	rc io.ReadCloser
	t  *time.Timer
	d  time.Duration
}

func (b *idleBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.t.Reset(b.d)
	}
	return n, err
}

func (b *idleBody) Close() error {
	b.t.Stop()
	return b.rc.Close()
}

func parseRetryAfter(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// penalizeStreamError: lỗi đọc body giữa chừng (mất mạng) thì phạt egress đó. Nếu không, proxy hỏng vẫn nằm trong
// pool và tiếp tục rò lỗi về client ở mọi request sau.
func penalizeStreamError(e *Egress, err error) {
	if e == nil || err == nil {
		return
	}
	if errors.Is(err, io.EOF) {
		return // EOF sạch = upstream kết thúc stream, không phải lỗi mạng
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return // client huỷ (Ctrl-C) hoặc hết hạn -> KHÔNG phạt egress, lỗi không phải do đường mạng
	}
	e.errs.Add(1)
	e.adapt(false)
	run := e.errRun.Add(1)
	back := 2 * time.Second << min(int(run)-1, 5)
	if back > time.Minute {
		back = time.Minute
	}
	e.coolEgress(back)
}

// pickModelsFor: danh sách model mà 1 lần forward() có thể phục vụ (dùng để tính Retry-After).
func pickModelsFor(model string, isPool bool) []string {
	if isPool {
		poolMu.RLock()
		pool := append([]string(nil), freeModelPool...)
		poolMu.RUnlock()
		if len(pool) > 0 {
			return pool
		}
	}
	return []string{model}
}

// retryAfterForClient: Retry-After trả về client. Cooldown của egress chỉ vài chục giây, nên khi quota tài khoản
// đang cạn (breaker chặn) thì mốc đúng để client chờ là lúc QUOTA HỒI, không phải lúc hết cooldown - client retry
// theo hạn cooldown sẽ lại nhận 429 ngay.
func retryAfterForClient(d time.Duration) time.Duration {
	best := d
	if acctGate.capped() {
		if g := time.Until(acctGate.resetTime()); g > best {
			best = g
		}
	}
	if best > acctGateResetMax {
		best = acctGateResetMax
	}
	return best
}

func appendAvoid(list []string, d string) []string {
	for _, x := range list {
		if x == d {
			return list
		}
	}
	list = append(list, d)
	if len(list) >= 7 {
		return nil
	}
	return list
}
func normalizeLastError(last *gwError) *gwError { // last==nil: mọi lần thử đều bị bỏ qua trước khi gửi được
	if last == nil {
		return &gwError{status: http.StatusBadGateway, typ: "upstream_error", msg: "No upstream attempt made"}
	}
	if last.status == http.StatusForbidden {
		return &gwError{
			status:     http.StatusTooManyRequests,
			typ:        "rate_limit_error",
			msg:        "All egress paths are currently rate limited on upstream free tier. Retrying shortly.",
			retryAfter: 15 * time.Second,
		}
	}
	return last
}

var (
	dynamicAliasMu sync.RWMutex
	dynamicAliases = map[string]string{}
)

func registerDynamicAlias(deprecated, replacement string) {
	if deprecated == "" || replacement == "" || deprecated == replacement {
		return
	}
	dynamicAliasMu.Lock()
	defer dynamicAliasMu.Unlock()
	dynamicAliases[deprecated] = replacement
	dBase := strings.TrimSuffix(deprecated, "-free")
	rBase := strings.TrimSuffix(replacement, "-free")
	dynamicAliases[dBase] = rBase
	dynamicAliases[dBase+"-free"] = rBase
}

func getDynamicAlias(model string) string {
	dynamicAliasMu.RLock()
	defer dynamicAliasMu.RUnlock()
	return dynamicAliases[model]
}

func parseReplacement(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var env struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
		Metadata struct {
			Replacement string `json:"replacement"`
		} `json:"metadata"`
	}
	if json.Unmarshal(body, &env) == nil {
		if env.Metadata.Replacement != "" {
			return env.Metadata.Replacement
		}
		if env.Error.Message != "" {
			idx := strings.Index(env.Error.Message, "Use ")
			if idx >= 0 {
				tail := env.Error.Message[idx+4:]
				if sp := strings.Index(tail, " instead"); sp > 0 {
					return strings.TrimSpace(tail[:sp])
				}
			}
		}
	}
	return ""
}

func isModelDeprecated(status int, body []byte) bool {
	if status == http.StatusGone {
		return true
	}
	if len(body) == 0 {
		return false
	}
	return bytes.Contains(body, []byte("ModelDeprecated")) || bytes.Contains(body, []byte("has been deprecated"))
}

func isModelParamError(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	return bytes.Contains(body, []byte("unsupported_value")) ||
		bytes.Contains(body, []byte("unsupported_parameter")) ||
		bytes.Contains(body, []byte("Endpoint is unavailable"))
}

func nextModelInPool(current string) string {
	poolMu.RLock()
	pool := append([]string(nil), freeModelPool...)
	poolMu.RUnlock()
	// bộ lọc (model, egress) đã cạn nằm trong pickAvoid
	n := len(pool)
	if n == 0 {
		return current
	}
	for i, m := range pool {
		if m == current {
			return pool[(i+1)%n]
		}
	}
	return pool[0]
}

// forward gửi request tới upstream, tự chọn egress (direct trước) và model (nếu isPool).
// Retry khi 429/502/503/504/lỗi mạng bằng egress khác; không bao giờ retry sau khi đã có byte gửi về client
// (hàm này chỉ trả về response khi header đã nhận, việc ghi cho client do caller làm).
func forward(ctx context.Context, model string, isPool bool, url, accept, key string, prep func(model string) []byte) (*upstream, *gwError) {
	// Ngân sách chờ của 1 request: mặc định MAX_WAIT, nhưng client có thể tự hạn bằng header X-Max-Wait.
	// Hạn của client là mốc TUYỆT ĐỐI (tính 1 lần lúc vào request) để các lần retry trong cùng request không
	// cộng dồn thời gian chờ - client yêu cầu 15s thì tổng thời gian chờ cũng là 15s, không phải N x 15s.
	wait := waitBudget(ctx)
	deadline := time.Now().Add(wait)
	// Auto: lọc pool theo profile (auto-fast/best/long) và giới hạn context đã học cho cỡ request này.
	tokens := 0
	if isPool {
		tokens = estTokens(len(prep(model)))
		ctx = withExclude(ctx, routeExclusions(profileFrom(ctx), tokens, excludedModels(ctx)))
	}
	var last *gwError
	avoidDaemons := avoidFrom(ctx)
	currentModel := model
	canSwitch := isPool || modelFallback(ctx) // model cụ thể ở chế độ cố định: không bao giờ đổi model
	var modelFailures int
	paramErrors := 0 // số lần upstream báo 400 do tham số của client

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
			}
		}
		// Tổng thời gian tối đa cho 1 request (MaxWait / X-Max-Wait của client): hết hạn thì DỪNG, không gửi thêm
		// request lên upstream. Đặt SAU khoảng nghỉ giữa 2 lần thử để không bỏ sót: nếu khoảng nghỉ đã dùng hết hạn
		// thì lần thử sau không được gửi đi. Trước đây attempt đầu tiên không bao giờ bị chặn.
		if !time.Now().Before(deadline) {
			if last != nil {
				return nil, normalizeLastError(last)
			}
			return nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
				msg: "Max wait exceeded.", retryAfter: mgr.soonest(pickModelsFor(model, isPool))}
		}
		m, e, gerr := mgr.acquire(ctx, currentModel, isPool, deadline, avoidDaemons...) // đã giữ chỗ (inflight++)
		if gerr != nil {
			return nil, gerr
		}

		var es *egSession
		if cfg.SessionIDMode == "egress" {
			if es = e.sess.acquire(cfg.SessionsPerEgress, cfg.SessionInflight); es == nil {
				// Egress vừa hết session giữa lúc pick: bỏ qua, chọn đường khác.
				e.release()
				avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
				continue
			}
		}
		relE := func() { e.sess.done(es); e.release() } // trả slot egress + chỗ trên session
		payload := prep(m)
		actx, cancel := context.WithCancel(ctx)
		// Hạn chờ của client áp cho cả thời gian chờ HEADER upstream: client đặt X-Max-Wait=15s thì không được ngồi
		// đó chờ upstream trả header 3 phút. Chỉ áp tới khi có header - phần stream sau đó KHÔNG bị cắt (bài toán xử
		// lý văn bản dài cần stream chạy tới cuối, có thể mất vài phút).
		var timedOut atomic.Bool
		var cancelRight atomic.Bool
		headerCh := make(chan struct{})
		if dl, ok := deadlineFrom(ctx); ok {
			go func() {
				t := time.NewTimer(time.Until(dl))
				defer t.Stop()
				select {
				case <-t.C:
					// Ai "chiếm" quyền hủy trước thì thắng: nếu header đã về thì thôi, không cắt body nữa.
					if cancelRight.CompareAndSwap(false, true) {
						timedOut.Store(true)
						cancel()
					}
				case <-headerCh:
				case <-actx.Done():
				}
			}()
		}
		req, err := http.NewRequestWithContext(actx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			cancel()
			relE()
			return nil, &gwError{status: http.StatusInternalServerError, typ: "internal_error", msg: "build request"}
		}
		setUpstreamHeaders(req, accept, key, es)

		e.reqs.Add(1)
		e.lastUsed.Store(time.Now().UnixNano())
		if e.Kind == kindDirect {
			mgr.noteDirect(m, int(e.inflight.Load()))
		}
		gen := e.gen.Load()
		start := time.Now()
		// Circuit Tor kẹt (chưa có header sau ttfbLimit): bỏ, đổi exit và thử đường khác thay vì để client chờ.
		// Với prompt lớn (như omp gửi 70k-100k token, payload 200KB-400KB), upstream cần 15-30s để xử lý context.
		ttfbLimit := cfg.TtfbRetry
		if bodyLen := len(payload); bodyLen > 100*1024 && ttfbLimit < 60*time.Second {
			ttfbLimit = 60 * time.Second
		} else if bodyLen > 30*1024 && ttfbLimit < 40*time.Second {
			ttfbLimit = 40 * time.Second
		}
		var slow atomic.Bool
		var slowTimer *time.Timer
		if e.canRotate && ttfbLimit > 0 {
			slowTimer = time.AfterFunc(ttfbLimit, func() { slow.Store(true); cancel() })
		}
		resp, err := e.Client().Do(req)
		cancelRight.CompareAndSwap(false, true) // đã có header (hoặc đã lỗi): không ai cắt body nữa
		close(headerCh)
		if slowTimer != nil {
			slowTimer.Stop()
		}
		if timedOut.Load() && err == nil {
			// Watchdog vừa kịp hủy đúng lúc header về: body có thể đã bị cắt -> không dùng, trả lỗi hết hạn.
			cancel()
			relE()
			resp.Body.Close()
			return nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
				msg: "Max wait exceeded while waiting for upstream.", retryAfter: mgr.soonest(pickModelsFor(model, isPool))}
		}
		if err != nil {
			cancel()
			relE()
			if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() != nil {
				// Client huỷ -> KHÔNG phạt egress: lỗi do phía client, không phải đường mạng.
				return nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
			}
			if timedOut.Load() {
				// Hết hạn chờ của client khi upstream chưa kịp trả header: báo 429 (nhất quán với "max wait exceeded").
				return nil, &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
					msg: "Max wait exceeded while waiting for upstream.", retryAfter: mgr.soonest(pickModelsFor(model, isPool))}
			}
			if ctx.Err() != nil {
				return nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
			}
			e.errs.Add(1)
			e.adapt(false)
			// Lỗi TCP/transport không gắn với model: cho CẢ egress nghỉ (không chỉ model này), backoff 2s,4s,8s... tối đa 1 phút,
			// để bộ điều phối sidelined egress hỏng thay vì cứ chọn lại và lại reset (tránh rò 502 về client).
			run := e.errRun.Add(1)
			back := 2 * time.Second << min(int(run)-1, 5)
			if back > time.Minute {
				back = time.Minute
			}
			// Tor tự xoay circuit (e.rotate) nên không cần nghỉ thêm; WARP/proxy thì cần nghỉ để bộ điều phối
			// không chọn lại đúng chỗ vừa lỗi.
			if !e.canRotate {
				e.coolEgress(back)
			}
			if e.tier == 1 && e.refreshURL != "" {
				// Proxy dân cư hỏng (IP chết / key hết hạn) thì MỖI lần thử tốn 20-40 giây chờ timeout -> phải
				// LOẠI NGAY khỏi routing, không để request tiếp theo lại dính nó. Xoay IP (nếu được) sẽ đưa nó
				// vào lại pool với IP mới; nếu key đã hết hạn thì egress bị loại hẳn.
				log.Printf("[%s] lỗi truyền tải, loại khỏi routing và xoay IP", e.Name)
				e.ready.Store(false)
				if !mgr.rotateResidentialProxy(e) {
					// Đang xoay hoặc bị giới hạn 60s giữa 2 lần xoay -> thử lại sau, nếu không egress này bị loại
					// vĩnh viễn mà không ai xoay IP cho nó.
					time.AfterFunc(45*time.Second, func() {
						if e.expired.Load() {
							return
						}
						e.ready.Store(true)
						mgr.rotateResidentialProxy(e)
					})
				}
			}
			// WARP: container/IP hỏng (lỗi TCP lặp lại) thì đổi danh tính để lấy IP mới, nếu không chỉ bị "giấu"
			// bằng cooldown rồi lại hỏng tiếp.
			if e.container != "" && run >= 3 && !e.renewing.Load() {
				log.Printf("[%s] lỗi truyền tải liên tiếp, đổi danh tính WARP", e.Name)
				mgr.renewIdentity(e)
				e.errRun.Store(0)
			}
			e.rotate(gen)
			// KHÔNG đưa egress vào avoidDaemons: cooldown toàn-egress đã loại nó khỏi vòng chọn, nếu chặn vĩnh viễn
			// trong request này thì khi pool chỉ có 1 egress, request sẽ chờ hết MAX_WAIT rồi nhận 429 dù egress đã
			// hồi. Có cooldown thì bộ điều phối tự bỏ qua nó cho tới khi hết hạn.
			if slow.Load() {
				err = fmt.Errorf("không có phản hồi sau %v (circuit chậm)", ttfbLimit)
			}
			log.Printf("[%s] attempt=%d model=%s transport error: %v", e.Name, attempt, m, err)
			last = &gwError{status: http.StatusBadGateway, typ: "upstream_error", msg: "Upstream transport error"}
			continue
		}
		ttfb := time.Since(start)

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			ra := parseRetryAfter(resp.Header)
			resp.Body.Close()
			cancel()
			relE()
			e.r429.Add(1)
			freeLimit := bytes.Contains(snippet, []byte("FreeUsageLimitError")) || bytes.Contains(snippet, []byte("Free usage limit"))
			if !freeLimit {
				e.adapt(false) // rate limit ngắn hạn: giảm trần in-flight của egress này
			}
			if freeLimit && ra >= acctGateMinCap {
				// Hạn mức free là CỦA CẶP (model, IP): IP này hết lượt cho model này -> ghi lại để gateway
				// không gửi thêm request vào nó (mỗi request 429 là vừa lãng phí vừa làm client chờ).
				noteModelExhausted(m, e.Name, time.Now().Add(ra))
				// Nếu model này đã cạn trên MỌI IP đang có -> phải lấy IP mới: WARP luôn có thể đổi danh tính
				// (mỗi lần ~6-13s) và IPv6 mới có ngân sách riêng. Cứ đổi tới khi được, rồi dùng IP đó tới hết
				// ngân sách rồi lại đổi tiếp. Giới hạn tần suất để không xoay liên tục.
				if e.container != "" && !acctGate.capped() && modelUsableEgresses(m, len(mgr.snapshot())) == 0 {
					if time.Since(e.lastIdentityChange) > 20*time.Second {
						log.Printf("[%s] model %s đã cạn trên mọi IP -> đổi danh tính để lấy IP mới", e.Name, m)
						e.lastIdentityChange = time.Now()
						mgr.renewIdentity(e)
					}
				}
				// Retry-after dài + (IP mới tinh cũng cạn) = bằng chứng quota cạn ở cấp tài khoản, không phải từng IP.
				// "IP mới" phải gắn với SESSION của request này: request đang chạy với session của IP CŨ (lấy trước lúc
				// đổi IP) thì không được tính là bằng chứng. Thế hệ pool (gen) là dấu hiệu sẵn có.
				fresh := e.ipIsFresh() && (es == nil || es.gen == e.sess.genNow())
				acctGate.noteFreshCap(e.Name, ra, fresh, m)
			}
			if es != nil && freeLimit {
				if !e.sess.blocked(es, ra) {
					// Session của IP cũ (egress vừa đổi IP): kết quả không áp cho pool mới, thử lại ngay.
					last = &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error", msg: "Rate limited on all egress paths. Retry shortly."}
					continue
				}
				// WARP đổi IP rẻ (~15-25s, không giới hạn): hễ egress không phục vụ được ngay (hết lượt mở session mới
				// và session còn lại đều đang nghỉ) là đổi danh tính luôn, không ngồi chờ session hết nghỉ.
				if e.container != "" && !e.sess.usable() && !acctGate.capped() {
					mgr.renewIdentity(e)
					avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
					last = &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error", msg: "Rate limited on all egress paths. Retry shortly."}
					continue
				}
				if !e.sess.exhausted() {
					// Chỉ session này cạn (hoặc vừa hết lượt mở session mới nhưng còn session sống): egress vẫn dùng được.
					log.Printf("[%s] attempt=%d model=%s session %s bị từ chối, egress còn session khác", e.Name, attempt, m, es.id[:16])
					last = &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error", msg: "Rate limited on all egress paths. Retry shortly."}
					continue
				}
				// IP hết session hôm nay: đổi danh tính WARP để lấy IP mới (nếu là WARP), không thì nghỉ tới khi reset.
				// Nhưng nếu breaker đang chặn (quota cạn cấp tài khoản) thì đổi IP VÔ ÍCH - đừng đổi.
				if e.container != "" && !acctGate.capped() {
					mgr.renewIdentity(e)
					avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
					last = &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error", msg: "Rate limited on all egress paths. Retry shortly."}
					continue
				}
			}
			d := e.exhausted(m, ra, e.gen.Load() == gen)
			if e.Kind == kindDirect {
				mgr.prewarmTor("direct vừa bị 429", 2)
			}
			if e.Kind == kindProxy {
				if e.tier == 1 && e.refreshURL != "" {
					// Quota cạn cấp tài khoản => đổi IP vô ích (và tốn tiền API), breaker đang chặn.
					if !acctGate.capped() {
						mgr.rotateResidentialProxy(e)
					}
				} else if e.tier == 2 && cfg.SessionIDMode != "egress" {
					// Quota cạn cấp tài khoản => đổi IP/restart container vô ích, breaker đang chặn.
					if (freeLimit || ra > 30*time.Minute) && !acctGate.capped() {
						renewWarpProxy(e.Name)
					}
				}
			}
			e.rotate(gen)
			avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
			log.Printf("[%s] attempt=%d model=%s 429 retry-after=%v cooldown=%v ttfb=%v body=%q",
				e.Name, attempt, m, ra, d.Round(time.Second), ttfb.Round(time.Millisecond), string(snippet))
			last = &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
				msg: "Rate limited on all egress paths. Retry shortly.", retryAfter: retryAfterForClient(d)}
			modelFailures++
			if canSwitch && modelFailures >= 3 && isFreeModelInPool(currentModel) {
				next := nextModelInPool(currentModel)
				if next != currentModel {
					log.Printf("[Router] model=%s bị 429, tự động chuyển ngay sang model=%s", currentModel, next)
					currentModel = next
					modelFailures = 0
					avoidDaemons = nil
				}
			}
			continue

		case resp.StatusCode == http.StatusInternalServerError || resp.StatusCode == http.StatusBadGateway ||
			resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout:
			resp.Body.Close()
			cancel()
			relE()
			e.errs.Add(1)
			e.adapt(false)
			e.transient(m, 5*time.Second)
			e.rotate(gen)
			avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
			log.Printf("[%s] attempt=%d model=%s HTTP %d, thử đường khác", e.Name, attempt, m, resp.StatusCode)
			last = &gwError{status: http.StatusBadGateway, typ: "upstream_error", msg: fmt.Sprintf("Upstream HTTP %d", resp.StatusCode)}
			continue

		case resp.StatusCode == http.StatusForbidden:
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			cancel()
			relE()
			e.errs.Add(1)
			coolDuration := 30 * time.Second
			if e.Kind == kindDirect {
				coolDuration = cfg.DirectCooldown
				if coolDuration <= 0 {
					coolDuration = 2 * time.Minute
				}
				mgr.prewarmTor("direct vừa bị 403/hết hạn mức", 2)
			}
			e.transient(m, coolDuration)
			if e.canRotate {
				e.rotate(gen)
			}
			avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
			log.Printf("[%s] attempt=%d model=%s 403 (exit bị chặn hoặc FreeTierError), đổi egress body=%q", e.Name, attempt, m, string(snippet))
			last = &gwError{status: http.StatusForbidden, typ: "permission_error", msg: "Egress blocked: " + strings.TrimSpace(string(snippet))}
			modelFailures++
			if canSwitch && modelFailures >= 3 && isFreeModelInPool(currentModel) {
				next := nextModelInPool(currentModel)
				if next != currentModel {
					log.Printf("[Router] model=%s bị lỗi liên tiếp, tự động failover sang model=%s", currentModel, next)
					currentModel = next
					modelFailures = 0
					avoidDaemons = nil
				}
			}
			continue
		case resp.StatusCode == http.StatusGone || (resp.StatusCode == http.StatusBadRequest && isModelDeprecated(resp.StatusCode, nil)):
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			cancel()
			relE()
			replacement := parseReplacement(snippet)
			if replacement == "" {
				replacement = nextModelInPool(currentModel)
			}
			log.Printf("[Router] model=%s bị 410/ModelDeprecated! Tự động chuyển vĩnh viễn sang model=%s", currentModel, replacement)
			registerDynamicAlias(currentModel, replacement)
			prunePool(currentModel)
			if replacement != currentModel && replacement != "" {
				currentModel = replacement
				modelFailures = 0
				avoidDaemons = nil
			}
			continue

		case resp.StatusCode == http.StatusBadRequest:
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			if isContextError(snippet) {
				// Model này không đọc nổi request cỡ này: ghi nhớ giới hạn; với auto thì chuyển sang model khác ngay.
				resp.Body.Close()
				cancel()
				relE()
				est := tokens
				if est == 0 {
					est = estTokens(len(payload))
				}
				noteContextLimit(m, est)
				log.Printf("[Context] model=%s không nhận ~%d token, ghi nhớ giới hạn", m, est)
				if isPool {
					ex := map[string]bool{m: true}
					for k, v := range excludedModels(ctx) {
						ex[k] = v
					}
					ctx = withExclude(ctx, ex)
					// Mọi model trong pool đều bị loại vì vượt context => request này không thể thành công với model nào.
					// Trả 400 luôn, đừng xếp hàng tới MAX_WAIT (30 phút) rồi trả 429 - client cần biết để chia nhỏ văn bản.
					if poolAllExcluded(ex) {
						last = &gwError{status: http.StatusBadRequest, typ: "invalid_request_error",
							msg: "Request vượt context của mọi model trong pool: hãy chia nhỏ văn bản"}
						return nil, last
					}
					continue
				}
				last = &gwError{status: http.StatusBadRequest, typ: "invalid_request_error", msg: "Upstream HTTP 400: " + strings.TrimSpace(string(snippet))}
				return nil, last
			}
			if isModelDeprecated(resp.StatusCode, snippet) {
				resp.Body.Close()
				cancel()
				relE()
				replacement := parseReplacement(snippet)
				if replacement == "" {
					replacement = nextModelInPool(currentModel)
				}
				log.Printf("[Router] model=%s báo ModelDeprecated (400)! Tự động chuyển sang model=%s", currentModel, replacement)
				registerDynamicAlias(currentModel, replacement)
				prunePool(currentModel)
				if replacement != currentModel && replacement != "" {
					currentModel = replacement
					modelFailures = 0
					avoidDaemons = nil
				}
				continue
			}
			if p := unsupportedParam(snippet); p != "" && autoParamSet(p) {
				// Tham số do gateway tự thêm cho auto (vd reasoning) mà nhà cung cấp phía sau không nhận: tạm ngừng gửi
				// tham số đó cho model này rồi gửi lại ngay, không tính là lỗi của egress.
				resp.Body.Close()
				cancel()
				relE()
				markParamUnsupported(m, p)
				log.Printf("[Auto] model=%s không nhận tham số %q, bỏ tham số và gửi lại", m, p)
				continue
			}
			if canSwitch && isModelParamError(snippet) && isFreeModelInPool(currentModel) {
				resp.Body.Close()
				cancel()
				relE()
				// KHÔNG tính e.errs: lỗi do tham số của client, egress hoàn toàn ổn.
				log.Printf("[Router] model=%s bị lỗi tham số hoặc backend (400), tự động failover sang model khác: %s", m, string(snippet))
				paramErrors++
				last = &gwError{status: http.StatusBadRequest, typ: "invalid_request_error",
					msg: "Upstream HTTP 400: " + strings.TrimSpace(string(snippet))}
				if paramErrors >= 2 {
					// Tham số của client bị từ chối ở 2 model liên tiếp -> tham số sai, không phải model.
					// Trả 400 luôn thay vì thử hết pool rồi ra 502 "No upstream attempt made".
					return nil, last
				}
				next := nextModelInPool(currentModel)
				if next != currentModel {
					currentModel = next
					modelFailures = 0
					avoidDaemons = nil
				}
				continue
			}
			resp.Body.Close()
			cancel()
			relE()
			log.Printf("[%s] attempt=%d model=%s HTTP 400: %s", e.Name, attempt, m, string(snippet))
			last = &gwError{status: http.StatusBadRequest, typ: "invalid_request_error", msg: "Upstream HTTP 400: " + strings.TrimSpace(string(snippet))}
			return nil, last
		}

		if resp.StatusCode == http.StatusOK {
			e.sess.success(es)
			if e.ipIsFresh() && es != nil && es.gen == e.sess.genNow() {
				// IP vừa đổi VÀ session của request này thuộc thế hệ hiện tại (tức cặp (IP, session) MỚI) mà vẫn trả
				// 200 => cặp mới vẫn được chấp nhận => quota đã hồi, mở breaker.
				// 200 của session CŨ không chứng minh được điều đó (lúc quota cạn một phần, session cũ vẫn chạy được).
				acctGate.clear()
			}
			// 1 request 200 KHÔNG có nghĩa egress hết lỗi TCP (request khác vẫn đang reset với nó). Nếu đang bị
			// cooldown lỗi TCP thì giữ nguyên bộ đếm lỗi và trần in-flight: nếu xoá, backoff không bao giờ leo và 502
			// tiếp tục rò. Chỉ khi egress KHÔNG bị cooldown mới coi là đã hồi hẳn.
			if e.downUntil.Load() == 0 {
				e.errRun.Store(0)
				e.adapt(true)
			}
			okRate.add()
			e.oks.Add(1)
			e.clear(m)
			e.observeLatency(gen, ttfb)
			mgr.retireIfSlow(e, gen)
		}
		// StreamIdle <= 0 nghĩa là "không cắt" - không được đặt timer 0 (sẽ cắt ngay mọi stream).
		idleD := cfg.StreamIdle
		if idleD <= 0 {
			idleD = 10 * time.Minute
		}
		resp.Body = &idleBody{rc: resp.Body, d: idleD, t: time.AfterFunc(idleD, cancel)}
		return &upstream{resp: resp, egress: e, ses: es, model: m, ttfb: ttfb, cancel: cancel}, nil
	}
	return nil, normalizeLastError(last)
}

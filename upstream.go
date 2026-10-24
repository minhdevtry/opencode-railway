package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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

// setUpstreamHeaders: mỗi lần gọi đều tạo session/project/request ID mới.
// Quota upstream tính theo IP chứ không theo session; cache nằm ở phía server chạy model nên đổi ID không mất cache.
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

// setUpstreamHeaders đặt bộ header giả danh opencode. Quota upstream tính theo IP chứ không theo session/device
// (đã thử cả 4 tổ hợp đổi/giữ ID trên IP đã hết quota: đều 429), nên ID chỉ để trông như client thật và để giữ prompt
// cache: mặc định mỗi hội thoại của client có 1 session/project ID ổn định; không biết hội thoại thì dùng ID mới.
func setUpstreamHeaders(req *http.Request, accept, key string) {
	ses, proj := newSessionID(), newProjectID()
	ks, kp := idsFromKey(key)
	switch cfg.SessionIDMode {
	case "stable":
		ses = stableSession
	case "client":
		if key != "" {
			ses = ks
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
	req.Header.Set("User-Agent", "opencode/2.1.1")
	req.Header.Set("x-opencode-client", "CLI")
	req.Header.Set("x-opencode-project", proj)
	req.Header.Set("x-opencode-request", newRequestID())
	req.Header.Set("x-opencode-session", ses)
	req.Header.Set("x-opencode-session-id", ses)
	req.Header.Set("x-session-id", ses)
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
	model  string
	ttfb   time.Duration
	cancel context.CancelFunc
	once   sync.Once
}

func (u *upstream) close() {
	u.once.Do(func() {
		u.resp.Body.Close()
		u.cancel()
		u.egress.inflight.Add(-1)
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
func normalizeLastError(last *gwError) *gwError {
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
	defer poolMu.RUnlock()
	n := len(freeModelPool)
	for i, m := range freeModelPool {
		if m == current {
			return freeModelPool[(i+1)%n]
		}
	}
	if n > 0 {
		return freeModelPool[0]
	}
	return current
}

// forward gửi request tới upstream, tự chọn egress (direct trước) và model (nếu isPool).
// Retry khi 429/502/503/504/lỗi mạng bằng egress khác; không bao giờ retry sau khi đã có byte gửi về client
// (hàm này chỉ trả về response khi header đã nhận, việc ghi cho client do caller làm).
func forward(ctx context.Context, model string, isPool bool, url, accept, key string, prep func(model string) []byte) (*upstream, *gwError) {
	deadline := time.Now().Add(cfg.MaxWait)
	var last *gwError
	var avoidDaemons []string
	currentModel := model
	var modelFailures int

	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		// Tổng thời gian tối đa cho 1 request (MaxWait): đã có lỗi và hết ngân sách thì trả lỗi cuối, không thử mãi.
		if last != nil && time.Now().After(deadline) {
			return nil, normalizeLastError(last)
		}
		if attempt > 0 {
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
			}
		}
		m, e, gerr := mgr.routeAvoid(ctx, currentModel, isPool, deadline, avoidDaemons...)
		if gerr != nil {
			return nil, gerr
		}

		payload := prep(m)
		actx, cancel := context.WithCancel(ctx)
		req, err := http.NewRequestWithContext(actx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return nil, &gwError{status: http.StatusInternalServerError, typ: "internal_error", msg: "build request"}
		}
		setUpstreamHeaders(req, accept, key)

		e.inflight.Add(1)
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
		if slowTimer != nil {
			slowTimer.Stop()
		}
		if err != nil {
			cancel()
			e.inflight.Add(-1)
			if ctx.Err() != nil {
				return nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
			}
			e.errs.Add(1)
			e.transient(m, 3*time.Second)
			e.rotate(gen)
			avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
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
			e.inflight.Add(-1)
			e.r429.Add(1)
			d := e.exhausted(m, ra, e.gen.Load() == gen)
			if e.Kind == kindDirect {
				mgr.prewarmTor("direct vừa bị 429", 2)
			}
			if e.Kind == kindProxy {
				if e.tier == 1 && e.refreshURL != "" {
					mgr.rotateResidentialProxy(e)
				} else if e.tier == 2 {
					if bytes.Contains(snippet, []byte("FreeUsageLimitError")) || bytes.Contains(snippet, []byte("Free usage limit reached")) || ra > 30*time.Minute {
						renewWarpProxy(e.Name)
					}
				}
			}
			e.rotate(gen)
			avoidDaemons = appendAvoid(avoidDaemons, e.daemon)
			log.Printf("[%s] attempt=%d model=%s 429 retry-after=%v cooldown=%v ttfb=%v body=%q",
				e.Name, attempt, m, ra, d.Round(time.Second), ttfb.Round(time.Millisecond), string(snippet))
			last = &gwError{status: http.StatusTooManyRequests, typ: "rate_limit_error",
				msg: "Rate limited on all egress paths. Retry shortly.", retryAfter: d}
			modelFailures++
			if modelFailures >= 3 && isFreeModelInPool(currentModel) {
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
			e.inflight.Add(-1)
			e.errs.Add(1)
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
			e.inflight.Add(-1)
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
			if modelFailures >= 3 && isFreeModelInPool(currentModel) {
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
			e.inflight.Add(-1)
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
			if isModelDeprecated(resp.StatusCode, snippet) {
				resp.Body.Close()
				cancel()
				e.inflight.Add(-1)
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
			if isModelParamError(snippet) && isFreeModelInPool(currentModel) {
				resp.Body.Close()
				cancel()
				e.inflight.Add(-1)
				e.errs.Add(1)
				log.Printf("[Router] model=%s bị lỗi tham số hoặc backend (400), tự động failover sang model khác: %s", currentModel, string(snippet))
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
			e.inflight.Add(-1)
			last = &gwError{status: http.StatusBadRequest, typ: "invalid_request_error", msg: "Upstream HTTP 400: " + strings.TrimSpace(string(snippet))}
			return nil, last
		}

		if resp.StatusCode == http.StatusOK {
			e.oks.Add(1)
			e.clear(m)
			e.observeLatency(gen, ttfb)
			mgr.retireIfSlow(e, gen)
		}
		resp.Body = &idleBody{rc: resp.Body, d: cfg.StreamIdle, t: time.AfterFunc(cfg.StreamIdle, cancel)}
		return &upstream{resp: resp, egress: e, model: m, ttfb: ttfb, cancel: cancel}, nil
	}
	return nil, normalizeLastError(last)
}

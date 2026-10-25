package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ======================== EGRESS ========================
// Egress = 1 đường ra Internet: IP trực tiếp của container, hoặc 1 SOCKS5/HTTP proxy (Tor...).
// Quota "client thật" của upstream tính theo (IP, session) (sessions.go); cooldown ngắn hạn (lỗi mạng, 5xx, rate limit
// ngắn) vẫn lưu theo (egress × model) vì có nhà cung cấp phía sau chỉ nghẽn ở 1 model.

const (
	kindDirect = "direct"
	kindTor    = "tor"
	kindProxy  = "proxy"
)

type coolState struct {
	until   time.Time
	strikes int
}

type Egress struct {
	Name      string
	Kind      string
	daemon    string // tên daemon ("tor-1", "tor-2", "direct"...)
	canRotate bool // Tor: đổi được exit bằng cách đổi thông tin xác thực SOCKS (IsolateSOCKSAuth)
	tier       int
	refreshURL string
	expired    atomic.Bool
	lastRotate time.Time

	client atomic.Pointer[http.Client]
	ready  atomic.Bool
	gen    atomic.Uint64 // "thế hệ" circuit; tăng lên là chuyển sang circuit/exit mới

	inflight atomic.Int32
	lastUsed atomic.Int64       // unix nano của lần dùng gần nhất (để tắt Tor nhàn rỗi)
	stop     context.CancelFunc // dừng Tor do gateway tự chạy (chỉ ở leader)
	dataDir  string             // thư mục tạm của daemon Tor (để dọn dẹp khi tắt)
	latMu    sync.Mutex
	latEwma  time.Duration // trung bình trượt ttfb của circuit hiện tại
	latN     int
	latGen   uint64
	members  []*Egress // leader của 1 daemon Tor: toàn bộ identity (gồm cả chính nó)
	reqs     atomic.Int64
	oks      atomic.Int64
	r429     atomic.Int64
	errs     atomic.Int64

	mu   sync.Mutex
	cool map[string]*coolState

	limit     atomic.Int32 // trần in-flight hiện tại (AIMD, sched.go)
	errRun    atomic.Int32 // lỗi truyền tải liên tiếp (proxy dân cư chập chờn thì xoay IP)
	okRun     atomic.Int32
	sess      sessionPool // session sống lâu của riêng egress này (quota upstream tính theo cặp IP×session)
	renewing  atomic.Bool
	renews    atomic.Int64
	renewTries atomic.Int32 // số lần đổi liên tiếp chưa ra IPv6
	container string // container WARP tương ứng (để đổi danh tính khi IP hết lượt)
}

func newTransport(proxy func(*http.Request) (*url.URL, error)) *http.Transport {
	if cfg.UpstreamTLS == "bun" {
		return newFingerprintTransport(proxy)
	}
	return &http.Transport{
		Proxy: proxy,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       180 * time.Second,
		TLSHandshakeTimeout:   25 * time.Second,
		ResponseHeaderTimeout: 180 * time.Second, // không đặt Client.Timeout để stream dài không bị cắt
		ExpectContinueTimeout: 1 * time.Second,
		WriteBufferSize:       64 * 1024,
		ReadBufferSize:        64 * 1024,
	}
}

func newEgress(name, kind string, proxy func(*http.Request) (*url.URL, error)) *Egress {
	e := &Egress{Name: name, Kind: kind, cool: make(map[string]*coolState)}
	e.client.Store(&http.Client{Transport: newTransport(proxy)})
	return e
}

// newTorEgress: SOCKS5 tới 1 Tor. Mỗi "thế hệ" dùng username khác nhau => Tor tách circuit riêng (IsolateSOCKSAuth).
// Transport của Go tách pool kết nối theo URL proxy nên kết nối mới đi circuit mới, không bị HTTP/2 dồn vào kết nối cũ.
func newTorEgress(name, socksAddr string) *Egress {
	var e *Egress
	nonce := randomHex(6)
	proxy := func(*http.Request) (*url.URL, error) {
		return &url.URL{Scheme: "socks5", Host: socksAddr,
			User: url.UserPassword(fmt.Sprintf("g%d-%s", e.gen.Load(), nonce), "x")}, nil
	}
	e = newEgress(name, kindTor, proxy)
	e.canRotate = true
	return e
}

// newTorGroup tạo k "identity" trên cùng 1 daemon Tor. Mỗi identity có username SOCKS riêng nên có circuit/exit IP riêng,
// (Tor hiện bị upstream chặn "abusive traffic", giữ lại cho tương thích; production dùng WARP + proxy dân cư.)
func newTorGroup(name, socksAddr string, k int) []*Egress {
	if k < 1 {
		k = 1
	}
	g := make([]*Egress, k)
	for i := range g {
		g[i] = newTorEgress(fmt.Sprintf("%s.%d", name, i+1), socksAddr)
		g[i].daemon = name
	}
	g[0].members = g
	return g
}

// setGroupReady đặt trạng thái sẵn sàng cho toàn bộ identity của daemon.
func (e *Egress) setGroupReady(v bool) {
	if len(e.members) == 0 {
		e.ready.Store(v)
		return
	}
	for _, x := range e.members {
		x.ready.Store(v)
	}
}

// groupLastUsed: lần dùng gần nhất của bất kỳ identity nào trong daemon.
func (e *Egress) groupLastUsed() int64 {
	last := e.lastUsed.Load()
	for _, x := range e.members {
		if v := x.lastUsed.Load(); v > last {
			last = v
		}
	}
	return last
}

// groupInflight: số request đang chạy trên cả daemon.
func (e *Egress) groupInflight() int {
	if len(e.members) == 0 {
		return int(e.inflight.Load())
	}
	n := 0
	for _, x := range e.members {
		n += int(x.inflight.Load())
	}
	return n
}

// observeLatency ghi ttfb của 1 request thành công (gen = circuit đã dùng; circuit mới thì bắt đầu lại từ đầu).
func (e *Egress) observeLatency(gen uint64, ttfb time.Duration) {
	e.latMu.Lock()
	defer e.latMu.Unlock()
	if gen != e.latGen {
		e.latGen, e.latEwma, e.latN = gen, 0, 0
	}
	if e.latN == 0 {
		e.latEwma = ttfb
	} else {
		e.latEwma = (e.latEwma*7 + ttfb*3) / 10
	}
	e.latN++
}

// latency trả về (ttfb trung bình trượt, số mẫu) của circuit hiện tại.
func (e *Egress) latency() (time.Duration, int) {
	e.latMu.Lock()
	defer e.latMu.Unlock()
	if e.latGen != e.gen.Load() {
		return 0, 0
	}
	return e.latEwma, e.latN
}

// medianLatency: trung vị ttfb của các egress Tor/proxy đã đủ mẫu (0 nếu chưa đủ 3 egress để so sánh).
func (m *Manager) medianLatency() time.Duration {
	var l []time.Duration
	for _, e := range m.snapshot() {
		if e.Kind == kindDirect {
			continue
		}
		if d, n := e.latency(); n >= 3 {
			l = append(l, d)
		}
	}
	if len(l) < 3 {
		return 0
	}
	sort.Slice(l, func(i, j int) bool { return l[i] < l[j] })
	return l[len(l)/2]
}

// retireIfSlow: circuit chậm hơn hẳn các circuit khác (exit tồi) thì đổi sang exit mới; dần dần chỉ còn lại exit nhanh.
func (m *Manager) retireIfSlow(e *Egress, gen uint64) {
	if !e.canRotate {
		return
	}
	d, n := e.latency()
	if n < 6 {
		return
	}
	med := m.medianLatency()
	if med > 0 && d > 3*time.Second && d > med*5/2 {
		log.Printf("[%s] circuit chậm (ttfb≈%v, trung vị %v), đổi exit", e.Name, d.Round(time.Millisecond), med.Round(time.Millisecond))
		e.rotate(gen)
	}
}

func (e *Egress) Client() *http.Client { return e.client.Load() }

func (e *Egress) available(model string) bool {
	if !e.ready.Load() {
		return false
	}
	if cfg.SessionIDMode == "egress" && !e.sess.usable() {
		return false
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.cool[model]
	return !ok || !time.Now().Before(c.until)
}

// strikes: số lần 429 liên tiếp của (egress, model) kể từ lần thành công gần nhất (0 = khỏe/chưa thử).
func (e *Egress) strikes(model string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.cool[model]; ok {
		return c.strikes
	}
	return 0
}

// claim giữ chỗ để dùng egress cho model. Egress đã từng bị 429 (còn strike) mà hết cooldown chỉ cho 1 request
// "thăm dò" tại một thời điểm: giữ chỗ bằng cách đẩy until ra probeLease; kết quả thăm dò sẽ ghi đè
// (429 => cooldown thật, thành công => clear). Egress khỏe thì không giới hạn.
func (e *Egress) claim(model string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.cool[model]
	if !ok {
		return true
	}
	if time.Now().Before(c.until) {
		return false
	}
	if c.strikes > 0 {
		c.until = time.Now().Add(probeLease)
	}
	return true
}

const probeLease = 15 * time.Second

func (e *Egress) clear(model string) {
	e.mu.Lock()
	delete(e.cool, model)
	e.mu.Unlock()
}
func (e *Egress) clearAll() {
	e.mu.Lock()
	e.cool = make(map[string]*coolState)
	e.mu.Unlock()
}


// transient: lỗi tạm thời (502, lỗi mạng), nghỉ ngắn, không tăng strike.
func (e *Egress) transient(model string, d time.Duration) {
	e.mu.Lock()
	c := e.cool[model]
	if c == nil {
		c = &coolState{}
		e.cool[model] = c
	}
	if until := time.Now().Add(d); until.After(c.until) {
		c.until = until
	}
	e.mu.Unlock()
}

// exhausted: (fresh = request này đi trên circuit hiện tại, chưa bị request khác xoay) upstream trả 429 cho (egress, model). Trả về thời gian nghỉ đã áp dụng.
//   - Tor có control port: IP sẽ được đổi nên chỉ nghỉ ngắn rồi thử lại với exit mới.
//   - Còn lại (direct, proxy tĩnh): theo Retry-After (tối đa CooldownMax) nếu có, không thì backoff mũ 15s, 30s, 1m... tới CooldownMax.
//     Mỗi lần hết hạn chỉ có 1 request "thăm dò" lại; thành công thì clear().
func (e *Egress) exhausted(model string, retryAfter time.Duration, fresh bool) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.cool[model]
	if c == nil {
		c = &coolState{}
		e.cool[model] = c
	}
	// Nhiều request đồng thời cùng nhận 429 của 1 sự kiện chỉ tính là 1 lần: nếu đang cooldown (direct) hoặc exit đã được
	// đổi bởi request khác (Tor, fresh=false) thì không cộng strike, không kéo dài backoff.
	if now := time.Now(); (!e.canRotate && now.Before(c.until)) || (e.canRotate && !fresh) {
		if left := c.until.Sub(now); left > 0 {
			return left
		}
		return cfg.TorRotateCooldown
	}
	c.strikes++
	var d time.Duration
	switch {
	case e.canRotate:
		// Đổi circuit là có exit mới nên thử lại gần như ngay; nhưng nếu 5+ lần liên tiếp đều 429 thì có thể cả bể exit
		// hôm nay đã cạn: giãn dần để không đập vào upstream.
		d = cfg.TorRotateCooldown
		if c.strikes > 4 {
			shift := c.strikes - 4
			if shift > 10 {
				shift = 10
			}
			d = cfg.TorRotateCooldown << shift
			if d > cfg.TorBackoffMax {
				d = cfg.TorBackoffMax
			}
		}
	case retryAfter > 0 && retryAfter <= cfg.CooldownMax:
		d = retryAfter
		if d > cfg.CooldownMax {
			d = cfg.CooldownMax
		}
	default:
		// Retry-After dài (luôn đếm tới nửa đêm UTC) không đáng tin: nghỉ ngắn rồi thăm dò lại. Hết lượt session thật sự
		// thì sessions.go chặn egress (và warp.go đổi IP cho WARP), không dựa vào cooldown này.
		if e.Kind == kindDirect && cfg.DirectCooldown > 0 {
			d = cfg.DirectCooldown
		} else {
			shift := c.strikes - 1
			if shift > 12 {
				shift = 12
			}
			d = 15 * time.Second << shift
			if d > cfg.CooldownMax {
				d = cfg.CooldownMax
			}
		}
	}
	c.until = time.Now().Add(d)
	return d
}

func (e *Egress) cooldownLeft(model string) time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c, ok := e.cool[model]; ok {
		if d := time.Until(c.until); d > 0 {
			return d
		}
	}
	return 0
}

// rotate chuyển Tor sang circuit/exit mới cho các kết nối mới (không giới hạn 10s như NEWNYM).
// Request đang chạy dở vẫn giữ circuit cũ cho tới khi xong. seen là thế hệ mà request đã dùng: nếu
// request khác đã xoay rồi (gen đã khác) thì không xoay chồng thêm.
func (e *Egress) rotate(seen uint64) {
	if !e.canRotate {
		return
	}
	if e.gen.CompareAndSwap(seen, seen+1) {
		log.Printf("[%s] đổi circuit (gen=%d)", e.Name, seen+1)
		if t, ok := e.Client().Transport.(*http.Transport); ok && cfg.UpstreamTLS == "bun" {
			t.CloseIdleConnections()
		}
	}
}

// ======================== MANAGER ========================

type Manager struct {
	mu       sync.RWMutex
	egresses []*Egress // egresses[0] luôn là direct (có thể bị vô hiệu bởi FORCE_TOR)
	rr       uint32

	dirMu  sync.Mutex
	dirWin map[string]*rateWindow

	torSpawned  int
	torSeq      int
	torStarting int
	ctx         context.Context
	wg          sync.WaitGroup
	proxyRefMu   sync.Mutex
	proxyRefLast time.Time

	// bộ điều phối (sched.go)
	wmu      sync.Mutex
	waitHigh []*waiter
	waitLow  []*waiter
	kick     chan struct{}
}

func newManager(ctx context.Context) *Manager {
	m := &Manager{ctx: ctx, dirWin: map[string]*rateWindow{}, kick: make(chan struct{}, 1)}
	direct := newEgress("direct", kindDirect, http.ProxyFromEnvironment)
	direct.daemon = "direct"
	direct.ready.Store(!cfg.ForceTor)
	initLimit(direct)
	m.egresses = []*Egress{direct}
	go m.dispatcher(ctx)
	return m
}

// warmDirect giữ sẵn kết nối TLS/HTTP2 tới upstream để request đầu tiên không phải trả phí bắt tay (không đụng API nên không tốn hạn mức).
func (m *Manager) warmDirect() {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		if d := m.egresses[0]; d.ready.Load() {
			ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
			if req, err := http.NewRequestWithContext(ctx, http.MethodHead, "https://opencode.ai/", nil); err == nil {
				if resp, err := d.Client().Do(req); err == nil {
					resp.Body.Close()
				}
			}
			cancel()
		}
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (m *Manager) snapshot() []*Egress {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.egresses
}

func (m *Manager) add(e *Egress) {
	initLimit(e)
	m.mu.Lock()
	m.egresses = append(m.egresses, e)
	m.mu.Unlock()
}

// addStatic nạp các proxy khai báo sẵn: TOR_INSTANCES (host:socks, phần :control nếu có sẽ bị bỏ qua) và PROXY_POOL (URL).
// addStatic nạp các proxy khai báo sẵn: TOR_INSTANCES (host:socks) và PROXY_POOL (URL).
func (m *Manager) addStatic(torInstances, proxyPool, resProxies, resRefresh string) {
	if !cfg.DisableTor {
		for i, item := range splitList(torInstances) {
			parts := strings.Split(item, ":")
			if len(parts) < 2 {
				continue
			}
			for _, e := range newTorGroup(fmt.Sprintf("tor-ext%d", i+1), parts[0]+":"+parts[1], cfg.TorIdentities) {
				e.ready.Store(true)
				m.add(e)
			}
		}
	}
	// Tier 1: Proxy dân cư xoay (Residential Proxies)
	resList := splitList(resProxies)
	refreshList := splitList(resRefresh)
	for i, p := range resList {
		u, err := url.Parse(p)
		if err != nil {
			log.Printf("[ProxyXoay] URL proxy không hợp lệ %q: %v", p, err)
			continue
		}
		e := newEgress(fmt.Sprintf("res%d", i+1), kindProxy, http.ProxyURL(u))
		e.tier = 1
		e.daemon = fmt.Sprintf("res%d", i+1)
		if i < len(refreshList) {
			e.refreshURL = refreshList[i]
		}
		e.ready.Store(true)
		m.add(e)
		log.Printf("[ProxyXoay] Tier 1 nạp proxy dân cư: %s (%s)", e.Name, p)
	}

	// Tier 2: Cụm proxy WARP
	for i, p := range splitList(proxyPool) {
		u, err := url.Parse(p)
		if err != nil {
			log.Printf("[Proxy] URL proxy không hợp lệ %q: %v", p, err)
			continue
		}
		e := newEgress(fmt.Sprintf("proxy%d", i+1), kindProxy, http.ProxyURL(u))
		e.tier = 2
		e.daemon = fmt.Sprintf("proxy%d", i+1)
		e.container = warpContainerFor(e, i)
		e.ready.Store(true)
		m.add(e)
		log.Printf("[ProxyWARP] Tier 2 nạp proxy WARP: %s (%s)", e.Name, p)
	}
}

var (
	resRotMu       sync.Mutex
	lastResRotTime = map[string]time.Time{}
)

func (m *Manager) rotateResidentialProxy(e *Egress) {
	if e.refreshURL == "" || e.expired.Load() {
		return
	}
	// Cờ riêng từng egress (không dùng cờ chung): lịch xoay lại của proxy này không bị proxy khác chặn mất.
	if !e.renewing.CompareAndSwap(false, true) {
		return
	}
	resRotMu.Lock()
	if time.Since(e.lastRotate) < 60*time.Second {
		resRotMu.Unlock()
		e.renewing.Store(false)
		return
	}
	e.lastRotate = time.Now()
	resRotMu.Unlock()

	go func() {
		defer e.renewing.Store(false)
		e.ready.Store(false)
		defer e.ready.Store(true)

		log.Printf("[ProxyXoay] Đang gọi API đổi IP so le cho %s...", e.Name)
		req, err := http.NewRequest(http.MethodGet, e.refreshURL, nil)
		if err != nil {
			return
		}
		client := &http.Client{Timeout: 10 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[ProxyXoay] Lỗi gọi API đổi IP cho %s: %v", e.Name, err)
			return
		}
		defer resp.Body.Close()
		var res struct {
			Status  int    `json:"status"`
			Message string `json:"message"`
		}
		if json.NewDecoder(resp.Body).Decode(&res) == nil {
			msgLower := strings.ToLower(res.Message)
			if res.Status == 102 || strings.Contains(msgLower, "khong ton tai") || strings.Contains(msgLower, "het han") {
				e.expired.Store(true)
				e.ready.Store(false)
				log.Printf("[ProxyXoay] Key của %s đã thực sự hết hạn (%s)! Tự động loại bỏ hoàn toàn khỏi routing, chuyển sang WARP.", e.Name, res.Message)
				return
			}
			if res.Status == 101 {
				// 101 là cooldown ("Con N s moi co the doi proxy"), KHÔNG PHẢI HẾT HẠN! Giữ IP hiện tại; nếu IP này đã hết
				// lượt session thì hẹn đổi lại ngay khi hết cooldown (không còn request nào đi qua để tự kích hoạt nữa).
				log.Printf("[ProxyXoay] %s đang trong cooldown đổi IP (%s), giữ nguyên IP hiện tại để phục vụ.", e.Name, res.Message)
				if cfg.SessionIDMode == "egress" && e.sess.exhausted() {
					wait := 60 * time.Second
					if n := firstInt(res.Message); n > 0 {
						wait = time.Duration(n+2) * time.Second
					}
					resRotMu.Lock()
					e.lastRotate = time.Time{}
					resRotMu.Unlock()
					time.AfterFunc(wait, func() {
						for !e.renewing.CompareAndSwap(false, false) { // đợi lượt xoay hiện tại kết thúc
							time.Sleep(time.Second)
						}
						m.rotateResidentialProxy(e)
					})
				}
				return
			}
			log.Printf("[ProxyXoay] %s đổi IP thành công (%s), chờ warmup 7s...", e.Name, res.Message)
		}
		warmup := cfg.ProxyWarmup
		if warmup <= 0 {
			warmup = 7 * time.Second
		}
		time.Sleep(warmup)
		e.clearAll()
		e.sess.reset() // IP mới => ngân sách session mới
		if t, ok := e.Client().Transport.(*http.Transport); ok {
			t.CloseIdleConnections()
		}
		m.kickSched()
		log.Printf("[ProxyXoay] %s đã sẵn sàng với IP dân cư mới!", e.Name)
	}()
}

func (m *Manager) refreshRotatingProxy(e *Egress) {
	if cfg.ProxyRefreshURL == "" {
		return
	}
	m.proxyRefMu.Lock()
	if time.Since(m.proxyRefLast) < 5*time.Second {
		m.proxyRefMu.Unlock()
		return
	}
	m.proxyRefLast = time.Now()
	m.proxyRefMu.Unlock()

	log.Printf("[Proxy] %s bị 429, đang gọi API đổi IP: %s", e.Name, cfg.ProxyRefreshURL)
	req, err := http.NewRequest(http.MethodGet, cfg.ProxyRefreshURL, nil)
	if err == nil {
		resp, rerr := http.DefaultClient.Do(req)
		if rerr == nil {
			resp.Body.Close()
			log.Printf("[Proxy] đổi IP thành công, chờ %v để proxy warmup...", cfg.ProxyWarmup)
			time.Sleep(cfg.ProxyWarmup)
		} else {
			log.Printf("[Proxy] gọi API đổi IP thất bại: %v", rerr)
		}
	}
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
var (
	renewMu       sync.Mutex
	isRenewing    atomic.Bool
	lastRenewTime = map[string]time.Time{}
)

func renewWarpProxy(egressName string) {
	var container string
	if egressName == "proxy1" {
		container = "warp-proxy"
	} else if strings.HasPrefix(egressName, "proxy") {
		num := strings.TrimPrefix(egressName, "proxy")
		container = "warp-proxy-" + num
	} else {
		return
	}

	// Chỉ cho phép 1 container đổi IP tại 1 thời điểm để 15 container còn lại luôn phục vụ 100% traffic
	if !isRenewing.CompareAndSwap(false, true) {
		return
	}

	renewMu.Lock()
	if t, ok := lastRenewTime[container]; ok && time.Since(t) < 60*time.Second {
		renewMu.Unlock()
		isRenewing.Store(false)
		return
	}
	lastRenewTime[container] = time.Now()
	renewMu.Unlock()

	time.AfterFunc(10*time.Second, func() {
		isRenewing.Store(false)
	})
	go func() {
		log.Printf("[WARP-Renew] Egress %s (%s) bị 429 quota cứng -> Tự động khởi động lại container để lấy IP Cloudflare mới...", egressName, container)
		tr := &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", "/var/run/docker.sock")
			},
		}
		client := &http.Client{Transport: tr, Timeout: 15 * time.Second}
		resp, err := client.Post("http://localhost/containers/"+container+"/restart", "application/json", nil)
		if err != nil {
			log.Printf("[WARP-Renew] Không gọi được docker restart qua socket: %v", err)
			return
		}
		resp.Body.Close()
		log.Printf("[WARP-Renew] %s đã restart thành công, IP Cloudflare WARP đã được làm mới!", container)
		if mgr != nil {
			for _, eg := range mgr.snapshot() {
				if eg.Name == egressName {
					eg.clearAll()
					break
				}
			}
		}
	}()
}


// pick chọn egress cho model: proxy dân cư trước, rồi WARP, direct làm chốt cuối (PREFER_DIRECT=true thì direct trước);
// mỗi egress không vượt trần in-flight thích ứng. nil nếu chưa có cái nào dùng được (request xếp hàng ở sched.go).
func (m *Manager) pick(model string) *Egress {
	return m.pickAvoid(model)
}

func (m *Manager) pickAvoid(model string, avoid ...string) *Egress {
	avoidMap := make(map[string]bool, len(avoid))
	for _, a := range avoid {
		if a != "" {
			avoidMap[a] = true
		}
	}
	list := m.snapshot()
	direct := list[0]
	if cfg.PreferDirect && !avoidMap[direct.daemon] && direct.available(model) && direct.inflight.Load() < direct.limit.Load() && direct.claim(model) {
		return direct
	}
	start := int(atomic.AddUint32(&m.rr, 1))

	// Dân cư trước, rồi WARP; mỗi egress không vượt trần in-flight thích ứng của nó (limit, xem sched.go).
	// Hết slot thì trả nil để request xếp hàng ở bộ điều phối thay vì dồn thêm vào egress đã đầy.
	if e := m.pickTierAvoid(list[1:], 1, model, avoidMap, start, -1); e != nil {
		return e
	}
	if e := m.pickTierAvoid(list[1:], 2, model, avoidMap, start, -1); e != nil {
		return e
	}
	if len(avoidMap) > 0 {
		if e := m.pickTierAvoid(list[1:], 1, model, nil, start, -1); e != nil {
			return e
		}
		if e := m.pickTierAvoid(list[1:], 2, model, nil, start, -1); e != nil {
			return e
		}
	}

	// Direct làm chốt chặn cuối
	if !cfg.PreferDirect && !avoidMap[direct.daemon] && direct.available(model) && direct.inflight.Load() < direct.limit.Load() && direct.claim(model) {
		return direct
	}
	return nil
}

func (m *Manager) pickTierAvoid(rest []*Egress, tier int, model string, avoidMap map[string]bool, start int, maxInflight int) *Egress {
	if len(rest) == 0 {
		return nil
	}
	var best *Egress
	var bestStrikes int
	var bestScore float64
	med := m.medianLatency()
	for i := range rest {
		e := rest[(start+i)%len(rest)]
		if e.tier != tier && (tier != 2 || e.tier != 0) {
			continue
		}
		if e.expired.Load() {
			continue
		}
		if len(avoidMap) > 0 && avoidMap[e.daemon] {
			continue
		}
		if maxInflight < 0 && e.inflight.Load() >= e.limit.Load() {
			continue // đã đạt trần thích ứng -> dàn sang egress khác
		}
		if maxInflight > 0 && int(e.inflight.Load()) >= maxInflight {
			continue
		}
		if !e.available(model) || (e.canRotate && int(e.inflight.Load()) >= cfg.TorMaxInflight) {
			continue
		}
		st := e.strikes(model)
		sc := e.loadScore(med)
		if best == nil || st < bestStrikes || (st == bestStrikes && sc < bestScore) {
			best, bestStrikes, bestScore = e, st, sc
		}
	}
	if best != nil && !best.claim(model) {
		return nil
	}
	return best
}

func (m *Manager) pickRest(rest []*Egress, model string) *Egress {
	return m.pickTierAvoid(rest, 2, model, nil, int(atomic.AddUint32(&m.rr, 1)), 0)
}

// loadScore: càng thấp càng nên chọn. Circuit chưa đủ mẫu coi như bằng trung vị (để được thử), mỗi request đang chạy
// cộng thêm ~25% chi phí để tải dàn đều chứ không dồn vào circuit nhanh nhất.
func (e *Egress) loadScore(median time.Duration) float64 {
	d, n := e.latency()
	if n < 3 {
		d = median
		if d == 0 {
			d = time.Second
		}
	}
	return float64(d) * (1 + 0.25*float64(e.inflight.Load()))
}

func (m *Manager) pickRestAvoid(rest []*Egress, model string, avoidMap map[string]bool) *Egress {
	if len(rest) == 0 {
		return nil
	}
	start := int(atomic.AddUint32(&m.rr, 1))
	var best *Egress
	var bestStrikes int
	var bestScore float64
	med := m.medianLatency()
	for i := range rest {
		e := rest[(start+i)%len(rest)]
		if len(avoidMap) > 0 && avoidMap[e.daemon] {
			continue
		}
		if !e.available(model) || (e.canRotate && int(e.inflight.Load()) >= cfg.TorMaxInflight) {
			continue
		}
		st := e.strikes(model)
		sc := e.loadScore(med)
		if best == nil || st < bestStrikes || (st == bestStrikes && sc < bestScore) {
			best, bestStrikes, bestScore = e, st, sc
		}
	}
	if best != nil && !best.claim(model) {
		return nil
	}
	return best
}
func isFreeModelInPool(model string) bool {
	for _, m := range freeModelPool {
		if m == model {
			return true
		}
	}
	return false
}

func (m *Manager) route(ctx context.Context, model string, isPool bool, deadline time.Time) (string, *Egress, *gwError) {
	return m.routeAvoid(ctx, model, isPool, deadline)
}

func (m *Manager) triggerEmergencyWarpRenew() {
	for _, eg := range m.snapshot() {
		if eg.Kind == kindProxy && strings.HasPrefix(eg.Name, "proxy") {
			eg.mu.Lock()
			hasCool := len(eg.cool) > 0
			eg.mu.Unlock()
			if hasCool {
				renewWarpProxy(eg.Name)
				return
			}
		}
	}
}

func (m *Manager) routeAvoid(ctx context.Context, model string, isPool bool, deadline time.Time, avoid ...string) (string, *Egress, *gwError) {
	for {
		if isPool {
			n := len(freeModelPool)
			start := int(atomic.AddUint32(&poolIndex, 1))
			for i := range n {
				cand := freeModelPool[(start+i)%n]
				if e := m.pickAvoid(cand, avoid...); e != nil {
					return cand, e, nil
				}
			}
		} else {
			if e := m.pickAvoid(model, avoid...); e != nil {
				return model, e, nil
			}
			if cfg.AutoModelFallback && isFreeModelInPool(model) {
				n := len(freeModelPool)
				start := int(atomic.AddUint32(&poolIndex, 1))
				for i := range n {
					cand := freeModelPool[(start+i)%n]
					if cand != model {
						if e := m.pickAvoid(cand, avoid...); e != nil {
							log.Printf("[Router] model=%s cạn quota trên mọi đường, tự động failover sang model=%s", model, cand)
							return cand, e, nil
						}
					}
				}
			}
		}

		m.growTor()

		models := []string{model}
		if isPool || (cfg.AutoModelFallback && isFreeModelInPool(model)) {
			models = freeModelPool
		}
		// Khi chỉ có 1 đường direct duy nhất (không có proxy pool để renew) và không có Tor: báo 429 nhanh.
		// Khi CÓ proxy pool (16-24 WARP proxies): KHÔNG ĐƯỢC NUỐT/VỨT BỎ REQUEST, kích hoạt renew IP khẩn cấp và giữ request chờ đường mới!
		if !m.canWaitForTor() && len(m.snapshot()) <= 1 && !m.anyUsableWithin(models, time.Until(deadline)) {
			return "", nil, &gwError{
				status:     http.StatusTooManyRequests,
				typ:        "rate_limit_error",
				msg:        "All egress paths are rate limited for this model. Retry later.",
				retryAfter: m.soonest(models),
			}
		}
		m.triggerEmergencyWarpRenew()
		// 2. Giữ request trong hàng đợi nội bộ, liên tục kiểm tra proxy vừa được renew cho đến khi có đường sống hoặc hết deadline (MaxWait 300s).
		if time.Now().After(deadline) {
			return "", nil, &gwError{
				status:     http.StatusTooManyRequests,
				typ:        "rate_limit_error",
				msg:        "All egress paths are rate limited. Max wait exceeded.",
				retryAfter: m.soonest(models),
			}
		}
		select {
		case <-ctx.Done():
			return "", nil, &gwError{status: 499, typ: "client_closed", msg: "client closed request"}
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// anyUsableWithin: có egress sẵn sàng nào dùng được ngay (kể cả đang bị request khác giữ chỗ thăm dò) hoặc hết cooldown
// trong khoảng `within` không. Nếu có thì nên chờ thay vì báo 429 (tránh báo nhầm khi cooldown vừa hết giữa lúc pick và kiểm tra).
func (m *Manager) anyUsableWithin(models []string, within time.Duration) bool {
	for _, e := range m.snapshot() {
		if !e.ready.Load() || e.expired.Load() {
			continue
		}
		if cfg.SessionIDMode == "egress" && !e.sess.readyWithin(within) {
			continue // IP đã hết lượt session và chưa tới lúc thăm dò lại
		}
		for _, mod := range models {
			if d := e.cooldownLeft(mod); d <= within {
				return true
			}
		}
	}
	return false
}

// soonest: thời gian ngắn nhất cho tới khi có 1 egress dùng lại được cho 1 trong các model.
func (m *Manager) soonest(models []string) time.Duration {
	best := time.Duration(0)
	for _, e := range m.snapshot() {
		if !e.ready.Load() {
			continue
		}
		for _, mod := range models {
			if d := e.cooldownLeft(mod); d > 0 && (best == 0 || d < best) {
				best = d
			}
		}
	}
	return best
}

// canWaitForTor: còn Tor đang khởi động, hoặc còn chỗ để bật thêm Tor.
func (m *Manager) canWaitForTor() bool {
	if !torAvailable {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.torStarting > 0 || m.torSpawned < cfg.TorMax
}

// ---------- Phát hiện "nguy hiểm" ở direct để bật Tor sẵn ----------

// rateWindow đếm số sự kiện trong 120 giây gần nhất (12 ô × 10 giây).
type rateWindow struct {
	buckets [12]int
	last    int64
}

func (w *rateWindow) add(nowSec int64) int {
	idx := nowSec / 10
	if steps := idx - w.last; steps > 0 {
		if steps >= 12 {
			w.buckets = [12]int{}
		} else {
			for i := int64(1); i <= steps; i++ {
				w.buckets[(w.last+i)%12] = 0
			}
		}
		w.last = idx
	}
	w.buckets[idx%12]++
	sum := 0
	for _, b := range w.buckets {
		sum += b
	}
	return sum
}

// noteDirect ghi nhận 1 request direct cho model và bật Tor sẵn nếu lưu lượng/đồng thời tiến gần ngưỡng nguy hiểm.
func (m *Manager) noteDirect(model string, directInflight int) {
	m.dirMu.Lock()
	w := m.dirWin[model]
	if w == nil {
		w = &rateWindow{}
		m.dirWin[model] = w
	}
	n := w.add(time.Now().Unix())
	m.dirMu.Unlock()
	if n >= cfg.DirectWarnReqs || directInflight >= cfg.DirectWarnInflight {
		m.prewarmTor("lưu lượng direct gần ngưỡng", 1)
	}
}

// prewarmTor đảm bảo có sẵn ít nhất `want` Tor đang chạy/khởi động (song song), chạy nền. Direct vừa 429 thì dựng 2 Tor
// cùng lúc để có chỗ chịu tải ngay khi bootstrap xong.
func (m *Manager) prewarmTor(reason string, want int) {
	if !torAvailable {
		return
	}
	if want > cfg.TorMax {
		want = cfg.TorMax
	}
	m.mu.RLock()
	have := m.torSpawned
	static := len(m.egresses) > 1 && m.torSpawned == 0
	m.mu.RUnlock()
	if static || have >= want {
		return
	}
	log.Printf("[Tor] bật %d Tor dự phòng (%s)", want-have, reason)
	for i := have; i < want; i++ {
		go m.growTorN(want)
	}
}

// reapIdleTor tắt Tor do gateway chạy mà không có request quá TorIdleStop.
func (m *Manager) reapIdleTor() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
		}
		for _, e := range m.snapshot() {
			if e.stop == nil || e.groupInflight() > 0 || !e.ready.Load() {
				continue
			}
			// Giữ lại tối thiểu TOR_WARM daemon để không bị cold-start (10-45s) khi người dùng quay lại
			m.mu.RLock()
			spawned := m.torSpawned
			m.mu.RUnlock()
			if spawned <= cfg.TorWarm {
				continue
			}
			if idle := time.Since(time.Unix(0, e.groupLastUsed())); idle > cfg.TorIdleStop {
				log.Printf("[%s] nhàn rỗi %v, tắt để nhường RAM", e.Name, idle.Round(time.Second))
				m.removeTor(e)
			}
		}
	}
}

func (m *Manager) removeTor(e *Egress) {
	e.setGroupReady(false)
	gone := map[*Egress]bool{e: true}
	for _, x := range e.members {
		gone[x] = true
	}
	m.mu.Lock()
	var keep []*Egress
	for _, x := range m.egresses {
		if !gone[x] {
			keep = append(keep, x)
		}
	}
	m.egresses = keep
	if m.torSpawned > 0 {
		m.torSpawned--
	}
	m.mu.Unlock()
	if e.stop != nil {
		e.stop()
	}
	if e.dataDir != "" {
		_ = os.RemoveAll(e.dataDir)
	}
}

// firstInt lấy số nguyên đầu tiên trong chuỗi (vd "Con 37s moi co the doi proxy" -> 37).
func firstInt(s string) int {
	n, seen := 0, false
	for _, r := range s {
		if r >= '0' && r <= '9' {
			n, seen = n*10+int(r-'0'), true
		} else if seen {
			break
		}
	}
	return n
}

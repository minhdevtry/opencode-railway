package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ======================== ĐỔI DANH TÍNH WARP ========================
//
// Khi 1 egress WARP hết lượt mở session mới trong ngày (IP đó đã đủ ~50 cặp IP×session), đăng ký lại thiết bị WARP
// ngay trong container (`warp-cli registration new`, ~10-15s, không restart) để Cloudflare cấp IP thoát mới, tức
// hạn mức mới. Gọi qua Docker Engine API trên /var/run/docker.sock (container gateway đã mount sẵn).

var renewSem chan struct{}

func warpContainerFor(e *Egress, index int) string {
	if list := splitList(cfg.WarpContainers); index < len(list) {
		return list[index]
	}
	if index == 0 {
		return "warp-proxy"
	}
	return fmt.Sprintf("warp-proxy-%d", index+1)
}

var dockerHTTP = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
	DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", "/var/run/docker.sock")
	},
	MaxIdleConns:    4,
	IdleConnTimeout: 30 * time.Second,
}}

func dockerClient() *http.Client { return dockerHTTP }

// dockerExec chạy lệnh trong container và trả về output (stdout+stderr gộp, đã bỏ header multiplex).
func dockerExec(container string, cmd ...string) (string, error) {
	c := dockerClient()
	body, _ := json.Marshal(map[string]any{"AttachStdout": true, "AttachStderr": true, "Cmd": cmd})
	resp, err := c.Post("http://docker/containers/"+container+"/exec", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	var created struct {
		ID      string `json:"Id"`
		Message string `json:"message"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if created.ID == "" {
		return "", fmt.Errorf("docker exec create: %s", created.Message)
	}
	resp, err = c.Post("http://docker/exec/"+created.ID+"/start", "application/json", strings.NewReader(`{"Detach":false,"Tty":false}`))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	// Bỏ header 8 byte của mỗi frame (stream multiplex của Docker).
	var out bytes.Buffer
	for len(raw) >= 8 {
		n := int(raw[4])<<24 | int(raw[5])<<16 | int(raw[6])<<8 | int(raw[7])
		raw = raw[8:]
		if n > len(raw) {
			n = len(raw)
		}
		out.Write(raw[:n])
		raw = raw[n:]
	}
	return out.String(), nil
}

// renewIdentity đổi danh tính WARP của egress (bất đồng bộ, mỗi egress 1 lần tại 1 thời điểm, giới hạn số container
// đổi cùng lúc). Egress không phải WARP thì bỏ qua (proxy dân cư có cơ chế xoay riêng).
func (m *Manager) renewIdentity(e *Egress) {
	m.renewIdentityFor(e, "")
}

// renewIdentityFor: đổi danh tính WARP, và nếu probeModel khác rỗng thì dùng model ĐÓ để kiểm tra
// IP mới còn quota hay không (thay vì model mục tiêu). Cần thiết vì quota là per-(IP, model):
// IP mới có thể còn quota cho beam nhưng vẫn cạn cho glm (đo 2026-11-03).
func (m *Manager) renewIdentityFor(e *Egress, probeModel string) {
	if e.container == "" {
		return
	}
	if time.Since(time.Unix(0, e.lastIdentityChange.Load())) < 45*time.Second {
		return // giới hạn tần suất: mỗi egress tối đa 1 lần / 45s
	}
	m.renewIdentityNow(e, probeModel)
}

// renewIdentityNow: xoay danh tính NGAY (không qua guard tần suất). Dùng cho:
//   - vòng "đổi tiếp" khi IP vừa xoay vẫn rơi vào /64 đã cạn;
//   - nơi gọi đã tự kiểm tra tần suất riêng.
// Lưu ý: KHÔNG set lastIdentityChange ở nơi gọi, vì hàm này sẽ tự set -> sẽ tự chặn chính nó.
func (m *Manager) renewIdentityNow(e *Egress, probeModel string) {
	if e.container == "" {
		return
	}
	if m.anyRenewing() {
		return // bảo vệ: chỉ cho phép tối đa 1 container xoay tại 1 thời điểm, các container khác phải ở lại phục vụ
	}
	if !e.renewing.CompareAndSwap(false, true) {
		return
	}
	e.lastIdentityChange.Store(time.Now().UnixNano())
	go func() {
		defer e.renewing.Store(false)
		t0 := time.Now()
		renewSem <- struct{}{}
		defer func() { <-renewSem }()
		e.ready.Store(false)
		defer e.ready.Store(true)

		out, err := dockerExec(e.container, "sh", "-c",
			"warp-cli --accept-tos registration delete >/dev/null 2>&1; warp-cli --accept-tos registration new && warp-cli --accept-tos connect")
		e.renewTries.Add(1)
		if err != nil || !strings.Contains(out, "Success") {
			log.Printf("[WARP] %s (%s) đổi danh tính thất bại: %v %q", e.Name, e.container, err, strings.TrimSpace(out))
			time.Sleep(30 * time.Second) // nghỉ sau khi đã trả slot, không chặn container khác
			return
		}
		// Chờ tunnel lên lại: thử kết nối tới upstream qua chính egress này.
		for range 30 {
			time.Sleep(time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://opencode.ai/cdn-cgi/trace", nil)
			resp, err := e.Client().Do(req)
			if err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				cancel()
				ip := ""
				for _, l := range strings.Split(string(b), "\n") {
					if strings.HasPrefix(l, "ip=") {
						ip = l[3:]
					}
				}
				// IPv4 của WARP (104.28.x) dùng chung cho mọi người dùng WARP trong vùng nên gần như luôn hết lượt:
				// đổi tiếp (tối đa 4 lần liên tiếp) để lấy IPv6 riêng.
				if ip != "" && !strings.Contains(ip, ":") && e.renewTries.Load() < 5 {
					log.Printf("[WARP] %s nhận IPv4 dùng chung %s, đổi tiếp", e.Name, ip)
					go func() { time.Sleep(2 * time.Second); m.renewIdentityNow(e, probeModel) }()
					return
				}

				// Kiểm tra IPv6 mới có còn quota cho model mục tiêu không (50% IP WARP chết sẵn 429 do chung /64)
				target := probeModel
				if target == "" {
					target = targetModelName()
				}
				// Hạn mức upstream tính theo DẢI /64: nếu /64 mới rơi vào đúng dải đã biết cạn thì xoay tiếp
				// (đo 2026-11-03: 2a09:bac1:7aa0:50:: OK, 2a09:bac1:7a80:50:: 429 cho cùng model).
				prefix := ipPrefixOf(ip)
				if prefixDead(target, prefix) && e.renewTries.Load() < 6 {
					log.Printf("[WARP] %s nhận IP %s thuộc /64 %s đã biết cạn cho model %s, đổi tiếp (lần %d)",
						e.Name, ip, prefix, target, e.renewTries.Load())
					go func() { time.Sleep(2 * time.Second); m.renewIdentityNow(e, target) }()
					return
				}
				if !m.probeEgressModel(e, target) && e.renewTries.Load() < 4 {
					log.Printf("[WARP] %s nhận IPv6 %s nhưng model %s đã cạn sẵn (429), đổi tiếp (lần %d)",
						e.Name, ip, target, e.renewTries.Load())
					go func() { time.Sleep(2 * time.Second); m.renewIdentityNow(e, target) }()
					return
				}

				e.renewTries.Store(0)
				e.renews.Add(1)
				e.ipAddr.Store(ip)
				e.setIPPrefix(ip)
				if strings.Contains(ip, ":") {
					e.ipKind.Store(ipKindOwn)
				} else {
					e.ipKind.Store(ipKindShared)
				}
				e.sess.reset()
				e.markIPChanged() // IP mới tinh: nếu IP này cũng bị cạn hạn mức thì là cạn cấp tài khoản
				e.clearAll()
				if t, ok := e.Client().Transport.(*http.Transport); ok {
					t.CloseIdleConnections()
				}
				log.Printf("[WARP] %s (%s) đã đổi danh tính sau %v, IP mới %s (đã xác thực model %s)",
					e.Name, e.container, time.Since(t0).Round(time.Second), ip, target)
				return
			}
			cancel()
		}
		log.Printf("[WARP] %s (%s) đổi danh tính xong nhưng tunnel chưa lên sau 30s", e.Name, e.container)
	}()
}

// probeEgressModel gửi 1 request siêu nhẹ (1 token) với model mục tiêu để kiểm tra xem IPv6 mới có bị cạn sẵn (FreeUsageLimitError) không.
func (m *Manager) probeEgressModel(e *Egress, model string) bool {
	if model == "" {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	body := map[string]any{
		"model": model,
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		},
		"max_tokens": 1,
	}
	applyFingerprint(body, model, true, stubDescription)
	payload, err := jsonMarshalNoEscape(body)
	if err != nil {
		return true
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, infURL+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return true
	}
	setUpstreamHeaders(req, "*/*", "probe", nil)

	resp, err := e.Client().Do(req)
	if err != nil {
		return true // lỗi mạng tạm thời hoặc timeout probe -> không chặn egress
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if bytes.Contains(snippet, []byte("FreeUsageLimitError")) || bytes.Contains(snippet, []byte("Free usage limit")) {
			return false // Cạn quota thật sự
		}
	}
	return true
}

// warpJanitor: định kỳ đổi danh tính cho egress WARP đang không phục vụ được (vd bị chặn giữa lúc không có traffic
// đi qua để tự kích hoạt), để lúc nào cũng có nhiều IP sạch nhất có thể.
func (m *Manager) warpJanitor(ctx context.Context) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if cfg.SessionIDMode != "egress" {
			continue
		}
		if acctGate.capped() {
			// Quota cạn cấp tài khoản: đừng đổi IP liên tục (vô ích). Cứ 5 phút thì đổi ĐÚNG 1 danh tính để có 1 IP
			// mới thật sự: nếu IP mới đó cũng bị cạn thì đó là bằng chứng cạn cấp tài khoản; nếu IP mới trả 200 thì
			// quota đã hồi và breaker mở lại. Không có bước này thì breaker không bao giờ tự mở được (vì lúc capped
			// không có IP nào được đổi nên không có "IP mới" để kiểm chứng).
			if time.Since(m.lastProbeRenew) > 5*time.Minute {
				m.lastProbeRenew = time.Now()
				for _, e := range m.snapshot() {
					if e.container != "" && e.ready.Load() && !e.renewing.Load() && !e.sess.usable() {
						m.renewIdentity(e)
						break
					}
				}
			}
			m.probeAccount() // thăm dò để gia hạn bằng chứng / phát hiện quota hồi
			continue
		}
		// 1. Model mục tiêu: xoay gối đầu (Pipeline Rotation)
		// - Nếu đã cạn trên MỌI IP (usable == 0) -> xoay 1 container để hồi phục.
		// - Nếu đã cạn trên egress này nhưng các egress khác vẫn còn (usable > 0) -> xoay gối đầu (pipeline rotation)
		//   khi không có container nào khác đang xoay.
		target := targetModelName()
		targetUsable := modelUsableEgresses(target, m.usableEgresses())
		if targetUsable == 0 || !m.anyRenewing() {
			for _, e := range m.snapshot() {
				if e.container == "" || !e.ready.Load() || e.renewing.Load() {
					continue
				}
				if time.Since(time.Unix(0, e.lastIdentityChange.Load())) < 30*time.Second {
					continue
				}
				if egressExhaustedOn(target, e.Name) {
					log.Printf("[WARP] %s xoay danh tính gối đầu cho model mục tiêu %s (usable=%d)", e.Name, target, targetUsable)
					m.renewIdentityFor(e, target)
					break // xoay từng container một
				}
			}
		}

		// 1b. Xoay gối đầu theo CẶP (model, egress): nếu 1 egress đã cạn hạn mức riêng cho model M
		// (đốt hết quota của IP đó cho M) nhưng M vẫn còn egress khác phục vụ được, thì xoay IP của
		// egress đó để lấy quota mới cho M. Không làm vậy thì 1 IP "chết" với 1 model sẽ bị bỏ hoang
		// suốt ngày (đo 2026-11-03: proxy1/proxy4 còn phục vụ được beam+nemotron nhưng 429 với
		// deepseek/glm/mimo -> 3/5 model mất 50% sức chứa).
		poolForPair := poolSnapshot()
		for _, model := range poolForPair {
			if modelUsableEgresses(model, m.usableEgresses()) == 0 {
				continue // model cạn khắp mọi egress -> xử lý ở mục 2
			}
			for _, e := range m.snapshot() {
				if e.container == "" || !e.ready.Load() || e.renewing.Load() {
					continue
				}
				if time.Since(time.Unix(0, e.lastIdentityChange.Load())) < 30*time.Second {
					continue
				}
				if egressExhaustedOn(model, e.Name) {
					log.Printf("[WARP] %s xoay danh tính để lấy quota mới cho model %s (IP này đã cạn riêng model này)", e.Name, model)
					m.renewIdentityFor(e, model)
					break
				}
			}
		}

		// 2. Các model khác trong pool đã cạn trên MỌI IP -> xoay 1 danh tính
		pool := poolSnapshot()
		for _, model := range pool {
			if model == target || modelUsableEgresses(model, m.usableEgresses()) != 0 {
				continue // target đã xử lý ở trên, hoặc model còn IP có ngân sách
			}
			for _, e := range m.snapshot() {
				if e.container == "" || !e.ready.Load() || e.renewing.Load() {
					continue
				}
				if time.Since(time.Unix(0, e.lastIdentityChange.Load())) < 30*time.Second {
					continue // giới hạn tần suất: không xoay liên tục
				}
				log.Printf("[WARP] %s xoay danh tính để lấy IP mới cho model %s (đã cạn trên mọi IP)", e.Name, model)
				m.renewIdentityFor(e, model)
				break // mỗi model 1 lần mỗi vòng
			}
		}
		for _, e := range m.snapshot() {
			if e.container != "" && e.ready.Load() && !e.renewing.Load() && e.sess.exhausted() {
				if time.Since(time.Unix(0, e.lastIdentityChange.Load())) < 30*time.Second {
					continue
				}
				log.Printf("[WARP] %s không còn session dùng được, đổi danh tính", e.Name)
				m.renewIdentityFor(e, "")
				break
			}
			// IP này đã hết ngân sách session mới trong ngày: IP chết tới 00:00 UTC, phải đổi IP để có ngân sách mới.
			// Không đổi thì egress đó nằm không cho tới hết ngày.
			if e.tier == 1 && e.refreshURL != "" && e.ready.Load() && !e.renewing.Load() && !e.sess.usable() {
				log.Printf("[%s] hết ngân sách session của IP này, xoay IP để lấy ngân sách mới", e.Name)
				m.rotateResidentialProxy(e)
			}
		}
	}
}

// checkResidentialLiveness kiểm tra proxy dân cư ngay lúc khởi động: proxy chết (IP hỏng / key hết hạn) thì
// mỗi lần thử tốn 20-40 giây chờ timeout -> request đầu tiên của client sẽ treo. Kiểm tra sớm rồi loại luôn.
func (m *Manager) checkResidentialLiveness(ctx context.Context) {
	for _, e := range m.snapshot() {
		if e.tier != 1 || e.proxyAddr == "" {
			continue
		}
		if e.expired.Load() {
			continue
		}
		// Phải thử một request THẬT qua proxy: proxy hết hạn vẫn mở cổng TCP (dial thành công) nhưng không
		// chuyển được traffic -> dial không đủ để kết luận sống/chết.
		c, cancel := context.WithTimeout(ctx, 12*time.Second)
		req, _ := http.NewRequestWithContext(c, http.MethodGet, "https://opencode.ai/cdn-cgi/trace", nil)
		resp, err := e.Client().Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		cancel()
		if err == nil {
			e.ready.Store(true) // còn sống -> dùng bình thường
			continue
		}
		log.Printf("[%s] proxy không phản hồi lúc khởi động (%v) -> loại khỏi routing và xoay IP", e.Name, err)
		e.ready.Store(false)
		if !m.rotateResidentialProxy(e) {
			time.AfterFunc(45*time.Second, func() {
				if !e.expired.Load() {
					e.ready.Store(true)
					m.rotateResidentialProxy(e)
				}
			})
		}
	}
}

// detectWarpIPs dò IP thoát thật của từng container WARP lúc khởi động: IPv6 riêng hay IPv4 dùng chung.
// Cần biết vì hạn mức phía upstream tính theo IP: 9 container dùng chung 1 IPv4 thì hạn mức cũng chung.
func (m *Manager) detectWarpIPs(ctx context.Context) {
	// SONG SONG: dò tuần tự 36 egress × tới 8s mỗi cái = mất mấy phút, trong lúc đó gateway chưa biết egress nào
	// có IP riêng (nên không ưu tiên đúng) và các container chỉ có IPv4 chung vẫn được dùng.
	var wg sync.WaitGroup
	for _, e := range m.snapshot() {
		if e.container == "" || !e.ready.Load() {
			continue
		}
		wg.Add(1)
		go func(e *Egress) {
			defer wg.Done()
			m.detectWarpIP(ctx, e)
		}(e)
	}
	wg.Wait()
}

func (m *Manager) detectWarpIP(ctx context.Context, e *Egress) {
	{
		ip := m.exitIPOf(ctx, e)
		switch {
		case ip == "":
		case strings.Contains(ip, ":"):
			e.ipAddr.Store(ip)
			e.setIPPrefix(ip)
			e.ipKind.Store(ipKindOwn)
			log.Printf("[WARP] %s có IPv6 riêng %s (/64 %s)", e.Name, ip, ipPrefixOf(ip))
		default:
			e.ipAddr.Store(ip)
			e.setIPPrefix(ip)
			e.ipKind.Store(ipKindShared)
			log.Printf("[WARP] %s đang dùng IPv4 DÙNG CHUNG %s (hạn mức upstream cũng dùng chung) - sẽ đổi danh tính để lấy IPv6 riêng", e.Name, ip)
		}
	}
}

// exitIPOf hỏi upstream IP mà container này nhìn thấy (dùng domain upstream để đúng họ IP).
func (m *Manager) exitIPOf(ctx context.Context, e *Egress) string {
	c, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(c, http.MethodGet, "https://opencode.ai/cdn-cgi/trace", nil)
	resp, err := e.Client().Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "ip=") {
			return l[3:]
		}
	}
	return ""
}

// probeAccount gửi 1 request rất nhẹ để xem quota tài khoản đã hồi chưa.
// Mục đích chính là GIA HẠN bằng chứng (mỗi lần probe vẫn thấy cạn thì breaker giữ nguyên), chứ không phải để
// mở breaker: 200 của prober có thể là 200 của session CŨ (session cũ vẫn chạy được khi quota cạn một phần).
func (m *Manager) probeAccount() {
	pool := poolSnapshot()
	if len(pool) == 0 {
		return // pool rỗng (mọi model đều bị loại): không có gì để thăm dò
	}
	// Thăm dò là 1 request thật lên upstream (tốn quota) -> không thăm dò quá dày. Janitor quét mỗi 20s.
	if time.Since(m.lastProbe) < 2*time.Minute {
		return
	}
	m.lastProbe = time.Now()
	for _, e := range m.snapshot() {
		if !e.ready.Load() || e.expired.Load() || e.container == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		up, gerr := forward(ctx, pool[0], true, infURL+"/chat/completions", "*/*", "acct-probe", func(mod string) []byte {
			body := map[string]any{"model": mod, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1}
			applyFingerprint(body, mod, true, stubDescription)
			b, _ := jsonMarshalNoEscape(body)
			return b
		})
		if gerr == nil && up != nil {
			up.close()
		}
		cancel()
		// KHÔNG tự mở breaker ở đây: 200 của prober có thể là 200 của session CŨ (session cũ vẫn chạy được khi
		// quota cạn một phần) -> không chứng minh quota đã hồi. Chỉ request 200 trên IP MỚI mới mở (upstream.go).
		return
	}
}

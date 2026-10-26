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
	if e.container == "" || !e.renewing.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer e.renewing.Store(false)
		e.ready.Store(false)
		defer e.ready.Store(true)
		t0 := time.Now()
		renewSem <- struct{}{}
		out, err := dockerExec(e.container, "sh", "-c",
			"warp-cli --accept-tos registration delete >/dev/null 2>&1; warp-cli --accept-tos registration new && warp-cli --accept-tos connect")
		<-renewSem
		e.renewTries.Add(1)
		if err != nil || !strings.Contains(out, "Success") {
			log.Printf("[WARP] %s (%s) đổi danh tính thất bại: %v %q", e.Name, e.container, err, strings.TrimSpace(out))
			time.Sleep(30 * time.Second) // nghỉ sau khi đã trả slot, không chặn container khác
			return
		}
		// Chờ tunnel lên lại: thử kết nối tới upstream qua chính egress này.
		for i := 0; i < 30; i++ {
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
				// đổi tiếp (tối đa 3 lần liên tiếp) để lấy IPv6 riêng.
				if ip != "" && !strings.Contains(ip, ":") && e.renewTries.Load() < 5 {
					log.Printf("[WARP] %s nhận IPv4 dùng chung %s, đổi tiếp", e.Name, ip)
					go func() { time.Sleep(2 * time.Second); m.renewIdentity(e) }()
					return
				}
				e.renewTries.Store(0)
				e.renews.Add(1)
				e.ipAddr.Store(ip)
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
				log.Printf("[WARP] %s (%s) đã đổi danh tính sau %v, IP mới %s", e.Name, e.container, time.Since(t0).Round(time.Second), ip)
				return
			}
			cancel()
		}
		log.Printf("[WARP] %s (%s) đổi danh tính xong nhưng tunnel chưa lên sau 30s", e.Name, e.container)
	}()
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
		// Model đã cạn trên MỌI IP -> xoay 1 danh tính để lấy IP mới cho model đó. WARP luôn có thể đổi danh tính
		// (mỗi lần ~6-13s) và IPv6 mới có ngân sách riêng: cứ đổi tới khi được IP còn hạn, rồi dùng IP đó tới
		// hết hạn rồi lại đổi tiếp.
		pool := poolSnapshot()
		for _, model := range pool {
			if modelUsableEgresses(model, len(m.snapshot())) != 0 {
				continue // model này còn IP nào có ngân sách -> không cần xoay
			}
			for _, e := range m.snapshot() {
				if e.container == "" || !e.ready.Load() || e.renewing.Load() {
					continue
				}
				if time.Since(e.lastIdentityChange) < 30*time.Second {
					continue // giới hạn tần suất: không xoay liên tục
				}
				log.Printf("[WARP] %s xoay danh tính để lấy IP mới cho model %s (đã cạn trên mọi IP)", e.Name, model)
				e.lastIdentityChange = time.Now()
				m.renewIdentity(e)
				break // mỗi model 1 lần mỗi vòng
			}
		}
		for _, e := range m.snapshot() {
			if e.container != "" && e.ready.Load() && !e.renewing.Load() && !e.sess.usable() {
				log.Printf("[WARP] %s không còn session dùng được, đổi danh tính", e.Name)
				m.renewIdentity(e)
				continue
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
	for _, e := range m.snapshot() {
		if e.container == "" || !e.ready.Load() {
			continue
		}
		ip := m.exitIPOf(ctx, e)
		switch {
		case ip == "":
		case strings.Contains(ip, ":"):
			e.ipAddr.Store(ip)
			e.ipKind.Store(ipKindOwn)
			log.Printf("[WARP] %s có IPv6 riêng %s", e.Name, ip)
		default:
			e.ipAddr.Store(ip)
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

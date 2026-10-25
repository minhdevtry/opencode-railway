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
				if ip != "" && !strings.Contains(ip, ":") && e.renewTries.Load() < 3 {
					log.Printf("[WARP] %s nhận IPv4 dùng chung %s, đổi tiếp", e.Name, ip)
					go func() { time.Sleep(2 * time.Second); m.renewIdentity(e) }()
					return
				}
				e.renewTries.Store(0)
				e.renews.Add(1)
				e.sess.reset()
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
		if acctGate.tripped() {
			m.probeAccount() // quota cạn cấp tài khoản: đừng đổi IP (vô ích), chỉ thăm dò để tự mở lại khi hồi
			continue
		}
		for _, e := range m.snapshot() {
			if e.container != "" && e.ready.Load() && !e.renewing.Load() && !e.sess.usable() {
				log.Printf("[WARP] %s không còn session dùng được, đổi danh tính", e.Name)
				m.renewIdentity(e)
			}
		}
	}
}

// probeAccount gửi 1 request rất nhẹ qua 1 egress để xem quota tài khoản đã hồi chưa (acctGate.clear() tự gọi khi 200).
func (m *Manager) probeAccount() {
	for _, e := range m.snapshot() {
		if !e.ready.Load() || e.expired.Load() || e.container == "" {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		up, gerr := forward(ctx, freeModelPool[0], true, infURL+"/chat/completions", "*/*", "acct-probe", func(mod string) []byte {
			body := map[string]any{"model": mod, "messages": []any{map[string]any{"role": "user", "content": "hi"}}, "max_tokens": 1}
			applyFingerprint(body, mod, true, stubDescription)
			b, _ := jsonMarshalNoEscape(body)
			return b
		})
		if gerr == nil && up != nil {
			up.close()
		}
		cancel()
		return
	}
}

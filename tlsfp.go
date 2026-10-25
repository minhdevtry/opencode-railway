package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	utls "github.com/refraction-networking/utls"
)

// ======================== TLS FINGERPRINT (BUN) ========================
//
// Upstream (Cloudflare Worker của opencode) phân loại request theo dấu vân tay TLS: cùng một chuỗi byte request,
// gửi bằng Bun (runtime của opencode CLI) thì được tính vào quota "client thật", gửi bằng Go crypto/tls, curl hay
// mitmproxy thì rơi vào quota chung đã cạn (429 FreeUsageLimitError). Đã đo 2026-10-24: Go + uTLS phát lại
// ClientHello thật của Bun 1.4 => 200, kể cả qua WARP SOCKS5. Xem research/README.md.

//go:embed bun_clienthello.bin
var bunClientHello []byte

// helloSpec dựng ClientHelloSpec mới từ ClientHello đã bắt (mỗi kết nối 1 bản, vì spec giữ state của extension).
func helloSpec() (*utls.ClientHelloSpec, error) {
	fp := utls.Fingerprinter{AllowBluntMimicry: true}
	spec, err := fp.FingerprintClientHello(bunClientHello)
	if err != nil {
		return nil, err
	}
	// Bun chỉ nói HTTP/1.1 với upstream; giữ đúng như vậy để Transport không cố lên h2.
	for _, ext := range spec.Extensions {
		if alpn, ok := ext.(*utls.ALPNExtension); ok {
			alpn.AlpnProtocols = []string{"http/1.1"}
		}
	}
	return spec, nil
}

// dialVia mở kết nối TCP tới addr, đi thẳng hoặc qua proxy. Proxy dân cư hay reset ngay ở bước CONNECT (~1/3 lần đo
// được) nên thử lại tối đa 3 lần trước khi coi là lỗi egress.
func dialVia(ctx context.Context, proxy *url.URL, addr string) (net.Conn, error) {
	var err error
	for i := 0; i < 3; i++ {
		var c net.Conn
		if c, err = dialOnce(ctx, proxy, addr); err == nil || proxy == nil || ctx.Err() != nil {
			return c, err
		}
		time.Sleep(time.Duration(100*(i+1)) * time.Millisecond)
	}
	return nil, err
}

func dialOnce(ctx context.Context, proxy *url.URL, addr string) (net.Conn, error) {
	d := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	if proxy == nil {
		return d.DialContext(ctx, "tcp", addr)
	}
	conn, err := d.DialContext(ctx, "tcp", proxy.Host)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	}
	switch proxy.Scheme {
	case "socks5", "socks5h", "socks":
		err = socks5Connect(conn, proxy.User, addr)
	case "http", "https":
		err = httpConnect(conn, proxy, addr)
	default:
		err = fmt.Errorf("proxy scheme %q không hỗ trợ", proxy.Scheme)
	}
	if err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func socks5Connect(c net.Conn, user *url.Userinfo, addr string) error {
	host, portS, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	port, _ := strconv.Atoi(portS)
	methods := []byte{0x00}
	if user != nil {
		methods = []byte{0x02, 0x00}
	}
	if _, err := c.Write(append([]byte{0x05, byte(len(methods))}, methods...)); err != nil {
		return err
	}
	var rep [2]byte
	if _, err := io.ReadFull(c, rep[:]); err != nil {
		return err
	}
	if rep[0] != 0x05 {
		return errors.New("socks5: phản hồi sai phiên bản")
	}
	switch rep[1] {
	case 0x00:
	case 0x02:
		u := user.Username()
		p, _ := user.Password()
		msg := append([]byte{0x01, byte(len(u))}, u...)
		msg = append(append(msg, byte(len(p))), p...)
		if _, err := c.Write(msg); err != nil {
			return err
		}
		if _, err := io.ReadFull(c, rep[:]); err != nil {
			return err
		}
		if rep[1] != 0x00 {
			return errors.New("socks5: xác thực thất bại")
		}
	default:
		return errors.New("socks5: không có phương thức xác thực phù hợp")
	}
	// Luôn gửi hostname (proxy tự phân giải): WARP phân giải ra IPv6 nên mỗi container có IP riêng.
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, host...)
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := c.Write(req); err != nil {
		return err
	}
	var hdr [4]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return err
	}
	if hdr[1] != 0x00 {
		return fmt.Errorf("socks5: CONNECT bị từ chối (mã %d)", hdr[1])
	}
	var skip int
	switch hdr[3] {
	case 0x01:
		skip = 4
	case 0x04:
		skip = 16
	case 0x03:
		var l [1]byte
		if _, err := io.ReadFull(c, l[:]); err != nil {
			return err
		}
		skip = int(l[0])
	}
	_, err = io.ReadFull(c, make([]byte, skip+2))
	return err
}

func httpConnect(c net.Conn, proxy *url.URL, addr string) error {
	req := &http.Request{Method: http.MethodConnect, URL: &url.URL{Opaque: addr}, Host: addr, Header: http.Header{}}
	if proxy.User != nil {
		p, _ := proxy.User.Password()
		req.SetBasicAuth(proxy.User.Username(), p)
		req.Header.Set("Proxy-Authorization", req.Header.Get("Authorization"))
		req.Header.Del("Authorization")
	}
	if err := req.Write(c); err != nil {
		return err
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("http proxy CONNECT: %s", resp.Status)
	}
	return nil
}

var tlsModeLogged sync.Once

// newFingerprintTransport: Transport HTTP/1.1 với TLS giả lập Bun, tự lo cả phần đi qua proxy (Transport.Proxy
// không dùng được vì Go sẽ tự bắt tay TLS bằng crypto/tls sau khi qua proxy).
func newFingerprintTransport(proxy func(*http.Request) (*url.URL, error)) *http.Transport {
	tlsModeLogged.Do(func() {
		if _, err := helloSpec(); err != nil {
			log.Printf("[TLS] Không dựng được ClientHello Bun: %v", err)
		} else {
			log.Printf("[TLS] Upstream dùng TLS fingerprint Bun (uTLS)")
		}
	})
	probe, _ := http.NewRequest(http.MethodGet, "https://opencode.ai/", nil)
	return &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var pu *url.URL
			if proxy != nil {
				var err error
				if pu, err = proxy(probe); err != nil {
					return nil, err
				}
			}
			raw, err := dialVia(ctx, pu, addr)
			if err != nil {
				return nil, err
			}
			host, _, _ := net.SplitHostPort(addr)
			spec, err := helloSpec()
			if err != nil {
				raw.Close()
				return nil, err
			}
			uc := utls.UClient(raw, &utls.Config{ServerName: host}, utls.HelloCustom)
			if err := uc.ApplyPreset(spec); err != nil {
				raw.Close()
				return nil, err
			}
			hctx, cancel := context.WithTimeout(ctx, 25*time.Second)
			defer cancel()
			if err := uc.HandshakeContext(hctx); err != nil {
				raw.Close()
				return nil, err
			}
			return uc, nil
		},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var pu *url.URL
			if proxy != nil {
				pu, _ = proxy(probe)
			}
			return dialVia(ctx, pu, addr)
		},
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          4096,
		MaxIdleConnsPerHost:   1024,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 180 * time.Second, // không đặt Client.Timeout để stream dài không bị cắt
		ExpectContinueTimeout: 1 * time.Second,
		WriteBufferSize:       64 * 1024,
		ReadBufferSize:        64 * 1024,
	}
}

// utlsprobe: gửi request (header+body đã dump từ Bun) tới opencode bằng Go nhưng TLS ClientHello giả lập Bun
// (uTLS, spec dựng từ ClientHello thật của Bun). Dùng: utlsprobe <dump.json> <clienthello.bin|go> [socks5 host:port]
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	tls "github.com/refraction-networking/utls"
	"golang.org/x/net/proxy"
)

func newSes() string {
	const a = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	v := (^((uint64(time.Now().UnixMilli())*0x1000 + 1) & 0xFFFFFFFFFFFF)) & 0xFFFFFFFFFFFF
	b := make([]byte, 14)
	for i := range b {
		b[i] = a[rand.Intn(62)]
	}
	return fmt.Sprintf("ses_%012x%s", v, b)
}

func main() {
	n, _ := strconv.Atoi(os.Getenv("N"))
	if n < 1 {
		n = 1
	}
	fixed := newSes()
	if s := os.Getenv("SES"); s != "" {
		fixed = s
	}
	fmt.Fprintln(os.Stderr, "session", fixed)
	for range n {
		ses := fixed
		if os.Getenv("NEWSES") != "" {
			ses = newSes()
		}
		st := once(ses)
		if st != 200 && os.Getenv("STOP429") != "" {
			fmt.Fprintf(os.Stderr, "stop after status %d\n", st)
			return
		}
	}
}

func once(ses string) int {
	var d struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers"`
		Body    string            `json:"body"`
	}
	b, _ := os.ReadFile(os.Args[1])
	json.Unmarshal(b, &d)
	var dial func(network, addr string) (net.Conn, error) = (&net.Dialer{Timeout: 10 * time.Second}).Dial
	if len(os.Args) > 3 {
		pd, err := proxy.SOCKS5("tcp", os.Args[3], nil, proxy.Direct)
		if err != nil {
			panic(err)
		}
		dial = pd.Dial
	}
	raw, err := dial("tcp", "opencode.ai:443")
	if err != nil {
		fmt.Printf("{\"status\":0,\"err\":%q}\n", err.Error())
		return 0
	}
	var conn net.Conn
	if os.Args[2] == "go" {
		conn = tls.Client(raw, &tls.Config{ServerName: "opencode.ai", NextProtos: []string{"http/1.1"}})
	} else {
		hello, _ := os.ReadFile(os.Args[2])
		fp := tls.Fingerprinter{AllowBluntMimicry: true}
		spec, err := fp.FingerprintClientHello(hello)
		if err != nil {
			panic(err)
		}
		for _, ext := range spec.Extensions {
			if alpn, ok := ext.(*tls.ALPNExtension); ok {
				alpn.AlpnProtocols = []string{"http/1.1"}
			}
		}
		uc := tls.UClient(raw, &tls.Config{ServerName: "opencode.ai"}, tls.HelloCustom)
		if err := uc.ApplyPreset(spec); err != nil {
			panic(err)
		}
		if err := uc.Handshake(); err != nil {
			panic(err)
		}
		conn = uc
	}
	if m := os.Getenv("MODEL"); m != "" {
		var bm map[string]any
		json.Unmarshal([]byte(d.Body), &bm)
		bm["model"] = m
		nb, _ := json.Marshal(bm)
		d.Body = string(nb)
	}
	req, _ := http.NewRequest("POST", d.URL, strings.NewReader(d.Body))
	for k, v := range d.Headers {
		lk := strings.ToLower(k)
		if strings.Contains(lk, "session") {
			v = ses
		}
		// Mỗi request phải có id riêng như client thật, nếu không upstream có thể gộp/dedup theo request id.
		if lk == "x-opencode-request" {
			v = "msg_" + newSes()[4:]
		}
		req.Header.Set(k, v)
	}
	t0 := time.Now()
	if err := req.Write(conn); err != nil {
		panic(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		fmt.Printf("{\"status\":0,\"err\":%q}\n", err.Error())
		return 0
	}
	defer conn.Close()
	body, _ := io.ReadAll(resp.Body)
	text := ""
	for _, line := range bytes.Split(body, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("data: ")) {
			var c struct {
				Choices []struct {
					Delta struct{ Content string } `json:"delta"`
				} `json:"choices"`
			}
			if json.Unmarshal(line[6:], &c) == nil && len(c.Choices) > 0 {
				text += c.Choices[0].Delta.Content
			}
		}
	}
	errS := ""
	if resp.StatusCode != 200 {
		errS = string(body[:min(len(body), 100)])
	}
	fmt.Printf("{\"status\":%d,\"secs\":%.2f,\"ra\":%q,\"t\":%q,\"text\":%q,\"err\":%q}\n", resp.StatusCode, time.Since(t0).Seconds(), resp.Header.Get("Retry-After"), time.Now().Format("15:04:05"), text, errS)
	return resp.StatusCode
}

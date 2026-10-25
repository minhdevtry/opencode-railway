package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ======================== REDIS (RESP tối giản, không phụ thuộc thư viện) ========================
//
// Chỉ đủ cho hàng đợi job: lệnh dạng mảng bulk string, đọc mọi kiểu reply. Pool kết nối đơn giản; lệnh chặn
// (BLMOVE/BRPOP) dùng kết nối riêng lấy từ pool rồi trả lại.

var errRedisNil = errors.New("redis: nil")

type redisPool struct {
	addr, pass string
	db         int
	mu         sync.Mutex
	idle       []*redisConn
}

type redisConn struct {
	c  net.Conn
	rd *bufio.Reader
}

func newRedisPool(rawURL string) (*redisPool, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	p := &redisPool{addr: u.Host}
	if u.User != nil {
		p.pass, _ = u.User.Password()
	}
	if s := strings.TrimPrefix(u.Path, "/"); s != "" {
		p.db, _ = strconv.Atoi(s)
	}
	if !strings.Contains(p.addr, ":") {
		p.addr += ":6379"
	}
	return p, nil
}

func (p *redisPool) get(ctx context.Context) (*redisConn, error) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		c := p.idle[n-1]
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	d := net.Dialer{Timeout: 5 * time.Second}
	nc, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, err
	}
	c := &redisConn{c: nc, rd: bufio.NewReaderSize(nc, 64*1024)}
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))
	if p.pass != "" {
		if _, err := c.do("AUTH", p.pass); err != nil {
			nc.Close()
			return nil, err
		}
	}
	if p.db != 0 {
		if _, err := c.do("SELECT", strconv.Itoa(p.db)); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return c, nil
}

func (p *redisPool) put(c *redisConn, broken bool) {
	if broken {
		c.c.Close()
		return
	}
	p.mu.Lock()
	if len(p.idle) < 1024 {
		p.idle = append(p.idle, c)
		c = nil
	}
	p.mu.Unlock()
	if c != nil {
		c.c.Close()
	}
}

// Do chạy 1 lệnh. timeout = 0 nghĩa là 10s (lệnh chặn thì truyền timeout lớn hơn thời gian chặn).
func (p *redisPool) Do(ctx context.Context, timeout time.Duration, args ...string) (any, error) {
	c, err := p.get(ctx)
	if err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	_ = c.c.SetDeadline(time.Now().Add(timeout))
	v, err := c.do(args...)
	var re redisError
	p.put(c, err != nil && !errors.As(err, &re) && err != errRedisNil)
	return v, err
}

type redisError string

func (e redisError) Error() string { return "redis: " + string(e) }

func (c *redisConn) do(args ...string) (any, error) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.c.Write([]byte(sb.String())); err != nil {
		return nil, err
	}
	return c.read()
}

func (c *redisConn) read() (any, error) {
	line, err := c.rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return nil, errors.New("redis: reply rỗng")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, redisError(line[1:])
	case ':':
		return strconv.ParseInt(line[1:], 10, 64)
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, errRedisNil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.rd, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return nil, errRedisNil
		}
		out := make([]any, n)
		var firstErr error
		for i := range out {
			v, err := c.read()
			if err != nil && err != errRedisNil {
				var re redisError
				if !errors.As(err, &re) {
					return nil, err // lỗi mạng: kết nối hỏng
				}
				if firstErr == nil {
					firstErr = err // lỗi trong phần tử: đọc hết mảng để kết nối không còn dữ liệu thừa
				}
			}
			out[i] = v
		}
		if firstErr != nil {
			return out, firstErr
		}
		return out, nil
	}
	return nil, fmt.Errorf("redis: kiểu reply lạ %q", line)
}

// Tiện ích chuyển kiểu reply.
func rStr(v any) string {
	s, _ := v.(string)
	return s
}

func rInt(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func rMap(v any) map[string]string {
	arr, _ := v.([]any)
	m := make(map[string]string, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		m[rStr(arr[i])] = rStr(arr[i+1])
	}
	return m
}

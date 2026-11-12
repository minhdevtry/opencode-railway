package main

// ======================== IN-PROCESS REDIS FOR TESTS ========================
//
// A minimal RESP server implementing exactly the commands jobs.go and
// statepersist.go use, so the durable job queue can be regression-tested
// without an external redis-server binary. Not a general Redis: unknown
// commands return an error reply, and persistence/eviction are not modeled.

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type mrEntry struct {
	s     string
	list  []string
	hash  map[string]string
	z     map[string]float64
	expAt time.Time // zero = no expiry
}

type miniRedis struct {
	mu   sync.Mutex
	data map[string]*mrEntry
	ln   net.Listener

	blocked []chan struct{} // signaled on every write, wakes BLMOVE waiters
}

func startMiniRedis(t *testing.T) *miniRedis {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mr := &miniRedis{data: map[string]*mrEntry{}, ln: ln}
	go mr.acceptLoop()
	t.Cleanup(func() { _ = ln.Close() })
	return mr
}

func (mr *miniRedis) url() string { return "redis://" + mr.ln.Addr().String() }

func (mr *miniRedis) acceptLoop() {
	for {
		c, err := mr.ln.Accept()
		if err != nil {
			return
		}
		go mr.serve(c)
	}
}

// entry returns (creating if needed) the entry for key, dropping it first if expired.
func (mr *miniRedis) entry(key string) *mrEntry {
	e := mr.data[key]
	if e != nil && !e.expAt.IsZero() && time.Now().After(e.expAt) {
		delete(mr.data, key)
		e = nil
	}
	if e == nil {
		e = &mrEntry{}
		mr.data[key] = e
	}
	return e
}

func (mr *miniRedis) signalWrite() {
	for _, ch := range mr.blocked {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (mr *miniRedis) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	wr := bufio.NewWriter(c)
	for {
		args, err := readRespArray(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		mr.handle(c, wr, args)
		if err := wr.Flush(); err != nil {
			return
		}
	}
}

func readRespArray(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("miniRedis: expected array, got %q", line)
	}
	n, _ := strconv.Atoi(line[1:])
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimSuffix(hdr, "\r\n")
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("miniRedis: expected bulk, got %q", hdr)
		}
		ln, _ := strconv.Atoi(hdr[1:])
		buf := make([]byte, ln+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		out = append(out, string(buf[:ln]))
	}
	return out, nil
}

func writeSimple(wr *bufio.Writer, s string) { fmt.Fprintf(wr, "+%s\r\n", s) }
func writeErr(wr *bufio.Writer, s string)    { fmt.Fprintf(wr, "-ERR %s\r\n", s) }
func writeInt(wr *bufio.Writer, n int64)     { fmt.Fprintf(wr, ":%d\r\n", n) }
func writeNil(wr *bufio.Writer)              { fmt.Fprint(wr, "$-1\r\n") }
func writeBulk(wr *bufio.Writer, s string)   { fmt.Fprintf(wr, "$%d\r\n%s\r\n", len(s), s) }

func writeArray(wr *bufio.Writer, items []string) {
	fmt.Fprintf(wr, "*%d\r\n", len(items))
	for _, s := range items {
		writeBulk(wr, s)
	}
}

func (mr *miniRedis) handle(c net.Conn, wr *bufio.Writer, args []string) {
	cmd := strings.ToUpper(args[0])
	a := args[1:]
	mr.mu.Lock()
	defer mr.mu.Unlock()
	switch cmd {
	case "PING":
		writeSimple(wr, "PONG")
	case "AUTH", "SELECT":
		writeSimple(wr, "OK")
	case "SET":
		if len(a) < 2 {
			writeErr(wr, "wrong number of arguments")
			return
		}
		mr.entry(a[0]).s = a[1]
		writeSimple(wr, "OK")
	case "GET":
		e := mr.entry(a[0])
		if e.s == "" && len(e.list) == 0 && len(e.hash) == 0 && len(e.z) == 0 {
			writeNil(wr)
			return
		}
		writeBulk(wr, e.s)
	case "DEL":
		var n int64
		for _, k := range a {
			if _, ok := mr.data[k]; ok {
				delete(mr.data, k)
				n++
			}
		}
		writeInt(wr, n)
	case "EXPIRE":
		secs, _ := strconv.Atoi(a[1])
		mr.entry(a[0]).expAt = time.Now().Add(time.Duration(secs) * time.Second)
		writeInt(wr, 1)
	case "HSET":
		e := mr.entry(a[0])
		if e.hash == nil {
			e.hash = map[string]string{}
		}
		var n int64
		for i := 1; i+1 < len(a); i += 2 {
			if _, ok := e.hash[a[i]]; !ok {
				n++
			}
			e.hash[a[i]] = a[i+1]
		}
		writeInt(wr, n)
	case "HGET":
		v, ok := mr.entry(a[0]).hash[a[1]]
		if !ok {
			writeNil(wr)
			return
		}
		writeBulk(wr, v)
	case "HMGET":
		e := mr.entry(a[0])
		fmt.Fprintf(wr, "*%d\r\n", len(a)-1)
		for _, f := range a[1:] {
			if v, ok := e.hash[f]; ok {
				writeBulk(wr, v)
			} else {
				writeNil(wr)
			}
		}
	case "HGETALL":
		e := mr.entry(a[0])
		keys := make([]string, 0, len(e.hash))
		for k := range e.hash {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]string, 0, 2*len(keys))
		for _, k := range keys {
			out = append(out, k, e.hash[k])
		}
		writeArray(wr, out)
	case "HINCRBY":
		e := mr.entry(a[0])
		if e.hash == nil {
			e.hash = map[string]string{}
		}
		inc, _ := strconv.ParseInt(a[2], 10, 64)
		cur, _ := strconv.ParseInt(e.hash[a[1]], 10, 64)
		e.hash[a[1]] = strconv.FormatInt(cur+inc, 10)
		writeInt(wr, cur+inc)
	case "LPUSH":
		e := mr.entry(a[0])
		for _, v := range a[1:] {
			e.list = append([]string{v}, e.list...)
		}
		mr.signalWrite()
		writeInt(wr, int64(len(e.list)))
	case "RPUSH":
		e := mr.entry(a[0])
		e.list = append(e.list, a[1:]...)
		mr.signalWrite()
		writeInt(wr, int64(len(e.list)))
	case "LLEN":
		writeInt(wr, int64(len(mr.entry(a[0]).list)))
	case "LRANGE":
		l := mr.entry(a[0]).list
		start, _ := strconv.Atoi(a[1])
		stop, _ := strconv.Atoi(a[2])
		if start < 0 {
			start = max(0, len(l)+start)
		}
		if stop < 0 {
			stop = len(l) + stop
		}
		stop = min(stop, len(l)-1)
		var out []string
		if start < len(l) {
			out = append(out, l[start:stop+1]...)
		}
		writeArray(wr, out)
	case "LREM":
		e := mr.entry(a[0])
		count, _ := strconv.Atoi(a[1])
		val := a[2]
		var removed int64
		keep := e.list[:0]
		// count > 0: head to tail; count < 0: tail to head; 0: all
		if count < 0 {
			for i := len(e.list) - 1; i >= 0; i-- {
				if e.list[i] == val && (count == 0 || removed < int64(-count)) {
					removed++
					continue
				}
				keep = append(keep, e.list[i])
			}
			for i, j := 0, len(keep)-1; i < j; i, j = i+1, j-1 {
				keep[i], keep[j] = keep[j], keep[i]
			}
		} else {
			for _, v := range e.list {
				if v == val && (count == 0 || removed < int64(count)) {
					removed++
					continue
				}
				keep = append(keep, v)
			}
		}
		e.list = keep
		writeInt(wr, removed)
	case "LMOVE":
		src, dst := mr.entry(a[0]), mr.entry(a[1])
		if len(src.list) == 0 {
			writeNil(wr)
			return
		}
		var v string
		if strings.EqualFold(a[2], "LEFT") {
			v, src.list = src.list[0], src.list[1:]
		} else {
			v, src.list = src.list[len(src.list)-1], src.list[:len(src.list)-1]
		}
		if strings.EqualFold(a[3], "LEFT") {
			dst.list = append([]string{v}, dst.list...)
		} else {
			dst.list = append(dst.list, v)
		}
		mr.signalWrite()
		writeBulk(wr, v)
	case "BLMOVE":
		mr.blmove(c, wr, a)
	case "ZADD":
		e := mr.entry(a[0])
		if e.z == nil {
			e.z = map[string]float64{}
		}
		i := 1
		xx := false
		if strings.EqualFold(a[i], "XX") {
			xx = true
			i++
		}
		var added int64
		for ; i+1 < len(a); i += 2 {
			sc, _ := strconv.ParseFloat(a[i], 64)
			if _, ok := e.z[a[i+1]]; !ok {
				if xx {
					continue
				}
				added++
			}
			e.z[a[i+1]] = sc
		}
		writeInt(wr, added)
	case "ZREM":
		e := mr.entry(a[0])
		var n int64
		for _, m := range a[1:] {
			if _, ok := e.z[m]; ok {
				delete(e.z, m)
				n++
			}
		}
		writeInt(wr, n)
	case "ZCARD":
		writeInt(wr, int64(len(mr.entry(a[0]).z)))
	case "ZSCORE":
		sc, ok := mr.entry(a[0]).z[a[1]]
		if !ok {
			writeNil(wr)
			return
		}
		writeBulk(wr, strconv.FormatFloat(sc, 'f', -1, 64))
	case "EVAL":
		mr.eval(wr, a)
	default:
		writeErr(wr, "unknown command "+cmd)
	}
}

// blmove releases the lock while waiting so writers can make progress.
func (mr *miniRedis) blmove(c net.Conn, wr *bufio.Writer, a []string) {
	timeoutSec, _ := strconv.Atoi(a[4])
	deadline := time.Now().Add(time.Duration(timeoutSec) * time.Second)
	ch := make(chan struct{}, 1)
	mr.blocked = append(mr.blocked, ch)
	defer func() {
		for i, x := range mr.blocked {
			if x == ch {
				mr.blocked = append(mr.blocked[:i], mr.blocked[i+1:]...)
				break
			}
		}
	}()
	for {
		src := mr.entry(a[0])
		if len(src.list) > 0 {
			mr.mu.Unlock()
			mr.handle(c, wr, append([]string{"LMOVE"}, a[:4]...))
			mr.mu.Lock()
			return
		}
		if time.Now().After(deadline) {
			writeNil(wr)
			return
		}
		mr.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(20 * time.Millisecond):
		}
		mr.mu.Lock()
	}
}

// eval implements the two Lua scripts used by the gateway by name of their
// KEYS layout, which is stable: moveDue(delay|lease -> q) and claim(job).
func (mr *miniRedis) eval(wr *bufio.Writer, a []string) {
	// a = [script, numkeys, key..., arg...]
	nk, _ := strconv.Atoi(a[1])
	keys := a[2 : 2+nk]
	argv := a[2+nk:]
	script := a[0]
	switch {
	case strings.Contains(script, "ZRANGEBYSCORE"): // luaMoveDue
		src := mr.entry(keys[0])
		now, _ := strconv.ParseFloat(argv[0], 64)
		var due []string
		for m, sc := range src.z {
			if sc <= now {
				due = append(due, m)
			}
		}
		sort.Slice(due, func(i, j int) bool { return src.z[due[i]] < src.z[due[j]] })
		if len(due) > 500 {
			due = due[:500]
		}
		dst := mr.entry(keys[1])
		for _, m := range due {
			delete(src.z, m)
			dst.list = append(dst.list, m)
		}
		if len(due) > 0 {
			mr.signalWrite()
		}
		writeInt(wr, int64(len(due)))
	case strings.Contains(script, "HINCRBY"): // luaClaim
		key, leaseKey, procKey := keys[0], keys[1], keys[2]
		id, lease := argv[0], argv[1]
		e := mr.entry(key)
		st := e.hash["status"]
		proc := mr.entry(procKey)
		keep := proc.list[:0]
		removed := false
		for _, v := range proc.list {
			if v == id && !removed {
				removed = true
				continue
			}
			keep = append(keep, v)
		}
		proc.list = keep
		if st == "" || st == "done" || st == "failed" {
			writeInt(wr, 0)
			return
		}
		if st == "running" {
			if _, ok := mr.entry(leaseKey).z[id]; ok {
				writeInt(wr, 0)
				return
			}
		}
		if e.hash == nil {
			e.hash = map[string]string{}
		}
		e.hash["status"] = "running"
		lz := mr.entry(leaseKey)
		if lz.z == nil {
			lz.z = map[string]float64{}
		}
		sc, _ := strconv.ParseFloat(lease, 64)
		lz.z[id] = sc
		cur, _ := strconv.ParseInt(e.hash["attempts"], 10, 64)
		e.hash["attempts"] = strconv.FormatInt(cur+1, 10)
		writeInt(wr, cur+1)
	default:
		writeErr(wr, "miniRedis: unsupported EVAL script")
	}
}

// ---------- helpers for tests ----------

func (mr *miniRedis) hgetall(key string) map[string]string {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	out := map[string]string{}
	for k, v := range mr.entry(key).hash {
		out[k] = v
	}
	return out
}

func (mr *miniRedis) llen(key string) int {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return len(mr.entry(key).list)
}

func (mr *miniRedis) zscore(key, member string) (float64, bool) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	sc, ok := mr.entry(key).z[member]
	return sc, ok
}

func (mr *miniRedis) forceExpire(key string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if e := mr.data[key]; e != nil {
		e.expAt = time.Now().Add(-time.Second)
	}
}

// waitFor polls cond until true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// testRedis wires a fresh miniRedis into the global rdb and returns it.
func testRedis(t *testing.T) *miniRedis {
	t.Helper()
	mr := startMiniRedis(t)
	p, err := newRedisPool(mr.url())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Do(context.Background(), time.Second, "PING"); err != nil {
		t.Fatal(err)
	}
	old := rdb
	rdb = p
	t.Cleanup(func() { rdb = old })
	return mr
}

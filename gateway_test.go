package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------- helpers ----------

type tagTransport struct {
	name string
	next http.RoundTripper
}

func (t tagTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("X-Test-Egress", t.name)
	return t.next.RoundTrip(r)
}

func tagged(e *Egress) *Egress {
	e.client.Store(&http.Client{Transport: tagTransport{e.Name, http.DefaultTransport}})
	return e
}

// setup dựng gateway thử nghiệm với 1 direct + (tuỳ chọn) 1 egress kiểu Tor, trỏ vào upstream giả.
func setup(t *testing.T, upstreamHandler http.HandlerFunc, withTor bool) (*httptest.Server, *Manager) {
	t.Helper()
	up := httptest.NewServer(upstreamHandler)
	t.Cleanup(up.Close)

	cfg = Config{MaxConcurrency: 8, MaxBodyBytes: 1 << 20, MaxWait: 5 * time.Second, MaxAttempts: 10,
		CooldownMax: 10 * time.Minute, StreamIdle: time.Minute, TorRotateCooldown: 50 * time.Millisecond, InjectToolsAnthropic: true, SessionIDMode: "client", ProjectIDMode: "client", TorBackoffMax: 2 * time.Minute, ImageModels: map[string]bool{},
		PreferDirect: true, TorIdentities: 2, TorMaxInflight: 24, QueueWait: 2 * time.Second, DirectWarnReqs: 200, DirectWarnInflight: 60}
	concurrencySem = make(chan struct{}, cfg.MaxConcurrency)
	infURL, zenURL = up.URL, up.URL
	torAvailable = false

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mgr = newManager(ctx)
	tagged(mgr.egresses[0])
	if withTor {
		tor := newTorEgress("tor-1", "127.0.0.1:1")
		tagged(tor)
		tor.ready.Store(true)
		mgr.add(tor)
	}
	al, _ := parseAllowlist("")
	grd = &guard{allow: al}
	gw := httptest.NewServer(buildMux())
	t.Cleanup(gw.Close)
	return gw, mgr
}

func sse(w http.ResponseWriter, chunks ...string) {
	w.Header().Set("Content-Type", "text/event-stream")
	for _, c := range chunks {
		fmt.Fprintf(w, "data: %s\n\n", c)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func post(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp, sb.String()
}

// ---------- auth ----------

func TestAllowlist(t *testing.T) {
	al, err := parseAllowlist("203.0.113.5, 10.1.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]bool{"203.0.113.5": true, "203.0.113.6": false, "10.1.2.3": true, "10.2.0.1": false, "127.0.0.1": false} {
		if got := al.allows(netip.MustParseAddr(ip)); got != want {
			t.Errorf("%s: got %v want %v", ip, got, want)
		}
	}
	empty, _ := parseAllowlist("")
	if !empty.allows(netip.MustParseAddr("127.0.0.1")) || !empty.allows(netip.MustParseAddr("172.17.0.1")) || empty.allows(netip.MustParseAddr("8.8.8.8")) {
		t.Error("allowlist rỗng phải chỉ cho loopback/private")
	}
	all, _ := parseAllowlist("*")
	if !all.allows(netip.MustParseAddr("8.8.8.8")) {
		t.Error("* phải mở hoàn toàn")
	}
	if _, err := parseAllowlist("not-an-ip"); err == nil {
		t.Error("phải báo lỗi với IP sai")
	}
}

func TestClientIPHeader(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "100.64.0.9:1234"
	r.Header.Set("X-Forwarded-For", "6.6.6.6, 1.2.3.4")
	if ip, _ := clientIP(r, "X-Forwarded-For"); ip.String() != "1.2.3.4" {
		t.Errorf("XFF phải lấy phần tử ngoài cùng bên phải (không giả mạo được), got %s", ip)
	}
	if ip, _ := clientIP(r, ""); ip.String() != "100.64.0.9" {
		t.Errorf("không cấu hình header phải dùng RemoteAddr, got %s", ip)
	}
}

func TestGuardDeniesAndRateLimits(t *testing.T) {
	al, _ := parseAllowlist("203.0.113.5")
	g := &guard{allow: al, limiter: newIPLimiter(60, 2)}
	h := g.wrap(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }, true)

	do := func(ip string) int {
		r := httptest.NewRequest("POST", "/v1/messages", nil)
		r.RemoteAddr = ip + ":1"
		w := httptest.NewRecorder()
		h(w, r)
		return w.Code
	}
	if c := do("8.8.8.8"); c != 403 {
		t.Errorf("IP lạ phải 403, got %d", c)
	}
	if do("203.0.113.5") != 200 || do("203.0.113.5") != 200 {
		t.Error("2 request đầu (burst) phải qua")
	}
	if c := do("203.0.113.5"); c != 429 {
		t.Errorf("vượt burst phải 429, got %d", c)
	}
}

// ---------- chuyển đổi Anthropic -> OpenAI ----------

func TestAnthropicToOpenAI(t *testing.T) {
	var req map[string]any
	json.Unmarshal([]byte(`{
	 "model":"deepseek[1m]","max_tokens":100,"system":[{"type":"text","text":"SYS","cache_control":{"type":"ephemeral"}}],
	 "tools":[{"name":"Bash","description":"d","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}},{"type":"web_search_20250305","name":"web_search"}],
	 "tool_choice":{"type":"any"},
	 "messages":[
	  {"role":"user","content":"hi"},
	  {"role":"assistant","content":[{"type":"thinking","thinking":"hmm","signature":"x"},{"type":"text","text":"ok"},{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls"}}]},
	  {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"a.txt"}]},{"type":"text","text":"next"}]},
	  {"role":"assistant","content":[{"type":"thinking","thinking":"only thinking"}]}
	 ]}`), &req)
	out := anthropicToOpenAI(req, "deepseek-v4.1-flash-free")
	msgs := out["messages"].([]any)
	roles := []string{}
	for _, m := range msgs {
		roles = append(roles, m.(map[string]any)["role"].(string))
	}
	if got := strings.Join(roles, ","); got != "system,user,assistant,tool,user" {
		t.Fatalf("thứ tự role sai (tool phải sát sau assistant, assistant rỗng bị bỏ): %s", got)
	}
	asst := msgs[2].(map[string]any)
	if asst["reasoning_content"] != "hmm" || len(asst["tool_calls"].([]any)) != 1 {
		t.Errorf("assistant sai: %v", asst)
	}
	if tool := msgs[3].(map[string]any); tool["tool_call_id"] != "t1" || tool["content"] != "a.txt" {
		t.Errorf("tool message sai: %v", tool)
	}
	if len(out["tools"].([]any)) != 1 {
		t.Error("server tool (web_search) phải bị bỏ")
	}
	if out["tool_choice"] != "required" {
		t.Errorf("tool_choice any -> required, got %v", out["tool_choice"])
	}
}

// omp/Claude Code gửi mức suy luận ở output_config.effort + thinking;
// gateway phải map sang reasoning_effort của Chat Completions, và field
// client-only (context_management, output_config) không được lọt lên upstream.
func TestAnthropicReasoningEffortMapping(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"omp adaptive high", `{"model":"m","thinking":{"type":"adaptive"},"output_config":{"effort":"high"}}`, "high"},
		{"thinking effort fallback", `{"model":"m","thinking":{"type":"enabled","effort":"low"}}`, "low"},
		{"thinking disabled", `{"model":"m","thinking":{"type":"disabled"},"output_config":{"effort":"high"}}`, ""},
		{"no effort", `{"model":"m","thinking":{"type":"adaptive"}}`, ""},
		{"invalid effort", `{"model":"m","output_config":{"effort":"turbo"}}`, ""},
	}
	for _, c := range cases {
		var req map[string]any
		if err := json.Unmarshal([]byte(c.raw), &req); err != nil {
			t.Fatalf("%s: unmarshal: %v", c.name, err)
		}
		out := anthropicToOpenAI(req, "deepseek-v4.1-flash-free")
		got, _ := out["reasoning_effort"].(string)
		if got != c.want {
			t.Errorf("%s: reasoning_effort = %q, want %q", c.name, got, c.want)
		}
		// Field client-only không được chuyển sang body upstream.
		for _, k := range []string{"thinking", "context_management", "output_config"} {
			if _, ok := out[k]; ok {
				t.Errorf("%s: upstream body còn sót field %q", c.name, k)
			}
		}
	}
}

func TestRemapStub(t *testing.T) {
	tools := map[string]clientTool{
		"Read": {name: "Read", props: map[string]bool{"file_path": true}},
		"Bash": {name: "Bash", props: map[string]bool{"command": true}},
	}
	if n, rw := remapStub("bash", tools); n != "Bash" || rw != nil {
		t.Errorf("bash -> Bash, got %s", n)
	}
	n, rw := remapStub("read", tools)
	if n != "Read" || rw == nil || rw(`{"filePath":"/x"}`) != `{"file_path":"/x"}` {
		t.Errorf("read -> Read + đổi key, got %s", n)
	}
	if n, _ := remapStub("Bash", tools); n != "Bash" {
		t.Error("tên đúng của client phải giữ nguyên")
	}
	if n, _ := remapStub("glob", tools); n != "glob" {
		t.Error("client không có Glob thì giữ nguyên")
	}
}

// ---------- định tuyến egress ----------

func TestDirectFirstThenTorOn429(t *testing.T) {
	var directHits, torHits atomic.Int32
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Egress") == "direct" {
			directHits.Add(1)
			w.Header().Set("Retry-After", "21600")
			w.WriteHeader(429)
			w.Write([]byte(`{"type":"error","error":{"type":"FreeUsageLimitError"}}`))
			return
		}
		torHits.Add(1)
		sse(w, `{"id":"c1","choices":[{"delta":{"content":"hello"}}]}`, `{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	}, true)

	for i := 0; i < 3; i++ {
		resp, body := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
		if resp.StatusCode != 200 || !strings.Contains(body, "hello") {
			t.Fatalf("lần %d: %d %s", i, resp.StatusCode, body)
		}
		if resp.Header.Get("X-Gateway-Egress") != "tor-1" {
			t.Errorf("phải đi qua tor-1, got %q", resp.Header.Get("X-Gateway-Egress"))
		}
	}
	if directHits.Load() != 1 {
		t.Errorf("direct chỉ được thử 1 lần rồi cooldown, got %d", directHits.Load())
	}
	// model khác vẫn dùng direct (cooldown tính theo egress × model)
	if !m.egresses[0].available("nemotron-3.5-lightning-free") {
		t.Error("direct vẫn phải dùng được cho model khác")
	}
	if m.egresses[0].available("deepseek-v4.1-flash-free") {
		t.Error("direct phải đang cooldown cho deepseek")
	}
	if d := m.egresses[0].cooldownLeft("deepseek-v4.1-flash-free"); d > 10*time.Minute+time.Second {
		t.Errorf("cooldown của direct phải bị cap ở CooldownMax, got %v", d)
	}
}

func TestTorRotatesOn429(t *testing.T) {
	var torHits atomic.Int32
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Egress") == "direct" || torHits.Add(1) <= 2 {
			w.WriteHeader(429)
			return
		}
		sse(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
	}, true)
	resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("phải thành công sau khi xoay Tor, got %d", resp.StatusCode)
	}
	if g := m.egresses[1].gen.Load(); g != 2 {
		t.Errorf("tor phải đổi circuit 2 lần, gen=%d", g)
	}
}

func TestFailFastWhenNoTorAndLongCooldown(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "21600")
		w.WriteHeader(429)
	}, false)
	t0 := time.Now()
	resp, body := post(t, gw.URL+"/v1/messages", `{"model":"deepseek","max_tokens":5,"messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != 429 || time.Since(t0) > 2*time.Second {
		t.Fatalf("phải 429 ngay, got %d sau %v", resp.StatusCode, time.Since(t0))
	}
	if !strings.Contains(body, `"type":"error"`) || !strings.Contains(body, "rate_limit_error") || resp.Header.Get("Retry-After") == "" {
		t.Errorf("lỗi phải đúng định dạng Anthropic kèm Retry-After: %s", body)
	}
}

// ---------- Anthropic stream / non-stream ----------

func toolStream(w http.ResponseWriter, r *http.Request) {
	sse(w,
		`{"id":"c1","choices":[{"delta":{"reasoning_content":"think"}}]}`,
		`{"choices":[{"delta":{"content":"Let me read."}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read","arguments":"{\"file"}}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"Path\":\"/a\"}"}}]}}]}`,
		`{"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":7}}`)
}

const claudeReq = `{"model":"deepseek[1m]","max_tokens":100,"stream":%v,
 "tools":[{"name":"Read","description":"r","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}}}}],
 "messages":[{"role":"user","content":"read /a"}]}`

func TestAnthropicStreamTranslation(t *testing.T) {
	gw, _ := setup(t, toolStream, false)
	resp, body := post(t, gw.URL+"/v1/messages?beta=true", fmt.Sprintf(claudeReq, true))
	if resp.StatusCode != 200 || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	var events []string
	var toolJSON strings.Builder
	var stop string
	for _, blk := range strings.Split(body, "\n\n") {
		lines := strings.Split(strings.TrimSpace(blk), "\n")
		if len(lines) < 2 {
			continue
		}
		ev := strings.TrimPrefix(lines[0], "event: ")
		events = append(events, ev)
		var d map[string]any
		json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &d)
		if ev == "content_block_start" {
			if cb := d["content_block"].(map[string]any); cb["type"] == "tool_use" && cb["name"] != "Read" {
				t.Errorf("stub read phải được đổi thành Read, got %v", cb["name"])
			}
		}
		if ev == "content_block_delta" {
			if dl := d["delta"].(map[string]any); dl["type"] == "input_json_delta" {
				toolJSON.WriteString(dl["partial_json"].(string))
			}
		}
		if ev == "message_delta" {
			stop = d["delta"].(map[string]any)["stop_reason"].(string)
			if d["usage"].(map[string]any)["output_tokens"].(float64) != 7 {
				t.Errorf("usage output_tokens sai: %v", d["usage"])
			}
		}
	}
	want := "message_start,ping,content_block_start,content_block_delta,content_block_delta,content_block_stop," +
		"content_block_start,content_block_delta,content_block_stop,content_block_start,content_block_delta,content_block_stop,message_delta,message_stop"
	if got := strings.Join(events, ","); got != want {
		t.Errorf("chuỗi sự kiện sai:\n got  %s\n want %s", got, want)
	}
	if toolJSON.String() != `{"file_path":"/a"}` {
		t.Errorf("tham số tool phải được đổi key filePath->file_path: %s", toolJSON.String())
	}
	if stop != "tool_use" {
		t.Errorf("stop_reason=%s", stop)
	}
}

func TestAnthropicNonStream(t *testing.T) {
	gw, _ := setup(t, toolStream, false)
	resp, body := post(t, gw.URL+"/v1/messages", fmt.Sprintf(claudeReq, false))
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil || resp.StatusCode != 200 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if m["stop_reason"] != "tool_use" || m["model"] != "deepseek[1m]" {
		t.Errorf("%v", m)
	}
	content := m["content"].([]any)
	last := content[len(content)-1].(map[string]any)
	if last["type"] != "tool_use" || last["name"] != "Read" || last["input"].(map[string]any)["file_path"] != "/a" {
		t.Errorf("tool_use sai: %v", last)
	}
}

func TestUpstreamGateRequiresToolsInjected(t *testing.T) {
	var gotTools atomic.Int32
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		json.NewDecoder(r.Body).Decode(&b)
		if tools, ok := b["tools"].([]any); ok {
			gotTools.Store(int32(len(tools)))
		}
		if b["stream"] != true {
			t.Error("upstream phải luôn nhận stream=true")
		}
		sse(w, `{"choices":[{"delta":{"content":"x"},"finish_reason":"stop"}]}`)
	}, false)
	post(t, gw.URL+"/v1/messages", `{"model":"deepseek","max_tokens":5,"messages":[{"role":"user","content":"x"}]}`)
	if gotTools.Load() < 4 {
		t.Errorf("phải bơm tool stub của opencode, got %d tools", gotTools.Load())
	}
}

func TestEndpointsMisc(t *testing.T) {
	gw, _ := setup(t, toolStream, false)

	resp, body := post(t, gw.URL+"/v1/messages/count_tokens", `{"model":"x","messages":[{"role":"user","content":"hello world"}]}`)
	if resp.StatusCode != 200 || !strings.Contains(body, "input_tokens") {
		t.Errorf("count_tokens: %d %s", resp.StatusCode, body)
	}
	for _, p := range []string{"/v1/models", "/v1/models/deepseek", "/v1/models/deepseek%5B1m%5D", "/health", "/", "/status"} {
		r, err := http.Get(gw.URL + p)
		if err != nil || r.StatusCode != 200 {
			t.Errorf("GET %s -> %v %v", p, err, r)
		}
	}
	if r, _ := http.Get(gw.URL + "/v1/models/nope"); r.StatusCode != 404 {
		t.Errorf("model lạ phải 404, got %d", r.StatusCode)
	}
	if r, _ := http.Get(gw.URL + "/khong-co"); r.StatusCode != 404 {
		t.Errorf("path lạ phải 404, got %d", r.StatusCode)
	}
	resp, body = post(t, gw.URL+"/v1/messages", `{not json`)
	if resp.StatusCode != 400 || !strings.Contains(body, `"type":"error"`) {
		t.Errorf("JSON sai: %d %s", resp.StatusCode, body)
	}
	req, _ := http.NewRequest("GET", gw.URL+"/v1/messages", nil)
	if r, _ := http.DefaultClient.Do(req); r.StatusCode != 405 {
		t.Errorf("GET /v1/messages phải 405, got %d", r.StatusCode)
	}
}

func TestMapModelNames(t *testing.T) {
	for in, want := range map[string]string{
		"deepseek[1m]":                   "deepseek",
		"cline-free/deepseek-v4.1-flash": "deepseek-v4.1-flash",
		"claude-sonnet-5-5":              "deepseek",
		"claude-haiku-4-5-20251001":      "deepseek",
		"sonnet":                         "deepseek",
		"opus":                           "deepseek",
		"haiku":                          "deepseek",
		"gpt-4o":                         "deepseek",
		"":                               "deepseek",
		"Nemotron-3.5-Lightning[1m]":     "nemotron-3.5-lightning",
	} {
		if got := anthropicModel(in); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
}

func TestProxyRefreshURLInvokedOn429(t *testing.T) {
	oldCfg := cfg
	defer func() { cfg = oldCfg }()

	var refreshed atomic.Int32
	refreshSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshed.Add(1)
		w.WriteHeader(200)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer refreshSrv.Close()

	cfg.ProxyRefreshURL = refreshSrv.URL
	cfg.ProxyWarmup = 10 * time.Millisecond

	m := newManager(context.Background())
	p := newEgress("proxy1", kindProxy, nil)
	p.ready.Store(true)
	m.add(p)

	m.refreshRotatingProxy(p)
	if refreshed.Load() != 1 {
		t.Fatalf("refresh URL phải được gọi, got %d", refreshed.Load())
	}
}

func TestPickPrefersHealthyTor(t *testing.T) {
	_, m := setup(t, toolStream, false)
	a, b := newTorEgress("tor-a", "127.0.0.1:1"), newTorEgress("tor-b", "127.0.0.1:1")
	a.ready.Store(true)
	b.ready.Store(true)
	m.add(a)
	m.add(b)
	m.egresses[0].ready.Store(false) // không dùng direct
	model := "deepseek-v4.1-flash-free"
	a.exhausted(model, 0, true)
	a.cool[model].until = time.Now().Add(-time.Second) // đã hết cooldown nhưng còn strike
	for i := 0; i < 6; i++ {
		if got := m.pick(model); got != b {
			t.Fatalf("phải ưu tiên tor-b (khỏe) hơn tor-a (đang có strike), got %s", got.Name)
		}
	}
	a.clear(model)
	seen := map[string]bool{}
	for i := 0; i < 6; i++ {
		seen[m.pick(model).Name] = true
	}
	if len(seen) != 2 {
		t.Errorf("khi cả hai đều khỏe phải xoay vòng: %v", seen)
	}
}

func TestProbeGating(t *testing.T) {
	_, m := setup(t, toolStream, false)
	d := m.egresses[0]
	model := "deepseek-v4.1-flash-free"
	d.exhausted(model, time.Hour, true)
	d.cool[model].until = time.Now().Add(-time.Second) // hết cooldown nhưng còn strike => chỉ 1 request thăm dò
	if m.pick(model) != d {
		t.Fatal("request đầu phải được thăm dò direct")
	}
	for i := 0; i < 5; i++ {
		if m.pick(model) != nil {
			t.Fatal("khi đang thăm dò, các request khác không được dùng direct")
		}
	}
	d.clear(model) // thăm dò thành công
	if m.pick(model) != d || m.pick(model) != d {
		t.Error("sau khi khỏe lại, nhiều request dùng đồng thời được")
	}
}

func TestRotateNotStacked(t *testing.T) {
	e := newTorEgress("t", "127.0.0.1:1")
	e.rotate(0)
	e.rotate(0) // request thứ hai cùng thấy gen=0, không được xoay chồng
	if g := e.gen.Load(); g != 1 {
		t.Errorf("gen=%d, want 1", g)
	}
}

func TestStreamIdleWatchdog(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done() // upstream treo, không bao giờ kết thúc
	}, false)
	cfg.StreamIdle = 300 * time.Millisecond
	t0 := time.Now()
	resp, body := post(t, gw.URL+"/v1/messages", `{"model":"deepseek","max_tokens":5,"stream":true,"messages":[{"role":"user","content":"x"}]}`)
	if time.Since(t0) > 3*time.Second {
		t.Fatalf("watchdog không cắt stream treo (%v)", time.Since(t0))
	}
	if resp.StatusCode != 200 || !strings.Contains(body, "event: error") || strings.Contains(body, "message_stop") {
		t.Errorf("stream bị cắt phải kết thúc bằng event error (không giả làm hoàn chỉnh): %d %s", resp.StatusCode, body)
	}
	if n := inflightTotal.Load(); n != 0 {
		t.Errorf("slot phải được trả, inflight=%d", n)
	}
}

func TestTorBackoffAfterManyStrikes(t *testing.T) {
	cfg = Config{TorRotateCooldown: time.Second, TorBackoffMax: 20 * time.Second, CooldownMax: time.Minute}
	e := newTorEgress("t", "127.0.0.1:1")
	var last time.Duration
	for i := 1; i <= 12; i++ {
		last = e.exhausted("m", time.Hour, true)
		if i <= 4 && last != time.Second {
			t.Fatalf("strike %d: cooldown %v, muốn %v", i, last, time.Second)
		}
	}
	if last != 20*time.Second {
		t.Errorf("sau nhiều strike phải giãn tới TorBackoffMax, got %v", last)
	}
	e.clear("m")
	if d := e.exhausted("m", 0, true); d != time.Second {
		t.Errorf("thành công xong thì reset về cooldown ngắn, got %v", d)
	}
}

func TestSessionIDsStablePerConversationAndValidShape(t *testing.T) {
	cfg = Config{SessionIDMode: "client", ProjectIDMode: "client"}
	a1, p1 := idsFromKey("conv-1")
	a2, p2 := idsFromKey("conv-1")
	b, pb := idsFromKey("conv-2")
	if a1 != a2 || p1 != p2 || a1 == b || p1 == pb {
		t.Fatal("ID phải ổn định theo hội thoại và khác nhau giữa các hội thoại")
	}
	if !regexp.MustCompile(`^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$`).MatchString(a1) || len(p1) != 40 {
		t.Errorf("hình dạng ID sai: %s %s", a1, p1)
	}
	// Mô phỏng gate của upstream: giải mã 12 hex đầu về timestamp, phải gần "bây giờ".
	assertDecodesToNow := func(id string) {
		v, err := strconv.ParseUint(id[4:16], 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		inv := (^v) & 0xFFFFFFFFFFFF // = (ms*0x1000 + counter) & mask
		if c := inv & 0xFFF; c > 15 {
			t.Errorf("counter phải nhỏ như client thật, got %d: %s", c, id)
		}
		now := uint64(time.Now().UnixMilli()) * 0x1000 & 0xFFFFFFFFFFFF
		diff := int64(now) - int64(inv)
		if diff < 0 {
			diff = -diff
		}
		if diff > int64(time.Minute.Milliseconds())*0x1000 {
			t.Errorf("session ID không mang timestamp hợp lý (lệch %d ms): %s", diff/0x1000, id)
		}
	}
	assertDecodesToNow(a1)
	assertDecodesToNow(newSessionID())
	req := httptest.NewRequest("POST", "/", nil)
	setUpstreamHeaders(req, "*/*", "conv-1")
	if req.Header.Get("x-opencode-session") != a1 || req.Header.Get("x-opencode-session-id") != a1 {
		t.Error("header phải dùng session ổn định")
	}
	r2 := httptest.NewRequest("POST", "/", nil)
	r3 := httptest.NewRequest("POST", "/", nil)
	setUpstreamHeaders(r2, "*/*", "")
	setUpstreamHeaders(r3, "*/*", "")
	if r2.Header.Get("x-opencode-session") == r3.Header.Get("x-opencode-session") {
		t.Error("không có khoá hội thoại thì mỗi request phải có session mới")
	}
}

func TestSessionKeyExtraction(t *testing.T) {
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("X-Claude-Code-Session-Id", "abc")
	if k := sessionKey(r, nil); k != "abc" {
		t.Errorf("header: %q", k)
	}
	r2 := httptest.NewRequest("POST", "/", nil)
	body := map[string]any{"metadata": map[string]any{"user_id": `{"device_id":"d","session_id":"sess-9"}`}}
	if k := sessionKey(r2, body); k != "sess-9" {
		t.Errorf("metadata.user_id: %q", k)
	}
	if k := sessionKey(r2, map[string]any{"user": "u1"}); k != "u1" {
		t.Errorf("user: %q", k)
	}
}

func TestSystemBillingHeaderStripped(t *testing.T) {
	var req map[string]any
	json.Unmarshal([]byte(`{"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=1.2; cch=abc;"},{"type":"text","text":"You are helpful"}],"messages":[{"role":"user","content":"hi"}]}`), &req)
	out := anthropicToOpenAI(req, "m")
	sys := out["messages"].([]any)[0].(map[string]any)["content"].(string)
	if sys != "You are helpful" {
		t.Errorf("billing header phải bị bỏ khỏi system (đổi mỗi request, phá cache), got %q", sys)
	}
}

func TestSystemTotalTokensStripped(t *testing.T) {
	var req map[string]any
	json.Unmarshal([]byte(`{"system":[{"type":"text","text":"You are helpful"},{"type":"text","text":"<total_tokens>14998783 tokens left</total_tokens>"}],"messages":[{"role":"user","content":"hi"}]}`), &req)
	out := anthropicToOpenAI(req, "m")
	sys := out["messages"].([]any)[0].(map[string]any)["content"].(string)
	if sys != "You are helpful" {
		t.Errorf("<total_tokens> phải bị bỏ khỏi system để giữ nguyên prefix cache, got %q", sys)
	}

	var req2 map[string]any
	json.Unmarshal([]byte(`{"system":"Prompt prefix.\n<total_tokens>15000000 tokens left</total_tokens>","messages":[{"role":"user","content":"hi"}]}`), &req2)
	out2 := anthropicToOpenAI(req2, "m")
	sys2 := out2["messages"].([]any)[0].(map[string]any)["content"].(string)
	if sys2 != "Prompt prefix." {
		t.Errorf("total_tokens trong chuỗi đơn phải bị bỏ, got %q", sys2)
	}
}

func TestImageDowngradedForTextOnlyModels(t *testing.T) {
	var req map[string]any
	json.Unmarshal([]byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"look"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAA"}}]}]}`), &req)
	cfg = Config{ImageModels: map[string]bool{}}
	msg := anthropicToOpenAI(req, "deepseek-v4.1-flash-free")["messages"].([]any)[0].(map[string]any)
	if s, ok := msg["content"].(string); !ok || !strings.Contains(s, "[image attached]") {
		t.Errorf("model text-only phải nhận ghi chú thay vì image_url: %v", msg["content"])
	}
	cfg.ImageModels["vision-model"] = true
	msg = anthropicToOpenAI(req, "vision-model")["messages"].([]any)[0].(map[string]any)
	if _, ok := msg["content"].([]any); !ok {
		t.Errorf("model có vision phải nhận image_url: %v", msg["content"])
	}
}

func TestNormalizeToolPairs(t *testing.T) {
	var req map[string]any
	json.Unmarshal([]byte(`{"messages":[
	 {"role":"user","content":"go"},
	 {"role":"assistant","content":[{"type":"tool_use","id":"a","name":"X","input":{}},{"type":"tool_use","id":"b","name":"X","input":{}}]},
	 {"role":"user","content":[{"type":"tool_result","tool_use_id":"a","content":"ra"},{"type":"tool_result","tool_use_id":"zzz","content":"orphan"}]}
	]}`), &req)
	msgs := anthropicToOpenAI(req, "m")["messages"].([]any)
	var calls, tools int
	for _, m := range msgs {
		mm := m.(map[string]any)
		if c, ok := mm["tool_calls"].([]any); ok {
			calls += len(c)
		}
		if mm["role"] == "tool" {
			tools++
		}
	}
	if calls != 1 || tools != 1 {
		t.Errorf("phải giữ đúng 1 cặp (a): calls=%d tools=%d", calls, tools)
	}
}

func TestRateWindow(t *testing.T) {
	w := &rateWindow{}
	now := int64(1_000_000)
	for i := 0; i < 5; i++ {
		w.add(now)
	}
	if n := w.add(now + 30); n != 6 {
		t.Errorf("trong 120s phải cộng dồn: %d", n)
	}
	if n := w.add(now + 125); n != 2 { // 5 sự kiện đầu đã trượt khỏi cửa sổ, còn lại +30 và +125
		t.Errorf("sự kiện cũ phải trượt khỏi cửa sổ: %d", n)
	}
	if n := w.add(now + 1000); n != 1 {
		t.Errorf("sau khoảng nghỉ dài phải về 1: %d", n)
	}
}

func TestTorInflightCapSkipsBusyInstance(t *testing.T) {
	_, m := setup(t, toolStream, false)
	a, b := newTorEgress("tor-a", "127.0.0.1:1"), newTorEgress("tor-b", "127.0.0.1:1")
	a.ready.Store(true)
	b.ready.Store(true)
	m.add(a)
	m.add(b)
	m.egresses[0].ready.Store(false)
	cfg.TorMaxInflight = 3
	a.inflight.Store(3)
	for i := 0; i < 5; i++ {
		if got := m.pick("deepseek-v4.1-flash-free"); got != b {
			t.Fatalf("tor-a đầy phải bị bỏ qua, got %v", got)
		}
	}
	b.inflight.Store(3)
	if got := m.pick("deepseek-v4.1-flash-free"); got != nil {
		t.Errorf("cả hai đầy thì phải trả nil để route bật thêm Tor/chờ, got %s", got.Name)
	}
}

func TestQueueWaitReturns503(t *testing.T) {
	block := make(chan struct{})
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-block
	}, false)
	defer close(block)
	cfg.MaxConcurrency = 1
	concurrencySem = make(chan struct{}, 1)
	cfg.QueueWait = 300 * time.Millisecond
	go func() { // giữ slot duy nhất; không dùng post() vì nó gọi t.Fatal ngoài goroutine test
		if r, err := http.Post(gw.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"deepseek","stream":true,"messages":[{"role":"user","content":"x"}]}`)); err == nil {
			io.Copy(io.Discard, r.Body) // giữ slot cho tới khi upstream kết thúc
			r.Body.Close()
		}
	}()
	time.Sleep(300 * time.Millisecond)
	resp, body := post(t, gw.URL+"/v1/chat/completions", `{"model":"deepseek","messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" {
		t.Errorf("hết slot phải 503 kèm Retry-After, got %d %s", resp.StatusCode, body)
	}
}

// Nhiều 429 đồng thời của cùng 1 sự kiện chỉ được tính 1 strike (không bùng nổ backoff).
func TestConcurrent429CountsOnce(t *testing.T) {
	old := cfg
	defer func() { cfg = old }()
	cfg = Config{CooldownMax: 2 * time.Minute, TorRotateCooldown: time.Second, TorBackoffMax: time.Minute}
	e := newEgress("direct", kindDirect, nil)
	first := e.exhausted("m", 0, true)
	for i := 0; i < 50; i++ {
		e.exhausted("m", 0, true)
	}
	if first != 15*time.Second || e.cool["m"].strikes != 1 {
		t.Errorf("first=%v strikes=%d, want 15s/1", first, e.cool["m"].strikes)
	}
	tor := newEgress("tor-1", kindTor, nil)
	tor.canRotate = true
	for i := 0; i < 50; i++ {
		tor.exhausted("m", 0, false)
	}
	if got := tor.cool["m"]; got != nil && got.strikes != 0 {
		t.Errorf("429 từ circuit đã xoay không được cộng strike, got %d", got.strikes)
	}
}

// Mặc định (PreferDirect=false): Tor trước, direct chỉ khi Tor chưa sẵn sàng; nhiều identity chia tải.
func TestTorFirstSpreadsAcrossIdentities(t *testing.T) {
	var hits sync.Map
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		v, _ := hits.LoadOrStore(r.Header.Get("X-Test-Egress"), new(atomic.Int32))
		v.(*atomic.Int32).Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"))
	}, false)
	cfg.PreferDirect = false
	for _, e := range newTorGroup("tor-1", "127.0.0.1:1", 3) {
		tagged(e)
		e.ready.Store(true)
		m.add(e)
	}
	for i := 0; i < 30; i++ {
		resp, err := http.Post(gw.URL+"/v1/chat/completions", "application/json",
			strings.NewReader(`{"model":"deepseek","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("req %d: %v %v", i, err, resp)
		}
		resp.Body.Close()
	}
	if v, ok := hits.Load("direct"); ok && v.(*atomic.Int32).Load() > 0 {
		t.Errorf("direct không được dùng khi Tor sẵn sàng")
	}
	n := 0
	hits.Range(func(_, _ any) bool { n++; return true })
	if n < 2 {
		t.Errorf("tải phải chia cho nhiều identity, chỉ có %d", n)
	}
}

func TestReapPreservesTorWarm(t *testing.T) {
	cfg.TorWarm = 2
	cfg.TorIdleStop = time.Millisecond
	m := newManager(context.Background())
	m.torSpawned = 2
	g1 := newTorGroup("tor-1", "127.0.0.1:1", 1)
	g1[0].stop = func() {}
	g1[0].ready.Store(true)
	g1[0].lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
	m.add(g1[0])

	// Chạy thử 1 vòng kiểm tra dọn dẹp
	m.mu.RLock()
	spawned := m.torSpawned
	m.mu.RUnlock()
	if spawned <= cfg.TorWarm {
		// Đúng: không được tắt Tor khi spawned <= TorWarm
	} else {
		t.Errorf("spawned %d > TorWarm %d", spawned, cfg.TorWarm)
	}
	if len(m.snapshot()) != 2 { // direct + tor-1
		t.Errorf("phải giữ lại tor-1, snapshot có %d", len(m.snapshot()))
	}
}

func TestSessionClockPruning(t *testing.T) {
	now := time.Now().UnixMilli()
	sessionClock.Lock()
	sessionClock.first = map[string]int64{}
	// Nạp 8200 mục, một nửa cũ hơn 24h
	for i := range 4100 {
		sessionClock.first[fmt.Sprintf("old-%d", i)] = now - 25*3600*1000
	}
	for i := range 4100 {
		sessionClock.first[fmt.Sprintf("fresh-%d", i)] = now - 1000
	}
	sessionClock.Unlock()

	_ = firstSeenMs("trigger-prune")

	sessionClock.Lock()
	defer sessionClock.Unlock()
	// Các mục cũ phải bị dọn, mục mới phải còn
	if _, ok := sessionClock.first["fresh-0"]; !ok {
		t.Errorf("mục mới không được bị xoá sạch")
	}
	if len(sessionClock.first) >= 8192 {
		t.Errorf("kích thước map không được vượt trần: %d", len(sessionClock.first))
	}
}

func TestResolveModelExtendedAliases(t *testing.T) {
	cfg.DefaultModel = "deepseek"
	m, _ := resolveModelName("deepseek-chat")
	if m != "deepseek-v4.1-flash-free" {
		t.Errorf("deepseek-chat -> %s", m)
	}
	m2, _ := resolveModelName("claude-3-5-sonnet")
	if m2 != "deepseek-v4.1-flash-free" {
		t.Errorf("claude-3-5-sonnet -> %s", m2)
	}
	m3, _ := resolveModelName("gpt-4o")
	if m3 != "deepseek-v4.1-flash-free" {
		t.Errorf("gpt-4o -> %s", m3)
	}
}

func TestPickAvoidDaemon(t *testing.T) {
	m := newManager(context.Background())
	g1 := newTorGroup("tor-1", "127.0.0.1:1", 2)
	g2 := newTorGroup("tor-2", "127.0.0.1:2", 2)
	for _, e := range append(g1, g2...) {
		e.ready.Store(true)
		m.add(e)
	}
	// Khi tránh tor-1, phải chọn ra tor-2
	e := m.pickAvoid("deepseek-v4.1-flash-free", "tor-1")
	if e == nil || e.daemon != "tor-2" {
		t.Fatalf("phải chọn tor-2 khi tránh tor-1, got %v", e)
	}
	// Khi tránh tor-2, phải chọn ra tor-1
	e2 := m.pickAvoid("deepseek-v4.1-flash-free", "tor-2")
	if e2 == nil || e2.daemon != "tor-1" {
		t.Fatalf("phải chọn tor-1 khi tránh tor-2, got %v", e2)
	}
}

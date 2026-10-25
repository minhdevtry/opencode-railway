// loadgen phát lại workload thật (gán nhãn ticket của operationbot, hoặc văn bản dài tổng hợp) vào gateway
// OpenAI-compatible, đo tỉ lệ thành công, độ trễ, thông lượng và phân bố egress/model.
//
//	go run ./research/loadgen -n 2000 -c 1000 -workload label
//	go run ./research/loadgen -n 500 -c 500 -workload long -long-kb 24
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const labelSystem = `Bạn là chuyên gia quản lý vận hành trường học Vuihoc phụ trác chuẩn hoá Ticket trên Itsaplan.
Nhiệm vụ: Phân tích tin nhắn của người gửi, tạo ra:
1. "normalized_title": Tiêu đề ticket chuẩn theo mẫu: "[Phòng ban][Nghiệp vụ] [Mã HS/Lớp nếu có] — Hành động tóm tắt ngắn gọn dưới 80 ký tự".
2. "category": Nhóm nghiệp vụ chính (Vận hành lớp học, Xếp lớp, Tài chính, Vận hành giáo viên, Kỹ thuật).
3. "labels": Mảng nhãn nghiệp vụ chi tiết (ví dụ: ["doi-lich", "xep-lop-moi", "loi-classin", "cam-mic", "hoc-sinh-vang", "bu-buoi", "hoan-phi", "cham-cong", "khan-cap"]).
4. "is_urgent": true nếu tin nhắn báo sự cố đang xảy ra NGAY lúc này trong ca học (cam/mic hỏng, lớp vỡ, học sinh mất tích, cần xử lý tức thì); false nếu là yêu cầu thông thường.

Trả về JSON duy nhất (không giải thích):
{"normalized_title": "...", "category": "...", "labels": ["..."], "is_urgent": false}`

const longSystem = `Bạn là trợ lý phân tích vận hành. Đọc toàn bộ nhật ký tin nhắn hỗ trợ bên dưới và trả về JSON duy nhất:
{"tong_so_tin": <số>, "chu_de_chinh": ["..."], "su_co_khan_cap": ["..."], "tom_tat": "<tối đa 120 từ>"}`

type caseRec struct {
	ID      string `json:"case_id"`
	Group   string `json:"group"`
	Message string `json:"message"`
}

type result struct {
	Idx     int     `json:"i"`
	Status  int     `json:"status"`
	Secs    float64 `json:"secs"`
	TTFB    float64 `json:"ttfb,omitempty"`
	Model   string  `json:"model,omitempty"`
	Egress  string  `json:"egress,omitempty"`
	OutTok  int     `json:"out_tok,omitempty"`
	InTok   int     `json:"in_tok,omitempty"`
	ValidJS bool    `json:"valid_json"`
	Err     string  `json:"err,omitempty"`
}

var jsonObj = regexp.MustCompile(`(?s)\{.*\}`)

func main() {
	url := flag.String("url", "http://127.0.0.1:8090/v1/chat/completions", "endpoint")
	model := flag.String("model", "auto", "model")
	n := flag.Int("n", 200, "tổng số request")
	c := flag.Int("c", 50, "số request đồng thời tối đa (= n để bắn dồn một lần)")
	workload := flag.String("workload", "label", "label | long | write | smoke")
	headers := flag.String("H", "", "header thêm, dạng 'K: V' (vd 'X-Model-Fallback: deny')")
	longKB := flag.Int("long-kb", 16, "kích thước văn bản cho workload long (KB)")
	maxTok := flag.Int("max-tokens", 512, "max_tokens")
	timeout := flag.Duration("timeout", 15*time.Minute, "timeout mỗi request (phía client)")
	data := flag.String("data", "research/data/archive_cases.json", "dataset")
	out := flag.String("out", "", "file JSONL kết quả (mặc định research/results/<ts>-<workload>.jsonl)")
	rps := flag.Float64("rps", 0, "giới hạn tốc độ gửi (0 = không giới hạn)")
	stream := flag.Bool("stream", false, "gửi stream=true")
	flag.Parse()

	cases := loadCases(*data)
	if len(cases) == 0 {
		fmt.Fprintln(os.Stderr, "dataset rỗng")
		os.Exit(1)
	}
	rng := rand.New(rand.NewSource(42))
	rng.Shuffle(len(cases), func(i, j int) { cases[i], cases[j] = cases[j], cases[i] })

	if *out == "" {
		*out = filepath.Join("research/results", fmt.Sprintf("%s-%s-n%d-c%d.jsonl", time.Now().Format("0102-150405"), *workload, *n, *c))
	}
	_ = os.MkdirAll(filepath.Dir(*out), 0o755)
	f, err := os.Create(*out)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	tr := &http.Transport{
		MaxIdleConns: 0, MaxIdleConnsPerHost: *c + 10, MaxConnsPerHost: 0,
		DialContext:     (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		IdleConnTimeout: 90 * time.Second,
	}
	client := &http.Client{Transport: tr, Timeout: *timeout}

	var (
		mu       sync.Mutex
		results  []result
		inflight atomic.Int32
		done     atomic.Int32
		okN      atomic.Int32
	)
	sem := make(chan struct{}, *c)
	var wg sync.WaitGroup
	start := time.Now()

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				el := time.Since(start).Seconds()
				fmt.Fprintf(os.Stderr, "[%5.0fs] done=%d/%d ok=%d inflight=%d  %.1f ok/s\n", el, done.Load(), *n, okN.Load(), inflight.Load(), float64(okN.Load())/el)
			}
		}
	}()

	var interval time.Duration
	if *rps > 0 {
		interval = time.Duration(float64(time.Second) / *rps)
	}
	for i := 0; i < *n; i++ {
		if interval > 0 && i > 0 {
			time.Sleep(interval)
		}
		sem <- struct{}{}
		wg.Add(1)
		body := buildBody(*workload, *model, *maxTok, *longKB, *stream, cases, i)
		go func(i int, body []byte) {
			defer func() { <-sem; wg.Done() }()
			inflight.Add(1)
			r := send(client, *url, body, *stream, *headers)
			inflight.Add(-1)
			r.Idx = i
			if r.Status == 200 {
				okN.Add(1)
			}
			done.Add(1)
			mu.Lock()
			results = append(results, r)
			b, _ := json.Marshal(r)
			w.Write(b)
			w.WriteByte('\n')
			mu.Unlock()
		}(i, body)
	}
	wg.Wait()
	close(stop)
	summarize(results, time.Since(start), *out, *workload, *n, *c)
}

func loadCases(path string) []caseRec {
	b, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var wrap struct {
		Cases []caseRec `json:"cases"`
	}
	if json.Unmarshal(b, &wrap) == nil && len(wrap.Cases) > 0 {
		return wrap.Cases
	}
	var arr []caseRec
	_ = json.Unmarshal(b, &arr)
	return arr
}

func buildBody(workload, model string, maxTok, longKB int, stream bool, cases []caseRec, i int) []byte {
	var msgs []map[string]string
	switch workload {
	case "long":
		var sb strings.Builder
		for j := 0; sb.Len() < longKB*1024; j++ {
			cs := cases[(i*97+j)%len(cases)]
			fmt.Fprintf(&sb, "[%d] (%s) %s\n", j+1, cs.Group, cs.Message)
		}
		msgs = []map[string]string{{"role": "system", "content": longSystem}, {"role": "user", "content": sb.String()}}
	case "write":
		// Output dài + cần suy luận: phân tích 1 nhóm ca hỗ trợ thành báo cáo ~600-900 từ.
		var sb strings.Builder
		for j := 0; j < 12; j++ {
			cs := cases[(i*13+j)%len(cases)]
			fmt.Fprintf(&sb, "- (%s) %s\n", cs.Group, cs.Message)
		}
		msgs = []map[string]string{{"role": "user", "content": "Dưới đây là 12 tin nhắn hỗ trợ vận hành. Viết báo cáo phân tích 600-900 từ: " +
			"phân nhóm vấn đề, nguyên nhân gốc có thể, mức độ ưu tiên, đề xuất quy trình xử lý cụ thể cho từng nhóm. Kết thúc bằng dòng HẾT.\n" + sb.String()}}
	case "smoke":
		msgs = []map[string]string{{"role": "user", "content": fmt.Sprintf("Trả lời đúng 1 từ: pong (%d)", i)}}
	default:
		cs := cases[i%len(cases)]
		msgs = []map[string]string{{"role": "system", "content": labelSystem},
			{"role": "user", "content": fmt.Sprintf("Nhóm: %s\nTin nhắn:\n\"%s\"", cs.Group, cs.Message)}}
	}
	body := map[string]any{"model": model, "messages": msgs, "max_tokens": maxTok, "temperature": 0.2}
	if stream {
		body["stream"] = true
	}
	b, _ := json.Marshal(body)
	return b
}

func send(client *http.Client, url string, body []byte, stream bool, extraHdr string) result {
	t0 := time.Now()
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if k, v, ok := strings.Cut(extraHdr, ":"); ok {
		req.Header.Set(strings.TrimSpace(k), strings.TrimSpace(v))
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{Status: 0, Secs: time.Since(t0).Seconds(), Err: trim(err.Error())}
	}
	defer resp.Body.Close()
	r := result{Status: resp.StatusCode, TTFB: time.Since(t0).Seconds(), Egress: resp.Header.Get("X-Gateway-Egress"), Model: resp.Header.Get("X-Gateway-Model")}
	raw, err := io.ReadAll(resp.Body)
	r.Secs = time.Since(t0).Seconds()
	if err != nil {
		r.Status, r.Err = 0, "read: "+trim(err.Error())
		return r
	}
	if resp.StatusCode != 200 {
		r.Err = trim(string(raw))
		return r
	}
	var content string
	if stream {
		for _, line := range strings.Split(string(raw), "\n") {
			if !strings.HasPrefix(line, "data: ") || strings.TrimSpace(line[6:]) == "[DONE]" {
				continue
			}
			var ch struct {
				Choices []struct {
					Delta struct{ Content string } `json:"delta"`
				} `json:"choices"`
				Usage *struct {
					P int `json:"prompt_tokens"`
					C int `json:"completion_tokens"`
				} `json:"usage"`
			}
			if json.Unmarshal([]byte(line[6:]), &ch) == nil {
				if len(ch.Choices) > 0 {
					content += ch.Choices[0].Delta.Content
				}
				if ch.Usage != nil {
					r.InTok, r.OutTok = ch.Usage.P, ch.Usage.C
				}
			}
		}
	} else {
		var cr struct {
			Model   string `json:"model"`
			Choices []struct {
				Message struct{ Content string } `json:"message"`
			} `json:"choices"`
			Usage struct {
				P int `json:"prompt_tokens"`
				C int `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(raw, &cr); err != nil {
			r.Status, r.Err = 0, "bad json: "+trim(string(raw))
			return r
		}
		if len(cr.Choices) > 0 {
			content = cr.Choices[0].Message.Content
		}
		if r.Model == "" {
			r.Model = cr.Model
		}
		r.InTok, r.OutTok = cr.Usage.P, cr.Usage.C
	}
	if m := jsonObj.FindString(content); m != "" {
		var v map[string]any
		r.ValidJS = json.Unmarshal([]byte(m), &v) == nil
	}
	if strings.TrimSpace(content) == "" {
		r.Err = "empty content"
	}
	return r
}

func trim(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 160 {
		return s[:160]
	}
	return s
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	return xs[int(float64(len(xs)-1)*p)]
}

func summarize(rs []result, el time.Duration, out, workload string, n, c int) {
	var lat []float64
	ok, valid, outTok := 0, 0, 0
	errs := map[string]int{}
	byModel := map[string]int{}
	byEgress := map[string]int{}
	for _, r := range rs {
		if r.Status == 200 && r.Err == "" {
			ok++
			lat = append(lat, r.Secs)
			outTok += r.OutTok
			if r.ValidJS {
				valid++
			}
			byModel[r.Model]++
			byEgress[r.Egress]++
		} else {
			k := fmt.Sprintf("%d %s", r.Status, r.Err)
			if len(k) > 90 {
				k = k[:90]
			}
			errs[k]++
		}
	}
	sort.Float64s(lat)
	s := map[string]any{
		"workload": workload, "n": n, "c": c, "elapsed_s": round(el.Seconds()), "ok": ok,
		"success_rate": round(float64(ok) / float64(max(1, len(rs)))), "valid_json": valid,
		"ok_per_s": round(float64(ok) / el.Seconds()), "out_tok_per_s": round(float64(outTok) / el.Seconds()),
		"p50_s": round(pct(lat, .5)), "p95_s": round(pct(lat, .95)), "max_s": round(pct(lat, 1)),
		"errors": errs, "by_model": byModel, "egress_used": len(byEgress), "out": out,
	}
	b, _ := json.MarshalIndent(s, "", "  ")
	fmt.Println(string(b))
	_ = os.WriteFile(strings.TrimSuffix(out, ".jsonl")+".summary.json", b, 0o644)
}

func round(x float64) float64 { return float64(int(x*100+0.5)) / 100 }

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ======================== HÀNG ĐỢI JOB BỀN (REDIS) ========================
//
// Dành cho xử lý lô lớn (hàng nghìn tới hàng trăm nghìn request): client đẩy cả lô vào 1 lần, gateway tự chạy ở
// tốc độ tối đa mà egress chịu được, lỗi tạm thời (429/5xx/timeout) thì xếp lại có backoff chứ không bao giờ bỏ,
// gateway khởi động lại vẫn chạy tiếp (job đang chạy dở bị thu hồi qua lease). Client lấy kết quả sau.
//
//	POST /v1/batches            {"requests":[{"custom_id":"a1","body":{...chat body...}}, ...]}  -> {"id":"batch_..."}
//	GET  /v1/batches/{id}       tiến độ
//	GET  /v1/batches/{id}/results?offset=0&limit=1000   kết quả theo thứ tự gửi
//	POST /v1/jobs               {...chat body...}  -> {"id":"job_..."}   (1 request lẻ)
//	GET  /v1/jobs/{id}?wait=60  long-poll tới khi xong
//
// Khoá Redis: orw:q (list id chờ), orw:delay (zset id->thời điểm thử lại), orw:lease (zset id->hạn lease),
// orw:job:{id} (hash), orw:batch:{id} (hash), orw:batch:{id}:items (list id theo thứ tự).

var (
	rdb         *redisPool
	jobsRunning atomic.Int32
	jobsDone    atomic.Int64
	jobsFailed  atomic.Int64
	jobsRetried atomic.Int64
)

const (
	jobTTL   = 7 * 24 * time.Hour
	jobLease = 15 * time.Minute
)

func startJobs(ctx context.Context) {
	if cfg.RedisURL == "" {
		log.Printf("[Jobs] REDIS_URL trống => tắt hàng đợi job bền (/v1/batches, /v1/jobs)")
		return
	}
	p, err := newRedisPool(cfg.RedisURL)
	if err != nil {
		log.Printf("[Jobs] REDIS_URL sai: %v", err)
		return
	}
	if _, err := p.Do(ctx, 0, "PING"); err != nil {
		log.Printf("[Jobs] Không kết nối được Redis %s: %v (job tắt)", cfg.RedisURL, err)
		return
	}
	rdb = p
	// Job còn nằm ở orw:proc là của lần chạy trước bị ngắt giữa BLMOVE và lúc giành (1 instance gateway / 1 Redis).
	for {
		v, err := rdb.Do(ctx, 0, "LMOVE", "orw:proc", "orw:q", "LEFT", "RIGHT")
		if err != nil || v == nil {
			break
		}
	}
	log.Printf("[Jobs] Redis sẵn sàng, %d worker", cfg.JobWorkers)
	go jobMover(ctx)
	for i := 0; i < cfg.JobWorkers; i++ {
		go jobWorker(ctx)
	}
}

func newJobID(prefix string) string { return prefix + "_" + makeID(false) }

// enqueueJob lưu job và đẩy vào hàng chờ.
func enqueueJob(ctx context.Context, id, batch, customID string, body []byte) error {
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)
	key := "orw:job:" + id
	if _, err := rdb.Do(ctx, 0, "HSET", key, "body", string(body), "batch", batch, "custom_id", customID,
		"status", "queued", "attempts", "0", "created", now, "updated", now); err != nil {
		return err
	}
	_, err := rdb.Do(ctx, 0, "LPUSH", "orw:q", id)
	return err
}

// moveDue chuyển nguyên tử (Lua) các id tới hạn từ 1 zset sang đầu lấy của orw:q.
const luaMoveDue = `local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 500)
for _, id in ipairs(ids) do redis.call('ZREM', KEYS[1], id); redis.call('RPUSH', KEYS[2], id) end
return #ids`

// jobMover: chuyển job tới hạn thử lại từ orw:delay sang orw:q, và thu hồi job có lease hết hạn (worker chết).
func jobMover(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := strconv.FormatInt(time.Now().UnixMilli(), 10)
		_, _ = rdb.Do(ctx, 0, "EVAL", luaMoveDue, "2", "orw:delay", "orw:q", now)
		if n, err := rdb.Do(ctx, 0, "EVAL", luaMoveDue, "2", "orw:lease", "orw:q", now); err == nil && rInt(n) > 0 {
			log.Printf("[Jobs] %d job hết lease (worker dừng giữa chừng), xếp lại", rInt(n))
		}
	}
}

// jobWorker lấy job bằng BLMOVE (orw:q -> orw:proc, nguyên tử: job không lúc nào nằm ngoài Redis).
func jobWorker(ctx context.Context) {
	for ctx.Err() == nil {
		v, err := rdb.Do(ctx, 10*time.Second, "BLMOVE", "orw:q", "orw:proc", "RIGHT", "LEFT", "5")
		if err != nil {
			if err != errRedisNil {
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
				}
			}
			continue
		}
		runJob(ctx, rStr(v))
	}
}

// luaClaim giành job nguyên tử: bỏ qua nếu đã xong, hoặc đang chạy mà lease còn (worker khác đang giữ). Thành công
// thì đặt running + lease + tăng attempts, và bỏ id khỏi orw:proc (từ giờ lease bảo vệ job).
const luaClaim = `local st = redis.call('HGET', KEYS[1], 'status')
redis.call('LREM', KEYS[3], 1, ARGV[1])
if (not st) or st == 'done' or st == 'failed' then return 0 end
if st == 'running' and redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'status', 'running')
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return redis.call('HINCRBY', KEYS[1], 'attempts', 1)`

func runJob(ctx context.Context, id string) {
	key := "orw:job:" + id
	lease := func() string { return strconv.FormatInt(time.Now().Add(jobLease).UnixMilli(), 10) }
	v, err := rdb.Do(ctx, 0, "EVAL", luaClaim, "3", key, "orw:lease", "orw:proc", id, lease())
	if err != nil {
		log.Printf("[Jobs] giành %s lỗi: %v (để lại orw:proc, sẽ được thu hồi khi khởi động)", id, err)
		return
	}
	attempts := int(rInt(v))
	if attempts == 0 {
		return
	}
	jv, err := rdb.Do(ctx, 0, "HMGET", key, "body", "batch")
	arr, _ := jv.([]any)
	if err != nil || len(arr) != 2 {
		_, _ = rdb.Do(ctx, 0, "ZADD", "orw:lease", "0", id) // để mover xếp lại ngay
		return
	}
	body, batch := rStr(arr[0]), rStr(arr[1])

	// Heartbeat gia hạn lease trong lúc chạy (job có thể chờ egress rất lâu).
	hbCtx, hbStop := context.WithCancel(context.Background())
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				_, _ = rdb.Do(hbCtx, 0, "ZADD", "orw:lease", "XX", lease(), id)
			}
		}
	}()
	jobsRunning.Add(1)
	status, out := execInternal(ctx, body)
	jobsRunning.Add(-1)
	hbStop()
	bg := context.Background() // ghi kết quả kể cả khi gateway đang tắt
	now := strconv.FormatInt(time.Now().UnixMilli(), 10)

	if ctx.Err() != nil {
		// Gateway đang tắt: không chốt kết quả, trả job về hàng chờ để lần chạy sau làm tiếp.
		_, _ = rdb.Do(bg, 0, "HSET", key, "status", "queued", "attempts", strconv.Itoa(attempts-1), "updated", now)
		_, _ = rdb.Do(bg, 0, "RPUSH", "orw:q", id)
		_, _ = rdb.Do(bg, 0, "ZREM", "orw:lease", id)
		return
	}
	if status == http.StatusOK && len(bytes.TrimSpace(out)) == 0 {
		status = 0 // handler không ghi gì (vd context kết thúc giữa chừng): coi là lỗi tạm thời
	}
	retryable := status == 0 || status == 499 || status == http.StatusTooManyRequests || status >= 500
	switch {
	case status == http.StatusOK:
		_, _ = rdb.Do(bg, 0, "HSET", key, "status", "done", "http_status", "200", "result", string(out), "updated", now)
		_, _ = rdb.Do(bg, 0, "EXPIRE", key, strconv.Itoa(int(jobTTL.Seconds())))
		_, _ = rdb.Do(bg, 0, "ZREM", "orw:lease", id)
		jobsDone.Add(1)
		bumpBatch(bg, batch, "done")
	case retryable && attempts < cfg.JobMaxAttempts:
		// Backoff mũ có trần: 2s, 4s, 8s ... tối đa JobMaxBackoff. Hết quota cả ngày thì cứ chờ, không bỏ job.
		d := 2 * time.Second << min(attempts-1, 10)
		if d > cfg.JobMaxBackoff {
			d = cfg.JobMaxBackoff
		}
		at := strconv.FormatInt(time.Now().Add(d).UnixMilli(), 10)
		_, _ = rdb.Do(bg, 0, "HSET", key, "status", "queued", "last_error", trimErr(out), "http_status", strconv.Itoa(status), "updated", now)
		_, _ = rdb.Do(bg, 0, "ZADD", "orw:delay", at, id) // vào delay trước rồi mới bỏ lease: không có khoảng hở
		_, _ = rdb.Do(bg, 0, "ZREM", "orw:lease", id)
		jobsRetried.Add(1)
	default:
		_, _ = rdb.Do(bg, 0, "HSET", key, "status", "failed", "http_status", strconv.Itoa(status), "error", string(out), "updated", now)
		_, _ = rdb.Do(bg, 0, "EXPIRE", key, strconv.Itoa(int(jobTTL.Seconds())))
		_, _ = rdb.Do(bg, 0, "ZREM", "orw:lease", id)
		jobsFailed.Add(1)
		bumpBatch(bg, batch, "failed")
	}
}

func trimErr(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}

func bumpBatch(ctx context.Context, batch, field string) {
	if batch == "" {
		return
	}
	_, _ = rdb.Do(ctx, 0, "HINCRBY", "orw:batch:"+batch, field, "1")
}

// execInternal chạy body qua đúng chatHandler (cùng định tuyến, auto, kiểm tra kết quả, retry) với độ ưu tiên thấp
// hơn request đồng bộ. stream bị tắt vì kết quả lưu nguyên khối.
func execInternal(ctx context.Context, body string) (int, []byte) {
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		return http.StatusBadRequest, []byte(`{"error":{"message":"body không phải JSON"}}`)
	}
	delete(m, "stream")
	b, _ := json.Marshal(m)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(b)).WithContext(withLowPriority(ctx))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	chatHandler(rec, req)
	return rec.Code, rec.Body.Bytes()
}

// ======================== HTTP ========================

func jobsUnavailable(w http.ResponseWriter) bool {
	if rdb == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "unavailable", "Job queue disabled: set REDIS_URL")
		return true
	}
	return false
}

func batchesHandler(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if jobsUnavailable(w) {
		return
	}
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/batches"), "/")
	ctx := r.Context()
	switch {
	case rest == "" && r.Method == http.MethodPost:
		var in struct {
			Requests []struct {
				CustomID string          `json:"custom_id"`
				Body     json.RawMessage `json:"body"`
			} `json:"requests"`
		}
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 512<<20))
		if err := dec.Decode(&in); err != nil || len(in.Requests) == 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid_request_error", "cần {\"requests\":[{\"custom_id\":..,\"body\":{..}}]}")
			return
		}
		id := newJobID("batch")
		now := strconv.FormatInt(time.Now().UnixMilli(), 10)
		if _, err := rdb.Do(ctx, 0, "HSET", "orw:batch:"+id, "total", strconv.Itoa(len(in.Requests)), "done", "0", "failed", "0", "created", now); err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, "redis_error", err.Error())
			return
		}
		ids := make([]string, 0, len(in.Requests))
		for i, rq := range in.Requests {
			jid := newJobID("job")
			cid := rq.CustomID
			if cid == "" {
				cid = strconv.Itoa(i)
			}
			if err := enqueueJob(ctx, jid, id, cid, rq.Body); err != nil {
				writeJSONError(w, http.StatusServiceUnavailable, "redis_error", err.Error())
				return
			}
			ids = append(ids, jid)
			if len(ids) == 1000 || i == len(in.Requests)-1 {
				_, _ = rdb.Do(ctx, 0, append([]string{"RPUSH", "orw:batch:" + id + ":items"}, ids...)...)
				ids = ids[:0]
			}
		}
		_, _ = rdb.Do(ctx, 0, "EXPIRE", "orw:batch:"+id, strconv.Itoa(int(jobTTL.Seconds())))
		_, _ = rdb.Do(ctx, 0, "EXPIRE", "orw:batch:"+id+":items", strconv.Itoa(int(jobTTL.Seconds())))
		writeJSON(w, map[string]any{"id": id, "object": "batch", "total": len(in.Requests), "status": "queued"})
	case rest != "" && !strings.Contains(rest, "/") && r.Method == http.MethodGet:
		v, err := rdb.Do(ctx, 0, "HGETALL", "orw:batch:"+rest)
		b := rMap(v)
		if err != nil || len(b) == 0 {
			writeJSONError(w, http.StatusNotFound, "not_found", "batch không tồn tại")
			return
		}
		total, done, failed := rInt(b["total"]), rInt(b["done"]), rInt(b["failed"])
		status := "running"
		if done+failed >= total {
			status = "completed"
		}
		writeJSON(w, map[string]any{"id": rest, "object": "batch", "status": status, "total": total, "done": done, "failed": failed,
			"pending": total - done - failed, "created_ms": rInt(b["created"])})
	case strings.HasSuffix(rest, "/results") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(rest, "/results")
		off, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		lim, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if lim <= 0 || lim > 5000 {
			lim = 1000
		}
		v, err := rdb.Do(ctx, 0, "LRANGE", "orw:batch:"+id+":items", strconv.Itoa(off), strconv.Itoa(off+lim-1))
		if err != nil {
			writeJSONError(w, http.StatusNotFound, "not_found", "batch không tồn tại")
			return
		}
		arr, _ := v.([]any)
		ids := make([]string, len(arr))
		for i, raw := range arr {
			ids[i] = rStr(raw)
		}
		writeJSON(w, map[string]any{"object": "list", "offset": off, "data": jobViews(ctx, ids)})
	default:
		writeJSONError(w, http.StatusNotFound, "not_found", "đường dẫn không hợp lệ")
	}
}

func jobsHandler(w http.ResponseWriter, r *http.Request) {
	setCORS(w)
	if jobsUnavailable(w) {
		return
	}
	id := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/jobs"), "/")
	ctx := r.Context()
	switch {
	case id == "" && r.Method == http.MethodPost:
		body, ok := readBody(w, r)
		if !ok {
			return
		}
		b, _ := json.Marshal(body)
		jid := newJobID("job")
		if err := enqueueJob(ctx, jid, "", "", b); err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, "redis_error", err.Error())
			return
		}
		writeJSON(w, map[string]any{"id": jid, "object": "job", "status": "queued"})
	case id != "" && r.Method == http.MethodGet:
		wait, _ := strconv.Atoi(r.URL.Query().Get("wait"))
		deadline := time.Now().Add(time.Duration(min(wait, 300)) * time.Second)
		for {
			jv := jobView(ctx, id, true)
			st, _ := jv["status"].(string)
			if st == "" {
				writeJSONError(w, http.StatusNotFound, "not_found", "job không tồn tại")
				return
			}
			if st == "done" || st == "failed" || time.Now().After(deadline) {
				writeJSON(w, jv)
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
	default:
		writeJSONError(w, http.StatusNotFound, "not_found", "đường dẫn không hợp lệ")
	}
}

func jobView(ctx context.Context, id string, withBody bool) map[string]any {
	v, _ := rdb.Do(ctx, 0, "HGETALL", "orw:job:"+id)
	j := rMap(v)
	out := map[string]any{"id": id, "custom_id": j["custom_id"], "status": j["status"], "attempts": rInt(j["attempts"])}
	if j["result"] != "" {
		out["response"] = json.RawMessage(j["result"])
	}
	if j["error"] != "" {
		out["error"] = j["error"]
	} else if j["last_error"] != "" && j["status"] != "done" {
		out["last_error"] = j["last_error"]
	}
	if withBody && j["batch"] != "" {
		out["batch"] = j["batch"]
	}
	return out
}

// jobViews đọc nhiều job song song (8 luồng), giữ đúng thứ tự: trang 1000-5000 kết quả trả về trong vài chục ms
// thay vì hàng nghìn lượt hỏi Redis tuần tự. (Ý tưởng từ phiên nghiên cứu omp 2026-10-25.)
func jobViews(ctx context.Context, ids []string) []any {
	out := make([]any, len(ids))
	ch := make(chan int, len(ids))
	for i := range ids {
		ch <- i
	}
	close(ch)
	var wg sync.WaitGroup
	for range min(8, len(ids)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range ch {
				out[i] = jobView(ctx, ids[i], false)
			}
		}()
	}
	wg.Wait()
	return out
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	b, _ := json.Marshal(v)
	_, _ = w.Write(b)
}

func jobStats(ctx context.Context) map[string]any {
	if rdb == nil {
		return map[string]any{"enabled": false}
	}
	q, _ := rdb.Do(ctx, 0, "LLEN", "orw:q")
	d, _ := rdb.Do(ctx, 0, "ZCARD", "orw:delay")
	l, _ := rdb.Do(ctx, 0, "ZCARD", "orw:lease")
	return map[string]any{"enabled": true, "workers": cfg.JobWorkers, "running": jobsRunning.Load(), "queued": rInt(q),
		"delayed": rInt(d), "leased": rInt(l), "done": jobsDone.Load(), "failed": jobsFailed.Load(), "retries": jobsRetried.Load()}
}

type prioKey struct{}

// withLowPriority đánh dấu request nội bộ của job: nhường egress cho request đồng bộ khi phải xếp hàng.
func withLowPriority(ctx context.Context) context.Context {
	return context.WithValue(ctx, prioKey{}, 1)
}

func isLowPriority(ctx context.Context) bool { v, _ := ctx.Value(prioKey{}).(int); return v == 1 }

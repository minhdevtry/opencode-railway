package main

// ======================== REGRESSION TESTS FOR THE DURABLE JOB QUEUE ========================
//
// jobs.go / redis.go previously had no test coverage (see PRIVATE-BRANCH.md).
// These tests run the real handlers and workers against an in-process Redis
// (miniredis_test.go), covering: enqueue/claim/complete, retry with backoff,
// permanent failure, lease reclaim, shutdown requeue, batch accounting and
// the HTTP surface.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func jobsTestConfig() {
	cfg.JobWorkers = 2
	cfg.JobMaxAttempts = 3
	cfg.JobMaxBackoff = 50 * time.Millisecond
	jobsDone.Store(0)
	jobsFailed.Store(0)
	jobsRetried.Store(0)
}

// quotaBlockedConfig makes a single-egress test gateway surface quota 429s
// fast: long retry-after trips the all-egress-exhausted shortcut instead of
// burning the whole MaxWait on per-model cooldown waits.
func quotaBlockedConfig() {
	cfg.MaxWait = 500 * time.Millisecond
	cfg.QueueWait = 500 * time.Millisecond
	cfg.ModelRenewCooldown = time.Hour
	cfg.CooldownMax = time.Minute
}

// okChatBody is a minimal chat request the fake upstream always answers.
const okChatBody = `{"model":"auto","messages":[{"role":"user","content":"hi"}]}`

func TestJobHappyPath(t *testing.T) {
	mr := testRedis(t)
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"choices":[{"delta":{"content":"hello"},"finish_reason":null}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`)
	}, false)
	jobsTestConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := enqueueJob(ctx, "job_ok", "", "c1", []byte(okChatBody)); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(ctx)
	go jobWorker(wctx)
	t.Cleanup(func() { wcancel(); time.Sleep(50 * time.Millisecond) })
	waitFor(t, 5*time.Second, func() bool { return mr.hgetall("orw:job:job_ok")["status"] == "done" })

	j := mr.hgetall("orw:job:job_ok")
	if j["http_status"] != "200" || !strings.Contains(j["result"], "hello") {
		t.Fatalf("expected stored 200 result, got %+v", j)
	}
	if _, leased := mr.zscore("orw:lease", "job_ok"); leased {
		t.Fatal("lease must be removed after completion")
	}
	if mr.llen("orw:proc") != 0 {
		t.Fatal("orw:proc must be empty after claim")
	}
}

func TestJobRetriesWithBackoffThenSucceeds(t *testing.T) {
	mr := testRedis(t)
	var calls atomic.Int32
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"Free usage limit reached","type":"FreeUsageLimitError"}}`)
			return
		}
		sse(w, `{"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}`)
	}, false)
	jobsTestConfig()
	quotaBlockedConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := enqueueJob(ctx, "job_retry", "", "", []byte(okChatBody)); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(ctx)
	go jobWorker(wctx)
	go jobMover(wctx)
	t.Cleanup(func() { wcancel(); time.Sleep(50 * time.Millisecond) })
	waitFor(t, 10*time.Second, func() bool { return mr.hgetall("orw:job:job_retry")["status"] == "done" })

	j := mr.hgetall("orw:job:job_retry")
	if j["attempts"] != "3" {
		t.Fatalf("expected 3 attempts, got %s", j["attempts"])
	}
	if jobsRetried.Load() < 2 {
		t.Fatalf("expected >=2 recorded retries, got %d", jobsRetried.Load())
	}
}

func TestJobPermanentFailureAfterMaxAttempts(t *testing.T) {
	mr := testRedis(t)
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"error":{"message":"Free usage limit reached","type":"FreeUsageLimitError"}}`)
	}, false)
	jobsTestConfig()
	quotaBlockedConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := enqueueJob(ctx, "job_fail", "batch_x", "", []byte(okChatBody)); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(ctx)
	go jobWorker(wctx)
	go jobMover(wctx)
	t.Cleanup(func() { wcancel(); time.Sleep(50 * time.Millisecond) })
	waitFor(t, 10*time.Second, func() bool { return mr.hgetall("orw:job:job_fail")["status"] == "failed" })

	j := mr.hgetall("orw:job:job_fail")
	if j["attempts"] != "3" || j["http_status"] != "429" {
		t.Fatalf("expected failed after 3 attempts with 429, got %+v", j)
	}
	if mr.hgetall("orw:batch:batch_x")["failed"] != "1" {
		t.Fatal("batch failed counter must be bumped")
	}
}

func TestJobNonRetryableStatusFailsImmediately(t *testing.T) {
	mr := testRedis(t)
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"bad request"}}`)
	}, false)
	jobsTestConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := enqueueJob(ctx, "job_400", "", "", []byte(okChatBody)); err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(ctx)
	go jobWorker(wctx)
	t.Cleanup(func() { wcancel(); time.Sleep(50 * time.Millisecond) })
	waitFor(t, 5*time.Second, func() bool { return mr.hgetall("orw:job:job_400")["status"] == "failed" })
	if j := mr.hgetall("orw:job:job_400"); j["attempts"] != "1" {
		t.Fatalf("400 must fail on first attempt, got %+v", j)
	}
}

// A worker that dies mid-job leaves a lease behind; the mover must requeue it
// once the lease expires, and the claim script must skip jobs already done.
func TestJobLeaseReclaimAndClaimSkipsDone(t *testing.T) {
	mr := testRedis(t)
	jobsTestConfig()
	ctx := context.Background()

	if err := enqueueJob(ctx, "job_dead", "", "", []byte(okChatBody)); err != nil {
		t.Fatal(err)
	}
	// Simulate a claimed job whose worker died: running + lease already expired.
	mr.mu.Lock()
	e := mr.entry("orw:job:job_dead")
	if e.hash == nil {
		e.hash = map[string]string{}
	}
	e.hash["status"] = "running"
	e.hash["attempts"] = "1"
	lz := mr.entry("orw:lease")
	if lz.z == nil {
		lz.z = map[string]float64{}
	}
	lz.z["job_dead"] = float64(time.Now().Add(-time.Minute).UnixMilli())
	mr.mu.Unlock()

	mctx, mcancel := context.WithCancel(ctx)
	go jobMover(mctx)
	t.Cleanup(func() { mcancel(); time.Sleep(50 * time.Millisecond) })
	waitFor(t, 5*time.Second, func() bool { return mr.llen("orw:q") > 0 })

	// Claiming a done job must be a no-op (duplicate delivery protection).
	if _, err := rdb.Do(ctx, 0, "HSET", "orw:job:job_done", "status", "done", "attempts", "1"); err != nil {
		t.Fatal(err)
	}
	v, err := rdb.Do(ctx, 0, "EVAL", luaClaim, "3", "orw:job:job_done", "orw:lease", "orw:proc", "job_done", "99999999999999")
	if err != nil || rInt(v) != 0 {
		t.Fatalf("claim of done job must return 0, got %v err %v", v, err)
	}
	// Claiming a running job with a live lease must also be a no-op.
	if _, err := rdb.Do(ctx, 0, "HSET", "orw:job:job_live", "status", "running", "attempts", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := rdb.Do(ctx, 0, "ZADD", "orw:lease", fmt.Sprint(time.Now().Add(time.Hour).UnixMilli()), "job_live"); err != nil {
		t.Fatal(err)
	}
	v, err = rdb.Do(ctx, 0, "EVAL", luaClaim, "3", "orw:job:job_live", "orw:lease", "orw:proc", "job_live", "99999999999999")
	if err != nil || rInt(v) != 0 {
		t.Fatalf("claim of live-leased job must return 0, got %v err %v", v, err)
	}
}

// Graceful shutdown: an in-flight job must go back to the queue, not be lost.
func TestJobShutdownRequeues(t *testing.T) {
	mr := testRedis(t)
	var once sync.Once
	started := make(chan struct{})
	setup(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		time.Sleep(300 * time.Millisecond) // hold the job until ctx is cancelled
		sse(w, `{"choices":[{"delta":{"content":"late"},"finish_reason":"stop"}]}`)
	}, false)
	jobsTestConfig()
	ctx, cancel := context.WithCancel(context.Background())

	if err := enqueueJob(ctx, "job_shut", "", "", []byte(okChatBody)); err != nil {
		t.Fatal(err)
	}
	go jobWorker(ctx)
	t.Cleanup(func() { cancel(); time.Sleep(50 * time.Millisecond) })
	<-started
	cancel() // gateway shutting down while the job runs
	waitFor(t, 5*time.Second, func() bool {
		j := mr.hgetall("orw:job:job_shut")
		return j["status"] == "queued" && mr.llen("orw:q") > 0
	})
	if _, leased := mr.zscore("orw:lease", "job_shut"); leased {
		t.Fatal("lease must be released on shutdown requeue")
	}
	if j := mr.hgetall("orw:job:job_shut"); j["attempts"] != "0" {
		t.Fatalf("shutdown must not burn an attempt, got %s", j["attempts"])
	}
}

func TestBatchHTTPFlow(t *testing.T) {
	mr := testRedis(t)
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"choices":[{"delta":{"content":"batched"},"finish_reason":"stop"}]}`)
	}, false)
	jobsTestConfig()
	ctx, cancel := context.WithCancel(context.Background())
	go jobWorker(ctx)
	t.Cleanup(func() { cancel(); time.Sleep(50 * time.Millisecond) })

	body := `{"requests":[{"custom_id":"a","body":` + okChatBody + `},{"custom_id":"b","body":` + okChatBody + `}]}`
	resp, out := post(t, gw.URL+"/v1/batches", body)
	if resp.StatusCode != 200 {
		t.Fatalf("batch submit: %d %s", resp.StatusCode, out)
	}
	var sub struct {
		ID    string `json:"id"`
		Total int    `json:"total"`
	}
	if err := json.Unmarshal([]byte(out), &sub); err != nil || sub.ID == "" || sub.Total != 2 {
		t.Fatalf("bad submit response: %s", out)
	}

	waitFor(t, 10*time.Second, func() bool {
		b := mr.hgetall("orw:batch:" + sub.ID)
		return b["done"] == "2"
	})

	req, err := http.Get(gw.URL + "/v1/batches/" + sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer req.Body.Close()
	var st struct {
		Status string `json:"status"`
		Done   int    `json:"done"`
		Total  int    `json:"total"`
	}
	if err := json.NewDecoder(req.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if st.Status != "completed" || st.Done != 2 || st.Total != 2 {
		t.Fatalf("expected completed batch, got %+v", st)
	}

	res, err := http.Get(gw.URL + "/v1/batches/" + sub.ID + "/results?offset=0&limit=10")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var list struct {
		Data []struct {
			CustomID string          `json:"custom_id"`
			Status   string          `json:"status"`
			Response json.RawMessage `json:"response"`
		} `json:"data"`
	}
	if err := json.NewDecoder(res.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Data) != 2 || list.Data[0].CustomID != "a" || list.Data[1].CustomID != "b" {
		t.Fatalf("results must preserve submit order, got %+v", list.Data)
	}
	for _, d := range list.Data {
		if d.Status != "done" || !strings.Contains(string(d.Response), "batched") {
			t.Fatalf("expected done result with content, got %+v", d)
		}
	}
}

func TestJobsHTTPDisabledWithoutRedis(t *testing.T) {
	old := rdb
	rdb = nil
	defer func() { rdb = old }()
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	resp, out := post(t, gw.URL+"/v1/jobs", okChatBody)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when Redis disabled, got %d %s", resp.StatusCode, out)
	}
}

func TestJobLongPollReturnsWhenDone(t *testing.T) {
	mr := testRedis(t)
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		sse(w, `{"choices":[{"delta":{"content":"polled"},"finish_reason":"stop"}]}`)
	}, false)
	jobsTestConfig()
	ctx, cancel := context.WithCancel(context.Background())
	wctx, wcancel := context.WithCancel(ctx)
	go jobWorker(wctx)
	t.Cleanup(func() { wcancel(); cancel(); time.Sleep(50 * time.Millisecond) })

	resp, out := post(t, gw.URL+"/v1/jobs", okChatBody)
	if resp.StatusCode != 200 {
		t.Fatalf("job submit: %d %s", resp.StatusCode, out)
	}
	var sub struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(out), &sub)

	res, err := http.Get(gw.URL + "/v1/jobs/" + sub.ID + "?wait=5")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var jv struct {
		Status   string          `json:"status"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.NewDecoder(res.Body).Decode(&jv); err != nil {
		t.Fatal(err)
	}
	if jv.Status != "done" || !strings.Contains(string(jv.Response), "polled") {
		t.Fatalf("long-poll should return the finished job, got %+v", jv)
	}
	if _, ok := mr.zscore("orw:lease", sub.ID); ok {
		t.Fatal("lease must be gone after completion")
	}
}

// ---------- RESP client unit tests ----------

func TestRedisReplyParsing(t *testing.T) {
	mr := testRedis(t)
	ctx := context.Background()
	if _, err := rdb.Do(ctx, 0, "HSET", "h", "a", "1", "b", "2"); err != nil {
		t.Fatal(err)
	}
	v, err := rdb.Do(ctx, 0, "HMGET", "h", "a", "missing")
	if err != nil {
		t.Fatal(err)
	}
	arr := v.([]any)
	if rStr(arr[0]) != "1" || arr[1] != nil {
		t.Fatalf("HMGET: got %#v", arr)
	}
	if _, err := rdb.Do(ctx, 0, "GET", "nope"); err != errRedisNil {
		t.Fatalf("missing key must be errRedisNil, got %v", err)
	}
	if _, err := rdb.Do(ctx, 0, "BOGUSCMD"); err == nil {
		t.Fatal("error reply must surface as error")
	}
	_ = mr
}

func TestRedisPoolReusesConnections(t *testing.T) {
	mr := testRedis(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, err := rdb.Do(ctx, 0, "SET", fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	rdb.mu.Lock()
	idle := len(rdb.idle)
	rdb.mu.Unlock()
	if idle == 0 || idle > 2 {
		t.Fatalf("sequential commands should reuse one pooled conn, idle=%d", idle)
	}
	_ = mr
}

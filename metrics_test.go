package main

// ======================== /metrics ENDPOINT ========================

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMetricsEndpointExposesCoreSeries(t *testing.T) {
	gw, m := setup(t, func(w http.ResponseWriter, r *http.Request) {
		sse(w, `{"choices":[{"delta":{"content":"hi"},"finish_reason":"stop"}]}`)
	}, false)

	// One real request so counters and the latency histogram have data.
	resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"x"}]}`)
	if resp.StatusCode != 200 {
		t.Fatalf("setup request failed: %d", resp.StatusCode)
	}

	res, err := http.Get(gw.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("unexpected content type %q", ct)
	}
	raw, _ := io.ReadAll(res.Body)
	body := string(raw)

	for _, want := range []string{
		"# TYPE orw_uptime_seconds gauge",
		"# TYPE orw_requests_total counter",
		"# TYPE orw_request_duration_seconds histogram",
		`orw_egress_requests_total{egress="direct",kind="direct"}`,
		`orw_queued_requests{class="sync"}`,
		"orw_account_gate_capped ",
		`orw_request_duration_seconds_bucket{le="+Inf"}`,
		"orw_request_duration_seconds_count ",
		"orw_jobs_done_total ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	// The served request must be counted somewhere.
	if !strings.Contains(body, "orw_requests_total 1") && !strings.Contains(body, "orw_requests_total 2") {
		t.Errorf("expected request counter >= 1, body:\n%s", body)
	}
	_ = m
}

func TestLatencyHistogramBuckets(t *testing.T) {
	h := &latHist{counts: make([]uint64, len(latBuckets)+1)}
	h.observe(50 * time.Millisecond)   // <= 0.1
	h.observe(1500 * time.Millisecond) // <= 2.5
	h.observe(10 * time.Minute)        // +Inf only
	cum, n, sum := h.snapshot()
	if n != 3 {
		t.Fatalf("count=%d", n)
	}
	if cum[0] != 1 {
		t.Fatalf("0.1s bucket cumulative=%d, want 1", cum[0])
	}
	if cum[len(cum)-1] != 3 {
		t.Fatalf("+Inf cumulative=%d, want 3", cum[len(cum)-1])
	}
	if sum < 600 {
		t.Fatalf("sum=%f should include the 10m observation", sum)
	}
	// Boundary: exactly on a bucket edge belongs to that bucket.
	h2 := &latHist{counts: make([]uint64, len(latBuckets)+1)}
	h2.observe(time.Second)
	c2, _, _ := h2.snapshot()
	idx := 3 // latBuckets[3] == 1
	if c2[idx] != 1 {
		t.Fatalf("1s observation must land in le=1 bucket, cum=%v", c2)
	}
}

func TestMetricsGateCappedReflectsBreaker(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	old := acctGate
	acctGate = newGate() // setup may have installed its own gate; swap after
	defer func() { acctGate = old }()

	get := func() string {
		res, err := http.Get(gw.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		return string(raw)
	}
	if !strings.Contains(get(), "orw_account_gate_capped 0") {
		t.Fatal("gate should start open")
	}
	acctGate.noteFreshCap("proxy1", time.Hour, true, "m1", "")
	acctGate.noteFreshCap("proxy2", time.Hour, true, "m1", "")
	acctGate.noteFreshCap("proxy3", time.Hour, true, "m2", "")
	acctGate.noteFreshCap("res1", time.Hour, true, "m2", "")
	if !strings.Contains(get(), "orw_account_gate_capped 1") {
		t.Fatal("tripped gate must show capped=1")
	}
}

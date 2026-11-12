package main

// ======================== PROMETHEUS METRICS ========================
//
// GET /metrics exposes gateway health in the Prometheus text exposition
// format (version 0.0.4) with no external dependency. Counters are derived
// from the same atomics /status already reads, so there is no second
// accounting path to drift. Latency is a cumulative histogram fed at the
// existing request-log call sites.

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Latency histogram buckets (seconds), tuned to the gateway's real range:
// sub-second auto-fast responses up to multi-minute long-context reads.
var latBuckets = []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300}

type latHist struct {
	mu     sync.Mutex
	counts []uint64 // len(latBuckets)+1 (last = +Inf)
	sum    float64
	n      uint64
}

var reqLatency = &latHist{counts: make([]uint64, len(latBuckets)+1)}

func (h *latHist) observe(d time.Duration) {
	s := d.Seconds()
	h.mu.Lock()
	h.n++
	h.sum += s
	i := sort.SearchFloat64s(latBuckets, s)
	if i == len(latBuckets) || latBuckets[i] < s {
		i = len(latBuckets) // strictly greater than the last finite bucket -> +Inf only
	}
	h.counts[i]++
	h.mu.Unlock()
}

// snapshot returns cumulative bucket counts, total count and sum.
func (h *latHist) snapshot() ([]uint64, uint64, float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]uint64, len(h.counts))
	var cum uint64
	for i, c := range h.counts {
		cum += c
		out[i] = cum
	}
	return out, h.n, h.sum
}

// observeRequestLatency is called from the request-log path (all endpoints).
func observeRequestLatency(d time.Duration) { reqLatency.observe(d) }

var (
	metricsRenewMu sync.Mutex
	metricsRenews  int64 // identity renewals triggered (WARP registration / container restart)
)

func noteIdentityRenew() {
	metricsRenewMu.Lock()
	metricsRenews++
	metricsRenewMu.Unlock()
}

func identityRenewCount() int64 {
	metricsRenewMu.Lock()
	defer metricsRenewMu.Unlock()
	return metricsRenews
}

func metricsHandler(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	metric := func(name, typ, help string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}
	gauge := func(name string, v float64, labels string) {
		if labels != "" {
			fmt.Fprintf(&b, "%s{%s} %g\n", name, labels, v)
		} else {
			fmt.Fprintf(&b, "%s %g\n", name, v)
		}
	}

	metric("orw_uptime_seconds", "gauge", "Seconds since gateway start.")
	gauge("orw_uptime_seconds", time.Since(started).Seconds(), "")

	metric("orw_inflight_requests", "gauge", "Requests currently being served (sync + jobs).")
	gauge("orw_inflight_requests", float64(inflightTotal.Load()), "")
	metric("orw_queued_requests", "gauge", "Requests waiting for an egress slot, by priority class.")
	gauge("orw_queued_requests", float64(queuedHigh.Load()), `class="sync"`)
	gauge("orw_queued_requests", float64(queuedLow.Load()), `class="job"`)

	metric("orw_account_gate_capped", "gauge", "1 while the account-level circuit breaker is blocking (quota exhausted account-wide).")
	capped := 0.0
	if acctGate.capped() {
		capped = 1
	}
	gauge("orw_account_gate_capped", capped, "")

	// Per-egress counters (same atomics /status reads).
	metric("orw_egress_requests_total", "counter", "Requests sent through an egress.")
	metric("orw_egress_ok_total", "counter", "Requests answered 200 by upstream through an egress.")
	metric("orw_egress_rate_limited_total", "counter", "Upstream 429 responses seen on an egress.")
	metric("orw_egress_errors_total", "counter", "Transport/5xx errors on an egress.")
	metric("orw_egress_inflight", "gauge", "In-flight requests on an egress.")
	metric("orw_egress_inflight_limit", "gauge", "Current AIMD in-flight cap of an egress.")
	metric("orw_egress_ready", "gauge", "1 if the egress is usable for routing.")
	metric("orw_egress_identity_renews_total", "counter", "Identity renewals completed on an egress (new exit IP).")
	var totReqs, totOK, tot429, totErrs int64
	for _, e := range mgr.snapshot() {
		labels := fmt.Sprintf("egress=%q,kind=%q", e.Name, e.Kind)
		reqs, ok, r429, errs := e.reqs.Load(), e.oks.Load(), e.r429.Load(), e.errs.Load()
		totReqs += reqs
		totOK += ok
		tot429 += r429
		totErrs += errs
		gauge("orw_egress_requests_total", float64(reqs), labels)
		gauge("orw_egress_ok_total", float64(ok), labels)
		gauge("orw_egress_rate_limited_total", float64(r429), labels)
		gauge("orw_egress_errors_total", float64(errs), labels)
		gauge("orw_egress_inflight", float64(e.inflight.Load()), labels)
		gauge("orw_egress_inflight_limit", float64(e.limit.Load()), labels)
		ready := 0.0
		if e.ready.Load() && !e.expired.Load() {
			ready = 1
		}
		gauge("orw_egress_ready", ready, labels)
		gauge("orw_egress_identity_renews_total", float64(e.renews.Load()), labels)
	}

	metric("orw_requests_total", "counter", "Total requests sent upstream (sum over egresses).")
	gauge("orw_requests_total", float64(totReqs), "")
	metric("orw_requests_ok_total", "counter", "Total upstream 200 responses.")
	gauge("orw_requests_ok_total", float64(totOK), "")
	metric("orw_rate_limited_total", "counter", "Total upstream 429 responses.")
	gauge("orw_rate_limited_total", float64(tot429), "")
	metric("orw_errors_total", "counter", "Total transport/5xx errors.")
	gauge("orw_errors_total", float64(totErrs), "")

	metric("orw_identity_renews_triggered_total", "counter", "Identity renewal attempts triggered by quota exhaustion.")
	gauge("orw_identity_renews_triggered_total", float64(identityRenewCount()), "")

	// Job queue (process-lifetime counters; queue depths come from /status when Redis is on).
	metric("orw_jobs_done_total", "counter", "Background jobs completed since start.")
	gauge("orw_jobs_done_total", float64(jobsDone.Load()), "")
	metric("orw_jobs_failed_total", "counter", "Background jobs permanently failed since start.")
	gauge("orw_jobs_failed_total", float64(jobsFailed.Load()), "")
	metric("orw_jobs_retried_total", "counter", "Background job retry scheduling events since start.")
	gauge("orw_jobs_retried_total", float64(jobsRetried.Load()), "")
	metric("orw_jobs_running", "gauge", "Background jobs currently executing.")
	gauge("orw_jobs_running", float64(jobsRunning.Load()), "")

	// Request latency histogram (all endpoints, measured at the log call sites).
	metric("orw_request_duration_seconds", "histogram", "End-to-end request duration as seen by the gateway.")
	cum, n, sum := reqLatency.snapshot()
	for i, le := range latBuckets {
		fmt.Fprintf(&b, "orw_request_duration_seconds_bucket{le=\"%g\"} %d\n", le, cum[i])
	}
	fmt.Fprintf(&b, "orw_request_duration_seconds_bucket{le=\"+Inf\"} %d\n", cum[len(cum)-1])
	fmt.Fprintf(&b, "orw_request_duration_seconds_sum %g\n", sum)
	fmt.Fprintf(&b, "orw_request_duration_seconds_count %d\n", n)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}

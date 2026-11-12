package main

// ======================== STATE PERSISTENCE ACROSS RESTART ========================
//
// The quota ledger and exhaustion evidence live in RAM; upstream quota does
// not reset when the gateway restarts. These tests lock in the Redis-backed
// snapshot/restore contract: same-day snapshots restore, stale ones are
// dropped, live counters are never overwritten downwards, expired evidence
// is not resurrected.

import (
	"context"
	"testing"
	"time"
)

func TestQuotaPersistRoundTrip(t *testing.T) {
	mr := testRedis(t)
	resetQuota()
	defer resetQuota()
	ctx := context.Background()

	quotaAttempt("glm", "proxy1", true)
	quotaAttempt("glm", "proxy1", true)
	quotaAttempt("glm", "proxy1", false)
	quotaAttempt("nemotron", "proxy2", false)
	quotaNoteLimit("glm", "proxy1")
	quotaSuccess("nemotron", "proxy2") // raises learned cap via positive evidence

	quotaPersistSave(ctx)
	if v, err := rdb.Do(ctx, 0, "GET", quotaPersistKey); err != nil || rStr(v) == "" {
		t.Fatalf("snapshot must be stored as a plain string key, got %v err %v", v, err)
	}
	_ = mr

	// Simulate restart: wipe RAM state, restore from Redis.
	resetQuota()
	if a, d := quotaUsed("glm", "proxy1"); a != 0 || d != 0 {
		t.Fatal("resetQuota must clear counters")
	}
	quotaPersistLoad(ctx)
	if a, d := quotaUsed("glm", "proxy1"); a != 2 || d != 1 {
		t.Fatalf("restore lost counters: auto=%d direct=%d", a, d)
	}
	if a, _ := quotaUsed("nemotron", "proxy2"); a != 0 {
		t.Fatalf("restore mixed channels: %d", a)
	}
	if quotaCap("nemotron") < 1 {
		t.Fatal("learned caps must survive restart")
	}
}

func TestQuotaPersistRejectsStaleDay(t *testing.T) {
	testRedis(t)
	resetQuota()
	defer resetQuota()
	ctx := context.Background()

	stale := quotaPersistSnap{Day: "2000-01-01", Pairs: map[string]map[string]quotaPersistState{
		"glm": {"proxy1": {Auto: 50, Direct: 50}},
	}}
	if quotaImport(stale) {
		t.Fatal("yesterday's snapshot must be rejected (upstream quota reset at 00:00 UTC)")
	}
	if a, d := quotaUsed("glm", "proxy1"); a != 0 || d != 0 {
		t.Fatal("stale snapshot must not inject counters")
	}
	_ = ctx
}

func TestQuotaImportNeverLowersLiveCounters(t *testing.T) {
	resetQuota()
	defer resetQuota()
	quotaAttempt("glm", "proxy1", true) // live: 1
	for i := 0; i < 9; i++ {
		quotaAttempt("glm", "proxy1", true)
	} // live: 10
	snap := quotaPersistSnap{Day: utcDay(), Pairs: map[string]map[string]quotaPersistState{
		"glm": {"proxy1": {Auto: 3, Direct: 7}},
	}}
	if !quotaImport(snap) {
		t.Fatal("same-day snapshot must be accepted")
	}
	if a, d := quotaUsed("glm", "proxy1"); a != 10 || d != 7 {
		t.Fatalf("import must take max per channel, got auto=%d direct=%d", a, d)
	}
}

func TestBudgetPersistRoundTripAndExpiry(t *testing.T) {
	testRedis(t)
	resetModelBudget()
	defer resetModelBudget()
	ctx := context.Background()

	future := time.Now().Add(2 * time.Hour)
	past := time.Now().Add(-time.Hour)
	noteModelExhausted("glm", "proxy1", future, 4)
	noteModelExhausted("glm", "proxy2", past, 4) // already expired: must not be persisted
	noteDeadPrefix("glm", "2a09:bac1:7a80::/64", future)

	budgetPersistSave(ctx)
	resetModelBudget()
	if egressExhaustedOn("glm", "proxy1") {
		t.Fatal("resetModelBudget must clear evidence")
	}
	budgetPersistLoad(ctx)
	if !egressExhaustedOn("glm", "proxy1") {
		t.Fatal("live exhaustion evidence must survive restart")
	}
	if egressExhaustedOn("glm", "proxy2") {
		t.Fatal("expired evidence must not be resurrected")
	}
	if !prefixDead("glm", "2a09:bac1:7a80::/64") {
		t.Fatal("dead /64 must survive restart")
	}
}

func TestBudgetImportKeepsLongerDeadline(t *testing.T) {
	resetModelBudget()
	defer resetModelBudget()
	soon := time.Now().Add(30 * time.Minute)
	later := time.Now().Add(3 * time.Hour)
	noteModelExhausted("glm", "proxy1", later, 4)
	budgetImport(budgetPersistSnap{Pairs: map[string]map[string]int64{
		"glm": {"proxy1": soon.UnixNano()},
	}})
	if !egressExhaustedOn("glm", "proxy1") {
		t.Fatal("pair must stay exhausted")
	}
	budgetMu.Lock()
	got := budgetPairs["glm"]["proxy1"]
	budgetMu.Unlock()
	if got.Before(later.Add(-time.Second)) {
		t.Fatalf("import must not shorten a live deadline: got %v want %v", got, later)
	}
}

func TestPersistDisabledWithoutRedis(t *testing.T) {
	old := rdb
	rdb = nil
	defer func() { rdb = old }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Must be no-ops, not panics.
	quotaPersistSave(ctx)
	quotaPersistLoad(ctx)
	budgetPersistSave(ctx)
	budgetPersistLoad(ctx)
	quotaPersistLoop(ctx, time.Second)
	budgetPersistLoop(ctx, time.Second)
}

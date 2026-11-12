package main

// ======================== AIMD LIMIT RECOVERY ========================
//
// Regression for the open item in PRIVATE-BRANCH.md: a flaky egress gets its
// in-flight cap cut and never recovers under low traffic, because the
// success-driven increase needs 2*lim requests. Time-based recovery must
// raise the cap back toward LIMIT_MAX while the egress is healthy.

import (
	"context"
	"testing"
	"time"
)

func aimdTestEgress(name string) *Egress {
	e := newEgress(name, kindProxy, nil)
	e.ready.Store(true)
	return e
}

func TestAIMDRecoveryRaisesCapAfterQuietPeriod(t *testing.T) {
	old := cfg
	defer func() { cfg = old }()
	cfg.LimitMax = 32
	cfg.LimitMin = 1
	cfg.LimitRecover = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newManager(ctx)
	e := aimdTestEgress("proxy1")
	m.add(e) // add() runs initLimit, so set the cap afterwards
	e.limit.Store(16)

	// A cut marks the recovery clock.
	e.adapt(false) // 16 -> 12
	if got := e.limit.Load(); got != 12 {
		t.Fatalf("expected cut to 12, got %d", got)
	}
	// Too soon: no recovery yet.
	m.recoverLimits()
	if got := e.limit.Load(); got != 12 {
		t.Fatalf("recovery must wait for LimitRecover, got %d", got)
	}
	time.Sleep(25 * time.Millisecond)
	m.recoverLimits()
	if got := e.limit.Load(); got != 13 {
		t.Fatalf("expected +1 recovery, got %d", got)
	}
	// Recovery never exceeds LIMIT_MAX.
	e.limit.Store(32)
	e.limitCutAt.Store(time.Now().Add(-time.Hour).UnixNano())
	m.recoverLimits()
	if got := e.limit.Load(); got != 32 {
		t.Fatalf("cap must not exceed LimitMax, got %d", got)
	}
}

func TestAIMDRecoverySkipsUnhealthyEgress(t *testing.T) {
	old := cfg
	defer func() { cfg = old }()
	cfg.LimitMax = 32
	cfg.LimitMin = 1
	cfg.LimitRecover = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := newManager(ctx)
	stale := time.Now().Add(-time.Hour).UnixNano()

	for _, tc := range []struct {
		name  string
		setup func(e *Egress)
	}{
		{"not ready", func(e *Egress) { e.ready.Store(false) }},
		{"expired", func(e *Egress) { e.expired.Store(true) }},
		{"tcp cooldown", func(e *Egress) { e.downUntil.Store(time.Now().Add(time.Minute).UnixNano()) }},
		{"recent transport errors", func(e *Egress) { e.errRun.Store(2) }},
	} {
		e := aimdTestEgress("proxy-" + tc.name)
		m.add(e)
		e.limit.Store(4)
		e.limitCutAt.Store(stale)
		tc.setup(e)
	}
	time.Sleep(2 * time.Millisecond)
	m.recoverLimits()
	for _, e := range m.snapshot() {
		if e.Name == "direct" {
			continue
		}
		if got := e.limit.Load(); got != 4 {
			t.Errorf("%s: unhealthy egress must not recover, limit=%d", e.Name, got)
		}
	}
}

func TestAIMDSuccessPathStillRaisesCap(t *testing.T) {
	old := cfg
	defer func() { cfg = old }()
	cfg.LimitMax = 32
	cfg.LimitMin = 1
	e := aimdTestEgress("proxy1")
	e.limit.Store(2)
	for i := 0; i < 4; i++ { // 2*lim successes -> +1
		e.adapt(true)
	}
	if got := e.limit.Load(); got != 3 {
		t.Fatalf("success-driven AIMD increase broken, got %d", got)
	}
}

package main

import (
	"net/http"
	"testing"
	"time"
)

// Egress có IP RIÊNG (IPv6 riêng của container) phải được ưu tiên hơn egress dùng IP CHUNG (IPv4 WARP):
// hạn mức phía upstream tính theo IP, nên 9 container chung 1 IPv4 thì hạn mức cũng chung -> ưu tiên IP riêng.
func TestPickerPrefersDedicatedIP(t *testing.T) {
	_, m := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	oldPrefer := cfg.PreferDirect
	cfg.PreferDirect = false // để direct không được ưu tiên, xem rõ chọn giữa các egress proxy
	defer func() { cfg.PreferDirect = oldPrefer }()

	shared := newEgress("proxy-shared", kindProxy, nil)
	shared.tier = 2
	shared.daemon = "proxy-shared"
	shared.ipKind.Store(ipKindShared)
	shared.ready.Store(true)
	m.add(shared)

	own := newEgress("proxy-own", kindProxy, nil)
	own.tier = 2
	own.daemon = "proxy-own"
	own.ipKind.Store(ipKindOwn)
	own.ready.Store(true)
	m.add(own)

	// Không có yêu cầu nào cả -> chọn egress nào?
	picked := m.pickAvoid("deepseek")
	if picked == nil {
		t.Fatal("phải chọn được egress")
	}
	if picked.Name != "proxy-own" {
		t.Fatalf("phải ưu tiên egress có IP riêng, chọn %s", picked.Name)
	}
}

// Nếu TẤT CẢ egress cùng họ đều dùng IP chung thì vẫn phải chọn được (không bỏ trống).
func TestPickerFallsBackToSharedIP(t *testing.T) {
	_, m := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	oldPrefer := cfg.PreferDirect
	cfg.PreferDirect = false // để direct không được ưu tiên, xem rõ chọn giữa các egress proxy
	defer func() { cfg.PreferDirect = oldPrefer }()

	shared := newEgress("proxy-shared", kindProxy, nil)
	shared.tier = 2
	shared.daemon = "proxy-shared"
	shared.ipKind.Store(ipKindShared)
	shared.ready.Store(true)
	m.add(shared)

	picked := m.pickAvoid("deepseek")
	if picked == nil || picked.Name != "proxy-shared" {
		t.Fatalf("phải dùng được egress IP chung khi không còn lựa chọn nào khác, got %v", picked)
	}
}

// Trạng thái IP được ghi nhớ: đổi danh tính xong thì phải cập nhật (own/shared).
func TestEgressIPKindRecorded(t *testing.T) {
	e := newEgress("p", kindProxy, nil)
	if e.ipKind.Load() != ipKindUnknown {
		t.Fatal("mặc định phải là chưa biết")
	}
	e.ipKind.Store(ipKindOwn)
	if e.ipKind.Load() != ipKindOwn {
		t.Fatal("phải nhớ được loại IP")
	}
	e.ipAddr.Store("2a09:bac5:d45c:16c8::245:88")
	if ip, ok := e.ipAddr.Load().(string); !ok || ip == "" {
		t.Fatal("phải nhớ được địa chỉ IP")
	}
	_ = time.Second
}

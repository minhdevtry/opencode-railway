package main

import (
	"net/http"
	"testing"
	"time"
)

// Cấu hình quota cho test: trần 64, auto được 60%, gọi thẳng không bị trần chặn.
func setupQuotaCfg(t *testing.T) {
	t.Helper()
	oldDef, oldShare, oldOv := cfg.QuotaDefault, cfg.QuotaAutoShare, cfg.QuotaOverrides
	cfg.QuotaDefault = 64
	cfg.QuotaAutoShare = 0.6
	cfg.QuotaOverrides = map[string]int{"big-model": 256}
	t.Cleanup(func() {
		cfg.QuotaDefault, cfg.QuotaAutoShare, cfg.QuotaOverrides = oldDef, oldShare, oldOv
		resetQuota()
	})
	resetQuota()
}

// Sổ đếm theo cặp (model, egress): khác model hoặc khác egress thì không lẫn vào nhau.
func TestQuotaCountsPerModelEgress(t *testing.T) {
	setupQuotaCfg(t)

	quotaAttempt("glm-5.3-flash-free", "proxy1", false)
	quotaAttempt("glm-5.3-flash-free", "proxy1", false)
	quotaAttempt("glm-5.3-flash-free", "proxy2", false)
	quotaAttempt("nemotron-3.5-lightning-free", "proxy1", false)

	auto, direct := quotaUsed("glm-5.3-flash-free", "proxy1")
	if auto != 0 || direct != 2 {
		t.Fatalf("proxy1 của glm phải có 2 request gọi thẳng, got auto=%d direct=%d", auto, direct)
	}
	if _, d := quotaUsed("glm-5.3-flash-free", "proxy2"); d != 1 {
		t.Fatalf("proxy2 của glm phải có 1 request, got %d", d)
	}
	if _, d := quotaUsed("nemotron-3.5-lightning-free", "proxy1"); d != 1 {
		t.Fatalf("proxy1 của nemotron phải có 1 request, got %d", d)
	}
	if r := quotaRank("glm-5.3-flash-free", "proxy9"); r != 0 {
		t.Fatalf("egress chưa dùng thì rank phải = 0, got %d", r)
	}
}

// CHIA MỀM: auto ăn hết phần share của cặp thì bị đẩy xuống cuối, còn gọi thẳng KHÔNG bị chặn.
func TestQuotaAutoShareSplit(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	// Trần 64, share 60% -> auto được 38 request trên cặp này.
	for i := 0; i < 38; i++ {
		quotaAttempt(model, "proxy1", true)
	}
	left, total := quotaAutoShareLeft(model, "proxy1")
	if total != 38 {
		t.Fatalf("share của auto phải = 38 (60%% của 64), got %d", total)
	}
	if left != 0 {
		t.Fatalf("auto đã ăn hết share thì phần còn lại phải = 0, got %d", left)
	}
	// Gọi thẳng vẫn dùng được cặp đó (không bị trần chặn) và không ăn phần của auto.
	quotaAttempt(model, "proxy1", false)
	if _, direct := quotaUsed(model, "proxy1"); direct != 1 {
		t.Fatalf("gọi thẳng phải vẫn được dùng cặp đó, got direct=%d", direct)
	}
	// Cặp khác chưa dùng gì thì vẫn còn nguyên share.
	if l, _ := quotaAutoShareLeft(model, "proxy2"); l != 38 {
		t.Fatalf("cặp chưa dùng phải còn nguyên share, got %d", l)
	}
}

// Trần của model học từ 429 quan sát được, và CHỈ tăng (không giảm khi gặp mẫu nhỏ hơn).
func TestQuotaCapLearnedFromLimit(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	for i := 0; i < 64; i++ {
		quotaAttempt(model, "proxy1", false)
	}
	if got := quotaCap(model); got != 64 {
		t.Fatalf("trần mặc định phải là 64, got %d", got)
	}
	quotaNoteLimit(model, "proxy1")
	if got := quotaCap(model); got != 64 {
		t.Fatalf("sau 429 ở request thứ 64 thì trần học được phải = 64, got %d", got)
	}
	// Mẫu nhỏ hơn ở IP khác không được làm trần đi xuống.
	quotaAttempt("glm-5.3-flash-free", "proxy2", false)
	quotaNoteLimit(model, "proxy2")
	if got := quotaCap(model); got != 64 {
		t.Fatalf("trần đã học không được giảm, got %d", got)
	}
	// Override env được dùng khi chưa học được gì từ 429.
	if got := quotaCap("big-model"); got != 256 {
		t.Fatalf("model có override phải dùng trần override, got %d", got)
	}
}

// 429 liên tiếp đếm tăng; một request thành công thì reset (sự kiện 429 cũ không còn ý nghĩa).
func TestQuotaLimit429Consecutive(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	if n := quotaNoteLimit(model, "proxy1"); n != 1 {
		t.Fatalf("lần 429 đầu phải = 1, got %d", n)
	}
	if n := quotaNoteLimit(model, "proxy1"); n != 2 {
		t.Fatalf("lần 429 thứ hai phải = 2, got %d", n)
	}
	quotaSuccess(model, "proxy1") // thành công -> reset
	if n := quotaNoteLimit(model, "proxy1"); n != 1 {
		t.Fatalf("sau khi thành công thì bộ đếm 429 phải reset, got %d", n)
	}
}

// Đổi IP -> ngân sách (IP, model) cũ hết hiệu lực, bộ đếm về 0.
func TestQuotaClearEgressOnIPChange(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	quotaAttempt(model, "proxy1", true)
	quotaAttempt(model, "proxy1", false)
	quotaNoteLimit(model, "proxy1")

	quotaClearEgress("proxy1")
	if a, d := quotaUsed(model, "proxy1"); a != 0 || d != 0 {
		t.Fatalf("đổi IP thì bộ đếm phải về 0, got auto=%d direct=%d", a, d)
	}
	if n := quotaNoteLimit(model, "proxy1"); n != 1 {
		t.Fatalf("sau đổi IP thì số 429 liên tiếp phải reset, got %d", n)
	}
}

// Sức chứng còn lại của model: auto tính theo share, gọi thẳng tính theo trần đầy đủ.
func TestQuotaModelCapacity(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	egs := []string{"proxy1", "proxy2"}
	if c := quotaModelCapacities([]string{model}, true, egs)[model]; c != 2*38 {
		t.Fatalf("sức chứng auto ban đầu = 2 x 38, got %d", c)
	}
	if c := quotaModelCapacities([]string{model}, false, egs)[model]; c != 2*64 {
		t.Fatalf("sức chứng gọi thẳng ban đầu = 2 x 64, got %d", c)
	}
	// Auto ăn hết share của proxy1 -> sức chứng auto chỉ còn proxy2.
	for i := 0; i < 38; i++ {
		quotaAttempt(model, "proxy1", true)
	}
	if c := quotaModelCapacities([]string{model}, true, egs)[model]; c != 38 {
		t.Fatalf("sức chứng auto sau khi ăn hết share proxy1 phải = 38, got %d", c)
	}
	// Gọi thẳng vẫn thấy cả 2 cặp (dùng chung bucket với auto nên sức chứng bị trừ theo tổng).
	if c := quotaModelCapacities([]string{model}, false, egs)[model]; c != 2*64-38 {
		t.Fatalf("sức chứng gọi thẳng phải trừ phần auto đã dùng, got %d", c)
	}
}

// Ngày UTC mới -> mọi bộ đếm về 0 (ngân sách upstream reset 00:00 UTC).
func TestQuotaResetOnNewDay(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	quotaAttempt(model, "proxy1", false)
	quotaMu.Lock()
	quotaDay = "2000-01-01" // giả vờ sang ngày mới
	quotaMu.Unlock()
	if a, d := quotaUsed(model, "proxy1"); a != 0 || d != 0 {
		t.Fatalf("sang ngày UTC mới thì bộ đếm phải về 0, got auto=%d direct=%d", a, d)
	}
	// Trần đã học thì giữ (đặc tính của model, không phụ thuộc ngày).
	quotaAttempt(model, "proxy1", false)
	quotaNoteLimit(model, "proxy1")
	quotaMu.Lock()
	quotaDay = "2000-01-02"
	quotaMu.Unlock()
	if got := quotaCap(model); got != 1 {
		t.Fatalf("trần đã học phải được giữ qua ngày, got %d", got)
	}
}

// Đổi IP xoá số liệu của egress đó qua clearBudgetForEgress (điểm gọi thật trong code).
func TestQuotaClearViaClearBudgetForEgress(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	quotaAttempt(model, "proxy1", false)
	clearBudgetForEgress("proxy1")
	if _, d := quotaUsed(model, "proxy1"); d != 0 {
		t.Fatalf("clearBudgetForEgress phải xoá luôn bộ đếm quota, got %d", d)
	}
}

// Bộ đếm không rò: số request ghi trong sổ khớp với số request thành công.
func TestQuotaNoLeak(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	for i := 0; i < 17; i++ {
		quotaAttempt(model, "proxy3", true)
		quotaAttempt(model, "proxy4", false)
	}
	snap := quotaSnapshot([]string{"proxy3", "proxy4"})
	m, ok := snap[model].(map[string]any)
	if !ok {
		t.Fatalf("snapshot phải có model %s, got %v", model, snap)
	}
	if m["used_auto"] != 17 || m["used_direct"] != 17 {
		t.Fatalf("snapshot phải khớp sổ, got auto=%v direct=%v", m["used_auto"], m["used_direct"])
	}
	if m["cap"] != 64 {
		t.Fatalf("snapshot phải hiện trần 64, got %v", m["cap"])
	}
}

// Test nhanh: quotaNote phải an toàn với input rỗng (không ghi rác).
func TestQuotaIgnoresEmptyKeys(t *testing.T) {
	setupQuotaCfg(t)
	quotaAttempt("", "proxy1", false)
	quotaAttempt("glm-5.3-flash-free", "", false)
	if n := quotaNoteLimit("", "proxy1"); n != 0 {
		t.Fatalf("model rỗng phải bỏ qua, got %d", n)
	}
	_ = time.Now()
}

// share = 0 (hoặc chưa biết trần) -> KHÔNG giới hạn auto. Nếu không, auto sẽ bị phạt ở mọi cặp và
// mọi request auto đều rơi vào hàng đợi (mìn cấu hình).
func TestQuotaShareZeroMeansNoRestriction(t *testing.T) {
	setupQuotaCfg(t)
	cfg.QuotaAutoShare = 0
	cfg.QuotaDefault = 0 // chưa biết trần

	if p := quotaPenaltyFor("glm-5.3-flash-free", "proxy1", true); p != 0 {
		t.Fatalf("share = 0 thì auto không bị phạt, got %v", p)
	}
	for i := 0; i < 100; i++ {
		quotaAttempt("glm-5.3-flash-free", "proxy1", true)
	}
	if p := quotaPenaltyFor("glm-5.3-flash-free", "proxy1", true); p != 0 {
		t.Fatalf("auto dùng nhiều cũng không bị phạt khi không có giới hạn share, got %v", p)
	}
}

// Chỉ kênh auto bị phạt khi ăn hết share; gọi thẳng thì không.
func TestQuotaPenaltyOnlyForAuto(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	for i := 0; i < 38; i++ {
		quotaAttempt(model, "proxy1", true)
	}
	if p := quotaPenaltyFor(model, "proxy1", true); p <= 0 {
		t.Fatalf("auto ăn hết share thì phải bị phạt, got %v", p)
	}
	if p := quotaPenaltyFor(model, "proxy1", false); p != 0 {
		t.Fatalf("gọi thẳng không được phạt theo share của auto, got %v", p)
	}
	// Cặp khác chưa dùng -> không phạt.
	if p := quotaPenaltyFor(model, "proxy2", true); p != 0 {
		t.Fatalf("cặp chưa dùng không được phạt, got %v", p)
	}
}

// Định tuyến: khi 2 egress cùng điều kiện thì phải chọn cặp (model, egress) ÍT ĐÃ DÙNG hơn,
// để trải đều ngân sách thay vì dồn hết vào một IP.
func TestPickPrefersLessUsedPair(t *testing.T) {
	setupQuotaCfg(t)
	_, m := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	oldPrefer := cfg.PreferDirect
	cfg.PreferDirect = false // để direct không được ưu tiên, xem rõ chọn giữa các egress proxy
	defer func() { cfg.PreferDirect = oldPrefer }()

	busy := newEgress("proxy-busy", kindProxy, nil)
	busy.tier = 2
	busy.daemon = "proxy-busy"
	busy.ipKind.Store(ipKindOwn)
	busy.ready.Store(true)
	m.add(busy)
	fresh := newEgress("proxy-fresh", kindProxy, nil)
	fresh.tier = 2
	fresh.daemon = "proxy-fresh"
	fresh.ipKind.Store(ipKindOwn)
	fresh.ready.Store(true)
	m.add(fresh)

	const model = "glm-5.3-flash-free"
	quotaAttempt(model, "proxy-busy", false)
	quotaAttempt(model, "proxy-busy", false)
	quotaAttempt(model, "proxy-busy", false)

	picked := m.pickAvoid(model, false)
	if picked == nil {
		t.Fatal("phải chọn được egress")
	}
	if picked.Name != "proxy-fresh" {
		t.Fatalf("phải ưu tiên cặp ít dùng hơn (proxy-fresh), chọn %s", picked.Name)
	}
}

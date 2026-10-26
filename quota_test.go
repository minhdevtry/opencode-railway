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

// Trần ước tính: 429 rời rạc KHÔNG được quyền hạ trần (đo được 429 ở request 59/69 nhưng cặp vẫn phục vụ
// thêm 102/104 request sau đó -> 429 không chứng minh trần). Trần chỉ NÂNG lên khi có bằng chứng tích cực:
// cặp đó đã phục vụ được N request. Override của người vận hành không bị ghi đè.
func TestQuotaCapOnlyRisesFromPositiveEvidence(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	for i := 0; i < 64; i++ {
		quotaAttempt(model, "proxy1", false)
	}
	if got := quotaCap(model); got != 64 {
		t.Fatalf("trần mặc định phải là 64, got %d", got)
	}
	// 429 ở request thứ 64 KHÔNG được hạ trần xuống 64 hay bất kỳ con số nào thấp hơn.
	quotaNoteLimit(model, "proxy1")
	if got := quotaCap(model); got != 64 {
		t.Fatalf("429 rời rạc không được hạ trần, got %d", got)
	}
	// Cặp phục vụ được NONG HƠN trần ước tính -> trần nâng lên (bằng chứng tích cực).
	for i := 0; i < 100; i++ {
		quotaAttempt(model, "proxy2", false)
	}
	quotaSuccess(model, "proxy2")
	if got := quotaCap(model); got != 100 {
		t.Fatalf("cặp phục vụ 100 request thì trần phải nâng lên 100, got %d", got)
	}
	// Override của người vận hành không bị trần đã học ghi đè.
	if got := quotaCap("big-model"); got != 256 {
		t.Fatalf("model có override phải dùng trần override, got %d", got)
	}
	quotaAttempt("big-model", "proxy1", false)
	quotaNoteLimit("big-model", "proxy1")
	if got := quotaCap("big-model"); got != 256 {
		t.Fatalf("429 không được ghi đè override, got %d", got)
	}
}

// 429 đếm theo SỰ KIỆN, không theo response: nhiều request song song trên cùng cặp bị 429 cùng lúc
// (1 sự kiện của upstream) chỉ tính 1 lần, nếu không thì lần đầu tiên đã vượt ngưỡng leo thang
// (khoá cứng + xoay WARP) - đúng cái điều cần tránh.
func TestQuotaLimit429Consecutive(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	if n := quotaNoteLimit(model, "proxy1"); n != 1 {
		t.Fatalf("sự kiện 429 đầu phải = 1, got %d", n)
	}
	// 5 response 429 cùng lúc (nhiều request song song) -> vẫn là 1 sự kiện.
	for i := 0; i < 5; i++ {
		if n := quotaNoteLimit(model, "proxy1"); n != 1 {
			t.Fatalf("các response 429 cùng lúc phải tính là 1 sự kiện, got %d", n)
		}
	}
	// Thành công ngay sau đó (response của request bay từ trước sự kiện) KHÔNG reset được bộ đếm.
	quotaSuccess(model, "proxy1")
	if n := quotaNoteLimit(model, "proxy1"); n != 1 {
		t.Fatalf("thành công ngay sau sự kiện không được reset bộ đếm, got %d", n)
	}
	// Sự kiện mới sau cửa sổ -> tăng lên 2 (đủ ngưỡng leo thang).
	quotaMu.Lock()
	quotaMap[model]["proxy1"].lastLimitAt = time.Now().Add(-2 * quotaLimitEventWindow)
	quotaMu.Unlock()
	if n := quotaNoteLimit(model, "proxy1"); n != 2 {
		t.Fatalf("sự kiện 429 mới (sau cửa sổ) phải = 2, got %d", n)
	}
	// Thành công ngay sau sự kiện thì KHÔNG reset (chưa chứng minh cặp hồi) -> vẫn 2.
	quotaSuccess(model, "proxy1")
	if n := quotaNoteLimit(model, "proxy1"); n != 2 {
		t.Fatalf("thành công ngay sau sự kiện không được reset bộ đếm, got %d", n)
	}
	// Thành công ở thời điểm khác sự kiện -> reset (cặp đã hồi).
	quotaMu.Lock()
	quotaMap[model]["proxy1"].lastLimitAt = time.Now().Add(-2 * quotaLimitEventWindow)
	quotaMu.Unlock()
	quotaSuccess(model, "proxy1")
	if n := quotaNoteLimit(model, "proxy1"); n != 1 {
		t.Fatalf("sau khi cặp hồi thì bộ đếm phải reset, got %d", n)
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

// Số cặp (IP, model) còn ngân sách: auto tính theo share, gọi thẳng tính theo trần đầy đủ.
// Đây là điểm xếp hạng model của kênh auto: nhiều cặp còn sống = model còn nhiều đường.
func TestQuotaModelCapacity(t *testing.T) {
	setupQuotaCfg(t)

	const model = "glm-5.3-flash-free"
	egs := []string{"proxy1", "proxy2"}
	if c := quotaModelCapacities([]string{model}, true, egs)[model]; c != 2 {
		t.Fatalf("mới khởi động phải còn 2 cặp, got %d", c)
	}
	if c := quotaModelCapacities([]string{model}, false, egs)[model]; c != 2 {
		t.Fatalf("mới khởi động phải còn 2 cặp, got %d", c)
	}
	// Auto ăn hết share của proxy1 -> cặp đó không còn sống cho auto.
	for i := 0; i < 38; i++ {
		quotaAttempt(model, "proxy1", true)
	}
	if c := quotaModelCapacities([]string{model}, true, egs)[model]; c != 1 {
		t.Fatalf("auto ăn hết share proxy1 thì còn 1 cặp, got %d", c)
	}
	// Gọi thẳng dùng chung bucket: auto đã ăn 38 trong 64 request của cặp -> cặp đó còn 26 cho gọi
	// thẳng nên vẫn còn sống (khác với auto, đã hết share của mình).
	if c := quotaModelCapacities([]string{model}, false, egs)[model]; c != 2 {
		t.Fatalf("gọi thẳng vẫn còn 2 cặp (cặp 1 còn 26/64 request), got %d", c)
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
	// Trần đã học (từ bằng chứng tích cực) thì giữ qua ngày (đặc tính của model, không theo ngày).
	quotaAttempt(model, "proxy1", false)
	quotaSuccess(model, "proxy1")
	quotaMu.Lock()
	quotaDay = "2000-01-02"
	quotaMu.Unlock()
	if got := quotaCap(model); got != 64 {
		t.Fatalf("trần phải giữ nguyên qua ngày, got %d", got)
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

// Bất biến cốt lõi của phần chia mềm: auto đã ăn hết share của cặp A thì bị ĐẨY XUỐNG (không phải bị cấm),
// còn cặp B còn share thì được ưu tiên. Nếu không còn cặp nào khác thì auto VẪN dùng được cặp A -
// đây là chỗ để trần ước tính sai không bao giờ làm sập gateway.
func TestAutoShareFullIsDeprioritizedNotBlocked(t *testing.T) {
	setupQuotaCfg(t)
	_, m := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	oldPrefer := cfg.PreferDirect
	cfg.PreferDirect = false
	defer func() { cfg.PreferDirect = oldPrefer }()

	full := newEgress("proxy-full", kindProxy, nil)
	full.tier = 2
	full.daemon = "proxy-full"
	full.ipKind.Store(ipKindOwn)
	full.ready.Store(true)
	m.add(full)
	other := newEgress("proxy-other", kindProxy, nil)
	other.tier = 2
	other.daemon = "proxy-other"
	other.ipKind.Store(ipKindOwn)
	other.ready.Store(true)
	m.add(other)

	const model = "glm-5.3-flash-free"
	// Auto ăn hết share của proxy-full (38 request với cap 64, share 0.6).
	for i := 0; i < 38; i++ {
		quotaAttempt(model, "proxy-full", true)
	}
	if p := quotaPenaltyFor(model, "proxy-full", true); p <= 0 {
		t.Fatalf("cặp đã ăn hết share phải bị phạt, got %v", p)
	}

	// Còn cặp khác có share -> auto phải chọn cặp khác.
	picked := m.pickAvoid(model, true)
	if picked == nil {
		t.Fatal("auto phải chọn được egress")
	}
	if picked.Name != "proxy-other" {
		t.Fatalf("auto phải tránh cặp đã ăn hết share, chọn %s", picked.Name)
	}

	// Gọi thẳng thì cặp đã ăn hết share VẪN dùng được (không bị trần chặn).
	if p := quotaPenaltyFor(model, "proxy-full", false); p != 0 {
		t.Fatalf("gọi thẳng không bị phạt, got %v", p)
	}
	if got := m.pickAvoid(model, false); got == nil {
		t.Fatal("gọi thẳng phải chọn được egress")
	}
}

// Thứ tự ưu tiên trong bộ chọn egress: cặp RIÊNG còn share > cặp RIÊNG đã hết share > IP DÙNG CHUNG.
// (IP dùng chung bị phạt nặng nhất vì hạn mức của nó dùng chung với container khác.)
func TestPenaltyOrderPrivateThenShared(t *testing.T) {
	setupQuotaCfg(t)
	_, m := setup(t, func(w http.ResponseWriter, r *http.Request) {}, false)
	oldPrefer := cfg.PreferDirect
	cfg.PreferDirect = false
	defer func() { cfg.PreferDirect = oldPrefer }()

	private := newEgress("proxy-private", kindProxy, nil)
	private.tier = 2
	private.daemon = "proxy-private"
	private.ipKind.Store(ipKindOwn)
	private.ready.Store(true)
	m.add(private)
	shared := newEgress("proxy-shared", kindProxy, nil)
	shared.tier = 2
	shared.daemon = "proxy-shared"
	shared.ipKind.Store(ipKindShared)
	shared.ready.Store(true)
	m.add(shared)

	const model = "glm-5.3-flash-free"
	// Auto ăn hết share của cặp RIÊNG -> nó bị phạt, nhưng vẫn phải thắng IP dùng chung.
	for i := 0; i < 38; i++ {
		quotaAttempt(model, "proxy-private", true)
	}
	picked := m.pickAvoid(model, true)
	if picked == nil {
		t.Fatal("auto phải chọn được egress")
	}
	if picked.Name != "proxy-private" {
		t.Fatalf("cặp riêng (dù đã hết share) phải thắng IP dùng chung, chọn %s", picked.Name)
	}
}

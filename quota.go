package main

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ======================== SỔ NGÂN SÁCH PER (MODEL, EGRESS) ========================
//
// Đo 2026-10-26 (research/limit-shape + research/limit-shape/model-dim, log thô trong logs/):
//
//  1. Bucket của upstream là per CẶP (IP, model), reset 00:00 UTC: cùng một IPv6,
//     glm-5.3-flash-free cạn sau 64 request còn nemotron-3.5-lightning-free vẫn 22/22 x 200
//     (và chiều ngược lại: nemotron chạy 203 request không cạn, glm vẫn 18/20 x 200).
//  2. Đơn vị tiêu thụ là REQUEST, không phải session: 1 session cố định phục vụ 64 request rồi mới 429.
//  3. Trần khác nhau theo model: glm ~60-70 request/IP/ngày, nemotron >203.
//  4. 429 hạn mức là SỰ KIỆN RỜI RẠC, không khoá cứng: sau sự kiện 429, 45/45 request tiếp theo vẫn 200.
//
// Sổ này đếm request đã dùng của từng cặp (model, egress) trong ngày UTC, tách 2 kênh (auto / gọi thẳng)
// để CHIA MỀM: auto và gọi thẳng dùng CHUNG bucket của upstream, nên auto chỉ được ăn tối đa
// QUOTA_AUTO_SHARE phần ngân sách của mỗi cặp, phần còn lại dành cho gọi thẳng model đó.
//
// NGUYÊN TẮC: con số trần là GỢI Ý, không bao giờ là thứ chặn traffic.
//   - Trần quá thấp  -> auto bị đẩy xuống cuối bảng xếp hạng (ít hiệu quả hơn, KHÔNG sập).
//   - Trần quá cao   -> auto dùng hơn mức thật, gặp 429, và cơ chế leo thang (2 lần 429 liên tiếp)
//     xử lý như sự kiện quan sát được, không dựa vào trần.
//   - Gọi thẳng KHÔNG BAO GIỜ bị trần chặn.
// Thứ duy nhất chặn traffic là 429 quan sát được từ upstream.

type quotaState struct {
	attemptsAuto   int // request đã gửi lên upstream qua kênh auto
	attemptsDirect int // request đã gửi lên upstream qua kênh gọi thẳng model
	limit429       int // số 429 hạn mức liên tiếp (reset khi có 1 request thành công)
}

var (
	quotaMu   sync.Mutex
	quotaDay  string                            // ngày UTC của số liệu đang đếm
	quotaMap  map[string]map[string]*quotaState // model -> egress -> số liệu
	quotaCaps map[string]int                    // model -> trần request/ngày HỌC ĐƯỢC từ 429 quan sát được
)

func init() {
	quotaMap = map[string]map[string]*quotaState{}
	quotaCaps = map[string]int{}
}

// quotaResetDayLocked: sang ngày UTC mới thì mọi bộ đếm về 0 (ngân sách upstream reset 00:00 UTC).
// Trần đã học thì GIỮ: đó là đặc tính của model, không phải của ngày.
func quotaResetDayLocked() {
	d := utcDay()
	if quotaDay == d {
		return
	}
	quotaDay = d
	quotaMap = map[string]map[string]*quotaState{}
}

// quotaStateFor: trả về (và tạo nếu chưa có) số liệu của cặp (model, egress). Gọi khi giữ khoá quotaMu.
func quotaStateFor(model, egress string) *quotaState {
	quotaResetDayLocked()
	if quotaMap[model] == nil {
		quotaMap[model] = map[string]*quotaState{}
	}
	st := quotaMap[model][egress]
	if st == nil {
		st = &quotaState{}
		quotaMap[model][egress] = st
	}
	return st
}

// quotaAttempt: 1 request ĐÃ GỬI lên upstream trên cặp (model, egress).
// Đếm cả request lỗi: nó vẫn nằm trên IP đó và vẫn làm hao ngân sách chung của IP.
// (Đếm request thành công thì circuit kẹt - chưa từng thành công - luôn trông "ít dùng" và bị ưu tiên.)
func quotaAttempt(model, egress string, auto bool) {
	if model == "" || egress == "" {
		return
	}
	quotaMu.Lock()
	defer quotaMu.Unlock()
	st := quotaStateFor(model, egress)
	if auto {
		st.attemptsAuto++
	} else {
		st.attemptsDirect++
	}
}

// quotaSuccess: 1 request thành công -> sự kiện 429 trước đó của cặp này không còn ý nghĩa.
func quotaSuccess(model, egress string) {
	if model == "" || egress == "" {
		return
	}
	quotaMu.Lock()
	defer quotaMu.Unlock()
	if st := quotaMap[model][egress]; st != nil {
		st.limit429 = 0
	}
}

// quotaNoteLimit: 1 lần 429 hạn mức trên cặp (model, egress).
// Trả về số lần 429 hạn mức LIÊN TIẾP của cặp này (dùng để leo thang: 1 lần = rời rạc,
// từ lần thứ N thì mới coi là cặp đó thật sự cạn).
func quotaNoteLimit(model, egress string) int {
	if model == "" || egress == "" {
		return 0
	}
	quotaMu.Lock()
	defer quotaMu.Unlock()
	st := quotaStateFor(model, egress)
	st.limit429++
	// Học trần của model: lần 429 xảy ra SAU khi đã gửi N request => trần của model trên IP này ≈ N.
	// Chỉ tăng, không giảm (mẫu khác nhau: 59 / 64 / 69 cùng model).
	if used := st.attemptsAuto + st.attemptsDirect; used > quotaCaps[model] {
		quotaCaps[model] = used
	}
	return st.limit429
}

// quotaCapLocked: trần request/ngày của model. Thứ tự ưu tiên: đã học từ 429 thật > override > mặc định.
func quotaCapLocked(model string) int {
	if c := quotaCaps[model]; c > 0 {
		return c
	}
	if c, ok := cfg.QuotaOverrides[model]; ok && c > 0 {
		return c
	}
	return cfg.QuotaDefault
}

// quotaCap: trần request/ngày của model (ước tính).
func quotaCap(model string) int {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	return quotaCapLocked(model)
}

// quotaUsed: số request đã gửi hôm nay của cặp (model, egress), tách 2 kênh.
func quotaUsed(model, egress string) (auto, direct int) {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	if st := quotaMap[model][egress]; st != nil {
		return st.attemptsAuto, st.attemptsDirect
	}
	return 0, 0
}

// quotaRank: dùng để xếp hạng egress cho 1 model. Thấp = còn nhiều ngân sách.
// Dùng tổng đã dùng (auto + gọi thẳng) vì 2 kênh dùng chung bucket của upstream.
func quotaRank(model, egress string) int {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	if st := quotaMap[model][egress]; st != nil {
		return st.attemptsAuto + st.attemptsDirect
	}
	return 0
}

// quotaAutoShareLeft: phần ngân sách của cặp mà auto còn được dùng (tính theo share của cfg).
// Trả về (còn, tổng) để bộ chọn egress dùng làm hạn mức ưu tiên.
// total <= 0 nghĩa là KHÔNG có giới hạn share (chưa biết trần, hoặc share bị tắt) -> auto dùng thoải mái.
func quotaAutoShareLeft(model, egress string) (left, total int) {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	cap := quotaCapLocked(model)
	total = int(float64(cap) * cfg.QuotaAutoShare)
	if total <= 0 {
		return cap, 0 // không giới hạn share
	}
	used := 0
	if st := quotaMap[model][egress]; st != nil {
		used = st.attemptsAuto
	}
	left = total - used
	if left < 0 {
		left = 0
	}
	return left, total
}

// quotaModelCapacity: tổng sức chứng còn lại của model trên các egress đưa vào.
// auto=true: tính theo phần share dành cho auto; auto=false: tính theo trần đầy đủ (gọi thẳng).
func quotaModelCapacity(model string, auto bool, egresses []string) int {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	cap := quotaCapLocked(model)
	limit := cap
	if auto {
		limit = int(float64(cap) * cfg.QuotaAutoShare)
	}
	total := 0
	for _, e := range egresses {
		var used int
		if st := quotaMap[model][e]; st != nil {
			if auto {
				used = st.attemptsAuto
			} else {
				used = st.attemptsAuto + st.attemptsDirect
			}
		}
		if r := limit - used; r > 0 {
			total += r
		}
	}
	return total
}

// quotaClearEgress: egress vừa đổi IP -> ngân sách (IP, model) cũ hết hiệu lực, bộ đếm về 0.
func quotaClearEgress(egress string) {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	for model, byEgress := range quotaMap {
		delete(byEgress, egress)
		if len(byEgress) == 0 {
			delete(quotaMap, model)
		}
	}
}

// quotaSnapshot: trạng thái ngân sách để hiện trong /status và /v1/capacity.
func quotaSnapshot(egresses []string) map[string]any {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	out := map[string]any{}
	for model := range quotaMap {
		cap := quotaCapLocked(model)
		auto, direct := 0, 0
		softFull, pairs := 0, 0
		for _, e := range egresses {
			st := quotaMap[model][e]
			if st == nil {
				continue
			}
			pairs++
			auto += st.attemptsAuto
			direct += st.attemptsDirect
			if st.limit429 > 0 {
				softFull++
			}
		}
		if pairs == 0 {
			continue
		}
		out[model] = map[string]any{
			"cap":             cap,
			"used_auto":       auto,
			"used_direct":     direct,
			"auto_share_left": int(float64(cap)*cfg.QuotaAutoShare) - auto,
			"pairs_used":      pairs,
			"pairs_soft_full": softFull,
			"reset_at":        time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Format(time.RFC3339),
		}
	}
	return out
}

// egressNames: tên các egress đang có (dùng để tính sức chứng theo model).
func (m *Manager) egressNames() []string {
	list := m.snapshot()
	out := make([]string, len(list))
	for i, e := range list {
		out[i] = e.Name
	}
	return out
}

// orderModelsByCapacity: xếp model theo sức chứng còn lại GIẢM DẦN.
// auto=true: tính theo phần share dành cho auto; auto=false: tính theo trần đầy đủ (gọi thẳng).
// Khi bằng nhau thì xoay vòng theo bộ đếm round-robin để không dồn hết vào một model.
func (m *Manager) orderModelsByCapacity(pool []string, auto bool) []string {
	n := len(pool)
	if n <= 1 {
		return pool
	}
	start := int(atomic.AddUint32(&poolIndex, 1)) % n
	names := make([]string, n)
	caps := make([]int, n)
	egs := m.egressNames()
	for i, mdl := range pool {
		names[i] = mdl
		caps[i] = quotaModelCapacity(mdl, auto, egs)
	}
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		if caps[idx[a]] != caps[idx[b]] {
			return caps[idx[a]] > caps[idx[b]]
		}
		ra := (idx[a] - start + n) % n
		rb := (idx[b] - start + n) % n
		return ra < rb
	})
	out := make([]string, n)
	for i, j := range idx {
		out[i] = names[j]
	}
	return out
}

// resetQuota (chỉ dùng trong test).
func resetQuota() {
	quotaMu.Lock()
	quotaDay = utcDay()
	quotaMap = map[string]map[string]*quotaState{}
	quotaCaps = map[string]int{}
	quotaMu.Unlock()
}

// quotaSharePenalty: hình phạt xếp hạng cho cặp mà kênh auto đã ăn hết phần share của nó.
// Hình phạt, KHÔNG phải cấm: nếu không còn đường nào khác thì auto vẫn dùng được (mất một phần
// ngân sách của gọi thẳng còn hơn không phục vụ được). Nhờ vậy trần ước tính sai KHÔNG gây sập.
const quotaSharePenalty = 1e6

// quotaPenaltyFor: hình phạt xếp hạng của cặp (model, egress) cho kênh auto.
func quotaPenaltyFor(model, egress string, auto bool) float64 {
	if !auto {
		return 0
	}
	left, total := quotaAutoShareLeft(model, egress)
	if total <= 0 {
		return 0 // không giới hạn share -> không phạt
	}
	if left <= 0 {
		return quotaSharePenalty
	}
	return 0
}

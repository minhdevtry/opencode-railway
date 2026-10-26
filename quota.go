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
//   - Trần quá cao   -> auto dùng hơn mức thật, gặp 429, và cơ chế leo thang (2 SỰ KIỆN 429)
//     xử lý như sự kiện quan sát được, không dựa vào trần.
//   - Gọi thẳng KHÔNG BAO GIỜ bị trần chặn.
// Thứ duy nhất chặn traffic là 429 quan sát được từ upstream.
//
// LƯU Ý VỀ PHẦN DÀNH CHO GỌI THẲNG: cơ chế bảo vệ phần của gọi thẳng là GIỚI HẠN của auto (auto ăn
// tối đa QUOTA_AUTO_SHARE), không phải việc chặn gọi thẳng. Gọi thẳng vẫn được dùng mọi cặp còn sức chứng
// và tự ưu tiên cặp ít dùng (quotaRankWeight).
//
// GIỚI HẠN ĐÃ BIẾT: sổ này nằm trong RAM của tiến trình, còn ngân sách thật của upstream thì KHÔNG reset
// khi ta restart. Restart gateway => ta tưởng cặp (IP, model) còn nguyên trong khi upstream đã dùng một phần.
// Cơ chế leo thang theo 429 là thứ bảo vệ ta trong tình huống đó.

type quotaState struct {
	attemptsAuto   int       // request đã gửi lên upstream qua kênh auto
	attemptsDirect int       // request đã gửi lên upstream qua kênh gọi thẳng model
	limit429       int       // số SỰ KIỆN 429 hạn mức liên tiếp (không phải số response)
	lastLimitAt    time.Time // lúc sự kiện 429 gần nhất
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

// quotaSuccess: 1 request thành công trên cặp (model, egress).
//   - Bằng chứng TÍCH CỰC cho trần: cặp này đã phục vụ được N request -> trần của model trên IP đó >= N.
//     (Không học trần từ 429: 429 rời rạc không chứng minh trần - đo được 429 ở request 59/69 nhưng
//     cặp vẫn phục vụ thêm 102/104 request sau đó.)
//   - Reset đếm sự kiện 429: chỉ reset khi thành công KHÁC sự kiện 429 gần nhất, còn thì đó là
//     response của request đã bay từ trước sự kiện, không phải bằng chứng cặp đã hồi.
func quotaSuccess(model, egress string) {
	if model == "" || egress == "" {
		return
	}
	quotaMu.Lock()
	defer quotaMu.Unlock()
	st := quotaMap[model][egress]
	if st == nil {
		return
	}
	if used := st.attemptsAuto + st.attemptsDirect; used > quotaCaps[model] {
		quotaCaps[model] = used
	}
	if time.Since(st.lastLimitAt) >= quotaLimitEventWindow {
		st.limit429 = 0
	}
}

// quotaLimitEventWindow: các response 429 trong khoảng này được coi là CÙNG 1 sự kiện.
// Gateway chạy nhiều request song song trên cùng cặp (IP, model), nên MỘT sự kiện 429 của upstream
// sinh ra K response 429 -> nếu đếm theo response thì ngay lần đầu đã vượt ngưỡng leo thang
// (khoá cứng + xoay WARP), đúng cái điều cần tránh. Sự kiện kế tiếp (sau khi cooldown + probe)
// cách nhau hơn cửa sổ này nên vẫn được đếm là sự kiện mới.
const quotaLimitEventWindow = 30 * time.Second

// quotaNoteLimit: 1 response 429 hạn mức trên cặp (model, egress).
// Trả về số SỰ KIỆN 429 hạn mức liên tiếp của cặp này (dùng để leo thang: 1 sự kiện = rời rạc,
// từ sự kiện thứ N thì mới coi là cặp đó thật sự cạn).
func quotaNoteLimit(model, egress string) int {
	if model == "" || egress == "" {
		return 0
	}
	quotaMu.Lock()
	defer quotaMu.Unlock()
	st := quotaStateFor(model, egress)
	now := time.Now()
	if now.Sub(st.lastLimitAt) < quotaLimitEventWindow {
		return st.limit429 // cùng 1 sự kiện (nhiều request song song cùng bị 429)
	}
	st.lastLimitAt = now
	st.limit429++
	return st.limit429
}

// quotaCapLocked: trần request/ngày của model = MAX(mặc định, override, trần đã học).
// Trần đã học chỉ có thể NÂNG lên (bằng chứng: cặp đó đã phục vụ được N request), không bao giờ
// hạ xuống - một sự kiện 429 rời rạc không có quyền ghi đè trần của người vận hành.
func quotaCapLocked(model string) int {
	c := cfg.QuotaDefault
	if o, ok := cfg.QuotaOverrides[model]; ok && o > c {
		c = o
	}
	if l := quotaCaps[model]; l > c {
		c = l
	}
	return c
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
			"cap":              cap,
			"used_auto":        auto,
			"used_direct":      direct,
			"auto_share_left":  int(float64(cap)*cfg.QuotaAutoShare) - auto,
			"pairs_used":       pairs,
			"pairs_recent_429": softFull, // số cặp vừa gặp 429 hạn mức (KHÔNG có nghĩa là cặp đã cạn: 429 rời rạc)
			"reset_at":         time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour).Format(time.RFC3339),
		}
	}
	return out
}

// quotaModelCapacities: số cặp (IP, model) CÒN NGÂN SÁCH của mỗi model, gộp trong 1 lần khoá
// (đường nóng: mỗi request đều xếp lại pool, không thể khoá 7 lần cho 7 model).
// auto=true: tính theo phần share dành cho auto; auto=false: tính theo trần đầy đủ (gọi thẳng).
//
// Xếp hạng theo SỐ CẶP CÒN SỐNG chứ không theo TỔNG sức chứng: mỗi model có bucket RIÊNG trên mỗi IP,
// nên xếp theo tổng sẽ dồn hết traffic auto vào model có trần lớn nhất (nemotron 256) và bỏ lãng
// phần sức chứng của các model khác. Số cặp còn sống + xoay vòng round-robin thì trải đều.
func quotaModelCapacities(models []string, auto bool, egresses []string) map[string]int {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	out := make(map[string]int, len(models))
	for _, model := range models {
		cap := quotaCapLocked(model)
		limit := cap
		if auto {
			limit = int(float64(cap) * cfg.QuotaAutoShare)
		}
		alive := 0
		for _, e := range egresses {
			var used int
			if st := quotaMap[model][e]; st != nil {
				if auto {
					used = st.attemptsAuto
				} else {
					used = st.attemptsAuto + st.attemptsDirect
				}
			}
			if limit-used > 0 {
				alive++
			}
		}
		out[model] = alive
	}
	return out
}

// quotaPairInfo: số liệu ngân sách của 1 cặp (model, egress).
type quotaPairInfo struct {
	rank       int // số request đã gửi hôm nay trên cặp này
	shareLeft  int // phần share của auto còn lại (0 = đã ăn hết, hoặc không áp giới hạn share)
	shareTotal int // tổng share của auto cho cặp này (0 = không áp giới hạn share)
}

// quotaPairInfos: lấy số liệu của NHIỀU cặp trong 1 lần khoá (đường nóng: mỗi request duyệt hết
// danh sách egress, không thể khoá cho từng cặp).
func quotaPairInfos(model string, egresses []string) []quotaPairInfo {
	quotaMu.Lock()
	defer quotaMu.Unlock()
	quotaResetDayLocked()
	cap := quotaCapLocked(model)
	total := int(float64(cap) * cfg.QuotaAutoShare)
	out := make([]quotaPairInfo, len(egresses))
	for i, e := range egresses {
		st := quotaMap[model][e]
		if st != nil {
			out[i].rank = st.attemptsAuto + st.attemptsDirect
		}
		if total > 0 {
			used := 0
			if st != nil {
				used = st.attemptsAuto
			}
			out[i].shareTotal = total
			out[i].shareLeft = total - used
			if out[i].shareLeft < 0 {
				out[i].shareLeft = 0
			}
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
// Khi bằng nhau thì xoay vòng theo bộ đếm round-robin để không dồn hết vào một model.
func (m *Manager) orderModelsByCapacity(pool []string, auto bool) []string {
	n := len(pool)
	if n <= 1 {
		return pool
	}
	start := int(atomic.AddUint32(&poolIndex, 1)) % n
	caps := quotaModelCapacities(pool, auto, m.egressNames())
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool {
		ca, cb := caps[pool[idx[a]]], caps[pool[idx[b]]]
		if ca != cb {
			return ca > cb
		}
		ra := (idx[a] - start + n) % n
		rb := (idx[b] - start + n) % n
		return ra < rb
	})
	out := make([]string, n)
	for i, j := range idx {
		out[i] = pool[j]
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

// quotaRankWeight: trọng số của số request đã dùng trong xếp hạng egress. Nhỏ hơn hình phạt của
// IP dùng chung (1e9) và của share đã ăn hết (1e6), nhưng lớn hơn chênh lệch độ trễ thường gặp
// -> ưu tiên cặp (IP, model) còn nhiều ngân sách mà vẫn không bỏ qua độ trễ.
const quotaRankWeight = 10

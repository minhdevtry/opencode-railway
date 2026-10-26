package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
)

// ======================== PROFILE CỦA MODEL AUTO + GIỚI HẠN CONTEXT TỰ HỌC ========================
//
// Client chỉ chọn "kiểu việc", gateway chọn model:
//
//	auto       mọi model trong pool (mặc định, tải cao)
//	auto-fast  model nhanh nhất khi tắt reasoning (đo 2026-10-24: nemotron-lightning ~1.3s, glm, beam)
//	auto-best  model trả lời kỹ nhất (deepseek, glm, mimo)
//	auto-long  mọi model đọc được văn bản dài (lọc theo giới hạn context đã học)
//
// Danh sách đổi được bằng env AUTO_FAST_MODELS / AUTO_BEST_MODELS / AUTO_LONG_MODELS (trống = mặc định dưới đây).
//
// Giới hạn context không gắn cứng theo catalog (catalog sai: models.dev ghi deepseek 200k nhưng đo đọc được 318k;
// nemotron/glm/beam đọc đúng ~300k chứ không phải 128k). Gateway ước lượng số token của request và ghi nhớ mỗi lần
// upstream báo 400 "vượt context": model đó không nhận request cỡ đó nữa, request hiện tại tự chuyển sang model khác.

var profileDefaults = map[string][]string{
	"fast": {"nemotron-3.5-lightning-free", "glm-5.3-flash-free", "beam-free"},
	"best": {"deepseek-v4.1-flash-free", "glm-5.3-flash-free", "mimo-v2.6-flash-free"},
	"long": nil, // nil = mọi model, chỉ lọc theo context
}

// profileOf: "" nếu không phải tên auto; còn lại "auto" | "fast" | "best" | "long".
func profileOf(name string) string {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "auto", "auto-free", "free", "pool", "free-pool":
		return "auto"
	case "auto-fast", "fast-free", "auto-burst":
		return "fast"
	case "auto-best", "auto-quality", "best-free":
		return "best"
	case "auto-long", "long-free":
		return "long"
	}
	return ""
}

func profileModels(p string) []string {
	env := map[string]string{"fast": cfg.AutoFastModels, "best": cfg.AutoBestModels, "long": cfg.AutoLongModels}[p]
	if l := splitList(env); len(l) > 0 {
		return l
	}
	return profileDefaults[p]
}

// ---- giới hạn context tự học ----

var (
	ctxMu    sync.Mutex
	ctxLimit = map[string]int{ // số token ước lượng nhỏ nhất từng bị upstream từ chối vì vượt context
		"big-pickle": 160000, // đo 2026-10-25: 800 KB (~295k) => 400; models.dev: input 160k
	}
)

// estTokens ước lượng số token từ kích thước payload JSON (đo trên dữ liệu tiếng Việt: ~2,7 byte/token).
func estTokens(payloadBytes int) int { return payloadBytes * 10 / 27 }

func noteContextLimit(model string, tokens int) {
	ctxMu.Lock()
	if old, ok := ctxLimit[model]; !ok || tokens < old {
		ctxLimit[model] = tokens
	}
	ctxMu.Unlock()
}

func contextTooBig(model string, tokens int) bool {
	ctxMu.Lock()
	defer ctxMu.Unlock()
	lim, ok := ctxLimit[model]
	return ok && tokens >= lim*95/100
}

func contextLimits() map[string]int {
	ctxMu.Lock()
	defer ctxMu.Unlock()
	out := make(map[string]int, len(ctxLimit))
	for k, v := range ctxLimit {
		out[k] = v
	}
	return out
}

func isContextError(body []byte) bool {
	b := bytes.ToLower(body)
	return bytes.Contains(b, []byte("maximum context")) || bytes.Contains(b, []byte("context length")) ||
		bytes.Contains(b, []byte("context_length")) || bytes.Contains(b, []byte("too many tokens")) ||
		bytes.Contains(b, []byte("prompt is too long"))
}

// ---- truyền profile qua context của request ----

type profileKey struct{}

func withProfile(ctx context.Context, modelName string) context.Context {
	if p := profileOf(modelName); p != "" {
		return context.WithValue(ctx, profileKey{}, p)
	}
	return ctx
}

func profileFrom(ctx context.Context) string {
	p, _ := ctx.Value(profileKey{}).(string)
	return p
}

// poolAllExcluded: mọi model trong pool đều bị loại => không còn model nào phục vụ được request này.
// Dùng để fail nhanh (400) thay vì xếp hàng tới MAX_WAIT rồi trả 429.
func poolAllExcluded(excl map[string]bool) bool {
	poolMu.RLock()
	pool := append([]string(nil), freeModelPool...)
	poolMu.RUnlock()
	if len(pool) == 0 {
		return false
	}
	for _, m := range pool {
		if !excl[m] {
			return false
		}
	}
	return true
}

// useProfileFilter: model có thuộc profile không (profile rỗng = cho phép tất cả).
func useProfileFilter(allowed map[string]bool, m string) bool {
	return len(allowed) > 0 && !allowed[m]
}

// routeExclusions: các model trong pool KHÔNG được dùng cho request (ngoài profile, hoặc context quá lớn với model đó).
// Không bao giờ loại hết: nếu bộ lọc loại sạch pool thì chỉ áp phần context (thà thử còn hơn từ chối).
func routeExclusions(profile string, tokens int, base map[string]bool) map[string]bool {
	poolMu.RLock()
	pool := append([]string(nil), freeModelPool...)
	poolMu.RUnlock()
	allowed := map[string]bool{}
	if list := profileModels(profile); len(list) > 0 {
		for _, m := range list {
			allowed[m] = true
		}
	}
	build := func(useProfile bool) map[string]bool {
		out := map[string]bool{}
		for k, v := range base {
			out[k] = v
		}
		for _, m := range pool {
			if (useProfile && len(allowed) > 0 && !allowed[m]) || contextTooBig(m, tokens) {
				out[m] = true
			}
		}
		// Model đã hết ngân sách free (đo 2026-10-26: hạn mức tính RIÊNG từng model) -> không gửi request vào nó.
		for m := range modelBudgetSnapshot() {
			out[m] = true
		}
		return out
	}
	for _, useProfile := range []bool{true, false} {
		ex := build(useProfile)
		for _, m := range pool {
			if !ex[m] {
				return ex
			}
		}
	}
	// Mọi model trong pool đều bị loại (kể cả model hết ngân sách) -> bỏ phần "hết ngân sách" đi và vẫn thử:
	// có thể upstream đã reset mà ta chưa biết, và thử còn hơn xếp hàng tới MAX_WAIT rồi trả 429.
	ex := map[string]bool{}
	for k, v := range base {
		ex[k] = v
	}
	for _, m := range pool {
		if (useProfileFilter(allowed, m)) || contextTooBig(m, tokens) {
			ex[m] = true
		}
	}
	for _, m := range pool {
		if !ex[m] {
			return ex
		}
	}
	return base // mọi model đều được ghi nhận là quá nhỏ: vẫn thử, upstream sẽ báo lỗi rõ ràng
}

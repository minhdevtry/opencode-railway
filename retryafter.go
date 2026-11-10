package main

import (
	"fmt"
	"net/http"
	"time"
)

// ======================== HEADER RETRY-AFTER CHO CLIENT ========================
//
// Mốc quota hồi của upstream luôn rơi vào nửa đêm UTC nên Retry-After có thể là hàng GIỜ (vd 32391 giây
// = 9h). Client chuẩn (Anthropic SDK / Claude Code) chỉ honor Retry-After tới retry.maxDelayMs (mặc định
// 300000 ms = 5 phút): header dài hơn -> client ABORT chứ không retry, request chết cứng (đo 2026-11-10).
//
// Vì vậy header Retry-After luôn được GIỚI HẠN (mặc định 60s) để client thử lại được, còn mốc quota hồi
// THẬT vẫn được trả về ở header phụ "X-Quota-Reset" (epoch giây) cho client muốn hẹn giờ thay vì polling.
//
// clientRetryAfter: giá trị Retry-After an toàn cho client (0 = không gửi header).
func clientRetryAfter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	lim := cfg.RetryAfterMax
	if lim <= 0 {
		lim = 60 * time.Second
	}
	if d > lim {
		return lim
	}
	return d
}

// setRetryAfterHeaders: ghi Retry-After (đã giới hạn) + X-Quota-Reset (mốc quota hồi thật).
func setRetryAfterHeaders(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		return
	}
	w.Header().Set("Retry-After", fmt.Sprint(int(clientRetryAfter(d).Seconds())+1))
	w.Header().Set("X-Quota-Reset", fmt.Sprint(time.Now().Add(d).Unix()))
}

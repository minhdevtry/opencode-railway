package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// X-Max-Wait phải là MỐC TUYỆT ĐỐI: mỗi lần forward() trong cùng 1 request phải dùng chung 1 hạn, không phải hạn mới.
// Nếu không, client yêu cầu 15s nhưng gateway retry 8 lần thì client thực tế chờ 8 x 15s.
func TestMaxWaitIsAbsoluteAcrossRetries(t *testing.T) {
	old := cfg.MaxWait
	cfg.MaxWait = 10 * time.Minute
	defer func() { cfg.MaxWait = old }()

	r, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set("X-Max-Wait", "15s")
	ctx := withMaxWait(context.Background(), r)

	dl, ok := deadlineFrom(ctx)
	if !ok {
		t.Fatal("X-Max-Wait phải tạo deadline")
	}
	if d := time.Until(dl); d <= 14*time.Second || d > 15*time.Second {
		t.Fatalf("deadline phải ~15s, got %v", d)
	}

	// Lần thứ 2 (giả lập retry): cùng 1 mốc tuyệt đối, ngân sách chờ phải RÚT NGẮN chứ không reset về 15s.
	first := waitBudget(ctx)
	time.Sleep(200 * time.Millisecond)
	dl2, _ := deadlineFrom(ctx)
	if !dl2.Equal(dl) {
		t.Fatalf("hạn phải là mốc tuyệt đối, không tính lại mỗi lần thử (dl=%v dl2=%v)", dl, dl2)
	}
	second := waitBudget(ctx)
	if second >= first {
		t.Fatalf("ngân sách chờ phải rút ngắn sau retry, không reset: %v -> %v", first, second)
	}
	if second > 15*time.Second {
		t.Fatalf("ngân sách chờ vượt hạn client đã đặt: %v", second)
	}
}

// Hạn client không được vượt quá MAX_WAIT của gateway.
func TestMaxWaitCappedByServerMaxWait(t *testing.T) {
	old := cfg.MaxWait
	cfg.MaxWait = 5 * time.Second
	defer func() { cfg.MaxWait = old }()

	r, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set("X-Max-Wait", "1h")
	dl, ok := deadlineFrom(withMaxWait(context.Background(), r))
	if !ok {
		t.Fatal("X-Max-Wait phải tạo deadline")
	}
	if d := time.Until(dl); d > 5*time.Second {
		t.Fatalf("hạn client phải bị chặn bởi MAX_WAIT của server, got %v", d)
	}
}

// Header rác / thiếu thì bỏ qua và dùng MAX_WAIT của server (client không được tự ý rút ngắn).
func TestMaxWaitIgnoresBadHeader(t *testing.T) {
	old := cfg.MaxWait
	cfg.MaxWait = 7 * time.Second
	defer func() { cfg.MaxWait = old }()

	for _, v := range []string{"", "abc", "0", "-5s"} {
		r, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
		if v != "" {
			r.Header.Set("X-Max-Wait", v)
		}
		dl, ok := deadlineFrom(withMaxWait(context.Background(), r))
		if !ok {
			t.Fatalf("header %q: phải có deadline (fallback về MAX_WAIT)", v)
		}
		if d := time.Until(dl); d > 7*time.Second {
			t.Fatalf("header %q: deadline phải theo MAX_WAIT của server, got %v", v, d)
		}
	}
}

// Client chỉ được RÚT NGẮN hạn, không được dài hơn MAX_WAIT của server.
func TestMaxWaitCannotExceedServerMaxWait(t *testing.T) {
	old := cfg.MaxWait
	cfg.MaxWait = 3 * time.Second
	defer func() { cfg.MaxWait = old }()

	r, _ := http.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set("X-Max-Wait", "1h")
	dl, ok := deadlineFrom(withMaxWait(context.Background(), r))
	if !ok {
		t.Fatal("phải có deadline")
	}
	if d := time.Until(dl); d > 3*time.Second {
		t.Fatalf("client không được dài hơn MAX_WAIT của server, got %v", d)
	}
}

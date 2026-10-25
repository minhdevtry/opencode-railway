package main

import "testing"

func TestRouteExclusionsProfilesAndContext(t *testing.T) {
	old, oldPool, oldLim := cfg, freeModelPool, ctxLimit
	defer func() { cfg, freeModelPool, ctxLimit = old, oldPool, oldLim }()
	cfg = Config{}
	freeModelPool = []string{"glm-5.3-flash-free", "deepseek-v4.1-flash-free", "nemotron-3.5-lightning-free", "big-pickle", "beam-free"}
	ctxLimit = map[string]int{"big-pickle": 160000}

	ex := routeExclusions("fast", 1000, nil)
	if ex["glm-5.3-flash-free"] || ex["nemotron-3.5-lightning-free"] || ex["beam-free"] || !ex["deepseek-v4.1-flash-free"] || !ex["big-pickle"] {
		t.Fatalf("auto-fast chỉ được dùng nhóm nhanh: %v", ex)
	}
	if ex := routeExclusions("auto", 200000, nil); !ex["big-pickle"] || ex["glm-5.3-flash-free"] {
		t.Fatalf("request ~200k token phải tránh big-pickle (giới hạn 160k): %v", ex)
	}
	// Học giới hạn mới từ lỗi 400 vượt context
	noteContextLimit("deepseek-v4.1-flash-free", 380000)
	if !contextTooBig("deepseek-v4.1-flash-free", 390000) || contextTooBig("deepseek-v4.1-flash-free", 300000) {
		t.Fatal("giới hạn context đã học phải chặn request lớn hơn và cho request nhỏ hơn")
	}
	// Nếu bộ lọc loại sạch pool thì không được loại hết (thà thử còn hơn từ chối)
	for _, m := range freeModelPool {
		noteContextLimit(m, 10)
	}
	ex = routeExclusions("best", 500000, nil)
	if len(ex) >= len(freeModelPool) {
		t.Fatalf("không được loại toàn bộ pool: %v", ex)
	}
	if !isContextError([]byte(`{"error":{"message":"Your request resulted in more tokens than this model's maximum context length"}}`)) {
		t.Fatal("phải nhận ra lỗi vượt context")
	}
	if profileOf("auto-long") != "long" || profileOf("deepseek") != "" || !isAutoModel("auto-best") {
		t.Fatal("nhận diện tên profile sai")
	}
}

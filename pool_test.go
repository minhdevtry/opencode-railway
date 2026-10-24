package main

import "testing"

// Test cho hành vi pool của model `auto`.
//
// Vấn đề gốc (đo 24/10): syncCatalog() gộp mọi model Free của upstream vào pool
// kể cả khi người dùng đã tự định nghĩa pool bằng AUTO_MODELS ⇒ biến môi
// trường bị vô hiệu hoá, và các model "phế" quay lại pool sau mỗi lần đồng bộ.
func TestReconcilePool(t *testing.T) {
	active := map[string]bool{
		"glm-5.3-flash-free":           true,
		"deepseek-v4.1-flash-free":     true,
		"nemotron-3.5-lightning-free": true,
		"big-pickle":                   true,
		"mimo-v2.6-flash-free":         true,
		"beam-free":                    true,
		"nemotron-3.5-super-vl-free":   true, // model mới xuất hiện ở upstream
		"jev-1.13-free":                true, // luôn loại trừ
		"flint-40b-free":               true, // luôn loại trừ
		"space-bunny-free":             true,
		"longcat-2.5-preview-free":     true,
	}

	t.Run("AUTO_MODELS rong => pool mo, them model free moi cua upstream", func(t *testing.T) {
		got := reconcilePool(defaultSolidModels, active, "")
		for _, want := range []string{"glm-5.3-flash-free", "big-pickle", "nemotron-3.5-super-vl-free"} {
			if !contains(got, want) {
				t.Fatalf("pool thieu %q: %v", want, got)
			}
		}
		if contains(got, "jev-1.13-free") || contains(got, "flint-40b-free") {
			t.Fatalf("pool phai loai jev-1.13-free va flint-40b-free: %v", got)
		}
	})

	t.Run("AUTO_MODELS co gia tri => pool DONG, khong them model khac", func(t *testing.T) {
		current := []string{"glm-5.3-flash-free", "deepseek-v4.1-flash-free"}
		got := reconcilePool(current, active, "glm-5.3-flash,deepseek-v4.1-flash")
		if len(got) != 2 {
			t.Fatalf("pool phai dung 2 model da chi dinh, got %v", got)
		}
		for _, want := range []string{"glm-5.3-flash-free", "deepseek-v4.1-flash-free"} {
			if !contains(got, want) {
				t.Fatalf("pool thieu %q: %v", want, got)
			}
		}
		if contains(got, "nemotron-3.5-super-vl-free") || contains(got, "beam-free") {
			t.Fatalf("pool bi ghep them model ngoai danh sach: %v", got)
		}
	})

	t.Run("model trong AUTO_MODELS nhung upstream da chet => loai khoi pool", func(t *testing.T) {
		got := reconcilePool([]string{"glm-5.3-flash-free", "dead-model-free"}, active, "glm-5.3-flash")
		if len(got) != 1 || got[0] != "glm-5.3-flash-free" {
			t.Fatalf("phai loai model da chet, got %v", got)
		}
	})
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

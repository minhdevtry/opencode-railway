package main

import (
	"net/http"
	"testing"
	"time"
)

// Request quá dài cho MỌI model trong pool: gateway phải trả 400 ngay (client cần biết để chia nhỏ văn bản), không
// xếp hàng tới MAX_WAIT (30 phút trong prod) rồi mới trả 429.
func TestContextTooLongForWholePoolFailsFast(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"This model's maximum context length is 200000 tokens","type":"invalid_request_error"}}`))
	}, false)

	done := make(chan int, 1)
	go func() {
		resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"x"}]}`)
		done <- resp.StatusCode
	}()

	select {
	case code := <-done:
		if code != http.StatusBadRequest {
			t.Fatalf("vượt context mọi model phải trả 400, got %d", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("phải trả lỗi nhanh, không xếp hàng chờ")
	}
}

// Tham số của client bị upstream từ chối (400) thì phải trả 400, không phải 502 "No upstream attempt made".
func TestClientParamErrorReturns400(t *testing.T) {
	gw, _ := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"Unsupported value: 'foo' does not exist with 'temperature'","type":"invalid_request_error"}}`))
	}, false)

	done := make(chan int, 1)
	go func() {
		resp, _ := post(t, gw.URL+"/v1/chat/completions", `{"model":"auto","messages":[{"role":"user","content":"x"}],"temperature":"foo"}`)
		done <- resp.StatusCode
	}()

	select {
	case code := <-done:
		if code != http.StatusBadRequest {
			t.Fatalf("lỗi tham số của client phải trả 400, got %d", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("phải trả lỗi nhanh, không xếp hàng chờ")
	}
}

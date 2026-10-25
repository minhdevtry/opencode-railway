package main

import "testing"

func TestUnsupportedParamTruncatedBody(t *testing.T) {
	body := []byte(`{"error":{"param":"reasoning","type":"invalid_request_error","message":"Upstream request failed: [unsupported_parameter] Unrecognized request argument supplied: reasoning. Please check the documentation for the full list of`)
	if got := unsupportedParam(body); got != "reasoning" {
		t.Fatalf("got %q", got)
	}
	if got := unsupportedParam([]byte(`{"error":{"param":"reasoning","type":"invalid_request_error","message":"Upstream request failed: [unknown_parameter] invalid request body: json: unknown field \"reasoning\""}}`)); got != "reasoning" {
		t.Fatalf("unknown_parameter: got %q", got)
	}
	if got := unsupportedParam([]byte(`{"error":{"param":"model","message":"model not found"}}`)); got != "" {
		t.Fatalf("lỗi khác không được coi là tham số không hỗ trợ, got %q", got)
	}
}

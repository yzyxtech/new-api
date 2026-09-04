package codex

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relaykit/dto"

	"github.com/gin-gonic/gin"
)

// ptrTo 返回字符串指针(测试构造用)。
func ptrTo(s string) *string {
	return &s
}

// newTestBridgeWriter 构造 writer 并返回 recorder,方便断言 wire 字节与提交状态。
func newTestBridgeWriter(t *testing.T) (*bridgeResponseWriter, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	return newBridgeResponseWriter(c), rec
}

// sampleClaudeResponse 构造一个非空 ClaudeResponse,使 SSE/JSON 帧 wire format 可判定。
func sampleClaudeResponse() dto.ClaudeResponse {
	return dto.ClaudeResponse{
		Type: "message_start",
		Message: &dto.ClaudeMediaMessage{
			Id:   "msg_01",
			Type: "message",
			Role: "assistant",
			Content: []dto.ClaudeMediaMessage{
				{Type: "text", Text: ptrTo("hello\nworld")},
			},
		},
	}
}

// TestBridgeResponseWriter_WriteSSEFrameFormat:断言 SSE 帧 wire format。
func TestBridgeResponseWriter_WriteSSEFrameFormat(t *testing.T) {
	w, rec := newTestBridgeWriter(t)
	resp := sampleClaudeResponse()

	if err := w.writeSSEFrame(resp); err != nil {
		t.Fatalf("writeSSEFrame returned error: %v", err)
	}

	// frame = event: <type>\ndata: <单行 compact json>\n\n。
	data, err := common.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal resp failed: %v", err)
	}
	wantFrame := "event: message_start\ndata: " + string(data) + "\n\n"
	if rec.Body.String() != wantFrame {
		t.Fatalf("SSE frame mismatch.\n got: %q\nwant: %q", rec.Body.String(), wantFrame)
	}
}

// TestBridgeResponseWriter_WriteJSONFormat:断言无 SSE framing + application/json + status 暂存。
func TestBridgeResponseWriter_WriteJSONFormat(t *testing.T) {
	w, rec := newTestBridgeWriter(t)
	resp := sampleClaudeResponse()

	if err := w.writeJSON(200, resp); err != nil {
		t.Fatalf("writeJSON returned error: %v", err)
	}

	data, err := common.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal resp failed: %v", err)
	}
	// 无 SSE framing:body 为单个 compact JSON 文档,不含 "event: " 前缀。
	got := rec.Body.String()
	if got != string(data) {
		t.Fatalf("writeJSON body mismatch.\n got: %q\nwant: %q", got, string(data))
	}
	if strings.Contains(got, "event:") {
		t.Fatalf("writeJSON must not contain SSE framing, got: %q", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !w.committed() {
		t.Fatalf("expected committed after write")
	}
	// 产物必须是合法 JSON 文档。
	if !json.Valid(data) {
		t.Fatalf("written bytes are not valid JSON: %q", got)
	}
}

// TestBridgeResponseWriter_CommittedDelegates:断言 committed() 直接委托 Written()。
func TestBridgeResponseWriter_CommittedDelegates(t *testing.T) {
	w, _ := newTestBridgeWriter(t)

	if w.committed() {
		t.Fatalf("expected not committed before any write")
	}
	resp := sampleClaudeResponse()
	if err := w.writeJSON(200, resp); err != nil {
		t.Fatalf("writeJSON returned error: %v", err)
	}
	if !w.committed() {
		t.Fatalf("expected committed after write")
	}
}

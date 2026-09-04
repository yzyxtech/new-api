package codex

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/types"

	"github.com/gin-gonic/gin"
)

func init() {
	// StreamScannerHandler 用 constant.StreamingTimeout 建 Ticker;0 会 panic。
	if constant.StreamingTimeout == 0 {
		constant.StreamingTimeout = 30
	}
}

// bridgeStreamTestHarness 构造 handleBridgeStream 所需的最小会话上下文:
// recorder 作为 gin writer(断言写入字节),info 携带上游模型与估算 prompt。
type bridgeStreamTestHarness struct {
	c       *gin.Context
	info    *relaycommon.RelayInfo
	rec     *httptest.ResponseRecorder
	session *bridgeResponseSession
}

func newBridgeStreamTestHarness(t *testing.T, model string, estimatePrompt int) *bridgeStreamTestHarness {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: model}}
	info.SetEstimatePromptTokens(estimatePrompt)

	reqSession := &bridgeRequestSession{
		options: bridgeRequestOptions{
			downstreamStream: true,
			upstreamStream:   true,
			upstreamAccept:   "text/event-stream",
		},
		mapping: codexToolNameMapping{},
	}
	session, apiErr := newBridgeResponseSession(c, info, reqSession)
	if apiErr != nil {
		t.Fatalf("newBridgeResponseSession failed: %v", apiErr)
	}
	return &bridgeStreamTestHarness{c: c, info: info, rec: rec, session: session}
}

// newBridgeAggregateTestHarness 构造 handleBridgeAggregate 所需的最小会话上下文:
// downstreamStream=false 的聚合路径。
type bridgeAggregateTestHarness struct {
	c       *gin.Context
	info    *relaycommon.RelayInfo
	rec     *httptest.ResponseRecorder
	session *bridgeResponseSession
}

func newBridgeAggregateTestHarness(t *testing.T, model string, estimatePrompt int) *bridgeAggregateTestHarness {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: model}}
	info.RelayFormat = types.RelayFormatClaude
	info.SetEstimatePromptTokens(estimatePrompt)

	reqSession := &bridgeRequestSession{
		options: bridgeRequestOptions{
			downstreamStream: false,
			upstreamStream:   true,
			upstreamAccept:   "text/event-stream",
		},
		mapping: codexToolNameMapping{},
	}
	session, apiErr := newBridgeResponseSession(c, info, reqSession)
	if apiErr != nil {
		t.Fatalf("newBridgeResponseSession failed: %v", apiErr)
	}
	return &bridgeAggregateTestHarness{c: c, info: info, rec: rec, session: session}
}

// TestHandleBridgeAggregate_CompletedWithUsage:mock 发 completed(含 usage)→ 写出单个
// Claude JSON(非 SSE)+ (usage, nil),usage 取终态权威值。
func TestHandleBridgeAggregate_CompletedWithUsage(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna","usage":{"input_tokens":10,"output_tokens":20,"total_tokens":30}}}`,
	)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr != nil {
		t.Fatalf("handleBridgeAggregate unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}
	if usage.PromptTokens != 10 || usage.CompletionTokens != 20 {
		t.Fatalf("usage mismatch: prompt=%d completion=%d, want 10/20", usage.PromptTokens, usage.CompletionTokens)
	}
	body := h.rec.Body.String()
	if body == "" {
		t.Fatal("expected single JSON body to be written")
	}
	// 聚合态:单 JSON(无 SSE framing)。
	if strings.Contains(body, "event: ") {
		t.Fatalf("aggregate must not write SSE frames, body=%q", body)
	}
	if !strings.HasPrefix(strings.TrimSpace(body), "{") {
		t.Fatalf("expected a single JSON object, body=%q", body)
	}
	if ct := h.rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if !strings.Contains(body, "Hello") {
		t.Fatalf("expected accumulated output text in JSON, body=%q", body)
	}
}

// TestHandleBridgeAggregate_CompletedMissingUsage:mock 发 completed(缺 usage)→ 单 JSON
// 写出 + 按统一估算规则回填 usage(UsageSource=estimated)。
func TestHandleBridgeAggregate_CompletedMissingUsage(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello world","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna"}}`,
	)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr != nil {
		t.Fatalf("handleBridgeAggregate unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil estimate")
	}
	if usage.CompletionTokens == 0 {
		t.Fatalf("expected estimated completion tokens > 0, got %d", usage.CompletionTokens)
	}
	if usage.UsageSource != "estimated" {
		t.Fatalf("UsageSource = %q, want estimated", usage.UsageSource)
	}
	body := h.rec.Body.String()
	if body == "" {
		t.Fatal("expected single JSON body to be written")
	}
	if !strings.Contains(body, "Hello world") {
		t.Fatalf("expected accumulated output text in JSON, body=%q", body)
	}
}

// TestHandleBridgeAggregate_Failed:mock 发 response.failed → (nil, err),且不写任何下游字节。
func TestHandleBridgeAggregate_Failed(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.failed","response":{"error":{"code":"x","message":"no quota"}}}`,
	)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr == nil {
		t.Fatal("expected response.failed error, got nil")
	}
	if usage != nil {
		t.Fatalf("pre-commit failure should return nil usage, got %+v", usage)
	}
	if body := h.rec.Body.String(); body != "" {
		t.Fatalf("pre-commit failure must not write any downstream bytes, got %q", body)
	}
	// 规范化:response.failed 仅 code/message 无 type → t 回退 api_error。
	if got := apiErr.ToClaudeError().Type; got != "api_error" {
		t.Fatalf("error.type = %q, want api_error", got)
	}
}

// TestHandleBridgeAggregate_EOFBeforeTerminal:mock 终态前流自然结束(clean EOF)→ (nil, err),
// 且不写任何下游字节。
func TestHandleBridgeAggregate_EOFBeforeTerminal(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
	)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr == nil {
		t.Fatal("expected clean EOF before terminal error, got nil")
	}
	if usage != nil {
		t.Fatalf("pre-commit EOF should return nil usage, got %+v", usage)
	}
	if body := h.rec.Body.String(); body != "" {
		t.Fatalf("EOF before terminal must not write any downstream bytes, got %q", body)
	}
}

// TestHandleBridgeAggregate_AuthZeroCompletionNotEstimated:mock 发 completed(含权威 usage
// 但 output_tokens=0)→ 按统一估算规则**不**覆盖权威零 completion,返回 (Prompt=10,
// Completion=0),UsageSource 非 estimated。
func TestHandleBridgeAggregate_AuthZeroCompletionNotEstimated(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna","usage":{"input_tokens":10,"output_tokens":0,"total_tokens":10}}}`,
	)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr != nil {
		t.Fatalf("handleBridgeAggregate unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}
	if usage.PromptTokens != 10 || usage.CompletionTokens != 0 {
		t.Fatalf("usage mismatch: prompt=%d completion=%d, want 10/0 (authoritative zero completion must not be overwritten)", usage.PromptTokens, usage.CompletionTokens)
	}
	if usage.UsageSource == "estimated" {
		t.Fatalf("usage with authoritative zero completion must not be marked estimated, got UsageSource=%q", usage.UsageSource)
	}
	if body := h.rec.Body.String(); body == "" {
		t.Fatal("expected single JSON body to be written")
	}
}

// TestHandleBridgeAggregate_AuthZeroPromptFilled:mock 发 completed(含权威 usage 但
// input_tokens=0)→ 按统一规则 PromptTokens 缺省即经估算回填(Prompt=estimate,Completion 权威保留)。
func TestHandleBridgeAggregate_AuthZeroPromptFilled(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna","usage":{"input_tokens":0,"output_tokens":20,"total_tokens":20}}}`,
	)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr != nil {
		t.Fatalf("handleBridgeAggregate unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}
	if usage.PromptTokens != 15 {
		t.Fatalf("PromptTokens = %d, want estimate 15 (default prompt must be filled)", usage.PromptTokens)
	}
	if usage.CompletionTokens != 20 {
		t.Fatalf("CompletionTokens = %d, want authoritative 20", usage.CompletionTokens)
	}
}

// panicReadingCloser 模拟上游读取出错:首次 Read 即 panic,用于覆盖 scanner 独立 goroutine
// 内 pre-commit panic 的 recover 与裁决。
type panicReadingCloser struct{}

func (panicReadingCloser) Read([]byte) (int, error) { panic("scanner boom") }
func (panicReadingCloser) Close() error             { return nil }

// TestHandleBridgeAggregate_ScannerGoroutinePanic:scanner goroutine 内 panic(读上游 Body
// 即崩)→ recover 回传主循环统一裁决 → 聚合态 pre-commit 返回 (nil, err),不输出任何下游
// 字节,且 panic 不外泄(进程不崩)。
func TestHandleBridgeAggregate_ScannerGoroutinePanic(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       panicReadingCloser{},
	}

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr == nil {
		t.Fatal("expected scanner goroutine panic to produce an error, got nil")
	}
	if usage != nil {
		t.Fatalf("pre-commit scan panic should return nil usage, got %+v", usage)
	}
	if body := h.rec.Body.String(); body != "" {
		t.Fatalf("pre-commit scan panic must not write any downstream bytes, got %q", body)
	}
	if got := apiErr.ToClaudeError().Type; got == "" {
		t.Fatal("panic error must carry a Claude error type")
	}
}

// TestHandleBridgeAggregate_ParseErrorSinglePrefix:非 JSON data 行触发聚合解析失败 →
// (nil, err),错误 message 恰为 "responses stream read error: <err>"(单次前缀,不重复)。
func TestHandleBridgeAggregate_ParseErrorSinglePrefix(t *testing.T) {
	h := newBridgeAggregateTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(`this is not json`)

	usage, apiErr := handleBridgeAggregate(h.session, resp)
	if apiErr == nil {
		t.Fatal("expected parse error, got nil")
	}
	if usage != nil {
		t.Fatalf("pre-commit parse failure should return nil usage, got %+v", usage)
	}
	if body := h.rec.Body.String(); body != "" {
		t.Fatalf("pre-commit parse failure must not write any downstream bytes, got %q", body)
	}
	msg := apiErr.ToClaudeError().Message
	if !strings.HasPrefix(msg, "responses stream read error: ") {
		t.Fatalf("parse error message = %q, want prefix \"responses stream read error: \"", msg)
	}
	if strings.Count(msg, "responses stream read error:") != 1 {
		t.Fatalf("parse error message must add the prefix exactly once, got %q", msg)
	}
}

func newMockUpstreamSSE(events ...string) *http.Response {
	var sb strings.Builder
	for _, e := range events {
		sb.WriteString("data: " + e + "\n\n")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sb.String())),
	}
}

// TestHandleBridgeStream_CompletedWithUsage:mock 发 completed(含 usage)→ (usage, nil),
// message_delta 与 message_stop 已写出。
func TestHandleBridgeStream_CompletedWithUsage(t *testing.T) {
	h := newBridgeStreamTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna","usage":{"input_tokens":10,"output_tokens":20,"total_tokens":30}}}`,
	)

	usage, apiErr := handleBridgeStream(h.session, resp)
	if apiErr != nil {
		t.Fatalf("handleBridgeStream unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}
	if usage.CompletionTokens != 20 || usage.PromptTokens != 10 {
		t.Fatalf("usage mismatch: prompt=%d completion=%d, want 10/20", usage.PromptTokens, usage.CompletionTokens)
	}
	body := h.rec.Body.String()
	if !strings.Contains(body, "event: message_delta") {
		t.Fatalf("message_delta not written, body=%q", body)
	}
	if !strings.Contains(body, "event: message_stop") {
		t.Fatalf("message_stop not written, body=%q", body)
	}
}

// TestHandleBridgeStream_CompletedMissingUsage:mock 发 completed(缺 usage)→ 估算 usage 回填。
func TestHandleBridgeStream_CompletedMissingUsage(t *testing.T) {
	h := newBridgeStreamTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello world","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna"}}`,
	)

	usage, apiErr := handleBridgeStream(h.session, resp)
	if apiErr != nil {
		t.Fatalf("handleBridgeStream unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}
	if usage.CompletionTokens == 0 {
		t.Fatalf("expected estimated completion tokens > 0, got %d", usage.CompletionTokens)
	}
	if usage.UsageSource != "estimated" {
		t.Fatalf("UsageSource = %q, want estimated", usage.UsageSource)
	}
}

// failWriteResponseWriter 模拟底层写出失败:每次 Write 恒返回 (0, error) 并计数调用次数。
// 经 gin.CreateTestContext 包裹(gin 的 responseWriter.reset(w))后,
// 首次 Write 内 WriteHeaderNow 使 gin 侧 Written()==true,与生产「写失败后已提交」语义一致。
type failWriteResponseWriter struct {
	writeCalls int
}

func (w *failWriteResponseWriter) Header() http.Header { return http.Header{} }
func (w *failWriteResponseWriter) WriteHeader(int)     {}
func (w *failWriteResponseWriter) Write(p []byte) (int, error) {
	w.writeCalls++
	return 0, errors.New("simulated write failure")
}

// newFailingWriterSession 用写失败 writer 构造 handleBridgeStream 会话。
func newFailingWriterSession(t *testing.T) (*bridgeStreamTestHarness, *failWriteResponseWriter) {
	t.Helper()
	fw := &failWriteResponseWriter{}
	c, _ := gin.CreateTestContext(fw)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{UpstreamModelName: "codex-luna"}}
	info.SetEstimatePromptTokens(15)

	reqSession := &bridgeRequestSession{
		options: bridgeRequestOptions{downstreamStream: true},
		mapping: codexToolNameMapping{},
	}
	session, apiErr := newBridgeResponseSession(c, info, reqSession)
	if apiErr != nil {
		t.Fatalf("newBridgeResponseSession failed: %v", apiErr)
	}
	return &bridgeStreamTestHarness{c: c, info: info, session: session}, fw
}

// TestHandleBridgeStream_WriteFailureSilentClose:底层写出失败(含首帧)按 A-21 静默关流——
// 不尝试写 error 帧(写出仅触发一次 Write 调用),返回 (usage估算, nil)。
func TestHandleBridgeStream_WriteFailureSilentClose(t *testing.T) {
	h, fw := newFailingWriterSession(t)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna"}}`,
	)

	usage, apiErr := handleBridgeStream(h.session, resp)
	if apiErr != nil {
		t.Fatalf("write failure should return (usage, nil), got err: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil estimate")
	}
	// 静默关流:写出失败后不得再尝试错误帧(若实现会尝试 error 帧,Write 会被二次调用)。
	if fw.writeCalls != 1 {
		t.Fatalf("write calls = %d, want 1 (must not retry error frame on write failure)", fw.writeCalls)
	}
}

// TestHandleBridgeStream_ResponseDoneNotTerminal:response.done 未获 design I-3 批准为成功
// 终态,若出现按未定义/非终态事件经 failStream 分流——未写下游字节时返回 (nil, err),
// 不得提前按 (usage, nil) 完成。
func TestHandleBridgeStream_ResponseDoneNotTerminal(t *testing.T) {
	h := newBridgeStreamTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.done","response":{"id":"resp_1","model":"codex-luna"}}`,
	)

	usage, apiErr := handleBridgeStream(h.session, resp)
	if apiErr == nil {
		t.Fatal("expected error for unapproved response.done event, got nil")
	}
	if usage != nil {
		t.Fatalf("expected nil usage, got %+v", usage)
	}
	if body := h.rec.Body.String(); body != "" {
		t.Fatalf("pre-commit response.done must not write downstream bytes, got %q", body)
	}
}

// TestHandleBridgeStream_PostCommitError:mock 发 message_start 后 error → 一次 SSE error +
// 关流 + (usage估算, nil)。
func TestHandleBridgeStream_PostCommitError(t *testing.T) {
	h := newBridgeStreamTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.failed","response":{"error":{"code":"quota","message":"mid stream"}}}`,
	)

	usage, apiErr := handleBridgeStream(h.session, resp)
	if apiErr != nil {
		t.Fatalf("post-commit error should return (usage, nil), got err: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil estimate")
	}
	body := h.rec.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Fatalf("expected one SSE error frame, body=%q", body)
	}
	if !strings.Contains(body, `"type":"api_error"`) {
		t.Fatalf("expected normalized api_error type in error frame, body=%q", body)
	}
}

// TestHandleBridgeStream_PreCommitError:mock 发 error(未写下游字节)→ (nil, err)。
func TestHandleBridgeStream_PreCommitError(t *testing.T) {
	h := newBridgeStreamTestHarness(t, "codex-luna", 15)
	resp := newMockUpstreamSSE(
		`{"type":"response.failed","response":{"error":{"code":"x","message":"no quota"}}}`,
	)

	usage, apiErr := handleBridgeStream(h.session, resp)
	if apiErr == nil {
		t.Fatal("expected pre-commit error, got nil")
	}
	if usage != nil {
		t.Fatalf("pre-commit error should return nil usage, got %+v", usage)
	}
	if body := h.rec.Body.String(); body != "" {
		t.Fatalf("pre-commit error must not write any downstream bytes, got %q", body)
	}
}

// TestHandleBridgeStream_ErrorNormalization:覆盖流内错误规范化——顶层 type=error 保留上游
// type;仅 code/message 无 type 的 response.failed 回退 api_error。
func TestHandleBridgeStream_ErrorNormalization(t *testing.T) {
	t.Run("TopLevelErrorPreservesType", func(t *testing.T) {
		h := newBridgeStreamTestHarness(t, "codex-luna", 15)
		resp := newMockUpstreamSSE(
			`{"type":"error","error":{"type":"rate_limit_exceeded","message":"quota"},"error_type":"invalid_request_error"}`,
		)
		_, apiErr := handleBridgeStream(h.session, resp)
		if apiErr == nil {
			t.Fatal("expected error, got nil")
		}
		if got := apiErr.ToClaudeError().Type; got != "rate_limit_exceeded" {
			t.Fatalf("error.type = %q, want preserved upstream type rate_limit_exceeded", got)
		}
		if got := apiErr.ToClaudeError().Message; got != "quota" {
			t.Fatalf("error.message = %q, want %q", got, "quota")
		}
	})

	t.Run("FailedWithoutTypeFallsBackToApiError", func(t *testing.T) {
		h := newBridgeStreamTestHarness(t, "codex-luna", 15)
		resp := newMockUpstreamSSE(
			`{"type":"response.failed","response":{"error":{"code":"some_code","message":"boom"}}}`,
		)
		_, apiErr := handleBridgeStream(h.session, resp)
		if apiErr == nil {
			t.Fatal("expected error, got nil")
		}
		if got := apiErr.ToClaudeError().Type; got != "api_error" {
			t.Fatalf("error.type = %q, want fallback api_error", got)
		}
		if got := apiErr.ToClaudeError().Message; got != "boom" {
			t.Fatalf("error.message = %q, want %q", got, "boom")
		}
	})
}

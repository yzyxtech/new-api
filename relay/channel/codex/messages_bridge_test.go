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
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"

	"github.com/gin-gonic/gin"
)

// newBridgeOrchestratorEnv 构造驱动 BridgeClaudeMessages 的最小端到端环境:
// httptest mock 上游 + 指向该上游的 RelayInfo(codex OAuth key 满足 SetupRequestHeader)。
// 经 channel.DoApiRequest 真实发往 server.URL/backend-api/codex/responses。
// 返回 recorder 供断言下游写出字节。
func newBridgeOrchestratorEnv(t *testing.T, downstreamStream bool, upstreamHandler http.HandlerFunc) (*gin.Context, *relaycommon.RelayInfo, *Adaptor, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(upstreamHandler)
	t.Cleanup(server.Close)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("Content-Type", "application/json")

	info := &relaycommon.RelayInfo{
		RelayFormat:     types.RelayFormatClaude,
		RelayMode:       relayconstant.RelayModeResponses,
		IsStream:        downstreamStream,
		OriginModelName: "gpt-5.6-luna",
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       constant.ChannelTypeCodex,
			ChannelBaseUrl:    server.URL,
			UpstreamModelName: "codex-luna",
			ApiKey:            `{"access_token":"tok-abc","account_id":"acct-1"}`,
		},
	}
	info.SetEstimatePromptTokens(15)
	return c, info, &Adaptor{}, rec
}

// newTestClaudeRequest 构造最小合法 Claude 请求。
func newTestClaudeRequest(stream bool) *dto.ClaudeRequest {
	s := stream
	return &dto.ClaudeRequest{
		Model:    "gpt-5.6-luna",
		Messages: []dto.ClaudeMessage{{Role: "user", Content: "hello"}},
		Stream:   &s,
	}
}

// newUpstreamSSEHandler 返回直接写出 text/event-stream SSE 数据的 mock 上游。
func newUpstreamSSEHandler(events ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		for _, e := range events {
			_, _ = io.WriteString(w, "data: "+e+"\n\n")
		}
	}
}

// TestBridgeClaudeMessages_StreamSuccess:mock 发 SSE 成功流 → 断言 (usage, nil) 且
// 下游流式响应已写出(message_delta/message_stop),usage 取终态权威值。
func TestBridgeClaudeMessages_StreamSuccess(t *testing.T) {
	upstream := newUpstreamSSEHandler(
		`{"type":"response.created","response":{"id":"resp_1","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"Hello","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_1","model":"codex-luna","usage":{"input_tokens":10,"output_tokens":20,"total_tokens":30}}}`,
	)
	c, info, adaptor, rec := newBridgeOrchestratorEnv(t, true, upstream)

	usage, apiErr := BridgeClaudeMessages(c, info, adaptor, newTestClaudeRequest(true))
	if apiErr != nil {
		t.Fatalf("BridgeClaudeMessages unexpected error: %v", apiErr)
	}
	// 两态契约:(usage, nil) 且 usage 为终态权威值。
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}
	if usage.PromptTokens != 10 || usage.CompletionTokens != 20 {
		t.Fatalf("usage mismatch: prompt=%d completion=%d, want 10/20", usage.PromptTokens, usage.CompletionTokens)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: message_delta") {
		t.Fatalf("message_delta not written, body=%q", body)
	}
	if !strings.Contains(body, "event: message_stop") {
		t.Fatalf("message_stop not written, body=%q", body)
	}
	if !strings.Contains(body, "Hello") {
		t.Fatalf("output text not written, body=%q", body)
	}
}

// TestBridgeClaudeMessages_UpstreamNon2xx:mock 发 401 → 断言 (nil, err) 且经
// RelayErrorHandler(错误来自上游非 2xx,status 取自上游/映射后),不写任何下游字节。
func TestBridgeClaudeMessages_UpstreamNon2xx(t *testing.T) {
	upstream := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"unauthorized"}}`)
	}
	c, info, adaptor, rec := newBridgeOrchestratorEnv(t, true, upstream)

	usage, apiErr := BridgeClaudeMessages(c, info, adaptor, newTestClaudeRequest(true))
	if apiErr == nil {
		t.Fatal("expected non-2xx upstream error, got nil")
	}
	// pre-commit 失败:usage 必须为 nil。
	if usage != nil {
		t.Fatalf("pre-commit failure should return nil usage, got %+v", usage)
	}
	// 落 A-2 RelayErrorHandler:非 2xx 上游(未触下游)不得向 recorder 写流式字节。
	if rec.Body.String() != "" {
		t.Fatalf("pre-commit non-2xx must not write downstream bytes, got %q", rec.Body.String())
	}
	if !errors.As(apiErr, new(*types.NewAPIError)) {
		t.Fatalf("error is not *types.NewAPIError: %T", apiErr)
	}
}

// TestBridgeClaudeMessages_InvalidResponseA23:上游返回无效响应 → 断言 A-23。
// doBridgeRequest 经 channel.DoApiRequest 返回具体 *http.Response,Go http 栈无法在真实
// HTTP 往返中制造 nil/typed-nil 响应体(A-23②/③ 由具体返回类型结构恒通过),故本用例直接
// 驱动 orchestrator 使用的权威 A-23 校验函数 validateBridgeUpstreamResponse,覆盖
// resp==nil(A-23①)、typed-nil(A-23③)、resp.Body==nil(A-23④)三等价条件,断言落
// ErrorCodeBadResponse 且 err 确定性非 nil;并经端到端成功用例确认该守卫接入 orchestrator
// 后不误伤合法响应(见 TestBridgeClaudeMessages_StreamSuccess)。
func TestBridgeClaudeMessages_InvalidResponseA23(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("NilResponse", func(t *testing.T) {
		apiErr := validateBridgeUpstreamResponse(nil)
		assertBridgeA23(t, apiErr)
	})
	t.Run("TypedNilResponse", func(t *testing.T) {
		var resp *http.Response = nil
		apiErr := validateBridgeUpstreamResponse(resp)
		assertBridgeA23(t, apiErr)
	})
	t.Run("NilBody", func(t *testing.T) {
		apiErr := validateBridgeUpstreamResponse(&http.Response{StatusCode: http.StatusOK, Body: nil})
		assertBridgeA23(t, apiErr)
	})
	// 非 nil Body 的合法响应不落 A-23。
	t.Run("ValidBodyNotA23", func(t *testing.T) {
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(""))}
		if apiErr := validateBridgeUpstreamResponse(resp); apiErr != nil {
			t.Fatalf("valid response should pass A-23, got err: %v", apiErr)
		}
	})
}

// assertBridgeA23 断言 A-23 错误:(nil-usage 语义由调用方承担)返回错误为非 nil 的
// *types.NewAPIError 且内部 ErrorCode=ErrorCodeBadResponse。
func assertBridgeA23(t *testing.T, apiErr *types.NewAPIError) {
	t.Helper()
	if apiErr == nil {
		t.Fatal("expected A-23 error, got nil")
	}
	if apiErr.GetErrorCode() != types.ErrorCodeBadResponse {
		t.Fatalf("ErrorCode = %q, want %q", apiErr.GetErrorCode(), types.ErrorCodeBadResponse)
	}
	if apiErr.GetErrorType() != types.ErrorTypeNewAPIError {
		t.Fatalf("external type = %q, want new_api_error", apiErr.GetErrorType())
	}
	if apiErr.Error() == "" {
		t.Fatal("A-23 error must carry a deterministic non-nil message")
	}
}

// TestBridgeClaudeMessages_ContentTypeNotSSEA24:mock 200 但 Content-Type 非 SSE(上游契约
// 违背)→ 断言 A-24:(nil, err) 且 ErrorCodeBadResponse,message 保留上游 error.message。
func TestBridgeClaudeMessages_ContentTypeNotSSEA24(t *testing.T) {
	upstream := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"error":{"message":"expected an event stream"}}`)
		return
	}
	c, info, adaptor, rec := newBridgeOrchestratorEnv(t, true, upstream)

	usage, apiErr := BridgeClaudeMessages(c, info, adaptor, newTestClaudeRequest(true))
	if apiErr == nil {
		t.Fatal("expected A-24 error, got nil")
	}
	if usage != nil {
		t.Fatalf("pre-commit A-24 should return nil usage, got %+v", usage)
	}
	if apiErr.GetErrorCode() != types.ErrorCodeBadResponse {
		t.Fatalf("ErrorCode = %q, want %q", apiErr.GetErrorCode(), types.ErrorCodeBadResponse)
	}
	// pre-commit:未触下游,不得写字节。
	if body := rec.Body.String(); body != "" {
		t.Fatalf("pre-commit A-24 must not write downstream bytes, got %q", body)
	}
	// A-24 从上游 body 提取的 error.message 应保留。
	if !strings.Contains(apiErr.Error(), "expected an event stream") {
		t.Fatalf("A-24 message should retain upstream error message, got %q", apiErr.Error())
	}
}

package codex

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/tidwall/gjson"

	"github.com/gin-gonic/gin"
)

// mockBridgeBilling 是 BillingSettler 接口的最小替身,用于验证 bridge 浅副本
// 对 Billing 接口字段只读、不重写。
type mockBridgeBilling struct{}

func (m *mockBridgeBilling) Settle(int) error         { return nil }
func (m *mockBridgeBilling) Refund(*gin.Context)      {}
func (m *mockBridgeBilling) NeedsRefund() bool        { return false }
func (m *mockBridgeBilling) GetPreConsumedQuota() int { return 0 }
func (m *mockBridgeBilling) Reserve(int) error        { return nil }

// TestBridgeRequestSession_ShallowCopyOnlyThreeScalars:构造原始 RelayInfo
// (含 RequestHeaders/RealtimeTools/Billing 等共享引用字段),创建 session 副本并
// 覆盖三标量 → 断言原始 RelayInfo 三标量与所有共享引用字段前后不变。
func TestBridgeRequestSession_ShallowCopyOnlyThreeScalars(t *testing.T) {
	gin.SetMode(gin.TestMode)

	orig := &relaycommon.RelayInfo{
		IsStream:       true,
		RelayMode:      123456, // 非 Responses 哨兵,区别于副本覆盖值
		RequestURLPath: "/v1/messages",
		RequestHeaders: map[string]string{"X-Sentinel": "keep"},
		RealtimeTools: []dto.RealTimeTool{
			{Description: "realtime-tool"},
		},
		ReasoningConversion: &dto.ReasoningConversionState{},
		Billing:             &mockBridgeBilling{},
	}

	// 记录原始共享引用字段,用于前后比对(构造 session 不得触碰它们)
	preHeaders := orig.RequestHeaders
	preRealtimeTools := orig.RealtimeTools
	preReasoningConversion := orig.ReasoningConversion
	preBilling := orig.Billing

	s := newBridgeRequestSession(orig, false, codexToolNameMapping{})

	// ① 原始三标量不被改写
	if orig.IsStream != true {
		t.Fatalf("original IsStream rewritten: %v", orig.IsStream)
	}
	if orig.RelayMode != 123456 {
		t.Fatalf("original RelayMode rewritten: %v", orig.RelayMode)
	}
	if orig.RequestURLPath != "/v1/messages" {
		t.Fatalf("original RequestURLPath rewritten: %q", orig.RequestURLPath)
	}

	// ② 原始共享引用字段不被改写(构造 session 只做值头复制,不深写)
	if !reflect.DeepEqual(orig.RequestHeaders, preHeaders) {
		t.Fatalf("original RequestHeaders mutated: %v", orig.RequestHeaders)
	}
	if !reflect.DeepEqual(orig.RealtimeTools, preRealtimeTools) {
		t.Fatalf("original RealtimeTools mutated: %v", orig.RealtimeTools)
	}
	if orig.ReasoningConversion != preReasoningConversion {
		t.Fatalf("original ReasoningConversion pointer changed")
	}
	if orig.Billing != preBilling {
		t.Fatalf("original Billing pointer changed")
	}

	// ③ 副本与原始共享同一底层容器(证明是浅副本;只读纪律才保护原始不被污染)
	if reflect.ValueOf(s.upstreamInfo.RequestHeaders).Pointer() != reflect.ValueOf(orig.RequestHeaders).Pointer() {
		t.Fatalf("upstreamInfo.RequestHeaders not shared with original (deep copy?)")
	}
	if reflect.ValueOf(s.upstreamInfo.RealtimeTools).Pointer() != reflect.ValueOf(orig.RealtimeTools).Pointer() {
		t.Fatalf("upstreamInfo.RealtimeTools not shared with original (deep copy?)")
	}
	if s.upstreamInfo.ReasoningConversion != orig.ReasoningConversion {
		t.Fatalf("upstreamInfo.ReasoningConversion not same pointer as original")
	}
	if s.upstreamInfo.Billing != orig.Billing {
		t.Fatalf("upstreamInfo.Billing not same interface value as original")
	}
}

// TestBridgeRequestSession_UpstreamInfoIsStreamFalse:断言副本 IsStream=false、
// downstreamStream 透传、upstreamAccept 恒 "text/event-stream"。
func TestBridgeRequestSession_UpstreamInfoIsStreamFalse(t *testing.T) {
	orig := &relaycommon.RelayInfo{
		IsStream:       true,
		RelayMode:      123456,
		RequestURLPath: "/v1/messages",
	}

	s := newBridgeRequestSession(orig, true, codexToolNameMapping{})

	if s.upstreamInfo.IsStream {
		t.Fatalf("upstreamInfo.IsStream = true, want false (suppress downstream SSE side effects)")
	}
	if !s.options.upstreamStream {
		t.Fatalf("upstreamStream = false, want true")
	}
	if s.options.upstreamAccept != "text/event-stream" {
		t.Fatalf("upstreamAccept = %q, want %q", s.options.upstreamAccept, "text/event-stream")
	}
	if !s.options.downstreamStream {
		t.Fatalf("downstreamStream not passed through")
	}

	// 三标量覆盖只发生在副本,原始保持真实
	if orig.IsStream != true || orig.RelayMode != 123456 || orig.RequestURLPath != "/v1/messages" {
		t.Fatalf("original RelayInfo scalars were rewritten by copy")
	}
}

// TestBridgeAdaptorWrapper_SetupRequestHeaderSSEAccept:wrapper 覆盖 SetupRequestHeader
// → 断言上游 Accept 恒为 text/event-stream,其它 header(curl 鉴权 / content-type /
// account-id / Authorization)委托 concrete adaptor 生效。
func TestBridgeAdaptorWrapper_SetupRequestHeaderSSEAccept(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ctx.Request.Header.Set("Content-Type", "application/json")
	ctx.Request.Header.Set("Accept", "application/json")

	info := &relaycommon.RelayInfo{
		RelayMode: 123456,
		ChannelMeta: &relaycommon.ChannelMeta{
			ApiKey: `{"access_token":"tok-abc","account_id":"acct-1"}`,
		},
	}

	concrete := &Adaptor{}
	wrapper := &bridgeAdaptorWrapper{Adaptor: concrete}

	headers := &http.Header{}
	if err := wrapper.SetupRequestHeader(ctx, headers, info); err != nil {
		t.Fatalf("SetupRequestHeader failed: %v", err)
	}

	// 上游强制 SSE Accept(wrapper 专属行为)
	if got := headers.Get("Accept"); got != "text/event-stream" {
		t.Fatalf("upstream Accept = %q, want %q", got, "text/event-stream")
	}

	// 其它 header 委托 concrete adaptor 生效
	if got := headers.Get("Authorization"); got != "Bearer tok-abc" {
		t.Fatalf("Authorization = %q, want %q", got, "Bearer tok-abc")
	}
	if got := headers.Get("chatgpt-account-id"); got != "acct-1" {
		t.Fatalf("chatgpt-account-id = %q, want %q", got, "acct-1")
	}
	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/json")
	}
	if got := headers.Get("OpenAI-Beta"); got != "responses=experimental" {
		t.Fatalf("OpenAI-Beta = %q, want %q", got, "responses=experimental")
	}
}

func ptrUint(v uint) *uint        { return &v }
func ptrFloat(v float64) *float64 { return &v }

// TestPrepareBridgeRequest_ForceStreamAndInclude:产物 JSON 含 stream=true、
// include=["reasoning.encrypted_content"]、store=false、parallel_tool_calls=true、无 top_p。
func TestPrepareBridgeRequest_ForceStreamAndInclude(t *testing.T) {
	info := &relaycommon.RelayInfo{}
	mapping := &codexToolNameMapping{}
	req := dto.OpenAIResponsesRequest{
		Model: "gpt-5.6-luna",
		TopP:  ptrFloat(0.5),
	}

	body, err := prepareBridgeRequest(info, req, mapping)
	if err != nil {
		t.Fatalf("prepareBridgeRequest failed: %v", err)
	}

	if !gjson.GetBytes(body, "stream").Bool() {
		t.Fatalf("stream != true: %s", gjson.GetBytes(body, "stream").Raw)
	}
	inc := gjson.GetBytes(body, "include").Array()
	if len(inc) != 1 || inc[0].String() != "reasoning.encrypted_content" {
		t.Fatalf("include = %s, want [\"reasoning.encrypted_content\"]", gjson.GetBytes(body, "include").Raw)
	}
	if gjson.GetBytes(body, "store").Bool() {
		t.Fatalf("store != false: %s", gjson.GetBytes(body, "store").Raw)
	}
	if !gjson.GetBytes(body, "parallel_tool_calls").Bool() {
		t.Fatalf("parallel_tool_calls != true: %s", gjson.GetBytes(body, "parallel_tool_calls").Raw)
	}
	if gjson.GetBytes(body, "top_p").Exists() {
		t.Fatalf("top_p should be removed, got %s", gjson.GetBytes(body, "top_p").Raw)
	}
}

// TestPrepareBridgeRequest_DisabledFieldRemoval:构造含全部 disabled-field 的请求 →
// 断言产物 JSON 无这些键;service_tier 非 "priority" 删、为 "priority" 保留;
// input part 内嵌 prompt_cache_breakpoint 被逐点删除。
func TestPrepareBridgeRequest_DisabledFieldRemoval(t *testing.T) {
	info := &relaycommon.RelayInfo{}

	req := dto.OpenAIResponsesRequest{
		Model:                "gpt-5.6-luna",
		MaxOutputTokens:      ptrUint(100),
		Temperature:          ptrFloat(0.7),
		TopP:                 ptrFloat(0.9),
		Truncation:           json.RawMessage(`"auto"`),
		PromptCacheOptions:   json.RawMessage(`{"type":"ephemeral"}`),
		PromptCacheRetention: json.RawMessage(`{"minutes":5}`),
		User:                 json.RawMessage(`"u123"`),
		ContextManagement:    json.RawMessage(`{"type":"compact"}`),
		ServiceTier:          "default",
	}

	body, err := prepareBridgeRequest(info, req, &codexToolNameMapping{})
	if err != nil {
		t.Fatalf("prepareBridgeRequest failed: %v", err)
	}
	absent := []string{
		"max_output_tokens", "max_completion_tokens", "temperature", "top_p",
		"truncation", "prompt_cache_options", "prompt_cache_retention",
		"user", "context_management", "service_tier",
	}
	for _, key := range absent {
		if gjson.GetBytes(body, key).Exists() {
			t.Fatalf("disabled field %q still present: %s", key, gjson.GetBytes(body, key).Raw)
		}
	}

	// service_tier = "priority" 时保留。
	reqPriority := dto.OpenAIResponsesRequest{Model: "gpt-5.6-luna", ServiceTier: "priority"}
	bodyPriority, err := prepareBridgeRequest(info, reqPriority, &codexToolNameMapping{})
	if err != nil {
		t.Fatalf("prepareBridgeRequest(priority) failed: %v", err)
	}
	if !gjson.GetBytes(bodyPriority, "service_tier").Exists() || gjson.GetBytes(bodyPriority, "service_tier").String() != "priority" {
		t.Fatalf("service_tier=priority should be retained, got %s", gjson.GetBytes(bodyPriority, "service_tier").Raw)
	}

	// 嵌套 prompt_cache_breakpoint 逐 part 删除。
	input := json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi","prompt_cache_breakpoint":"auto"},{"type":"input_text","text":"x"}]}]`)
	reqNested := dto.OpenAIResponsesRequest{Model: "gpt-5.6-luna", Input: input}
	bodyNested, err := prepareBridgeRequest(info, reqNested, &codexToolNameMapping{})
	if err != nil {
		t.Fatalf("prepareBridgeRequest(nested) failed: %v", err)
	}
	if gjson.GetBytes(bodyNested, "input.0.content.0.prompt_cache_breakpoint").Exists() {
		t.Fatalf("nested prompt_cache_breakpoint not removed: %s", bodyNested)
	}
	if gjson.GetBytes(bodyNested, "input.0.content.1.prompt_cache_breakpoint").Exists() {
		t.Fatalf("nested prompt_cache_breakpoint leaked to part 1: %s", bodyNested)
	}
}

// TestPrepareBridgeRequest_ShrinkWritesMapping:工具名超 64 字节 → 产物 JSON 收缩且 mapping 写入。
func TestPrepareBridgeRequest_ShrinkWritesMapping(t *testing.T) {
	info := &relaycommon.RelayInfo{}
	longName := strings.Repeat("a", 100)
	tools := json.RawMessage(`[{"type":"function","name":` + strconv.Quote(longName) + `,"parameters":{"type":"object","properties":{}}}]`)
	req := dto.OpenAIResponsesRequest{Model: "gpt-5.6-luna", Tools: tools}
	mapping := &codexToolNameMapping{}

	body, err := prepareBridgeRequest(info, req, mapping)
	if err != nil {
		t.Fatalf("prepareBridgeRequest failed: %v", err)
	}
	short := gjson.GetBytes(body, "tools.0.name").String()
	if short == longName {
		t.Fatalf("tool name not shrunk")
	}
	if len([]byte(short)) > codexToolNameLimit {
		t.Fatalf("short name %d bytes exceeds %d", len([]byte(short)), codexToolNameLimit)
	}
	if mapping.nameByShort[short] != longName {
		t.Fatalf("mapping.nameByShort missing short->original; got %q", mapping.nameByShort[short])
	}
}

// TestPrepareBridgeRequest_SingleMarshal:返回完整 outbound JSON []byte,调用方只读持有,
// 是一次合法 JSON 文档(不重复 marshal、无半成品)。
func TestPrepareBridgeRequest_SingleMarshal(t *testing.T) {
	info := &relaycommon.RelayInfo{}
	req := dto.OpenAIResponsesRequest{Model: "gpt-5.6-luna"}

	body, err := prepareBridgeRequest(info, req, &codexToolNameMapping{})
	if err != nil {
		t.Fatalf("prepareBridgeRequest failed: %v", err)
	}
	if len(body) == 0 {
		t.Fatalf("empty outbound body")
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("outbound body is not a single valid JSON document: %v", err)
	}
	if _, ok := doc["stream"]; !ok {
		t.Fatalf("single-marshaled body missing stream")
	}
}

// TestPrepareBridgeRequest_MarshalFailureA27:非法 RawMessage 使唯一 marshal 失败 → 落 A-27③。
func TestPrepareBridgeRequest_MarshalFailureA27(t *testing.T) {
	info := &relaycommon.RelayInfo{}
	req := dto.OpenAIResponsesRequest{Model: "gpt-5.6-luna", Input: json.RawMessage(`{`)}

	_, err := prepareBridgeRequest(info, req, &codexToolNameMapping{})
	if err == nil {
		t.Fatalf("expected marshal failure")
	}
	var apiErr *types.NewAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is not *types.NewAPIError: %T", err)
	}
	if apiErr.GetErrorCode() != types.ErrorCodeConvertRequestFailed {
		t.Fatalf("ErrorCode = %q, want %q", apiErr.GetErrorCode(), types.ErrorCodeConvertRequestFailed)
	}
	if !types.IsSkipRetryError(apiErr) {
		t.Fatalf("expected skip retry for A-27 request construction failure")
	}
}

// TestPrepareBridgeRequest_OutboundBodyConstructFailureA27:outbound body 构造失败
// (param override 产物已非合法 JSON 文档)→ 落 A-27⑥ 独立分类,而非③ marshal /
// ④ disabled-field / ⑤ param override。⑥ 与③④同出口(ErrorCodeConvertRequestFailed +
// SkipRetry),但经独立构造路径,使该环节失败可单独定位。
func TestPrepareBridgeRequest_OutboundBodyConstructFailureA27(t *testing.T) {
	// 直接构造 ⑥ 的失败源:非法的 outbound 字节进入最终组装环节。
	bad, err := constructOutboundBody([]byte(`{"model":"gpt-5.6-luna",`))
	if err == nil {
		t.Fatalf("constructOutboundBody: expected failure on malformed outbound body, got %q", bad)
	}

	// 经 ⑥ 独立错误构造路径包裹,断言落入 A-27 请求构造失败分类。
	apiErr := newBridgeOutboundBodyConstructionError(err)
	if apiErr == nil {
		t.Fatalf("expected *types.NewAPIError")
	}
	if apiErr.GetErrorCode() != types.ErrorCodeConvertRequestFailed {
		t.Fatalf("ErrorCode = %q, want %q", apiErr.GetErrorCode(), types.ErrorCodeConvertRequestFailed)
	}
	if apiErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want 500", apiErr.StatusCode)
	}
	if apiErr.GetErrorType() != types.ErrorTypeNewAPIError {
		t.Fatalf("external type = %q, want new_api_error", apiErr.GetErrorType())
	}
	if !types.IsSkipRetryError(apiErr) {
		t.Fatalf("expected skip retry for A-27⑥ outbound body construction failure")
	}

	// 合法字节经 ⑥ 组装原样通过,不产生分类错误。
	ok, err := constructOutboundBody([]byte(`{"model":"gpt-5.6-luna","stream":true}`))
	if err != nil {
		t.Fatalf("constructOutboundBody: valid body rejected: %v", err)
	}
	if len(ok) == 0 {
		t.Fatalf("constructOutboundBody: empty output on valid body")
	}

	// 对比边界:③ marshal 失败用共同出口 newBridgeRequestConstructionError,
	// ⑥ 用独立出口 newBridgeOutboundBodyConstructionError——两出口分类相同但路径可区分。
	if newBridgeRequestConstructionError(err) == nil {
		t.Fatalf("③/④ helper must also produce an A-27 construction error")
	}
}

// TestDoBridgeRequest_TransportFailureA26:transport/setup 失败(无 response)→ 落 A-26 语义。
// 错误链无 *NewAPIError → new_api_error / 500 / 不主动 SkipRetry;已含 *NewAPIError → errors.As 原样保留。
func TestDoBridgeRequest_TransportFailureA26(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	// 无效 base URL 使 http.NewRequest 在触网前失败(普通 error,无 *NewAPIError)。
	orig := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{ChannelBaseUrl: "http://%"}}
	session := newBridgeRequestSession(orig, true, codexToolNameMapping{})

	resp, err := doBridgeRequest(ctx, session, &Adaptor{}, []byte(`{"model":"gpt-5.6-luna"}`))
	if resp != nil {
		t.Fatalf("expected nil response for transport failure")
	}
	if err == nil {
		t.Fatalf("expected transport failure error")
	}
	var apiErr *types.NewAPIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error is not *types.NewAPIError: %T", err)
	}
	if apiErr.GetErrorCode() != types.ErrorCodeDoRequestFailed {
		t.Fatalf("ErrorCode = %q, want do_request_failed", apiErr.GetErrorCode())
	}
	if apiErr.GetErrorType() != types.ErrorTypeNewAPIError {
		t.Fatalf("external type = %q, want new_api_error", apiErr.GetErrorType())
	}
	if apiErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want 500", apiErr.StatusCode)
	}
	if types.IsSkipRetryError(apiErr) {
		t.Fatalf("A-26② must not actively set skip retry")
	}

	// A-26①:错误链已含 *NewAPIError 时 errors.As 原样保留内层 code/type/status/SkipRetry。
	inner := types.NewError(errors.New("x"), types.ErrorCodeChannelHeaderOverrideInvalid, types.ErrOptionWithSkipRetry())
	wrapped := bridgeWrapDoRequestFailedError(inner)
	if wrapped.GetErrorCode() != types.ErrorCodeChannelHeaderOverrideInvalid {
		t.Fatalf("inner ErrorCode lost: %q", wrapped.GetErrorCode())
	}
	if wrapped.StatusCode != http.StatusInternalServerError {
		t.Fatalf("inner StatusCode lost: %d", wrapped.StatusCode)
	}
	if !types.IsSkipRetryError(wrapped) {
		t.Fatalf("inner SkipRetry lost")
	}
}

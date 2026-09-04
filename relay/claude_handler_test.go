package relay

import (
	"net/http"
	"testing"

	"github.com/QuantumNous/new-api/relay/channel/openai"
	"github.com/QuantumNous/new-api/relaykit/types"
)

// TestClaudeHelper_InvalidAdaptorTypeA29:构造 Codex 渠道类型(bridge 谓词命中)但既有
// channel.Adaptor 无法断言为 concrete *codex.Adaptor(此处用非 Codex 的 openai adaptor
// 宽接口,模拟注册/配置不变量破坏)→ 断言落 A-29:ErrorCodeInvalidApiType、初始 500、
// skip、type=new_api_error。
//
// A-29 是结构性防御守卫:标准流程下 ClaudeHelper 先经 InitChannelMeta 从渠道类型派生
// ApiType(ChannelType2APIType(ChannelTypeCodex)=APITypeCodex),故 Codex 渠道恒得到
// *codex.Adaptor,该分支无法通过完整 ClaudeHelper 路径自然触发。本用例直接驱动 bridge
// 分支使用的权威断言函数 assertCodexBridgeAdaptor(ClaudeHelper 的 bridge 入口即调用它),
// 用非 Codex adaptor 覆盖"不可收窄"路径,断言其落 A-29。
func TestClaudeHelper_InvalidAdaptorTypeA29(t *testing.T) {
	// 非 Codex adaptor 宽接口(如 openai)不可断言为 *codex.Adaptor。
	codexAdaptor, apiErr := assertCodexBridgeAdaptor(&openai.Adaptor{})
	if apiErr == nil {
		t.Fatal("expected A-29 error, got nil")
	}
	if codexAdaptor != nil {
		t.Fatalf("expected nil codex adaptor on failure, got %v", codexAdaptor)
	}
	if apiErr.GetErrorCode() != types.ErrorCodeInvalidApiType {
		t.Fatalf("ErrorCode = %q, want %q", apiErr.GetErrorCode(), types.ErrorCodeInvalidApiType)
	}
	// A-29 初始 500(不参与 mapping)。
	if apiErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want 500", apiErr.StatusCode)
	}
	if apiErr.GetErrorType() != types.ErrorTypeNewAPIError {
		t.Fatalf("external type = %q, want new_api_error", apiErr.GetErrorType())
	}
	if !types.IsSkipRetryError(apiErr) {
		t.Fatal("A-29 must set skip retry")
	}
}

package service

import (
	"testing"

	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/model_setting"
	"github.com/stretchr/testify/assert"
)

// setGlobalPolicyForTest 写入全局 policy 并返回恢复函数。
func setGlobalPolicyForTest(policy model_setting.ChatCompletionsToResponsesPolicy) func() {
	gs := model_setting.GetGlobalSettings()
	prev := gs.ChatCompletionsToResponsesPolicy
	gs.ChatCompletionsToResponsesPolicy = policy
	return func() {
		gs.ChatCompletionsToResponsesPolicy = prev
	}
}

func TestShouldClaudeMessagesBridgeToCodex_Hit(t *testing.T) {
	restore := setGlobalPolicyForTest(model_setting.ChatCompletionsToResponsesPolicy{
		Enabled:       true,
		AllChannels:   true,
		ModelPatterns: []string{"^gpt-5\\.6-luna$"},
	})
	defer restore()

	got := ShouldClaudeMessagesBridgeToCodex(1, constant.ChannelTypeCodex, "gpt-5.6-luna")
	assert.True(t, got, "policy 命中且 channelType 为 Codex 应返回 true")
}

func TestShouldClaudeMessagesBridgeToCodex_PolicyHitNonCodex(t *testing.T) {
	restore := setGlobalPolicyForTest(model_setting.ChatCompletionsToResponsesPolicy{
		Enabled:       true,
		AllChannels:   true,
		ModelPatterns: []string{"^gpt-5\\.6-luna$"},
	})
	defer restore()

	got := ShouldClaudeMessagesBridgeToCodex(1, constant.ChannelTypeAnthropic, "gpt-5.6-luna")
	assert.False(t, got, "policy 命中但 channelType 非 Codex 应返回 false")
}

func TestShouldClaudeMessagesBridgeToCodex_PolicyMiss(t *testing.T) {
	restore := setGlobalPolicyForTest(model_setting.ChatCompletionsToResponsesPolicy{
		Enabled:       false,
		AllChannels:   true,
		ModelPatterns: []string{"^gpt-5\\.6-luna$"},
	})
	defer restore()

	got := ShouldClaudeMessagesBridgeToCodex(1, constant.ChannelTypeCodex, "gpt-5.6-luna")
	assert.False(t, got, "policy 未命中(未启用)即使 channelType 为 Codex 也应返回 false")
}

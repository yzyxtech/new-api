package dto

import (
	"testing"

	kitutil "github.com/QuantumNous/new-api/relaykit/relayconvert/kitutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// 验证 type=reasoning 的 ResponsesOutput 经 marshal→unmarshal 往返后
// encrypted_content 原值保留(D-1 DTO 层验证口径)。
func TestResponsesOutput_ReasoningEncryptedContentRoundTrip(t *testing.T) {
	const sig = "sig-9f8c7b6a5d4e3f21"
	item := ResponsesOutput{
		Type:             "reasoning",
		ID:               "rs_abc123",
		EncryptedContent: sig,
	}

	data, err := kitutil.Marshal(item)
	require.NoError(t, err)

	var decoded ResponsesOutput
	require.NoError(t, kitutil.Unmarshal(data, &decoded))
	assert.Equal(t, "reasoning", decoded.Type)
	assert.Equal(t, sig, decoded.EncryptedContent)
	assert.True(t, gjson.ValidBytes(data), "marshal must produce valid JSON")
	assert.Equal(t, sig, gjson.GetBytes(data, "encrypted_content").String(), "serialized JSON must carry encrypted_content")
}

// 验证 type=web_search_call 的 ResponsesOutput 往返后 output_item_id 原值保留,
// 且专用 MarshalJSON 不泄漏 status/role/quality/size 等其它 union 字段。
func TestResponsesOutput_WebSearchOutputItemIDRoundTrip(t *testing.T) {
	const id = "websearch_001"
	item := ResponsesOutput{
		Type:         "web_search_call",
		ID:           "ws_001",
		OutputItemID: id,
	}

	data, err := kitutil.Marshal(item)
	require.NoError(t, err)
	require.True(t, gjson.ValidBytes(data))

	var decoded ResponsesOutput
	require.NoError(t, kitutil.Unmarshal(data, &decoded))
	assert.Equal(t, "web_search_call", decoded.Type)
	assert.Equal(t, id, decoded.OutputItemID)

	// union 不泄漏:web_search_call 专用分支之外的非 web_search 字段不得出现。
	jsonStr := string(data)
	for _, key := range []string{"status", "role", "quality", "size", "content", "summary", "call_id", "name"} {
		assert.False(t, gjson.GetBytes(data, key).Exists(), "web_search_call must not leak %q, got: %s", key, jsonStr)
	}
	// 判别键 type 恒在。
	assert.Equal(t, "web_search_call", gjson.GetBytes(data, "type").String())
	assert.Equal(t, id, gjson.GetBytes(data, "output_item_id").String())
}

// 验证非适用 item 携带两个新字段时专用分支不产生对应键(D-1 的 DTO shape 断言,
// 非 reasoning 的 web_search_call 不产 encrypted_content;非 web_search 的 mcp_call 不产 output_item_id)。
func TestResponsesOutput_NonApplicableFieldsIgnored(t *testing.T) {
	t.Run("web_search_call carries encrypted_content", func(t *testing.T) {
		item := ResponsesOutput{
			Type:             "web_search_call",
			EncryptedContent: "leaked-sig",
		}
		data, err := kitutil.Marshal(item)
		require.NoError(t, err)
		assert.False(t, gjson.GetBytes(data, "encrypted_content").Exists(), "web_search_call must not serialize encrypted_content: %s", string(data))
	})

	t.Run("mcp_call carries output_item_id", func(t *testing.T) {
		item := ResponsesOutput{
			Type:         "mcp_call",
			OutputItemID: "leaked-id",
		}
		data, err := kitutil.Marshal(item)
		require.NoError(t, err)
		assert.False(t, gjson.GetBytes(data, "output_item_id").Exists(), "mcp_call must not serialize output_item_id: %s", string(data))
	})
}

// 验证非 type=error 事件 round-trip 后顶层 JSON 不含 error/error_type 键(内嵌 item 自带的
// 既有 error 字段不受影响,D-1b 单测①)。恒存判别键 type 除外,error 相关键不得出现。
func TestResponsesStreamResponse_NonErrorEventNoErrorKeys(t *testing.T) {
	evt := ResponsesStreamResponse{
		Type: "response.output_item.added",
		Item: &ResponsesOutput{
			Type: "mcp_call",
			ID:   "item_001",
		},
	}

	data, err := kitutil.Marshal(evt)
	require.NoError(t, err)
	require.True(t, gjson.ValidBytes(data))

	// 恒存判别键 type 恒在
	assert.Equal(t, "response.output_item.added", gjson.GetBytes(data, "type").String())
	// 顶层 error/error_type 键不得出现
	assert.False(t, gjson.GetBytes(data, "error").Exists(), "non-error event must not serialize top-level error: %s", string(data))
	assert.False(t, gjson.GetBytes(data, "error_type").Exists(), "non-error event must not serialize error_type: %s", string(data))

	// round-trip 后 Error/ErrorType 字段保持零值
	var decoded ResponsesStreamResponse
	require.NoError(t, kitutil.Unmarshal(data, &decoded))
	assert.Nil(t, decoded.Error)
	assert.Empty(t, decoded.ErrorType)
}

// 验证 Codex 线形顶层 error 事件 round-trip 后,除恒存 type 判别键外,error 相关键仅含
// error/error_type(+根 message 若存在),不含根 code/param(D-1b 单测②)。这是兼容断言:
// Codex fixture 本身不含根 Code/Param,新增字段不得引入多余键、既有根字段互不污染。
func TestResponsesStreamResponse_CodexErrorEventKeySet(t *testing.T) {
	evt := ResponsesStreamResponse{
		Type: "error",
		Error: map[string]any{
			"type":    "invalid_request",
			"message": "bad request from upstream",
			"code":    "invalid_param",
		},
		ErrorType: "invalid_request_error",
		Message:   "bad request from upstream",
	}

	data, err := kitutil.Marshal(evt)
	require.NoError(t, err)
	require.True(t, gjson.ValidBytes(data))

	// 恒存 type 判别键
	assert.Equal(t, "error", gjson.GetBytes(data, "type").String())
	// error/error_type 键在场
	assert.True(t, gjson.GetBytes(data, "error").Exists(), "codex error event must carry error object: %s", string(data))
	assert.Equal(t, "invalid_request", gjson.GetBytes(data, "error.type").String())
	assert.Equal(t, "bad request from upstream", gjson.GetBytes(data, "error.message").String())
	assert.Equal(t, "invalid_request_error", gjson.GetBytes(data, "error_type").String())
	// 根 message 若存在则保留
	assert.Equal(t, "bad request from upstream", gjson.GetBytes(data, "message").String())
	// 不含根 code/param
	assert.False(t, gjson.GetBytes(data, "code").Exists(), "codex error event must not carry root code: %s", string(data))
	assert.False(t, gjson.GetBytes(data, "param").Exists(), "codex error event must not carry root param: %s", string(data))

	// round-trip 后 Error/ErrorType 原值保留
	var decoded ResponsesStreamResponse
	require.NoError(t, kitutil.Unmarshal(data, &decoded))
	gotErr := GetOpenAIError(decoded.Error)
	require.NotNil(t, gotErr)
	assert.Equal(t, "invalid_request", gotErr.Type)
	assert.Equal(t, "bad request from upstream", gotErr.Message)
	assert.Equal(t, "invalid_param", gotErr.Code)
	assert.Equal(t, "invalid_request_error", decoded.ErrorType)
}

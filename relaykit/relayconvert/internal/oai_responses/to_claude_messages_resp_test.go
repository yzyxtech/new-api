package oairesponses

import (
	"testing"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponsesToClaudeMessages_ReasoningSignature(t *testing.T) {
	// 终态 reasoning item 含非空 encrypted_content → 断言 thinking 块 signature 字段原样映射(D-6)。
	const sig = "enc_sig_123"
	reasoningItem := dto.ResponsesOutput{
		Type:             responsesOutputTypeReasoning,
		ID:               "rs_1",
		EncryptedContent: sig,
	}
	resp := &dto.OpenAIResponsesResponse{
		ID:     "resp_1",
		Model:  "gpt-test",
		Output: []dto.ResponsesOutput{reasoningItem},
	}
	claude, _, err := ResponsesResponseToClaudeMessagesResponse(resp)
	require.NoError(t, err)

	var thinkingBlocks []dto.ClaudeMediaMessage
	for _, block := range claude.Content {
		if block.Type == "thinking" {
			thinkingBlocks = append(thinkingBlocks, block)
		}
	}
	require.Len(t, thinkingBlocks, 1)
	assert.Equal(t, "thinking", thinkingBlocks[0].Type)
	assert.Equal(t, sig, thinkingBlocks[0].Signature)
}

func TestResponsesToClaudeMessages_WebSearchBlocks(t *testing.T) {
	const query = "what is the capital of france"
	actionRaw := rawJSON(t, map[string]any{"type": "web_search", "query": query})
	resultsRaw := rawJSON(t, []map[string]string{
		{"url": "https://en.wikipedia.org/wiki/France", "title": "France - Wikipedia"},
	})
	item := dto.ResponsesOutput{
		Type:    dto.BuildInCallWebSearchCall,
		ID:      "ws_1",
		Action:  actionRaw,
		Results: resultsRaw,
	}
	resp := &dto.OpenAIResponsesResponse{
		ID:     "resp_1",
		Model:  "gpt-test",
		Output: []dto.ResponsesOutput{item},
	}
	claude, _, err := ResponsesResponseToClaudeMessagesResponse(resp)
	require.NoError(t, err)

	require.Len(t, claude.Content, 2)
	useBlock := claude.Content[0]
	resultBlock := claude.Content[1]

	// server_tool_use 块:id/name/input。
	assert.Equal(t, "server_tool_use", useBlock.Type)
	assert.Equal(t, "ws_1", useBlock.Id)
	assert.Equal(t, "web_search", useBlock.Name)
	useInput, ok := useBlock.Input.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, query, useInput["query"])

	// web_search_tool_result 块:tool_use_id 与 web_search_result 数组。
	assert.Equal(t, "web_search_tool_result", resultBlock.Type)
	assert.Equal(t, "ws_1", resultBlock.ToolUseId)
	content, ok := resultBlock.Content.([]any)
	require.True(t, ok)
	require.Len(t, content, 1)
	result, ok := content[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "web_search_result", result["type"])
	assert.Equal(t, "France - Wikipedia", result["title"])
	assert.Equal(t, "https://en.wikipedia.org/wiki/France", result["url"])
	assert.Nil(t, result["page_age"])
}

func TestResponsesToClaudeMessages_WebSearchIDFallback(t *testing.T) {
	// 覆盖 tool_use_id 回退链四级来源(非流式无根事件,不用 ResponsesStreamResponse.ItemID):
	// item.id / item.output_item_id / item.call_id / 上一已知 web_search ID。每级独立 resp 隔离。
	actionRaw := rawJSON(t, map[string]string{"query": "q"})
	resultsRaw := rawJSON(t, []map[string]string{{"url": "https://e.com", "title": "Example"}})

	blockIDs := func(resp *dto.OpenAIResponsesResponse) []string {
		claude, _, err := ResponsesResponseToClaudeMessagesResponse(resp)
		require.NoError(t, err)
		var ids []string
		for _, block := range claude.Content {
			if block.Type == "server_tool_use" {
				ids = append(ids, block.Id)
			}
		}
		return ids
	}

	// 1) item.id
	assert.Equal(t, []string{"ws_id"}, blockIDs(&dto.OpenAIResponsesResponse{
		Output: []dto.ResponsesOutput{{Type: dto.BuildInCallWebSearchCall, ID: "ws_id", Action: actionRaw, Results: resultsRaw}},
	}))
	// 2) item.output_item_id
	assert.Equal(t, []string{"ws_oid"}, blockIDs(&dto.OpenAIResponsesResponse{
		Output: []dto.ResponsesOutput{{Type: dto.BuildInCallWebSearchCall, OutputItemID: "ws_oid", Action: actionRaw, Results: resultsRaw}},
	}))
	// 3) item.call_id
	assert.Equal(t, []string{"ws_call"}, blockIDs(&dto.OpenAIResponsesResponse{
		Output: []dto.ResponsesOutput{{Type: dto.BuildInCallWebSearchCall, CallId: "ws_call", Action: actionRaw, Results: resultsRaw}},
	}))
	// 4) 上一已知 ID:前一 item 确立 lastWebSearchID,后一 all-empty item 回退到该 ID;
	// 因该 ID 已发射,按 ID 去重不产生第二组。
	{
		first := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
		first.ID = "ws_prior"
		second := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
		resp := &dto.OpenAIResponsesResponse{
			Output: []dto.ResponsesOutput{first, second},
		}
		claude, _, err := ResponsesResponseToClaudeMessagesResponse(resp)
		require.NoError(t, err)
		var useIDs []string
		for _, block := range claude.Content {
			if block.Type == "server_tool_use" {
				useIDs = append(useIDs, block.Id)
			}
		}
		require.Equal(t, []string{"ws_prior"}, useIDs, "上一已知 ID 回退命中但该 ID 已发射,去重后仅一组")
	}
}

func TestResponsesToClaudeMessages_WebSearchAllEmptyID(t *testing.T) {
	// 全空 ID(无 item.id/output_item_id/call_id,无上一已知 ID)→ 断言不产出任何 web_search 块。
	actionRaw := rawJSON(t, map[string]string{"query": "q"})
	resultsRaw := rawJSON(t, []map[string]string{{"url": "https://e.com", "title": "Example"}})
	item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
	resp := &dto.OpenAIResponsesResponse{
		Output: []dto.ResponsesOutput{item},
	}
	claude, _, err := ResponsesResponseToClaudeMessagesResponse(resp)
	require.NoError(t, err)

	// 全空 ID 不产出,且既有内容为空的保底 text 块被填充。
	require.Len(t, claude.Content, 1)
	require.Equal(t, "text", claude.Content[0].Type, "不应有任何 web_search 块")
}

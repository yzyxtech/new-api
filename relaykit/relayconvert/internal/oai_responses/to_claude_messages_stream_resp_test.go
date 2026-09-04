package oairesponses

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/QuantumNous/new-api/relaykit/dto"
	kitutil "github.com/QuantumNous/new-api/relaykit/relayconvert/kitutil"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResponsesToClaudeStreamDoesNotRepeatBlocksFromDoneAndCompletedEvents(t *testing.T) {
	state := NewResponsesToClaudeStreamState("", "")
	arguments := `{"q":"x"}`
	argumentRaw, err := kitutil.Marshal(arguments)
	require.NoError(t, err)
	statusRaw, err := kitutil.Marshal("completed")
	require.NoError(t, err)

	reasoningItem := dto.ResponsesOutput{
		Type:    responsesOutputTypeReasoning,
		ID:      "rs_1",
		Summary: []dto.ResponsesReasoningSummaryPart{{Type: "summary_text", Text: "plan"}},
	}
	messageItem := dto.ResponsesOutput{
		Type:    responsesOutputTypeMessage,
		ID:      "msg_1",
		Role:    "assistant",
		Content: []dto.ResponsesOutputContent{{Type: "output_text", Text: "hello"}},
	}
	toolItem := dto.ResponsesOutput{
		Type:      responsesOutputTypeFunctionCall,
		ID:        "fc_1",
		CallId:    "call_1",
		Name:      "lookup",
		Arguments: argumentRaw,
	}

	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "resp_1", Model: "gpt-test"}},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: reasoningItem.ID, Item: &dto.ResponsesOutput{Type: reasoningItem.Type, ID: reasoningItem.ID}},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: reasoningItem.ID, Delta: "plan"},
		{Type: responsesEventReasoningSummaryDone, OutputIndex: kitutil.GetPointer(0), ItemID: reasoningItem.ID, Text: kitutil.GetPointer("plan")},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: reasoningItem.ID, Item: &reasoningItem},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(1), ItemID: messageItem.ID, Item: &dto.ResponsesOutput{Type: messageItem.Type, ID: messageItem.ID, Role: "assistant"}},
		{Type: responsesEventOutputTextDelta, OutputIndex: kitutil.GetPointer(1), ItemID: messageItem.ID, Delta: "hello"},
		{Type: responsesEventOutputTextDone, OutputIndex: kitutil.GetPointer(1), ItemID: messageItem.ID, Text: kitutil.GetPointer("hello")},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), ItemID: messageItem.ID, Item: &messageItem},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(2), ItemID: toolItem.ID, Item: &dto.ResponsesOutput{Type: toolItem.Type, ID: toolItem.ID, CallId: toolItem.CallId, Name: toolItem.Name}},
		{Type: responsesEventFunctionArgsDelta, OutputIndex: kitutil.GetPointer(2), ItemID: toolItem.ID, Delta: `{"q":`},
		{Type: responsesEventFunctionArgsDelta, OutputIndex: kitutil.GetPointer(2), ItemID: toolItem.ID, Delta: `"x"}`},
		{Type: responsesEventFunctionArgsDone, OutputIndex: kitutil.GetPointer(2), ItemID: toolItem.ID, Arguments: &arguments},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(2), ItemID: toolItem.ID, Item: &toolItem},
		{
			Type: responsesEventCompleted,
			Response: &dto.OpenAIResponsesResponse{
				ID:     "resp_1",
				Model:  "gpt-test",
				Status: statusRaw,
				Output: []dto.ResponsesOutput{reasoningItem, messageItem, toolItem},
				Usage:  &dto.Usage{InputTokens: 11, OutputTokens: 7, TotalTokens: 18},
			},
		},
	}

	var output []*dto.ClaudeResponse
	for _, event := range events {
		converted, _, err := state.ConvertChunk(event, 9)
		require.NoError(t, err)
		output = append(output, converted...)
	}

	starts := responsesOfType(output, "content_block_start")
	stops := responsesOfType(output, "content_block_stop")
	require.Len(t, responsesOfType(output, "message_start"), 1)
	require.Len(t, starts, 3)
	require.Len(t, stops, 3)
	require.Len(t, responsesOfType(output, "message_delta"), 1)
	require.Len(t, responsesOfType(output, "message_stop"), 1)
	assert.Equal(t, []int{0, 1, 2}, []int{starts[0].GetIndex(), starts[1].GetIndex(), starts[2].GetIndex()})
	assert.Equal(t, []string{"thinking", "text", "tool_use"}, []string{starts[0].ContentBlock.Type, starts[1].ContentBlock.Type, starts[2].ContentBlock.Type})
	assert.Equal(t, "plan", joinedClaudeDeltas(output, "thinking_delta"))
	assert.Equal(t, "hello", joinedClaudeDeltas(output, "text_delta"))
	assert.Equal(t, arguments, joinedClaudeDeltas(output, "input_json_delta"))
	messageDelta := responsesOfType(output, "message_delta")[0]
	require.NotNil(t, messageDelta.Delta.StopReason)
	assert.Equal(t, "tool_use", *messageDelta.Delta.StopReason)

	finalized, err := state.Finalize(9)
	require.NoError(t, err)
	assert.Empty(t, finalized)
	repeated, _, err := state.ConvertChunk(events[len(events)-1], 9)
	require.NoError(t, err)
	assert.Empty(t, repeated)
}

func TestResponsesToClaudeStream_SignatureDeltaBasic(t *testing.T) {
	const sig = "sig_abc123"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_basic", EncryptedContent: sig}
	done := &dto.ResponsesOutput{
		Type:             responsesOutputTypeReasoning,
		ID:               "rs_basic",
		EncryptedContent: sig,
		Summary:          []dto.ResponsesReasoningSummaryPart{{Type: "summary_text", Text: "plan"}},
	}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "r1", Model: "m"}},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_basic", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_basic", Delta: "plan"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_basic", Item: done},
	}
	output := runStreamEvents(t, state, events)

	starts := responsesOfType(output, "content_block_start")
	stops := responsesOfType(output, "content_block_stop")
	require.Len(t, starts, 1)
	require.Equal(t, "thinking", starts[0].ContentBlock.Type)
	require.Len(t, stops, 1)
	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))

	// 完整帧 JSON 含 index 与内嵌 delta 结构(delta.type=signature_delta / signature 原值)。
	frame := findSignatureDeltaResponse(output)
	require.NotNil(t, frame)
	require.NotNil(t, frame.Index)
	assert.Equal(t, starts[0].GetIndex(), *frame.Index)
	raw, err := kitutil.Marshal(frame)
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, kitutil.Unmarshal(raw, &wire))
	assert.Equal(t, "content_block_delta", wire["type"])
	assert.Equal(t, float64(starts[0].GetIndex()), wire["index"])
	deltaObj, ok := wire["delta"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "signature_delta", deltaObj["type"])
	assert.Equal(t, sig, deltaObj["signature"])
}

func TestResponsesToClaudeStream_SignatureOnly(t *testing.T) {
	const sig = "edge_sig_only"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_only"}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_only", EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_only", Item: added},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_only", Item: done},
	}
	output := runStreamEvents(t, state, events)

	starts := responsesOfType(output, "content_block_start")
	stops := responsesOfType(output, "content_block_stop")
	require.Len(t, starts, 1)
	require.Equal(t, "thinking", starts[0].ContentBlock.Type)
	require.Len(t, stops, 1)
	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_SignatureOnlyTextInterleaved(t *testing.T) {
	const sig = "interleaved_sig"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_inter", EncryptedContent: sig}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_inter", EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_inter", Item: added},
		{Type: responsesEventOutputTextDelta, OutputIndex: kitutil.GetPointer(1), ItemID: "msg_inter", Delta: "hello"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_inter", Item: done},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:text", "delta:0:text_delta", "stop:0", "start:1:thinking", "delta:1:signature_delta", "stop:1"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_EarlyCloseLateDone(t *testing.T) {
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_early", EncryptedContent: ""}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_early", EncryptedContent: "late_sig"}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_early", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_early", Delta: "plan"},
		{Type: responsesEventOutputTextDelta, OutputIndex: kitutil.GetPointer(1), ItemID: "msg_early", Delta: "hello"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_early", Item: done},
	}
	output := runStreamEvents(t, state, events)

	// 提前关闭后 done 迟到且快照为空 → 0 次 signature_delta,块序列仍合法。
	require.Empty(t, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "stop:0", "start:1:text", "delta:1:text_delta"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_ReasoningInterleavedIdempotent(t *testing.T) {
	const sigA = "sig_A"
	const sigB = "sig_B"
	state := NewResponsesToClaudeStreamState("", "")
	addedA := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsA", EncryptedContent: sigA}
	doneA := &dto.ResponsesOutput{
		Type:             responsesOutputTypeReasoning,
		ID:               "rsA",
		EncryptedContent: sigA,
		Summary:          []dto.ResponsesReasoningSummaryPart{{Type: "summary_text", Text: "pa"}},
	}
	addedB := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsB", EncryptedContent: sigB}
	doneB := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsB", EncryptedContent: sigB}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Item: addedA},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Delta: "pa"},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Item: addedB},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Item: doneA},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Item: doneB},
		// 重复 done:幂等,无额外输出,且不污染其它 item 状态。
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Item: doneA},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Item: doneB},
	}
	output := runStreamEvents(t, state, events)

	// A(带 summary 的常规块)与 B(signature-only)各自独立,签名各发射恰好一次且不互串。
	assert.Equal(t, []string{sigA, sigB}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0",
			"start:1:thinking", "delta:1:signature_delta", "stop:1"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_SignaturePreservesWhitespace(t *testing.T) {
	// FR-007/INV-2:signature 存在性仅以原值是否非空判断,发射值直接使用原始
	// encrypted_content,不得 TrimSpace。含首尾空白的非空 signature 原样透传。
	const sig = "  sig wspace  "
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_wsp", EncryptedContent: sig}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_wsp", EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_wsp", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_wsp", Delta: "plan"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_wsp", Item: done},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
}

func TestResponsesToClaudeStream_IdentityLateAliasRegistered(t *testing.T) {
	// summary/done 事件后到携带稳定标识时,把该标识作为既有 state 的 alias 补登记,
	// 复用同一 state 而非新建;签名恰好发射一次。
	const sig = "sig_late"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: sig}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "r1", Model: "m"}},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), Delta: "plan"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_late", Item: done},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))
	// 后到 ID 作为 alias 补登记后,按 ID 解析仍命中同一 state。
	require.Len(t, state.reasoningItems, 1)
	assert.Same(t, state.reasoningItems[0], state.reasoningByItemID["rs_late"])
}

func TestResponsesToClaudeStream_IdentityConflictError(t *testing.T) {
	// 两个稳定标识分别指向不同 state 时不得合并,按响应转换错误返回。
	state := NewResponsesToClaudeStreamState("", "")
	addedA := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "res_a", EncryptedContent: "sig_a"}
	addedB := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "res_b", EncryptedContent: "sig_b"}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "res_a", Item: addedA},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(1), ItemID: "res_b", Item: addedB},
		// idx 指向 A、item.id 绑定 B,身份冲突,不得合并。
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "res_b", Item: addedB},
	}
	output, _, err := feedStreamEvents(t, state, events)
	require.Error(t, err)
	require.Empty(t, signatureDeltas(output))
}

func TestResponsesToClaudeStream_UnidentifiedDoneRejectedError(t *testing.T) {
	// 无稳定标识且无 active anonymous state 的 done:身份不可判定,按转换错误返回,
	// 不得按 D-4 之外的口径新建 state。
	state := NewResponsesToClaudeStreamState("", "")
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: "sig_x"}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemDone, Item: done},
	}
	_, _, err := feedStreamEvents(t, state, events)
	require.Error(t, err)
	require.Empty(t, state.reasoningItems)
}

func TestResponsesToClaudeStream_UnidentifiedSummaryRejectedError(t *testing.T) {
	// 无 active state 时收到无标识 reasoning summary:身份不可判定,按 D-4 报响应转换
	// 错误。身份解析须在副作用之前完成——不得创建 block、不得发射内容、不得建 reasoning state。
	state := NewResponsesToClaudeStreamState("", "")
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "r1", Model: "m"}},
		{Type: responsesEventReasoningSummaryDelta, Delta: "plan"},
	}
	output, _, err := feedStreamEvents(t, state, events)
	require.Error(t, err)
	require.Empty(t, signatureDeltas(output))
	require.Empty(t, state.reasoningItems)
	require.Empty(t, state.blocks, "身份不可判定不得创建内部 block")
}

// TestResponsesToClaudeStream_UnidentifiedSummaryPreservesOpenThinking verifies the
// side-effect ordering when an unidentified summary arrives while another identified
// thinking block is still open: identity resolution precedes finalize/ensureBlock, so
// the error path leaves no new block, does not finalize or otherwise mutate the open
// thinking block, and does not emit its signature.
func TestResponsesToClaudeStream_UnidentifiedSummaryPreservesOpenThinking(t *testing.T) {
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_open", EncryptedContent: "keep_sig"}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "r1", Model: "m"}},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_open", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_open", Delta: "plan"},
	}
	// 先消费前序事件,让 thinking 块 (0) 处于打开态(未 stop)。
	_, _, err := feedStreamEvents(t, state, events[:3])
	require.NoError(t, err)
	require.Len(t, state.blocks, 1)
	openBlock := state.blocks[0]
	require.True(t, openBlock.Started)
	require.False(t, openBlock.Stopped)

	// 此刻再投递一个无标识 summary(无 output_index/item_id,且无 active anonymous)。
	output, _, err := feedStreamEvents(t, state, []*dto.ResponsesStreamResponse{
		{Type: responsesEventReasoningSummaryDelta, Delta: "intruded"},
	})
	require.Error(t, err)

	// 无副作用:不新建 block;已有打开块状态未变(仍打开、内容未变、签名未发射)。
	require.Len(t, state.blocks, 1, "身份不可判定不得创建新 block")
	require.Same(t, openBlock, state.blocks[0])
	require.True(t, openBlock.Started)
	require.False(t, openBlock.Stopped, "不得因身份校验失败而提前收尾已打开 thinking 块")
	require.Equal(t, "plan", openBlock.Value.String(), "已打开块的内容必须保持不变")
	require.Empty(t, responsesOfType(output, "content_block_delta"), "错误路径不得发射任何 delta")
	require.Empty(t, signatureDeltas(output))
}

func TestResponsesToClaudeStream_ContentPartAddedFinalizesOpenThinking(t *testing.T) {
	// INV-5:response.content_part.added 属于内容边界事件,先到先触发——在进入新块
	// 前收尾已打开的 thinking 块,先 signature_delta 再 content_block_stop。
	const sig = "cp_sig"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_cp", EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "r1", Model: "m"}},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_cp", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_cp", Delta: "plan"},
		{Type: responsesEventContentPartAdded},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_AddedIdentityConflictError(t *testing.T) {
	// added 也纳入统一 resolver:当事件的 output_index 与 item_id 已分别绑定到不同 state
	// 时,added 阶段同样触发身份冲突错误,不得新建/合并状态。
	state := NewResponsesToClaudeStreamState("", "")
	addedA := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "res_a", EncryptedContent: "sig_a"}
	addedB := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "res_b", EncryptedContent: "sig_b"}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "res_a", Item: addedA},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(1), ItemID: "res_b", Item: addedB},
		// added 的 idx 指向 A、item.id 绑定 B,身份冲突,不得合并。
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "res_b", Item: addedB},
	}
	_, _, err := feedStreamEvents(t, state, events)
	require.Error(t, err)
	require.Len(t, state.reasoningItems, 2, "冲突事件不得新建 state")
}

func TestResponsesToClaudeStream_AnonymousLateAliasRegistered(t *testing.T) {
	// 纯 anonymous(reasoning 无 output_index 亦无稳定标识)item:added 建匿名 state,
	// summary/done 绑定该 active anonymous;done 后到携带稳定标识时补登记为该 state 的
	// alias,签名仍恰好发射一次。
	const sig = "anon_late"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: sig}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, Item: added},
		{Type: responsesEventReasoningSummaryDelta, Delta: "plan"},
		{Type: responsesEventOutputItemDone, ItemID: "rs_late", Item: done},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))
	// 后到 ID 作为 alias 补登记到该 anonymous state。
	require.Len(t, state.reasoningItems, 1)
	assert.Same(t, state.reasoningItems[0], state.reasoningByItemID["rs_late"])
}

func TestResponsesToClaudeStream_AnonymousSignatureOnly(t *testing.T) {
	// 纯 anonymous reasoning item:无 output_index/稳定标识,added 建 active anonymous,
	// done 绑定同一 state 走 signature-only 收尾,签名发射恰好一次。
	const sig = "anon_sig_only"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: sig}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, Item: added},
		{Type: responsesEventOutputItemDone, Item: done},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_FunctionArgsDeltaFinalizesOpenThinking(t *testing.T) {
	// INV-5:thinking 块仍打开时收到 function args delta(tool 边界),先 finalize 旧
	// thinking 块(signature_delta → content_block_stop)再开 tool 块。
	const sig = "tool_boundary_sig"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_t", EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_t", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_t", Delta: "plan"},
		{Type: responsesEventFunctionArgsDelta, OutputIndex: kitutil.GetPointer(1), ItemID: "fc_1",
			Item:  &dto.ResponsesOutput{Type: responsesOutputTypeFunctionCall, ID: "fc_1", Name: "lookup", CallId: "call_1"},
			Delta: `{"a":"x"}`},
	}
	output := runStreamEvents(t, state, events)

	require.Equal(t, []string{sig}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0",
			"start:1:tool_use", "delta:1:input_json_delta"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_ReasoningSummaryInterleaveFinalizes(t *testing.T) {
	// INV-5:thinking 块仍打开时收到另一 reasoning 的 summary delta,先 finalize 旧
	// thinking 块再开新块;新 item 的 done 正常收尾,旧 item 的 done 因提前关闭走状态③
	// 不再发射。
	const sigA = "interleave_a"
	const sigB = "interleave_b"
	state := NewResponsesToClaudeStreamState("", "")
	addedA := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsA", EncryptedContent: sigA}
	addedB := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsB", EncryptedContent: sigB}
	doneB := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsB", EncryptedContent: sigB}
	doneA := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsA", EncryptedContent: sigA}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Item: addedA},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Delta: "plan-a"},
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Item: addedB},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Delta: "plan-b"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Item: doneB},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Item: doneA},
	}
	output := runStreamEvents(t, state, events)

	// A 在 B 的 summary 边界被提前收尾发出 sigA,B 正常收尾发出 sigB;A 的迟到 done 不再发。
	require.Equal(t, []string{sigA, sigB}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0",
			"start:1:thinking", "delta:1:thinking_delta", "delta:1:signature_delta", "stop:1"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_DoneFirstWithSummaryFinalizesOtherThinking(t *testing.T) {
	// INV-5(done-first 携带 summary):A 的 thinking 块仍打开时,B 的 done 先于 B 的
	// added/summary 到达且携带 summary 文本。B 的 done 会新建 thinking 块,必须先
	// finalize A 仍打开的块(signature_delta → stop),再进入 B 自身块收尾/创建。
	const sigA = "donefirst_a"
	const sigB = "donefirst_b"
	state := NewResponsesToClaudeStreamState("", "")
	addedA := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rsA", EncryptedContent: sigA}
	doneB := &dto.ResponsesOutput{
		Type:             responsesOutputTypeReasoning,
		ID:               "rsB",
		EncryptedContent: sigB,
		Summary:          []dto.ResponsesReasoningSummaryPart{{Type: "summary_text", Text: "plan-b"}},
	}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Item: addedA},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rsA", Delta: "plan-a"},
		// B 的 done-first:done 携带 summary 先于 B 的 added/summary。
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), ItemID: "rsB", Item: doneB},
	}
	output := runStreamEvents(t, state, events)

	// A 在 B 的 done 边界被先收尾并发出 sigA,然后 B 才新建自己的 thinking 块(把 done
	// 携带的 summary 文本合并进该块)发出 sigB:新块 start 必须先于旧块 stop,不得在旧块
	// 仍打开时开启新块。
	require.Equal(t, []string{sigA, sigB}, signatureDeltas(output))
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0",
			"start:1:thinking", "delta:1:thinking_delta", "delta:1:signature_delta", "stop:1"},
		describeBlockSequence(output))
}

func TestResponsesToClaudeStream_RootItemIDAuthoritativeBlockBinding(t *testing.T) {
	// 根事件 item_id 与 item.id 不同且根 item_id 已绑定时,reasoning 分支的块查找/收尾
	// 必须以已解析 reasoning state 的 block 为权威,不得再用 item.id 优先的通用 findBlock。
	// 否则 done 事件的 item.id 未命中 byItemID 时会把同一 thinking 块误判为干扰块提前收尾,
	// 使 done 走状态③而丢弃应有的 signature_delta。
	const sig = "root_binding_sig"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "item_inner", EncryptedContent: sig}
	// done item 不携带 inner id(身份由根 item_id 承载),ensureDone 的通用 item.id 查找
	// 无法按 byItemID 复用块——块归属必须以已解析 state.block 为准。
	done := &dto.ResponsesOutput{
		Type:             responsesOutputTypeReasoning,
		EncryptedContent: sig,
		Summary:          []dto.ResponsesReasoningSummaryPart{{Type: "summary_text", Text: "plan"}},
	}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, ItemID: "root_item", Item: added},
		{Type: responsesEventReasoningSummaryDelta, ItemID: "root_item", Item: added, Delta: "plan"},
		{Type: responsesEventOutputItemDone, ItemID: "root_item", Item: done},
	}
	output := runStreamEvents(t, state, events)

	// 不误收尾同一块:thinking 块恰一组 start/delta/signature_delta/stop,签名正常发射一次。
	require.Equal(t, []string{sig}, signatureDeltas(output))
	starts := responsesOfType(output, "content_block_start")
	stops := responsesOfType(output, "content_block_stop")
	require.Len(t, starts, 1)
	require.Equal(t, "thinking", starts[0].ContentBlock.Type)
	require.Len(t, stops, 1)
	assert.Equal(t,
		[]string{"start:0:thinking", "delta:0:thinking_delta", "delta:0:signature_delta", "stop:0"},
		describeBlockSequence(output))

	// 只有一个 thinking 块,未被重建;state.block 与块序列同引用(块归属以 state 为权威)。
	require.Len(t, state.reasoningItems, 1)
	require.Len(t, state.blocks, 1)
	assert.Same(t, state.blocks[0], state.reasoningItems[0].block)
	require.False(t, state.reasoningItems[0].earlyClosed, "同一块不得被误判为提前关闭")
}

func TestResponsesToClaudeStream_WebSearchDone(t *testing.T) {
	const query = "what is the capital of france"
	// done + web_search_call(含 item.id)→ 断言一组 server_tool_use + web_search_tool_result。
	state := NewResponsesToClaudeStreamState("", "")
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
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventCreated, Response: &dto.OpenAIResponsesResponse{ID: "r1", Model: "m"}},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "ws_1", Item: &item},
	}
	output := runStreamEvents(t, state, events)

	starts := responsesOfType(output, "content_block_start")
	stops := responsesOfType(output, "content_block_stop")
	require.Len(t, starts, 2)
	require.Len(t, stops, 2)
	assert.Equal(t, "server_tool_use", starts[0].ContentBlock.Type)
	assert.Equal(t, "ws_1", starts[0].ContentBlock.Id)
	assert.Equal(t, "web_search", starts[0].ContentBlock.Name)
	assert.Equal(t, "web_search_tool_result", starts[1].ContentBlock.Type)
	assert.Equal(t, "ws_1", starts[1].ContentBlock.ToolUseId)

	// server_tool_use 的 input_json_delta 携带 {"query":...}。
	assert.Equal(t, `{"query":"what is the capital of france"}`, joinedClaudeDeltas(output, "input_json_delta"))

	// web_search_tool_result content 为 web_search_result 数组,title/url 对齐基线。
	content, ok := starts[1].ContentBlock.Content.([]any)
	require.True(t, ok)
	require.Len(t, content, 1)
	resultBlock, ok := content[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "web_search_result", resultBlock["type"])
	assert.Equal(t, "France - Wikipedia", resultBlock["title"])
	assert.Equal(t, "https://en.wikipedia.org/wiki/France", resultBlock["url"])
	assert.Nil(t, resultBlock["page_age"])

	// page_age 恒 null 的线形校验(raw marshal 不含引号的 null)。
	resultRaw, err := kitutil.Marshal(resultBlock)
	require.NoError(t, err)
	require.Contains(t, string(resultRaw), `"page_age":null`)

	assert.Equal(t, []string{
		"start:0:server_tool_use", "delta:0:input_json_delta", "stop:0",
		"start:1:web_search_tool_result", "stop:1",
	}, describeBlockSequence(output))
}

func TestResponsesToClaudeStream_WebSearchIDFallbackChain(t *testing.T) {
	// 覆盖 tool_use_id 回退链五级来源:item.id / item.output_item_id / item.call_id /
	// 根 event.item_id / 上一已知 ID。每级用独立 state 隔离,互不污染。
	actionRaw := rawJSON(t, map[string]string{"query": "q"})
	resultsRaw := rawJSON(t, []map[string]string{{"url": "https://e.com", "title": "Example"}})

	// 1) item.id
	{
		state := NewResponsesToClaudeStreamState("", "")
		item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, ID: "ws_id", Action: actionRaw, Results: resultsRaw}
		output := runStreamEvents(t, state, []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), Item: &item},
		})
		assert.Equal(t, "ws_id", webSearchServerToolUseID(output))
	}
	// 2) item.output_item_id
	{
		state := NewResponsesToClaudeStreamState("", "")
		item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, OutputItemID: "ws_oid", Action: actionRaw, Results: resultsRaw}
		output := runStreamEvents(t, state, []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), Item: &item},
		})
		assert.Equal(t, "ws_oid", webSearchServerToolUseID(output))
	}
	// 3) item.call_id
	{
		state := NewResponsesToClaudeStreamState("", "")
		item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, CallId: "ws_call", Action: actionRaw, Results: resultsRaw}
		output := runStreamEvents(t, state, []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), Item: &item},
		})
		assert.Equal(t, "ws_call", webSearchServerToolUseID(output))
	}
	// 4) 根 event.item_id
	{
		state := NewResponsesToClaudeStreamState("", "")
		item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
		output := runStreamEvents(t, state, []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "ws_root", Item: &item},
		})
		assert.Equal(t, "ws_root", webSearchServerToolUseID(output))
	}
	// 5) 上一已知 ID:前一 web_search 确立 lastWebSearchID,后一 all-empty done 回退到该
	// ID;因该 ID 已发射而按去重不产生第二组(设计 D-4:每个 ID 各至多产出一组)。
	{
		state := NewResponsesToClaudeStreamState("", "")
		first := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
		output := runStreamEvents(t, state, []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "ws_prior", Item: &first},
		})
		require.Equal(t, "ws_prior", webSearchServerToolUseID(output))
		require.Equal(t, "ws_prior", state.lastWebSearchID)

		second := first
		before := len(responsesOfType(output, "content_block_start"))
		more := runStreamEvents(t, state, []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), Item: &second},
		})
		// all-empty done 回退到已发射的 lastWebSearchID,故不新增任何 content_block。
		require.Empty(t, more)
		assert.Equal(t, before, len(responsesOfType(append(output, more...), "content_block_start")))
	}
}

func TestResponsesToClaudeStream_WebSearchAddedIDFallback(t *testing.T) {
	// added 提供 ID、随后的 done 前四级全空 → 凭 added 阶段记录的 lastWebSearchID 产出一组。
	// 覆盖第五级回退:此前 added 分支直接返回未记录 ID,done 全空时无法回退。
	state := NewResponsesToClaudeStreamState("", "")
	actionRaw := rawJSON(t, map[string]string{"query": "q"})
	resultsRaw := rawJSON(t, []map[string]string{{"url": "https://e.com", "title": "Example"}})
	added := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, ID: "ws_added", Action: actionRaw, Results: resultsRaw}
	done := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "ws_added", Item: &added},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), Item: &done},
	}
	output := runStreamEvents(t, state, events)

	// added 阶段只记录 ID 不产出块;done 全空时凭 lastWebSearchID 回退产出一组。
	require.Equal(t, "ws_added", state.lastWebSearchID)
	require.Equal(t, "ws_added", webSearchServerToolUseID(output))
	starts := responsesOfType(output, "content_block_start")
	stops := responsesOfType(output, "content_block_stop")
	require.Len(t, starts, 2)
	require.Len(t, stops, 2)
	assert.Equal(t, "server_tool_use", starts[0].ContentBlock.Type)
	assert.Equal(t, "ws_added", starts[0].ContentBlock.Id)
	assert.Equal(t, "web_search_tool_result", starts[1].ContentBlock.Type)
	assert.Equal(t, "ws_added", starts[1].ContentBlock.ToolUseId)
}

func TestResponsesToClaudeStream_WebSearchAllEmptyID(t *testing.T) {
	// 全空 ID(无 item.id/output_item_id/call_id,无根 event.item_id,亦无上一已知 ID)→
	// 断言不产出任何 web_search 块。
	state := NewResponsesToClaudeStreamState("", "")
	actionRaw := rawJSON(t, map[string]string{"query": "q"})
	resultsRaw := rawJSON(t, []map[string]string{{"url": "https://e.com", "title": "Example"}})
	item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, Action: actionRaw, Results: resultsRaw}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemDone, Item: &item},
	}
	output := runStreamEvents(t, state, events)
	require.Empty(t, responsesOfType(output, "content_block_start"))
	require.Empty(t, responsesOfType(output, "content_block_delta"))
	require.Empty(t, responsesOfType(output, "content_block_stop"))
}

func TestResponsesToClaudeStream_WebSearchDedup(t *testing.T) {
	// 同 ID 重复 done → 断言仅产出一组 server_tool_use + web_search_tool_result。
	state := NewResponsesToClaudeStreamState("", "")
	actionRaw := rawJSON(t, map[string]string{"query": "q"})
	resultsRaw := rawJSON(t, []map[string]string{{"url": "https://e.com", "title": "Example"}})
	item := dto.ResponsesOutput{Type: dto.BuildInCallWebSearchCall, ID: "ws_dup", Action: actionRaw, Results: resultsRaw}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "ws_dup", Item: &item},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(1), ItemID: "ws_dup", Item: &item},
	}
	output := runStreamEvents(t, state, events)

	var serverStarts, resultStarts int
	for _, start := range responsesOfType(output, "content_block_start") {
		switch start.ContentBlock.Type {
		case "server_tool_use":
			serverStarts++
		case "web_search_tool_result":
			resultStarts++
		}
	}
	require.Equal(t, 1, serverStarts)
	require.Equal(t, 1, resultStarts)
}

func TestResponsesToClaudeStream_EarlyCloseDiagnosticCount(t *testing.T) {
	// 交错 fixture(added 空 → summary delta 块开 → output_text.delta 触发提前关闭 →
	// done 终值非空):容量未满(n<32)且仅一个新 drop → 断言诊断计数 +1,且重复 done 不再记录。
	const sig = "late_sig"
	state := NewResponsesToClaudeStreamState("", "")
	added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_diag", EncryptedContent: ""}
	done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: "rs_diag", EncryptedContent: sig}
	events := []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_diag", Item: added},
		{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_diag", Delta: "plan"},
		{Type: responsesEventOutputTextDelta, OutputIndex: kitutil.GetPointer(1), ItemID: "msg_diag", Delta: "hello"},
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_diag", Item: done},
	}
	output := runStreamEvents(t, state, events)

	// 状态③:无 signature_delta,块序列仍合法。
	require.Empty(t, signatureDeltas(output))

	// 容量未满且仅一个新 drop → 诊断计数 +1,truncated=false。
	require.False(t, state.ConversionDiagnosticsTruncated())
	diags := state.ConversionDiagnostics()
	require.Len(t, diags, 1)
	assert.Equal(t, signatureDroppedAfterEarlyClose, diags[0].Code)
	assert.Equal(t, types.ConversionDiagnosticWarning, diags[0].Severity)
	assert.Equal(t, types.RelayFormat(types.RelayFormatOpenAIResponses), diags[0].From)
	assert.Equal(t, types.RelayFormat(types.RelayFormatClaude), diags[0].To)
	assert.Equal(t, "items/0/reasoning/encrypted_content", diags[0].Path)

	// 同一 item 重复 done:至多一次记录尝试,不新增诊断条目。
	repeated, _, err := feedStreamEvents(t, state, []*dto.ResponsesStreamResponse{
		{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(0), ItemID: "rs_diag", Item: done},
	})
	require.NoError(t, err)
	require.Empty(t, repeated)
	require.Len(t, state.ConversionDiagnostics(), 1)
}

func TestResponsesToClaudeStream_EarlyCloseDiagnosticTruncated(t *testing.T) {
	// 已占满 32 条 distinct 诊断后,后续 drop 不新增条目但 truncated=true。
	// 每个 item 用唯一 output_index 触发一次 distinct 状态③记录,直至达到上限。
	state := NewResponsesToClaudeStreamState("", "")
	for i := 0; i < 33; i++ {
		id := fmt.Sprintf("rs_trunc_%d", i)
		added := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: id, EncryptedContent: ""}
		done := &dto.ResponsesOutput{Type: responsesOutputTypeReasoning, ID: id, EncryptedContent: "late_sig"}
		items := []*dto.ResponsesStreamResponse{
			{Type: responsesEventOutputItemAdded, OutputIndex: kitutil.GetPointer(i), ItemID: id, Item: added},
			{Type: responsesEventReasoningSummaryDelta, OutputIndex: kitutil.GetPointer(i), ItemID: id, Delta: "plan"},
			// text 边界用与 reasoning 不同的 index,触发提前关闭且不误判为 thinking 块。
			{Type: responsesEventOutputTextDelta, OutputIndex: kitutil.GetPointer(1000 + i), ItemID: fmt.Sprintf("msg_trunc_%d", i), Delta: "x"},
			{Type: responsesEventOutputItemDone, OutputIndex: kitutil.GetPointer(i), ItemID: id, Item: done},
		}
		runStreamEvents(t, state, items)
	}

	// 本类别 distinct drop 数已超过 32(溢出信号 conversionDiagnosticsTruncated 置位)。
	// 为让 root 侧 RecordConversionDiagnostics 能观察到第 33 条溢出(从而按全请求 32 条
	// 全局上限收紧并置 admin_info.conversion_diagnostics_truncated),转换器不再本地丢弃:
	// 全部 distinct 条目经 ConversionDiagnostics() 交付 root——此处共 33 条,每条以唯一
	// output_index 区分。
	require.True(t, state.ConversionDiagnosticsTruncated())
	diags := state.ConversionDiagnostics()
	require.Len(t, diags, 33)
	for _, diag := range diags {
		assert.Equal(t, signatureDroppedAfterEarlyClose, diag.Code)
	}
}

// rawJSON 构造 json.RawMessage 帮助函数,失败即 fatal。
func rawJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := kitutil.Marshal(value)
	require.NoError(t, err)
	return json.RawMessage(raw)
}

// webSearchServerToolUseID 返回输出中 server_tool_use content_block_start 的 id;不存在返回 ""。
func webSearchServerToolUseID(output []*dto.ClaudeResponse) string {
	for _, r := range output {
		if r != nil && r.Type == "content_block_start" && r.ContentBlock != nil && r.ContentBlock.Type == "server_tool_use" {
			return r.ContentBlock.Id
		}
	}
	return ""
}

func feedStreamEvents(t *testing.T, state *ResponsesToClaudeStreamState, events []*dto.ResponsesStreamResponse) ([]*dto.ClaudeResponse, *dto.Usage, error) {
	t.Helper()
	var output []*dto.ClaudeResponse
	for _, event := range events {
		converted, usage, err := state.ConvertChunk(event, 9)
		if err != nil {
			return output, usage, err
		}
		output = append(output, converted...)
	}
	return output, state.Usage, nil
}

func runStreamEvents(t *testing.T, state *ResponsesToClaudeStreamState, events []*dto.ResponsesStreamResponse) []*dto.ClaudeResponse {
	t.Helper()
	var output []*dto.ClaudeResponse
	for _, event := range events {
		converted, _, err := state.ConvertChunk(event, 9)
		require.NoError(t, err)
		output = append(output, converted...)
	}
	return output
}

func signatureDeltas(responses []*dto.ClaudeResponse) []string {
	var out []string
	for _, r := range responses {
		if r == nil || r.Type != "content_block_delta" || r.Delta == nil || r.Delta.Type != "signature_delta" {
			continue
		}
		out = append(out, r.Delta.Signature)
	}
	return out
}

func findSignatureDeltaResponse(responses []*dto.ClaudeResponse) *dto.ClaudeResponse {
	for _, r := range responses {
		if r != nil && r.Type == "content_block_delta" && r.Delta != nil && r.Delta.Type == "signature_delta" {
			return r
		}
	}
	return nil
}

// describeBlockSequence 把 content_block 事件折叠为顺序字符串(start/delta/stop,带
// 块 index 与 delta 子类型),便于断言帧序与块合法性。
func describeBlockSequence(responses []*dto.ClaudeResponse) []string {
	var out []string
	for _, r := range responses {
		if r == nil || r.Index == nil {
			continue
		}
		switch r.Type {
		case "content_block_start":
			ct := ""
			if r.ContentBlock != nil {
				ct = r.ContentBlock.Type
			}
			out = append(out, fmt.Sprintf("start:%d:%s", *r.Index, ct))
		case "content_block_delta":
			dt := ""
			if r.Delta != nil {
				dt = r.Delta.Type
			}
			out = append(out, fmt.Sprintf("delta:%d:%s", *r.Index, dt))
		case "content_block_stop":
			out = append(out, fmt.Sprintf("stop:%d", *r.Index))
		}
	}
	return out
}

func responsesOfType(responses []*dto.ClaudeResponse, responseType string) []*dto.ClaudeResponse {
	filtered := make([]*dto.ClaudeResponse, 0)
	for _, response := range responses {
		if response != nil && response.Type == responseType {
			filtered = append(filtered, response)
		}
	}
	return filtered
}

func joinedClaudeDeltas(responses []*dto.ClaudeResponse, deltaType string) string {
	result := ""
	for _, response := range responses {
		if response == nil || response.Type != "content_block_delta" || response.Delta == nil || response.Delta.Type != deltaType {
			continue
		}
		switch deltaType {
		case "thinking_delta":
			if response.Delta.Thinking != nil {
				result += *response.Delta.Thinking
			}
		case "text_delta":
			if response.Delta.Text != nil {
				result += *response.Delta.Text
			}
		case "input_json_delta":
			if response.Delta.PartialJson != nil {
				result += *response.Delta.PartialJson
			}
		}
	}
	return result
}

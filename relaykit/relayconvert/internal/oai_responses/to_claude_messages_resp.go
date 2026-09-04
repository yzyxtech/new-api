package oairesponses

import (
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/reasonmap"
	sharedclaude "github.com/QuantumNous/new-api/relaykit/relayconvert/internal/shared/claude"
	kitutil "github.com/QuantumNous/new-api/relaykit/relayconvert/kitutil"
)

func ResponsesResponseToClaudeMessagesResponse(resp *dto.OpenAIResponsesResponse) (*dto.ClaudeResponse, *dto.Usage, error) {
	if resp == nil {
		return nil, nil, errors.New("response is nil")
	}

	usage := UsageFromResponsesUsage(resp.Usage)
	claudeResponse := &dto.ClaudeResponse{
		Id:    resp.ID,
		Type:  "message",
		Role:  "assistant",
		Model: resp.Model,
		Usage: sharedclaude.UsageFromOpenAI(usage),
	}
	sawToolCall := false
	webSearch := &responsesNonStreamWebSearch{}
	for index := range resp.Output {
		output := resp.Output[index]
		if output.Type == responsesOutputTypeMessage && output.Role != "" && output.Role != "assistant" {
			continue
		}
		switch output.Type {
		case responsesOutputTypeReasoning:
			// 终态 reasoning item 的 encrypted_content 映射为 thinking 块 signature 字段
			// (D-6);正文/摘要沿用既有 reasoning_text 映射。二者皆空则不产出。
			thinking := reasoningOutputText(&output)
			if thinking != "" || output.EncryptedContent != "" {
				block := dto.ClaudeMediaMessage{Type: "thinking", Signature: output.EncryptedContent}
				if thinking != "" {
					block.Thinking = kitutil.GetPointer(thinking)
				}
				claudeResponse.Content = append(claudeResponse.Content, block)
			}
		case dto.BuildInCallWebSearchCall:
			claudeResponse.Content = webSearch.appendBlocks(claudeResponse.Content, &output)
		case responsesOutputTypeMessage:
			for _, content := range output.Content {
				if content.Type != "output_text" {
					continue
				}
				block := dto.ClaudeMediaMessage{Type: "text", Text: kitutil.GetPointer(content.Text)}
				if citations := responsesAnnotationsToClaude(content.Annotations, content.Text); len(citations) > 0 {
					block.Citations, _ = kitutil.Marshal(citations)
				}
				claudeResponse.Content = append(claudeResponse.Content, block)
			}
		case responsesOutputTypeFunctionCall, responsesOutputTypeCustomToolCall:
			sawToolCall = true
			callID := strings.TrimSpace(output.CallId)
			if callID == "" {
				callID = strings.TrimSpace(output.ID)
			}
			claudeResponse.Content = append(claudeResponse.Content, dto.ClaudeMediaMessage{
				Type:  "tool_use",
				Id:    callID,
				Name:  output.Name,
				Input: responsesArgumentsToClaudeInput(output.ArgumentsString()),
			})
		}
	}
	if len(claudeResponse.Content) == 0 {
		claudeResponse.Content = []dto.ClaudeMediaMessage{{Type: "text", Text: kitutil.GetPointer("")}}
	}
	claudeResponse.StopReason = responsesClaudeStopReason(resp, sawToolCall)
	return claudeResponse, usage, nil
}

func responsesArgumentsToClaudeInput(arguments string) map[string]any {
	input := make(map[string]any)
	if strings.TrimSpace(arguments) == "" {
		return input
	}
	if err := kitutil.Unmarshal([]byte(arguments), &input); err == nil && input != nil {
		return input
	}
	return map[string]any{"input": arguments}
}

func responsesClaudeStopReason(resp *dto.OpenAIResponsesResponse, sawToolCall bool) string {
	if finishReason, ok := ResponsesFinishReasonFromStatus(resp); ok {
		return reasonmap.OpenAIFinishReasonToClaudeStopReason(finishReason)
	}
	if sawToolCall {
		return "tool_use"
	}
	return "end_turn"
}

func responsesAnnotationsToClaude(annotations []interface{}, text string) []json.RawMessage {
	citations := make([]json.RawMessage, 0, len(annotations))
	for _, rawAnnotation := range annotations {
		annotation, err := kitutil.Any2Type[map[string]any](rawAnnotation)
		if err != nil || strings.TrimSpace(kitutil.Interface2String(annotation["type"])) != "url_citation" {
			continue
		}
		citation := annotation
		if nested, ok := annotation["url_citation"].(map[string]any); ok {
			citation = nested
		}
		url := strings.TrimSpace(kitutil.Interface2String(citation["url"]))
		if url == "" {
			continue
		}
		converted := map[string]any{
			"type":  "web_search_result_location",
			"url":   url,
			"title": strings.TrimSpace(kitutil.Interface2String(citation["title"])),
		}
		if citedText := kitutil.Interface2String(citation["cited_text"]); citedText != "" {
			converted["cited_text"] = citedText
		} else if citedText := responsesCitedText(text, citation); citedText != "" {
			converted["cited_text"] = citedText
		}
		if encryptedIndex := kitutil.Interface2String(citation["encrypted_index"]); encryptedIndex != "" {
			converted["encrypted_index"] = encryptedIndex
		}
		if converted["title"] == "" {
			delete(converted, "title")
		}
		encoded, err := kitutil.Marshal(converted)
		if err == nil {
			citations = append(citations, encoded)
		}
	}
	return citations
}

func responsesCitedText(text string, citation map[string]any) string {
	start, startOK := responsesAnnotationIndex(citation["start_index"])
	end, endOK := responsesAnnotationIndex(citation["end_index"])
	if !startOK || !endOK || start < 0 || end <= start || end > utf8.RuneCountInString(text) {
		return ""
	}
	runes := []rune(text)
	return string(runes[start:end])
}

func responsesAnnotationIndex(value any) (int, bool) {
	switch number := value.(type) {
	case float64:
		return int(number), number >= 0 && number == float64(int(number))
	case int:
		return number, number >= 0
	case json.Number:
		parsed, err := number.Int64()
		return int(parsed), err == nil && parsed >= 0
	default:
		return 0, false
	}
}

// responsesNonStreamWebSearch 承载非流式 web_search_call 映射的去重与上一已知 ID 回退状态。
// 非流式无根事件,故不使用 ResponsesStreamResponse.ItemID(D-6)。
type responsesNonStreamWebSearch struct {
	seen            map[string]struct{}
	lastWebSearchID string
}

// appendBlocks 把 web_search_call item 映射为 server_tool_use + web_search_tool_result
// 一对块(对齐基线 non-stream 形态 appendCodexWebSearchNonStreamBlocks)。tool_use_id 回退链
// = item.id → item.output_item_id → item.call_id → 上一已知 web_search ID;全空不产出;
// 按最终 ID 去重,每个 ID 各至多产出一对。query 与 results 皆空时也不产出(对齐基线 guard)。
func (st *responsesNonStreamWebSearch) appendBlocks(content []dto.ClaudeMediaMessage, item *dto.ResponsesOutput) []dto.ClaudeMediaMessage {
	toolUseID := st.webSearchToolUseID(item)
	if toolUseID == "" {
		return content
	}
	if st.seen == nil {
		st.seen = make(map[string]struct{})
	}
	if _, seen := st.seen[toolUseID]; seen {
		return content
	}
	query := nonStreamWebSearchQuery(item)
	resultContent := nonStreamWebSearchResultContent(item)
	if query == "" && len(resultContent) == 0 {
		return content
	}
	if resultContent == nil {
		resultContent = []any{}
	}
	content = append(content, dto.ClaudeMediaMessage{
		Type:  "server_tool_use",
		Id:    toolUseID,
		Name:  "web_search",
		Input: nonStreamWebSearchInput(query),
	})
	content = append(content, dto.ClaudeMediaMessage{
		Type:      "web_search_tool_result",
		ToolUseId: toolUseID,
		Content:   resultContent,
	})
	st.seen[toolUseID] = struct{}{}
	st.lastWebSearchID = toolUseID
	return content
}

// webSearchToolUseID 解析非流式回退链:item.id → item.output_item_id → item.call_id →
// 上一已知 web_search ID。全部为空返回空串(不产出,避免不可关联结果)。
func (st *responsesNonStreamWebSearch) webSearchToolUseID(item *dto.ResponsesOutput) string {
	for _, id := range []string{item.ID, item.OutputItemID, item.CallId} {
		if id = strings.TrimSpace(id); id != "" {
			return id
		}
	}
	return st.lastWebSearchID
}

// nonStreamWebSearchQuery 从 web_search_call item 的 action 对象提取查询词(基线 action.query)。
func nonStreamWebSearchQuery(item *dto.ResponsesOutput) string {
	if item == nil || len(item.Action) == 0 {
		return ""
	}
	var action struct {
		Query string `json:"query"`
	}
	if err := kitutil.Unmarshal(item.Action, &action); err != nil {
		return ""
	}
	return strings.TrimSpace(action.Query)
}

// nonStreamWebSearchResultContent 把 web_search_call item 的 results 数组映射为 Claude
// web_search_result 块数组:url 取 result.url(trim 空则跳过该 result);title 取 result.title
// (trim 空则回退为 url);page_age 恒为 nil(null)。
func nonStreamWebSearchResultContent(item *dto.ResponsesOutput) []any {
	if item == nil || len(item.Results) == 0 {
		return nil
	}
	var results []struct {
		URL   string `json:"url"`
		Title string `json:"title"`
	}
	if err := kitutil.Unmarshal(item.Results, &results); err != nil {
		return nil
	}
	var blocks []any
	for _, result := range results {
		url := strings.TrimSpace(result.URL)
		if url == "" {
			continue
		}
		title := strings.TrimSpace(result.Title)
		if title == "" {
			title = url
		}
		blocks = append(blocks, map[string]any{
			"type":     "web_search_result",
			"title":    title,
			"url":      url,
			"page_age": nil,
		})
	}
	return blocks
}

// nonStreamWebSearchInput 构造 server_tool_use 的 input:query 非空时内嵌,否则空对象。
func nonStreamWebSearchInput(query string) any {
	if query == "" {
		return map[string]any{}
	}
	return map[string]any{"query": query}
}

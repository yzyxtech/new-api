package claudemessages

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/convmeta"
	kitutil "github.com/QuantumNous/new-api/relaykit/relayconvert/kitutil"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/reasoning"
)

func ClaudeMessagesRequestToOpenAIResponses(claudeRequest dto.ClaudeRequest, info convmeta.Meta) (*dto.OpenAIResponsesRequest, error) {
	if strings.TrimSpace(claudeRequest.Model) == "" {
		return nil, errors.New("model is required")
	}

	input, err := claudeMessagesToResponsesInput(claudeRequest.Messages)
	if err != nil {
		return nil, err
	}
	instructions, err := claudeSystemToResponsesInstructions(&claudeRequest)
	if err != nil {
		return nil, err
	}
	tools, err := claudeToolsToResponsesTools(claudeRequest.Tools)
	if err != nil {
		return nil, err
	}
	toolChoice, parallelToolCalls, err := claudeToolChoiceToResponses(claudeRequest.ToolChoice, claudeRequest.Tools)
	if err != nil {
		return nil, err
	}

	// Claude context_management is an object containing protocol-specific edit
	// strategies. Responses expects an array of compaction entries, so copying
	// the raw Claude value would produce an invalid upstream request.
	responsesRequest := &dto.OpenAIResponsesRequest{
		Model:             claudeRequest.Model,
		Input:             input,
		Instructions:      instructions,
		Metadata:          append(json.RawMessage(nil), claudeRequest.Metadata...),
		ServiceTier:       claudeRequest.ServiceTier,
		Stream:            claudeRequest.Stream,
		Temperature:       claudeRequest.Temperature,
		Tools:             tools,
		ToolChoice:        toolChoice,
		ParallelToolCalls: parallelToolCalls,
		TopP:              claudeRequest.TopP,
	}
	if info != nil && !convmeta.OptionsOf(info).OpenRouterDialect {
		// Keep the outgoing -thinking suffix so a cascaded downstream new-api
		// can recover reasoning intent from the model name. This is an
		// emission-side policy, not converter-side suffix parsing.
		thinkingSuffix := "-thinking"
		if strings.HasSuffix(info.GetOriginModelName(), thinkingSuffix) && !strings.HasSuffix(responsesRequest.Model, thinkingSuffix) {
			responsesRequest.Model += thinkingSuffix
		}
	}
	if claudeRequest.MaxTokens != nil {
		maxOutputTokens := *claudeRequest.MaxTokens
		responsesRequest.MaxOutputTokens = &maxOutputTokens
	} else if claudeRequest.MaxTokensToSample != nil {
		maxOutputTokens := *claudeRequest.MaxTokensToSample
		responsesRequest.MaxOutputTokens = &maxOutputTokens
	}

	textFormat, err := claudeResponsesTextFormat(claudeRequest.OutputConfig)
	if err != nil {
		return nil, err
	}
	if len(textFormat) > 0 {
		responsesRequest.Text = textFormat
	}

	reasoningIntent, effectiveEffort, err := claudeRequestReasoningIntent(&claudeRequest, info)
	if err != nil {
		return nil, reasoning.AsClientError(err)
	}
	if err := reasoning.ApplyToOpenAIResponses(responsesRequest, reasoningIntent); err != nil {
		return nil, reasoning.AsClientError(err)
	}
	// The "auto" intent from a -1 budget cannot be represented in the shared
	// intent vocabulary, so ApplyToOpenAIResponses normalized it into a
	// budget-derived fallback level. Re-assert the baseline -1 → auto wire
	// effort so the emitted reasoning.effort matches the baseline semantics.
	if effectiveEffort == claudeEffortAuto && responsesRequest.Reasoning != nil {
		responsesRequest.Reasoning.Effort = string(claudeEffortAuto)
	}
	if info != nil && effectiveEffort != "" {
		info.SetReasoningEffort(string(effectiveEffort))
	}

	return responsesRequest, nil
}

func claudeRequestReasoningIntent(claudeRequest *dto.ClaudeRequest, info convmeta.Meta) (reasoning.Intent, reasoning.Effort, error) {
	reasoningIntent, err := reasoning.FromClaude(claudeRequest)
	if err != nil {
		return reasoning.Intent{}, "", err
	}
	sourceModel := claudeRequest.Model
	if info != nil && info.GetOriginModelName() != "" {
		sourceModel = info.GetOriginModelName()
	}
	if suffix := reasoning.IntentFromState(convmeta.ReasoningStateOf(info)); !suffix.IsEmpty() {
		reasoningIntent, err = reasoning.MergeExplicitAndSuffix(reasoningIntent, suffix, sourceModel)
		if err != nil {
			return reasoning.Intent{}, "", err
		}
	}
	reasoningIntent = reasoning.ResolveClaudeDefault(sourceModel, reasoningIntent)
	// Align a numeric thinking budget (no explicit effort) to the baseline
	// budget→level vocabulary. Without this, the shared pipeline's budget
	// derivation caps at high and never emits xhigh for very large budgets.
	// A -1 budget maps to the "auto" level, which the shared intent vocabulary
	// cannot carry (its ParseEffort would reject it), so that wire-level intent
	// is reported through the returned effective effort without being written
	// into Intent.Effort; the caller re-applies it to the emitted request.
	effort := reasoning.EffectiveEffort(reasoningIntent)
	if reasoningIntent.Effort == "" && reasoningIntent.BudgetTokens != nil {
		if level, ok := claudeBudgetToLevel(*reasoningIntent.BudgetTokens); ok {
			if level != claudeEffortAuto {
				reasoningIntent.Effort = level
			}
			effort = level
		}
	}
	return reasoningIntent, effort, nil
}

// claudeEffortAuto is the baseline "auto" level used for a -1 budget (the model
// decides). The shared reasoning vocabulary has no auto constant, so this local
// alias reproduces the baseline sign handling for a -1 budget without adding a
// reasoning package export.
const claudeEffortAuto reasoning.Effort = "auto"

// claudeBudgetToLevel maps a thinking budget_tokens value to the baseline
// ConvertBudgetToLevel tiers and mirrors its negative-budget handling: -1 maps
// to "auto", and any value below -1 is invalid and produces no tier, leaving the
// existing intent pipeline to keep its default handling. It is applied only when
// the request carries a numeric budget without an explicit effort, so an
// output_config.effort stays the source of truth for adaptive thinking. Callers
// must treat the "auto" result as a wire-only level: it is reported out-of-band
// (never written into Intent.Effort) because the shared reasoning vocabulary has
// no "auto" value that its validation would accept.
func claudeBudgetToLevel(budget int) (reasoning.Effort, bool) {
	switch {
	case budget < -1:
		return "", false
	case budget == -1:
		return claudeEffortAuto, true
	case budget == 0:
		return reasoning.EffortNone, true
	case budget <= 512:
		return reasoning.EffortMinimal, true
	case budget <= 1024:
		return reasoning.EffortLow, true
	case budget <= 8192:
		return reasoning.EffortMedium, true
	case budget <= 24576:
		return reasoning.EffortHigh, true
	default:
		return reasoning.EffortXHigh, true
	}
}

func claudeSystemToResponsesInstructions(request *dto.ClaudeRequest) (json.RawMessage, error) {
	if request == nil || request.System == nil {
		return nil, nil
	}
	if request.IsStringSystem() {
		if isClaudeCodeAttributionSystemText(request.GetStringSystem()) {
			return nil, nil
		}
		return kitutil.Marshal(request.GetStringSystem())
	}

	var instructions strings.Builder
	systemBlocks, err := kitutil.Any2Type[[]dto.ClaudeMediaMessage](request.System)
	if err != nil {
		return nil, fmt.Errorf("invalid Claude system content: %w", err)
	}
	for _, block := range systemBlocks {
		if isClaudeCodeAttributionSystemText(block.GetText()) {
			continue
		}
		if block.Type == "text" || block.Type == "input_text" || block.Type == "" {
			instructions.WriteString(block.GetText())
		}
	}
	if instructions.Len() == 0 {
		return nil, nil
	}
	return kitutil.Marshal(instructions.String())
}

// claudeCodeAttributionSystemPrefix is the leading marker of the Claude Code
// per-request billing and prompt-fingerprint attribution block.
const claudeCodeAttributionSystemPrefix = "x-anthropic-billing-header:"

// isClaudeCodeAttributionSystemText reports whether text is a Claude Code
// attribution block. Upstream Responses treats such a block as ordinary prompt
// text, so it must be dropped rather than forwarded into instructions.
func isClaudeCodeAttributionSystemText(text string) bool {
	text = strings.TrimLeftFunc(text, unicode.IsSpace)
	return strings.HasPrefix(text, claudeCodeAttributionSystemPrefix)
}

// alignClaudeToolResults mirrors the baseline AlignClaudeToolResults within a
// single user message: it splits tool_result blocks from the other blocks and,
// only on a complete one-to-one match against the pending tool-use call order,
// reorders the block list results-first (the tool results in call order, then
// the other parts). Any less-than-complete match returns the original block
// order unchanged, so no part is silently dropped.
func alignClaudeToolResults(blocks []dto.ClaudeMediaMessage, toolUseIDs []string) []dto.ClaudeMediaMessage {
	if len(toolUseIDs) == 0 {
		return blocks
	}
	toolResults := make([]dto.ClaudeMediaMessage, 0, len(toolUseIDs))
	otherParts := make([]dto.ClaudeMediaMessage, 0, len(blocks))
	for _, block := range blocks {
		if block.Type == "tool_result" {
			toolResults = append(toolResults, block)
			continue
		}
		otherParts = append(otherParts, block)
	}
	if len(toolResults) != len(toolUseIDs) {
		return blocks
	}

	used := make([]bool, len(toolResults))
	ordered := make([]dto.ClaudeMediaMessage, 0, len(blocks))
	for _, callID := range toolUseIDs {
		matched := -1
		for i := range toolResults {
			if !used[i] && callID != "" && toolResults[i].ToolUseId == callID {
				matched = i
				break
			}
		}
		if matched < 0 {
			return blocks
		}
		used[matched] = true
		ordered = append(ordered, toolResults[matched])
	}
	return append(ordered, otherParts...)
}

func claudeMessagesToResponsesInput(messages []dto.ClaudeMessage) (json.RawMessage, error) {
	input := make([]map[string]any, 0, len(messages))
	var pendingToolUseIDs []string
	for messageIndex := range messages {
		message := messages[messageIndex]
		role := strings.TrimSpace(message.Role)
		if role == "" {
			continue
		}
		if message.IsStringContent() {
			// A string-content message is still a real turn boundary: it breaks
			// any pending tool-use association from the preceding assistant
			// turn, so a later tool_result must never align against calls that
			// predate it. Pending calls are therefore cleared here too, exactly
			// as in the block-content path below.
			pendingToolUseIDs = nil
			input = append(input, map[string]any{
				"role":    role,
				"content": message.GetStringContent(),
			})
			continue
		}

		blocks, err := message.ParseContent()
		if err != nil {
			return nil, fmt.Errorf("messages[%d].content: %w", messageIndex, err)
		}

		// A user message whose tool_result blocks correspond to the tool-use
		// calls of the preceding assistant turn is aligned before item
		// generation. Alignment follows the baseline AlignClaudeToolResults: a
		// complete one-to-one match reorders results ahead of the other content
		// (in call order), anything less leaves the blocks in their original
		// order. Pending calls are reset per message so one turn's results never
		// bleed into the next.
		if role == "user" && len(pendingToolUseIDs) > 0 {
			blocks = alignClaudeToolResults(blocks, pendingToolUseIDs)
		}
		pendingToolUseIDs = nil

		contentParts := make([]map[string]any, 0, len(blocks))
		flushContent := func() {
			if len(contentParts) == 0 {
				return
			}
			input = append(input, map[string]any{
				"role":    role,
				"content": contentParts,
			})
			contentParts = nil
		}

		for blockIndex := range blocks {
			block := blocks[blockIndex]
			switch block.Type {
			case "text", "input_text":
				partType := "input_text"
				if role == "assistant" {
					partType = "output_text"
				}
				contentParts = append(contentParts, map[string]any{
					"type": partType,
					"text": block.GetText(),
				})
			case "image":
				if source := claudeSourceURL(block.Source); source != "" {
					contentParts = append(contentParts, map[string]any{
						"type":      "input_image",
						"image_url": source,
					})
				}
			case "document":
				if source := claudeSourceURL(block.Source); source != "" {
					contentParts = append(contentParts, map[string]any{
						"type":      "input_file",
						"file_data": source,
					})
				}
			case "thinking":
				if strings.TrimSpace(block.Signature) == "" {
					// Signature-less thinking adds no upstream value and is
					// dropped. The raw payload only carries reasoning display
					// text that Responses cannot reconstruct.
					continue
				}
				// A signed thinking block must reach upstream as a standalone
				// top-level reasoning replay item so the signature round-trips
				// verbatim. It forms a flush boundary: accumulated parts are
				// emitted first, then the reasoning item, and later parts start
				// a brand-new message item.
				flushContent()
				input = append(input, map[string]any{
					"type":              "reasoning",
					"summary":           []any{},
					"content":           nil,
					"encrypted_content": block.Signature,
				})
			case "redacted_thinking":
				// The block payload lives in `data`, which carries no
				// signature, so nothing can be faithfully replayed upstream.
				continue
			case "tool_use":
				flushContent()
				arguments, err := kitutil.Marshal(block.Input)
				if err != nil {
					return nil, fmt.Errorf("messages[%d].content[%d].input: %w", messageIndex, blockIndex, err)
				}
				if block.Input == nil {
					arguments = []byte("{}")
				}
				id := strings.TrimSpace(block.Id)
				if id != "" {
					pendingToolUseIDs = append(pendingToolUseIDs, id)
				}
				input = append(input, map[string]any{
					"type":      "function_call",
					"call_id":   block.Id,
					"name":      block.Name,
					"arguments": string(arguments),
				})
			case "tool_result":
				// A tool_result is a tool item boundary: it never merges into
				// the message item. Because alignment placed the results first
				// in call order for a matched user message, each emitted
				// function_call_output precedes the message item that carries
				// this turn's other content; an unmatched message keeps its
				// original order so the emitted items follow the input verbatim.
				flushContent()
				output, err := claudeToolResultToResponsesOutput(block.Content)
				if err != nil {
					return nil, fmt.Errorf("messages[%d].content[%d].content: %w", messageIndex, blockIndex, err)
				}
				input = append(input, map[string]any{
					"type":    "function_call_output",
					"call_id": block.ToolUseId,
					"output":  output,
				})
			}
		}
		flushContent()
	}
	return kitutil.Marshal(input)
}

func claudeToolsToResponsesTools(value any) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := kitutil.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("invalid Claude tools: %w", err)
	}
	var rawTools []json.RawMessage
	if err := kitutil.Unmarshal(raw, &rawTools); err != nil {
		return nil, fmt.Errorf("invalid Claude tools: %w", err)
	}
	converted := make([]map[string]any, 0, len(rawTools))
	for _, rawTool := range rawTools {
		if webSearch, ok := claudeWebSearchTool(rawTool); ok {
			converted = append(converted, webSearch)
			continue
		}
		var tool dto.Tool
		if err := kitutil.Unmarshal(rawTool, &tool); err != nil {
			return nil, fmt.Errorf("invalid Claude tool: %w", err)
		}
		function := map[string]any{
			"type":        "function",
			"name":        tool.Name,
			"description": tool.Description,
			"parameters":  tool.InputSchema,
		}
		if tool.Strict != nil {
			function["strict"] = *tool.Strict
		}
		converted = append(converted, function)
	}
	return kitutil.Marshal(converted)
}

// claudeWebSearchTool maps a hosted Claude web-search tool definition to its
// Responses `{"type":"web_search"}` equivalent. Recognition is purely by the
// tool's `type` field (the two currently known server versions), so a regular
// function tool that merely shares a web-search-like name is never mistaken for
// one. The tool name is intentionally dropped: Responses has no web_search
// name slot, and the Claude side allows a custom name.
func claudeWebSearchTool(raw json.RawMessage) (map[string]any, bool) {
	var tool map[string]any
	if err := kitutil.Unmarshal(raw, &tool); err != nil {
		return nil, false
	}
	switch strings.TrimSpace(kitutil.Interface2String(tool["type"])) {
	case "web_search_20250305", "web_search_20260209":
	default:
		return nil, false
	}
	result := map[string]any{"type": "web_search"}
	if domains := claudeStringSlice(tool["allowed_domains"]); len(domains) > 0 {
		result["filters"] = map[string]any{"allowed_domains": domains}
	}
	if location, ok := tool["user_location"].(map[string]any); ok && len(location) > 0 {
		result["user_location"] = location
	}
	return result, true
}

// claudeStringSlice coerces a JSON string array value into a trimmed string
// slice, discarding empty entries.
func claudeStringSlice(value any) []string {
	list, ok := value.([]any)
	if !ok {
		return nil
	}
	items := make([]string, 0, len(list))
	for _, item := range list {
		if e := strings.TrimSpace(kitutil.Interface2String(item)); e != "" {
			items = append(items, e)
		}
	}
	return items
}

// claudeChoiceNamesWebSearch reports whether the tool named by a named
// tool_choice is a Claude web-search hosted tool. Recognition requires both a
// name match and a web-search `type`, so a function sharing the name is ignored.
func claudeChoiceNamesWebSearch(tools any, name string) bool {
	if tools == nil || strings.TrimSpace(name) == "" {
		return false
	}
	raw, err := kitutil.Marshal(tools)
	if err != nil {
		return false
	}
	var rawTools []json.RawMessage
	if err := kitutil.Unmarshal(raw, &rawTools); err != nil {
		return false
	}
	for _, rawTool := range rawTools {
		if _, ok := claudeWebSearchTool(rawTool); !ok {
			continue
		}
		var tool map[string]any
		if err := kitutil.Unmarshal(rawTool, &tool); err != nil {
			continue
		}
		if strings.TrimSpace(kitutil.Interface2String(tool["name"])) == strings.TrimSpace(name) {
			return true
		}
	}
	return false
}

func claudeToolChoiceToResponses(value any, tools any) (json.RawMessage, json.RawMessage, error) {
	if value == nil {
		return nil, nil, nil
	}
	choice, err := kitutil.Any2Type[dto.ClaudeToolChoice](value)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid Claude tool_choice: %w", err)
	}

	var converted any
	switch choice.Type {
	case "", "auto":
		converted = "auto"
	case "any":
		converted = "required"
	case "none":
		converted = "none"
	case "tool":
		if claudeChoiceNamesWebSearch(tools, choice.Name) {
			converted = map[string]any{"type": "web_search"}
		} else {
			converted = map[string]any{"type": "function", "name": choice.Name}
		}
	default:
		return nil, nil, fmt.Errorf("unsupported Claude tool_choice type %q", choice.Type)
	}
	toolChoice, err := kitutil.Marshal(converted)
	if err != nil {
		return nil, nil, err
	}

	var parallelToolCalls json.RawMessage
	if choice.DisableParallelToolUse && choice.Type != "none" {
		parallelToolCalls, err = kitutil.Marshal(false)
		if err != nil {
			return nil, nil, err
		}
	}
	return toolChoice, parallelToolCalls, nil
}

func claudeToolResultToResponsesOutput(content any) (any, error) {
	if content == nil {
		return "", nil
	}
	if text, ok := content.(string); ok {
		return text, nil
	}
	blocks, err := kitutil.Any2Type[[]dto.ClaudeMediaMessage](content)
	if err != nil {
		return content, nil
	}
	parts := make([]map[string]any, 0, len(blocks))
	for _, block := range blocks {
		switch block.Type {
		case "text", "input_text":
			parts = append(parts, map[string]any{"type": "input_text", "text": block.GetText()})
		case "image":
			if source := claudeSourceURL(block.Source); source != "" {
				parts = append(parts, map[string]any{"type": "input_image", "image_url": source})
			}
		case "document":
			if source := claudeSourceURL(block.Source); source != "" {
				parts = append(parts, map[string]any{"type": "input_file", "file_data": source})
			}
		}
	}
	if len(parts) == 0 {
		return content, nil
	}
	return parts, nil
}

func claudeSourceURL(source *dto.ClaudeMessageSource) string {
	if source == nil {
		return ""
	}
	if strings.TrimSpace(source.Url) != "" {
		return source.Url
	}
	data := kitutil.Interface2String(source.Data)
	if data == "" {
		return ""
	}
	if strings.HasPrefix(data, "data:") {
		return data
	}
	return fmt.Sprintf("data:%s;base64,%s", source.MediaType, data)
}

// claudeResponsesTextFormat maps Claude output_config.format (json_schema) to
// the Responses `text.format` object, mirroring the baseline rules. The mapping
// applies only when output_config.format is a JSON object whose `type` is
// "json_schema" and whose `schema` is a JSON object. `name` defaults to the
// baseline value when absent, `strict` defaults to true unless explicitly set
// false, and `schema` is passed through verbatim. Returns nil when the gate does
// not hold.
func claudeResponsesTextFormat(outputConfig json.RawMessage) (json.RawMessage, error) {
	if len(outputConfig) == 0 {
		return nil, nil
	}
	var cfg struct {
		Format json.RawMessage `json:"format"`
	}
	if err := kitutil.Unmarshal(outputConfig, &cfg); err != nil {
		return nil, fmt.Errorf("invalid Claude output_config: %w", err)
	}
	if len(cfg.Format) == 0 {
		return nil, nil
	}
	var format struct {
		Type   string          `json:"type"`
		Name   string          `json:"name"`
		Strict *bool           `json:"strict"`
		Schema json.RawMessage `json:"schema"`
	}
	if err := kitutil.Unmarshal(cfg.Format, &format); err != nil {
		return nil, fmt.Errorf("invalid Claude output_config.format: %w", err)
	}
	if strings.TrimSpace(format.Type) != "json_schema" || !isJSONObject(format.Schema) {
		return nil, nil
	}

	// Mirror the baseline: only an empty string is replaced by the default. A
	// non-empty name (including leading/trailing whitespace or whitespace-only)
	// is passed through verbatim, because the baseline writes `format.name`
	// unchanged whenever it is non-empty.
	name := "cli_proxy_structured_output"
	if format.Name != "" {
		name = format.Name
	}
	strict := true
	if format.Strict != nil && !*format.Strict {
		strict = false
	}
	formatJSON := map[string]any{
		"type":   "json_schema",
		"name":   name,
		"strict": strict,
		"schema": format.Schema,
	}
	textJSON, err := kitutil.Marshal(map[string]any{"format": formatJSON})
	if err != nil {
		return nil, err
	}
	return textJSON, nil
}

// isJSONObject reports whether raw is a non-null JSON object.
func isJSONObject(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var obj map[string]any
	if err := kitutil.Unmarshal(raw, &obj); err != nil {
		return false
	}
	return obj != nil
}

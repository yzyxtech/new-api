package claudemessages

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/convmeta"
	kitutil "github.com/QuantumNous/new-api/relaykit/relayconvert/kitutil"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/reasoning"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msg(role string, blocks []any) dto.ClaudeMessage {
	return dto.ClaudeMessage{Role: role, Content: blocks}
}

// decodeInput rounds the raw JSON result through kitutil.Unmarshal back into a
// slice of generic items so assertions can inspect item type sequences.
func decodeInput(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var items []map[string]any
	require.NoError(t, kitutil.Unmarshal(raw, &items))
	return items
}

// runInputConvert converts a single-assistant-message history through the
// request input converter and decodes its JSON payload into items.
func runInputConvert(t *testing.T, messages []dto.ClaudeMessage) []map[string]any {
	t.Helper()
	raw, err := claudeMessagesToResponsesInput(messages)
	require.NoError(t, err)
	return decodeInput(t, raw)
}

// itemType returns the logical input item kind. Reasoning/function_call items
// carry a `type` key, whereas plain message items are identified by `role` and
// `content` only, so an item without `type` is treated as a message.
func itemType(it map[string]any) string {
	if t, ok := it["type"].(string); ok && t != "" {
		return t
	}
	return "message"
}

func TestClaudeMessagesToResponsesInput_ThinkingReplay(t *testing.T) {
	t.Parallel()
	const sig = "sig:ABC123"
	items := runInputConvert(t, []dto.ClaudeMessage{
		msg("assistant", []any{
			map[string]any{"type": "thinking", "thinking": "draft", "signature": sig},
			map[string]any{"type": "text", "text": "final answer"},
		}),
	})
	require.Len(t, items, 2)
	assert.Equal(t, "reasoning", itemType(items[0]))
	assert.Equal(t, "message", itemType(items[1]))

	serialized, err := kitutil.Marshal(items[0])
	require.NoError(t, err)
	require.JSONEq(t, fmt.Sprintf(`{"type":"reasoning","summary":[],"content":null,"encrypted_content":%q}`, sig), string(serialized))

	// The trailing text must land in its own message item, not be merged into
	// the reasoning item.
	content, ok := items[1]["content"].([]any)
	require.True(t, ok)
	require.Len(t, content, 1)
	part, ok := content[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "output_text", part["type"])
	assert.Equal(t, "final answer", part["text"])
}

func TestClaudeMessagesToResponsesInput_ThinkingReplayPositions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		blocks  []any
		wantSeq []string
	}{
		{
			name: "thinking_lead",
			blocks: []any{
				map[string]any{"type": "thinking", "signature": "s1"},
				map[string]any{"type": "text", "text": "a"},
			},
			wantSeq: []string{"reasoning", "message"},
		},
		{
			name: "thinking_middle",
			blocks: []any{
				map[string]any{"type": "text", "text": "a"},
				map[string]any{"type": "thinking", "signature": "s1"},
				map[string]any{"type": "text", "text": "b"},
			},
			wantSeq: []string{"message", "reasoning", "message"},
		},
		{
			name: "thinking_tail",
			blocks: []any{
				map[string]any{"type": "text", "text": "a"},
				map[string]any{"type": "thinking", "signature": "s1"},
			},
			wantSeq: []string{"message", "reasoning"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			items := runInputConvert(t, []dto.ClaudeMessage{msg("assistant", tc.blocks)})
			gotSeq := make([]string, 0, len(items))
			for _, it := range items {
				gotSeq = append(gotSeq, itemType(it))
			}
			assert.Equal(t, tc.wantSeq, gotSeq)
		})
	}
}

func TestClaudeMessagesToResponsesInput_RedactedThinkingSkipped(t *testing.T) {
	t.Parallel()
	items := runInputConvert(t, []dto.ClaudeMessage{
		msg("assistant", []any{
			map[string]any{"type": "text", "text": "a"},
			map[string]any{"type": "redacted_thinking", "data": "some data"},
		}),
	})
	require.Len(t, items, 1)
	assert.Equal(t, "message", itemType(items[0]))
	// Nothing about redacted_thinking may leak into any item.
	serialized, err := kitutil.Marshal(items)
	require.NoError(t, err)
	assert.NotContains(t, string(serialized), "redacted_thinking")
	assert.NotContains(t, string(serialized), "some data")
}

func TestClaudeMessagesToResponsesInput_EmptySignatureSkipped(t *testing.T) {
	t.Parallel()
	items := runInputConvert(t, []dto.ClaudeMessage{
		msg("assistant", []any{
			map[string]any{"type": "thinking", "thinking": "draft"},
			map[string]any{"type": "text", "text": "a"},
		}),
	})
	require.Len(t, items, 1)
	// Signature-less thinking must not produce a reasoning replay item; the
	// surrounding text still forms the single message item.
	assert.Equal(t, "message", itemType(items[0]))
}

func TestClaudeMessagesToResponsesInput_ThinkingToolUseInterleave(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		blocks  []any
		wantSeq []string
	}{
		{
			name: "text_thinking_tool_use",
			blocks: []any{
				map[string]any{"type": "text", "text": "t"},
				map[string]any{"type": "thinking", "signature": "s1"},
				map[string]any{"type": "tool_use", "id": "tu1", "name": "ns.api", "input": map[string]any{}},
			},
			wantSeq: []string{"message", "reasoning", "function_call"},
		},
		{
			name: "thinking_tool_use",
			blocks: []any{
				map[string]any{"type": "thinking", "signature": "s1"},
				map[string]any{"type": "tool_use", "id": "tu1", "name": "ns.api", "input": map[string]any{}},
			},
			wantSeq: []string{"reasoning", "function_call"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			items := runInputConvert(t, []dto.ClaudeMessage{msg("assistant", tc.blocks)})
			gotSeq := make([]string, 0, len(items))
			for _, it := range items {
				gotSeq = append(gotSeq, itemType(it))
			}
			assert.Equal(t, tc.wantSeq, gotSeq)
		})
	}
}

// decodeTools rounds the raw tools payload through kitutil.Unmarshal back into
// generic maps so tests can assert on tool shapes.
func decodeTools(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var out []map[string]any
	require.NoError(t, kitutil.Unmarshal(raw, &out))
	return out
}

func TestClaudeToolsToResponsesTools_WebSearchDefinition(t *testing.T) {
	t.Parallel()
	raw, err := claudeToolsToResponsesTools(json.RawMessage(`[
		{"type":"web_search_20250305","name":"custom_search","max_uses":3,"allowed_domains":["a.com","b.com"],"user_location":{"type":"approximate","country":"US","city":"SF"}},
		{"type":"function","name":"do_thing","description":"d","input_schema":{"type":"object"}}
	]`))
	require.NoError(t, err)
	out := decodeTools(t, raw)
	require.Len(t, out, 2)

	// Web-search hosted tool maps to {"type":"web_search"} with
	// filters.allowed_domains and user_location passed through verbatim.
	assert.Equal(t, "web_search", out[0]["type"])
	filters, ok := out[0]["filters"].(map[string]any)
	require.True(t, ok)
	domains, ok := filters["allowed_domains"].([]any)
	require.True(t, ok)
	assert.ElementsMatch(t, []any{"a.com", "b.com"}, domains)
	location, ok := out[0]["user_location"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "US", location["country"])
	assert.Equal(t, "SF", location["city"])

	// The adjacent function tool is untouched by the web-search branch.
	assert.Equal(t, "function", out[1]["type"])
	assert.Equal(t, "do_thing", out[1]["name"])
}

func TestClaudeToolsToResponsesTools_WebSearch20260209(t *testing.T) {
	t.Parallel()
	raw, err := claudeToolsToResponsesTools(json.RawMessage(`[
		{"type":"web_search_20260209","name":"ws","allowed_domains":["x.com"]}
	]`))
	require.NoError(t, err)
	out := decodeTools(t, raw)
	require.Len(t, out, 1)
	assert.Equal(t, "web_search", out[0]["type"])
	filters, ok := out[0]["filters"].(map[string]any)
	require.True(t, ok)
	domains, ok := filters["allowed_domains"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"x.com"}, domains)
}

func TestClaudeToolsToResponsesTools_FunctionNotWebSearch(t *testing.T) {
	t.Parallel()
	// A function tool whose name merely echoes a web-search type must stay a
	// function: recognition is by `type`, never by name.
	raw, err := claudeToolsToResponsesTools(json.RawMessage(`[
		{"name":"web_search_20250305","description":"a real function","input_schema":{"type":"object"}}
	]`))
	require.NoError(t, err)
	out := decodeTools(t, raw)
	require.Len(t, out, 1)
	assert.Equal(t, "function", out[0]["type"])
	assert.Equal(t, "web_search_20250305", out[0]["name"])
}

func TestClaudeToolChoiceToResponses_WebSearchNamed(t *testing.T) {
	t.Parallel()
	tools := json.RawMessage(`[{"type":"web_search_20250305","name":"custom_search"}]`)
	toolChoice, _, err := claudeToolChoiceToResponses(json.RawMessage(`{"type":"tool","name":"custom_search"}`), tools)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"web_search"}`, string(toolChoice))
}

func TestClaudeSystemToResponsesInstructions_AttributionFiltered(t *testing.T) {
	t.Parallel()

	decodeInstructions := func(t *testing.T, req *dto.ClaudeRequest) string {
		t.Helper()
		raw, err := claudeSystemToResponsesInstructions(req)
		require.NoError(t, err)
		if raw == nil {
			return ""
		}
		var out string
		require.NoError(t, kitutil.Unmarshal(raw, &out))
		return out
	}

	t.Run("array_system_drops_attribution_blocks", func(t *testing.T) {
		t.Parallel()
		req := &dto.ClaudeRequest{
			System: []any{
				map[string]any{"type": "text", "text": "x-anthropic-billing-header: <fp>fingerprint</fp>"},
				map[string]any{"type": "text", "text": "  " + "x-anthropic-billing-header: leading-ws attribution"},
				map[string]any{"type": "text", "text": "real instruction"},
			},
		}
		got := decodeInstructions(t, req)
		assert.Equal(t, "real instruction", got)
		assert.NotContains(t, got, "x-anthropic-billing-header")
	})

	t.Run("all_attribution_blocks_drop_instructions", func(t *testing.T) {
		t.Parallel()
		req := &dto.ClaudeRequest{
			System: []any{
				map[string]any{"type": "text", "text": "x-anthropic-billing-header: <fp>fp</fp>"},
			},
		}
		assert.Equal(t, "", decodeInstructions(t, req))
	})

	t.Run("string_system_all_attribution_dropped", func(t *testing.T) {
		t.Parallel()
		req := &dto.ClaudeRequest{}
		req.SetStringSystem("x-anthropic-billing-header: <fp>fp</fp>")
		assert.Equal(t, "", decodeInstructions(t, req))
	})

	t.Run("clean_system_kept", func(t *testing.T) {
		t.Parallel()
		req := &dto.ClaudeRequest{}
		req.SetStringSystem("regular system prompt")
		assert.Equal(t, "regular system prompt", decodeInstructions(t, req))
	})
}

// TestClaudeMessagesToResponsesInput_StringContentMessageBreaksPendingToolUseIDs
// locks the per-message reset of pending tool-use associations across a
// string-content user message. A string user message is still a real turn
// boundary: it must clear the pending tool-use calls of the preceding assistant
// turn, so a later tool_result can never be aligned (results-first) against a
// call that predates the string message. Without the reset the later
// `[text, tool_result(call_a)]` user message would form a complete one-to-one
// match against the stale pending call and be reordered ahead of its text.
func TestClaudeMessagesToResponsesInput_StringContentMessageBreaksPendingToolUseIDs(t *testing.T) {
	t.Parallel()

	items := runInputConvert(t, []dto.ClaudeMessage{
		msg("assistant", []any{
			map[string]any{"type": "tool_use", "id": "call_a", "name": "ns.a", "input": map[string]any{}},
		}),
		dto.ClaudeMessage{Role: "user", Content: "interjected string question"},
		msg("user", []any{
			map[string]any{"type": "text", "text": "context"},
			map[string]any{"type": "tool_result", "tool_use_id": "call_a", "content": "a"},
		}),
	})

	seq := make([]string, 0, len(items))
	for _, it := range items {
		seq = append(seq, itemType(it))
	}
	// The string user message itself is a message item and breaks the pending
	// association, so the block user message keeps its input order: message item
	// first, then the function_call_output (never reordered results-first
	// against the stale call).
	require.Len(t, items, 4)
	assert.Equal(t, []string{
		"function_call",
		"message",
		"message",
		"function_call_output",
	}, seq)
	assert.Equal(t, "call_a", items[3]["call_id"])
	assert.Equal(t, "a", items[3]["output"])
}

func TestClaudeMessagesToResponsesInput_MisplacedToolResultAligned(t *testing.T) {
	t.Parallel()

	t.Run("complete_match_reorders_by_call_order", func(t *testing.T) {
		t.Parallel()
		// Pending calls are call_b, call_a. The user message carries both results
		// (a complete one-to-one match), so the baseline reorders them into the
		// tool-use call order (call_b first) regardless of their input position.
		items := runInputConvert(t, []dto.ClaudeMessage{
			msg("assistant", []any{
				map[string]any{"type": "tool_use", "id": "call_b", "name": "ns.b", "input": map[string]any{}},
				map[string]any{"type": "tool_use", "id": "call_a", "name": "ns.a", "input": map[string]any{}},
			}),
			msg("user", []any{
				map[string]any{"type": "tool_result", "tool_use_id": "call_a", "content": "a"},
				map[string]any{"type": "tool_result", "tool_use_id": "call_b", "content": "b"},
			}),
		})
		// Two function_call items from the assistant turn, then the two aligned
		// function_call_output items in call order.
		require.Len(t, items, 4)
		assert.Equal(t, "function_call", itemType(items[0]))
		assert.Equal(t, "function_call", itemType(items[1]))
		assert.Equal(t, "function_call_output", itemType(items[2]))
		assert.Equal(t, "call_b", items[2]["call_id"])
		assert.Equal(t, "b", items[2]["output"])
		assert.Equal(t, "function_call_output", itemType(items[3]))
		assert.Equal(t, "call_a", items[3]["call_id"])
		assert.Equal(t, "a", items[3]["output"])
	})

	t.Run("incomplete_match_keeps_original_order", func(t *testing.T) {
		t.Parallel()
		// Pending calls are call_a and call_b (two), but this user message
		// carries only one tool_result. The baseline returns the content
		// unchanged rather than dropping or inventing results: the message item
		// keeps its position ahead of the single function_call_output, and no
		// result is silently discarded.
		items := runInputConvert(t, []dto.ClaudeMessage{
			msg("assistant", []any{
				map[string]any{"type": "tool_use", "id": "call_a", "name": "ns.a", "input": map[string]any{}},
				map[string]any{"type": "tool_use", "id": "call_b", "name": "ns.b", "input": map[string]any{}},
			}),
			msg("user", []any{
				map[string]any{"type": "text", "text": "context"},
				map[string]any{"type": "tool_result", "tool_use_id": "call_b", "content": "b"},
			}),
		})
		require.Len(t, items, 4)
		assert.Equal(t, "function_call", itemType(items[0]))
		assert.Equal(t, "function_call", itemType(items[1]))
		assert.Equal(t, "message", itemType(items[2]))
		assert.Equal(t, "function_call_output", itemType(items[3]))
		assert.Equal(t, "call_b", items[3]["call_id"])
		assert.Equal(t, "b", items[3]["output"])
	})

	t.Run("mismatched_result_keeps_original_order", func(t *testing.T) {
		t.Parallel()
		// A tool_result whose id matches no pending call breaks the one-to-one
		// correspondence, so the baseline leaves the whole user message in its
		// original order instead of isolating a single orphan result.
		items := runInputConvert(t, []dto.ClaudeMessage{
			msg("assistant", []any{
				map[string]any{"type": "tool_use", "id": "call_a", "name": "ns.a", "input": map[string]any{}},
				map[string]any{"type": "tool_use", "id": "call_b", "name": "ns.b", "input": map[string]any{}},
			}),
			msg("user", []any{
				map[string]any{"type": "tool_result", "tool_use_id": "call_a", "content": "a"},
				map[string]any{"type": "tool_result", "tool_use_id": "call_unknown", "content": "u"},
			}),
		})
		require.Len(t, items, 4)
		assert.Equal(t, "function_call", itemType(items[0]))
		assert.Equal(t, "function_call", itemType(items[1]))
		assert.Equal(t, "call_a", items[2]["call_id"])
		assert.Equal(t, "a", items[2]["output"])
		// The orphan result is preserved, not swallowed.
		assert.Equal(t, "call_unknown", items[3]["call_id"])
		assert.Equal(t, "u", items[3]["output"])
	})
}

// TestClaudeMessagesToResponsesInput_ToolResultsPrecedeContent locks the
// baseline AlignClaudeToolResults reordering within a single matched user
// message: all function_call_output items of the turn are emitted before the
// single message item that carries the interleaved text, so a `[text,
// tool_result(A), text, tool_result(B)]` input becomes `fco(A) → fco(B) →
// message` (results-first, in call order). This guards against flushing
// accumulated content ahead of collected tool results, which would interleave
// the outputs with the message item.
func TestClaudeMessagesToResponsesInput_ToolResultsPrecedeContent(t *testing.T) {
	t.Parallel()

	items := runInputConvert(t, []dto.ClaudeMessage{
		msg("assistant", []any{
			map[string]any{"type": "tool_use", "id": "call_a", "name": "ns.a", "input": map[string]any{}},
			map[string]any{"type": "tool_use", "id": "call_b", "name": "ns.b", "input": map[string]any{}},
		}),
		msg("user", []any{
			map[string]any{"type": "text", "text": "context before"},
			map[string]any{"type": "tool_result", "tool_use_id": "call_a", "content": "a"},
			map[string]any{"type": "text", "text": "context after"},
			map[string]any{"type": "tool_result", "tool_use_id": "call_b", "content": "b"},
		}),
	})

	seq := make([]string, 0, len(items))
	for _, it := range items {
		seq = append(seq, itemType(it))
	}
	// The assistant turn yields the two function_call items. Within the user
	// turn both function_call_output items precede the message item (never
	// interleaved with the surrounding text), ordered by call.
	assert.Equal(t, []string{
		"function_call",
		"function_call",
		"function_call_output",
		"function_call_output",
		"message",
	}, seq)
	assert.Equal(t, "call_a", items[2]["call_id"])
	assert.Equal(t, "a", items[2]["output"])
	assert.Equal(t, "call_b", items[3]["call_id"])
	assert.Equal(t, "b", items[3]["output"])
	// The message item carries both interleaved text parts, results-first.
	content, ok := items[4]["content"].([]any)
	require.True(t, ok)
	require.Len(t, content, 2)
	part1, ok := content[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "context before", part1["text"])
	part2, ok := content[1].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "context after", part2["text"])
}

// runRequestConvert converts the full Claude request through the Responses
// request converter, returning the produced request.
func runRequestConvert(t *testing.T, req *dto.ClaudeRequest) *dto.OpenAIResponsesRequest {
	t.Helper()
	out, err := ClaudeMessagesRequestToOpenAIResponses(*req, nil)
	require.NoError(t, err)
	return out
}

// runRequestConvertWithInfo is runRequestConvert with an explicit conversion
// context, so tests can inject the host-derived reasoning suffix state.
func runRequestConvertWithInfo(t *testing.T, req *dto.ClaudeRequest, info convmeta.Meta) *dto.OpenAIResponsesRequest {
	t.Helper()
	out, err := ClaudeMessagesRequestToOpenAIResponses(*req, info)
	require.NoError(t, err)
	return out
}

// TestClaudeMessagesToResponsesInput_MinusOneBudgetSuffixEmitsAuto is the
// full-chain regression for a `-thinking--1` model suffix: a -1 thinking
// budget (baseline: auto) must survive the complete conversion and emit
// reasoning.effort:"auto" instead of failing the shared reasoning validation
// or silently downgrading to a budget-derived level.
func TestClaudeMessagesToResponsesInput_MinusOneBudgetSuffixEmitsAuto(t *testing.T) {
	t.Parallel()
	budget := -1
	include := true
	info := &convmeta.Values{
		OriginModelName: "claude-sonnet-4-6-thinking--1",
		ReasoningConversion: &dto.ReasoningConversionState{
			Mode:            string(reasoning.ModeEnabled),
			BudgetTokens:    &budget,
			IncludeThoughts: &include,
		},
	}
	out := runRequestConvertWithInfo(t, &dto.ClaudeRequest{
		Model: "claude-sonnet-4-6",
	}, info)

	require.NotNil(t, out.Reasoning)
	assert.Equal(t, string(claudeEffortAuto), out.Reasoning.Effort)
	// The exact -1 budget is retained in the conversion state for any later
	// in-process hop, and the derived effort is recorded on the meta.
	require.NotNil(t, out.ReasoningConversion)
	require.NotNil(t, out.ReasoningConversion.BudgetTokens)
	assert.Equal(t, -1, *out.ReasoningConversion.BudgetTokens)
	assert.Equal(t, string(claudeEffortAuto), info.ReasoningEffort)
}

func TestClaudeMessagesToResponsesInput_OutputConfigTextFormat(t *testing.T) {
	t.Parallel()

	const schema = `{"type":"object","properties":{"x":{"type":"string"}}}`

	t.Run("maps_json_schema_format_with_name_and_strict_false", func(t *testing.T) {
		t.Parallel()
		out := runRequestConvert(t, &dto.ClaudeRequest{
			Model: "gpt-5.6",
			OutputConfig: json.RawMessage(`{"format":{"type":"json_schema","name":"my_schema","strict":false,"schema":` +
				schema + `}}`),
		})
		require.NotEmpty(t, out.Text)
		var text map[string]any
		require.NoError(t, kitutil.Unmarshal(out.Text, &text))
		format, ok := text["format"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "json_schema", format["type"])
		assert.Equal(t, "my_schema", format["name"])
		assert.Equal(t, false, format["strict"])
		schemaJSON, err := kitutil.Marshal(format["schema"])
		require.NoError(t, err)
		assert.JSONEq(t, schema, string(schemaJSON))
	})

	t.Run("defaults_name_and_strict", func(t *testing.T) {
		t.Parallel()
		out := runRequestConvert(t, &dto.ClaudeRequest{
			Model: "gpt-5.6",
			OutputConfig: json.RawMessage(`{"format":{"type":"json_schema","schema":` +
				schema + `}}`),
		})
		require.NotEmpty(t, out.Text)
		var text map[string]any
		require.NoError(t, kitutil.Unmarshal(out.Text, &text))
		format, ok := text["format"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "cli_proxy_structured_output", format["name"])
		assert.Equal(t, true, format["strict"])
	})

	t.Run("whitespace_name_passthrough_verbatim", func(t *testing.T) {
		t.Parallel()
		out := runRequestConvert(t, &dto.ClaudeRequest{
			Model: "gpt-5.6",
			OutputConfig: json.RawMessage(`{"format":{"type":"json_schema","name":" my_schema ","schema":` +
				schema + `}}`),
		})
		require.NotEmpty(t, out.Text)
		var text map[string]any
		require.NoError(t, kitutil.Unmarshal(out.Text, &text))
		format, ok := text["format"].(map[string]any)
		require.True(t, ok)
		// A non-empty name (here whitespace-padded) passes through verbatim,
		// matching the baseline behavior of not trimming `format.name`.
		assert.Equal(t, " my_schema ", format["name"])
	})

	t.Run("non_json_schema_type_leaves_text_unset", func(t *testing.T) {
		t.Parallel()
		out := runRequestConvert(t, &dto.ClaudeRequest{
			Model: "gpt-5.6",
			OutputConfig: json.RawMessage(`{"format":{"type":"text","schema":` +
				schema + `}}`),
		})
		assert.Empty(t, out.Text)
	})
}

func TestClaudeRequestReasoningIntent_EffortMapping(t *testing.T) {
	t.Parallel()

	t.Run("budget_tokens_maps_to_aligned_level", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			budget int
			want   reasoning.Effort
		}{
			{budget: 1024, want: reasoning.EffortLow},
			{budget: 5000, want: reasoning.EffortMedium},
			{budget: 16384, want: reasoning.EffortHigh},
			{budget: 30000, want: reasoning.EffortXHigh},
		}
		for _, tc := range cases {
			intent, effort, err := claudeRequestReasoningIntent(&dto.ClaudeRequest{
				Model:    "gpt-5.6",
				Thinking: &dto.Thinking{Type: "enabled", BudgetTokens: &tc.budget},
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.want, effort)
			assert.Equal(t, tc.want, intent.Effort)
		}
	})

	t.Run("negative_budget_follows_baseline", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			budget int
			want   reasoning.Effort
			ok     bool
		}{
			{budget: -1, want: claudeEffortAuto, ok: true},
			{budget: -2, ok: false},
			{budget: 0, want: reasoning.EffortNone, ok: true},
			{budget: 512, want: reasoning.EffortMinimal, ok: true},
		}
		for _, tc := range cases {
			level, ok := claudeBudgetToLevel(tc.budget)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, level)
		}
	})

	t.Run("output_config_effort_passthrough_wins", func(t *testing.T) {
		t.Parallel()
		adaptive := "adaptive"
		intent, effort, err := claudeRequestReasoningIntent(&dto.ClaudeRequest{
			Model:        "gpt-5.6",
			Thinking:     &dto.Thinking{Type: adaptive},
			OutputConfig: json.RawMessage(`{"effort":"high"}`),
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, reasoning.EffortHigh, effort)
		assert.Equal(t, reasoning.EffortHigh, intent.Effort)
	})

	t.Run("disabled_thinking_maps_to_none", func(t *testing.T) {
		t.Parallel()
		_, effort, err := claudeRequestReasoningIntent(&dto.ClaudeRequest{
			Model:    "gpt-5.6",
			Thinking: &dto.Thinking{Type: "disabled"},
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, reasoning.EffortNone, effort)
	})
}

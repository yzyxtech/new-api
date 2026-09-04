package oairesponses

import (
	"fmt"
	"strings"

	"github.com/QuantumNous/new-api/relaykit/dto"
	sharedclaude "github.com/QuantumNous/new-api/relaykit/relayconvert/internal/shared/claude"
	kitutil "github.com/QuantumNous/new-api/relaykit/relayconvert/kitutil"
	"github.com/QuantumNous/new-api/relaykit/types"
)

const (
	responsesEventOutputTextDone   = "response.output_text.done"
	responsesEventContentPartAdded = "response.content_part.added"

	// maxStreamConversionDiagnostics 是本流式转换器单次请求可保留的不同诊断条数上限,
	// 镜像 relay/common.RecordConversionDiagnostics 的全局 32 条 distinct 有界口径:
	// 类别内条目不无限增长,溢出时置 conversionDiagnosticsTruncated。
	maxStreamConversionDiagnostics = 32

	// signatureDroppedAfterEarlyClose 标识 reasoning item 的 thinking 块已因 text/content
	// 边界被提前 finalize 后,该 item 的 done 才携带非空 encrypted_content 到达的签名丢弃。
	signatureDroppedAfterEarlyClose = "signature_dropped_after_early_close"
)

// streamConversionDiagnosticKey 是流式转换诊断去重的稳定键(code/path/severity/from/to),
// 与 relay/common.conversionDiagnosticKey 同构,保证同一条目不会重复记录。
type streamConversionDiagnosticKey struct {
	code     string
	path     string
	severity types.ConversionDiagnosticSeverity
	from     types.RelayFormat
	to       types.RelayFormat
}

type ResponsesToClaudeStreamState struct {
	ID    string
	Model string
	Usage *dto.Usage

	sentMessageStart bool
	done             bool
	sawToolCall      bool
	nextBlockIndex   int
	blocks           []*responsesClaudeStreamBlock
	byOutputIndex    map[int]*responsesClaudeStreamBlock
	byItemID         map[string]*responsesClaudeStreamBlock
	lastByKind       map[string]*responsesClaudeStreamBlock
	usageText        strings.Builder

	reasoningByOutputIndex   map[int]*responsesReasoningItemState
	reasoningByItemID        map[string]*responsesReasoningItemState
	reasoningItems           []*responsesReasoningItemState
	activeAnonymousReasoning *responsesReasoningItemState
	nextAnonymousReasoning   uint64

	// webSearchEmitted 按最终选定的非空 tool_use_id 去重:server_tool_use 与
	// web_search_tool_result 每个 ID 各至多产出一组,独立于 reasoning item 状态。
	webSearchEmitted map[string]struct{}
	// lastWebSearchID 记录上一已知的非空 web_search tool_use_id,作为纯匿名
	// web_search_call 的最后一级回退来源。
	lastWebSearchID string

	// 下行为有界诊断:本流式转换器观察到的转换丢失诊断(每个 distinct 条目至多一次)。
	// 有界保留(≤全请求 maxStreamConversionDiagnostics 条 distinct)由 root 侧既有
	// RecordConversionDiagnostics 统一裁决,最终落入 admin_info.conversion_diagnostics;
	// 此处自持一份 distinct 收集以便本转换器内自测可观测,并在 distinct 条数超过
	// maxStreamConversionDiagnostics 时置 conversionDiagnosticsTruncated 作为本类别的
	// 溢出信号(不本地丢弃,全部 distinct 仍交 root 使边界 n=0、d=33 也能触发其截断)。
	conversionDiagnostics          []types.ConversionDiagnostic
	seenConversionDiagnostics      map[streamConversionDiagnosticKey]struct{}
	conversionDiagnosticsTruncated bool
}

// responsesReasoningItemState 是每个 reasoning item 独享的 typed 流式状态,
// 承载签名快照与收尾标志。不得用流级布尔(signatureSent)代替该 item 级状态。
type responsesReasoningItemState struct {
	outputIndex        *int
	itemID             string
	anonymousOrdinal   uint64
	block              *responsesClaudeStreamBlock
	fallbackSignature  string
	summarySeen        bool
	earlyClosed        bool
	signatureEmitted   bool
	diagnosticRecorded bool
	terminal           bool
}

type responsesClaudeStreamBlock struct {
	Index               int
	Kind                string
	ItemID              string
	CallID              string
	Name                string
	Started             bool
	Stopped             bool
	Value               strings.Builder
	SentBytes           int
	AnnotationCount     int
	NeedsReasoningBreak bool
}

func NewResponsesToClaudeStreamState(id string, model string) *ResponsesToClaudeStreamState {
	return &ResponsesToClaudeStreamState{
		ID:                     strings.TrimSpace(id),
		Model:                  strings.TrimSpace(model),
		byOutputIndex:          make(map[int]*responsesClaudeStreamBlock),
		byItemID:               make(map[string]*responsesClaudeStreamBlock),
		lastByKind:             make(map[string]*responsesClaudeStreamBlock),
		reasoningByOutputIndex: make(map[int]*responsesReasoningItemState),
		reasoningByItemID:      make(map[string]*responsesReasoningItemState),
		webSearchEmitted:       make(map[string]struct{}),
	}
}

func (s *ResponsesToClaudeStreamState) UsageText() string {
	if s == nil {
		return ""
	}
	return s.usageText.String()
}

func (s *ResponsesToClaudeStreamState) Done() bool {
	return s != nil && s.done
}

func (s *ResponsesToClaudeStreamState) SetUsage(usage *dto.Usage) {
	if s != nil && usage != nil {
		s.Usage = usage
	}
}

func (s *ResponsesToClaudeStreamState) StreamUsage() *dto.Usage {
	if s == nil {
		return nil
	}
	return s.Usage
}

func (s *ResponsesToClaudeStreamState) SetStreamUsage(usage *dto.Usage) {
	s.SetUsage(usage)
}

// ConversionDiagnostics 返回本流式转换器累积的转换丢失诊断(有界、distinct)。保留该
// 可观测出口以便 root 侧经由既有 RecordConversionDiagnostics 落入 admin_info。
func (s *ResponsesToClaudeStreamState) ConversionDiagnostics() []types.ConversionDiagnostic {
	if s == nil || len(s.conversionDiagnostics) == 0 {
		return nil
	}
	return append([]types.ConversionDiagnostic{}, s.conversionDiagnostics...)
}

// ConversionDiagnosticsTruncated 报告本类别 distinct 诊断条数是否已超过
// maxStreamConversionDiagnostics 这条有界保留量(溢出信号;不代表本转换器丢弃了条目,仅
// 反映"该类别 drop 数超出保留上限")。全请求权威 truncated(admin_info.
// conversion_diagnostics_truncated)由 root RecordConversionDiagnostics 按全局 32 条
// 上限裁决。
func (s *ResponsesToClaudeStreamState) ConversionDiagnosticsTruncated() bool {
	return s != nil && s.conversionDiagnosticsTruncated
}

// recordConversionDiagnostic 记录一条 distinct 诊断:同一条目(code/path/severity/
// from/to)只记录一次,distinct 条目不重复。当 distinct 条数超过
// maxStreamConversionDiagnostics 时置 conversionDiagnosticsTruncated=true 作为本类别
// 的"超出有界保留量"信号,但**不再本地丢弃新条目**——有界保留公式 min(d,max(0,32-n))
// 由 root 侧 RecordConversionDiagnostics 按全请求已收集 distinct 条数 n 统一收紧并置
// admin_info.conversion_diagnostics_truncated。若此处本地丢弃,root 只收得到 32 条、
// 观察不到第 33 条溢出,n=0、d=33 等边界将无法置 truncated(true)。
func (s *ResponsesToClaudeStreamState) recordConversionDiagnostic(diagnostic types.ConversionDiagnostic) {
	if s == nil {
		return
	}
	if s.seenConversionDiagnostics == nil {
		s.seenConversionDiagnostics = make(map[streamConversionDiagnosticKey]struct{})
	}
	key := streamConversionDiagnosticKey{
		code:     diagnostic.Code,
		path:     diagnostic.Path,
		severity: diagnostic.Severity,
		from:     diagnostic.From,
		to:       diagnostic.To,
	}
	if _, exists := s.seenConversionDiagnostics[key]; exists {
		return
	}
	s.seenConversionDiagnostics[key] = struct{}{}
	if len(s.conversionDiagnostics) >= maxStreamConversionDiagnostics {
		s.conversionDiagnosticsTruncated = true
	}
	s.conversionDiagnostics = append(s.conversionDiagnostics, diagnostic)
}

// reasoningItemDiagnosticPath 为该 reasoning item 构造稳定的诊断路径,优先取稳定 output
// index,其次根 item_id,再次 anonymous 序号;每个被丢弃 item 的路径唯一,以避开既有
// code/path 去重导致的不同 drop 被折叠。
func (s *ResponsesToClaudeStreamState) reasoningItemDiagnosticPath(st *responsesReasoningItemState) string {
	if st == nil {
		return "items/<unknown>/reasoning/encrypted_content"
	}
	if st.outputIndex != nil {
		return fmt.Sprintf("items/%d/reasoning/encrypted_content", *st.outputIndex)
	}
	if st.itemID != "" {
		return fmt.Sprintf("items/%s/reasoning/encrypted_content", st.itemID)
	}
	return fmt.Sprintf("items/anonymous-%d/reasoning/encrypted_content", st.anonymousOrdinal)
}

// recordDroppedAfterEarlyClose 对"thinking 块已提前关闭后、done 迟到的非空签名丢弃"做
// 一次诊断记录尝试:Code=signature_dropped_after_early_close、Severity=warning、
// From=openai_responses、To=claude、Path 取该 item 稳定路径。
func (s *ResponsesToClaudeStreamState) recordDroppedAfterEarlyClose(st *responsesReasoningItemState) {
	if s == nil || st == nil {
		return
	}
	s.recordConversionDiagnostic(types.ConversionDiagnostic{
		Code:     signatureDroppedAfterEarlyClose,
		Path:     s.reasoningItemDiagnosticPath(st),
		Message:  "reasoning item signature dropped after thinking block was early closed",
		Severity: types.ConversionDiagnosticWarning,
		From:     types.RelayFormatOpenAIResponses,
		To:       types.RelayFormatClaude,
	})
}

func (s *ResponsesToClaudeStreamState) ConvertChunk(event *dto.ResponsesStreamResponse, estimatedInputTokens int) ([]*dto.ClaudeResponse, *dto.Usage, error) {
	if s == nil {
		return nil, nil, nil
	}
	if event == nil || s.done {
		return nil, s.Usage, nil
	}

	s.applyResponseMetadata(event.Response)
	switch event.Type {
	case responsesEventCreated:
		return s.ensureMessageStart(estimatedInputTokens), s.Usage, nil
	case responsesEventReasoningSummaryDelta, responsesEventReasoningTextDelta:
		// 先于任何副作用(收尾其它块/建块)完成 reasoning 身份解析:无标识 summary 直接
		// 报身份不可判定错误,不建块、不改其它已打开块的状态。
		rst, err := s.resolveOrCreateReasoningState(event)
		if err != nil {
			return nil, s.Usage, err
		}
		finalize, err := s.finalizeReasoningThinking(event, rst, estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureReasoningBlock(event, rst, "thinking")
		if err != nil {
			return nil, s.Usage, err
		}
		s.reasoningBlockEntered(rst, block)
		delta := event.Delta
		if block.NeedsReasoningBreak && delta != "" {
			delta = separatedResponsesDelta(delta)
			block.NeedsReasoningBreak = false
		}
		responses := append(finalize, s.appendDelta(block, delta, estimatedInputTokens)...)
		return responses, s.Usage, nil
	case responsesEventReasoningSummaryDone, responsesEventReasoningTextDone:
		// 先于任何副作用完成 reasoning 身份解析,再进入收尾/建块。
		rst, err := s.resolveOrCreateReasoningState(event)
		if err != nil {
			return nil, s.Usage, err
		}
		finalize, err := s.finalizeReasoningThinking(event, rst, estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureReasoningBlock(event, rst, "thinking")
		if err != nil {
			return nil, s.Usage, err
		}
		s.reasoningBlockEntered(rst, block)
		var responses []*dto.ClaudeResponse
		responses = append(responses, finalize...)
		if event.Text != nil {
			responses = append(responses, s.mergeFinalValue(block, *event.Text, estimatedInputTokens)...)
		}
		if block.Value.Len() > 0 {
			block.NeedsReasoningBreak = true
		}
		return responses, s.Usage, nil
	case responsesEventOutputTextDelta:
		finalize, err := s.finalizeOpenThinking(estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureBlock(event, "text")
		if err != nil {
			return nil, s.Usage, err
		}
		responses := append(finalize, s.appendDelta(block, event.Delta, estimatedInputTokens)...)
		return responses, s.Usage, nil
	case responsesEventOutputTextDone:
		finalize, err := s.finalizeOpenThinking(estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureBlock(event, "text")
		if err != nil {
			return nil, s.Usage, err
		}
		if event.Text == nil {
			return finalize, s.Usage, nil
		}
		responses := append(finalize, s.mergeFinalValue(block, *event.Text, estimatedInputTokens)...)
		return responses, s.Usage, nil
	case responsesEventOutputTextAnnotationAdded:
		finalize, err := s.finalizeOpenThinking(estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureBlock(event, "text")
		if err != nil {
			return nil, s.Usage, err
		}
		var annotation any
		if err := kitutil.Unmarshal(event.Annotation, &annotation); err != nil {
			return nil, s.Usage, fmt.Errorf("invalid Responses stream annotation: %w", err)
		}
		responses := append(finalize, s.appendAnnotations(block, []any{annotation}, estimatedInputTokens, false)...)
		return responses, s.Usage, nil
	case responsesEventOutputItemAdded, responsesEventOutputItemDone:
		responses, err := s.applyOutputItem(event, estimatedInputTokens, event.Type == responsesEventOutputItemDone)
		return responses, s.Usage, err
	case responsesEventFunctionArgsDelta, responsesEventCustomToolInputDelta:
		finalize, err := s.finalizeInterferingThinking(event, estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureBlock(event, "tool_use")
		if err != nil {
			return nil, s.Usage, err
		}
		responses := append(finalize, s.appendDelta(block, event.Delta, estimatedInputTokens)...)
		return responses, s.Usage, nil
	case responsesEventFunctionArgsDone, responsesEventCustomToolInputDone:
		finalize, err := s.finalizeInterferingThinking(event, estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		block, err := s.ensureBlock(event, "tool_use")
		if err != nil {
			return nil, s.Usage, err
		}
		if event.Arguments == nil {
			return finalize, s.Usage, nil
		}
		responses := append(finalize, s.mergeFinalValue(block, *event.Arguments, estimatedInputTokens)...)
		return responses, s.Usage, nil
	case responsesEventCompleted, responsesEventDone, responsesEventIncomplete:
		responses, err := s.finish(event.Response, estimatedInputTokens)
		return responses, s.Usage, err
	case responsesEventContentPartAdded:
		// INV-5:内容边界事件先到先触发——进入新块前先收尾已打开的 thinking 块。
		responses, err := s.finalizeOpenThinking(estimatedInputTokens)
		if err != nil {
			return nil, s.Usage, err
		}
		return responses, s.Usage, nil
	case responsesEventFailed, responsesEventError:
		message := strings.TrimSpace(event.Message)
		if message == "" {
			message = event.Type
		}
		return nil, s.Usage, fmt.Errorf("responses stream error: %s", message)
	default:
		return nil, s.Usage, nil
	}
}

func (s *ResponsesToClaudeStreamState) Finalize(estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	if s == nil || s.done {
		return nil, nil
	}
	return s.finish(nil, estimatedInputTokens)
}

func (s *ResponsesToClaudeStreamState) applyResponseMetadata(response *dto.OpenAIResponsesResponse) {
	if s == nil || response == nil {
		return
	}
	if response.ID != "" {
		s.ID = response.ID
	}
	if response.Model != "" {
		s.Model = response.Model
	}
	if response.Usage != nil {
		s.Usage = dto.MergeUsageNonZero(s.Usage, UsageFromResponsesUsage(response.Usage))
	}
}

func (s *ResponsesToClaudeStreamState) ensureMessageStart(estimatedInputTokens int) []*dto.ClaudeResponse {
	if s.sentMessageStart {
		return nil
	}
	s.sentMessageStart = true
	inputTokens := estimatedInputTokens
	if s.Usage != nil {
		if usage := sharedclaude.UsageFromOpenAI(s.Usage); usage != nil {
			inputTokens = usage.InputTokens
		}
	}
	message := &dto.ClaudeMediaMessage{
		Id:    s.ID,
		Type:  "message",
		Role:  "assistant",
		Model: s.Model,
		Usage: &dto.ClaudeUsage{InputTokens: inputTokens},
	}
	message.SetContent(make([]any, 0))
	return []*dto.ClaudeResponse{{Type: "message_start", Message: message}}
}

func (s *ResponsesToClaudeStreamState) ensureBlock(event *dto.ResponsesStreamResponse, kind string) (*responsesClaudeStreamBlock, error) {
	block := s.findBlock(event)
	if block == nil {
		if last := s.lastByKind[kind]; last != nil && !last.Stopped && event.OutputIndex == nil && responseStreamEventItemID(event) == "" {
			block = last
		}
	}
	if block == nil {
		block = &responsesClaudeStreamBlock{Index: s.nextBlockIndex, Kind: kind}
		s.nextBlockIndex++
		s.blocks = append(s.blocks, block)
	}
	if block.Kind == "" {
		block.Kind = kind
	}
	if block.Kind != kind {
		return nil, fmt.Errorf("Responses output item changed from %s to %s", block.Kind, kind)
	}
	s.applyBlockMetadata(block, event)
	s.lastByKind[kind] = block
	return block, nil
}

func (s *ResponsesToClaudeStreamState) findBlock(event *dto.ResponsesStreamResponse) *responsesClaudeStreamBlock {
	if event == nil {
		return nil
	}
	if event.OutputIndex != nil {
		if block := s.byOutputIndex[*event.OutputIndex]; block != nil {
			return block
		}
	}
	if itemID := responseStreamEventItemID(event); itemID != "" {
		return s.byItemID[itemID]
	}
	return nil
}

// ensureReasoningBlock 供 reasoning 分支以已解析的 reasoning item 状态的 block 为权威
// 绑定。当 st.block 已建立(此前 added/summary/done 已把本 item 绑到某块)时直接复用该块,
// 避免再用 item.id 优先的通用 findBlock 按根 item_id 已绑定的身份错过同一块;st.block 为
// nil 时才走通用 ensureBlock 建块。
func (s *ResponsesToClaudeStreamState) ensureReasoningBlock(event *dto.ResponsesStreamResponse, st *responsesReasoningItemState, kind string) (*responsesClaudeStreamBlock, error) {
	if st != nil && st.block != nil {
		block := st.block
		if block.Kind == "" {
			block.Kind = kind
		} else if block.Kind != kind {
			return nil, fmt.Errorf("Responses output item changed from %s to %s", block.Kind, kind)
		}
		s.applyBlockMetadata(block, event)
		s.lastByKind[kind] = block
		return block, nil
	}
	return s.ensureBlock(event, kind)
}

// finalizeReasoningThinking 供 reasoning 分支以已解析的 reasoning item 状态的 block 为
// 权威判断"本事件是否继续同一块":当 st.block 是仍打开的 thinking 块时,本事件是同一
// reasoning item 的延续,不 finalize;否则(本 item 无块、或块已关闭/st.block 属于其它
// 类型)按 INV-5 收尾其它仍打开的 thinking 块。
func (s *ResponsesToClaudeStreamState) finalizeReasoningThinking(event *dto.ResponsesStreamResponse, st *responsesReasoningItemState, estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	if st != nil && st.block != nil && st.block.Kind == "thinking" && st.block.Started && !st.block.Stopped {
		return nil, nil
	}
	return s.finalizeOpenThinking(estimatedInputTokens)
}

func (s *ResponsesToClaudeStreamState) applyBlockMetadata(block *responsesClaudeStreamBlock, event *dto.ResponsesStreamResponse) {
	if block == nil || event == nil {
		return
	}
	if event.OutputIndex != nil {
		s.byOutputIndex[*event.OutputIndex] = block
	}
	if itemID := responseStreamEventItemID(event); itemID != "" {
		block.ItemID = itemID
		s.byItemID[itemID] = block
	}
	if event.Item == nil {
		return
	}
	if callID := strings.TrimSpace(event.Item.CallId); callID != "" {
		block.CallID = callID
	} else if block.CallID == "" {
		block.CallID = strings.TrimSpace(event.Item.ID)
	}
	if name := strings.TrimSpace(event.Item.Name); name != "" {
		block.Name = name
	}
}

func (s *ResponsesToClaudeStreamState) startBlock(block *responsesClaudeStreamBlock, estimatedInputTokens int) []*dto.ClaudeResponse {
	if block == nil || block.Started || block.Stopped {
		return nil
	}
	var content dto.ClaudeMediaMessage
	switch block.Kind {
	case "text":
		content = dto.ClaudeMediaMessage{Type: "text", Text: kitutil.GetPointer("")}
	case "thinking":
		content = dto.ClaudeMediaMessage{Type: "thinking", Thinking: kitutil.GetPointer("")}
	case "tool_use":
		if block.Name == "" {
			return nil
		}
		callID := block.CallID
		if callID == "" {
			callID = block.ItemID
		}
		content = dto.ClaudeMediaMessage{Type: "tool_use", Id: callID, Name: block.Name, Input: map[string]any{}}
		s.sawToolCall = true
	default:
		return nil
	}
	block.Started = true
	responses := s.ensureMessageStart(estimatedInputTokens)
	index := block.Index
	responses = append(responses, &dto.ClaudeResponse{Type: "content_block_start", Index: &index, ContentBlock: &content})
	return responses
}

func (s *ResponsesToClaudeStreamState) appendDelta(block *responsesClaudeStreamBlock, delta string, estimatedInputTokens int) []*dto.ClaudeResponse {
	if block == nil || block.Stopped || delta == "" {
		return nil
	}
	block.Value.WriteString(delta)
	return s.flushBlock(block, estimatedInputTokens)
}

func (s *ResponsesToClaudeStreamState) mergeFinalValue(block *responsesClaudeStreamBlock, finalValue string, estimatedInputTokens int) []*dto.ClaudeResponse {
	if block == nil || block.Stopped {
		return nil
	}
	current := block.Value.String()
	if current == "" {
		block.Value.WriteString(finalValue)
	} else if strings.HasPrefix(finalValue, current) {
		block.Value.WriteString(finalValue[len(current):])
	}
	return s.flushBlock(block, estimatedInputTokens)
}

func (s *ResponsesToClaudeStreamState) flushBlock(block *responsesClaudeStreamBlock, estimatedInputTokens int) []*dto.ClaudeResponse {
	if block == nil || block.Stopped {
		return nil
	}
	responses := s.startBlock(block, estimatedInputTokens)
	if !block.Started {
		return responses
	}
	value := block.Value.String()
	if block.SentBytes >= len(value) {
		return responses
	}
	delta := value[block.SentBytes:]
	block.SentBytes = len(value)
	s.usageText.WriteString(delta)
	index := block.Index
	media := &dto.ClaudeMediaMessage{}
	switch block.Kind {
	case "text":
		media.Type = "text_delta"
		media.Text = &delta
	case "thinking":
		media.Type = "thinking_delta"
		media.Thinking = &delta
	case "tool_use":
		media.Type = "input_json_delta"
		media.PartialJson = &delta
	}
	responses = append(responses, &dto.ClaudeResponse{Type: "content_block_delta", Index: &index, Delta: media})
	return responses
}

func (s *ResponsesToClaudeStreamState) stopBlock(block *responsesClaudeStreamBlock, estimatedInputTokens int) []*dto.ClaudeResponse {
	if block == nil || block.Stopped {
		return nil
	}
	responses := s.flushBlock(block, estimatedInputTokens)
	responses = append(responses, s.startBlock(block, estimatedInputTokens)...)
	if !block.Started {
		return responses
	}
	block.Stopped = true
	index := block.Index
	return append(responses, &dto.ClaudeResponse{Type: "content_block_stop", Index: &index})
}

func (s *ResponsesToClaudeStreamState) applyOutputItem(event *dto.ResponsesStreamResponse, estimatedInputTokens int, stop bool) ([]*dto.ClaudeResponse, error) {
	if event == nil || event.Item == nil {
		return nil, nil
	}
	item := event.Item
	if item.Type == responsesOutputTypeReasoning {
		if stop {
			return s.handleReasoningDone(event, item, estimatedInputTokens)
		}
		return s.handleReasoningAdded(event, item, estimatedInputTokens)
	}
	if item.Type == dto.BuildInCallWebSearchCall {
		// added/状态事件(如 in_progress):识别前四级中首个非空 ID 记录为
		// lastWebSearchID(不标记已发射、不产出块),作为该 item 后续 done 前四级全空时的
		// 最后一级回退来源。done 时按 D-4 映射。
		if !stop {
			if id := s.webSearchProvidedID(event, item); id != "" {
				s.lastWebSearchID = id
			}
			return nil, nil
		}
		return s.emitWebSearchBlocks(event, item, estimatedInputTokens)
	}
	var kind string
	switch item.Type {
	case responsesOutputTypeMessage:
		if item.Role != "" && item.Role != "assistant" {
			return nil, nil
		}
		kind = "text"
	case responsesOutputTypeFunctionCall, responsesOutputTypeCustomToolCall:
		kind = "tool_use"
	default:
		return nil, nil
	}
	var responses []*dto.ClaudeResponse
	switch kind {
	case "text":
		finalize, err := s.finalizeOpenThinking(estimatedInputTokens)
		if err != nil {
			return nil, err
		}
		responses = append(responses, finalize...)
	case "tool_use":
		finalize, err := s.finalizeInterferingThinking(event, estimatedInputTokens)
		if err != nil {
			return nil, err
		}
		responses = append(responses, finalize...)
	}
	block, err := s.ensureBlock(event, kind)
	if err != nil {
		return nil, err
	}
	switch kind {
	case "text":
		var text strings.Builder
		var annotations []any
		for _, content := range item.Content {
			if content.Type != "output_text" {
				continue
			}
			text.WriteString(content.Text)
			annotations = append(annotations, content.Annotations...)
		}
		responses = append(responses, s.mergeFinalValue(block, text.String(), estimatedInputTokens)...)
		responses = append(responses, s.appendAnnotations(block, annotations, estimatedInputTokens, true)...)
	case "tool_use":
		responses = append(responses, s.mergeFinalValue(block, item.ArgumentsString(), estimatedInputTokens)...)
	}
	if stop {
		responses = append(responses, s.stopBlock(block, estimatedInputTokens)...)
	}
	return responses, nil
}

// webSearchProvidedID 提取 web_search 事件前四级来源的首次非空 ID(item.id →
// item.output_item_id → item.call_id → 根事件 item_id),不回退到 lastWebSearchID。
// 供 added/状态事件记录最近已知 ID,以及 done 回退链的前四级来源解析共用。
func (s *ResponsesToClaudeStreamState) webSearchProvidedID(event *dto.ResponsesStreamResponse, item *dto.ResponsesOutput) string {
	for _, candidate := range []string{
		strings.TrimSpace(item.ID),
		strings.TrimSpace(item.OutputItemID),
		strings.TrimSpace(item.CallId),
	} {
		if candidate != "" {
			return candidate
		}
	}
	if event != nil {
		return strings.TrimSpace(event.ItemID)
	}
	return ""
}

// webSearchToolUseID 按 design D-4 的 tool_use_id 回退链解析 web_search_call 的最终
// 非空 tool_use_id:item.id → item.output_item_id → item.call_id → 根事件 item_id →
// 流状态中上一已知 web_search ID。全部为空则返回空串(不产出,避免不可关联结果)。
func (s *ResponsesToClaudeStreamState) webSearchToolUseID(event *dto.ResponsesStreamResponse, item *dto.ResponsesOutput) string {
	if id := s.webSearchProvidedID(event, item); id != "" {
		return id
	}
	return s.lastWebSearchID
}

// webSearchQuery 从 web_search_call item 的 action 对象提取查询词(基线 action.query)。
func (s *ResponsesToClaudeStreamState) webSearchQuery(item *dto.ResponsesOutput) string {
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

// webSearchResultContent 把 web_search_call item 的 results 数组映射为 Claude
// web_search_result 块数组:url 取 result.url(trim 空则跳过该 result);title 取
// result.title(trim 空则回退为 url);page_age 恒为 null。
func (s *ResponsesToClaudeStreamState) webSearchResultContent(item *dto.ResponsesOutput) []any {
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

// emitWebSearchBlocks 在 response.output_item.done 且 item.type=web_search_call 时,
// 按 D-4 产出一组 server_tool_use(input 含 query)+ web_search_tool_result(content 为
// web_search_result 数组)。tool_use_id 恒空的 item 不产出;同一 ID 至多产出一组。
func (s *ResponsesToClaudeStreamState) emitWebSearchBlocks(event *dto.ResponsesStreamResponse, item *dto.ResponsesOutput, estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	toolUseID := s.webSearchToolUseID(event, item)
	if toolUseID == "" {
		return nil, nil
	}
	if _, seen := s.webSearchEmitted[toolUseID]; seen {
		return nil, nil
	}

	// INV-5:web_search 属于内容边界,进入新块前先收尾仍打开的 thinking 块。
	finalize, err := s.finalizeOpenThinking(estimatedInputTokens)
	if err != nil {
		return nil, err
	}
	var responses []*dto.ClaudeResponse
	responses = append(responses, s.ensureMessageStart(estimatedInputTokens)...)
	responses = append(responses, finalize...)

	query := s.webSearchQuery(item)
	responses = append(responses, s.emitServerToolUse(toolUseID, query, estimatedInputTokens)...)
	responses = append(responses, s.emitWebSearchToolResult(toolUseID, item, estimatedInputTokens)...)

	s.webSearchEmitted[toolUseID] = struct{}{}
	s.lastWebSearchID = toolUseID
	return responses, nil
}

// emitServerToolUse 发射一个 server_tool_use content_block:start(空 input)→ 若 query
// 非空发射 input_json_delta {"query":...} → stop。
func (s *ResponsesToClaudeStreamState) emitServerToolUse(toolUseID string, query string, _ int) []*dto.ClaudeResponse {
	index := s.nextBlockIndex
	s.nextBlockIndex++
	content := dto.ClaudeMediaMessage{
		Type:  "server_tool_use",
		Id:    toolUseID,
		Name:  "web_search",
		Input: map[string]any{},
	}
	var responses []*dto.ClaudeResponse
	responses = append(responses, &dto.ClaudeResponse{Type: "content_block_start", Index: &index, ContentBlock: &content})
	if query != "" {
		partial, _ := kitutil.Marshal(map[string]string{"query": query})
		delta := &dto.ClaudeMediaMessage{Type: "input_json_delta", PartialJson: kitutil.GetPointer(string(partial))}
		responses = append(responses, &dto.ClaudeResponse{Type: "content_block_delta", Index: &index, Delta: delta})
	}
	responses = append(responses, &dto.ClaudeResponse{Type: "content_block_stop", Index: &index})
	return responses
}

// emitWebSearchToolResult 发射一个 web_search_tool_result content_block:start(携带
// tool_use_id 与 web_search_result 数组,无结果时为 [])→ stop。
func (s *ResponsesToClaudeStreamState) emitWebSearchToolResult(toolUseID string, item *dto.ResponsesOutput, _ int) []*dto.ClaudeResponse {
	index := s.nextBlockIndex
	s.nextBlockIndex++
	content := s.webSearchResultContent(item)
	if content == nil {
		content = []any{}
	}
	block := dto.ClaudeMediaMessage{
		Type:      "web_search_tool_result",
		ToolUseId: toolUseID,
		Content:   content,
	}
	return []*dto.ClaudeResponse{
		{Type: "content_block_start", Index: &index, ContentBlock: &block},
		{Type: "content_block_stop", Index: &index},
	}
}

func (s *ResponsesToClaudeStreamState) newBlock(kind string) *responsesClaudeStreamBlock {
	block := &responsesClaudeStreamBlock{Index: s.nextBlockIndex, Kind: kind}
	s.nextBlockIndex++
	s.blocks = append(s.blocks, block)
	return block
}

// reasoningEventItemID 按 design identity 优先级提取 reasoning 事件的稳定标识:根事件
// item_id 优先,其次 item.id(与通用 responseStreamEventItemID 的 item.id 优先不同)。
func reasoningEventItemID(event *dto.ResponsesStreamResponse) string {
	if event == nil {
		return ""
	}
	if id := strings.TrimSpace(event.ItemID); id != "" {
		return id
	}
	if event.Item != nil {
		if id := strings.TrimSpace(event.Item.ID); id != "" {
			return id
		}
	}
	return ""
}

// linkReasoningIdentity 按 identity 规则为 reasoning item 状态登记 output_index 与
// item_id 别名,并检测身份冲突(两个稳定标识分别绑定不同 state)。
func (s *ResponsesToClaudeStreamState) linkReasoningIdentity(event *dto.ResponsesStreamResponse, st *responsesReasoningItemState) error {
	if event == nil {
		return nil
	}
	if idx := event.OutputIndex; idx != nil {
		if existing := s.reasoningByOutputIndex[*idx]; existing != nil {
			if existing != st {
				return fmt.Errorf("responses conversion error: output index %d bound to different reasoning item", *idx)
			}
		} else {
			s.reasoningByOutputIndex[*idx] = st
			st.outputIndex = idx
		}
	}
	if itemID := reasoningEventItemID(event); itemID != "" {
		if existing := s.reasoningByItemID[itemID]; existing != nil {
			if existing != st {
				return fmt.Errorf("responses conversion error: item id %q bound to different reasoning item", itemID)
			}
		} else {
			s.reasoningByItemID[itemID] = st
			st.itemID = itemID
		}
	}
	return nil
}

// resolveReasoningState 对 reasoning 事件执行统一 identity 解析(优先 output_index →
// 根 item_id → item.id)。added/summary/done 所有 reasoning 事件共用同一 resolver:
// 同时校验 index/ID 的既有绑定、后到的稳定标识作为同一 state 的 alias 补登记、并
// 检测两个标识已分别绑定不同 state 的冲突。未命中任何绑定 state 时,回退到仍活跃
// (未终态)的 active anonymous;否则返回 nil。
func (s *ResponsesToClaudeStreamState) resolveReasoningState(event *dto.ResponsesStreamResponse) (*responsesReasoningItemState, error) {
	if event == nil {
		return nil, nil
	}
	idx := event.OutputIndex
	itemID := reasoningEventItemID(event)

	var found *responsesReasoningItemState
	if idx != nil {
		found = s.reasoningByOutputIndex[*idx]
	}
	if itemID != "" {
		if st := s.reasoningByItemID[itemID]; st != nil {
			if found != nil && found != st {
				return nil, fmt.Errorf("responses conversion error: output index %d and item id %q bound to different reasoning items", *idx, itemID)
			}
			found = st
		}
	}
	if found != nil {
		// 后到的稳定标识作为同一 state 的 alias 补登记(linkReasoningIdentity 内部再校验冲突)。
		if err := s.linkReasoningIdentity(event, found); err != nil {
			return nil, err
		}
		return found, nil
	}
	if aa := s.activeAnonymousReasoning; aa != nil && !aa.terminal {
		// 回退到 active anonymous 时,若本事件携带稳定标识,一并作为该 state 的 alias
		// 补登记(后到的标识识别此前未知项的匿名身份),并保留冲突校验。
		if err := s.linkReasoningIdentity(event, aa); err != nil {
			return nil, err
		}
		return aa, nil
	}
	return nil, nil
}

func (s *ResponsesToClaudeStreamState) stateForThinkingBlock(block *responsesClaudeStreamBlock) *responsesReasoningItemState {
	for _, st := range s.reasoningItems {
		if st.block == block {
			return st
		}
	}
	return nil
}

// handleReasoningAdded 在 output_item.added(reasoning) 时登记 item 级状态,并捕获
// fallback 签名快照。稳定标识缺失时设为 activeAnonymousReasoning。
func (s *ResponsesToClaudeStreamState) handleReasoningAdded(event *dto.ResponsesStreamResponse, item *dto.ResponsesOutput, _ int) ([]*dto.ClaudeResponse, error) {
	// added 同样纳入统一 resolver:先解析是否已登记/可回退的既有 state(校验绑定/冲突),
	// 未命中再新建。
	st, err := s.resolveReasoningState(event)
	if err != nil {
		return nil, err
	}
	if st != nil {
		// added 阶段命中既有 state 进行登记/绑定校验。若命中来自 active anonymous 回退
		// 而本 added 无稳定标识,意味着第二个无标识 reasoning item 与前一 active
		// anonymous 并存 → 身份歧义,不得把签名归到它。
		if s.activeAnonymousReasoning == st && event.OutputIndex == nil && reasoningEventItemID(event) == "" {
			return nil, fmt.Errorf("responses conversion error: ambiguous anonymous reasoning item")
		}
		if st.fallbackSignature == "" && item.EncryptedContent != "" {
			st.fallbackSignature = item.EncryptedContent
		}
		return nil, nil
	}
	st = &responsesReasoningItemState{fallbackSignature: item.EncryptedContent}
	if err := s.linkReasoningIdentity(event, st); err != nil {
		return nil, err
	}
	s.reasoningItems = append(s.reasoningItems, st)
	if st.outputIndex == nil && st.itemID == "" {
		if s.activeAnonymousReasoning != nil && !s.activeAnonymousReasoning.terminal {
			return nil, fmt.Errorf("responses conversion error: ambiguous anonymous reasoning item")
		}
		s.nextAnonymousReasoning++
		st.anonymousOrdinal = s.nextAnonymousReasoning
		s.activeAnonymousReasoning = st
	}
	return nil, nil
}

// handleReasoningDone 在 output_item.done(reasoning) 时执行终态收尾:首个 done 置
// terminal,重复 done 无输出;按签名的三态规则发射/抑制 signature_delta。
func (s *ResponsesToClaudeStreamState) handleReasoningDone(event *dto.ResponsesStreamResponse, item *dto.ResponsesOutput, estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	st, err := s.resolveReasoningState(event)
	if err != nil {
		return nil, err
	}
	if st == nil {
		if event.OutputIndex == nil && reasoningEventItemID(event) == "" {
			// 无稳定标识且无 active state:身份不可判定,按响应转换错误分流,不得新建 state。
			return nil, fmt.Errorf("responses conversion error: cannot resolve reasoning item for done without identifier")
		}
		// 稳定标识存在但无已登记 state(聚合 finish 路径或 done 先于 added):补建状态。
		st = &responsesReasoningItemState{}
		if err := s.linkReasoningIdentity(event, st); err != nil {
			return nil, err
		}
		s.reasoningItems = append(s.reasoningItems, st)
		st.summarySeen = reasoningOutputText(item) != ""
	}
	if st.terminal {
		return nil, nil
	}
	st.terminal = true
	if s.activeAnonymousReasoning == st {
		s.activeAnonymousReasoning = nil
	}

	// 选用 signature = done 终值优先,否则退回调 fallback 快照;存在性仅以原值是否
	// 非空判断,发射值直接使用原始 encrypted_content(不 TrimSpace,FR-007/INV-2)。
	signature := item.EncryptedContent
	if signature == "" {
		signature = st.fallbackSignature
	}

	// 状态③:提前关闭后 done 迟到。不补发、不重开块,仅做一次诊断留痕。
	if st.earlyClosed {
		if signature != "" && !st.diagnosticRecorded {
			// 有界诊断(signature_dropped_after_early_close):每个被丢弃 item 至多发起一次
			// 记录尝试,无论是否因容量上限被保留,diagnosticRecorded 均置 true 防重复。
			s.recordDroppedAfterEarlyClose(st)
			st.diagnosticRecorded = true
		}
		return nil, nil
	}

	// 状态①:块已因 summary 事件打开(或 done-first 携带 summary 时尚未建块)。合并终值,
	// 签名已知则恰发一次,再 stop。INV-5:此路径可能打开新 thinking 块(done-first),须先
	// finalize 其它仍打开的 thinking 块,再进入自身块的收尾/创建(继续同一块时不收尾)。
	if st.summarySeen {
		var responses []*dto.ClaudeResponse
		finalize, err := s.finalizeReasoningThinking(event, st, estimatedInputTokens)
		if err != nil {
			return nil, err
		}
		responses = append(responses, finalize...)
		block := st.block
		if block == nil {
			block = s.newBlock("thinking")
			st.block = block
		}
		block.Kind = "thinking"
		responses = append(responses, s.mergeFinalValue(block, reasoningOutputText(item), estimatedInputTokens)...)
		if signature != "" && !st.signatureEmitted {
			responses = append(responses, s.emitSignatureDelta(block, signature, estimatedInputTokens)...)
			st.signatureEmitted = true
		}
		responses = append(responses, s.stopBlock(block, estimatedInputTokens)...)
		return responses, nil
	}

	// 状态②:块从未打开但 done 终值非空(signature-only)。先关 text 块,再开空
	// thinking 块输出一次 signature_delta。此 item 无块,若存在其它 item 仍打开的
	// thinking 块,先按 INV-5 收尾再开新块。
	if signature != "" {
		finalize, err := s.finalizeOpenThinking(estimatedInputTokens)
		if err != nil {
			return nil, err
		}
		signatureOnly, err := s.emitSignatureOnly(st, signature, estimatedInputTokens)
		if err != nil {
			return nil, err
		}
		return append(finalize, signatureOnly...), nil
	}
	return nil, nil
}

func (s *ResponsesToClaudeStreamState) emitSignatureOnly(st *responsesReasoningItemState, signature string, estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	var responses []*dto.ClaudeResponse
	if lastText := s.lastByKind["text"]; lastText != nil && !lastText.Stopped {
		responses = append(responses, s.stopBlock(lastText, estimatedInputTokens)...)
	}
	block := s.newBlock("thinking")
	st.block = block
	responses = append(responses, s.emitSignatureDelta(block, signature, estimatedInputTokens)...)
	st.signatureEmitted = true
	responses = append(responses, s.stopBlock(block, estimatedInputTokens)...)
	return responses, nil
}

// emitSignatureDelta 发射一次 content_block_delta,delta 内嵌 type=signature_delta
// 子结构(signature 为原字符串)。
func (s *ResponsesToClaudeStreamState) emitSignatureDelta(block *responsesClaudeStreamBlock, signature string, estimatedInputTokens int) []*dto.ClaudeResponse {
	var responses []*dto.ClaudeResponse
	block.Kind = "thinking"
	responses = append(responses, s.startBlock(block, estimatedInputTokens)...)
	if block.Stopped {
		return responses
	}
	index := block.Index
	responses = append(responses, &dto.ClaudeResponse{
		Type:  "content_block_delta",
		Index: &index,
		Delta: &dto.ClaudeMediaMessage{Type: "signature_delta", Signature: signature},
	})
	return responses
}

// resolveOrCreateReasoningState 在任何副作用(收尾其它块/建块)前解析 reasoning 身份:
// 命中既有或可回退到的 state 直接返回;无稳定标识且无 active anonymous 时报身份不可判定
// 错误(不建块、不改其它已打开块的状态);有稳定标识但尚未登记 state(事件顺序与 added
// 交错)时补建并登记,供后续绑定块。
func (s *ResponsesToClaudeStreamState) resolveOrCreateReasoningState(event *dto.ResponsesStreamResponse) (*responsesReasoningItemState, error) {
	st, err := s.resolveReasoningState(event)
	if err != nil {
		return nil, err
	}
	if st != nil {
		return st, nil
	}
	if event.OutputIndex == nil && reasoningEventItemID(event) == "" {
		// 无稳定标识且无 active anonymous state:身份不可判定,按 D-4 报响应转换错误,
		// 不得继续建块/发射内容,也不得改变其它已打开块的状态。
		return nil, fmt.Errorf("responses conversion error: cannot resolve reasoning item for summary without identifier")
	}
	// 稳定标识存在但尚未登记 state(如 summary 的事件顺序与 added 交错)补建并登记。
	st = &responsesReasoningItemState{}
	if err := s.linkReasoningIdentity(event, st); err != nil {
		return nil, err
	}
	s.reasoningItems = append(s.reasoningItems, st)
	return st, nil
}

// reasoningBlockEntered 在 reasoning summary/content 事件进入 thinking 块时,把已解析的
// item 级状态绑定到块并标记 summarySeen。身份解析须由调用方在副作用之前经
// resolveOrCreateReasoningState 完成,这里只做块绑定与状态标记。
func (s *ResponsesToClaudeStreamState) reasoningBlockEntered(st *responsesReasoningItemState, block *responsesClaudeStreamBlock) {
	st.block = block
	if block != nil && block.Kind == "thinking" {
		st.summarySeen = true
	}
}

// finalizeOpenThinking 实现块收尾:在 text/content 边界事件前,先收尾仍打开的
// thinking 块——已知 fallback 签名则先发一次 signature_delta 再 content_block_stop。
func (s *ResponsesToClaudeStreamState) finalizeOpenThinking(estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	var responses []*dto.ClaudeResponse
	for _, block := range s.blocks {
		if block.Kind != "thinking" || !block.Started || block.Stopped {
			continue
		}
		if st := s.stateForThinkingBlock(block); st != nil {
			st.earlyClosed = true
			// fallback 快照存在性仅以原值是否非空判断,发射值不 TrimSpace。
			if sig := st.fallbackSignature; sig != "" && !st.signatureEmitted {
				responses = append(responses, s.emitSignatureDelta(block, sig, estimatedInputTokens)...)
				st.signatureEmitted = true
			}
		}
		responses = append(responses, s.stopBlock(block, estimatedInputTokens)...)
	}
	return responses, nil
}

// finalizeInterferingThinking 用于即将开始/切换到新内容块的边界(不含继续同一 thinking
// 块的 reasoning 事件):若目标块仍未创建,而此前仍有打开的 thinking 块,先按 INV-5
// 收尾。当本事件会继续同一 thinking 块(按 index/id 或 anonymous 回退解析到)时不收尾,
// 避免把同一块提前关闭。
func (s *ResponsesToClaudeStreamState) finalizeInterferingThinking(event *dto.ResponsesStreamResponse, estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	block := s.findBlock(event)
	if block == nil {
		if last := s.lastByKind["thinking"]; last != nil && !last.Stopped &&
			event.OutputIndex == nil && responseStreamEventItemID(event) == "" {
			block = last
		}
	}
	if block != nil && block.Kind == "thinking" && block.Started && !block.Stopped {
		return nil, nil
	}
	return s.finalizeOpenThinking(estimatedInputTokens)
}

func (s *ResponsesToClaudeStreamState) appendAnnotations(block *responsesClaudeStreamBlock, annotations []any, estimatedInputTokens int, snapshot bool) []*dto.ClaudeResponse {
	if block == nil || block.Kind != "text" || block.Stopped || len(annotations) == 0 {
		return nil
	}
	remaining := annotations
	if snapshot {
		if len(annotations) <= block.AnnotationCount {
			return nil
		}
		remaining = annotations[block.AnnotationCount:]
		block.AnnotationCount = len(annotations)
	} else {
		block.AnnotationCount += len(annotations)
	}
	citations := responsesAnnotationsToClaude(remaining, block.Value.String())
	if len(citations) == 0 {
		return nil
	}
	responses := s.startBlock(block, estimatedInputTokens)
	index := block.Index
	for _, citation := range citations {
		responses = append(responses, &dto.ClaudeResponse{
			Type:  "content_block_delta",
			Index: &index,
			Delta: &dto.ClaudeMediaMessage{Type: "citations_delta", Citation: citation},
		})
	}
	return responses
}

func (s *ResponsesToClaudeStreamState) finish(response *dto.OpenAIResponsesResponse, estimatedInputTokens int) ([]*dto.ClaudeResponse, error) {
	if s.done {
		return nil, nil
	}
	s.applyResponseMetadata(response)
	responses := make([]*dto.ClaudeResponse, 0)
	if response != nil {
		for outputIndex := range response.Output {
			index := outputIndex
			item := response.Output[outputIndex]
			event := &dto.ResponsesStreamResponse{OutputIndex: &index, ItemID: item.ID, Item: &item}
			itemResponses, err := s.applyOutputItem(event, estimatedInputTokens, true)
			if err != nil {
				return nil, err
			}
			responses = append(responses, itemResponses...)
		}
	}
	for _, block := range s.blocks {
		responses = append(responses, s.stopBlock(block, estimatedInputTokens)...)
	}
	responses = append(responses, s.ensureMessageStart(estimatedInputTokens)...)
	stopReason := responsesClaudeStopReason(response, s.sawToolCall)
	usage := sharedclaude.UsageFromOpenAI(s.Usage)
	responses = append(responses,
		&dto.ClaudeResponse{
			Type:  "message_delta",
			Usage: usage,
			Delta: &dto.ClaudeMediaMessage{StopReason: &stopReason},
		},
		&dto.ClaudeResponse{Type: "message_stop"},
	)
	s.done = true
	return responses, nil
}

func separatedResponsesDelta(delta string) string {
	if strings.HasPrefix(delta, "\n\n") {
		return delta
	}
	if strings.HasPrefix(delta, "\n") {
		return "\n" + delta
	}
	return "\n\n" + delta
}

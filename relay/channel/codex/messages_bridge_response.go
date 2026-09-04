package codex

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"

	"github.com/gin-gonic/gin"
)

// 流内成功终态(design I-3 钉死):到达即成功,usage 缺失一律走统一估算回填后正常交付,
// message_delta/message_stop 由流式转换器在本 chunk 的 finish() 阶段产出。
// 注意:response.done 未获 design I-3 批准为成功终态——若出现,按未定义/非终态事件处理
// (failStream 两段分流),不得提前按 (usage, nil) 完成。
const (
	bridgeStreamTerminalCompleted  = "response.completed"
	bridgeStreamTerminalIncomplete = "response.incomplete"
)

// 流内错误终态:按下述错误规范化规则提取并规范化,pre/post-commit 共用同一对象。
const (
	bridgeStreamEventError   = "error"
	bridgeStreamEventFailed  = "response.failed"
	bridgeStreamEventRespErr = "response.error"
)

// bridgeResponseSession 是单次 controller attempt 的响应会话状态,stream 与 aggregate
// 两处理器共享。显式持有原始 c/info、只读 options 与只读名称映射、relaykit 流式转换
// 状态与帧写器。本对象只负责转换协调(delayed-commit 两态分流),不实现 framing 与
// 名称算法(分别委托帧写器与名称映射)。
//
// 生命周期 = 单次 attempt;retry 每次新建,返回即丢弃。帧写器在两处理器入口各自构造,
// 请求内二处理器互斥只运行其一,故一次请求一实例。
type bridgeResponseSession struct {
	c       *gin.Context
	info    *relaycommon.RelayInfo
	options bridgeRequestOptions
	mapping codexToolNameMapping
	writer  *bridgeResponseWriter
	stream  *relayconvert.ResponseStreamState
}

// newBridgeResponseSession 基于请求会话构造响应会话。
// relaykit 流式状态经既有 registry 以 responses→claude_messages 转换器创建;
// ID 取自请求日志 ID、Model 取上游模型名。流状态初始化失败属响应处理失败,返回错误。
func newBridgeResponseSession(c *gin.Context, info *relaycommon.RelayInfo, reqSession *bridgeRequestSession) (*bridgeResponseSession, *types.NewAPIError) {
	if c == nil || info == nil {
		return nil, types.NewError(errors.New("bridge response session context/info is nil"), types.ErrorCodeBadResponse)
	}
	opts := bridgeRequestOptions{}
	var mapping codexToolNameMapping
	if reqSession != nil {
		opts = reqSession.options
		mapping = reqSession.mapping
	}
	state, err := relayconvert.NewResponseStreamStateByID(relayconvert.ConverterOpenAIResponsesToClaudeMessages, relayconvert.ResponseStreamOptions{
		ID:      helper.GetResponseID(c),
		Model:   info.UpstreamModelName,
		Created: common.GetTimestamp(),
	})
	if err != nil {
		return nil, types.NewError(err, types.ErrorCodeBadResponse)
	}
	return &bridgeResponseSession{
		c:       c,
		info:    info,
		options: opts,
		mapping: mapping,
		stream:  state,
	}, nil
}

// handleBridgeStream 是 bridge 的 delayed-commit 流处理器。
// 复用 StreamScannerHandler 的 timeout/client-gone/StreamStatus 机制;SSE headers 于
// StreamScannerHandler 启动时写入 header map(不 commit),pre-commit 出口处的 headers
// 与 event_stream_headers_set 标记由入口编排方统一清除,本函数不自行清理。
//
// 前置:session.c/info 非 nil;session.options.downstreamStream==true;resp 为经上游
// 响应校验通过的响应(非 nil + 非 nil Body),body 由本函数负责关闭。
//
// 两态返回:
//   - 成功终态 completed/incomplete(含缺 usage 按统一估算规则回填)→ (usage, nil);
//   - 下游已开始响应(committed)时 error/failed/EOF/解析失败 → 一次 SSE error + 关流 +
//     估算 (usage, nil);
//   - 已开始响应后的 client_gone/idle timeout → 估算 (usage, nil)(无客户端可见动作);
//   - 下游未开始响应(pre-commit)时 error/failed/解析失败/EOF/超时/断连/panic → (nil, err)。
func handleBridgeStream(session *bridgeResponseSession, resp *http.Response) (*dto.Usage, *types.NewAPIError) {
	if session == nil || session.c == nil || session.info == nil {
		return nil, types.NewError(errors.New("bridge stream requires response session"), types.ErrorCodeBadResponse)
	}
	if !session.options.downstreamStream {
		return nil, types.NewError(errors.New("bridge stream requires downstream stream mode"), types.ErrorCodeBadResponse)
	}
	if session.stream == nil {
		return nil, types.NewError(errors.New("bridge stream state is required"), types.ErrorCodeBadResponse)
	}
	if resp == nil || resp.Body == nil {
		return nil, types.NewError(errors.New("invalid upstream response"), types.ErrorCodeBadResponse)
	}
	defer resp.Body.Close()

	writer := newBridgeResponseWriter(session.c)
	session.writer = writer

	// 诊断留痕:本次 bridge 以流形态向下交付。
	session.info.RecordConversionDiagnostics(session.c, []types.ConversionDiagnostic{
		{
			Code:     "downstream_stream",
			Path:     "stream",
			Message:  "bridge downstream stream=true",
			Severity: types.ConversionDiagnosticWarning,
			From:     types.RelayFormatOpenAIResponses,
			To:       types.RelayFormatClaude,
		},
	})

	var terminal bool
	var preCommitErr *types.NewAPIError
	var postCommitDelivered bool

	writerErrFrame := func(apiErr *types.NewAPIError) {
		// 下游已开始响应后的唯一交付形态:SSE `event: error`,data 为与 HTTP 完全同构的
		// Claude error envelope;best-effort,写失败不再尝试(连接已开始响应)。
		if apiErr == nil || writer == nil {
			return
		}
		_ = writer.writeSSEFrame(dto.ClaudeResponse{Type: "error", Error: apiErr.ToClaudeError()})
	}

	// 按 committed 两段分流处理一个流内错误事件 / 解析失败 / 转换失败。
	failStream := func(apiErr *types.NewAPIError, sr *helper.StreamResult) {
		if writer.committed() {
			// 下游已开始响应:一次 SSE error + 关流,返回 (usage估算, nil)。
			writerErrFrame(apiErr)
			postCommitDelivered = true
			sr.Stop(nil)
			return
		}
		// 下游未开始响应:不写任何字节,(nil, err)。
		preCommitErr = apiErr
		sr.Stop(nil)
	}

	// 响应处理失败(转换 / finalize 等本地错误)按 committed 两段分流。
	failConvert := func(err error, sr *helper.StreamResult) {
		failStream(types.NewError(err, types.ErrorCodeBadResponse), sr)
	}

	// 下游帧组帧 / 写出失败按帧写器稳定边界分流:组帧失败与底层写出失败经 I-8 writer 的
	// 类型化错误(bridgeFrameError vs bridgeWriteError)区分(A-20 vs A-21)。
	failFrame := func(err error, sr *helper.StreamResult) {
		// 组帧/序列化失败发生于任何 Write 之前;若尚未提交 → pre-commit,上抛(A-20)。
		if !writer.committed() {
			preCommitErr = types.NewError(err, types.ErrorCodeJsonMarshalFailed)
			sr.Stop(nil)
			return
		}
		// 已 commit:
		var frameErr *bridgeFrameError
		if errors.As(err, &frameErr) {
			// 组帧失败但已 commit(此前帧已交付,如已写 message_start 后再组帧失败)→
			// 一次 SSE error + 关流(A-19 形态)。
			writerErrFrame(types.NewError(frameErr, types.ErrorCodeBadResponse))
			postCommitDelivered = true
			sr.Stop(nil)
			return
		}
		// 底层写出失败(c.Writer.Write 返回 error / 短写;gin v1.9.1 实测写失败后 Written()
		// 恒 true,含首帧)→ 视同断连静默关流(A-21),不再尝试 error 帧或 HTTP envelope,
		// 按已交付内容估算 (usage, nil)。
		postCommitDelivered = true
		sr.Stop(nil)
	}

	writeResults := func(results []relayconvert.ResponseResult) error {
		for i := range results {
			frame, ok := results[i].Value.(*dto.ClaudeResponse)
			if !ok || frame == nil {
				continue
			}
			session.restoreMappedNames(frame)
			if err := writer.writeSSEFrame(*frame); err != nil {
				return err
			}
		}
		return nil
	}

	convertChunk := func(sr *helper.StreamResult, event *dto.ResponsesStreamResponse) {
		results, err := service.ConvertStreamResponseChunk(session.c, session.info, session.stream, event)
		if err != nil {
			failConvert(err, sr)
			return
		}
		if err := writeResults(results); err != nil {
			failFrame(err, sr)
		}
	}

	helper.StreamScannerHandler(session.c, resp, session.info, func(data string, sr *helper.StreamResult) {
		var event dto.ResponsesStreamResponse
		if err := common.UnmarshalJsonStr(data, &event); err != nil {
			// 流内解析失败:统一构造读错误并两段分流。
			failStream(normalizeStreamReadError(fmt.Errorf("responses stream read error: %v", err)), sr)
			return
		}
		switch event.Type {
		case bridgeStreamTerminalCompleted, bridgeStreamTerminalIncomplete:
			// 成功终态:转换产出 message_delta + message_stop,随后 finalize 兜底收尾
			// (已终态守卫幂等,重复不产出额外帧)。
			results, cerr := service.ConvertStreamResponseChunk(session.c, session.info, session.stream, &event)
			if cerr != nil {
				failConvert(cerr, sr)
				return
			}
			if werr := writeResults(results); werr != nil {
				failFrame(werr, sr)
				return
			}
			finals, ferr := service.FinalizeStreamResponse(session.c, session.info, session.stream)
			if ferr != nil {
				failConvert(ferr, sr)
				return
			}
			if werr := writeResults(finals); werr != nil {
				failFrame(werr, sr)
				return
			}
			terminal = true
			sr.Done()
		case "response.done":
			// response.done 未获 design I-3 批准为成功终态;按未定义/非终态事件处理,
			// 经 failStream 两段分流,不将未定义事件提前收敛为 (usage, nil)。
			failStream(normalizeStreamEventError(&event), sr)
		case bridgeStreamEventError, bridgeStreamEventFailed, bridgeStreamEventRespErr:
			// 流内错误终态:规范化构造并两段分流。
			failStream(normalizeStreamEventError(&event), sr)
		default:
			convertChunk(sr, &event)
		}
	})

	// StreamScannerHandler 返回后,基于终态标志 / 已捕获错误 / 流结束原因分类。
	switch {
	case terminal:
		// 正常终态(含缺 usage 估算)。
		return session.computeUsage(), nil
	case preCommitErr != nil:
		// pre-commit 错误已捕获 → (nil, err)。
		return nil, preCommitErr
	case postCommitDelivered:
		// 已交付 SSE error / 静默关流 → (usage估算, nil)。
		return session.computeUsage(), nil
	}

	// 无终态、无显式捕获:按流结束原因 + 当前提交状态分类。
	reason := session.info.StreamStatus.EndReason
	committed := writer.committed()
	switch reason {
	case relaycommon.StreamEndReasonEOF:
		// 终态前 clean EOF(scanner 无读取错误)。
		if !committed {
			return nil, normalizeStreamCleanEOF()
		}
		writerErrFrame(normalizeStreamCleanEOF())
		return session.computeUsage(), nil
	case relaycommon.StreamEndReasonScannerErr:
		// 流内解析失败 / scanner 非 EOF 错误。
		rerr := session.info.StreamStatus.EndError
		if rerr == nil {
			rerr = errors.New("responses stream read error")
		}
		normErr := normalizeStreamReadError(rerr)
		if !committed {
			return nil, normErr
		}
		writerErrFrame(normErr)
		return session.computeUsage(), nil
	case relaycommon.StreamEndReasonClientGone:
		// 客户端断连:已开始响应 → 估算,nil;未开始 → 上抛 SkipRetry。
		if !committed {
			return nil, types.NewErrorWithStatusCode(errors.New("client disconnected"), types.ErrorCodeBadResponse, http.StatusInternalServerError, types.ErrOptionWithSkipRetry())
		}
		return session.computeUsage(), nil
	case relaycommon.StreamEndReasonTimeout:
		// 上游 idle 超时:已开始响应 → 保持 200 直接关流(不写 error 帧)。
		if !committed {
			return nil, types.NewErrorWithStatusCode(errors.New("upstream stream idle timeout"), types.ErrorCodeBadResponse, http.StatusInternalServerError)
		}
		return session.computeUsage(), nil
	case relaycommon.StreamEndReasonPanic:
		// panic:固定 500 不参与状态映射;已开始响应 → 估算,nil。
		if !committed {
			return nil, types.NewError(errors.New("bridge stream panic"), types.ErrorCodeInvalidRequest)
		}
		return session.computeUsage(), nil
	default:
		// HandlerStop 等未在闭包中捕获的结束(不应发生):按提交状态兜底。
		if !committed {
			return nil, types.NewError(errors.New("bridge stream stopped unexpectedly"), types.ErrorCodeBadResponse)
		}
		return session.computeUsage(), nil
	}
}

// handleBridgeAggregate 是 bridge 的聚合响应处理器(downstreamStream=false)。
// 到达成功终态(response.completed/incomplete)前不向下游写任何字节;终态后组装单个
// Claude JSON 经 I-8 writer writeJSON 写出。本路径自有扫描循环 + NewStreamScanner
// (不经 StreamScannerHandler),恒不设 SSE headers。
//
// 两态返回:
//   - 成功终态 completed/incomplete → 写 200 单 JSON,(usage, nil);
//     缺 usage 时按统一估算规则从已累积流状态回填(usage估算, nil)。
//   - failed/流内 error/解析失败/终态前 EOF/断连/idle 超时/pre-commit panic
//     → (nil, err)(聚合态恒 pre-commit 语义,controller 写 HTTP envelope)。
//   - 终态后响应处理失败(A-22)两分:组帧/marshal 失败 → (nil, err);
//     writeJSON 写出失败 → 关流 + 估算 (usage, nil)。
func handleBridgeAggregate(session *bridgeResponseSession, resp *http.Response) (outUsage *dto.Usage, outErr *types.NewAPIError) {
	if session == nil || session.c == nil || session.info == nil {
		return nil, types.NewError(errors.New("bridge aggregate requires response session"), types.ErrorCodeBadResponse)
	}
	if session.options.downstreamStream {
		return nil, types.NewError(errors.New("bridge aggregate requires non-stream downstream mode"), types.ErrorCodeBadResponse)
	}
	if session.stream == nil {
		return nil, types.NewError(errors.New("bridge aggregate stream state is required"), types.ErrorCodeBadResponse)
	}
	if resp == nil || resp.Body == nil {
		return nil, types.NewError(errors.New("invalid upstream response"), types.ErrorCodeBadResponse)
	}
	defer resp.Body.Close()

	// panic 唯一裁决条件 = recover 时 committed():false → A-25 固定 500 envelope;
	// true → A-28 关流 + 估算 (usage估算, nil)。aggPanic 为统一裁决函数,主循环 recover
	// defer 与 scanner goroutine 回传的 panic 共用同一裁决逻辑;defer 在本函数最前面安装
	// (先于 StreamStatus 初始化 / writer 构造 / 写诊断 / 启动 scanner),保证覆盖全部代码
	// 路径;aggPanic 对 StreamStatus 尚未来得及初始化的早期 panic 作 nil 安全处理(视同
	// pre-commit 留痕缺失,仍正常返回 (nil, err))。
	var writer *bridgeResponseWriter
	committed := func() bool { return writer != nil && writer.committed() }
	aggPanic := func(r any) (*dto.Usage, *types.NewAPIError) {
		if session.info.StreamStatus != nil {
			session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonPanic, fmt.Errorf("bridge aggregate panic: %v", r))
		}
		if committed() {
			return session.computeUsage(), nil
		}
		return nil, types.NewError(fmt.Errorf("bridge aggregate panic: %v", r), types.ErrorCodeInvalidRequest)
	}
	defer func() {
		if r := recover(); r != nil {
			outUsage, outErr = aggPanic(r)
		}
	}()

	// StreamStatus 随后初始化:panic 裁决留痕与断连/超时分类依赖它。
	session.info.StreamStatus = relaycommon.NewStreamStatus()

	writer = newBridgeResponseWriter(session.c)
	session.writer = writer

	// 诊断留痕:本次 bridge 以聚合(非流)形态向下交付。
	session.info.RecordConversionDiagnostics(session.c, []types.ConversionDiagnostic{
		{
			Code:     "downstream_stream",
			Path:     "aggregate",
			Message:  "bridge downstream stream=false",
			Severity: types.ConversionDiagnosticWarning,
			From:     types.RelayFormatOpenAIResponses,
			To:       types.RelayFormatClaude,
		},
	})

	scanner := helper.NewStreamScanner(resp.Body)
	scanner.Split(bufio.ScanLines)

	// 自有扫描 goroutine 把解析后的行/结束标记投递到 lines 通道;主循环 select 监听
	// 行、idle 超时与 request context 取消(客户端断连)。
	ctx := session.c.Request.Context()
	streamingTimeout := time.Duration(constant.StreamingTimeout) * time.Second

	type scannerMsg struct {
		data     []byte
		eof      bool
		err      error
		panicVal any
	}
	lines := make(chan scannerMsg, 2)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		defer close(lines)
		// scanner goroutine 内 panic:recover 后把 panic 值回传主循环经 aggPanic 统一
		// 裁决(scanner 只读上游、从不写下游,其 panic 恒 pre-commit → (nil, err)),避免
		// goroutine 泄出崩掉整个进程。
		defer func() {
			if r := recover(); r != nil {
				select {
				case lines <- scannerMsg{panicVal: r}:
				case <-stop:
				case <-ctx.Done():
				}
			}
		}()
		for scanner.Scan() {
			line := append([]byte(nil), scanner.Bytes()...)
			select {
			case lines <- scannerMsg{data: line}:
			case <-stop:
				return
			case <-ctx.Done():
				return
			}
		}
		// 扫描自然结束:投递 eof 标记(含 scanner.Err,clean EOF 时为 nil)。
		select {
		case lines <- scannerMsg{eof: true, err: scanner.Err()}:
		case <-stop:
		case <-ctx.Done():
		}
	}()

	accumulator := relayconvert.NewResponsesBufferedAccumulator()
	var finalResponse *dto.OpenAIResponsesResponse
	terminal := false
	var apiErr *types.NewAPIError

	idleTimer := time.NewTimer(streamingTimeout)
	defer idleTimer.Stop()

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				// 通道意外关闭(未投递 eof 标记,不应发生):按 clean EOF 兜底。
				if terminal {
					goto assemble
				}
				return nil, normalizeStreamCleanEOF()
			}
			if line.panicVal != nil {
				// scanner goroutine 内 pre-commit panic:经唯一裁决条件分流。
				return aggPanic(line.panicVal)
			}
			if line.eof {
				if line.err != nil {
					session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonScannerErr, line.err)
					return nil, normalizeStreamReadError(line.err)
				}
				// 终态前 clean EOF / 已终态自然结束。
				if terminal {
					session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonDone, nil)
					goto assemble
				}
				session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonEOF, nil)
				return nil, normalizeStreamCleanEOF()
			}
			terminal, finalResponse, apiErr = session.aggregateFeedLine(line.data, accumulator, finalResponse)
			if apiErr != nil {
				return nil, apiErr
			}
			if terminal {
				session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonDone, nil)
				goto assemble
			}
			idleTimer.Reset(streamingTimeout)
		case <-idleTimer.C:
			session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonTimeout, nil)
			return nil, types.NewErrorWithStatusCode(errors.New("upstream stream idle timeout"), types.ErrorCodeBadResponse, http.StatusInternalServerError)
		case <-ctx.Done():
			session.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonClientGone, ctx.Err())
			return nil, types.NewErrorWithStatusCode(errors.New("client disconnected"), types.ErrorCodeBadResponse, http.StatusInternalServerError, types.ErrOptionWithSkipRetry())
		}
	}

assemble:
	// 组装单个 Claude message JSON 并写出(A-15/A-16)。
	if finalResponse == nil {
		finalResponse = &dto.OpenAIResponsesResponse{
			ID:        helper.GetResponseID(session.c),
			Model:     session.info.UpstreamModelName,
			Status:    []byte(`"completed"`),
			CreatedAt: int(time.Now().Unix()),
		}
	}
	accumulator.SupplementResponseOutput(finalResponse)

	result, err := service.ConvertResponse(session.c, session.info, session.info.RelayFormat, finalResponse)
	if err != nil {
		// A-22①:ConvertResponse / 组帧失败(pre-commit)。
		return nil, types.NewError(err, types.ErrorCodeBadResponse)
	}
	claudeResp, ok := result.Value.(*dto.ClaudeResponse)
	if !ok {
		return nil, types.NewError(errors.New("bridge aggregate conversion returned invalid claude response"), types.ErrorCodeBadResponse)
	}
	session.restoreMappedNames(claudeResp)

	// 终态含 usage → 以权威值落流状态,computeUsage 原样返回;否则按统一估算规则回填。
	if finalResponse.Usage != nil {
		if u := relayconvert.UsageFromResponsesUsage(finalResponse.Usage); u != nil {
			session.stream.SetUsage(u)
		}
	}

	// A-22 两分:writeJSON 内 marshal 在任何 Write 前完成,组帧失败时 committed()==false
	// → (nil, err);Write 失败时 committed()==true → 关流 + 估算 (usage, nil)。
	if werr := writer.writeJSON(http.StatusOK, *claudeResp); werr != nil {
		if writer.committed() {
			return session.computeUsage(), nil
		}
		return nil, types.NewError(werr, types.ErrorCodeJsonMarshalFailed)
	}
	return session.computeUsage(), nil
}

// aggregateFeedLine 处理聚合路径的单个上游 SSE data 行:解析事件、累积到 buffered
// accumulator 与流状态(估算/诊断),识别成功终态。返回 (terminal, finalResponse,
// apiErr);terminal 为 true 时 finalResponse 为成功终态事件携带的完整响应。
func (s *bridgeResponseSession) aggregateFeedLine(data []byte, accumulator *relayconvert.ResponsesBufferedAccumulator, finalResponse *dto.OpenAIResponsesResponse) (bool, *dto.OpenAIResponsesResponse, *types.NewAPIError) {
	line := strings.TrimSpace(string(data))
	if !strings.HasPrefix(line, "data:") {
		return false, finalResponse, nil
	}
	payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
	if payload == "" || payload == "[DONE]" {
		return false, finalResponse, nil
	}

	var event dto.ResponsesStreamResponse
	// 解析失败:聚合态恒 pre-commit → (nil, err)。normalizeStreamReadError 自带
	// "responses stream read error: " 前缀,此处直接传原始 err(不二次包装),最终 message
	// 恰为 "responses stream read error: <err>"。
	if err := common.UnmarshalJsonStr(payload, &event); err != nil {
		s.info.StreamStatus.SetEndReason(relaycommon.StreamEndReasonScannerErr, err)
		return false, finalResponse, normalizeStreamReadError(err)
	}

	switch event.Type {
	case bridgeStreamTerminalCompleted, bridgeStreamTerminalIncomplete:
		accumulator.ProcessEvent(&event)
		if cerr := s.feedAggregateStream(&event); cerr != nil {
			return false, finalResponse, types.NewError(cerr, types.ErrorCodeBadResponse)
		}
		return true, event.Response, nil
	case bridgeStreamEventError, bridgeStreamEventFailed, bridgeStreamEventRespErr, "response.done":
		// 流内错误(含未获批的 response.done):规范化构造并返回 (nil, err)。
		return false, finalResponse, normalizeStreamEventError(&event)
	default:
		accumulator.ProcessEvent(&event)
		if cerr := s.feedAggregateStream(&event); cerr != nil {
			return false, finalResponse, types.NewError(cerr, types.ErrorCodeBadResponse)
		}
		return false, finalResponse, nil
	}
}

// feedAggregateStream 把单个上游事件喂入流状态用于估算/诊断累积,丢弃流式转换产出
// (聚合态不需其帧);转换失败返回底层错误。
func (s *bridgeResponseSession) feedAggregateStream(event *dto.ResponsesStreamResponse) error {
	_, err := service.ConvertStreamResponseChunk(s.c, s.info, s.stream, event)
	return err
}

// computeUsage 按统一估算规则产出本次 attempt 的最终 usage,与直连
// OaiResponsesStreamHandler 的 scanner 结束估算口径语义级对齐:
// 仅当流状态 usage **缺失**(Usage() 返回 nil,即终态未提供权威 usage)时才整量估算——
// CompletionTokens 按已累积输出文本经既有 tokenizer / 估算器计算,
// PromptTokens 经 info.GetEstimatePromptTokens() 回填,估算事件以 UsageSource='estimated'
// 留痕;权威 usage 存在时不覆盖其任何字段,仅按统一规则在 PromptTokens 缺省(0)时回填。
func (s *bridgeResponseSession) computeUsage() *dto.Usage {
	if s == nil {
		return &dto.Usage{}
	}
	usage := s.stream.Usage()
	if usage == nil {
		// usage 缺失:从已累积流状态全量估算。
		usage = &dto.Usage{}
		usage.UsageSource = "estimated"
		if text := s.stream.UsageText(); text != "" {
			usage.CompletionTokens = service.CountTextToken(text, s.info.UpstreamModelName)
		}
	}
	// 统一规则:PromptTokens 缺省(0)即经既有估算回填,不依赖 completion 是否非零。
	if usage.PromptTokens == 0 {
		usage.PromptTokens = s.info.GetEstimatePromptTokens()
	}
	usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens
	if usage.BillingUsage != nil {
		usage.BillingUsage = dto.CloneBillingUsageWithEstimatedCompletion(usage.BillingUsage, usage.CompletionTokens)
	}
	return usage
}

// restoreMappedBlock 对单个 content block 还原工具名 / call_id:命中名称映射还原原值,
// 未命中原样透传。只处理 tool_use / server_tool_use / web_search_tool_result 名称与 id。
func (s *bridgeResponseSession) restoreMappedBlock(cb *dto.ClaudeMediaMessage) {
	if cb == nil {
		return
	}
	switch cb.Type {
	case "tool_use", "server_tool_use":
		cb.Name = s.mapping.restoreName(cb.Name)
		cb.Id = s.mapping.restoreCallID(cb.Id)
	case "web_search_tool_result":
		cb.ToolUseId = s.mapping.restoreCallID(cb.ToolUseId)
	}
}

// restoreMappedNames 对转换产出的单个 Claude 帧做工具名 / call_id 还原:覆盖流式
// ContentBlock 单块与聚合 Content 内容块数组两种形态。
func (s *bridgeResponseSession) restoreMappedNames(frame *dto.ClaudeResponse) {
	if frame == nil {
		return
	}
	s.restoreMappedBlock(frame.ContentBlock)
	for i := range frame.Content {
		s.restoreMappedBlock(&frame.Content[i])
	}
}

// normalizeStreamEventError 按流内错误规范化规则构造上游流内错误,统一 WithClaudeError
// (ClaudeError{t,m}, 500),error.type 完全由规范化规则确定(保留上游 type;缺省 api_error;
// invalid_request / cyber_policy → invalid_request_error)。m 链:嵌套 message → 根级
// Message → 嵌套 code → t。
func normalizeStreamEventError(event *dto.ResponsesStreamResponse) *types.NewAPIError {
	var errObj *types.OpenAIError
	switch event.Type {
	case bridgeStreamEventError:
		errObj = dto.GetOpenAIError(event.Error)
	case bridgeStreamEventFailed, bridgeStreamEventRespErr:
		if event.Response != nil {
			errObj = event.Response.GetOpenAIError()
		}
	}
	if errObj == nil {
		// 提取不到错误对象 → 泛化。
		return types.WithClaudeError(types.ClaudeError{
			Type:    "api_error",
			Message: fmt.Sprintf("responses stream error: %s", event.Type),
		}, 500)
	}
	// t 链:上游 type(空 → 根级 error_type → api_error);invalid_request / cyber_policy 特化。
	t := errObj.Type
	if t == "" {
		t = event.ErrorType
	}
	if t == "invalid_request" || codeString(errObj.Code) == "cyber_policy" {
		t = "invalid_request_error"
	}
	if t == "" {
		t = "api_error"
	}
	// m 链:嵌套 message → 根级 Message → 嵌套 code → t。
	m := errObj.Message
	if m == "" {
		m = event.Message
	}
	if m == "" {
		m = codeString(errObj.Code)
	}
	if m == "" {
		m = t
	}
	return types.WithClaudeError(types.ClaudeError{Type: t, Message: m}, 500)
}

// normalizeStreamReadError 构造流内读取 / 解析错误:t=api_error,
// m="responses stream read error: <err>"。
func normalizeStreamReadError(err error) *types.NewAPIError {
	return types.WithClaudeError(types.ClaudeError{
		Type:    "api_error",
		Message: fmt.Sprintf("responses stream read error: %v", err),
	}, 500)
}

// normalizeStreamCleanEOF 构造终态前 clean EOF:t=api_error,
// m="upstream stream ended before terminal event"(err 经 WithClaudeError 确定性非 nil)。
func normalizeStreamCleanEOF() *types.NewAPIError {
	return types.WithClaudeError(types.ClaudeError{
		Type:    "api_error",
		Message: "upstream stream ended before terminal event",
	}, 500)
}

// codeString 把 OpenAIError.Code(any) 转字符串,m 链回退来源。
func codeString(code any) string {
	if s, ok := code.(string); ok {
		return s
	}
	if code != nil {
		return fmt.Sprintf("%v", code)
	}
	return ""
}

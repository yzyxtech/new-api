package codex

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/QuantumNous/new-api/logger"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/service"

	"github.com/gin-gonic/gin"
)

// bridgeNotSSEBodyPreviewLimit 是 A-24 错误从上游响应体提取 message 时读取的字节上限。
const bridgeNotSSEBodyPreviewLimit = 4096

// BridgeClaudeMessages 是 codex bridge 的唯一导出编排入口(design I-1),统一编排
// 转换 → 规范化 → 上游 → stream/聚合响应 → usage 的唯一责任。ClaudeHelper 仅在
// bridge 谓词命中(选中渠道=Codex 且 policy 命中)时调用它;对既有宽接口 `channel.Adaptor`
// 的类型断言失败在调用方按 A-29 处理,本函数签名层已收窄为 concrete *Adaptor。
//
// 两态结果契约(调用方处理规则为契约的一部分):
//   - (usage != nil, nil):bridge 已完成本次尝试的响应处理职责(正常终态 / post-commit
//     失败 / 中断 / panic 收尾,含按统一估算规则估算的 usage,可为零)。调用方不再写
//     controller envelope、不重试,以既有方式 PostTextConsumeQuota 差额结算后返回。
//   - (nil, err != nil):pre-commit 失败,未写任何下游字节。调用方上抛,controller 写
//     HTTP error envelope,重试与预扣回滚走既有路径。
//   - 禁止 (usage != nil, err != nil) 与 (nil, nil)。
//
// status mapping 唯一责任方在此:对经 (nil, err) 返回的错误,在返回前由 resetStatusCodeOnce
// 完成恰好一次 service.ResetStatusCode(A-1/A-13/A-25/A-26/A-27 等固定码经该 helper 跳过)。
// pre-commit 出口统一清理下游 SSE headers 与 event_stream_headers_set 标记,保证 controller
// `c.JSON` 写出 application/json。
//
// 前置:c/info/adaptor/request 均非 nil;request 只读不修改。入口创建 D-5b
// bridgeRequestOptions/session,上游调用只使用 attempt-local RelayInfo 值副本与
// package-private adaptor wrapper;原始 RelayInfo 标量不被临时改写,gin context 不承载
// bridge mode 或名称映射。
func BridgeClaudeMessages(c *gin.Context, info *relaycommon.RelayInfo, adaptor *Adaptor, request *dto.ClaudeRequest) (*dto.Usage, *types.NewAPIError) {
	// 前置:c/info/adaptor/request 均非 nil(c.Request 亦须存在,上游调用以此发请求)。
	if c == nil || info == nil || adaptor == nil || request == nil {
		return nil, types.NewError(errors.New("bridge requires non-nil context, relay info, adaptor and request"), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}
	if c.Request == nil {
		return nil, types.NewError(errors.New("bridge requires request context"), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}

	// 每 bridge 请求一条英文摘要日志(同 request ID,不逐条记录 SSE 正文,NFR-004)。
	logger.LogDebug(c, "bridge claude messages -> openai responses: id=%s model=%s stream=%t",
		helper.GetResponseID(c), info.OriginModelName, info.IsStream)

	// 入口创建 D-5b bridgeRequestOptions/session:attempt-local RelayInfo 浅副本 +
	// 请求作用域名称映射,随本尝试返回即丢弃;上游调用只经此副本与 wrapper,不污染原始 RelayInfo。
	mapping := newCodexToolNameMapping(nil)
	session := newBridgeRequestSession(info, info.IsStream, *mapping)

	// 编排第 1 步:转换(Claude Messages -> OpenAI Responses)。诊断写入原始 info。
	result, err := service.ConvertRequest(c, info, types.RelayFormatOpenAIResponses, request)
	if err != nil {
		// A-1:bridge 转换失败,用户输入类,固定 400、skip、不参与 mapping。
		return nil, types.NewErrorWithStatusCode(err, types.ErrorCodeInvalidRequest, http.StatusBadRequest, types.ErrOptionWithSkipRetry())
	}
	responsesReq, ok := result.Value.(*dto.OpenAIResponsesRequest)
	if !ok {
		// A-27①:转换结果类型断言失败,恒 SkipRetry、不参与 mapping。
		return nil, types.NewError(fmt.Errorf("expected OpenAI responses request, got %T", result.Value), types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
	}
	// 记录 consume log request_conversion = Claude Messages -> OpenAI Responses(与 via-responses 同机制)。
	relaycommon.AppendRequestConversionFromRequest(info, responsesReq)

	// 编排第 2 步:prepareBridgeRequest(强制 stream/include、字段清理、64 字节收缩、唯一 marshal)。
	reqBody, err := prepareBridgeRequest(info, *responsesReq, &session.mapping)
	if err != nil {
		// A-27:请求构造失败,错误已含精确 ErrorCode/Status/SkipRetry,不参与 mapping。
		return nil, toBridgeNewAPIError(err, types.ErrorCodeConvertRequestFailed)
	}

	// 编排第 3 步:doBridgeRequest(transport;A-26 已在内部包装)。
	resp, err := doBridgeRequest(c, session, adaptor, reqBody)
	if err != nil {
		// A-26:DoRequest transport/setup/network 失败(无 response),不参与 mapping。
		return nil, toBridgeNewAPIError(err, types.ErrorCodeDoRequestFailed)
	}

	// 编排第 4 步:上游响应校验顺序固定(A-23 -> A-2 -> A-24)。
	//   A-23:doBridgeRequest 返回具体 *http.Response,类型断言(comma-ok)由签名结构满足
	//   (A-23②/③ 恒通过);此处统一校验 resp==nil(A-23①)与 resp.Body==nil(A-23④)。
	if a23Err := validateBridgeUpstreamResponse(resp); a23Err != nil {
		resetStatusCodeOnce(c, a23Err)
		return nil, a23Err
	}
	//   A-2:仅非 nil response 且 status ≠ 200 进 RelayErrorHandler;bridge 不覆盖其
	//   status/error type/SkipRetry,RelayErrorHandler 后完成一次 status mapping。
	if resp.StatusCode != http.StatusOK {
		apiErr := service.RelayErrorHandler(c.Request.Context(), resp, false)
		resetStatusCodeOnce(c, apiErr)
		return nil, apiErr
	}
	//   A-24:200 但 Content-Type 非 text/event-stream(上游契约违背)。读 body 提取上游
	//   error message(有则保留),否则泛化文案;参与 mapping。
	if !isBridgeSSEContentType(resp.Header.Get("Content-Type")) {
		apiErr := buildNotSSEError(resp)
		resetStatusCodeOnce(c, apiErr)
		return nil, apiErr
	}

	// 编排第 5 步:构造响应会话并分流 stream/aggregate。
	respSession, apiErr := newBridgeResponseSession(c, info, session)
	if apiErr != nil {
		// A-18:响应会话初始化失败(本地响应处理失败,参与 mapping)。
		resetStatusCodeOnce(c, apiErr)
		return nil, apiErr
	}

	var usage *dto.Usage
	if session.options.downstreamStream {
		usage, apiErr = handleBridgeStream(respSession, resp)
	} else {
		usage, apiErr = handleBridgeAggregate(respSession, resp)
	}
	if apiErr != nil {
		// pre-commit 失败:统一清理下游 SSE headers + event_stream_headers_set 标记
		// (aggregate 恒未设、stream 已设;清理幂等,保证 controller c.JSON 写出 application/json),
		// 再对参与 mapping 的错误完成恰好一次 status mapping(A-13/A-25 panic 等固定码经 helper 跳过)。
		clearDownstreamSSEHeaders(c)
		resetStatusCodeOnce(c, apiErr)
		return nil, apiErr
	}

	// 成功终态或 post-commit 收尾:(usage, nil),controller 不再写 envelope、不重试。
	return usage, nil
}

// validateBridgeUpstreamResponse 是 design I-1 上游响应校验顺序的第 1 步(A-23)。
// doBridgeRequest 已在消息桥内返回具体 *http.Response(类型断言由签名结构满足),
// 此处统一以 resp==nil(A-23①)与 resp.Body==nil(A-23④)判定无效上游响应。返回非 nil
// 表示应落 A-23(ErrorCodeBadResponse,err 确定性非 nil),由调用方完成 status mapping。
func validateBridgeUpstreamResponse(resp *http.Response) *types.NewAPIError {
	if resp == nil || resp.Body == nil {
		return types.NewError(errors.New("invalid upstream response"), types.ErrorCodeBadResponse)
	}
	return nil
}

// buildNotSSEError 构造 A-24 错误:HTTP 200 但 Content-Type 非 text/event-stream。
// 可解析出上游 error 对象时经 GetOpenAIError 提取 message 保留,否则泛化文案。
func buildNotSSEError(resp *http.Response) *types.NewAPIError {
	msg := "upstream response content-type is not text/event-stream"
	if resp != nil && resp.Body != nil {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, bridgeNotSSEBodyPreviewLimit))
		_ = resp.Body.Close()
		var payload struct {
			Error any `json:"error"`
		}
		if json.Unmarshal(body, &payload) == nil {
			if oe := dto.GetOpenAIError(payload.Error); oe != nil && strings.TrimSpace(oe.Message) != "" {
				msg = "upstream response content-type is not text/event-stream: " + strings.TrimSpace(oe.Message)
			}
		}
	}
	return types.NewError(errors.New(msg), types.ErrorCodeBadResponse)
}

// isBridgeSSEContentType 判断 Content-Type 是否为 SSE(bridge 上游恒强制 stream)。
func isBridgeSSEContentType(contentType string) bool {
	return strings.Contains(strings.ToLower(contentType), "text/event-stream")
}

// resetStatusCodeOnce 是 status mapping 唯一责任方:对参与 mapping 的 (nil, err) 返回前
// 调用恰好一次 service.ResetStatusCode。A-1 转换失败 / A-13、A-25 panic / A-26 DoRequest
// transport / A-27 请求构造失败 / A-29 接线前置等错误保持构造时的固定码,不参与 mapping,
// 此处直接跳过。以内部 ErrorCode 区分(各固定码错误自带明确 ErrorCode)。
func resetStatusCodeOnce(c *gin.Context, apiErr *types.NewAPIError) {
	if c == nil || apiErr == nil {
		return
	}
	switch apiErr.GetErrorCode() {
	case types.ErrorCodeInvalidRequest, // A-1 转换失败(400)、A-13/A-25 panic(500)
		types.ErrorCodeConvertRequestFailed, // A-27 请求构造失败(400/500)
		types.ErrorCodeDoRequestFailed,      // A-26 transport/setup/network(500)
		types.ErrorCodeInvalidApiType:       // A-29 接线前置(500)
		return
	}
	service.ResetStatusCode(apiErr, c.GetString("status_code_mapping"))
}

// clearDownstreamSSEHeaders 清理 stream 处理器经 SetEventStreamHeaders 写入的下游 SSE
// header map 与 event_stream_headers_set 标记,保证 pre-commit 出口 controller `c.JSON`
// 写出 application/json。清理幂等:aggregate 与未设过 SSE header 的路径为 no-op。
func clearDownstreamSSEHeaders(c *gin.Context) {
	if c == nil {
		return
	}
	if c.Keys != nil {
		delete(c.Keys, "event_stream_headers_set")
	}
	h := c.Writer.Header()
	h.Del("Content-Type")
	h.Del("Cache-Control")
	h.Del("Connection")
	h.Del("Transfer-Encoding")
	h.Del("X-Accel-Buffering")
}

// toBridgeNewAPIError 把组件返回的普通 error 规范化为 *types.NewAPIError。prepareBridgeRequest
// 与 doBridgeRequest 的错误链已含精确 *types.NewAPIError(A-26/A-27),errors.As 原样保留其
// code/type/status/SkipRetry;理论上的普通 error 则按给定 ErrorCode 构造(不参与 mapping 的
// A-26/A-27 固定码)。
func toBridgeNewAPIError(err error, fallback types.ErrorCode) *types.NewAPIError {
	var apiErr *types.NewAPIError
	if errors.As(err, &apiErr) && apiErr != nil {
		return apiErr
	}
	return types.NewError(err, fallback)
}

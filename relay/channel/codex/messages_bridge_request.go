package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/gin-gonic/gin"
)

const (
	// bridgeUpstreamAccept 是 bridge 上游恒定的 SSE Accept 值。
	// Codex /backend-api/codex/responses 只接受流式,故 bridge 上游无条件声明 SSE。
	bridgeUpstreamAccept = "text/event-stream"

	// bridgeUpstreamPath 是 bridge 上游 Responses 请求的路径元数据。
	// 仅写进 attempt-local RequestURLPath 浅副本,不参与实际 URL 构造
	// (URL 由 concrete adaptor 的 GetRequestURL 依 RelayMode 决定)。
	bridgeUpstreamPath = "/v1/responses"
)

// bridgeRequestOptions 承载 bridge 上游请求的固定模式选项。
// 三字段均 package-private,只在 codex bridge 内使用;下游模式只读,
// 上游两项为不变量。
type bridgeRequestOptions struct {
	downstreamStream bool   // 原始客户端模式;只读
	upstreamStream   bool   // 不变量:恒 true
	upstreamAccept   string // 不变量:恒 "text/event-stream"
}

// bridgeRequestSession 是单次 controller attempt 的请求会话状态。
// 生命周期 = 单次 attempt;retry 每次新建,返回即丢弃,不进入 gin context。
type bridgeRequestSession struct {
	options      bridgeRequestOptions
	upstreamInfo relaycommon.RelayInfo // 尝试级值副本
	mapping      codexToolNameMapping
}

// newBridgeRequestSession 基于原始 *RelayInfo 创建 attempt-local 请求会话。
//
// 浅拷贝纪律(round-17 architecture m-1):upstreamInfo 是 src 的浅值副本,
// 唯一允许覆盖的字段固定为 IsStream/RelayMode/RequestURLPath 三个标量;
// 其余字段一律只读——尤其不得经副本写 RequestHeaders map、RealtimeTools
// slice、ReasoningConversion 指针或 Billing 接口。共享引用字段因此与原始
// RelayInfo 指向同一底层对象,任何经副本的写入都会污染直连路径与并发安全,
// 读取时须保持只读。转换诊断、usage、StreamStatus 与 Billing 状态仍只写
// 原始 RelayInfo,不写 upstreamInfo。
//
// IsStream=false 仅用于抑制通用 doRequest 的下游 SSE header/pinger 副作用,
// 上游 SSE 由 wrapper 的显式 upstreamAccept 保证,不属于可绕过强制。
func newBridgeRequestSession(src *relaycommon.RelayInfo, downstreamStream bool, mapping codexToolNameMapping) *bridgeRequestSession {
	if src == nil {
		src = &relaycommon.RelayInfo{}
	}
	opts := bridgeRequestOptions{
		downstreamStream: downstreamStream,
		upstreamStream:   true,
		upstreamAccept:   bridgeUpstreamAccept,
	}
	// 浅值副本:结构体赋值复制各字段的"值头"(标量值 / map·slice / 指针 / 接口引用),
	// 不深拷贝任何底层容器。下面的三标量覆盖只发生在副本上,不触碰原始 src。
	upstream := *src
	upstream.IsStream = false
	upstream.RelayMode = relayconstant.RelayModeResponses
	upstream.RequestURLPath = bridgeUpstreamPath
	return &bridgeRequestSession{
		options:      opts,
		upstreamInfo: upstream,
		mapping:      mapping,
	}
}

// bridgeAdaptorWrapper 是 package-private 的 concrete *Adaptor 委托包装。
// 仅覆盖 SetupRequestHeader 强制上游 SSE Accept,其它方法经内嵌 *Adaptor
// 方法提升委托给 concrete adaptor(完整满足 channel.Adaptor 接口)。
type bridgeAdaptorWrapper struct {
	*Adaptor
}

// ensureBridgeAdaptorWrapper 使 bridgeAdaptorWrapper 编译期满足 channel.Adaptor
// (Go 方法提升自动补齐除 SetupRequestHeader 外的全部接口方法)。
var _ interface {
	SetupRequestHeader(*gin.Context, *http.Header, *relaycommon.RelayInfo) error
} = (*bridgeAdaptorWrapper)(nil)

// SetupRequestHeader 先委托 concrete adaptor 完成既有鉴权/Content-Type 等设置,
// 再把上游 Accept 无条件改写为 SSE。只改实际 http.Request header(即 *http.Header),
// 不得借 upstreamInfo.RequestHeaders 注入——那会污染共享引用字段。
func (w *bridgeAdaptorWrapper) SetupRequestHeader(c *gin.Context, req *http.Header, info *relaycommon.RelayInfo) error {
	if w == nil || w.Adaptor == nil {
		return nil
	}
	if err := w.Adaptor.SetupRequestHeader(c, req, info); err != nil {
		return err
	}
	req.Set("Accept", bridgeUpstreamAccept)
	return nil
}

// prepareBridgeRequest 完成 bridge 上游的全部请求变换,并在函数内唯一一次 marshal
// 返回完整的 outbound JSON bytes。调用方只读持有返回结果(可为 transport 创建
// reader),不得再修改或重复 marshal。
//
// 变换顺序固定:
//  1. 强制 stream=true / include=["reasoning.encrypted_content"] / store=false /
//     默认 parallel_tool_calls=true(struct 层);
//  2. 清除 Codex 不支持的 token 限制 / 采样字段(struct 层:max_output_tokens、
//     temperature、top_p);
//  3. 工具 name / call_id 64 字节收缩并登记进 mapping(T031 映射);
//  4. 唯一一次 marshal → 完整 outbound JSON bytes(A-27③);
//  5. disabled-field 移除(JSON 层 sjson,含 DTO 未建模字段,A-27④);
//  6. param override(A-27⑤);
//  7. 最终 outbound body 组装 + 结构健全性校验(A-27⑥)。
func prepareBridgeRequest(info *relaycommon.RelayInfo, request dto.OpenAIResponsesRequest, mapping *codexToolNameMapping) (requestBody []byte, err error) {
	// 强制 stream 与 include / store / parallel_tool_calls。
	streamTrue := true
	request.Stream = &streamTrue
	request.Include = json.RawMessage(`["reasoning.encrypted_content"]`)
	request.Store = json.RawMessage("false")
	request.ParallelToolCalls = json.RawMessage("true")
	// 清除 Codex 不支持的 token 限制 / 采样字段。
	request.MaxOutputTokens = nil
	request.Temperature = nil
	request.TopP = nil

	// 工具 name / call_id 收缩并写入 mapping。
	var shrunkNames, shrunkCallIDs int
	if mapping != nil {
		if shrunkNames, shrunkCallIDs, err = shrinkToolNamesAndCallIDs(&request, mapping); err != nil {
			// 收缩即 outbound 变换的一环;失败落请求构造类(恒 SkipRetry,不参与 mapping)。
			return nil, newBridgeRequestConstructionError(err)
		}
	}

	// 唯一一次 marshal:产物为完整 outbound JSON bytes,A-27③。
	body, err := common.Marshal(request)
	if err != nil {
		return nil, newBridgeRequestConstructionError(err)
	}

	// disabled-field 移除,JSON 层 sjson 删除,D-5b 契约下不触碰 struct 已剔除字段为 no-op;
	// 任一删除失败落 A-27④。
	body, err = removeDisabledFields(body)
	if err != nil {
		return nil, newBridgeRequestConstructionError(err)
	}

	// param override;⑤b 显式 return_error 保留自定义 status/code/type/skip_retry,⑤a 普通错误 skip。
	body, err = relaycommon.ApplyParamOverrideWithRelayInfo(body, info)
	if err != nil {
		return nil, wrapBridgeParamOverrideError(err)
	}

	// 最终 outbound body 组装:A-27⑥ 独立分类边界。param override(⑤)产物在交给
	// transport 前的最后一道组装/结构健全性校验;该环节失败与③ marshal /
	// ④ disabled-field / ⑤ param override 的失败各自独立可区分。
	body, err = constructOutboundBody(body)
	if err != nil {
		return nil, newBridgeOutboundBodyConstructionError(err)
	}

	// NFR-004 诊断:强制 stream 与本次收缩计数(写原始 info,不写 shallow 副本)。
	if info != nil {
		info.RecordConversionDiagnostics(context.Background(), []types.ConversionDiagnostic{
			{
				Code:     "upstream_stream_forced",
				Path:     "stream",
				Message:  "bridge forced upstream stream=true for codex responses",
				Severity: types.ConversionDiagnosticWarning,
				From:     types.RelayFormatClaude,
				To:       types.RelayFormatOpenAIResponses,
			},
			{
				Code: "request_field_shrink",
				Path: "tools/input",
				Message: fmt.Sprintf("shrunk %d tool names and %d call_ids for 64-byte codex limit",
					shrunkNames, shrunkCallIDs),
				Severity: types.ConversionDiagnosticWarning,
				From:     types.RelayFormatClaude,
				To:       types.RelayFormatOpenAIResponses,
			},
		})
	}

	return body, nil
}

// doBridgeRequest 只以 requestBody 创建本次调用的 reader 并经 package-private
// wrapper + channel.DoApiRequest 执行 transport;不改 body、不重编码。
// session 的 upstreamInfo 浅副本交给既有 channel.DoApiRequest,共享引用字段只读纪律
// 见 newBridgeRequestSession。transport/setup/network 失败(无 response)落 A-26:
// 错误链已含 *NewAPIError 时 errors.As 原样保留内层 code/type/status/SkipRetry,
// 否则 new_api_error / 500 / 不主动 SkipRetry。
func doBridgeRequest(c *gin.Context, session *bridgeRequestSession, adaptor *Adaptor, requestBody []byte) (*http.Response, error) {
	if c == nil || session == nil {
		return nil, types.NewError(errors.New("bridge request session or context is nil"), types.ErrorCodeDoRequestFailed)
	}
	reader := bytes.NewReader(requestBody)
	wrapper := &bridgeAdaptorWrapper{Adaptor: adaptor}
	resp, err := channel.DoApiRequest(wrapper, c, &session.upstreamInfo, reader)
	if err != nil {
		return nil, bridgeWrapDoRequestFailedError(err)
	}
	return resp, nil
}

// bridgeWrapDoRequestFailedError 封装 A-26 语义。
// types.NewError 的 errors.As 分支天然保留内层 *NewAPIError 的 code/type/status/SkipRetry;
// 普通 error 则新建(new_api_error / 500 / 不主动 SkipRetry)。
func bridgeWrapDoRequestFailedError(err error) *types.NewAPIError {
	return types.NewError(err, types.ErrorCodeDoRequestFailed)
}

// newBridgeRequestConstructionError 构造 A-27③/④ 请求构造失败错误:
// ErrorCodeConvertRequestFailed、恒 SkipRetry、不参与 mapping。
func newBridgeRequestConstructionError(err error) *types.NewAPIError {
	return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
}

// constructOutboundBody 是 A-27⑥ 的出口:把 param override(⑤)产出的字节组装为
// 最终 outbound body 并做一次结构健全性校验(交给 transport 前恒须为合法 JSON 文档)。
// 任何该环节失败(如 override/disabled-field 变换后产物已非合法文档)落 A-27⑥ 独立分类。
func constructOutboundBody(overridden []byte) ([]byte, error) {
	if !json.Valid(overridden) {
		return nil, errors.New("outbound body is not valid JSON after param override")
	}
	return overridden, nil
}

// newBridgeOutboundBodyConstructionError 构造 A-27⑥ 错误:与③/④同出口
// (ErrorCodeConvertRequestFailed + 恒 SkipRetry + 不参与 mapping),但作为独立分类
// 边界,使"最终 outbound 组装失败"能与单个变换阶段(③ marshal / ④ disabled-field /
// ⑤ param override)的失败区分开(诊断/测试定位用)。
func newBridgeOutboundBodyConstructionError(err error) *types.NewAPIError {
	return types.NewError(err, types.ErrorCodeConvertRequestFailed, types.ErrOptionWithSkipRetry())
}

// wrapBridgeParamOverrideError 构造 A-27⑤ 错误:⑤b 显式 return_error 经
// NewAPIErrorFromParamOverride 保留自定义状态;否则⑤a 普通 override 错误
// (channel:param_override_invalid + SkipRetry)。语义对齐 relay 包私有 helper。
func wrapBridgeParamOverrideError(err error) *types.NewAPIError {
	if fixed, ok := relaycommon.AsParamOverrideReturnError(err); ok {
		return relaycommon.NewAPIErrorFromParamOverride(fixed)
	}
	return types.NewError(err, types.ErrorCodeChannelParamOverrideInvalid, types.ErrOptionWithSkipRetry())
}

// disabledBridgeFields 是 Codex 上游拒绝、须从 outbound body 删除的顶层字段全集
// (对齐 CLIProxyAPI 基线 codex_openai-responses_request.go 的 deleteCodexRequestFields /
// applyResponsesCompactionCompatibility)。service_tier 与非顶层 prompt_cache_breakpoint
// 单独处理(见 removeDisabledFields)。
var disabledBridgeFields = []string{
	"max_output_tokens",
	"max_completion_tokens",
	"temperature",
	"top_p",
	"truncation",
	"prompt_cache_options",
	"prompt_cache_retention",
	"user",
	"context_management",
}

// removeDisabledFields 从 outbound body 删除全部 disabled-field;任一删除失败返回 error
// (落 A-27④)。service_tier 仅当值非 "priority" 时删除;嵌套 prompt_cache_breakpoint
// 逐 input[].content[] part 删除。
func removeDisabledFields(body []byte) ([]byte, error) {
	var err error
	for _, path := range disabledBridgeFields {
		if body, err = deleteJSONFieldIfExists(body, path); err != nil {
			return nil, err
		}
	}
	// service_tier:仅当值非 "priority" 时删除。
	if st := gjson.GetBytes(body, "service_tier"); st.Exists() && st.String() != "priority" {
		if body, err = sjson.DeleteBytes(body, "service_tier"); err != nil {
			return nil, err
		}
	}
	return deleteNestedCacheBreakpoints(body)
}

// deleteJSONFieldIfExists 仅当键存在时用 sjson.DeleteBytes 删除,返回最新 body。
func deleteJSONFieldIfExists(body []byte, path string) ([]byte, error) {
	if !gjson.GetBytes(body, path).Exists() {
		return body, nil
	}
	return sjson.DeleteBytes(body, path)
}

// deleteNestedCacheBreakpoints 删除 input[].content[] 各 part 内嵌的
// prompt_cache_breakpoint(sjson 路径 input.<i>.content.<j>.prompt_cache_breakpoint)。
// 无该键或 input 非数组时原样返回。
func deleteNestedCacheBreakpoints(body []byte) ([]byte, error) {
	if !bytes.Contains(body, []byte(`"prompt_cache_breakpoint"`)) {
		return body, nil
	}
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body, nil
	}
	n := int(input.Get("#").Int())
	for i := 0; i < n; i++ {
		content := input.Get(fmt.Sprintf("%d.content", i))
		if !content.IsArray() {
			continue
		}
		m := int(content.Get("#").Int())
		for j := 0; j < m; j++ {
			p := fmt.Sprintf("input.%d.content.%d.prompt_cache_breakpoint", i, j)
			if !gjson.GetBytes(body, p).Exists() {
				continue
			}
			var err error
			if body, err = sjson.DeleteBytes(body, p); err != nil {
				return nil, err
			}
		}
	}
	return body, nil
}

// shrinkToolNamesAndCallIDs 对 request 的 Tools 与 Input 做 64 字节收缩并登记进 mapping:
//   - 全量工具函数名(先收集,再按请求内全集唯一化)写入 nameByShort,并改写 tools.name;
//   - input 中 function_call 的 name 与 call_id、function_call_output 的 call_id 收缩改写;
//
// 返回本次收缩的工具名 / call_id 数量(供 NFR-004 诊断)。
func shrinkToolNamesAndCallIDs(req *dto.OpenAIResponsesRequest, mapping *codexToolNameMapping) (int, int, error) {
	if mapping == nil {
		return 0, 0, nil
	}
	// mapping 可能以空值传入(session 未预构建),此时保证两张表已初始化可用。
	if mapping.nameByShort == nil {
		mapping.nameByShort = make(map[string]string)
	}
	if mapping.callIDByShort == nil {
		mapping.callIDByShort = make(map[string]string)
	}
	names := collectFunctionToolNames(req)
	if len(mapping.nameByShort) == 0 && len(names) > 0 {
		mapping.buildShortNameMap(names)
	}

	var shrunkNames, shrunkCallIDs int

	// 改写 tools 中的 function 工具名。
	if len(req.Tools) > 0 {
		tools := gjson.ParseBytes(req.Tools)
		if tools.IsArray() {
			updated := req.Tools
			for i := 0; i < int(tools.Get("#").Int()); i++ {
				if tools.Get(fmt.Sprintf("%d.type", i)).String() != "function" {
					continue
				}
				name := tools.Get(fmt.Sprintf("%d.name", i)).String()
				if name == "" {
					continue
				}
				short := mapping.shrinkName(name)
				if short == name {
					continue
				}
				var err error
				if updated, err = sjson.SetBytes(updated, fmt.Sprintf("%d.name", i), short); err != nil {
					return 0, 0, err
				}
				shrunkNames++
			}
			req.Tools = updated
		}
	}

	// 改写 input 中的 function_call name(收缩)与 call_id(收缩)。
	if len(req.Input) > 0 {
		input := gjson.ParseBytes(req.Input)
		if input.IsArray() {
			updated := req.Input
			for i := 0; i < int(input.Get("#").Int()); i++ {
				itemType := input.Get(fmt.Sprintf("%d.type", i)).String()
				if itemType == "function_call" {
					if name := input.Get(fmt.Sprintf("%d.name", i)).String(); name != "" {
						short := mapping.shrinkName(name)
						if short != name {
							var err error
							if updated, err = sjson.SetBytes(updated, fmt.Sprintf("%d.name", i), short); err != nil {
								return 0, 0, err
							}
							shrunkNames++
						}
					}
				}
				if itemType == "function_call" || itemType == "function_call_output" {
					if callID := input.Get(fmt.Sprintf("%d.call_id", i)).String(); callID != "" {
						short := mapping.shrinkCallID(callID)
						if short != callID {
							var err error
							if updated, err = sjson.SetBytes(updated, fmt.Sprintf("%d.call_id", i), short); err != nil {
								return 0, 0, err
							}
							shrunkCallIDs++
						}
					}
				}
			}
			req.Input = updated
		}
	}

	return shrunkNames, shrunkCallIDs, nil
}

// collectFunctionToolNames 提取 Tools 数组中 type=function 且 name 非空的全部工具名。
func collectFunctionToolNames(req *dto.OpenAIResponsesRequest) []string {
	if len(req.Tools) == 0 {
		return nil
	}
	tools := gjson.ParseBytes(req.Tools)
	if !tools.IsArray() {
		return nil
	}
	names := make([]string, 0, int(tools.Get("#").Int()))
	for i := 0; i < int(tools.Get("#").Int()); i++ {
		if tools.Get(fmt.Sprintf("%d.type", i)).String() != "function" {
			continue
		}
		if name := tools.Get(fmt.Sprintf("%d.name", i)).String(); name != "" {
			names = append(names, name)
		}
	}
	return names
}

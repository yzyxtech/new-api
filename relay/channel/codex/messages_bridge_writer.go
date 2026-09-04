package codex

import (
	"fmt"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/relaykit/dto"

	"github.com/gin-gonic/gin"
)

// bridgeResponseWriter 是 bridge 自建的下游写出契约,替代 helper.ClaudeData。
// helper.ClaudeData 的 marshal 失败仅记日志、忽略写出/flush 错误、恒返 nil
// (relay/helper/common.go:61-74),无法支撑两段式分流;本 writer 以显式 error
// 返回暴露 marshal 失败与写出失败,供调用方按 A-20 与 A-21 稳定边界分流。
//
// receiver/构造契约:类型与全部方法均 package-private;唯一字段持有 c *gin.Context;
// 构造唯一经 newBridgeResponseWriter(c),由 I-3/I-4 入口处各自构造,一次请求一实例。
// writer 不缓存任何提交状态,committed() 直接委托 c.Writer.Written()——I-3/I-4
// 与 I-1 经同一个 c 观察同一提交状态。
type bridgeResponseWriter struct {
	c *gin.Context
}

// bridgeFrameError 标识「组帧/序列化失败」:marshal 与 SSE 帧字节组装在任何 Write 调用
// 之前完成,失败发生在写出前,commit 状态确定性未成立。调用方据此与底层写出失败区分,
// 按稳定边界分流(A-20 组帧失败:未 commit 上抛 / 已 commit 写一次 SSE error)。
type bridgeFrameError struct{ err error }

func (e *bridgeFrameError) Error() string { return e.err.Error() }
func (e *bridgeFrameError) Unwrap() error { return e.err }

// bridgeWriteError 标识「底层写出失败」:c.Writer.Write 返回 error 或短写。gin v1.9.1
// 实测 Write 先 WriteHeaderNow,写失败后 Written() 恒 true(含首帧)——调用方将其一律
// 视同断连静默关流(A-21),不再尝试 error 帧或 HTTP envelope。
type bridgeWriteError struct{ err error }

func (e *bridgeWriteError) Error() string { return e.err.Error() }
func (e *bridgeWriteError) Unwrap() error { return e.err }

// newBridgeResponseWriter 创建 writer。每个请求 I-3 与 I-4 互斥只运行其一,各构造
// 一实例;writer 生命周期=单次 bridge attempt。
func newBridgeResponseWriter(c *gin.Context) *bridgeResponseWriter {
	return &bridgeResponseWriter{c: c}
}

// writeSSEFrame 以钉死的 SSE wire format 写出单个 Claude 响应帧:
//
//	event: <resp.Type>
//	data: <单行 compact json>
//	(空行)
//
// 组帧(marshal + 字节组装)在任何 Write 调用之前完成,组帧失败在写出前返回
// (commit 状态确定性未成立,= pre-commit)。media type text/event-stream 由 I-3
// 经 StreamScannerHandler 在首帧前写入 header map(event_stream_headers_set 幂等
// 守卫防重复),本方法不触碰 Content-Type。frame 字节组完后单次 c.Writer.Write
// 捕获 (n, err)——err != nil 或 n != len 统一视为写出失败(短写防御)。成功后
// flush 触发,flush 本身无 error 返回,网络失败由下一次写出失败暴露。
func (w *bridgeResponseWriter) writeSSEFrame(resp dto.ClaudeResponse) error {
	// 先组帧后写出:任何 Write 之前完成 marshal 与 SSE frame 字节组装;marshal 失败以
	// bridgeFrameError 暴露(组帧失败,A-20 分流)。
	data, err := common.Marshal(resp)
	if err != nil {
		return &bridgeFrameError{err: err}
	}
	frame := []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", resp.Type, string(data)))
	// 单次 c.Writer.Write 捕获 (n, err)——err != nil 或 n != len 统一视为写出失败
	// (短写防御),以 bridgeWriteError 暴露(A-21:gin 实测写失败后 Written() 恒 true)。
	n, err := w.c.Writer.Write(frame)
	if err != nil || n != len(frame) {
		if err == nil {
			err = shortWriteError(n, len(frame))
		}
		return &bridgeWriteError{err: err}
	}
	w.c.Writer.Flush()
	return nil
}

// writeJSON 以钉死的无 SSE framing 形态写出单个 Claude message JSON 字节。
// marshal 在任何 Write 前完成,失败返回(= A-22① pre-commit)。首次 Write 前
// 设 application/json 并 WriteHeader(status) 暂存最终 status(gin v1.9.1
// WriteHeader 仅写内部 status 字段、非真实提交,提交发生在后续 Write 内的
// WriteHeaderNow)。单次 c.Writer.Write 捕获 (n, err),err != nil 或 n != len
// 统一视为写出失败(A-22②:Write 已触发 WriteHeaderNow,status 已提交,无法安全
// 回退 envelope)。禁 c.JSON(经 Render 无 error 返回,无法捕获写失败)。
func (w *bridgeResponseWriter) writeJSON(status int, resp dto.ClaudeResponse) error {
	data, err := common.Marshal(resp)
	if err != nil {
		return err
	}
	// 先 Set header 再 WriteHeader:WriteHeaderNow 会在首次 Write 时真正提交 headers,
	// 故 Content-Type 必须在任何实际写出之前落到 header map。
	w.c.Writer.Header().Set("Content-Type", "application/json")
	w.c.Writer.WriteHeader(status)
	n, werr := w.c.Writer.Write(data)
	if werr != nil || n != len(data) {
		if werr == nil {
			werr = shortWriteError(n, len(data))
		}
		return werr
	}
	return nil
}

// committed 委托 c.Writer.Written() 观察实时提交状态,不缓存。仅表示 HTTP
// status/headers 已提交,不等同客户端收到字节。任何 Write 之前的防御性确认、
// 组帧失败时的 envelope/SSE error 帧分流,以及 aggregate panic 的 pre/post-commit
// 分流以此为判据。
func (w *bridgeResponseWriter) committed() bool {
	return w.c.Writer.Written()
}

// shortWriteError 构造短写失败错误,供写失败与 n != len 防御复用。
func shortWriteError(n, want int) error {
	return fmt.Errorf("short write: wrote %d of %d bytes", n, want)
}

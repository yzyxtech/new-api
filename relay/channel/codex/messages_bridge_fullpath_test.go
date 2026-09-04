package codex_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/controller"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/relay/channel/codex"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/router"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting/model_setting"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/ratio_setting"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/go-redis/redis/v8"
	"github.com/tidwall/gjson"
	"gorm.io/gorm"
)

// ---------------------------------------------------------------------------
// full-path fixture helpers
//
// These build a real registered `POST /v1/messages` route (router.SetRelayRouter)
// backed by an in-memory SQLite DB (model.DB), an in-process miniredis, a codex
// channel (type 57) whose base URL points at a swappable httptest upstream that
// mock serves the Codex Responses SSE stream, plus the minimal global settings so
// the request actually exercises middleware -> controller.Relay -> channel
// selection -> billing -> codex adaptor -> bridge -> relaykit conversion end to end.
// ---------------------------------------------------------------------------

// fullPathEnv is the environment returned by newFullPathEnv.
type fullPathEnv struct {
	engine    *gin.Engine
	rec       *httptest.ResponseRecorder
	channelID int
	userID    int
	tokenID   int
	baseURL   string
	upstream  *swappableUpstream
}

// swappableUpstream lets a test swap the httptest handler between scenarios.
type swappableUpstream struct {
	mu      chan struct{} // not a real lock; serializes through test (single goroutine)
	handler http.HandlerFunc
}

func (s *swappableUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := s.handler
	if h == nil {
		http.Error(w, "no upstream handler configured", http.StatusInternalServerError)
		return
	}
	h(w, r)
}

func (s *swappableUpstream) set(h http.HandlerFunc) { s.handler = h }

var (
	savedRDB           *redis.Client
	savedRedisEnabled  bool
	savedMemCache      bool
	savedDB            *gorm.DB
	savedLogDB         *gorm.DB
	savedQuotaPreCon   bool
	savedRetryTimes    int
	memoSetupGlobals   bool
	savedModelCountTok bool
	savedRatioJSON     string
	savedPolicy        model_setting.ChatCompletionsToResponsesPolicy
)

func saveGlobalState(t *testing.T) {
	t.Helper()
	if !memoSetupGlobals {
		savedRDB = common.RDB
		savedRedisEnabled = common.RedisEnabled
		savedMemCache = common.MemoryCacheEnabled
		savedDB = model.DB
		savedLogDB = model.LOG_DB
		savedQuotaPreCon = operation_setting.GetQuotaSetting().EnableFreeModelPreConsume
		savedRetryTimes = common.RetryTimes
		savedModelCountTok = constant.CountToken
		savedRatioJSON = ratio_setting.ModelRatio2JSONString()
		savedPolicy = model_setting.GetGlobalSettings().ChatCompletionsToResponsesPolicy
		memoSetupGlobals = true
	}
}

func restoreGlobalState(t *testing.T) {
	t.Helper()
	common.RDB = savedRDB
	common.RedisEnabled = savedRedisEnabled
	common.MemoryCacheEnabled = savedMemCache
	model.DB = savedDB
	model.LOG_DB = savedLogDB
	operation_setting.GetQuotaSetting().EnableFreeModelPreConsume = savedQuotaPreCon
	common.RetryTimes = savedRetryTimes
	constant.CountToken = savedModelCountTok
	_ = ratio_setting.UpdateModelRatioByJSONString(savedRatioJSON)
	model_setting.GetGlobalSettings().ChatCompletionsToResponsesPolicy = savedPolicy
}

// newFullPathEnv seeds an in-memory DB + miniredis, configures global settings so
// the Codex bridge engages, registers the real relay router, and returns the env.
// The channel BaseURL points at the swappable upstream server.
func newFullPathEnv(t *testing.T) *fullPathEnv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	saveGlobalState(t)
	t.Cleanup(func() { restoreGlobalState(t) })

	// 估算 usage 路径(缺权威 usage 时 bridge 按统一规则回填)依赖 tokenizer 全局编码器,
	// 而测试进程未走真实 main 的 InitTokenEncoders;此处显式初始化,使 post-commit 失败 /
	// 缺 usage 终态的估算不因 nil codec panic。
	service.InitTokenEncoders()

	// --- miniredis as the app-wide redis client -------------------------------
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	common.RDB = client
	common.RedisEnabled = true
	// Channel selection in this new-api runs from an in-memory snapshot
	// (model.InitChannelCache), which only populates when MemoryCacheEnabled.
	common.MemoryCacheEnabled = true

	// --- in-memory SQLite and migration ---------------------------------------
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.User{}, &model.Token{}, &model.Channel{}, &model.Ability{}, &model.Log{}, &model.Option{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	model.DB = db
	model.LOG_DB = db
	// initCol() sets the (unexported) commonKeyCol used by GetTokenByKey; it only
	// runs during DB setup. InitLogDB with no LOG_SQL_DSN triggers initCol() with
	// the SQLite branch (backtick-quoted columns) without opening another DB.
	_ = model.InitLogDB()
	// Re-point the DBs at the in-memory db now that initCol has run.
	model.DB = db
	model.LOG_DB = db

	// --- upstream mock server (codex Responses SSE) ----------------------------
	up := &swappableUpstream{}
	server := httptest.NewServer(up)
	t.Cleanup(server.Close)

	// --- global settings so the Codex bridge engages ---------------------------
	// Free model (ratio 0 + free pre-consume disabled) so the request flows through
	// the controller/billing code without requiring a funded wallet session; the
	// billing path (PostTextConsumeQuota) still executes.
	if err := ratio_setting.UpdateModelRatioByJSONString(`{"gpt-5.6-luna": 0}`); err != nil {
		t.Fatalf("set model ratio: %v", err)
	}
	operation_setting.GetQuotaSetting().EnableFreeModelPreConsume = false
	model_setting.GetGlobalSettings().ChatCompletionsToResponsesPolicy = model_setting.ChatCompletionsToResponsesPolicy{
		Enabled:       true,
		AllChannels:   false,
		ChannelTypes:  []int{constant.ChannelTypeCodex},
		ModelPatterns: []string{"gpt-5.6-luna"},
	}

	// --- seed user / token / codex channel ------------------------------------
	userID := 1
	tokenID := 1
	channelID := 1
	if err := db.Create(&model.User{
		Id:          userID,
		Username:    "fullpath-user",
		Password:    "unused-password",
		Role:        common.RoleCommonUser,
		Status:      common.UserStatusEnabled,
		Quota:       1000000000,
		Group:       "default",
		AuthVersion: 1,
	}).Error; err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if err := db.Create(&model.Token{
		Id:             tokenID,
		UserId:         userID,
		Key:            "fullpath",
		Status:         common.TokenStatusEnabled,
		Name:           "fullpath-token",
		ExpiredTime:    -1,
		UnlimitedQuota: true,
		Group:          "default",
	}).Error; err != nil {
		t.Fatalf("seed token: %v", err)
	}
	baseURL := server.URL
	if err := db.Create(&model.Channel{
		Id:          channelID,
		Type:        constant.ChannelTypeCodex,
		Name:        "codex-fullpath",
		Key:         `{"access_token":"tok-fullpath","account_id":"acct-fullpath"}`,
		Status:      common.ChannelStatusEnabled,
		BaseURL:     &baseURL,
		Models:      "gpt-5.6-luna",
		Group:       "default",
		CreatedTime: common.GetTimestamp(),
		Priority:    ptrInt64(100),
	}).Error; err != nil {
		t.Fatalf("seed channel: %v", err)
	}
	prio := int64(100)
	if err := db.Create(&model.Ability{
		Group:     "default",
		Model:     "gpt-5.6-luna",
		ChannelId: channelID,
		Enabled:   true,
		Priority:  &prio,
	}).Error; err != nil {
		t.Fatalf("seed ability: %v", err)
	}

	// populate the in-memory channel snapshot used by the distributor/controller.
	model.InitChannelCache()

	// --- register the real relay router ---------------------------------------
	engine := gin.New()
	// 与生产 main.go 一致,给每个请求赋予一个 request id(NFR-004“同 request ID”摘要依赖它):
	// 该中间件把 id 写入 gin Keys、请求 context 与响应头 X-Oneapi-Request-Id,
	// 使测试能从同一 HTTP 请求的响应头读出期望的 request id,并逐字断言摘要日志含之。
	engine.Use(middleware.RequestId())
	router.SetRelayRouter(engine)

	return &fullPathEnv{
		engine:    engine,
		rec:       httptest.NewRecorder(),
		channelID: channelID,
		userID:    userID,
		tokenID:   tokenID,
		baseURL:   server.URL,
		upstream:  up,
	}
}

func ptrInt64(v int64) *int64 { return &v }

// postMessages sends a real HTTP POST to the registered /v1/messages route.
// It returns the full downstream response body.
func (env *fullPathEnv) postMessages(t *testing.T, body string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-fullpath")
	req.Header.Set("x-api-key", "sk-fullpath")
	req.Header.Set("anthropic-version", "2023-06-01")
	env.rec = httptest.NewRecorder()
	env.engine.ServeHTTP(env.rec, req)
	return env.rec.Body.String()
}

// sseFrame is one parsed downstream Claude SSE frame (event + data JSON).
type sseFrame struct {
	Etype string
	Data  gjson.Result
	Full  string
}

// parseClaudeSSE splits a downstream Claude SSE body into frames.
func parseClaudeSSE(t *testing.T, body string) []sseFrame {
	t.Helper()
	var frames []sseFrame
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var etype, data string
		for _, line := range strings.Split(block, "\n") {
			if strings.HasPrefix(line, "event: ") {
				etype = strings.TrimPrefix(line, "event: ")
			} else if strings.HasPrefix(line, "data: ") {
				data = strings.TrimPrefix(line, "data: ")
			}
		}
		if etype == "" {
			if !gjson.Valid(block) {
				t.Fatalf("unexpected non-SSE block: %q", block)
			}
			continue
		}
		frames = append(frames, sseFrame{
			Etype: etype,
			Data:  gjson.Parse(data),
			Full:  block,
		})
	}
	return frames
}

// parseChatSSE splits an OpenAI chat SSE body into its raw `data: <payload>`
// frames in order (including the terminal `data: [DONE]`), preserving each
// frame's full raw line (`data: ` prefix included). Callers can therefore
// assert the exact raw frame lineage, the `\n\n` separator between frames, and
// every baseline payload field without losing the wire shape. If a block is not
// a single `data:` line it is rejected, so any `event:`/multi-line divergence
// from the baseline chat SSE shape surfaces immediately.
func parseChatSSE(t *testing.T, body string) []string {
	t.Helper()
	var out []string
	for _, block := range strings.Split(body, "\n\n") {
		block = strings.TrimRight(block, "\n")
		if strings.TrimSpace(block) == "" {
			continue
		}
		if !strings.HasPrefix(block, "data: ") || strings.Count(block, "\n") != 0 {
			t.Fatalf("malformed OpenAI chat SSE frame (want single `data:` line as-is): %q", block)
		}
		out = append(out, block)
	}
	if len(out) == 0 {
		t.Fatalf("no OpenAI `data: ` frames found in body: %q", body)
	}
	return out
}

// chatFramePayloadEqual asserts that the data payload of a single OpenAI chat SSE
// frame (`data: <payload>`) is exactly equal to the complete baseline payload,
// field-for-field at every nesting depth. Only the inherently non-deterministic
// top-level `id` (random chat completion id) and `created` (unix timestamp) are
// exempted from byte equality: each is verified present (non-empty / positive),
// and the actual values are injected into the baseline template before a full
// recursive equality of the decoded JSON objects. Any extra field, missing field,
// or drifted value anywhere in the payload fails the comparison, so a frame whose
// payload gains/loses a key the baseline does not have cannot slip through.
func chatFramePayloadEqual(t *testing.T, raw, wantTemplate string) {
	t.Helper()
	got := gjson.Parse(strings.TrimPrefix(raw, "data: "))
	if !got.IsObject() {
		t.Fatalf("chat frame data payload is not an object: %s", raw)
	}
	id := got.Get("id").String()
	created := got.Get("created").Int()
	if id == "" {
		t.Fatalf("chat frame data missing non-empty id: %s", raw)
	}
	if created <= 0 {
		t.Fatalf("chat frame data missing positive created timestamp: %s", raw)
	}
	want := strings.ReplaceAll(
		strings.ReplaceAll(wantTemplate, "@id@", strconv.Quote(id)),
		"@created@", strconv.FormatInt(created, 10),
	)
	var wantObj, gotObj any
	if err := json.Unmarshal([]byte(want), &wantObj); err != nil {
		t.Fatalf("invalid chat baseline payload template: %v\n%s", err, want)
	}
	if err := json.Unmarshal([]byte(got.Raw), &gotObj); err != nil {
		t.Fatalf("failed to decode actual chat frame payload: %v\n%s", err, got.Raw)
	}
	if !reflect.DeepEqual(wantObj, gotObj) {
		t.Fatalf("chat frame payload not exactly equal to complete baseline:\n  want=%s\n  got =%s", want, got.Raw)
	}
}

// responsesSSEFrames builds the exact expected downstream Responses SSE body
// for the direct /v1/responses passthrough given the raw upstream SSE event
// strings. Each event becomes one `event: <type>\ndata: <raw>\n\n` frame (the
// event line type is the event JSON's `type` field). It returns the full body
// and the per-frame expected raw block strings so callers can byte-assert both
// the whole body (event/data line + `\n\n` separator lineage) and each frame.
func responsesSSEFrames(events ...string) (string, []string) {
	var b strings.Builder
	frames := make([]string, 0, len(events))
	for _, e := range events {
		etype := gjson.Parse(e).Get("type").String()
		fr := "event: " + etype + "\ndata: " + e + "\n\n"
		frames = append(frames, fr)
		b.WriteString(fr)
	}
	return b.String(), frames
}

func framesByType(frames []sseFrame, etype string) []sseFrame {
	var out []sseFrame
	for _, f := range frames {
		if f.Etype == etype {
			out = append(out, f)
		}
	}
	return out
}

// signatureDeltas returns the signature values from all signature_delta deltas.
func signatureDeltas(frames []sseFrame) []string {
	var sigs []string
	for _, f := range framesByType(frames, "content_block_delta") {
		if f.Data.Get("delta.type").String() == "signature_delta" {
			sigs = append(sigs, f.Data.Get("delta.signature").String())
		}
	}
	return sigs
}

// assertSSEStateMachineINV validates design §6.1 INV-1..3 over the exact
// downstream Claude SSE frame sequence:
//
//	INV-1: message_start is emitted before any content_block_* frame, and
//	       message_stop closes the stream (no business frame follows it).
//	INV-2: every opened content block runs the complete, non-overlapping
//	       lifecycle content_block_start -> 0+ content_block_delta ->
//	       content_block_stop; a delta/stop only ever references the currently
//	       open block, and no block is left open at message_stop.
//	INV-3: block indices start at 0 and are strictly increasing across blocks
//	       (gaps are permitted; the codex pipeline can skip intermediate slots).
func assertSSEStateMachineINV(t *testing.T, frames []sseFrame) {
	t.Helper()
	startAt, stopAt, firstBlock := -1, -1, -1
	for i, f := range frames {
		switch f.Etype {
		case "message_start":
			if startAt != -1 {
				t.Fatalf("duplicate message_start")
			}
			startAt = i
		case "message_stop":
			stopAt = i
		case "content_block_start":
			if firstBlock == -1 {
				firstBlock = i
			}
		}
	}
	if startAt < 0 {
		t.Fatal("missing message_start frame")
	}
	if firstBlock < 0 {
		t.Fatal("stream has no content_block_start frame")
	}
	// INV-1: message_start must precede the first content-block frame.
	if startAt > firstBlock {
		t.Fatalf("message_start@%d emitted after content_block_start@%d (INV-1)", startAt, firstBlock)
	}
	if stopAt < 0 {
		t.Fatal("missing message_stop frame")
	}
	// INV-1: no business frames after message_stop.
	for i := stopAt + 1; i < len(frames); i++ {
		et := frames[i].Etype
		if strings.HasPrefix(et, "content_block_") || et == "message_delta" {
			t.Fatalf("business frame %q emitted after message_stop (INV-1)", et)
		}
	}
	// INV-2/INV-3: per-block lifecycle walk with strictly increasing indices.
	openIdx, open := -1, false
	for i := startAt + 1; i < stopAt; i++ {
		f := frames[i]
		switch f.Etype {
		case "content_block_start":
			if open {
				t.Fatalf("content_block_start while block %d still open (INV-2)", openIdx)
			}
			idx := f.Data.Get("index").Int()
			if idx <= int64(openIdx) {
				t.Fatalf("content_block index not strictly increasing (INV-3): %d after %d", idx, openIdx)
			}
			if openIdx == -1 && idx != 0 {
				t.Fatalf("first content_block index = %d, want 0 (INV-3)", idx)
			}
			openIdx = int(idx)
			open = true
		case "content_block_delta":
			if !open || f.Data.Get("index").Int() != int64(openIdx) {
				t.Fatalf("content_block_delta@%d outside open block %d (INV-2)", f.Data.Get("index").Int(), openIdx)
			}
		case "content_block_stop":
			if !open || f.Data.Get("index").Int() != int64(openIdx) {
				t.Fatalf("content_block_stop@%d has no matching open block %d (INV-2)", f.Data.Get("index").Int(), openIdx)
			}
			open = false
		}
	}
	if open {
		t.Fatalf("block %d left open at message_stop (INV-2)", openIdx)
	}
}

// lastConsumeLog returns the latest consume-log row for the env user as a struct.
func (env *fullPathEnv) lastConsumeLog(t *testing.T) *model.Log {
	t.Helper()
	var log model.Log
	if err := model.DB.Where("user_id = ?", env.userID).Order("id desc").First(&log).Error; err != nil {
		t.Fatalf("load consume log: %v", err)
	}
	return &log
}

// lastConsumeLogOther returns the `other` JSON of the latest consume-log row for the env user.
func (env *fullPathEnv) lastConsumeLogOther(t *testing.T) gjson.Result {
	t.Helper()
	return gjson.Parse(env.lastConsumeLog(t).Other)
}

// countDiag counts occurrences of a given diagnostic code in admin_info.conversion_diagnostics.
func (env *fullPathEnv) countDiag(t *testing.T, code string) int {
	t.Helper()
	other := env.lastConsumeLogOther(t)
	arr := other.Get("admin_info.conversion_diagnostics")
	n := 0
	arr.ForEach(func(_, v gjson.Result) bool {
		if v.Get("code").String() == code {
			n++
		}
		return true
	})
	return n
}

// countDiagEntries returns the total distinct entries retained in
// admin_info.conversion_diagnostics (the bounded diagnostics buffer).
func (env *fullPathEnv) conversionDiagTotal(t *testing.T) int64 {
	t.Helper()
	return env.lastConsumeLogOther(t).Get("admin_info.conversion_diagnostics.#").Int()
}

// sseEvents writes a downstream `event: type` sequence with `data: ...` payloads
// (SSE wire format matching the codex bridge upstream).
func sseEvents(events []string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		for _, e := range events {
			_, _ = io.WriteString(w, "data: "+e+"\n\n")
		}
	}
}

// newB1HappyPathEvents drives two reasoning items A/B whose events genuinely
// interleave on the wire: item B starts (added) while item A is not yet done,
// then A's done, then B's summary + done, plus a duplicate done for B. This
// exercises per-reasoning-item isolation (A closing must not corrupt B), each
// item emitting exactly its own final signature once, and duplicate-done
// idempotency.
func newB1HappyPathEvents() []string {
	return []string{
		`{"type":"response.created","response":{"id":"resp_b1","model":"codex-luna"}}`,
		`{"type":"response.output_item.added","item":{"type":"reasoning","id":"reason_A","encrypted_content":"sigA_initial"}}`,
		`{"type":"response.reasoning_summary_part.added","item_id":"reason_A"}`,
		`{"type":"response.reasoning_summary_text.delta","item_id":"reason_A","delta":"thinking A1"}`,
		`{"type":"response.reasoning_summary_part.done","item_id":"reason_A"}`,
		// B 在 A done 之前开始 → A/B 真正交错:B 已 added 时 A 块仍在开合中。
		`{"type":"response.output_item.added","item":{"type":"reasoning","id":"reason_B","encrypted_content":"sigB_initial"}}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","id":"reason_A","encrypted_content":"sigA_final"}}`,
		`{"type":"response.reasoning_summary_part.added","item_id":"reason_B"}`,
		`{"type":"response.reasoning_summary_text.delta","item_id":"reason_B","delta":"thinking B1"}`,
		`{"type":"response.reasoning_summary_part.done","item_id":"reason_B"}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","id":"reason_B","encrypted_content":"sigB_final"}}`,
		// duplicate done for B: idempotent, must not emit a second signature.
		`{"type":"response.output_item.done","item":{"type":"reasoning","id":"reason_B","encrypted_content":"sigB_final"}}`,
		`{"type":"response.content_part.added"}`,
		`{"type":"response.output_text.delta","delta":"hello world","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_b1","model":"codex-luna","usage":{"input_tokens":41,"output_tokens":17,"total_tokens":58}}}`,
	}
}

// newB1EarlyCloseEvents builds a single reasoning item whose block is opened then
// closed early (text starts before its done), so the late done signature is dropped.
func newB1EarlyCloseEvents() []string {
	return []string{
		`{"type":"response.created","response":{"id":"resp_b1ec","model":"codex-luna"}}`,
		`{"type":"response.output_item.added","item":{"type":"reasoning","id":"reason_EC"}}`,
		`{"type":"response.reasoning_summary_part.added","item_id":"reason_EC"}`,
		`{"type":"response.reasoning_summary_text.delta","item_id":"reason_EC","delta":"thinking EC"}`,
		`{"type":"response.reasoning_summary_part.done","item_id":"reason_EC"}`,
		// text boundary closes the thinking block before done arrives.
		`{"type":"response.output_text.delta","delta":"early text","output_index":0}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","id":"reason_EC","encrypted_content":"sigEC_final"}}`,
		`{"type":"response.completed","response":{"id":"resp_b1ec","model":"codex-luna","usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8}}}`,
	}
}

func claudeStreamRequest(body string) string {
	return fmt.Sprintf(`{
	  "model":"gpt-5.6-luna",
	  "stream":true,
	  "max_tokens":1024,
	  "messages":[{"role":"user","content":"%s"}]
	}`, body)
}

// ---------------------------------------------------------------------------
// B-1: Claude Code multi-turn stream end-to-end fidelity.
// ---------------------------------------------------------------------------

func TestFullPath_B1_StreamMultiTurn(t *testing.T) {
	env := newFullPathEnv(t)

	// --- happy path: genuinely interleaved A/B reasoning + text + duplicate done
	env.upstream.set(sseEvents(newB1HappyPathEvents()))
	body := env.postMessages(t, claudeStreamRequest("hello"))
	frames := parseClaudeSSE(t, body)

	msgStart := framesByType(frames, "message_start")
	if len(msgStart) != 1 {
		t.Fatalf("expected one message_start, got %d", len(msgStart))
	}

	// SSE state machine (INV-1..3): message_start before every content_block_*,
	// per-block start->delta->stop lifecycle, strictly increasing block indices,
	// and message_stop closing the stream with no trailing business frame.
	assertSSEStateMachineINV(t, frames)

	// SSE state machine: two thinking blocks (A/B) then one text block.
	starts := framesByType(frames, "content_block_start")
	stops := framesByType(frames, "content_block_stop")
	thinkingStarts := 0
	textStarts := 0
	for _, f := range starts {
		switch f.Data.Get("content_block.type").String() {
		case "thinking":
			thinkingStarts++
		case "text":
			textStarts++
		}
	}
	if thinkingStarts != 2 {
		t.Fatalf("expected 2 thinking block starts, got %d", thinkingStarts)
	}
	if textStarts != 1 {
		t.Fatalf("expected 1 text block start, got %d", textStarts)
	}
	// Every started block must be stopped exactly once (INV-1).
	if len(starts) != len(stops) {
		t.Fatalf("started blocks != stopped blocks: %d vs %d", len(starts), len(stops))
	}

	// signature equality & per-item isolation under interleaving: even though B was
	// added before A's done, each item emits exactly its own final signature once;
	// the duplicate done for B must NOT emit a second signature (idempotent).
	sigs := signatureDeltas(frames)
	if len(sigs) != 2 {
		t.Fatalf("expected exactly 2 signature_delta (one per interleaved item, duplicate done idempotent), got %v", sigs)
	}
	if sigs[0] != "sigA_final" || sigs[1] != "sigB_final" {
		t.Fatalf("signatures = %v, want [sigA_final sigB_final]", sigs)
	}
	// Pre-content snapshots must not leak into the stream (per-item value isolation).
	if strings.Contains(body, "sigA_initial") || strings.Contains(body, "sigB_initial") {
		t.Fatalf("pre-content encrypted_content leaked into stream")
	}

	// text content present, in the right delta.
	var text strings.Builder
	for _, f := range framesByType(frames, "content_block_delta") {
		if f.Data.Get("delta.type").String() == "text_delta" {
			text.WriteString(f.Data.Get("delta.text").String())
		}
	}
	if text.String() != "hello world" {
		t.Fatalf("text = %q, want %q", text.String(), "hello world")
	}

	// usage matches mock terminal state.
	md := framesByType(frames, "message_delta")
	if len(md) == 0 {
		t.Fatal("no message_delta frame")
	}
	usage := md[len(md)-1].Data.Get("usage")
	if usage.Get("input_tokens").Int() != 41 || usage.Get("output_tokens").Int() != 17 {
		t.Fatalf("usage = %s, want input=41 output=17", usage.Raw)
	}

	if len(framesByType(frames, "message_stop")) != 1 {
		t.Fatal("expected one message_stop")
	}

	// ---------------------------------------------------------------------------
	// early-close scenario: a late done signature is dropped after the block was
	// already closed by text; only one diagnostic is recorded and capacity is not
	// exhausted, so signature_dropped_after_early_close count == 1 (no truncation).
	// Per design §6.1 INV-2 state ③ (explicitly accepted degradation), the dropped
	// item must emit ZERO signature_delta frames downstream — the late terminal
	// value is never补发/replayed.
	// ---------------------------------------------------------------------------
	env.upstream.set(sseEvents(newB1EarlyCloseEvents()))
	bodyEC := env.postMessages(t, claudeStreamRequest("second turn"))
	framesEC := parseClaudeSSE(t, bodyEC)
	if sigs := signatureDeltas(framesEC); len(sigs) != 0 {
		t.Fatalf("early-close (INV-2 state 3) must emit 0 signature_delta, got %v", sigs)
	}
	assertSSEStateMachineINV(t, framesEC)
	if env.countDiag(t, "signature_dropped_after_early_close") != 1 {
		t.Fatalf("signature_dropped_after_early_close count != 1")
	}
	other := env.lastConsumeLogOther(t)
	if other.Get("admin_info.conversion_diagnostics_truncated").Bool() {
		t.Fatal("diagnostics unexpectedly truncated with only one dropped item")
	}
}

// TestFullPath_B1_DiagnosticsTruncated first fills the bounded diagnostics buffer
// (32 distinct entries) and then drives additional dropped events, asserting the
// retained count DOES NOT increase past capacity (precise ==32 total, with the
// request-path diagnostics fixed at 3) and conversion_diagnostics_truncated=true.
func TestFullPath_B1_DiagnosticsTruncated(t *testing.T) {
	env := newFullPathEnv(t)

	events := []string{`{"type":"response.created","response":{"id":"resp_trunc","model":"codex-luna"}}`}
	// Drive far more dropped items than the buffer can hold. The buffer has a fixed
	// 32-entry cap; the request path pre-occupies exactly 3 distinct entries
	// (upstream_stream_forced / request_field_shrink / downstream_stream), so the
	// signature_dropped_after_early_close category can retain exactly 32-3 = 29.
	const totalDrops = 40
	for i := 0; i < totalDrops; i++ {
		events = append(events,
			fmt.Sprintf(`{"type":"response.output_item.added","item":{"type":"reasoning","id":"r_%d"}}`, i),
			fmt.Sprintf(`{"type":"response.reasoning_summary_part.added","item_id":"r_%d"}`, i),
			fmt.Sprintf(`{"type":"response.reasoning_summary_text.delta","item_id":"r_%d","delta":"t%d"}`, i, i),
			fmt.Sprintf(`{"type":"response.reasoning_summary_part.done","item_id":"r_%d"}`, i),
			`{"type":"response.output_text.delta","delta":"x","output_index":0}`,
			fmt.Sprintf(`{"type":"response.output_item.done","item":{"type":"reasoning","id":"r_%d","encrypted_content":"sig%d"}`, i, i)+`}`,
		)
	}
	events = append(events, `{"type":"response.completed","response":{"id":"resp_trunc","model":"codex-luna","usage":{"input_tokens":4,"output_tokens":2,"total_tokens":6}}}`)
	env.upstream.set(sseEvents(events))
	env.postMessages(t, claudeStreamRequest("many turns"))

	other := env.lastConsumeLogOther(t)
	// Truncation flag must be set once capacity was exceeded.
	if !other.Get("admin_info.conversion_diagnostics_truncated").Bool() {
		t.Fatal("conversion_diagnostics_truncated not set after exhausting capacity")
	}
	// Precise total: the bounded buffer holds exactly 32 entries, never more.
	if total := env.conversionDiagTotal(t); total != 32 {
		t.Fatalf("conversion_diagnostics retained %d entries, want exactly 32 (bounded)", total)
	}
	// Precise dropped count: all 40 driven drops could not fit; exactly 29 fit
	// (32 cap minus 3 fixed request-path entries). Driving 40 drops must NOT grow
	// the retained count — the first 29 fill to cap, the remaining 11 are dropped.
	dropCount := env.countDiag(t, "signature_dropped_after_early_close")
	if dropCount != 29 {
		t.Fatalf("signature_dropped_after_early_close retained %d, want exactly 29 (32-3 fixed); extra drops must not increase the retained count", dropCount)
	}
}

// TestFullPath_B1_RetryFreshSession drives a real controller retry (attempt 1 fails
// pre-commit with a retriable upstream 5xx, attempt 2 succeeds) through the
// registered /v1/messages route, and asserts in full-path that:
//   - the upstream received exactly two requests (a retry happened, i.e. a second
//     `BridgeClaudeMessages`/request-session instance was created for the attempt);
//   - each attempt's outbound body is independently and freshly built (forced
//     stream=true + 64-byte tool-name shrink present in BOTH requests), i.e. no
//     cross-retry session/mapping state leaks into the retried request;
//   - the original RelayInfo scalars are unchanged by the bridge's attempt-local
//     shallow copy: consume log keeps request_path=/v1/messages (RequestURLPath not
//     overwritten) and IsStream=true on the log row;
//   - the final downstream response is a complete, valid Claude SSE stream (the
//     fresh second session processed the whole stream cleanly).
func TestFullPath_B1_RetryFreshSession(t *testing.T) {
	env := newFullPathEnv(t)
	common.RetryTimes = 1 // allow exactly one controller retry; restored by cleanup.

	tool := "tool_retry_" + strings.Repeat("r", 56) // >64 bytes -> must be shrunk upstream.

	var reqs int32
	var bodies [][]byte
	env.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, b)
		if atomic.AddInt32(&reqs, 1) == 1 {
			// Attempt 1: retriable pre-commit failure (upstream 5xx). The bridge
			// returns (nil, err) with no downstream bytes, so the controller retries.
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"type":"server_error","message":"transient"}}`)
			return
		}
		// Attempt 2: valid codex SSE stream with a reasoning signature + text.
		w.Header().Set("Content-Type", "text/event-stream")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		_, _ = io.WriteString(w, "data: "+`{"type":"response.created","response":{"id":"resp_retry","model":"codex-luna"}}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"response.output_item.added","item":{"type":"reasoning","id":"reason_rt","encrypted_content":"sig_rt"}}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"response.output_item.done","item":{"type":"reasoning","id":"reason_rt","encrypted_content":"sig_rt"}}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"response.content_part.added"}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"response.output_text.delta","delta":"retry ok","output_index":0}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"resp_retry","model":"codex-luna","usage":{"input_tokens":6,"output_tokens":4,"total_tokens":10}}}`+"\n\n")
	})

	reqBody := fmt.Sprintf(`{
	  "model":"gpt-5.6-luna",
	  "stream":true,
	  "max_tokens":1024,
	  "tools":[{"name":%q,"description":"retry tool","input_schema":{"type":"object"}}],
	  "messages":[{"role":"user","content":"retry me"}]
	}`, tool)
	body := env.postMessages(t, reqBody)

	// A retry happened: two upstream requests (two attempt sessions).
	if reqs != 2 {
		t.Fatalf("upstream received %d requests, want 2 (controller retry created a fresh session)", reqs)
	}
	if len(bodies) != 2 {
		t.Fatalf("captured %d request bodies, want 2", len(bodies))
	}
	// Both attempts carry a freshly built, complete codex outbound body: forced
	// stream=true and the tool name shrunk to <=64 bytes on EACH attempt (a leaked
	// request-session/mapping would corrupt or skip the second attempt's build).
	for i, b := range bodies {
		parsed := gjson.ParseBytes(b)
		if !parsed.Get("stream").Bool() {
			t.Fatalf("attempt %d did not force upstream stream=true", i+1)
		}
		var toolNamesOK int
		parsed.Get("tools").ForEach(func(_, tool gjson.Result) bool {
			if tool.Get("type").String() == "function" {
				if n := len([]byte(tool.Get("name").String())); n > 64 {
					t.Fatalf("attempt %d tool name exceeds 64 bytes on retry (fresh shrink missing): %q", i+1, tool.Get("name").String())
				}
				toolNamesOK++
			}
			return true
		})
		if toolNamesOK != 1 {
			t.Fatalf("attempt %d expected 1 function tool upstream, got %d", i+1, toolNamesOK)
		}
	}

	// Original RelayInfo scalars are unchanged by the attempt-local shallow copy
	// (observable via the consume-log bookkeeping driven by the original info):
	// request_path stays /v1/messages (RequestURLPath not overwritten to /v1/responses)
	// and the log row records IsStream=true (not flipped to false by upstreamInfo).
	logRow := env.lastConsumeLog(t)
	if got := gjson.Parse(logRow.Other).Get("request_path").String(); got != "/v1/messages" {
		t.Fatalf("original RelayInfo RequestURLPath leaked: consume log request_path=%q, want /v1/messages", got)
	}
	if !logRow.IsStream {
		t.Fatal("original RelayInfo IsStream corrupted to false by attempt-local shallow copy")
	}
	// Both attempts used the same channel: admin_info.use_channel = ["1","1"].
	if got := gjson.Parse(logRow.Other).Get("admin_info.use_channel").String(); got != `["1","1"]` {
		t.Fatalf("consume log use_channel = %s, want [\"1\",\"1\"] (two attempts on channel 1)", got)
	}

	// The fresh second session processed the whole stream cleanly: one start, one
	// signature, one stop, text, one stop.
	frames := parseClaudeSSE(t, body)
	if len(framesByType(frames, "message_start")) != 1 || len(framesByType(frames, "message_stop")) != 1 {
		t.Fatalf("final downstream response is not a complete SSE stream (start/stop mismatch)")
	}
	if sigs := signatureDeltas(frames); len(sigs) != 1 || sigs[0] != "sig_rt" {
		t.Fatalf("final downstream signature = %v, want [sig_rt]", sigs)
	}
	var text strings.Builder
	for _, f := range framesByType(frames, "content_block_delta") {
		if f.Data.Get("delta.type").String() == "text_delta" {
			text.WriteString(f.Data.Get("delta.text").String())
		}
	}
	if text.String() != "retry ok" {
		t.Fatalf("final downstream text = %q, want %q", text.String(), "retry ok")
	}
}

// mockRelayBillingT055 is a minimal relaycommon.BillingSettler used only to give
// the original RelayInfo an observable (non-nil) Billing reference field. The
// bridge never calls billing itself, so its value here is solely an invariant
// sentinel.
type mockRelayBillingT055 struct{}

func (*mockRelayBillingT055) Settle(actualQuota int) error  { return nil }
func (*mockRelayBillingT055) Refund(c *gin.Context)         {}
func (*mockRelayBillingT055) NeedsRefund() bool             { return false }
func (*mockRelayBillingT055) GetPreConsumedQuota() int      { return 0 }
func (*mockRelayBillingT055) Reserve(targetQuota int) error { return nil }

// TestFullPath_B1_RelayInfoInvariance asserts the full RelayInfo immutability
// contract behind the attempt-local shallow copy (design §13.3 B-1 / §9 R-007 /
// D-5b): the bridge must only ever overwrite the three scalars on ITS copy, and
// must leave the ORIGINAL RelayInfo untouched — all three scalars (IsStream /
// RelayMode / RequestURLPath) plus every reference field the shallow copy shares
// (RequestHeaders map / RealtimeTools slice / ReasoningConversion pointer /
// Billing interface). Those reference fields are only observable by holding the
// original pointer, so this full-path test drives the real bridge entry
// (codex.BridgeClaudeMessages) against the same mock upstream the HTTP-based
// tests use, then compares the original pointer's fields before/after.
func TestFullPath_B1_RelayInfoInvariance(t *testing.T) {
	env := newFullPathEnv(t)
	env.upstream.set(sseEvents(newB1HappyPathEvents()))

	// Reconstruct the original /v1/messages RelayInfo (mirroring what
	// controller.Relay + genBaseRelayInfo produce) with the three scalars the
	// shallow copy is allowed to overwrite and every shared reference field
	// populated, so any write-through by the bridge is detectable.
	orig := &relaycommon.RelayInfo{
		RelayFormat:         types.RelayFormatClaude,
		RelayMode:           123456, // sentinel, distinct from RelayModeResponses
		IsStream:            true,
		OriginModelName:     "gpt-5.6-luna",
		RequestURLPath:      "/v1/messages",
		RequestHeaders:      map[string]string{"Content-Type": "application/json", "X-Sentinel": "keep"},
		RealtimeTools:       []dto.RealTimeTool{{Description: "realtime-sentinel"}},
		ReasoningConversion: &dto.ReasoningConversionState{},
		Billing:             &mockRelayBillingT055{},
		ChannelMeta: &relaycommon.ChannelMeta{
			ChannelType:       constant.ChannelTypeCodex,
			ChannelBaseUrl:    env.baseURL,
			UpstreamModelName: "codex-luna",
			ApiKey:            `{"access_token":"tok-fullpath","account_id":"acct-fullpath"}`,
		},
	}
	orig.SetEstimatePromptTokens(15)

	// "Before" snapshot of the original RelayInfo.
	preMode := orig.RelayMode
	preIsStream := orig.IsStream
	preURLPath := orig.RequestURLPath
	preHeaders := maps.Clone(orig.RequestHeaders)
	preTools := slices.Clone(orig.RealtimeTools)
	preRC := orig.ReasoningConversion
	preBilling := orig.Billing

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("Content-Type", "application/json")

	stream := true
	usage, apiErr := codex.BridgeClaudeMessages(c, orig, &codex.Adaptor{}, &dto.ClaudeRequest{
		Model:    "gpt-5.6-luna",
		Messages: []dto.ClaudeMessage{{Role: "user", Content: "hello"}},
		Stream:   &stream,
	})
	if apiErr != nil {
		t.Fatalf("BridgeClaudeMessages unexpected error: %v", apiErr)
	}
	if usage == nil {
		t.Fatal("usage is nil, want non-nil")
	}

	// ① 三标量:bridge 不改写原始 RelayInfo(只在它自己尝试级副本上改这三标量)。
	if orig.IsStream != preIsStream || !orig.IsStream {
		t.Fatalf("original RelayInfo.IsStream corrupted: %v (pre %v)", orig.IsStream, preIsStream)
	}
	if orig.RelayMode != preMode {
		t.Fatalf("original RelayInfo.RelayMode rewritten: %v (pre %v)", orig.RelayMode, preMode)
	}
	if orig.RelayMode == relayconstant.RelayModeResponses {
		t.Fatal("original RelayInfo.RelayMode overwritten to RelayModeResponses by shallow copy")
	}
	if orig.RequestURLPath != preURLPath || orig.RequestURLPath != "/v1/messages" {
		t.Fatalf("original RelayInfo.RequestURLPath rewritten: %q", orig.RequestURLPath)
	}

	// ② 全部共享引用字段前后等值/未变更。
	if !reflect.DeepEqual(orig.RequestHeaders, preHeaders) {
		t.Fatalf("original RelayInfo.RequestHeaders mutated: %v", orig.RequestHeaders)
	}
	if !reflect.DeepEqual(orig.RealtimeTools, preTools) {
		t.Fatalf("original RelayInfo.RealtimeTools mutated: %v", orig.RealtimeTools)
	}
	if orig.ReasoningConversion != preRC || orig.ReasoningConversion == nil {
		t.Fatal("original RelayInfo.ReasoningConversion pointer changed/released")
	}
	if orig.Billing != preBilling || orig.Billing == nil {
		t.Fatal("original RelayInfo.Billing interface value changed/released")
	}
}

// ---------------------------------------------------------------------------
// B-2: tool round-trip (historical tool_use/tool_result + parallel calls +
// 64 / 65 / longer-byte name boundary + call_id 64/65-byte shrink & restore +
// same-prefix collision + unmapped passthrough).
// ---------------------------------------------------------------------------

// unmappedCallID is the call_id of the one parallel function_call (unmapped_func)
// whose name AND call_id appear in NO request mapping: per INV-4 it must pass
// through downstream verbatim (name unmapped_func, id = this call_id), never being
// routed through or fabricated from the historical call-hist-1 mapping.
const unmappedCallID = "brand_new_unmapped_call_id_0001"

func TestFullPath_B2_ToolRoundTrip(t *testing.T) {
	env := newFullPathEnv(t)

	// Original (client-facing) tool names spanning the 64-byte boundary:
	//   tool64  = exactly 64 bytes  -> passes through unchanged;
	//   tool65  = exactly 65 bytes  -> shrunk to 64 (65-byte boundary sample);
	//   toolAlpha/toolBravo = 66 bytes -> shrunk (>65, the "longer" class);
	//   collideA/collideB share identical first 64 bytes -> must be disambiguated.
	toolAlpha := "tool_alpha_" + strings.Repeat("m", 55) // 66 bytes
	toolBravo := "tool_bravo_" + strings.Repeat("x", 55) // 66 bytes
	tool64 := strings.Repeat("z", 64)                    // exactly 64 bytes -> passthrough
	tool65 := "tool_sixfive_" + strings.Repeat("n", 52)  // exactly 65 bytes -> shrunk
	collideA := strings.Repeat("q", 64) + "AA"           // 66 bytes
	collideB := strings.Repeat("q", 64) + "BB"           // 66 bytes

	// call_id fixtures spanning the 64-byte boundary (historical tool_use /
	// tool_result): call64 (exactly 64 bytes) must pass through verbatim on both
	// function_call and function_call_output; call65 (exactly 65 bytes) must be
	// shrunk on both, and the downstream tool_use must restore the original
	// 65-byte value. Per design D-5, the call_id < = 64 bytes is never touched.
	call64 := strings.Repeat("c", 64) // exactly 64 bytes -> passthrough
	call65 := strings.Repeat("d", 65) // exactly 65 bytes -> shrunk

	var shortNames []string
	var shortCollideA, shortCollideB string
	var fcCallIDs, outputCallIDs []string
	var call65Short string // the shrunk short form of the 65-byte call_id (captured upstream)

	env.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		reqBody, _ := io.ReadAll(r.Body)
		parsed := gjson.ParseBytes(reqBody)

		parsed.Get("tools").ForEach(func(_, tool gjson.Result) bool {
			if tool.Get("type").String() != "function" {
				return true
			}
			name := tool.Get("name").String()
			shortNames = append(shortNames, name)
			if strings.HasPrefix(name, strings.Repeat("q", 8)) {
				if shortCollideA == "" {
					shortCollideA = name
				} else {
					shortCollideB = name
				}
			}
			// Codex 64-byte contract: every tool name on the wire must be <=64 bytes.
			if len([]byte(name)) > 64 {
				http.Error(w, "tool name exceeds 64 bytes: "+name, http.StatusBadRequest)
				return false
			}
			return true
		})
		parsed.Get("input").ForEach(func(_, it gjson.Result) bool {
			switch it.Get("type").String() {
			case "function_call":
				fcCallIDs = append(fcCallIDs, it.Get("call_id").String())
			case "function_call_output":
				outputCallIDs = append(outputCallIDs, it.Get("call_id").String())
			}
			return true
		})
		for _, v := range append(append([]string{}, fcCallIDs...), outputCallIDs...) {
			if len([]byte(v)) > 64 {
				http.Error(w, "call_id exceeds 64 bytes", http.StatusBadRequest)
				return
			}
		}
		// Bidirectional historical association (design B-2): the historical
		// tool_use (id=call-hist-1) must travel upstream as a function_call AND its
		// historically paired tool_result must travel upstream as a
		// function_call_output, BOTH carrying the SAME call_id call-hist-1.
		if !containsString(fcCallIDs, "call-hist-1") {
			http.Error(w, "historical tool_use not assoc as function_call(call-hist-1)", http.StatusBadRequest)
			return
		}
		if !containsString(outputCallIDs, "call-hist-1") {
			http.Error(w, "historical tool_result not assoc as function_call_output(call-hist-1)", http.StatusBadRequest)
			return
		}
		// call_id 64-/65-byte boundary (design D-5): the exactly-64-byte call_id must
		// pass through VERBATIM on both function_call and function_call_output; the
		// exactly-65-byte call_id must be shrunk on BOTH sides (its original value must
		// never appear upstream), and both sides must carry the SAME shrunk short so the
		// bidirecional historical association survives the shrink.
		if !containsString(fcCallIDs, call64) || !containsString(outputCallIDs, call64) {
			http.Error(w, "64-byte call_id not preserved on both function_call & output", http.StatusBadRequest)
			return
		}
		if containsString(fcCallIDs, call65) || containsString(outputCallIDs, call65) {
			http.Error(w, "65-byte call_id not shrunk upstream (original value leaked)", http.StatusBadRequest)
			return
		}
		// The one output call_id that is neither the historical call-hist-1 nor the
		// 64-byte call64 must be the shrunk short of the 65-byte original.
		for _, v := range outputCallIDs {
			if v != "call-hist-1" && v != call64 {
				call65Short = v
			}
		}
		if call65Short == "" || call65Short == call64 || len([]byte(call65Short)) > 64 {
			http.Error(w, "65-byte call_id shrink did not produce a valid <=64-byte short", http.StatusBadRequest)
			return
		}
		if !containsString(fcCallIDs, call65Short) {
			http.Error(w, "shrunk 65-byte call_id not associated on function_call side", http.StatusBadRequest)
			return
		}
		// tool64 (exactly 64 bytes) must have passed through intact.
		var saw64 bool
		for _, n := range shortNames {
			if n == tool64 {
				saw64 = true
			}
		}
		if !saw64 {
			http.Error(w, "64-byte tool name not passed through unchanged", http.StatusBadRequest)
			return
		}
		// Same-prefix collision must be disambiguated into distinct shorts.
		if shortCollideA == "" || shortCollideB == "" || shortCollideA == shortCollideB {
			http.Error(w, "same-prefix collision not present and disambiguated", http.StatusBadRequest)
			return
		}

		alphaShort := shortToOriginalNameByPrefix(shortNames, "tool_alpha")
		bravoShort := shortToOriginalNameByPrefix(shortNames, "tool_bravo")
		sixfiveShort := shortToOriginalNameByPrefix(shortNames, "tool_sixfive")
		if alphaShort == "" || bravoShort == "" || sixfiveShort == "" {
			http.Error(w, "expected alpha/bravo/sixfive tools upstream", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		write := func(data string) { _, _ = io.WriteString(w, "data: "+data+"\n\n") }
		write(`{"type":"response.created","response":{"id":"resp_b2","model":"codex-luna"}}`)
		// Four parallel function_call items (parallel_tool_calls=true upstream).
		// Downstream order must match, and each must restore its original name, exact
		// item id, and per-call arguments.
		write(`{"type":"response.output_item.added","item":{"type":"function_call","id":"fc_1","name":"` + alphaShort + `"}}`)
		write(`{"type":"response.function_call_arguments.delta","delta":"{\"q\":\"hist-result\"}","output_index":0}`)
		write(`{"type":"response.output_item.added","item":{"type":"function_call","id":"fc_2","name":"` + bravoShort + `"}}`)
		write(`{"type":"response.function_call_arguments.delta","delta":"{\"n\":1}","output_index":1}`)
		write(`{"type":"response.output_item.added","item":{"type":"function_call","id":"fc_3","name":"` + sixfiveShort + `"}}`)
		write(`{"type":"response.function_call_arguments.delta","delta":"{\"m\":2}","output_index":2}`)
		write(`{"type":"response.output_item.done","item":{"type":"function_call","id":"fc_1","name":"` + alphaShort + `","call_id":"call-hist-1","arguments":"{\"q\":\"hist-result\"}"}}`)
		write(`{"type":"response.output_item.done","item":{"type":"function_call","id":"fc_2","name":"` + bravoShort + `","call_id":"fc_2_call","arguments":"{\"n\":1}"}}`)
		write(`{"type":"response.output_item.done","item":{"type":"function_call","id":"fc_3","name":"` + sixfiveShort + `","call_id":"fc_3_call","arguments":"{\"m\":2}"}}`)
		// unmapped passthrough (INV-4): name AND call_id appear in no request
		// mapping, so they must pass through downstream verbatim. The item's id equals
		// its call_id here, so the downstream tool_use id is observable as the
		// unmapped call_id passed through unchanged.
		write(`{"type":"response.output_item.added","item":{"type":"function_call","id":"` + unmappedCallID + `","name":"unmapped_func"}}`)
		write(`{"type":"response.output_item.done","item":{"type":"function_call","id":"` + unmappedCallID + `","name":"unmapped_func","call_id":"` + unmappedCallID + `","arguments":"{}"}}`)
		// 65-byte historical call_id round-trip: the upstream function_call comes back
		// tagged with the SHRUNK short as its item id; downstream must restore the exact
		// original 65-byte value (call65) as the tool_use id.
		write(`{"type":"response.output_item.added","item":{"type":"function_call","id":"` + call65Short + `","name":"` + sixfiveShort + `"}}`)
		write(`{"type":"response.output_item.done","item":{"type":"function_call","id":"` + call65Short + `","name":"` + sixfiveShort + `","call_id":"` + call65Short + `","arguments":"{}"}}`)
		// 64-byte historical call_id round-trip: preserved verbatim on the wire, so the
		// upstream function_call keeps the original 64-byte call_id as its item id and
		// downstream restores it unchanged (id == call64).
		write(`{"type":"response.output_item.added","item":{"type":"function_call","id":"` + call64 + `","name":"` + tool64 + `"}}`)
		write(`{"type":"response.output_item.done","item":{"type":"function_call","id":"` + call64 + `","name":"` + tool64 + `","call_id":"` + call64 + `","arguments":"{}"}}`)
		write(`{"type":"response.completed","response":{"id":"resp_b2","model":"codex-luna","usage":{"input_tokens":9,"output_tokens":7,"total_tokens":16}}}`)
	})

	reqBody := fmt.Sprintf(`{
	  "model":"gpt-5.6-luna",
	  "stream":true,
	  "max_tokens":1024,
	  "tools":[
	    {"name":%q,"description":"alpha","input_schema":{"type":"object"}},
	    {"name":%q,"description":"bravo","input_schema":{"type":"object"}},
	    {"name":%q,"description":"sixtyfour","input_schema":{"type":"object"}},
	    {"name":%q,"description":"sixtyfive","input_schema":{"type":"object"}},
	    {"name":%q,"description":"collide a","input_schema":{"type":"object"}},
	    {"name":%q,"description":"collide b","input_schema":{"type":"object"}}
	  ],
	  "messages":[
	    {"role":"user","content":"use the tools"},
	    {"role":"assistant","content":[
	      {"type":"text","text":"calling"},
	      {"type":"tool_use","id":"call-hist-1","name":%q,"input":{"q":"hist"}},
	      {"type":"tool_use","id":%q,"name":%q,"input":{"q":1}},
	      {"type":"tool_use","id":%q,"name":%q,"input":{"q":2}}
	    ]},
	    {"role":"user","content":[
	      {"type":"tool_result","tool_use_id":"call-hist-1","content":"ok"},
	      {"type":"tool_result","tool_use_id":%q,"content":"ok64"},
	      {"type":"tool_result","tool_use_id":%q,"content":"ok65"}
	    ]}
	  ]
	}`, toolAlpha, toolBravo, tool64, tool65, collideA, collideB, toolAlpha, call64, tool64, call65, tool65, call64, call65)

	body := env.postMessages(t, reqBody)
	frames := parseClaudeSSE(t, body)

	// --- upstream-side edge assertions ------------------------------------------
	if len(shortNames) == 0 {
		t.Fatal("upstream received no function tools")
	}
	for _, v := range append(append(append([]string{}, shortNames...), fcCallIDs...), outputCallIDs...) {
		if len([]byte(v)) > 64 {
			t.Fatalf("upstream value exceeds 64 bytes: %q", v)
		}
	}
	// tool64 must pass through exactly; tool65 must be shrunken (its short != original).
	if !containsString(shortNames, tool64) {
		t.Fatal("64-byte tool name did not pass through unchanged upstream")
	}
	if containsString(shortNames, tool65) {
		t.Fatal("65-byte tool name was not shrunk upstream (must differ from original)")
	}
	// same-prefix collision pair must reach upstream as two distinct ≤64-byte shorts.
	if shortCollideA == "" || shortCollideB == "" {
		t.Fatal("same-prefix collision pair not both shrunken upstream")
	}
	if shortCollideA == shortCollideB {
		t.Fatal("same-prefix collision was not disambiguated (both shorts identical)")
	}

	// --- downstream-side restore assertions --------------------------------------
	// Every parallel tool_use must be restored with its exact original name AND exact
	// item id, in upstream order (6 calls):
	//   fc_1 → toolAlpha, fc_2 → toolBravo, fc_3 → tool65,
	//   unmapped → unmapped_func with id = the unmapped call_id passed through as-is;
	//   call65short → tool65 with id = the restored original 65-byte call_id (call65);
	//   call64 → tool64 with id = the unchanged 64-byte call_id.
	type toolUse struct {
		name, id string
	}
	var uses []toolUse
	for _, f := range framesByType(frames, "content_block_start") {
		if f.Data.Get("content_block.type").String() != "tool_use" {
			continue
		}
		uses = append(uses, toolUse{
			name: f.Data.Get("content_block.name").String(),
			id:   f.Data.Get("content_block.id").String(),
		})
	}
	wantUses := []toolUse{
		{name: toolAlpha, id: "fc_1"},
		{name: toolBravo, id: "fc_2"},
		{name: tool65, id: "fc_3"},
		{name: "unmapped_func", id: unmappedCallID},
		{name: tool65, id: call65}, // 65-byte call_id shrunk upstream, restored exactly downstream
		{name: tool64, id: call64}, // 64-byte call_id preserved verbatim end-to-end
	}
	if len(uses) != len(wantUses) {
		t.Fatalf("expected %d parallel tool_use blocks downstream, got %d: %+v", len(wantUses), len(uses), uses)
	}
	for i, want := range wantUses {
		if uses[i].name != want.name || uses[i].id != want.id {
			t.Fatalf("downstream tool_use[%d] = {name:%q id:%q}, want {name:%q id:%q}", i, uses[i].name, uses[i].id, want.name, want.id)
		}
	}
	// Explicit unmapped call_id passthrough (INV-4): the downstream tool_use id for
	// the unmapped call is byte-for-byte the unmapped call_id, and the id is NOT
	// rewritten to either the historical call-hist-1 or any mapped short id. It is
	// the 4th of the 6 downstream tool_use blocks (index 3).
	unmapped := uses[3]
	if unmapped.name != "unmapped_func" || unmapped.id != unmappedCallID {
		t.Fatalf("unmapped call_id passthrough broken: got id=%q name=%q", unmapped.id, unmapped.name)
	}
	if unmapped.id == "call-hist-1" || unmapped.id == "fc_1" {
		t.Fatalf("unmapped call_id was remapped into historical/mapped id %q (INV-4 passthrough violated)", unmapped.id)
	}

	// Per-call arguments: every parallel call's arguments survive into downstream
	// input_json_delta frames, asserted per call by concatenated partial JSON in
	// upstream emit order. fc_4 (unmapped passthrough) and the call65/call64
	// round-trip calls each keep their own `{}` arguments.
	argsByCall := []string{}
	cur := strings.Builder{}
	for _, f := range framesByType(frames, "content_block_delta") {
		if f.Data.Get("delta.type").String() == "input_json_delta" {
			if cur.Len() > 0 {
				argsByCall = append(argsByCall, cur.String())
				cur.Reset()
			}
			cur.WriteString(f.Data.Get("delta.partial_json").String())
		}
	}
	if cur.Len() > 0 {
		argsByCall = append(argsByCall, cur.String())
	}
	wantArgs := []string{`{"q":"hist-result"}`, `{"n":1}`, `{"m":2}`, `{}`, `{}`, `{}`}
	if len(argsByCall) != len(wantArgs) {
		t.Fatalf("expected %d per-call arguments downstream, got %v", len(wantArgs), argsByCall)
	}
	for i, want := range wantArgs {
		if argsByCall[i] != want {
			t.Fatalf("arguments[%d] = %q, want %q", i, argsByCall[i], want)
		}
	}
}

// shortToOriginalNameByPrefix finds the upstream short tool name that started with
// the given original prefix.
func shortToOriginalNameByPrefix(shorts []string, prefix string) string {
	for _, s := range shorts {
		if strings.HasPrefix(s, prefix) {
			return s
		}
	}
	return ""
}

// containsString reports whether s appears in the slice.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// claudeNonStreamRequest builds a stream:false /v1/messages request body. The
// bridge still forces the codex upstream to SSE, but the downstream consumer is
// the aggregate (non-stream) handler.
func claudeNonStreamRequest(body string) string {
	return fmt.Sprintf(`{
	  "model":"gpt-5.6-luna",
	  "stream":false,
	  "max_tokens":1024,
	  "messages":[{"role":"user","content":"%s"}]
	}`, body)
}

// assertClaudeErrorEnvelope asserts body is a single non-SSE Claude error envelope
// `{"type":"error","error":{...}}` written by the controller (aggregate / pre-commit
// two-phase errors always end here) with a non-2xx status, and returns the error object.
func assertClaudeErrorEnvelope(t *testing.T, code int, body string) gjson.Result {
	t.Helper()
	if code < 400 || code >= 600 {
		t.Fatalf("expected non-2xx error status, got %d", code)
	}
	if !gjson.Valid(body) {
		t.Fatalf("error body is not a single JSON document: %q", body)
	}
	if strings.Contains(body, "event: ") {
		t.Fatalf("pre-commit/two-phase error must be a JSON envelope, got SSE framing")
	}
	r := gjson.Parse(body)
	if r.Get("type").String() != "error" {
		t.Fatalf("expected Claude error envelope type=error, got %s", r.Raw)
	}
	return r.Get("error")
}

// joinedTextDeltas concatenates all text_delta payloads across frames.
func joinedTextDeltas(frames []sseFrame) string {
	var b strings.Builder
	for _, f := range framesByType(frames, "content_block_delta") {
		if f.Data.Get("delta.type").String() == "text_delta" {
			b.WriteString(f.Data.Get("delta.text").String())
		}
	}
	return b.String()
}

// webSearchServerIDs extracts the `server_tool_use` content_block ids from frames.
func webSearchServerIDs(frames []sseFrame) []string {
	var ids []string
	for _, f := range framesByType(frames, "content_block_start") {
		if f.Data.Get("content_block.type").String() == "server_tool_use" {
			ids = append(ids, f.Data.Get("content_block.id").String())
		}
	}
	return ids
}

// webSearchToolResultIDs extracts the `web_search_tool_result` tool_use_id values.
func webSearchToolResultIDs(frames []sseFrame) []string {
	var ids []string
	for _, f := range framesByType(frames, "content_block_start") {
		if f.Data.Get("content_block.type").String() == "web_search_tool_result" {
			ids = append(ids, f.Data.Get("content_block.tool_use_id").String())
		}
	}
	return ids
}

// ---------------------------------------------------------------------------
// B-3: non-stream aggregate (downstream stream=false). Single JSON matches the
// terminal state; failed / stream error / parse failure / pre-terminal EOF all
// land in a controller HTTP error envelope; partial content is never delivered
// as success alongside a failing terminal.
// ---------------------------------------------------------------------------

func TestFullPath_B3_NonStreamAggregate(t *testing.T) {
	env := newFullPathEnv(t)

	// --- ① success: single JSON matches terminal state ------------------------
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"resp_b3","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"hello aggregate","output_index":0}`,
		`{"type":"response.completed","response":{"id":"resp_b3","model":"codex-luna","usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8}}}`,
	}))
	body := env.postMessages(t, claudeNonStreamRequest("aggregate"))
	if env.rec.Code != http.StatusOK {
		t.Fatalf("non-stream success status = %d, want 200", env.rec.Code)
	}
	if !gjson.Valid(body) {
		t.Fatalf("non-stream success body is not a single JSON document: %q", body)
	}
	if strings.Contains(body, "event: ") {
		t.Fatalf("non-stream success must be a single JSON, not SSE framing")
	}
	r := gjson.Parse(body)
	if r.Get("type").String() != "message" {
		t.Fatalf("non-stream success is not a Claude message JSON: %s", r.Raw)
	}
	if got := r.Get("content.0.text").String(); got != "hello aggregate" {
		t.Fatalf("non-stream text = %q, want %q", got, "hello aggregate")
	}
	// usage matches the mock terminal state.
	if r.Get("usage.input_tokens").Int() != 5 || r.Get("usage.output_tokens").Int() != 3 {
		t.Fatalf("non-stream usage = %s, want input=5 output=3", r.Get("usage").Raw)
	}

	// --- ② response.failed → HTTP envelope ------------------------------------
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"r","model":"codex-luna"}}`,
		`{"type":"response.failed","response":{"id":"r","error":{"type":"server_error","message":"boom failed"}}}`,
	}))
	body = env.postMessages(t, claudeNonStreamRequest("failed"))
	errObj := assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if errObj.Get("type").String() != "server_error" || !strings.Contains(errObj.Get("message").String(), "boom failed") {
		t.Fatalf("response.failed envelope = %s", errObj.Raw)
	}

	// --- ③ stream `error` event → HTTP envelope --------------------------------
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"r","model":"codex-luna"}}`,
		`{"type":"error","error":{"type":"invalid_request","message":"bad request"}}`,
	}))
	body = env.postMessages(t, claudeNonStreamRequest("stream error"))
	errObj = assertClaudeErrorEnvelope(t, env.rec.Code, body)
	// invalid_request 特化为 invalid_request_error(§8.3 规范化)。
	if errObj.Get("type").String() != "invalid_request_error" || !strings.Contains(errObj.Get("message").String(), "bad request") {
		t.Fatalf("stream error envelope = %s", errObj.Raw)
	}

	// --- ④ parse failure → HTTP envelope ----------------------------------------
	env.upstream.set(sseEvents([]string{`{invalid json`}))
	body = env.postMessages(t, claudeNonStreamRequest("parse"))
	errObj = assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if !strings.Contains(errObj.Get("message").String(), "responses stream read error") {
		t.Fatalf("parse-failure envelope message = %s, want responses stream read error", errObj.Get("message").String())
	}

	// --- ⑤ clean EOF before terminal → HTTP envelope -----------------------------
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"r","model":"codex-luna"}}`,
	}))
	body = env.postMessages(t, claudeNonStreamRequest("eof"))
	errObj = assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if !strings.Contains(errObj.Get("message").String(), "upstream stream ended before terminal event") {
		t.Fatalf("EOF envelope message = %s", errObj.Get("message").String())
	}

	// --- ⑥ partial content must NOT be delivered as success when a terminal fails ---
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"r","model":"codex-luna"}}`,
		`{"type":"response.output_text.delta","delta":"partial","output_index":0}`,
		`{"type":"error","error":{"type":"server_error","message":"boom after partial"}}`,
	}))
	body = env.postMessages(t, claudeNonStreamRequest("partial"))
	errObj = assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if errObj.Get("type").String() != "server_error" {
		t.Fatalf("partial-then-error envelope type = %s", errObj.Get("type").String())
	}
	// 部分文本绝不能被当作成功交付:响应必须是 error envelope,不得出现 message 响应体
	// 或任何 content 数组(即不把已累积的部分内容当成成功 JSON 下发)。
	if strings.Contains(body, `"type":"message"`) || strings.Contains(body, `"content"`) {
		t.Fatalf("partial content was delivered as a success message alongside a failing terminal")
	}
}

// ---------------------------------------------------------------------------
// B-4: delayed-commit error two-phase semantics (stream=true) + web_search
// server tool round-trip (stream fallback chain & non-stream output array).
// ---------------------------------------------------------------------------

func TestFullPath_B4_ErrorTwoPhaseAndWebSearch(t *testing.T) {
	env := newFullPathEnv(t)

	// ===========================================================================
	// Part A: delayed-commit error two-phase semantics.
	// ===========================================================================
	// --- pre-commit: the very first event is a stream error; nothing committed, so
	// the controller writes a single HTTP error envelope (one upstream request, no
	// retry, no downstream SSE bytes).
	var preCommitBodies int32
	env.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		atomic.AddInt32(&preCommitBodies, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		_, _ = io.WriteString(w, "data: "+`{"type":"error","error":{"type":"invalid_request","message":"bad request"}}`+"\n\n")
	})
	body := env.postMessages(t, claudeStreamRequest("pre commit"))
	if preCommitBodies != 1 {
		t.Fatalf("pre-commit upstream requests = %d, want exactly 1", preCommitBodies)
	}
	errObj := assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if errObj.Get("type").String() != "invalid_request_error" || !strings.Contains(errObj.Get("message").String(), "bad request") {
		t.Fatalf("pre-commit envelope = %s", errObj.Raw)
	}

	// --- post-commit: partial content commits first, then an error stream event must
	// emit exactly ONE SSE error frame and close the stream — the bridge returns
	// success so NO retry happens (exactly one upstream request).
	var postCommitBodies int32
	env.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		atomic.AddInt32(&postCommitBodies, 1)
		w.Header().Set("Content-Type", "text/event-stream")
		if fl, ok := w.(http.Flusher); ok {
			fl.Flush()
		}
		_, _ = io.WriteString(w, "data: "+`{"type":"response.created","response":{"id":"r","model":"codex-luna"}}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"response.output_text.delta","delta":"hello partial","output_index":0}`+"\n\n")
		_, _ = io.WriteString(w, "data: "+`{"type":"error","error":{"type":"invalid_request","message":"post commit boom"}}`+"\n\n")
	})
	body = env.postMessages(t, claudeStreamRequest("post commit"))
	if postCommitBodies != 1 {
		t.Fatalf("post-commit upstream requests = %d, want exactly 1 (no retry after commit)", postCommitBodies)
	}
	frames := parseClaudeSSE(t, body)
	errFrames := framesByType(frames, "error")
	if len(errFrames) != 1 {
		t.Fatalf("post-commit stream error frames = %d, want exactly 1", len(errFrames))
	}
	if errFrames[0].Data.Get("type").String() != "error" {
		t.Fatalf("error frame data type = %s, want error", errFrames[0].Data.Get("type").String())
	}
	if errFrames[0].Data.Get("error.type").String() != "invalid_request_error" || !strings.Contains(errFrames[0].Data.Get("error.message").String(), "post commit boom") {
		t.Fatalf("post-commit error frame = %s", errFrames[0].Data.Raw)
	}
	// the single error frame is the last frame (stream closed right after it).
	lastErrIdx := -1
	for i, f := range frames {
		if f.Etype == "error" {
			lastErrIdx = i
		}
	}
	if lastErrIdx != len(frames)-1 {
		t.Fatalf("error frame is not the final downstream frame (%d total)", len(frames))
	}
	// partial content was genuinely committed before the error.
	if got := joinedTextDeltas(frames); got != "hello partial" {
		t.Fatalf("post-commit partial text = %q, want %q", got, "hello partial")
	}

	// ===========================================================================
	// Part B: web_search server tool round-trip (stream fallback chain).
	// ===========================================================================
	const wsResult = `[{"url":"https://go.dev","title":"Go"}]`
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"resp_b4ws","model":"codex-luna"}}`,
		// 全空 ID(无 item.id/output_item_id/call_id、无根 item_id、无上一已知 ID)→ 不产出。
		`{"type":"response.output_item.done","item":{"type":"web_search_call","action":{"query":"empty"},"results":` + wsResult + `}}`,
		// 上一已知 ID:added 先记录 wsSeed,随后 all-empty done 回退到 wsSeed 产出一组。
		`{"type":"response.output_item.added","item":{"type":"web_search_call","id":"wsSeed","action":{"query":"seed"},"results":` + wsResult + `}}`,
		`{"type":"response.output_item.done","item":{"type":"web_search_call","action":{"query":"seed"}}}`,
		// item.id
		`{"type":"response.output_item.done","item":{"type":"web_search_call","id":"wsA","action":{"query":"a"},"results":` + wsResult + `}}`,
		// item.output_item_id(OutputItemID typed round-trip 不丢失)
		`{"type":"response.output_item.done","item":{"type":"web_search_call","output_item_id":"wsB","action":{"query":"b"},"results":` + wsResult + `}}`,
		// item.call_id
		`{"type":"response.output_item.done","item":{"type":"web_search_call","call_id":"wsC","action":{"query":"c"},"results":` + wsResult + `}}`,
		// 根 event.item_id
		`{"type":"response.output_item.done","item_id":"wsD","item":{"type":"web_search_call","action":{"query":"d"},"results":` + wsResult + `}}`,
		`{"type":"response.completed","response":{"id":"resp_b4ws","model":"codex-luna","usage":{"input_tokens":20,"output_tokens":10,"total_tokens":30}}}`,
	}))
	body = env.postMessages(t, claudeStreamRequest("web search"))
	frames = parseClaudeSSE(t, body)
	wantIDs := []string{"wsSeed", "wsA", "wsB", "wsC", "wsD"}
	if got := webSearchServerIDs(frames); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("stream server_tool_use ids = %v, want %v", got, wantIDs)
	}
	if got := webSearchToolResultIDs(frames); !reflect.DeepEqual(got, wantIDs) {
		t.Fatalf("stream web_search_tool_result ids = %v, want %v", got, wantIDs)
	}

	// ===========================================================================
	// Part C: non-stream web_search (terminal output array; no root event ids).
	// ===========================================================================
	env.upstream.set(sseEvents([]string{
		`{"type":"response.created","response":{"id":"resp_b4nws","model":"codex-luna"}}`,
		`{"type":"response.completed","response":{"id":"resp_b4nws","model":"codex-luna","usage":{"input_tokens":9,"output_tokens":7,"total_tokens":16},"output":[` +
			`{"type":"web_search_call","id":"wsNA","action":{"query":"na"},"results":[{"url":"https://na.dev","title":"NA"}]},` +
			`{"type":"web_search_call","output_item_id":"wsNB","action":{"query":"nb"},"results":[{"url":"https://nb.dev","title":"NB"}]},` +
			`{"type":"web_search_call","call_id":"wsNC","action":{"query":"nc"},"results":[{"url":"https://nc.dev","title":"NC"}]},` +
			`{"type":"web_search_call","action":{"query":"prevknown"}}` +
			`]}}`,
	}))
	body = env.postMessages(t, claudeNonStreamRequest("web nonstream"))
	if !gjson.Valid(body) {
		t.Fatalf("non-stream web_search body is not a single JSON document: %q", body)
	}
	r := gjson.Parse(body)
	if r.Get("type").String() != "message" {
		t.Fatalf("non-stream web_search JSON is not a message: %s", r.Raw)
	}
	var nwsServer, nwsResultIDs []string
	r.Get("content").ForEach(func(_, cb gjson.Result) bool {
		switch cb.Get("type").String() {
		case "server_tool_use":
			nwsServer = append(nwsServer, cb.Get("id").String())
		case "web_search_tool_result":
			nwsResultIDs = append(nwsResultIDs, cb.Get("tool_use_id").String())
		}
		return true
	})
	// 前三项(item.id / item.output_item_id / item.call_id)+ 上一已知 ID:第 4 个
	// all-empty item 回退到上一已知 ID(wsNC)后被去重,不新增组 → 恰好 3 组。
	wantNWS := []string{"wsNA", "wsNB", "wsNC"}
	if !reflect.DeepEqual(nwsServer, wantNWS) {
		t.Fatalf("non-stream server_tool_use ids = %v, want %v", nwsServer, wantNWS)
	}
	if !reflect.DeepEqual(nwsResultIDs, wantNWS) {
		t.Fatalf("non-stream web_search_tool_result ids = %v, want %v", nwsResultIDs, wantNWS)
	}
	// OutputItemID typed round-trip: wsNB 的 tool_use_id 精确等于其 output_item_id 字段值。
	if nwsServer[1] != "wsNB" {
		t.Fatalf("non-stream OutputItemID round-trip lost: got %q, want wsNB", nwsServer[1])
	}
	// 全空 ID 不产出:server_tool_use 总数恰为 3(wsNA/wsNB/wsNC),all-empty 项无块。
	if got := len(nwsServer); got != 3 {
		t.Fatalf("non-stream all-empty web_search must not emit a group; server_tool_use=%d", got)
	}
}

// ---------------------------------------------------------------------------
// B-5 / B-6 full-path harness extras:
//   - control-plane helpers for the /api/option/ endpoints (RootAuth PAT);
//   - a secondary non-Codex (NewAPI, type 60) channel used for the non-Codex
//     negative probe and the cross-channel retry fallback;
//   - a test-only /v1/responses route reproducing the relay middleware chain so
//     the codex channel can be driven as a genuine direct-responses regression;
//   - a Claude SSE upstream writer for the native NewAPI passthrough.
// ---------------------------------------------------------------------------

// rootAccessToken is a fixed 32-char PAT seeded for the root admin user used to
// drive the RootAuth()-guarded /api/option/ control-plane endpoints.
const rootAccessToken = "0123456789abcdef0123456789abcdef"

// seedRootAdmin inserts a root admin row carrying a valid AccessToken so the
// /api/option/ PUT/GET endpoints can be exercised in full-path (A-09/A-10).
func (env *fullPathEnv) seedRootAdmin(t *testing.T) {
	t.Helper()
	pat := rootAccessToken
	if err := model.DB.Create(&model.User{
		Id:          2,
		Username:    "fullpath-root",
		Password:    "unused-password",
		Role:        common.RoleRootUser,
		Status:      common.UserStatusEnabled,
		Quota:       1000000000,
		Group:       "default",
		AuthVersion: 1,
		AffCode:     "root-aff-b5b6",
		AccessToken: &pat,
	}).Error; err != nil {
		t.Fatalf("seed root admin: %v", err)
	}
}

// registerOptionAdminRoutes adds just the RootAuth()-guarded /api/option/ GET/PUT
// admin routes that router.SetApiRouter would otherwise register. The relay
// router (SetRelayRouter) does not include them, but B-5's control-plane probe
// (A-09/A-10) drives the policy through these endpoints. The engine-level globals
// (CORS / BodyStorageCleanup / Stats) already apply from router.SetRelayRouter.
func registerOptionAdminRoutes(t *testing.T, env *fullPathEnv) {
	t.Helper()
	// updateOptionMap 依赖 common.OptionMap;真实应用由 model.InitOptionMap 在启动时填充,
	// 测试进程未走 main,故此处补齐(纯内存 map,无 DB/副作用)。
	model.InitOptionMap()
	opt := env.engine.Group("/api/option")
	opt.Use(middleware.RootAuth())
	opt.GET("/", controller.GetOptions)
	opt.PUT("/", controller.UpdateOption)
}

// putOption drives a real PUT /api/option/ with the root PAT. body is the
// OptionUpdateRequest JSON. It returns the HTTP status and parsed response JSON.
func (env *fullPathEnv) putOption(t *testing.T, body string) (int, gjson.Result) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/api/option/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+rootAccessToken)
	rec := httptest.NewRecorder()
	env.engine.ServeHTTP(rec, req)
	return rec.Code, gjson.Parse(rec.Body.String())
}

// getOptions drives a real GET /api/option/ with the root PAT.
func (env *fullPathEnv) getOptions(t *testing.T) (int, gjson.Result) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/option/", nil)
	req.Header.Set("Authorization", "Bearer "+rootAccessToken)
	rec := httptest.NewRecorder()
	env.engine.ServeHTTP(rec, req)
	return rec.Code, gjson.Parse(rec.Body.String())
}

// policyOptionByKey extracts the stored `value` string for a given option key
// from a GET /api/option/ `data` array. §8.1 A-09: data:Option[], array order is
// undefined, so the caller MUST look up by key (never by position). The target
// option must be UNIQUE in the array — a missing key or more than one match
// (duplicate rows) fails the test, so a lookup can never silently pick an
// ambiguous/single value and call it the persisted policy.
func policyOptionByKey(t *testing.T, body gjson.Result, key string) string {
	t.Helper()
	if !body.Get("success").Bool() {
		t.Fatalf("GET /api/option/ success=false: %s", body.Raw)
	}
	count := 0
	found := ""
	body.Get("data").ForEach(func(_, v gjson.Result) bool {
		if v.Get("key").String() == key {
			count++
			if count > 1 {
				return false // stop iterating: we already know it is ambiguous
			}
			found = v.Get("value").String()
		}
		return true
	})
	if count != 1 {
		t.Fatalf("GET /api/option/ matched %d option(s) for key %q, want exactly 1 (unique target option)", count, key)
	}
	return found
}

// assertPolicyObject verifies a parsed persisted policy parses to EXACTLY the
// expected full five-field ChatCompletionsToResponsesPolicy object (§8.1 A-10
// wire contract: all fields explicit). design §13.3 B-5 steps ③/⑦ read the
// persisted value back and must corroborate every one of the five fields —
// enabled / all_channels / channel_ids / channel_types / model_patterns — not
// just a single flag.
func assertPolicyObject(t *testing.T, p gjson.Result, enabled, allChannels bool, channelIDs, channelTypes []int, modelPatterns []string) {
	t.Helper()
	// Wire contract (§8.1 A-10) requires all five fields to be present
	// explicitly. gjson defaults would mask a missing false-valued key
	// (e.g. .Get("enabled").Bool() == false for an absent key) or a missing
	// empty array, so verify key presence before comparing values.
	t.Run("fields-explicit", func(t *testing.T) {
		for _, key := range []string{"enabled", "all_channels", "channel_ids", "channel_types", "model_patterns"} {
			if !p.Get(key).Exists() {
				t.Fatalf("policy field %q missing (all five must be explicit in persisted JSON)", key)
			}
		}
	})
	if got := p.Get("enabled").Bool(); got != enabled {
		t.Fatalf("policy enabled=%v, want %v", got, enabled)
	}
	if got := p.Get("all_channels").Bool(); got != allChannels {
		t.Fatalf("policy all_channels=%v, want %v", got, allChannels)
	}
	if got := gjsonInts(p.Get("channel_ids")); !reflect.DeepEqual(got, channelIDs) {
		t.Fatalf("policy channel_ids=%v, want %v", got, channelIDs)
	}
	if got := gjsonInts(p.Get("channel_types")); !reflect.DeepEqual(got, channelTypes) {
		t.Fatalf("policy channel_types=%v, want %v", got, channelTypes)
	}
	if got := gjsonStrings(p.Get("model_patterns")); !reflect.DeepEqual(got, modelPatterns) {
		t.Fatalf("policy model_patterns=%v, want %v", got, modelPatterns)
	}
}

// gjsonInts flattens a JSON number array into a []int (empty array -> []int{}).
func gjsonInts(arr gjson.Result) []int {
	out := make([]int, 0)
	arr.ForEach(func(_, v gjson.Result) bool {
		out = append(out, int(v.Int()))
		return true
	})
	return out
}

// gjsonStrings flattens a JSON string array into a []string (empty -> []string{}).
func gjsonStrings(arr gjson.Result) []string {
	out := make([]string, 0)
	arr.ForEach(func(_, v gjson.Result) bool {
		out = append(out, v.String())
		return true
	})
	return out
}

// seedNewApiChannel inserts a non-Codex (NewAPI, type 60) channel + abilities
// and refreshes the in-memory channel snapshot so the distributor can select it
// (model.InitChannelCache re-reads DB, so later seeds must re-init).
func (env *fullPathEnv) seedNewApiChannel(t *testing.T, id int, models string, prio int64) {
	t.Helper()
	baseURL := env.baseURL
	if err := model.DB.Create(&model.Channel{
		Id:          id,
		Type:        constant.ChannelTypeNewAPI,
		Name:        "newapi-fullpath",
		Key:         "sk-newapi",
		Status:      common.ChannelStatusEnabled,
		BaseURL:     &baseURL,
		Models:      models,
		Group:       "default",
		CreatedTime: common.GetTimestamp(),
		Priority:    ptrInt64(prio),
	}).Error; err != nil {
		t.Fatalf("seed newapi channel: %v", err)
	}
	prioPtr := ptrInt64(prio)
	for _, m := range strings.Split(models, ",") {
		if err := model.DB.Create(&model.Ability{
			Group: "default", Model: m, ChannelId: id, Enabled: true, Priority: prioPtr,
		}).Error; err != nil {
			t.Fatalf("seed newapi ability: %v", err)
		}
	}
	model.InitChannelCache()
}

// postRelay sends a real HTTP POST to an arbitrary relay route with the same
// auth headers as postMessages. It returns the full downstream response body.
func (env *fullPathEnv) postRelay(t *testing.T, path, body string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer sk-fullpath")
	req.Header.Set("x-api-key", "sk-fullpath")
	req.Header.Set("anthropic-version", "2023-06-01")
	env.rec = httptest.NewRecorder()
	env.engine.ServeHTTP(env.rec, req)
	return env.rec.Body.String()
}

// registerDirectResponsesRoute adds a test-only POST /v1/responses route onto
// the relay engine, reproducing the same middleware chain the /v1/messages route
// gets from router.SetRelayRouter. This lets the codex channel be driven as a
// genuine direct `/v1/responses` request (A-06 "直连") that shares the usage
// extraction path with the bridge, so B-6 can compare billing/usage fields.
func registerDirectResponsesRoute(t *testing.T, env *fullPathEnv) {
	t.Helper()
	g := env.engine.Group("/v1")
	g.Use(middleware.RouteTag("relay"))
	g.Use(middleware.SystemPerformanceCheck())
	g.Use(middleware.TokenAuth())
	g.Use(middleware.ModelRequestRateLimit())
	g.POST("/responses", middleware.Distribute(), func(c *gin.Context) {
		controller.Relay(c, types.RelayFormatOpenAIResponses)
	})
}

// claudeSSEStream writes a standard Anthropic Messages SSE stream (message_start
// -> content_block_start -> text delta -> stop -> message_delta -> message_stop)
// to the upstream mock response, used by the NewAPI (non-Codex) native channel.
func claudeSSEStream(w http.ResponseWriter, model, text string, in, out int) {
	w.Header().Set("Content-Type", "text/event-stream")
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
	_, _ = io.WriteString(w, "event: message_start\ndata: "+
		`{"type":"message_start","message":{"id":"msg_native","model":"`+model+`","role":"assistant","content":[],`+
		`"usage":{"input_tokens":`+strconv.Itoa(in)+`,"output_tokens":`+strconv.Itoa(out)+`}}}`+"\n\n")
	_, _ = io.WriteString(w, "event: content_block_start\ndata: "+`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`+"\n\n")
	_, _ = io.WriteString(w, "event: content_block_delta\ndata: "+`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"`+text+`"}}`+"\n\n")
	_, _ = io.WriteString(w, "event: content_block_stop\ndata: "+`{"type":"content_block_stop","index":0}`+"\n\n")
	_, _ = io.WriteString(w, "event: message_delta\ndata: "+`{"type":"message_delta","usage":{"output_tokens":`+strconv.Itoa(out)+`}}`+"\n\n")
	_, _ = io.WriteString(w, "event: message_stop\ndata: "+`{"type":"message_stop"}`+"\n\n")
}

// requestConversionHas reports whether the consume-log `request_conversion`
// array (a slice of human-readable format names) contains the given name.
func requestConversionHas(other gjson.Result, name string) bool {
	has := false
	other.Get("request_conversion").ForEach(func(_, v gjson.Result) bool {
		if v.String() == name {
			has = true
		}
		return true
	})
	return has
}

// claudeStreamRequestModel builds a stream=true /v1/messages request for a given
// model. claudeStreamRequest keeps the luna default; this helper generalizes it.
func claudeStreamRequestModel(model, body string) string {
	return fmt.Sprintf(`{
	  "model":%q,
	  "stream":true,
	  "max_tokens":1024,
	  "messages":[{"role":"user","content":%q}]
	}`, model, body)
}

// ---------------------------------------------------------------------------
// B-5: switch semantics — 配置写入→读取核对→生效;关闭=基线;非 Codex 负向;开启后再关闭。
// Probe sequence (design §13.3 B-5): ① initial-off baseline → ② PUT full 5-field
// recommended policy → ③ GET verify persisted value → ④ Codex positive probe
// (bridge live) → ⑤ non-Codex negative probe (not bridged) → ⑥ PUT off → ⑦ GET
// verify → ⑧ Codex channel returns to baseline.
// ---------------------------------------------------------------------------

func TestFullPath_B5_SwitchSemantics(t *testing.T) {
	env := newFullPathEnv(t)
	env.seedRootAdmin(t)
	registerOptionAdminRoutes(t, env)

	// full five-field policy objects (wire contract §8.1 A-10: all fields explicit).
	// 关闭态与推荐开启态各一;步骤⑥ 与初始前置条件复用同一关闭对象。
	policyKey := "global.chat_completions_to_responses_policy"
	recommendedEnabled := `{"enabled":true,"all_channels":false,"channel_ids":[],"channel_types":[57],"model_patterns":["^gpt-5\\.6-(luna|terra|sol)$"]}`
	disabled := `{"enabled":false,"all_channels":false,"channel_ids":[],"channel_types":[57],"model_patterns":["^gpt-5\\.6-(luna|terra|sol)$"]}`

	// 初始关闭态 fixture 前置条件(design §13.3 B-5 步①的前提):经 /api/option/ 控制面
	// 把 policy 持久化为“关闭”,再经 GET 按 key 唯一读回、逐字段核验为关闭。开关操作
	// (前置关闭 / 步骤② 开启 / 步骤⑥ 再关闭)全部经控制面完成,不绕过控制面直改运行态全局。
	// newFullPathEnv 默认把 policy 置为开启(供 B-1~B-4 等测试),此处用控制面写入的
	// enabled:false完整对象覆盖之——运行态生效由 UpdateOption→handleConfigUpdate 完成,
	// 与步骤②/⑥同一条真实生效链路。
	preCode, preResp := env.putOption(t, fmt.Sprintf(`{"key":%q,"value":%s}`, policyKey, strconv.Quote(disabled)))
	if preCode != http.StatusOK || !preResp.Get("success").Bool() {
		t.Fatalf("initial-off PUT option status=%d resp=%s", preCode, preResp.Raw)
	}
	preGcode, preGresp := env.getOptions(t)
	if preGcode != http.StatusOK {
		t.Fatalf("initial-off GET /api/option/ status=%d body=%s", preGcode, preGresp.Raw)
	}
	if stored := policyOptionByKey(t, preGresp, policyKey); !gjson.Valid(stored) {
		t.Fatalf("initial-off persisted policy is not a JSON document: %q", stored)
	} else {
		assertPolicyObject(t, gjson.Parse(stored), false, false, []int{}, []int{constant.ChannelTypeCodex}, []string{`^gpt-5\.6-(luna|terra|sol)$`})
	}

	// 负向探针用的非 Codex 渠道(NewAPI,type 60)服务 gpt-5.6-terra,走原生 claude 路径,
	// 与 codex 渠道共用同一 upstream mock,按请求路径分流。
	ratio_setting.UpdateModelRatioByJSONString(`{"gpt-5.6-luna":0,"gpt-5.6-terra":0}`)
	env.seedNewApiChannel(t, 2, "gpt-5.6-terra", 90)

	var codexHits, nativeHits int32
	var nativePath string
	env.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/backend-api/codex") {
			atomic.AddInt32(&codexHits, 1)
			sseEvents([]string{
				`{"type":"response.created","response":{"id":"resp_b5","model":"codex-luna"}}`,
				`{"type":"response.output_text.delta","delta":"bridge works","output_index":0}`,
				`{"type":"response.completed","response":{"id":"resp_b5","model":"codex-luna","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
			})(w, r)
			return
		}
		atomic.AddInt32(&nativeHits, 1)
		nativePath = r.URL.Path
		claudeSSEStream(w, "gpt-5.6-terra", "native ok", 3, 2)
	})

	// --- ① 初始关闭态:codex 渠道 /v1/messages → 原生 codex 路径,回归改动前基线错误形态(A-04)。
	body := env.postRelay(t, "/v1/messages", claudeStreamRequest("baseline off"))
	baseErr := assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if !strings.Contains(baseErr.Get("message").String(), "endpoint not supported") {
		t.Fatalf("baseline-off probe error message = %s, want codex endpoint-not-supported", baseErr.Get("message").String())
	}
	if codexHits != 0 {
		t.Fatalf("baseline-off must not reach the codex upstream (no bridge, native codex rejects messages); codexHits=%d", codexHits)
	}

	// --- ② 控制面 PUT:写入完整五字段推荐 policy(A-10,design ADR-2)。
	putBody := fmt.Sprintf(`{"key":%q,"value":%s}`, policyKey, strconv.Quote(recommendedEnabled))
	code, resp := env.putOption(t, putBody)
	if code != http.StatusOK || !resp.Get("success").Bool() {
		t.Fatalf("PUT option status=%d resp=%s", code, resp.Raw)
	}

	// --- ③ GET 核对持久化值(A-09):按 key 唯一查找(顺序未定义,匹配数必须恰为 1),
	// 解析为完整五字段对象逐项核对与写入一致。
	gcode, gresp := env.getOptions(t)
	if gcode != http.StatusOK {
		t.Fatalf("GET /api/option/ status=%d body=%s", gcode, gresp.Raw)
	}
	stored := policyOptionByKey(t, gresp, policyKey)
	if !gjson.Valid(stored) {
		t.Fatalf("persisted policy value is not a JSON document: %q", stored)
	}
	assertPolicyObject(t, gjson.Parse(stored), true, false, []int{}, []int{constant.ChannelTypeCodex}, []string{`^gpt-5\.6-(luna|terra|sol)$`})

	// --- ④ Codex 正向探针:policy 命中且渠道为 Codex → bridge 生效(A-01)。
	body = env.postRelay(t, "/v1/messages", claudeStreamRequest("bridge on"))
	if env.rec.Code != http.StatusOK {
		t.Fatalf("codex positive probe status=%d body=%s", env.rec.Code, body)
	}
	frames := parseClaudeSSE(t, body)
	if len(framesByType(frames, "message_start")) != 1 || len(framesByType(frames, "message_stop")) != 1 {
		t.Fatalf("codex positive probe did not emit a complete Claude SSE stream")
	}
	if got := joinedTextDeltas(frames); got != "bridge works" {
		t.Fatalf("codex positive probe text = %q, want %q", got, "bridge works")
	}
	if codexHits == 0 {
		t.Fatal("codex positive probe never reached the codex upstream (bridge not engaged)")
	}
	// bridge 生效在 consume log 留痕:request_conversion 含 OpenAI Responses,且 upstream_stream_forced 出现。
	other := env.lastConsumeLogOther(t)
	if !requestConversionHas(other, "OpenAI Responses") || !requestConversionHas(other, "Claude Messages") {
		t.Fatalf("bridge consume-log request_conversion = %s, want Claude Messages -> OpenAI Responses", other.Get("request_conversion").Raw)
	}
	if env.countDiag(t, "upstream_stream_forced") != 1 {
		t.Fatalf("bridge consume-log upstream_stream_forced count != 1")
	}

	// --- ⑤ 非 Codex 负向探针:非 Codex 渠道(NewAPI,gpt-5.6-terra)验证不 bridge(A-08 原生路径)。
	bodyTerra := env.postRelay(t, "/v1/messages", claudeStreamRequestModel("gpt-5.6-terra", "native probe"))
	if env.rec.Code != http.StatusOK {
		t.Fatalf("non-codex negative probe status=%d body=%s", env.rec.Code, bodyTerra)
	}
	if nativeHits == 0 {
		t.Fatal("non-codex negative probe never reached the native Claude upstream")
	}
	if !strings.Contains(nativePath, "/v1/messages") {
		t.Fatalf("non-codex negative probe upstream path = %q, want /v1/messages", nativePath)
	}
	other = env.lastConsumeLogOther(t)
	if env.countDiag(t, "upstream_stream_forced") != 0 {
		t.Fatal("non-codex negative probe must NOT emit upstream_stream_forced (no bridge)")
	}
	if env.countDiag(t, "downstream_stream") != 0 {
		t.Fatal("non-codex negative probe must NOT emit downstream_stream (no bridge)")
	}
	if requestConversionHas(other, "OpenAI Responses") {
		t.Fatalf("non-codex negative probe request_conversion = %s, must not contain OpenAI Responses", other.Get("request_conversion").Raw)
	}

	// --- ⑥ 状态转换:再次 PUT(enabled:false 完整对象)关闭(A-10)。
	putBody = fmt.Sprintf(`{"key":%q,"value":%s}`, policyKey, strconv.Quote(disabled))
	code, resp = env.putOption(t, putBody)
	if code != http.StatusOK || !resp.Get("success").Bool() {
		t.Fatalf("PUT-off option status=%d resp=%s", code, resp.Raw)
	}

	// --- ⑦ GET 核对关闭后的持久化值(A-09):按 key 唯一查找,复核关闭态完整五字段对象。
	gcode, gresp = env.getOptions(t)
	if gcode != http.StatusOK {
		t.Fatalf("GET /api/option/ (off) status=%d", gcode)
	}
	stored = policyOptionByKey(t, gresp, policyKey)
	if !gjson.Valid(stored) {
		t.Fatalf("persisted off policy not a JSON doc: %q", stored)
	}
	assertPolicyObject(t, gjson.Parse(stored), false, false, []int{}, []int{constant.ChannelTypeCodex}, []string{`^gpt-5\.6-(luna|terra|sol)$`})

	// --- ⑧ Codex 渠道回落基线行为(A-04):再次 POST /v1/messages → 原生 codex 错误形态,不再 bridge。
	body = env.postRelay(t, "/v1/messages", claudeStreamRequest("back to baseline"))
	offErr := assertClaudeErrorEnvelope(t, env.rec.Code, body)
	if !strings.Contains(offErr.Get("message").String(), "endpoint not supported") {
		t.Fatalf("turned-off probe error message = %s, want codex endpoint-not-supported", offErr.Get("message").String())
	}
	if env.countDiag(t, "upstream_stream_forced") != 0 {
		t.Fatal("turned-off probe must NOT bridge (no upstream_stream_forced in latest consume log)")
	}
}

// ---------------------------------------------------------------------------
// B-6: billing consistency — bridge vs direct final charge/usage fields agree;
// consume log carries request_conversion + the four diagnostic families + one
// English summary log line per bridge request (same request id); plus the
// FR-001 cross-channel retry fallback (bridge fails retriable -> controller
// switches to a non-Codex native channel and completes final differential settle).
// ---------------------------------------------------------------------------

func TestFullPath_B6_BillingConsistency(t *testing.T) {
	env := newFullPathEnv(t)
	// newFullPathEnv 将 gpt-5.6-luna 配为免费模型(ratio 0 + 免预扣),让请求走完整计费
	// 链路又不触发订阅/分档计费的 DB 表依赖;此处断言 bridge 与直连的 usage 计费输入 token
	// 与最终 Quota 完全一致(FR-011:bridge 与直连同一条 usage 提取路径,不重复计费、不漂移)。
	registerDirectResponsesRoute(t, env)

	// fixture 含一个提前关闭(文本边界先于 reasoning done)的 reasoning item,驱使
	// signature_dropped_after_early_close 诊断留痕;usage 取 response.completed 的 41/17。
	fixture := []string{
		`{"type":"response.created","response":{"id":"resp_b6","model":"codex-luna"}}`,
		`{"type":"response.output_item.added","item":{"type":"reasoning","id":"r_ec"}}`,
		`{"type":"response.reasoning_summary_part.added","item_id":"r_ec"}`,
		`{"type":"response.reasoning_summary_text.delta","item_id":"r_ec","delta":"thinking ec"}`,
		`{"type":"response.reasoning_summary_part.done","item_id":"r_ec"}`,
		`{"type":"response.output_text.delta","delta":"billing ok","output_index":0}`,
		`{"type":"response.output_item.done","item":{"type":"reasoning","id":"r_ec","encrypted_content":"sig_ec"}}`,
		`{"type":"response.completed","response":{"id":"resp_b6","model":"codex-luna","usage":{"input_tokens":41,"output_tokens":17,"total_tokens":58}}}`,
	}
	const wantIn, wantOut = 41, 17

	// --- 每 bridge 请求一条英文摘要日志(同 request ID):捕获 writer 观察 LogDebug 输出。
	common.DebugEnabled = true
	logBuf := &bytes.Buffer{}
	gin.DefaultErrorWriter = logBuf
	restoreLogger := func() {
		common.DebugEnabled = false
		gin.DefaultErrorWriter = gin.DefaultWriter // 仅归还指针,不重设 DefaultWriter 本身
	}
	t.Cleanup(restoreLogger)

	// --- (a) bridge 请求:POST /v1/messages(codex 渠道)经 bridge 全链。
	env.upstream.set(sseEvents(fixture))
	body := env.postRelay(t, "/v1/messages", claudeStreamRequest("billing bridge"))
	if env.rec.Code != http.StatusOK {
		t.Fatalf("bridge request status=%d body=%s", env.rec.Code, body)
	}
	frames := parseClaudeSSE(t, body)
	if got := joinedTextDeltas(frames); got != "billing ok" {
		t.Fatalf("bridge text = %q, want %q", got, "billing ok")
	}
	bridgeLog := env.lastConsumeLog(t)
	bridgeOther := gjson.Parse(bridgeLog.Other)

	// 断言 consume log 的 request_conversion 与四类诊断。
	if !requestConversionHas(bridgeOther, "Claude Messages") || !requestConversionHas(bridgeOther, "OpenAI Responses") {
		t.Fatalf("bridge request_conversion = %s", bridgeOther.Get("request_conversion").Raw)
	}
	if env.countDiag(t, "upstream_stream_forced") != 1 {
		t.Fatalf("missing upstream_stream_forced diagnostic")
	}
	if env.countDiag(t, "downstream_stream") != 1 {
		t.Fatalf("missing downstream_stream diagnostic")
	}
	// 收缩计数诊断(request_field_shrink)即便本测试无超 64 字节名也会记录(shrunk 0 …)。
	if env.countDiag(t, "request_field_shrink") != 1 {
		t.Fatalf("missing request_field_shrink (shrink-count) diagnostic")
	}
	// signature_dropped_after_early_close:fixture 的 reasoning done 迟到(文本边界先关块)
	// → 恰好 1 条丢签名诊断(四类之一,承载通道为 conversion_diagnostics 的 warning)。
	if env.countDiag(t, "signature_dropped_after_early_close") != 1 {
		t.Fatalf("missing signature_dropped_after_early_close diagnostic (expected exactly 1 dropped item)")
	}

	// (a2) 每 bridge 请求一条英文摘要日志(同 request ID):恰好一条,内容为英文明细
	// (model + stream),且摘要中的 request id 与同一 HTTP 请求的期望 request id 逐字一致。
	// RequestId 中间件把 id 同时写入响应头 X-Oneapi-Request-Id 与 gin Keys;bridge 摘要
	// 经 helper.GetResponseID 取 gin Keys 渲染为 `id=chatcmpl-<id>`。因此从 bridge 请求的
	// 响应头读出请求 id,即可逐字断言摘要携带了该请求的 request id(NFR-004“同 request ID”，
	// 不以“每请求一行”替代)。
	summaryContent := logBuf.String()
	if got := strings.Count(summaryContent, "bridge claude messages -> openai responses"); got != 1 {
		t.Fatalf("bridge English summary log lines = %d, want exactly 1 per bridge request", got)
	}
	if !strings.Contains(summaryContent, "gpt-5.6-luna") || !strings.Contains(summaryContent, "stream=true") {
		t.Fatalf("bridge summary log is not the expected one-line English summary: %s", summaryContent)
	}
	// 请求 id 取自同一 HTTP 请求(bridge 请求后读取响应头);摘要必须逐字携带该 id。
	reqID := env.rec.Header().Get(string(common.RequestIdKey))
	if reqID == "" {
		t.Fatal("expected X-Oneapi-Request-Id response header on the bridge request, got empty")
	}
	if !strings.Contains(summaryContent, "id=chatcmpl-"+reqID) {
		t.Fatalf("bridge summary log does not carry the request's id (want `id=chatcmpl-%s`): %s", reqID, summaryContent)
	}

	// --- (b) 直连 /v1/responses:同一 fixture 下同 usage 提取路径(共享 UsageFromResponsesUsage)。
	env.upstream.set(sseEvents(fixture))
	directBody := env.postRelay(t, "/v1/responses", `{"model":"gpt-5.6-luna","input":"billing direct","stream":true}`)
	if env.rec.Code != http.StatusOK {
		t.Fatalf("direct /v1/responses status=%d body=%s", env.rec.Code, directBody)
	}
	directLog := env.lastConsumeLog(t)

	// 同 fixture 下 bridge 与直连最终扣费/usage 字段一致(仅 request_conversion 等诊断字段可不同)。
	if bridgeLog.PromptTokens != wantIn || directLog.PromptTokens != wantIn {
		t.Fatalf("PromptTokens bridge=%d direct=%d, want both=%d", bridgeLog.PromptTokens, directLog.PromptTokens, wantIn)
	}
	if bridgeLog.CompletionTokens != wantOut || directLog.CompletionTokens != wantOut {
		t.Fatalf("CompletionTokens bridge=%d direct=%d, want both=%d", bridgeLog.CompletionTokens, directLog.CompletionTokens, wantOut)
	}
	if bridgeLog.Quota != directLog.Quota {
		t.Fatalf("final Quota mismatch: bridge=%d direct=%d, want equal (same fixture/usage)", bridgeLog.Quota, directLog.Quota)
	}

	// ---------------------------------------------------------------------------
	// (c) FR-001 跨渠道重试降级:bridge(codex)尝试返回可重试错误 (nil,err) → controller
	// 重试切到非 Codex(newapi)原生渠道成功,完成最终差额结算(跨渠道重试路径)。
	// ---------------------------------------------------------------------------
	env2 := newFullPathEnv(t)
	common.RetryTimes = 1 // 允许一次 controller 重试;cleanup 恢复。
	// codex 渠道(prio 100,newFullPathEnv 种子)+ 非 Codex newapi 渠道(prio 50)同服务 gpt-5.6-luna。
	env2.seedNewApiChannel(t, 2, "gpt-5.6-luna", 50)
	// 渠道选择:retry 0 → codex(prio 100 档),retry 1 → newapi(prio 50 档)。
	var bridgeAttempts int32
	env2.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/backend-api/codex") {
			atomic.AddInt32(&bridgeAttempts, 1)
			// bridge 尝试返回可重试的 5xx(pre-commit,nil,err → controller 重试)。
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"error":{"type":"server_error","message":"transient upstream"}}`)
			return
		}
		// newapi 原生 /v1/messages 成功。
		claudeSSEStream(w, "gpt-5.6-luna", "retry native ok", 7, 5)
	})
	body2 := env2.postRelay(t, "/v1/messages", claudeStreamRequest("retry fallback"))
	if env2.rec.Code != http.StatusOK {
		t.Fatalf("cross-channel retry final status=%d body=%s", env2.rec.Code, body2)
	}
	frames2 := parseClaudeSSE(t, body2)
	if got := joinedTextDeltas(frames2); got != "retry native ok" {
		t.Fatalf("cross-channel retry final text = %q, want %q", got, "retry native ok")
	}
	if bridgeAttempts == 0 {
		t.Fatal("cross-channel retry: bridge (codex) attempt never happened")
	}
	retryLog := env2.lastConsumeLog(t)
	// 跨渠道重试留痕:consume-log 记录 used channel [codex,newapi] = ["1","2"]。
	if got := gjson.Parse(retryLog.Other).Get("admin_info.use_channel").String(); got != `["1","2"]` {
		t.Fatalf("cross-channel retry use_channel = %s, want [\"1\",\"2\"]", got)
	}
	// 最终差额结算落在 newapi 原生渠道:usage 来自 native 成功响应。
	if retryLog.PromptTokens != 7 || retryLog.CompletionTokens != 5 {
		t.Fatalf("cross-channel retry final tokens = %d/%d, want 7/5", retryLog.PromptTokens, retryLog.CompletionTokens)
	}
	// 最终成功路径是跨渠道切到非 Codex(newapi)原生,并非 bridge:consume-log 的
	// request_conversion 只能是原生 Claude Messages、不得含 OpenAI Responses。注意
	// attempt 1 的 codex bridge 诊断(upstream_stream_forced 等)会残留在共享 relayInfo
	// 上并随最终日志一并写出,故这里只断言“成功路径非 bridge”,不断言诊断为 0。
	retryOther := gjson.Parse(retryLog.Other)
	if requestConversionHas(retryOther, "OpenAI Responses") || !requestConversionHas(retryOther, "Claude Messages") {
		t.Fatalf("cross-channel retry final request_conversion = %s, want native Claude Messages only", retryOther.Get("request_conversion").Raw)
	}
}

// ---------------------------------------------------------------------------
// B-7: 三类回归 — default baseline (A-06/07/08), response-side additive
// (ADR-7) via non-Codex via-responses, request-side additive (D-3 thinking
// replay), and strict-upstream negative (R-008 canonical error propagation).
//
// design §13.3 B-7:缺省场景(不注入 encrypted_content/web_search_call、历史不含
// 带 signature 的 thinking block、不声明 web_search 工具定义)状态码与响应格式与
// 基线一致;响应侧增补断言注入 encrypted_content → 新增 signature_delta/thinking,
// 声明 web_search 定义后注入 web_search_call → 新增 server_tool_use+web_search_tool_result;
// 请求侧增补断言历史含 signature thinking → 上行 reasoning replay item,
// redacted_thinking 不产出、负向 fixture 上行与基线一致;严格负向(mock 4xx)不吞错。
// 这四个函数全部落在本 full-path 文件,经真实注册路由发 HTTP。
// ---------------------------------------------------------------------------

// newB7ViaResponsesEnv seeds a non-Codex (NewAPI, type 60) channel serving
// gpt-5.6-terra alongside the default codex channel, and widens the policy to
// AllChannels + a gpt-5.6-* pattern so the NewAPI channel ALSO matches the
// via-responses branch (A-08 #3) for /v1/messages. This drives the SHARED
// relaykit converters (Claude<->Responses) on a non-Codex via-responses path,
// independently of the Codex bridge.
func newB7ViaResponsesEnv(t *testing.T) *fullPathEnv {
	t.Helper()
	env := newFullPathEnv(t)
	model_setting.GetGlobalSettings().ChatCompletionsToResponsesPolicy = model_setting.ChatCompletionsToResponsesPolicy{
		Enabled:       true,
		AllChannels:   true,
		ModelPatterns: []string{`gpt-5\.6.*`},
	}
	if err := ratio_setting.UpdateModelRatioByJSONString(`{"gpt-5.6-terra":0}`); err != nil {
		t.Fatalf("set terra ratio: %v", err)
	}
	// 仅 newapi 渠道服务 gpt-5.6-terra -> distributor 路由确定性:该模型恒走非 Codex 渠道。
	env.seedNewApiChannel(t, 2, "gpt-5.6-terra", 200)
	return env
}

// claudeNonStreamRequestModel builds a stream:false /v1/messages request for an
// arbitrary model (claudeNonStreamRequest keeps a gpt-5.6-luna default).
func claudeNonStreamRequestModel(model, body string) string {
	return fmt.Sprintf(`{
	  "model":%q,
	  "stream":false,
	  "max_tokens":1024,
	  "messages":[{"role":"user","content":%q}]
	}`, model, body)
}

// reasoningReplay checks whether the captured upstream `input` array contains a
// reasoning replay item whose encrypted_content equals sig, matching BOTH the
// D-3 exact key set and exact values: the item's JSON object keys, sorted, must
// equal exactly {type, summary, content, encrypted_content} (no extra/missing
// keys), and summary must serialize to `[]` (empty array) while content is null.
func reasoningReplay(inputRaw, sig string) (bool, string) {
	if inputRaw == "" {
		return false, ""
	}
	var found bool
	var detail string
	gjson.Parse(inputRaw).ForEach(func(_, it gjson.Result) bool {
		if found {
			return false
		}
		if it.Get("type").String() != "reasoning" {
			return true
		}
		// 精确键集合校验:对匹配 item 的 JSON object keys 排序后与 D-3 四键集合
		// {type, summary, content, encrypted_content} 精确相等,不允许多/缺键。
		var keys []string
		it.ForEach(func(k, _ gjson.Result) bool {
			keys = append(keys, k.String())
			return true
		})
		slices.Sort(keys)
		wantKeys := []string{"content", "encrypted_content", "summary", "type"}
		exactKeys := slices.Equal(keys, wantKeys)
		// 值断言:encrypted_content = sig;summary 恒为 `[]`(空数组);content 恒为 null。
		exactVals := it.Get("encrypted_content").String() == sig &&
			it.Get("summary").Get("#").Int() == 0 &&
			it.Get("summary").Raw == "[]" &&
			it.Get("content").Exists() &&
			it.Get("content").Type == gjson.Null
		found = exactKeys && exactVals
		if !found {
			detail = it.Raw
		}
		return !found
	})
	return found, detail
}

// countReasoningItems counts `type=reasoning` items in the captured input array.
func countReasoningItems(inputRaw string) int {
	n := 0
	if inputRaw != "" {
		gjson.Parse(inputRaw).ForEach(func(_, it gjson.Result) bool {
			if it.Get("type").String() == "reasoning" {
				n++
			}
			return true
		})
	}
	return n
}

// TestRegression_B7_DefaultBaseline locks the default (no-trigger) regression
// across the three endpoint classes (A-06 direct /v1/responses, A-07
// /v1/chat/completions, A-08 non-Codex /v1/messages): without any injected
// trigger info the status code and response format (Claude SSE / chat SSE /
// responses SSE) match the pre-change baseline.
func TestRegression_B7_DefaultBaseline(t *testing.T) {
	t.Run("a06-direct-responses", func(t *testing.T) {
		env := newFullPathEnv(t)
		registerDirectResponsesRoute(t, env)
		directEvents := []string{
			`{"type":"response.created","response":{"id":"b7a06","model":"gpt-5.6-luna"}}`,
			`{"type":"response.output_text.delta","delta":"direct baseline","output_index":0}`,
			`{"type":"response.completed","response":{"id":"b7a06","model":"gpt-5.6-luna","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		}
		env.upstream.set(sseEvents(directEvents))
		body := env.postRelay(t, "/v1/responses", `{"model":"gpt-5.6-luna","input":"direct","stream":true}`)
		if env.rec.Code != http.StatusOK {
			t.Fatalf("direct /v1/responses status=%d body=%s", env.rec.Code, body)
		}
		// 缺省场景直连 /v1/responses 保持基线 passthrough:下游为与上游逐帧一致的
		// Responses SSE(`event: <type>` + `data: <payload>`,事件类型与载荷与直连
		// responses 基线逐帧比对),不得被 Claude Messages 事件帧污染。
		wantBody, wantFrames := responsesSSEFrames(directEvents...)
		// 逐帧精确原始字节帧断言:整条 body 必须逐字节等于基线期望——每个
		// `event: <type>` 行、`data: <payload>` 行、帧间 `\n\n` 分隔符、以及全部
		// 载荷字段逐一钉死,任何帧序/线形/载荷/分隔符漂移都会在此暴露。
		if body != wantBody {
			t.Fatalf("direct /v1/responses SSE body not byte-identical to baseline:\n  want=%q\n  got =%q", wantBody, body)
		}
		frames := parseClaudeSSE(t, body)
		if len(frames) != len(wantFrames) {
			t.Fatalf("direct /v1/responses must emit exactly %d Responses SSE frames, got %d: %s", len(wantFrames), len(frames), body)
		}
		wantTypes := []string{"response.created", "response.output_text.delta", "response.completed"}
		for i, wt := range wantTypes {
			if frames[i].Etype != wt {
				t.Fatalf("direct responses frame[%d] event=%q want %q; body=%s", i, frames[i].Etype, wt, body)
			}
			// 精确帧原始字节:每帧必须逐字节等于 `event: <type>\ndata: <payload>`(帧末
			// `\n\n` 分隔符已由整条 body 的逐字节重建比对覆盖)。
			if frames[i].Full != strings.TrimSuffix(wantFrames[i], "\n\n") {
				t.Fatalf("direct responses frame[%d] raw bytes drifted:\n  want=%q\n  got =%q", i, wantFrames[i], frames[i].Full)
			}
		}
		// 逐帧载荷与直连 Responses 基线比对。
		if frames[0].Data.Get("type").String() != "response.created" || frames[0].Data.Get("response.id").String() != "b7a06" || frames[0].Data.Get("response.model").String() != "gpt-5.6-luna" {
			t.Fatalf("response.created payload drifted: %s", frames[0].Data.Raw)
		}
		if frames[1].Data.Get("delta").String() != "direct baseline" || frames[1].Data.Get("output_index").Int() != 0 {
			t.Fatalf("response.output_text.delta payload drifted: %s", frames[1].Data.Raw)
		}
		if frames[2].Data.Get("response.id").String() != "b7a06" || frames[2].Data.Get("response.usage.total_tokens").Int() != 5 {
			t.Fatalf("response.completed payload drifted: %s", frames[2].Data.Raw)
		}
	})

	t.Run("a07-chat-completions", func(t *testing.T) {
		env := newB7ViaResponsesEnv(t)
		env.upstream.set(sseEvents([]string{
			`{"type":"response.created","response":{"id":"b7a07","model":"gpt-5.6-terra"}}`,
			`{"type":"response.output_text.delta","delta":"chat baseline","output_index":0}`,
			`{"type":"response.completed","response":{"id":"b7a07","model":"gpt-5.6-terra","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		}))
		body := env.postRelay(t, "/v1/chat/completions", `{"model":"gpt-5.6-terra","messages":[{"role":"user","content":"hi"}],"stream":true}`)
		if env.rec.Code != http.StatusOK {
			t.Fatalf("chat/completions status=%d body=%s", env.rec.Code, body)
		}
		// 缺省 chat/completions 保持基线 OpenAI chat SSE(`data: {chat.completion.chunk}` 行,
		// 结尾 `data: [DONE]`,不是 Claude `event:` 帧)。逐帧与基线 OpenAI chat SSE 精确比对:
		// 保留每个完整原始帧(`data: <payload>` 单行),校验帧数、帧间 `\n\n` 分隔符、
		// 整条 body 线形与全部基线载荷字段。
		raw := parseChatSSE(t, body)
		if len(raw) != 5 {
			t.Fatalf("chat/completions must emit 5 chunks (role/content/finish/usage/DONE), got %d: %s", len(raw), body)
		}
		// 整条 body 必须恰为 5 个原始 `data:` 帧以 `\n\n` 连接(逐字节重建比对,
		// 任何多余行 / 缺分隔 / 尾随字节漂移都会暴露)。
		if want := strings.Join(raw, "\n\n") + "\n\n"; want != body {
			t.Fatalf("chat SSE body not exactly `data:` frames joined by \\n\\n (raw lineage drifted):\n  want=%q\n  got =%q", want, body)
		}
		// 终帧必须逐字为 `data: [DONE]`。
		if raw[4] != "data: [DONE]" {
			t.Fatalf("chat/completions must terminate with data: [DONE], got %q", raw[4])
		}
		// 与完整基线 payload 逐帧精确比较:每个 chunk 的原始 data payload 都必须与
		// 预期基线 JSON 对象全字段一致(而非仅部分字段),任何额外/缺失字段、任意
		// 嵌套层的值漂移都会失败。基线模板仅对两次运行之间天然变化的 `id`/`created`
		// 两个顶层字段占位(它们另行校验 present 且合法),其余字段逐一钉死。
		chatFramePayloadEqual(t, raw[0],
			`{"id":@id@,"object":"chat.completion.chunk","created":@created@,"model":"gpt-5.6-terra","system_fingerprint":null,`+
				`"choices":[{"delta":{"content":"","role":"assistant"},"logprobs":null,"finish_reason":null,"index":0}],"usage":null}`)
		chatFramePayloadEqual(t, raw[1],
			`{"id":@id@,"object":"chat.completion.chunk","created":@created@,"model":"gpt-5.6-terra","system_fingerprint":null,`+
				`"choices":[{"delta":{"content":"chat baseline"},"logprobs":null,"finish_reason":null,"index":0}],"usage":null}`)
		chatFramePayloadEqual(t, raw[2],
			`{"id":@id@,"object":"chat.completion.chunk","created":@created@,"model":"gpt-5.6-terra","system_fingerprint":null,`+
				`"choices":[{"delta":{},"logprobs":null,"finish_reason":"stop","index":0}],"usage":null}`)
		chatFramePayloadEqual(t, raw[3],
			`{"id":@id@,"object":"chat.completion.chunk","created":@created@,"model":"gpt-5.6-terra","system_fingerprint":null,"choices":[],`+
				`"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,`+
				`"billing_usage":{"source":"oai_responses","semantic":"openai",`+
				`"openai_usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":5,`+
				`"prompt_tokens_details":{"cached_tokens":0,"text_tokens":0,"audio_tokens":0,"image_tokens":0},`+
				`"completion_tokens_details":{"text_tokens":0,"audio_tokens":0,"image_tokens":0,"reasoning_tokens":0},`+
				`"input_tokens":3,"output_tokens":2,"input_tokens_details":null,"claude_cache_creation_5_m_tokens":0,"claude_cache_creation_1_h_tokens":0}},`+
				`"prompt_tokens_details":{"cached_tokens":0,"text_tokens":0,"audio_tokens":0,"image_tokens":0},`+
				`"completion_tokens_details":{"text_tokens":0,"audio_tokens":0,"image_tokens":0,"reasoning_tokens":0},`+
				`"input_tokens":3,"output_tokens":2,"input_tokens_details":null,"claude_cache_creation_5_m_tokens":0,"claude_cache_creation_1_h_tokens":0}}`)
	})

	t.Run("a08-non-codex-messages", func(t *testing.T) {
		env := newB7ViaResponsesEnv(t)
		env.upstream.set(sseEvents([]string{
			`{"type":"response.created","response":{"id":"b7a08","model":"gpt-5.6-terra"}}`,
			`{"type":"response.output_text.delta","delta":"messages baseline","output_index":0}`,
			`{"type":"response.completed","response":{"id":"b7a08","model":"gpt-5.6-terra","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		}))
		body := env.postRelay(t, "/v1/messages", claudeStreamRequestModel("gpt-5.6-terra", "msgs baseline"))
		if env.rec.Code != http.StatusOK {
			t.Fatalf("non-codex /v1/messages status=%d body=%s", env.rec.Code, body)
		}
		frames := parseClaudeSSE(t, body)
		assertSSEStateMachineINV(t, frames)
		if got := joinedTextDeltas(frames); got != "messages baseline" {
			t.Fatalf("non-codex messages text = %q, want %q", got, "messages baseline")
		}
		// 缺省场景:恰一个 text block、无 thinking、无 signature_delta、无 web_search 块。
		starts := framesByType(frames, "content_block_start")
		if len(starts) != 1 || starts[0].Data.Get("content_block.type").String() != "text" {
			t.Fatalf("default non-codex messages must emit exactly one text block, got %s", body)
		}
		if len(signatureDeltas(frames)) != 0 {
			t.Fatalf("default messages must not emit signature_delta, got %v", signatureDeltas(frames))
		}
		if len(webSearchServerIDs(frames)) != 0 || len(webSearchToolResultIDs(frames)) != 0 {
			t.Fatalf("default messages must not emit web_search blocks")
		}
	})
}

// TestRegression_B7_ResponseAdditive locks the ADR-7 response-side additive
// behavior on the shared response converter via a NON-Codex via-responses
// (A-08 #3) path, for both stream=true and stream=false:
//   - inject reasoning item with encrypted_content -> signature_delta / thinking
//     block appears downstream;
//   - declare a web_search tool definition and inject web_search_call ->
//     server_tool_use + web_search_tool_result appear downstream.
func TestRegression_B7_ResponseAdditive(t *testing.T) {
	env := newB7ViaResponsesEnv(t)
	const sigReplay = "sig_additive_reasoning"

	t.Run("stream-reasoning", func(t *testing.T) {
		env.upstream.set(sseEvents([]string{
			`{"type":"response.created","response":{"id":"b7adds","model":"gpt-5.6-terra"}}`,
			`{"type":"response.output_item.added","item":{"type":"reasoning","id":"ra"}}`,
			`{"type":"response.output_item.done","item":{"type":"reasoning","id":"ra","encrypted_content":"` + sigReplay + `"}}`,
			`{"type":"response.output_text.delta","delta":"additive stream text","output_index":0}`,
			`{"type":"response.completed","response":{"id":"b7adds","model":"gpt-5.6-terra","usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8}}}`,
		}))
		body := env.postRelay(t, "/v1/messages", claudeStreamRequestModel("gpt-5.6-terra", "additive stream"))
		if env.rec.Code != http.StatusOK {
			t.Fatalf("stream additive status=%d body=%s", env.rec.Code, body)
		}
		frames := parseClaudeSSE(t, body)
		if sigs := signatureDeltas(frames); len(sigs) != 1 || sigs[0] != sigReplay {
			t.Fatalf("stream additive signature_delta = %v, want [%s]", sigs, sigReplay)
		}
		sawThinking := false
		for _, f := range framesByType(frames, "content_block_start") {
			if f.Data.Get("content_block.type").String() == "thinking" {
				sawThinking = true
			}
		}
		if !sawThinking {
			t.Fatal("stream additive must add a thinking block for injected encrypted_content")
		}
	})

	t.Run("nonstream-reasoning", func(t *testing.T) {
		env.upstream.set(sseEvents([]string{
			`{"type":"response.created","response":{"id":"b7addn","model":"gpt-5.6-terra"}}`,
			`{"type":"response.completed","response":{"id":"b7addn","model":"gpt-5.6-terra","usage":{"input_tokens":5,"output_tokens":3,"total_tokens":8},"output":[` +
				`{"type":"reasoning","id":"ran","encrypted_content":"` + sigReplay + `"},` +
				`{"type":"message","id":"m1","role":"assistant","content":[{"type":"output_text","text":"additive nonstream text"}]}` +
				`]}}`,
		}))
		body := env.postRelay(t, "/v1/messages", claudeNonStreamRequestModel("gpt-5.6-terra", "additive nonstream"))
		if env.rec.Code != http.StatusOK {
			t.Fatalf("nonstream additive status=%d body=%s", env.rec.Code, body)
		}
		if !gjson.Valid(body) || strings.Contains(body, "event: ") {
			t.Fatalf("nonstream additive must be a single JSON Claude message: %s", body)
		}
		sawThinkingSig := false
		gjson.Parse(body).Get("content").ForEach(func(_, cb gjson.Result) bool {
			if cb.Get("type").String() == "thinking" && cb.Get("signature").String() == sigReplay {
				sawThinkingSig = true
			}
			return true
		})
		if !sawThinkingSig {
			t.Fatalf("nonstream additive must carry thinking block with signature %q: %s", sigReplay, body)
		}
	})

	t.Run("stream-websearch", func(t *testing.T) {
		wsReq := `{
		  "model":"gpt-5.6-terra",
		  "stream":true,
		  "max_tokens":1024,
		  "tools":[{"type":"web_search_20250305","name":"web_search","max_uses":1}],
		  "messages":[{"role":"user","content":"search something"}]
		}`
		env.upstream.set(sseEvents([]string{
			`{"type":"response.created","response":{"id":"b7addws","model":"gpt-5.6-terra"}}`,
			`{"type":"response.output_item.done","item":{"type":"web_search_call","id":"wsb7s","action":{"query":"go.dev"},"results":[{"url":"https://go.dev","title":"Go"}]}}`,
			`{"type":"response.output_text.delta","delta":"ws text","output_index":0}`,
			`{"type":"response.completed","response":{"id":"b7addws","model":"gpt-5.6-terra","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		}))
		body := env.postRelay(t, "/v1/messages", wsReq)
		if env.rec.Code != http.StatusOK {
			t.Fatalf("stream web_search additive status=%d body=%s", env.rec.Code, body)
		}
		frames := parseClaudeSSE(t, body)
		if got := webSearchServerIDs(frames); !reflect.DeepEqual(got, []string{"wsb7s"}) {
			t.Fatalf("stream web_search server_tool_use ids = %v, want [wsb7s]", got)
		}
		if got := webSearchToolResultIDs(frames); !reflect.DeepEqual(got, []string{"wsb7s"}) {
			t.Fatalf("stream web_search web_search_tool_result ids = %v, want [wsb7s]", got)
		}
	})

	t.Run("nonstream-websearch", func(t *testing.T) {
		wsReq := `{
		  "model":"gpt-5.6-terra",
		  "stream":false,
		  "max_tokens":1024,
		  "tools":[{"type":"web_search_20260209","name":"web_search","max_uses":1}],
		  "messages":[{"role":"user","content":"search nonstream"}]
		}`
		env.upstream.set(sseEvents([]string{
			`{"type":"response.created","response":{"id":"b7addwsn","model":"gpt-5.6-terra"}}`,
			`{"type":"response.completed","response":{"id":"b7addwsn","model":"gpt-5.6-terra","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5},"output":[` +
				`{"type":"web_search_call","id":"wsb7n","action":{"query":"golang"},"results":[{"url":"https://golang.org","title":"Golang"}]},` +
				`{"type":"message","id":"m","role":"assistant","content":[{"type":"output_text","text":"ws nonstream text"}]}` +
				`]}}`,
		}))
		body := env.postRelay(t, "/v1/messages", wsReq)
		if env.rec.Code != http.StatusOK {
			t.Fatalf("nonstream web_search additive status=%d body=%s", env.rec.Code, body)
		}
		if !gjson.Valid(body) || strings.Contains(body, "event: ") {
			t.Fatalf("nonstream web_search additive must be a single JSON message: %s", body)
		}
		var serverID, resultID string
		gjson.Parse(body).Get("content").ForEach(func(_, cb gjson.Result) bool {
			switch cb.Get("type").String() {
			case "server_tool_use":
				serverID = cb.Get("id").String()
			case "web_search_tool_result":
				resultID = cb.Get("tool_use_id").String()
			}
			return true
		})
		if serverID != "wsb7n" || resultID != "wsb7n" {
			t.Fatalf("nonstream web_search server=%q result=%q, want both [wsb7n]", serverID, resultID)
		}
	})
}

// TestRegression_B7_RequestAdditive locks the D-3 request-side additive behavior
// on the shared Claude->Responses request converter via a NON-Codex via-responses
// (A-08 #3) path: history with a signature non-empty thinking block produces the
// original-string reasoning replay item upstream; redacted_thinking produces NO
// replay item; a negative fixture (history with no signature thinking block)
// keeps the upstream request at the baseline (no reasoning item injected).
func TestRegression_B7_RequestAdditive(t *testing.T) {
	env := newB7ViaResponsesEnv(t)
	const sigReplay = "sig-orig-request-replay"

	var inputRaw string
	var fullBodyRaw string
	serve := func(events []string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			reqBody, _ := io.ReadAll(r.Body)
			inputRaw = gjson.ParseBytes(reqBody).Get("input").Raw
			fullBodyRaw = string(reqBody)
			sseEvents(events)(w, r)
		}
	}
	events := []string{
		`{"type":"response.created","response":{"id":"b7req","model":"gpt-5.6-terra"}}`,
		`{"type":"response.output_text.delta","delta":"ok","output_index":0}`,
		`{"type":"response.completed","response":{"id":"b7req","model":"gpt-5.6-terra","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
	}

	// 正向:历史含 signature 非空 thinking block -> 上行产生原字符串 reasoning replay item。
	t.Run("thinking-replay", func(t *testing.T) {
		env.upstream.set(serve(events))
		req := fmt.Sprintf(`{
		  "model":"gpt-5.6-terra","stream":true,"max_tokens":1024,
		  "messages":[
		    {"role":"user","content":"question"},
		    {"role":"assistant","content":[{"type":"thinking","thinking":"hmm","signature":%q},{"type":"text","text":"answer"}]},
		    {"role":"user","content":"continue"}
		  ]}`, sigReplay)
		body := env.postRelay(t, "/v1/messages", req)
		if env.rec.Code != http.StatusOK {
			t.Fatalf("thinking-replay status=%d body=%s", env.rec.Code, body)
		}
		ok, detail := reasoningReplay(inputRaw, sigReplay)
		if !ok {
			t.Fatalf("upstream input missing original-string reasoning replay for %q; input=%s detail=%s", sigReplay, inputRaw, detail)
		}
	})

	// 负向:历史不含 signature thinking block -> 上行请求体与零 additive 基线逐字节一致。
	// 基线场景 = 同一会话的 assistant 轮额外含一个 signature 为空的 thinking block(D-3
	// 空 signature 被跳过、不产出 replay item),故其上游响应请求体应与该负向 fixture
	// 逐字节相同。二者都不得携带 reasoning/encrypted_content 等 additive 字段。
	// 用全量请求体字节比较(而非仅 reasoning item 数)可捕获任何请求体/字段/形态回归。
	t.Run("negative-baseline", func(t *testing.T) {
		baselineReq := `{
		  "model":"gpt-5.6-terra","stream":true,"max_tokens":1024,
		  "messages":[
		    {"role":"user","content":"question"},
		    {"role":"assistant","content":[{"type":"thinking","thinking":"hmm","signature":""},{"type":"text","text":"plain reply"}]},
		    {"role":"user","content":"continue"}
		  ]}`
		env.upstream.set(serve(events))
		if body := env.postRelay(t, "/v1/messages", baselineReq); env.rec.Code != http.StatusOK {
			t.Fatalf("negative-baseline zero-additive reference status=%d body=%s", env.rec.Code, body)
		}
		baselineBody := fullBodyRaw
		if n := countReasoningItems(gjson.Parse(baselineBody).Get("input").Raw); n != 0 {
			t.Fatalf("zero-additive reference must inject 0 reasoning item; input=%s", baselineBody)
		}

		req := `{
		  "model":"gpt-5.6-terra","stream":true,"max_tokens":1024,
		  "messages":[
		    {"role":"user","content":"question"},
		    {"role":"assistant","content":[{"type":"text","text":"plain reply"}]},
		    {"role":"user","content":"continue"}
		  ]}`
		if body := env.postRelay(t, "/v1/messages", req); env.rec.Code != http.StatusOK {
			t.Fatalf("negative-baseline status=%d body=%s", env.rec.Code, body)
		}
		negBody := fullBodyRaw
		// 完整上行请求体与零 additive 基线逐字节一致(含 input 数组形态、字段集合、顺序)。
		if negBody != baselineBody {
			t.Fatalf("negative fixture upstream body drifted from zero-additive baseline:\n  base=%s\n  neg =%s", baselineBody, negBody)
		}
		// 全长请求体不得出现任何 additive 信号(encrypted_content/reasoning/signature)。
		for _, token := range []string{"encrypted_content", "\"reasoning\"", "signature"} {
			if strings.Contains(negBody, token) {
				t.Fatalf("negative-baseline body must not contain %q: %s", token, negBody)
			}
		}
		// input 数组与 3-turn golden 基线逐 item 断言(内容/角色/顺序)。
		golden := `[{"content":"question","role":"user"},{"content":[{"text":"plain reply","type":"output_text"}],"role":"assistant"},{"content":"continue","role":"user"}]`
		if got := gjson.Parse(negBody).Get("input").Raw; got != golden {
			t.Fatalf("negative-baseline input drifted from golden baseline:\n  want=%s\n  got =%s", golden, got)
		}
	})

	// redacted_thinking 显式跳过:历史含 redacted_thinking block -> 不产出 replay item。
	t.Run("redacted-thinking-skipped", func(t *testing.T) {
		env.upstream.set(serve(events))
		req := `{
		  "model":"gpt-5.6-terra","stream":true,"max_tokens":1024,
		  "messages":[
		    {"role":"user","content":"question"},
		    {"role":"assistant","content":[{"type":"redacted_thinking","data":"redacted payload"},{"type":"text","text":"still reply"}]}
		  ]}`
		if body := env.postRelay(t, "/v1/messages", req); env.rec.Code != http.StatusOK {
			t.Fatalf("redacted-thinking status=%d body=%s", env.rec.Code, body)
		}
		if n := countReasoningItems(inputRaw); n != 0 {
			t.Fatalf("redacted_thinking must NOT produce a reasoning replay item, got %d; input=%s", n, inputRaw)
		}
	})
}

// TestRegression_B7_StrictNegativeMock locks the R-008 strict-upstream risk
// detection: when the upstream rejects an unknown encrypted_content/reasoning
// input item with a 400, the gateway must propagate the 400 as a canonical
// Claude error envelope with the upstream message preserved — never swallowed
// into a successful response. Exercised on both the Codex bridge and the
// non-Codex via-responses path. The mock only returns 400 when it actually
// captured a reasoning item carrying the foreign signature, so any regression
// where the reasoning item is not sent upstream surfaces as a missing 400.
func TestRegression_B7_StrictNegativeMock(t *testing.T) {
	const foreignSig = "foreign-signature-xyz"
	const negMsg = "unrecognized encrypted_content item"

	negReqWithHistory := func(model string) string {
		return fmt.Sprintf(`{
		  "model":%q,"stream":true,"max_tokens":1024,
		  "messages":[
		    {"role":"user","content":"q"},
		    {"role":"assistant","content":[{"type":"thinking","thinking":"h","signature":%q},{"type":"text","text":"a"}]},
		    {"role":"user","content":"more"}
		  ]}`, model, foreignSig)
	}

	// strictNegMock 读取并验证请求真含未知 reasoning/encrypted_content input item:
	// 仅在捕获到时返回 400;若 reasoning replay 未上行则返回 200,测试对 400 的
	// 期望随即失败,暴露"reasoning item 未真正到达上游"的回归。
	strictNegMock := func() http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			reqBody, _ := io.ReadAll(r.Body)
			inputRaw := gjson.ParseBytes(reqBody).Get("input").Raw
			found, detail := reasoningReplay(inputRaw, foreignSig)
			if !found {
				t.Errorf("strict-negative mock did NOT capture reasoning item with encrypted_content=%q; input=%s detail=%s", foreignSig, inputRaw, detail)
				sseEvents([]string{
					`{"type":"response.created","response":{"id":"x","model":"g"}}`,
					`{"type":"response.completed","response":{"id":"x","model":"g","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
				})(w, r)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error","message":"`+negMsg+`","code":"unrecognized_reasoning"}}`)
		}
	}

	// assertCanonical400 asserts the downstream is an exact 400 Claude canonical
	// error envelope whose message preserves the upstream text (not swallowed).
	assertCanonical400 := func(t *testing.T, code int, body string) {
		t.Helper()
		if code != http.StatusBadRequest {
			t.Fatalf("upstream 400 must be preserved as canonical 400, got status=%d body=%s", code, body)
		}
		errObj := assertClaudeErrorEnvelope(t, code, body)
		if !strings.Contains(errObj.Get("message").String(), "unrecognized") {
			t.Fatalf("canonical error envelope lost upstream message: %s", errObj.Raw)
		}
	}

	t.Run("bridge", func(t *testing.T) {
		env := newFullPathEnv(t)
		env.upstream.set(strictNegMock())
		body := env.postRelay(t, "/v1/messages", negReqWithHistory("gpt-5.6-luna"))
		assertCanonical400(t, env.rec.Code, body)
	})

	t.Run("via-responses", func(t *testing.T) {
		env := newB7ViaResponsesEnv(t)
		env.upstream.set(strictNegMock())
		body := env.postRelay(t, "/v1/messages", negReqWithHistory("gpt-5.6-terra"))
		assertCanonical400(t, env.rec.Code, body)
	})
}

// ---------------------------------------------------------------------------
// B-8: count_tokens local-estimation fallback regression (ADR-3, zero code
// change). POST /v1/messages/count_tokens must keep the existing success /
// validation-failure / counting-failure assertions and must never dispatch a
// Codex upstream request — it estimates tokens locally (service.CountRequestToken
// via CountClaudeTokens), it does not bridge, and it does not relay.
// ---------------------------------------------------------------------------

func TestRegression_B8_CountTokens(t *testing.T) {
	env := newFullPathEnv(t)

	// count_tokens is a local-estimation fallback (ADR-3): it must never reach any
	// upstream. A zero-hit upstream that fails hard if ever invoked proves both that
	// no Codex relay request happens and that the counting-failure / success path
	// never spills into the channel layer.
	var hits int32
	env.upstream.set(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Error(w, "count_tokens must never reach the upstream", http.StatusInternalServerError)
	})
	assertNoUpstream := func(t *testing.T) {
		t.Helper()
		if got := atomic.LoadInt32(&hits); got != 0 {
			t.Fatalf("count_tokens dispatched %d upstream request(s); ADR-3 local estimation must never reach an upstream", got)
		}
	}

	// ① success: valid request → 200 + positive input_tokens (existing assertion,
	// mirrors controller relay_count_tokens_test success shape).
	t.Run("success", func(t *testing.T) {
		body := env.postRelay(t, "/v1/messages/count_tokens", `{
		  "model":"gpt-5.6-luna",
		  "messages":[{"role":"user","content":"count this prompt"}],
		  "tools":[{"name":"lookup","description":"Look up a value","input_schema":{"type":"object","properties":{"query":{"type":"string"}}}}]
		}`)
		if env.rec.Code != http.StatusOK {
			t.Fatalf("count_tokens success status=%d body=%s", env.rec.Code, body)
		}
		if n := gjson.Parse(body).Get("input_tokens").Int(); n <= 0 {
			t.Fatalf("count_tokens input_tokens=%d, want positive", n)
		}
		assertNoUpstream(t)
	})

	// ② validation failure: missing required field → 400 invalid_request_error
	// (existing assertion; message must mention messages).
	t.Run("validation-failure", func(t *testing.T) {
		body := env.postRelay(t, "/v1/messages/count_tokens", `{"model":"gpt-5.6-luna"}`)
		if env.rec.Code != http.StatusBadRequest {
			t.Fatalf("count_tokens validation status=%d, want 400, body=%s", env.rec.Code, body)
		}
		resp := gjson.Parse(body)
		if resp.Get("type").String() != "error" {
			t.Fatalf("count_tokens validation envelope type=%q, want error", resp.Get("type").String())
		}
		if resp.Get("error.type").String() != "invalid_request_error" {
			t.Fatalf("count_tokens validation error.type=%q, want invalid_request_error", resp.Get("error.type").String())
		}
		if msg := resp.Get("error.message").String(); !strings.Contains(msg, "messages") {
			t.Fatalf("count_tokens validation message=%q, want a mention of messages", msg)
		}
		assertNoUpstream(t)
	})

	// ③ counting failure: CountRequestToken errors → 500 api_error (existing
	// assertion). A Claude image block whose URL is on a refused port makes the
	// local media fetch fail deterministically (LoadFileSource → loadFromURL).
	t.Run("counting-failure", func(t *testing.T) {
		preMedia := constant.GetMediaToken
		preMediaNotStream := constant.GetMediaTokenNotStream
		constant.GetMediaToken = true
		constant.GetMediaTokenNotStream = true
		t.Cleanup(func() {
			constant.GetMediaToken = preMedia
			constant.GetMediaTokenNotStream = preMediaNotStream
		})

		body := env.postRelay(t, "/v1/messages/count_tokens", `{
		  "model":"gpt-5.6-luna",
		  "messages":[{"role":"user","content":[{"type":"image","source":{"type":"url","url":"http://127.0.0.1:1/image.png"}}]}]
		}`)
		if env.rec.Code != http.StatusInternalServerError {
			t.Fatalf("count_tokens counting-failure status=%d, want 500, body=%s", env.rec.Code, body)
		}
		resp := gjson.Parse(body)
		if resp.Get("type").String() != "error" {
			t.Fatalf("count_tokens counting-failure envelope type=%q, want error", resp.Get("type").String())
		}
		if resp.Get("error.type").String() != "api_error" {
			t.Fatalf("count_tokens counting-failure error.type=%q, want api_error", resp.Get("error.type").String())
		}
		// The refused image fetch must never have been serviced by the relay upstream:
		// only the local token counter attempted (and failed) the retrieval.
		assertNoUpstream(t)
	})
}

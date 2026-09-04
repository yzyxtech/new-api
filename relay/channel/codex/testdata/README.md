# full-path test fixtures

本目录为 B-1 / B-2 full-path 测试的 fixture 来源说明（`relay/channel/codex/messages_bridge_fullpath_test.go`）。

## 来源

- 上游 mock Codex Responses SSE 输入事件逐字段等价重建自 CLIProxyAPI 基线
  `internal/translator/codex/claude/codex_claude_response_test.go` 的
  `TestConvertCodexResponseToClaude_Stream*` 系列用例（基线 commit `17a65ee`，见 Plan Conventions）。
- 请求侧（工具名 / 64 字节收缩 / tool_result 对齐）对照
  `codex_claude_request_test.go` 与 `codex_claude_response_test.go` 的既有事件形态。

## 本测试的字段等价重建说明

- reasoning 的 `output_item.added` / `output_item.done` 沿用基线 `item.encrypted_content`
  快照 / 终值语义；为在 full-path 多请求场景稳定标识 A/B 两个 reasoning item，
  在 `item_id` 缺失时补充了根级 `item_id` 字段（响应转换器 `resolveReasoningState`
  的 identity 解析：优先 output_index → 根 item_id → item.id），summary 事件同样携带
  `item_id` 以关联到对应 item。
- 重复 `output_item.done` 用于验证 per-item 终态幂等。
- 工具名 / call_id 的 64 字节收缩与还原映射在测试内通过「上游回读请求体 → 回显
  short 名 → 下游断言还原原值」的黑盒方式验证，不依赖 bridge 内部映射细节。

## 目录约定

fixture 事件以字符串切片形式内联在该测试文件（`newB1HappyPathEvents` /
`newB1EarlyCloseEvents` / B-2 handler 内联），本目录仅作 provenance 留痕。

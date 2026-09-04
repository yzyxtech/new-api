# 本地真实凭证联调 SOP

本 SOP 是 `/v1/messages` 经 Codex 渠道桥接(Claude → OpenAI Responses → Codex
上游 → Responses → Claude 转换链路)的**真实凭证联调手册**。它面向已在本仓库部署
new-api 本地实例、并希望在**非线上**环境用真实 Codex 凭证完成桥接打通性验证的
维护者与联调人员。

> 重要边界:本 SOP 只描述**手动执行**的联调流程,不进入自动化测试(make test)。
> 自动化链路不依赖任何真实凭证(见 design §12)。联调期间请使用**独立于线上的本地
> 凭证**,严格遵守「唯一 refresh owner」红线,并在完成或中断后**清除**本地注入的凭证。

---

## 前置条件

准备以下事项后开始联调:

- 一份**独立的 Codex 订阅凭证**。它是「唯一 refresh owner」——即该凭证只在本本地
  实例上消费、只由本实例触发 OAuth/refresh 轮换,不得同时在线上实例、CLIProxy
  或任何其它服务中使用,避免两个客户端争夺同一 refresh token 导致轮换冲突
  (token rotation conflict)。
- 一台可独立起停的 new-api 本地实例(独立 Compose project + 全新 named volumes,
  见 `docker-compose.dev.yml`),不共享线上 DB / Redis。
- 一个可用的 Claude Code 客户端(或任意 Anthropic Messages 客户端),用于发起
  `/v1/messages` 请求。
- 准备至少两个非 Codex 的 Responses 上游(见「验证清单」的兼容性验证)以完成
  R-008 要求的真实上游接受性验证。

---

### 独立凭证注入

本节点明联调凭证的注入口径与「唯一 refresh owner」纪律。

Codex 渠道的鉴权由既有 adaptor 注入 `access_token` / `account_id`(auth header
不进日志)。联调时不使用线上 token,而是注入一份本地专用的独立凭证。

1. 在 new-api 管理端新建/选取一个 Codex 渠道,将其模型指向要联调的 Codex 订阅
   模型(如 `gpt-5.6-luna` / `gpt-5.6-terra` / `gpt-5.6-sol`)。
2. 在该渠道上配置**独立凭证**:填入本地专用、不与线上共享的 token/账户凭证。该
   凭证成为本次联调的唯一 refresh owner。
3. 记录渠道的 channel ID 与 ChannelType(Codex),以及模型名,供后续
   `chat_completions_to_responses_policy` 精确命中。

**唯一 refresh owner 语义**:

- 一份凭证在同一时间段内只能被一个客户端消费并驱动 refresh 轮换。
- 若同一凭证同时被本地实例与线上实例(或 CLIProxy)使用,refresh token 会被多端
  轮换,导致其中一端认证失效——这是联调期最应避免的越界事故。
- 联调结束(或中断)后,必须执行「凭证清除」章节清理本地注入,恢复凭证的唯一
  owner 状态。

---

### 本地实例打通步骤

目标:让 Claude Code 客户端经本地 new-api 实例转发到 Codex 上游,并验证桥接
(`chat_completions_to_responses_policy` 命中 Codex 渠道 + 目标模型)生效。

1. **启动本地实例**,确认服务监听端口(记为 `<PORT>`),并确认该实例不依赖线上
   DB/Redis。
2. **注入策略**:让 `/v1/messages` 命中桥接,须把全局策略
   `chat_completions_to_responses_policy` 写为完整五字段对象。推荐对象(恒发完整
   五字段,勿省略字段):

   ```json
   {
     "chat_completions_to_responses_policy": {
       "enabled": true,
       "all_channels": false,
       "channel_ids": [<codex_channel_id>],
       "channel_types": [<codex_channel_type>],
       "model_patterns": ["gpt-5.6-.*"]
     }
   }
   ```

   写入方式:管理端「模型设置-全局」粘贴上述完整对象保存;或经通用 option 端点
   PUT 写回。注意:该 key 无专项校验,`success:true` 不代表已生效——必须用探针
   序列核对(写入 → GET 读回核对持久化值 → POST `/v1/messages` 观察桥接是否生效)。
3. **打通 Claude Code 到本地实例**:配置客户端环境变量,使其指向本地 new-api。

   - `ANTHROPIC_BASE_URL`:指向本地 new-api 的**根地址**(不含 `/v1`,SDK 会在其
     后拼接 `/v1/messages`)。例如:

     ```bash
     export ANTHROPIC_BASE_URL=http://127.0.0.1:<PORT>
     ```

   - `ANTHROPIC_AUTH_TOKEN`:填入本地 new-api 侧对该客户端鉴权用的 token/key
     (管理端分配,非渠道凭证本身)。

   ```bash
   export ANTHROPIC_AUTH_TOKEN=<local_newapi_api_key>
   ```

4. **确认打通**:用客户端发出最小请求,确认:

   - 请求到达本地实例并命中 Codex 渠道;
   - 策略命中,桥接生效(bridge 强制上游 `stream=true`);
   - 响应以 Anthropic Messages SSE 状态机正常返回。

   ```bash
   # 仅作连通性检查;以下 curl 是等价于客户端行为的原始验证
   curl -sS -N \
     -H "content-type: application/json" \
     -H "anthropic-version: 2023-06-01" \
     -H "x-api-key: ${ANTHROPIC_AUTH_TOKEN}" \
     -d '{"model":"gpt-5.6-luna","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}' \
     "${ANTHROPIC_BASE_URL}/v1/messages"
   ```

5. **核对策略生效**:若第 4 步未走桥接(表现为非预期回落),回到第 2 步核对策略
   五字段对象是否完整、target 渠道/模型是否被 `channel_ids`/`model_patterns`
   覆盖。GET `/api/option/` 读回的是**持久化值**,不是当前有效运行值;生效只能经
   「写入 → 读回核对 → 实际请求观察」三点探针完成。

---

### 验证清单

联调验证分三类:多轮、工具、thinking(signature)。每类通过后才算该项完成。

#### 多轮验证

- [ ] 单轮 stream 请求返回正确的 SSE 事件序列(`message_start` → 内容块 →
      `message_delta` → `message_stop`)。
- [ ] 第二轮携带首轮 assistant 输出作为历史,能正确继续对话,且 `reasoning`
      历史中的 thinking/signature 逐一轮转正确。
- [ ] 非 stream(`stream=false`)聚合请求返回单个合法 Claude message JSON。

#### 工具验证

- [ ] 历史含 `tool_use` / `tool_result` 时,上行能正确回放,上游 `call_id` 与本地
      工具调用精确关联。
- [ ] 并行工具调用、工具名/call_id 接近 64 字节边界时,上行收缩、下行还原一致
      (还原名/arguments 完整)。
- [ ] web_search 场景:请求侧声明 `web_search_20250305` / `web_search_20260209`
      工具定义时,上行映射为 `web_search` 工具并透传 filters;响应侧产出
      `server_tool_use` + `web_search_tool_result`。

#### thinking(signature)验证

- [ ] 含 signature 的 thinking 历史能按**原字符串**上行(不经 sigcompat 归一化/
      过滤),响应侧以 `content_block_delta` 内嵌 `delta.type=signature_delta`
      还原。
- [ ] signature-only(仅 `output_item.done` 终值非空)能先开空 thinking 块再输出
      一次 signature_delta。
- [ ] 提前关闭后 done 迟到(signature 被丢弃)时,行为合法且诊断数组仅记
      `signature_dropped_after_early_close` 对应条目。

#### R-008:signature 上游接受性验证(必须在真实验证前不得把非 Codex 请求侧增量描述为已确认兼容)

本项用于确认请求侧 signature 增量(把 Anthropic thinking signature 原字符串映射
为 `reasoning.encrypted_content`)在真实上游的接受性。安排 **Codex 自会话
signature 与外来 signature 各一组**、以及**至少一个非 Codex Responses 上游对含
signature thinking 历史的真实验证**。

- [ ] Codex **自会话 signature** 组:由本实例自己首轮产生 thinking + signature,
      第二轮作为历史上行。记录原字符串上行后的**接受**。
- [ ] Codex **外来 signature** 组:注入一份非本会话产生的 signature 字符串上行。
      记录是**接受**还是**拒绝**;若拒绝,记录失败 HTTP status / envelope 形态,以及
      是否可安全回退。
- [ ] **非 Codex Responses 上游**:选至少一个非 Codex 的 `/v1/responses` 上游,
      对含 signature thinking 历史做真实请求。记录:
      - 原字符串(不经 sigcompat)上行后的接受/拒绝;
      - 对 `reasoning.encrypted_content` / 未知 reasoning item 的行为;
      - 失败 HTTP status / error envelope;
      - 是否可安全回退。
- [ ] 验证完成前,不得把「非 Codex 请求侧增量已确认兼容」写入任何文档/描述。
      若未验证或验证被拒,保留「未验证状态」。

> 结果登记表(联调执行时填写):

| 上游 | signature 来源 | 接受/拒绝 | reasoning item 行为 | 失败 status/envelope | 可安全回退 |
| --- | --- | --- | --- | --- | --- |
| Codex | 自会话 | | | | |
| Codex | 外来 | | | | |
| 非 Codex(`<上游名>`) | 外来/含 signature 历史 | | | | |

---

### 凭证清除

联调完成或中断后,必须清除本地注入的凭证,恢复凭证「唯一 owner」状态。

1. **清除客户端侧**:删除 `ANTHROPIC_BASE_URL` 与 `ANTHROPIC_AUTH_TOKEN` 环境
   变量(或恢复为线上默认值),避免后续误发到本地实例:

   ```bash
   unset ANTHROPIC_BASE_URL
   unset ANTHROPIC_AUTH_TOKEN
   ```

2. **清除服务侧**:在本地 new-api 管理端删除联调用 Codex 渠道(或移除其中的独立
   凭证),并停用策略——把 `chat_completions_to_responses_policy` 的
   `enabled` 写回 `false`(恒发完整五字段对象):

   ```json
   {
     "chat_completions_to_responses_policy": {
       "enabled": false,
       "all_channels": true
     }
   }
   ```

3. **清理实例**:停掉本地实例与独立 Compose project;如需彻底清除,删除对应的
   named volumes,确保不含任何 token / session 残留。
4. **恢复 owner 状态**:确认独立凭证已不再被本地实例引用;该凭证若仍需复用,仅保留
   一个消费方(恢复「唯一 refresh owner」);若不再使用,直接吊销。
5. **验证无残留**:重启后的实例不应再加载联调期注入的凭证/策略;用 `GET
   /api/option/` 核对策略已回落为关闭态。

---

### count_tokens 本地估算偏差口径

`count_tokens` 端点采用**本地估算回退**,`/v1/messages/count_tokens` 不发起任何
Codex 上游请求(ADR-3 零代码变更)。

- **实现**:经既有 `service.CountRequestToken` 用 tiktoken 在本地计数;成功返回
  `{"input_tokens": <integer>}`,校验失败 400 `invalid_request_error`,计数失败
  500 `api_error`。无计费副作用。
- **偏差口径**:本地估算与 Anthropic 服务端 tokenizer 存在偏差(R-005),两者对同
  一输入可能给出不同 token 数。`count_tokens` 的返回值用于**预扣/估算**场景,
  不是计费权威值;最终计费以成功请求终态 usage 为准。
- **结论**:该偏差已被接受(ADR-3 裁决)。联调时若发现 `count_tokens` 与上游实际
  usage 差异明显,属预期;如需更精确的上游对标(Codex backend 是否存在
  `input_tokens` 对标端点)属后续迭代,本期不在此 SOP 范围。
- **自动化保证**:full-path 回归只断言既有成功/校验失败/计数失败行为且不发起
  Codex 上游请求,不承诺与上游精确一致。

---

## 维护说明:bridge 估算与直连语义级对齐

`/v1/messages`(bridge 命中)在上游终态缺 usage、或 stream 已交付内容后失败/中断
时,对已输出内容做本地估算(`CompletionTokens` 用既有 tokenizer 估算已累积内容,
`PromptTokens` 缺省经 `GetEstimatePromptTokens()` 回填),以 `(usage估算, nil)`
差额结算。

该估算逻辑与 Codex `/v1/responses` **直连路径**的
`OaiResponsesStreamHandler`(`relay/channel/openai/relay_responses.go:142-156`)
是**两份独立实现、语义级对齐**。二者必须保持一致:

- bridge response session 的实现与直连 `relay_responses.go:142-156` 语义对齐,
  同 fixture 下最终扣费/usage 字段一致。
- **任何对直连估算口径的变更,必须同步桥接侧的估算逻辑**,否则 bridge 与直连会
  出现计费不一致(隐性耦合)。修改直连估算时,请同步检查/更新 bridge 侧对应代码,
  并在注释中钉死对齐源文件与行号。

---

## 术语与红线速查

| 术语 / 红线 | 含义 |
| --- | --- |
| 唯一 refresh owner | 一份凭证同时只能被一个客户端消费并驱动 refresh 轮换;联调凭证不与线上/CLIProxy 共享 |
| sigcompat | 基线上的 signature 归一化/过滤层;本桥接按原字符串上行,有意省略 |
| `reasoning.encrypted_content` | Requests 侧承载 Anthropic thinking signature 原字符串的字段 |
| 未验证状态 | 非 Codex 请求侧增量的上游接受性尚未确证;验证完成前不得描述为「已确认兼容」 |

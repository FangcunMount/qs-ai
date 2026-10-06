# 模型绑定、协议适配与响应规范化

## 本文回答

业务冻结路由如何解析成本地 endpoint 和凭证，如何保留不同提供方的真实回执，以及网关失败如何影响恢复。

## 结论

业务侧冻结模型执行身份，网关从宿主 `Settings` 解析连接与凭证。当前 Router 支持 DeepSeek Responses 与智谱 Chat 适配，并保留旧 DeepSeek route 的兼容入口。LangChain 模型对象负责一次异步协议调用，持久发送资格与恢复由应用和数据库负责；网关不自行重试、回退模型或修改冻结选择。

## 概念与责任

| 对象 | 保存什么 | 所属边界 |
|---|---|---|
| `ModelRouteV2` | provider/model/protocol/adapter contract、binding ID/revision、超时与输出参数 | 冻结资产和执行配置 |
| 本地 binding | endpoint、启用状态、协议实现版本关联 | 宿主配置 |
| credential | DeepSeek 或智谱 SecretStr | 宿主运行环境，不序列化到请求快照 |
| `ModelExecutionIdentity` | 实际绑定身份、请求模型、route fingerprint | 模型响应证据 |
| `ModelResponse` | 原输出、校验输出、规范化动作、request ID、usage、latency | 持久回执与成果追溯 |

治理时“当前可选模型”与执行时“已冻结绑定”分开。Router 不查询可变模型资格来重新选模型；找不到冻结 binding/revision、绑定停用或协议不一致，则明确失败。

## 调用链

`PublishedReportWorkflow` 解析请求绑定配置 → `DurableGeneration` 保存冻结请求并建立发送资格 → `ModelGatewayRouter` 解析 binding 与密钥 → 提供方适配器构建请求 → LangChain `ainvoke` → 校验并规范化提供方响应 → 持久回执 → 业务输出验证与 Artifact。

DeepSeek 路径按 QS Responses wire 构建 system/developer/user 角色，智谱路径采用 Chat 请求；不能为了统一界面把提供方私有字段、角色或响应状态 silently 丢弃。请求构建与适配契约以代码和资产 Schema 为准。

提供方客户端只支持异步调用，禁用 streaming/cache；调用处禁用 LangSmith tracing，传入空 callbacks，避免业务正文被环境中的云追踪配置采集。HTTP 客户端不使用环境代理与自动重定向。

## 响应接受不变量

- 回执 invocation 必须匹配本次调用，response model 必须匹配请求路由；v2 额外保存绑定 revision 和 route 指纹。
- DeepSeek Responses 需要终态 completed、非空 request ID、恰好一条 message 和可用 output_text；拒绝、未终态、错误模型、非法 usage 分别分类。
- JSON 解析拒绝重复字段和非法常量；输出规范化保留 `raw_output` 与 `validation_output`，不能把修饰后的文本伪称为供应商原始输出。
- 响应有大小和超时限制；业务 JSON Schema、证据引用和安全规则继续在后续输出验证层检查。网关拿到 200 不能证明语义正确。

## 失败语义

连接建立失败、明确 429、认证或余额问题、明确请求拒绝、提供方非终态/超时/传输中断分别保留错误码。可能已经执行但无法确认结果的失败携带 `result_unknown=True`；由持久恢复层阻止再次发送。

`retryable` 描述失败类型，不能授权原 invocation 再发。自动 retry/backoff、候选替换、模型 fallback 都必须经过业务执行策略与新调用账本，不能藏在 LangChain 或 HTTP 适配器内。

## 选择、代价与限制

统一 `ModelResponse` 保留跨提供方共性，专用适配器保留 wire 细节，代价是每个新协议都需测试角色、参数、终态、usage、身份与 unknown 分类。通用 OpenAI-compatible 名称不足以证明提供方在所有字段上兼容。

将 endpoint/凭证写入冻结业务快照可以方便重放，但会泄漏秘密并把部署地址固化；当前保留 binding ID/revision，让运维供给连接，同时故障时明确失败。历史 binding 的删除或修改需要考虑仍可执行的冻结配置，不能只看新请求资格。

## 实现与验证

- Router：[`router.py`](../../../src/qs_ai/infrastructure/models/router.py)。
- Responses：[`qs_server/responses.py`](../../../src/qs_ai/infrastructure/qs_server/responses.py)、[`models/responses.py`](../../../src/qs_ai/infrastructure/models/responses.py)、[`response_contract.py`](../../../src/qs_ai/infrastructure/models/response_contract.py)。
- 智谱与规范化：[`zhipu.py`](../../../src/qs_ai/infrastructure/models/zhipu.py)、[`normalization.py`](../../../src/qs_ai/infrastructure/qs_server/normalization.py)。
- 测试：[`test_multi_provider_gateway.py`](../../../tests/test_multi_provider_gateway.py)、[`test_langchain_gateway.py`](../../../tests/test_langchain_gateway.py)、[`test_provider_normalization.py`](../../../tests/test_provider_normalization.py)、[`test_model_receipt_identity.py`](../../../tests/test_model_receipt_identity.py)。这些可使用提供方替身，不证明线上绑定与余额可用。

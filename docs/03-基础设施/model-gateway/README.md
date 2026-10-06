# 模型绑定、协议适配与响应规范化

网关把**已经冻结的模型执行选择**变成一次供应商请求，再返回可持久化的响应记录。它不负责重新选择模型、自动重试、接受业务成果或决定质量。一个返回 200 的请求仍可能因终态、模型身份、JSON、报告引用或质量检查失败；一个超时的请求也可能已经在供应商处执行。

例如，某评测候选绑定 `zhipu-official / v1`。当前目录新增 v2 不会让原执行改用 v2；v1 被删除或停用则明确失败。若请求已经发出后超时，网关报告结果未知，原执行事实留给持久恢复层裁定，不能换到 DeepSeek 再试一次。

本文核对 `766b2aa` 的未变业务源码，日期为 2026-10-06。模型能力的治理选择见 [方案资产设计](../../02-业务模块/governance/01-方案资产与原子准备设计.md)，调用账本与恢复见 [执行恢复](../execution-recovery/README.md)。这里描述协议代码，不据此宣布任何线上模型、凭证、余额或质量可用。

## 冻结 Route 与宿主连接各保存什么

| 对象 | 实际内容 | 是否进入业务快照 |
| --- | --- | --- |
| ModelRoute v1 | route/revision、provider/model、protocol、输出模式、token/超时/reasoning 参数 | 是；保留原字段顺序及省略规则 |
| ModelRouteV2 | 在上述字段上增加 format_version、model_key/catalog_revision、binding_id/revision、adapter_contract、thinking/sampling | 是；规范 JSON 形成 route fingerprint |
| ModelCapability | 当前可选 model_key/model_id、目录修订、用途、verified/evidence_ref、参数能力和默认值 | 用于新选择/准入，不是响应时重新选模型的依据 |
| ModelBinding | provider/protocol/adapter、endpoint、credential_slot、enabled 与修订 | 宿主 Settings；Route 只保存其身份 |
| SecretStr 凭证 | DeepSeek 或智谱 API key | 宿主环境；不写进 Route、ModelCall、响应身份或治理目录 |
| ModelExecutionIdentity | provider、requested_model、protocol、adapter_contract、binding_id/revision、route_fingerprint | v2 ModelResponse 的安全执行身份，无 endpoint/secret |

[ModelConfiguration](../../../src/qs_ai/model_configuration.py) 对 v2 binding 限定 HTTPS 官方 host/path，禁止账号信息、查询串、fragment 和非 443 端口。DeepSeek 为 `api.deepseek.com /responses`，智谱为 `open.bigmodel.cn /api/paas/v4/chat/completions`；provider、protocol、adapter 和 credential_slot 必须一致。它不是一个可填任意“兼容地址”的代理配置。

新模型选择由目录 `resolve(key, revision, purpose)` 判断：条目存在、verified、有 generation/semantic 用途、目录修订精确匹配、binding enabled。`verified=true` 要有 evidence_ref，但配置校验不读取该证据并重做质量实验。固定 Route 还要通过参数上限及组合校验；界面展示过的目录修订不能代替提交时的新准入。

[ModelRouteV2](../../../src/qs_ai/application/interpretation/model_route_v2.py) 支持的协议组合只有：

| provider | protocol / adapter_contract | 输出和参数边界 |
| --- | --- | --- |
| deepseek | responses / deepseek-responses/v1 | json_schema；thinking 必须为空，使用 reasoning_effort；温度只用于 none，top_p 受 thinking 模式及不低于 0.95 的限制 |
| zhipu | chat_completions / zhipu-chat/v1 | json_object；thinking 显式 enabled/disabled，必须与 reasoning_effort 一致 |

两者 v2 均限制超时 1–180 秒、输出 token 1–12000，temperature 与 top_p 不能同时设；目录可进一步收紧。`idempotent_redispatch` 与 `retrieve_by_invocation_id` 必须为 false。Invocation ID 是本地执行关联，不能因智谱 request_id 恰好携带它就声称供应商支持幂等重发或查询。

## 实际执行查 binding，不重新选目录

[ModelGatewayRouter](../../../src/qs_ai/infrastructure/models/router.py) 先核对生成 Route 与 PreparedExplanation 的发布 route，随后按固定 Route 发起调用。语义裁判复用 `generate_messages`，消息和 Schema 来自该阶段的冻结资产。

1. v2 精确查 `(binding_id, binding_revision)`；缺失或 disabled 为 `binding_unavailable`，provider/protocol/adapter 不一致为 `model_route_unsupported`。不取最新修订，不更换模型。
2. v1 只支持原 DeepSeek Responses，连接取 `settings.generation.endpoint`，保留旧资产契约；没有把它自动转成 v2。
3. 按 provider 取本地凭证。DeepSeek 使用 effective_deepseek_api_key，兼容旧 model_api_key；新旧值并存必须相同。智谱使用独立 zhipu_api_key。无 endpoint 或空凭证也返回 binding_unavailable。
4. 专用适配器执行一次调用。成功后 v2 追加原 Route 的 ModelExecutionIdentity；v1 保留原响应结构。

Router 不重新查当前 ModelCapability，允许原固定执行使用仍保留的历史 binding。这与新准入仍须检查当前目录并不矛盾。部署所有者必须保留可执行历史 binding，并为连接/契约变更建立新修订；仅凭 Route 的 ID 不能证明宿主没有错误修改同一修订的配置。

[GenerationProvider](../../../src/qs_ai/bootstrap/providers/generation.py) 在 REQUEST scope 创建并关闭 httpx 客户端，设置 `trust_env=False`、`follow_redirects=False`。它要求 QS 当前授权地址与可用连接配置；禁用 generation 时提供 UnconfiguredWorkflow。评测使用同类网关，资源寿命见 [Dishka 所有权](../../01-运行时/02-Dishka作用域与资源所有权.md)。

## 两种 wire 保留不同的角色和终态

| 位置 | DeepSeek Responses | 智谱 Chat |
| --- | --- | --- |
| system/task/data | instructions=system；developer 放任务；user 放 data_preamble 和 data_json | system 合并原 system、任务及完整 Schema 指令；user 放独立报告数据 |
| 输出约束 | text.format=json_schema 或旧 json_object；省略 strict/store | response_format=json_object；stream=false；request_id=本 invocation |
| 参数 | max_output_tokens、reasoning.effort；v2按冻结值添加sampling | max_tokens、thinking；enabled时添加reasoning_effort，按冻结值添加sampling |
| 成功终态 | status=completed，恰好一条 message，至少一个非空 output_text | 恰好一个 choice，finish_reason=stop，非空文本对象，不能含 tool_calls |
| 身份/用量 | 非空响应 id、model 等于原 Route；可选 input/output/reasoning token 合法 | 非空响应 id、model 等于原 Route；可选 prompt/completion token 合法 |

DeepSeek 多个 output_text 按原顺序连接；拒绝项、错误模型、非法用量、缺 message 或空正文各有分类错误。queued/in_progress 是非终态未知结果，不能接受为成果。智谱 length 为 token 上限失败，sensitive/content_filter/refusal 为拒绝，其他结束原因或工具调用不属于该契约。

[DeepSeek 请求投影](../../../src/qs_ai/infrastructure/qs_server/deepseek_request.py) 只向供应商提供其支持的 Schema 子集：展开本地 ref、const 转 enum、oneOf 转 anyOf，省略部分长度/数量约束；三主题 prefixItems 则转为通用 section 数组。远端能返回结构不证明顺序、互斥和业务数量正确。完整本地 Schema、证据引用及安全规则始终在调用后重新验证。智谱 json_object 也只是 JSON 形式，不能代替本地完整 Schema。

LangChain 的 [QSResponsesModel](../../../src/qs_ai/infrastructure/models/responses.py) 和 [ZhipuChatModel](../../../src/qs_ai/infrastructure/models/zhipu.py) 仅实现异步调用，cache=false、disable_streaming=true，拒绝自定义 stop 和错误消息角色。调用设置空 callbacks，并在 `tracing_context(enabled=False)` 中运行；即使环境开启 LangSmith tracing，也不能让测评正文自动进入云追踪。

## HTTP 与响应失败怎样进入持久执行

单次 HTTP 在 Route timeout 下执行，另设置 httpx timeout；逐块读响应，默认最多 2,097,152 字节。限制针对响应正文，不等于模型输出字符上限或 Artifact 投递上限。适配器没有内置 retry/backoff、模型 fallback、结果轮询或断路器。

| 观察到的失败 | code | retryable / result_unknown | 恢复层应理解什么 |
| --- | --- | --- | --- |
| 连接建立失败/连接超时 | provider_connect_failed | true / false | 与已发出后的传输中断不同；仍要按原持久调用记录处理 |
| HTTP 429 | provider_rate_limited | true / false | 明确限流；是否能开新尝试由冻结业务策略决定 |
| HTTP 401/403、402、其他明确请求拒绝 | provider_authentication_failed / provider_insufficient_balance / provider_request_rejected | false / false | 不暴露上游原错误正文，不自行换凭证或模型 |
| HTTP 408 或至少500 | provider_server_error | true / true | 请求可能已经执行；技术可恢复标签不授权重发 |
| 总超时/httpx超时 | provider_timeout | true / true | 缺响应不证明供应商未执行 |
| 其他HTTP传输错误 | provider_transport_error | true / true | 可能已发送，保存未知边界 |
| 成功HTTP正文超出字节上限 | provider_response_too_large | false / true | 不能因未读完整而当作未执行 |
| Responses queued/in_progress | provider_response_not_terminal | false / true | 当前协议不支持按 invocation 查询后续结果 |

已收到完整响应后的契约失败继续保留专用分类，不能一概说都是 unknown；例如响应拒绝、token 上限、错模型、缺 id、非法 usage、非法 JSON。DeepSeek failed 的 error code 和 cancelled 状态可分别带 retryable，但仍不能绕过业务阶段白名单。

[DurableGeneration](../../../src/qs_ai/application/execution/generation.py) 在外发前持久建立 ModelCall。只有 `created=true` 的原调用才能进入 gateway；已有 response_received 解码原响应，已有 dispatched/unknown 返回 provider_result_unknown，已有 failed 复用原 code。成功响应还核对 invocation/model；不匹配时以 result_unknown 处理。取消、lease 丢失或响应提交失败可能留下 dispatched 标记，接手 worker 不能靠“没有 receipt”再发一次。

评测的 serial/candidate、预算与恢复规则见 [评测设计](../../02-业务模块/evaluation/01-评测候选门槛与审核设计.md)。网关的 retryable 描述一次失败，不是任何路径的再次发送资格。

## 规范化保留原文，不修补业务内容

[normalize_output](../../../src/qs_ai/infrastructure/qs_server/normalization.py) 仅对 DeepSeek 解开有限包装：完整独占的 json Markdown fence，或唯一字段的 parameters/json 对象、json_string 对象文本。它拒绝把前后附文、多个字段、数组、重复 key、NaN 或损坏 JSON 当成合法包装；不会补括号、删业务字段、补引用、加免责声明或修改分数。

```text
raw_output:        {"json": {  "summary": "原文"  }}
validation_output:{  "summary": "原文"  }
normalization:    envelope_unwrapped
```

保留 raw_output；对象 envelope 的 validation_output 保留取出的内部格式，使空白仍计入输出字符预算。Markdown及json_string去除取出文本外围空白，不修改对象内容。对象 envelope 只去一层；Markdown 外层加一个支持的 envelope 可以按明确规则解开。其他 provider 不应用这项兼容处理，智谱必须已经返回严格 JSON 对象。

ModelResponse 保存 invocation、供应商 request_id、model、上述两段文本、normalization、可空用量、latency 和可选执行身份；它不是完整 HTTP wire 抓包，不能复原所有响应头和原 envelope 字节。未知用量为 None，不补造0。业务解析后 Artifact.content_json 还会重新序列化并形成自己的字节摘要；不能把 content hash 当成 raw_output hash。

## 验证入口与证明范围

| 现有测试 | 直接核验的机制 |
| --- | --- |
| [test_model_configuration.py](../../../tests/test_model_configuration.py)、[test_model_route_v2.py](../../../tests/test_model_route_v2.py)、[test_model_selection_v2.py](../../../tests/test_model_selection_v2.py) | binding白名单、目录修订、verified/用途、参数互斥、旧Route字节和显式版本 |
| [test_deepseek_request.py](../../../tests/test_deepseek_request.py)、[test_langchain_gateway.py](../../../tests/test_langchain_gateway.py) | Responses请求角色/参数、一次HTTP、终态/用量/拒绝分类、LangChain异步与trace隔离 |
| [test_multi_provider_gateway.py](../../../tests/test_multi_provider_gateway.py) | 智谱wire/回执、当前目录缺失不改历史binding、旧revision不换新值、不同凭证、错误不retry/fallback |
| [test_provider_normalization.py](../../../tests/test_provider_normalization.py)、[test_model_receipt_identity.py](../../../tests/test_model_receipt_identity.py) | 有限包装、原文与字符预算、v2响应身份与endpoint不入回执 |
| [test_mbti_runtime.py](../../../tests/test_mbti_runtime.py)、[test_report_graph.py](../../../tests/test_report_graph.py)、[test_semantic_gateway.py](../../../tests/test_semantic_gateway.py) | 原响应恢复不外发、unknown不重发、语义阶段固定请求与失败分类 |

上述替身测试可以保护协议和恢复边界，不能证明实际网络、API key、当前模型支持、余额、输出质量或发布批准。新 binding/adapter 要补齐自己的协议证据及冻结恢复契约，不能只因自称“OpenAI compatible”就继承现有两种适配器的保证。

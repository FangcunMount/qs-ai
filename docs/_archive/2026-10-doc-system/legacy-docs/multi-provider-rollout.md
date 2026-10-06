# 多供应商模型配置与评测接入台账

本项目按已批准 M0–M5 计划实施；技术验证、人工质量批准和正式发布分开记录。

## 当前状态（2026-09-21）

| 阶段 | 状态 | 尚缺证据 |
|---|---|---|
| 后端兼容发布 | PR #118 已合并，main 为 `3e3d4b1`；主干检查及自动部署成功；单进程、就绪及 mTLS 已核证 | 新版真实业务生成回归 |
| 可操作的多模型评测 | 目录/方案/执行接线已合并；受控探测已完成四模型六用途连通验证 | 参数目录、三端接入、五轮完整评测 |
| 业务上线与结项 | 未开始 | 含智谱组合的人工审核发布、真实结果及恢复、过渡配置清理 |

以下批次记录为历史实施证据，以本表为当前状态，不将旧待办误作最新结论。

## 基线

- qs-ai: 66d4aa117cf21534704bcf45be45106520ae4b75。
- qs-server: 0358557a2（main）。
- Operating: af6bfa7（main）。
- 专属分支 codex/multi-provider；不修改原主干工作区。
- 原路线、方案、LangChain 定向基线14项通过。

## 首轮候选（不是已启用清单）

| 代号 | API model ID | 协议 | 目标用途 | 证据状态 |
|---|---|---|---|---|
| DQ | deepseek-v4-pro | Responses | 生成、语义 | 旧路径已有生产证据，新路由待验收 |
| DL | deepseek-flash | Responses | 生成 | 官方文档列出；账户及业务待验证 |
| ZQ | glm-5.3 | Chat Completions | 生成、语义 | 官方文档列出；账户及业务待验证 |
| ZL | glm-5.3-flash（待实际 API 确认） | Chat Completions | 生成 | 用户指定 GLM-5.3-Flash；文档参数段提及该系列，但模型枚举未列出，准确 ID、账户及业务待验证 |

模型选择按用户最新指定固定为 DeepSeek V4 Pro、DeepSeek Flash、GLM-5.3、GLM-5.3-Flash。不得因 Flash 不可用而自动替换为 GLM-4.7-Flash；不可用时报告阻塞。

来源：[DeepSeek](https://api-docs.deepseek.com/zh-cn/api/create-response/)、[智谱](https://docs.bigmodel.cn/api-reference/模型-api/对话补全)，2026-09-21核对。

DeepSeek现有生产Secret名为QS_AI_MODEL_API_KEY；智谱预定QS_AI_ZHIPU_API_KEY，已请求用户配置，不记录值。不进行未经账本记录的生成调用。

## 固定执行预算

DQ/DQ、DL/DQ、ZQ/DQ、ZL/DQ、DQ/ZQ五轮；每轮7组×5候选和预检。正常350次调用；按现有各阶段70次上限最多700次，探测单列。当前组织预算待正式入口核对，不自动提高上限或重置已用量。

## 当前批次 M1a：兼容格式与配置基础

- 增加严格v2冻结路线、明确供应商/协议/适配版本/绑定身份。
- 旧v1定义序列化不变；原始冻结请求中route使用显式联合类型，v2回执恢复保留新增字段。
- 格式或适配身份损坏明确拒绝，禁止回落成v1。
- 增加部署绑定/能力配置类型；新模型无验证证据不得标记verified。
- models.v2_writes_enabled默认false，目录默认空；本批不开放新写入、不修改现有运行装配。
- 只接入配置读取和路线解码；M1完整能力接口、方案准备及写开关执行门禁仍待后续批次。
- 无数据库迁移、无生产修改。

## 余项

- M0：真实账户可用性、完整参数矩阵和三端契约示例待完成。
- M1b：目录到方案选择/准备/准入、能力API、目录版本冲突与写入门禁。
- M2：统一Router、GLM适配、回执扩展、错误分类和隔离恢复。
- M3：QS及Operating、五轮正式评测矩阵、管理员独立操作。
- M4：含智谱组合真实人工审核发布、生成展示及生产恢复。
- M5：迁移部署配置、移除临时别名、完成证据。

v2开始写入后，仅允许回滚到已验证支持v2的镜像。保留有效v1读取器和当前线上v6；不自动代签、不降低质量门槛。

## 密钥迁移接线

production已确认存在QS_AI_DEEPSEEK_API_KEY、QS_AI_ZHIPU_API_KEY和旧QS_AI_MODEL_API_KEY（只检查名称）。Actions传入三者供部署前冲突检查；新runtime文件仅注入供应商专用名称。运行时保留旧DeepSeek名称读取兼容；两者不一致则拒绝，不输出值。生成和评测统一使用解析后的DeepSeek凭据。

智谱密钥已接入配置和部署，不代表智谱适配器或真实调用已完成。旧Secret待生产新名称调用通过后再删除。原发布目录不重写，用于回滚。

本批78项配置/部署/装配回归通过；ruff与目标mypy通过。未合并、未发布，线上仍使用原版本。

## M1b / M2a — selection wiring and Zhipu adapter

Implemented on `codex/multi-provider` (not yet deployed):

- Solution selections accept explicit `model_key`, `catalog_revision`, thinking and sampling fields. Save and atomic preparation validate current eligibility, purpose and bounds; frozen routes retain the selected binding revision. Legacy command hashes omit absent v2 fields.
- Admission checks v2 model identity and binding against the current catalog; accepted execution resolves only its frozen binding, independently of current catalog eligibility. Catalog revision conflicts return gRPC `ABORTED`.
- Capability responses retain legacy fields and add a redacted catalog with provider, protocol, adapter contract and availability reasons. They never expose endpoints or credentials.
- Generation and evaluation now share `ModelGatewayRouter`. DeepSeek retains its Responses request projection; Zhipu uses a controlled async LangChain chat model with independent user data, JSON-object output, strict receipt parsing and the same bounded single-send transport. No provider fallback or implicit retry is introduced.
- Binding-only dependency assembly permits receipt recovery without legacy credentials. A new dispatch with a missing or disabled original binding fails explicitly.

Local evidence: focused selection, adapter, legacy wire, container and route tests pass; disposable MySQL 8.4 solution tests cover Zhipu generation and semantic route freezing, 35 candidate obligations, and original preparation receipt recovery after catalog removal. Interop tests requiring the Go harness must run with that harness/CI; a skipped test is not acceptance evidence.

Still pending: production catalog population after account-level probes, model-specific capability/default refinement, receipt observability metadata, QS/Operating integration and real four-model evaluation/publication. GLM-5.3-Flash remains unverified. The v2 write flag remains disabled by default; this batch does not change the published v6 configuration.


## 第一批生产核证与第二批探测

- PR #118 合并 main：`3e3d4b1360814ddc4b2b2cdcc9d122b7b1bbb299`。
- 主干 CI `35557080244`、部署 `35557544359` 均成功。
- serverA 实际镜像匹配；`docker top` 仅一个 Python 业务进程；readyz 200，mTLS 探针通过。
- 生成、评测仍开启，v2 写入关闭；两家专用凭据存在，旧通用密钥未注入。GitHub 旧 Secret 暂未删除。
- 发布指针一条，部署前后摘要均为 `3c82153cbf8e025f25c5f86ef637e3115608065d250f769810183e4349c8eb31`。
- 初轮六次合成探测中，智谱两个生成用途因 disabled 思考配置被拒绝；明确改为 enabled/low 后另做两次参数校正探测，均成功。没有自动重试或供应商切换。
- 四模型六声明用途组合均有连通证据；`glm-5.3-flash` 精确 API ID 已获实际响应验证。该结论不代表最大参数边界、业务质量、完整评测或人工批准。
- 脱敏原始探测结果见 [连通证据](evidence/multi-provider-connectivity-20260921.json)，包括失败记录；不含正文、凭据或推理内容。

下一批必须把智谱 enabled/low 要求进入参数能力与默认值，补齐观测回执及页面接入，再进行完整评测。不得将本次合成成功直接计作 M3/M4 完成。

## Governance wiring batch (in review)

- Production catalog now declares the four requested model IDs and purpose-specific defaults.
  `verified` denotes connectivity evidence only; it does not denote quality approval.
  v2 writes remain disabled until the compatible editor is deployed.
- Save and new admission enforce the same model-specific thinking/sampling constraints.
  The GLM entries expose enabled thinking / low effort only; unverified sampling remains unavailable.
- v2 dispatch receipts retain provider, requested model, adapter, original binding and route fingerprint.
  v1 model-call JSON omits the new identity field. Missing evaluation token usage stays unknown.
- Operating editor integration is on `codex/multi-provider-ui`; explicit defaults, purpose filtering,
  capability revision protection and model identity differences are included.
- Local validation: backend non-integration suite 1279 passed, 1 existing skip; Ruff and mypy passed.
  Production matrix has not started. MySQL/interop CI and deployed UI acceptance remain mandatory.

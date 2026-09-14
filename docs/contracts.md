# 接口、授权与模型契约


最新责任边界见 [迁移决定](migration-boundary.md)：首版从 QS 业务入口发起，qs-ai 持有 AI 生命周期并可靠回传；早期直接用户入口/认证代理假设不再作为本批实现方向。
状态：M5 已移除初期独立 HTTP 会话路由，HTTP 只保留健康接口。正式业务使用 mTLS gRPC，见 [运行契约](../integrations/workflow/README.md)；其余下文保留早期设计参考，不代表现行接口。

## 接口原则

FastAPI 提供版本化业务 HTTP 接口，通过现有产品网关接入。API 接收业务命令，不暴露 LangGraph thread/checkpoint ID。前端 JSON 中的外部 uint64 ID 用字符串；内部 gRPC 按 proto 定义。

模型调用异步执行：开始、回答、重试返回 202 和业务资源标识；查询返回持久状态，首次上线用轮询，SSE 后续按客户端兼容性增加。用户断开连接不删除已受理任务。

## 历史 HTTP 草案（已退役，不注册）

| 方法与路径 | 用例 | 关键要求 |
| --- | --- | --- |
| POST /v1/interpretation-sessions | CreateSession | testee_id、goal、选中 assessment_ids；201 |
| GET /v1/interpretation-sessions | ListSessions | 授权过滤、游标分页、Testee 筛选 |
| GET /v1/interpretation-sessions/{id} | GetSession | 状态、version、当前问题、安全错误和成果引用 |
| POST /v1/interpretation-sessions/{id}/runs | StartInterpretation | expected_version；202，仅 created 接受 |
| POST /v1/interpretation-sessions/{id}/answers | SubmitAnswer | question_id、expected_version、answer 或 skip；202 |
| POST /v1/interpretation-sessions/{id}/cancel | CancelSession | expected_version；撤销推进权，不承诺取消外部费用 |
| POST /v1/interpretation-sessions/{id}/retry | RetryExecution | 仅 blocked 且可恢复；unknown 需独立对账/授权决策 |
| GET /v1/interpretation-sessions/{id}/messages | ListMessages | 游标分页，不返回隐含模型推理 |
| GET /v1/interpretation-sessions/{id}/artifact | GetArtifact | 已接受成果、来源状态、版本、限制 |

早期五个端点已随 M5 删除，访问返回正常 404。上表中的列表、消息、重试、成果草案从未作为独立 HTTP 入口上线。

所有变更请求要求 Idempotency-Key。作用域由组织、操作者、操作及资源组成，存请求规范化哈希。相同键/同内容返回原回执，相同键/不同内容返回 409。expected_version 不匹配返回 409；不能自动把旧回答绑定到新问题。临时 5xx 不声称命令未提交，客户端用原键重试/查询。

错误格式固定 code、safe_message、retryable、trace_id；权限失败 401/403，防枚举查询策略可统一 404，状态/幂等冲突 409，语义参数错误 422，容量 429，依赖不可用 503。具体策略在机器契约中保持一致。认证标识和组织不从 body 信任，body 的 testee_id 只是被请求访问的资源。

## qs-server 事实端口

当前源码已有 ParticipantReportService.GetAssessmentReport/ListMyReports，但显示 DTO 不是完整的 AI 证据契约。现有线索见 [gRPC 接入说明](../integrations/qs_server/README.md)。

应用端口命名建议 EvidenceReader；远程候选 RPC 为 BatchGetInterpretationEvidence，**是待新增提案，不是已存在接口**。

请求：显式 assessment_ids、有界数量、目标 audience/用途；组织/调用者通过可信身份上下文传递。响应逐项包含：assessment_id、testee_id、不可变 report_id、outcome_id、报告/模型/计分/常模版本、测评时间与报告时间、填写者身份/角色（权限允许且数据实际存在时）、标准总体/维度事实、fact_ref、schema_version、fingerprint、来源状态。

首版所选报告采用全量成功语义；一项不可访问/不一致则整个解读准备失败，不悄悄丢掉某份报告。可选背景缺失用明确 missing 标识，不由模型补齐。查询当前报告与按精确 report_id 读取历史事实要分开，避免恢复时重绑新报告。

proto 由 qs-server 一处维护。当前报告协议已按 `17344553f6d134dd5f00145ddf635ececb914af6` 固定，来源哈希见 `integrations/qs_server/manifest.json`，生成器支持 `--check` 检测漂移。这只是现有显示 RPC 的传输验证；不能把缺少 report_id/source_version 的响应映射成合格 EvidenceSet。qs-ai 的防腐层将 protobuf 映射到自身 Evidence DTO，避免上游协议字段扩散到领域。Channel 使用 TLS、deadline、取消传播；只对语义安全的读取应用有界重试。

## 身份与可见性

认证依赖现有身份体系；服务身份和委托操作者是两种语义，不可相互替代。qs-server 决定报告权限，qs-ai 决定会话、回答、记忆与成果权限。未确认现有委托协议前不自创兼容 token。

首版默认 owner 私有，组织管理员也不自动获得全部正文。授权共享为后续明确功能；同一 Testee 的父母/老师不能默认互看自述。每次请求和恢复校验关系有效性，删除/撤权后不经缓存绕过。日志追踪不包含 token。

## 模型端口和调用配置

应用层定义 GapAnalyzer、InterpretationGenerator 两个语义端口，输入为受控事实和任务上下文，输出为候选 DTO；不直接暴露 ChatOpenAI、ChatDeepSeek 或 LangChain Message。

配置以 release_id 固定 provider、model/version、endpoint/protocol、推理配置、结构化模式、超时、输出上限、Prompt/Schema 版本和适用范围。不把三家简单当作替换 base_url；响应解析、工具调用和思考模式差异由适配处理。模型的具体能力必须在选定路线实测。

统一核心 Prompt 定义目标、事实边界、允许推断、引用和输出；有证据的模型特定补充单独版本化。首版一个默认路线，其他路线做对照评测，不在超时后静默换模型。

| 输出类型 | 最小内容 |
| --- | --- |
| GapDecision | sufficient、缺口类型、问题、提问理由、是否可跳过；不得要求无关信息 |
| InterpretationCandidate | summary、insights、suggestions、limitations；每项结论带 source_refs 与类型 |
| ValidationReceipt | schema、引用存在性、主体/版本、范围与业务策略校验结果；保留失败分类 |

约束输出需同时经过结构校验与领域策略；结构合法不证明事实正确。自由文本用户回答当作数据，不得覆盖系统规则、访问范围或工具权限。允许工具固定列表，参数再次做主体授权。

## 质量发布

Release manifest 将 Prompt、模型路线、输入输出 Schema、workflow_version、适用范围和评测集版本绑定。发布后不可原地修改，新版本只默认用于新会话。

回归样例至少包括：单报告、同类可比/不可比、观察者差异、跨类型冲突、上下文不足、跳过问题、纠正信息、无效引用、越权/注入、输出截断和未知结果。评测看事实引用、无依据推断、提问价值、结构符合率、中文可读性、时延/成本；阈值由 P2 基线确定，不虚构分数。

发布记录先采用仓库版本化 manifest + 可审阅评测报告，不先重建完整治理后台。旧 qs-server Profile/Artifact 不直接导入为新契约的合格数据；已有成果在旧服务保持可读，新系统通过显式标识区分来源。

关联阅读：[执行](runtime.md)、[模型与数据](domain-data.md)、[计划](roadmap.md)。

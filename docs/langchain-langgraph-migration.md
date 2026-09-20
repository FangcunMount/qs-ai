# qs-ai LangChain + LangGraph 接入计划

状态：目标已激活；完成初步代码排查与设计；已在独立 worktree 安装并锁定框架依赖，尚未切换业务实现。
基线：2026-09-21，qs-ai 本地及远端 main 均为 451e88ee1e62ebfdaed5a284f4fc631965a2300c，工作区干净。

## P. 代码分析报告

### 范围与调用链

生成：GenerationProvider → PublishedReportWorkflow → ReportWorkflow → DurableGeneration → DeepSeekResponses。ExecuteNext 在工作流外负责领取、续租、两次授权检查及结果提交；结果投递独立保留。

评测：EvaluationProvider → EvaluationWorker → 已有准备、dispatch、模型调用、完成与恢复逻辑。evaluation_checkpoints 及生成/语义 completion 是已有恢复依据。

已检查文件：bootstrap/providers/{generation,evaluation}.py、application/execution/{configuration,report_workflow,generation,worker}.py、infrastructure/qs_server/{responses,deepseek_request}.py，以及 tests/integration/{test_generation,test_report_workflow,test_evaluation_recovery}.py。

### 已确认事实与边界

- 当前依赖未包含 LangChain 或 LangGraph。
- ModelGateway 已提供模型适配接缝，但生成与评测分别装配 HTTP 客户端。
- 当前 DeepSeekResponses 明确只接受 provider=deepseek、protocol=responses；保持 instructions/developer/user 消息结构、Schema 投影、参数省略规则和响应规范化。不能依据类名直接采用 ChatDeepSeek。
- DurableGeneration 先记录调用，再请求模型，最后保存回执；dispatched/unknown 不允许因恢复而重发。
- 已接单任务读固定发布配置；执行与提交前都检查当前授权。
- 测试代码已覆盖取消后恢复、提交失败、未知结果和原配置复用。本次只检查测试内容，未声称运行通过。

### 判断

入口与职责边界清晰；框架依赖与模型映射复杂度中等；替换持久恢复复杂度高，因此不纳入本轮。主要风险是 SDK 隐式重试、消息/Schema 转换、回执信息丢失、共享图状态与异步取消语义变化。

## 设计决策

1. LangChain 承担真实模型适配或模型抽象，LangGraph 承担真实执行路径的阶段编排。仅安装依赖、包装旧调用或增加演示图不算完成。
2. 继续一个服务、容器、Python 进程；保留 Dishka、MySQL、gRPC、配置中心与日志设施。
3. 领域及应用端口不依赖框架类型。框架适配器放在基础设施层，由 bootstrap 装配；复用现有 Workflow、ModelGateway，确有第二个实现需要时才新增工厂接缝。
4. 生成图包含准备/输入校验、持久生成、输出校验/成果构造。授权、任务租约、终态提交及投递仍由既有应用流程负责。节点不直接建立第二套业务状态机。
5. 评测图以一次受控评测执行单元为边界，复用现有准备/dispatch/生成或语义评测/提交操作。不得把全部案例和候选并发搬进图，突破既有容量、预算和检查点 CAS。
6. 本轮不启用 LangGraph 独立持久 checkpointer；重启后由既有任务与回执驱动重新进入图。不是节点级图恢复，不宣称完成 interrupt、time travel 或长期记忆。
7. 图定义可以共享；每次执行状态、请求作用域和数据库会话必须隔离。数据库会话、客户端和密钥不得进入可序列化图状态。
8. 现有业务版本和资产指纹不因引入框架改写。图实现标识进入部署与日志证据；只有确需改变持久执行语义时，另行设计版本迁移。
9. 不开启 LangSmith 外发 tracing；不需要注册账号。保留结构化日志与关联 ID，节点日志只记录允许字段，不输出 Prompt/报告/原始响应。

## M0：协议可行性与验收基线

任务：专属 codex 分支/worktree；固定依赖版本；核查官方模型适配器与实际 Responses 契约；建立请求、响应、错误与调用次数对照测试。

矩阵包括：消息角色及顺序、Schema/省略字段、模型与 token/reasoning 参数、timeout、usage、response ID、拒绝/截断/畸形 JSON/重复键/超大响应、401/403/429/408/5xx、连接失败/读超时/取消。

验收：对照结果明确，不能仅证明 happy path。自动重试必须禁用或证明与现有安全规则完全一致；不能因适配器方便而修改已发布路线。若官方适配器无法满足契约，明确缺口并选择受测试保护的自定义 LangChain 模型实现，不静默降级。

## M1：统一模型适配

任务：在现有模型端口后接入 LangChain 模型；生成与语义评测共用适配能力，保持各自作用域与调用账本；保留原始回执和严格校验责任。

验收：M0 矩阵通过；每次授权 dispatch 最多一次网络发送；未知结果不重发；冻结配置、调用 ID、响应模型校验、大小限制、超时和取消行为不变；日志无敏感数据。原生 SDK 若丢失必要原始信息，必须在切换前解决。

## M2：生成流程图化

任务：实现异步 StateGraph 并接入实际 Workflow；保留已有准备和输出验证服务，消除切换后重复的顺序编排。

验收：真实执行路径确实经过图；成功/输入错误/输出错误/前后撤权均与原行为一致；恢复复用回执；提交失败不导致再次发送；并发任务状态不串扰；停机取消与租约丢失仍有效；HTTP/gRPC 不被同步节点阻塞。

## M3：评测执行接入

任务：将评测生成与语义步骤接入图执行，保留准入、配额、CAS、预算、人工审核与发布逻辑。

验收：完整案例及候选义务不变；原 run/checkpoint/回执可恢复；准备未发送可安全释放，已发送未知必须阻断；取消、预算和重复 worker 竞争不重复 dispatch；配置替换不影响已有评测；人工审核与发布不自动代签。

## M4：回归、发布及清理

任务：删除无调用的旧编排/适配实现及临时切换接线；保留仍有职责的解析与安全校验。完成依赖、文档、镜像、部署核证和回滚演练。

验收：适用 MySQL 8.0/8.4、Go→Python、发布快照、配额/恢复/幂等回归通过；镜像实际包含锁定依赖；生产仍单容器单进程；健康/mTLS 正常；受控生成和评测走新框架，结果回传正确；发布前后任务恢复及兼容回滚有证据。不以 CI 代替生产业务验证。

## 发布与执行纪律

- M0 为阻断性协议门槛，先通过再切换供应商调用。
- M1/M2 可合为第一批生产发布；M3 为第二批；M4 汇总核证及清理。每批只在形成完整、可回滚的运行路径后发布。
- 旧版本回滚依靠保留镜像与配置，不长期保留两个可写执行器。切换使用现有排空机制，避免新旧 worker 重叠。
- 默认不新增数据库迁移；发现确有持久字段需要时，先记录兼容策略与增量迁移，不破坏历史记录。
- 基线检查一次，各批运行相关测试；最终及依赖/公共契约变更才运行适用广泛检查。记录提交和结果，未受影响且已通过的检查不重复执行。
- 生产模型调用优先复用现有配置和凭据，执行前确认复用方式与测试对象；不在聊天收集密钥。涉及有权限人员的人工审核保留人工确认。
- 不修改 Operating 页面、QS 协议，不新增供应商、主动追问、多报告、长期记忆或新配置产品。
- 既有日志 infra 接入是单列进行项，不将其未完成部分计入本目标成果。

## 结项与台账

M0：设计完成；现有模型协议基线 57 项通过，安装框架后同组 57 项再次通过。锁文件固定 langchain 1.4.2、langchain-core 1.6.3、langgraph 1.2.11。真实框架适配对照实验尚未完成，不能将现有测试通过视为新适配器已验收。
M1–M4：待实施。

### M0 协议保护补充

新增 403/408/500、重复 JSON 键、非 JSON 数值、非法编码、连接/读写超时、传输错误、在途取消保护；相关测试现为 70 项，通过，ruff 与 diff 检查通过。仅使用 MockTransport，不调用真实供应商。

官方集成资料核对：ChatDeepSeek 面向 chat completions；ChatOpenAI 面向官方 OpenAI 协议，不能据此假定保存第三方全部响应信息。当前严格重复键检查及响应大小限制需要保留原始 HTTP 字节，因此优先验证自定义 BaseChatModel，将现有协议发送与解析作为模型实现本身，而非额外包装旧网关。LangChain 消息/输出转换、异步调用及 tracing 隔离仍须单独验证。

资料：https://docs.langchain.com/oss/python/integrations/chat/deepseek ，https://docs.langchain.com/oss/python/integrations/chat/openai 。

完成定义：LangChain 与 LangGraph 均在生成和评测实际路径中发挥上述职责；安全与恢复契约不退化；代码合并、生产发布、现场核证完成；旧替代实现清理。没有固定观察等待期。页面新增能力和图持久化不属于本轮完成定义。

每批记录：代码 SHA、适用测试、CI、镜像 SHA、部署、业务证据、遗留项与风险，分别标记，不相互替代。

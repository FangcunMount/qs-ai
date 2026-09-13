# P. 代码分析报告

## 分析目标

M1.1 架构梳理：确定真实 AI 解读迁移边界和首个案例接入缺口，服务于 [M1–M5 计划](migration-milestones.md)。本次未修改 QS 业务代码，未验收 M1。

## 分析范围

2026-09-11：QS 专属 worktree `qs-server-ai-report-snapshot`，分支 `codex/ai-report-snapshot`，提交 e567a7cd，工作区干净；AI 草稿提交 226801dc。QS 主工作区存在其他任务的 Testee 改动，本任务没有触碰。以下 QS 路径均相对于该仓库根目录。生产发布配置和实际启用的 Profile 尚需只读核对，不能据源码目录推断生产启用清单。

## 入口与调用路径

- 旧产品入口：`internal/collection-server/transport/rest/handler/ai_explanation_handler.go` 的 Capability / Request / Get / Export → QS AI participant 服务 → source/input/Profile/Prompt → execution → Provider → validation/safety → persistence → Artifact / 展示。
- 运行绑定：`internal/apiserver/container/modules/interpretation/assemble.go` 装配 Prompt catalog、route catalog、Provider、safety gate、executor、仓储及管理服务。旧执行仍有 worker 的 AIExplanationAutomationClient 调用，不能仅改 REST 入口便删除执行 RPC。
- 草稿新入口：RequestWorkflow → `application/aibridge/Participant.Request` → 访问授权 → ResolveCurrent → 报告主体/版本绑定 → MySQL 发起持久化 → AI gRPC → 快照和任务持久化。
- AI 草稿 ExecuteNext 对 `qs-snapshot-v1` 直接使用已存证据，并跳过前后两次 source.authorize。这是已读代码事实，不能宣称执行时撤权已经验证。

## 当前职责与依赖

| 编号 | QS 代码证据 | 当前责任 | 目标归属 / 保留约束 | 验收关联 |
| --- | --- | --- | --- | --- |
| C01 | collection handler；application/interpretation/aiexplanation/participant/service.go | 能力、发起、查询、来源状态和展示 | QS 保留入口/权限/投影，替换生成依赖 | M2.4、M4、M5 |
| C02 | application/interpretation/aiexplanation/source；domain/interpretation/report | 当前标准报告、Outcome 一致性 | QS 保留中立事实与版本，不迁评分 | M1.3–4 |
| C03 | application/interpretation/aiexplanation/input/assembler.go | 按 Profile 策略组装输入，生成 ProviderPayload | AI 接管 AI 组装；QS 提供报告/Outcome/模型必要事实 | M1.5、M2.1 |
| C04 | infra/aiexplanation/prompt/catalog.go；application/.../prompt/render.go | 不可变 Prompt 包、占位符渲染和指纹 | AI 接管；保留指令/数据隔离与版本匹配 | M1.5、M3 |
| C05 | infra/aiexplanation/responsesapi；execution/executor.go | 冻结模型路线、结构化请求、调用回执、错误分类 | AI 接管，不把旧供应商适配等同于通用接口 | M2.1、M4 |
| C06 | application/.../validation；assemble.go 的 safetyGate | 字段/事实/策略与安全校验 | AI 接管；QS 保留接收契约校验 | M2.1 |
| C07 | application/.../execution、recovery、persistence | 生成/运行、租约、恢复、原子成果提交 | AI 接管；QS 在途旧任务先排空 | M2.2–3、M5 |
| C08 | application/.../governance、administration、evaluation | Profile 发布、评测审批、预算、语义复核、失败恢复 | AI 接管，管理操作者权限保留既有信任链 | M3、M4 |
| C09 | infra/mongo/interpretation/aiexplanation | Profile/Generation/Run/Artifact/评测及容量存储 | AI 新数据归 MySQL；旧 Mongo 历史保留读取或显式迁移 | M3.5、M5.4 |
| C10 | application/.../subjectexport；participant Get/Export | 主体成果导出、历史内容访问 | AI 成果权威；QS 授权入口与历史兼容保留 | M4、M5 |
| C11 | internal/worker/container；infra/grpcclient/AIExplanationAutomationClient | 旧生成/评测事件消费及 RPC | 替代验证后仅退役 AI 专属绑定，保留共享 worker/MQ | M4、M5.3 |
| C12 | application/aibridge；infra/mysql/aibridge/store.go | QS 发起与接收、幂等、请求主体绑定 | QS 长期保留；完整成果契约仍待补齐 | M1.3、M2.3–4 |
| C13 | options/ai_explanation.go；configs；assemble.go | 功能开关、模型配置和运行装配 | AI 专属配置迁出；桥接/业务开关留 QS | M3、M5 |

以上是代码责任族清单，不是完整生产资产清单。M1.1 仍需补齐具体在用类型、Profile/release、集合/事件/后台路由与专属 Secret 名称的逐项对应；不得据此直接批量删除目录。

2026-09-13：旧 AI 管理 REST 的 25 个实际注册入口已补充到 [M3 接管对照表](m3-management-parity.md)，逐项区分历史保留、有实现待验收及实质缺口。该表不替代上述生产类型、资产和共享依赖盘点。

## 行为 / 契约 / 不变量边界

- Prompt catalog 支持 `cross-dimension-participant-scale` v1–v6，实际使用版本由 Profile 决定。生产包是编译期 Go 内容，不在请求时解析 Markdown。不能未经查询就选择最高版本 v6。
- Render 校验包与 Profile 的 template/version 一致；只允许白名单变量；system/data preamble 禁止动态占位。发送给模型的数据只能含 context 与 facts，source/Profile/鉴权等不应泄露给模型。
- 旧 Input.Document 还包含报告来源、Outcome、模板/构建版本、Profile 身份；Facts 包含 runtime/model/overall_result/dimensions/standard_suggestions/model_result。草稿只序列化 report.Content 成 standard_report 字符串，不能据此认为已复用旧输入协议。
- 草稿 Participant.Request 在读事实前授权；重放只复用原版本，报告/Testee/组织不一致拒绝。需要保持这些边界，并补上长任务执行时的持久主体授权查询。
- 成果回传和生成重试必须分开；旧生成/Artifact 标识不得直接改名冒充新 Session。迁移后历史访问、source_state 等现有展示语义需回归。

## 测试与可观测性现状

已读取 `TestParticipantSnapshotAuthorizationAndSourceBinding`：覆盖发起时拒绝、错组织/主体、旧报告及重放不重复读取；它通过替身 access 验证发起阶段，并不覆盖排队后撤权。Prompt Render 测试覆盖指令数据隔离、未知占位/额外字段、身份不匹配。执行器含调用回执校验与生成指标；仍需用真实部署请求串联双方日志及版本证据。本轮未重跑这些测试，不把存在测试文件写成验证通过。

## 分析指标与判定

| 指标 | 判定 | 依据与触发标准 |
| --- | --- | --- |
| 入口清晰度 | 绿 | 已追到 handler、participant、assemble 和执行提交关键节点 |
| 行为边界 | 黄 | 旧输入结构明确，但新快照未证明信息与语义等价 |
| 测试保护 | 黄 | 发起阶段有断言，执行阶段撤权路径被绕开，保护不覆盖 M1 门槛 |
| 边界稳定性 | 黄 | source 与 AI input 紧邻但责任不同，整目录删除会误伤事实来源 |
| 变更放大 | 黄 | 管理、生成、存储、worker、展示多个入口需要按契约协调切换 |

## 主要风险点

1. 快照分支授权绕过违背 M1.4；下一步必须提供 QS 持久主体复核端口，并让冻结快照同样经过复核，测试排队后撤权及依赖故障不调用模型。
2. 模板文本复用不足以保持效果：必须一并冻结 Profile、实际模型路线、输入和校验策略；旧报告内容不能直接当 ProviderPayload。
3. 活跃类型和发布版本尚未知：仓库有 v6 不证明现网使用 v6，离线样本不能代替合法真实报告对照。
4. QS 主 worktree 正在其他改动中；后续合并要关注 Testee 授权/存储契约变化，禁止覆盖或混入。

## 初步发现或假设

已证实可复用资产包括不可变 Prompt catalog、Render 的占位变量和数据隔离规则、输入/输出 Schema 与对照样例。能否直接选择某一案例包还依赖生产 Profile/模型版本的只读核对。当前两个草稿应继续修正，而不是直接合并上线宣布事实链完成。

## 下一步建议

先补持久主体授权复核及对应的队列撤权测试；并只读导出现网启用配置的非敏感元数据，选定首个对照案例。完成这些证据后再迁入精确的 Prompt 包与输入约定。用户已授权实施，不需要额外请求通用重构许可。

# M3 旧管理入口接管对照

2026-09-13：M3 尚未完成。本表把已注册的旧 AI 管理入口逐项映射到新服务，区分代码可用、历史保留与尚缺等价能力，避免按新增 PR 数量判断接管进度。生产治理与执行仍按 M1/M2 前置验收开放；本表不是切换批准。

## 范围与证据

- QS 基线：`ac170ec3a93eacafa77583406bcadfe5bae3df9d`，`internal/apiserver/transport/rest/routes_interpretation.go`。已读取 v1/v2 的实际注册；旧管理共 25 个 method-path 操作。QS 原工作区不修改，取消代理在本任务独立 worktree 的 `codex/ai-evaluation-cancellation` 分支。
- AI 基线：`3db269b6ee98f5566b56ea6530c881630ba58f65`（PR #76 合并），`integrations/workflow/proto/workflow.proto`、`src/qs_ai/transport/grpc/` 及 `src/qs_ai/infrastructure/persistence/mysql/`。新 Cancel 已通过 PR CI，合并后 CI/部署另行跟踪。
- 容量/恢复语义核对：QS `application/interpretation/aiexplanation/administration/service.go` 的 `FindEvaluationCapacity`、`FindParticipantCapacity`、`RetryParticipantGeneration`。日配额和并发占用不是单个 Run 的最大调用次数；不能用 Prepare 返回的冻结执行预算冒充等价接管。
- 本表覆盖当前旧 AI 管理 REST 入口；不代替 [M1 责任清单](m1-inventory.md) 中测评类型、生产资产、数据集合、事件、专属进程和凭据的完整盘点。标准报告、Testee、业务权限及历史读取继续归 QS。

表内 v1 = `/internal/v1/interpretation/ai-explanation`，v2 = `/internal/v2/interpretation/ai-explanation`。新管理由 QS `/internal/v2/interpretation/ai-workflow` 授权代理到 AI；Operating 只负责界面。所有“有实现”均不等于实际管理闭环验收通过。

## 25 个旧入口及去向

| ID | 旧入口 | 责任去向 / 新实现 | 尚需关闭的差异 |
| --- | --- | --- | --- |
| P01 | v1 GET `/prompt-evaluations` | QS 保留旧 v1 历史列表 | 验证旧 ID、筛选、权限与历史数据持续可读；不要求把旧运行伪装成新 UUID |
| P02 | v1 GET `/prompt-evaluations/:run_id` | QS 保留旧 v1 历史详情 | 同上，隔离旧写实现后回归 |
| P03 | v1 GET `/prompt-evaluations/:run_id/attempts/:case_id/:attempt` | QS 保留旧执行历史 | 回归原始输出与失败证据读取，不因生成实现退役丢失 |
| P04 | v1 GET `/prompt-evaluations/:run_id/attempts/:case_id/:attempt/rechecks` | QS 保留旧复查历史列表 | 历史保留验收 |
| P05 | v1 GET `/prompt-evaluations/:run_id/attempts/:case_id/:attempt/rechecks/:recheck_id` | QS 保留旧复查详情 | 历史保留验收 |
| P06 | v1 GET `/profiles` | AI AssetCatalog.List + 草稿/发布生命周期；旧版本仍由 QS 读 | 不可变资产列表不等于旧 draft/published/disabled 状态筛选；补做列表语义与实际在用版本映射 |
| P07 | v1 GET `/profiles/:profile_id/versions/:version` | AI AssetCatalog.Get；旧版本读取保留 | 旧 ID/版本与新内容指纹逐条对账 |
| P08 | v1 GET `/prompt-evaluation-capacity` | AI 应承接评测容量与准入状态 | 未找到等价管理 RPC；需核对组织日预算预留、剩余完整运行次数、活跃 Run 限制及查询权限 |
| P09 | v1 GET `/participant-capacity` | AI 应承接生成执行容量；QS 保留业务权限 | 未找到等价管理 RPC；需核对机构/用户/测评日预算及机构活动执行占用，不能只显示进程并发配置 |
| P10 | v1 POST `/generations/:generation_id/retry` | AI 承接受控恢复，QS 只发授权命令 | 评测 ResolveUnknown 不覆盖参与者 Generation；需原尝试、命令幂等、风险确认、费用预留与历史 ID 关联的完整映射 |
| P11 | v1 POST `/profiles` | AI PromptDraft Create/Revise/Freeze + Profile Register | 有组成能力；验收完整编辑/校验/注册流程，明确原 Profile 草稿语义映射 |
| P12 | v1 POST `/profiles/:profile_id/versions/:version/publish` | AI PublicationManagement.Publish | 有实现；实际质量门槛→发布→新生成版本绑定验收及旧写关闭 |
| P13 | v1 POST `/profiles/:profile_id/versions/:version/disable` | AI PublicationManagement.Disable | 有实现；核对发布 selector 映射、停用对在途及新任务的行为 |
| P14 | v2 GET `/prompt-evaluations` | AI 原生评测列表；QS 保留旧 v2 列表 | AI List 首批 CI 已通过；QS 授权代理已提交，Go→Python→MySQL 的分页摘要、详情及权限隔离联测通过。两端最新增量 CI/发布与 Operating 列表/选择入口仍待完成，不以接口联测代替接管 |
| P15 | v2 GET `/prompt-evaluations/:run_id` | AI EvaluationManagement.Get | 有实现；原始创建、审核、复审、未知处置、取消回执已逐步接入；旧 ID 读取保留 |
| P16 | v2 GET `/prompt-evaluations/:run_id/candidates/:candidate_id` | AI ListCandidates/GetCandidate | 有实现；Cancel 已修复废弃复审后详情误拒绝；仍需真实管理读取验收 |
| P17 | v2 GET `/prompt-evaluations/:run_id/executions/:execution_id/output` | AI 运行执行证据查询 | 候选详情只提供已接受生成/语义输出；未知列表只提供元数据。失败/未形成候选的 execution 输出和诊断不能视为已接管 |
| P18 | v2 POST `/prompt-evaluations` | AI Prepare→Create→Start | 有实现；核对容量准入、费用确认及实际启动闭环，不能只验证固定引用 |
| P19 | v2 POST `/prompt-evaluations/:run_id/cancel` | AI Cancel + QS 管理代理 + Operating | AI PR #76 与 QS #104 已发布；Operating #33 已实现并通过 CI，等待依赖发布。普通取消/待审核废弃保持区分 |
| P20 | v2 POST `/legacy-prompt-evaluations/:run_id/attempts/:case_id/:attempt/rechecks` | 新复查执行迁至 AI，旧结果/复查历史在 QS 读 | 不能长期保留 QS 旧模型调用写入口；需旧证据导入或明确映射为新 AI 运行，保留前后来源并验收 |
| P21 | v2 POST `/prompt-evaluations/:run_id/reviews` | AI Review（单项） | 有实现；权限、原文核对、双职责签名的实际操作验收 |
| P22 | v2 POST `/prompt-evaluations/:run_id/reviews/batch` | AI Review（批次） | 有实现；批次原子性已测，实际管理及旧写关闭仍待验收 |
| P23 | v2 POST `/prompt-evaluations/:run_id/finalize` | AI PreviewGates/Finalize | 有实现；真实审批拒绝/通过与发布引用验证 |
| P24 | v2 POST `/prompt-evaluations/:run_id/reopen-review` | AI ReopenReview | 有实现；历史保留与复核后重新审核已有测试，实际操作验收待补 |
| P25 | v2 POST `/prompt-evaluations/:run_id/result-unknown/resolve` | AI ListUnknownExecutions/ResolveUnknown | AI/QS/Operating 均有实现；实际账号权限及供应商未知结果恢复仍未验收 |

## 实际使用证据

2026-09-13 06:52 UTC 已补充 [生产记录核对](m3-production-usage.md)：唯一已发布 v6 关联一个经复审批准的 v2 评测；审核、复审、未知结果处置、复查和预算均有存量证据。25 个路由用于逐项归属，不要求原样复制 25 套新入口；先验证在用 v6 闭环，再补必要行为与减少三层重复规则。0 次观测不作为直接删除依据。

## 按关闭条件收敛

### 集中交付方式（2026-09-13 用户调整）

剩余 M3 能力集中实现、统一联测后提交关联审核，不再按单个接口创建、合并和发布一轮 PR。已开放的草稿继续承载本批工作；每个仓库形成一份最终范围明确的审核，必要时合并已有分支差异，保留源提交与无关工作。现有生产服务保持当前版本，不因局部查询实现完成而提前切换。

| 工作包 | 必须覆盖 | 实现取舍与交付证据 |
| --- | --- | --- |
| A 查询与操作 | P06/P07/P14/P17/P19 | 复用现有资产、任务、调用记录及取消事务；补状态/旧版本映射、任务目录、失败执行诊断与页面接入。列表进入现有详情，不新建一套管理工作台。 |
| B 准入与恢复 | P08/P09/P10/P18 | qs-ai 统一持久预算、活动占用和受控恢复；复用冻结版本/调用记录/原回执。QS 负责当前授权及转发，Operating 展示容量和显式操作，不重算配额与状态机。 |
| C 旧记录衔接与接管 | P20 及配置对账/旧写切换 | QS 保留旧证据查询；新复查通过明确来源映射由 AI 执行，旧草稿/ID 可追溯。形成配置对账与逐项旧写切换方案；不删除历史、不自动批准旧任务。 |

集中审核包包括：三端最终契约、必需数据库迁移、相关功能与故障回归、完整 CI、在用 v6 配置对照及剩余业务验收清单。小步提交可保留，完整回归、PR 审核和部署按整批组织，不为已稳定部分重复全量发布。

本调整改变交付节奏，不削减原有能力或验收门槛。代码及隔离联测完成可提交审核；M1/M2 真实案例和模型闭环、M3 真实管理操作、完整生产配置对账及旧写关闭仍分别验收。统一审核前不扩展 Prompt 可视化、多报告/记忆或通用控制器重构。

1. **查询与操作完整性**：集中处理 P06/P14/P17/P19。验收可从列表找到任务，读取各阶段原执行/失败诊断，普通取消或明确废弃后仍能审计原输出；旧 ID 与新 UUID 的入口清楚区分。
2. **准入与恢复完整性**：集中核对 P08/P09/P10/P18/P20 的实际生产政策、存储与调用链，再实现尚缺的等价行为。组织日配额、用户/测评预算、并发预留和未知调用风险属于原有能力，不能作为新增产品排除。配额数值复用当前 QS 配置，不自行提高限额。
3. **真实接管验收**：前置 M1/M2 真实报告、授权/撤权、模型结果和可靠回传通过后，从 Operating 完成“修改→评测→审核→发布→生成→回退”，附实际版本与脱敏证据。对全部生产在用配置完成数量、版本、引用和来源对账。
4. **关闭旧写权威**：逐行确认写入口的替代/映射和回退方案，关闭旧准入及模型执行写路径，再删除无调用者实现。历史只读、标准报告和业务权限保留；不自动清理历史数据或共享凭据。

取消功能交付后不宣称 M3 只剩“验收手续”；上述实质差异全部关闭且生产证据满足 [里程碑门槛](migration-milestones.md) 后，才验收 M3。后续 Prompt 可视化串联、主动提问、多报告及长期记忆不计入本表。

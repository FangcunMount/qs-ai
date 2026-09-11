# P. 代码分析报告

## 分析目标

M3 发布管理迁移前的架构梳理：确定当前发布行为与首批实现边界。结论：必须迁移 Profile、评测证据及发布选择约束；仅提供 Prompt 文件上传不能替代现有管理能力。M1/M2 尚未验收，本报告不授权提前切换生产管理权威。

## 分析范围

2026-09-12，QS 本任务独立 worktree，`codex/ai-workflow-read` / `040052084`；qs-ai `b59bc68`（恢复测试 PR #10 已合并）。下列 QS 路径相对 qs-server 仓库；AI 路径相对本仓库。范围为 Profile 草稿/发布/禁用及其关键依赖，不冒充 V2 评测全流程、全部前端页面或生产资产对账已分析完毕。

## 入口与调用路径

1. QS `internal/apiserver/transport/rest/routes_interpretation.go`：v1 profiles 的 list/get/create/publish/disable；读取要求 audit-interpretation，写入另要求 org-admin。V2 是唯一可写 Prompt 评测入口，v1 留历史读取。
2. `transport/rest/handler/ai_explanation_administration.go::PublishProfile` 从已有身份上下文取得 actor，从路径取得 Profile/version，从请求取得 evaluation_run_id/reason，交给 administration。
3. `application/interpretation/aiexplanation/administration/service.go::PublishProfile` 校验参数和审计原因，调用 `AuthorizeGovernance` 后转 governance。
4. `application/interpretation/aiexplanation/governance/service.go::Publish`：读取草稿、读取 V2 evidence、核对批准/门槛/未决结果/完成时间、核对精确 release 绑定、检查 selector 冲突、发布并保存。
5. `infra/mongo/interpretation/aiexplanation/repositories.go::ProfileRepository.Save`：状态与指纹条件更新；数据库唯一索引保护已发布 selector 槽位。
6. `domain/interpretation/aiexplanation/profile/profile.go::Resolve`：仅解析 published，以 selector specificity 2→0 选择；同级多候选报错。装配位于 `container/modules/interpretation/assemble.go::assembleAIExplanation`。

## 当前职责与依赖

| 现有职责 | 当前证据 | 迁入约束 |
| --- | --- | --- |
| 不可变 Profile 定义及指纹 | NewDraftForRelease、CreateDraft、ProfileRepository.Save | MySQL 保存原定义与原指纹；不能因 JSON 重排破坏历史引用 |
| 发布审批凭证 | governance.Publish / validateReleaseMatch | AI 自己读取可信证据记录；不能信任请求体传入 approved=true |
| 生效版本选择 | Resolve、已发布 selector 唯一索引 | AI 负责选择，数据库防并发双生效；QS 不再独立选第二套 Profile |
| 管理身份与业务权限 | administration + REST capability middleware | QS 保留已有身份/权限边界，AI 验证可信服务及受约束的操作者声明，不能接收任意 actor 字符串 |
| 模型/Prompt/schema 包 | assemble 中 prompt/schema/route catalogs | 形成不可变 release，密钥始终与业务包分离 |
| 历史评测与成果 | v1 read routes、Profile 发布 evidence_run_id | 保留来源标识及历史读取，不伪造为新 AI 运行 |

qs-ai 当前 `src/qs_ai/bootstrap/providers/generation.py::GenerationProvider.workflow` 通过 `load_migrated_release`、`load_prompt` 加载固定包；`application/interpretation/release.py::ExplanationRelease` 是不可变执行输入，没有持久发布状态机或管理接口。这是迁移缺口，不是现有 QS 发布功能已退役的证据。

## 行为 / 契约 / 不变量边界

- 发布只允许 draft。证据必须是 V2、approved、gate passed、unresolved_result_unknown_count=0，且已 finalized。
- Profile ID/version/fingerprint、Prompt ID/version、输入/输出 schema 版本与 generation route ID 均须匹配证据。
- 创建草稿重新计算指纹，与 expected_fingerprint 不一致拒绝。创建人、原因、发布人、原因和证据 ID 均有记录。
- 数据库 `(profile_id, version)` 唯一；发布槽位受唯一索引约束。应用层先查询不替代并发保护。
- 原生命周期 draft→published→disabled；当前所读路径没有重新启用 disabled 的方法。新项目“切回上一版本”必须显式设计激活记录与并发规则，不能偷偷修改旧内容或直接把状态改回 published。
- 重启恢复必须沿用生成时冻结的 release；管理端发布新版本不能改变已持久模型请求。现有恢复测试保护这一点。

## 测试与可观测性现状

本轮运行 QS governance 与 domain/profile 两个包测试通过。已读取 TestPublishRejectsIncompleteOrMismatchedEvidence：未评审证据、V1 冒充、release 不匹配均被拒绝，前两者断言没有保存；另有 selector specificity 与冲突测试。数据库索引和条件更新已读源码，本轮未做 Mongo 并发发布实测。

AI PR #10 精确头 `a9b35e5f562d823e5e510bdb7bb3ec983cfd8459` 的 CI `34621971297` 在 MySQL 8.0.36/8.4 与镜像检查通过，已合并。它证明执行恢复保护，不能替代发布管理验收。

## 分析指标与判定

| 指标 | 判定 | 证据及标准 |
| --- | --- | --- |
| 入口清晰度 | 绿 | 已追到路由、身份、应用、领域、数据库、运行选择 |
| 行为边界 | 黄 | 发布条件明确；回退激活及完整 V2 评测迁移尚需设计和逐条对齐 |
| 状态保护 | 绿 | 现有存储有前置状态/指纹匹配及唯一发布槽位，迁移必须保留 |
| 测试保护 | 黄 | 发布主路径拒绝分支通过；尚无 AI MySQL 发布/并发/审批集成测试 |
| 变更范围 | 黄 | 涉及管理入口、权限转发、评测证据、版本存储、运行选择，不能单点切换 |

## 主要风险点

1. 将“冻结包可执行”误当“发布流程迁完”，会绕过现有质量门槛。
2. 用新上传的布尔状态代替评测证据，或接受伪造 actor，会削弱既有发布权限。
3. MySQL 只做先查后写，将失去旧 Mongo 唯一槽位的并发保护。
4. 按 disabled→published 原地回退，缺少激活审计和运行引用隔离；必须有独立验收。

## 初步发现或假设

已证实：Profile 发布是完整 release 与审批证据的绑定，不是 Prompt 模板文本发布。固定 v6 包可作为迁移基准，但不能推断其他生产资产已全部盘点。V2 管理动作还包括 cancel、单条/批量 reviews、finalize、reopen、result-unknown resolve 与 legacy recheck；本次只确认路由存在，后续必须逐条读取其实现与前端消费者。

## 下一步建议

首批实现只建立 AI 端不可变发布资产与 MySQL 存储保护，并进行 QS 原定义/指纹往返契约测试；暂不暴露可绕过 evidence 的 publish 接口。随后按以下依赖交付：

1. **资产与版本**：原 Profile/Prompt/route/schema 导入和版本对账，重复导入幂等、同版本异内容冲突、历史版本读取。
2. **可信证据与发布**：迁入 V2 evidence 格式、发布 gate 和 selector 原子激活/审计。双发布竞态、重复命令、未批准、未知调用、错误 release 均有 MySQL 集成测试。
3. **管理入口**：QS 保留权限并转发，原页面路径兼容；操作者冒充/越权拒绝与正确读写贯通。
4. **评测运行与审核**：逐条迁入 V2 路由对应行为、预算/受控重试/复核/结果未知处理，保留 V1 历史查询。
5. **管理切流验收**：实际入口完成草稿→评测→批准→发布→生成→回退；停止 QS 同类写入，核对无双权威。此步须遵守 M1→M2→M3 验收次序。

前四项可做代码与隔离环境验证；第五项不能以测试替身或静态文档通过替代。

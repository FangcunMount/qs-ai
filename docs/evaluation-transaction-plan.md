# 评测 Run 联合事务

当前已有检查点 CAS、冻结策略、失败分类和生成清单；尚无完整评测 Run 仓库。下一批应以 Run 为一致性边界，在一笔数据库事务中更新执行流水、候选/回执、预算状态与检查点，禁止先提交检查点再写证据。

## 已落实的事务能力

`save_checkpoint(session, state, expected_version)` 接受外层 AsyncSession，只执行版本条件更新，不自行 commit。原 `MySQLCheckpoints.save` 为单操作包装。隔离 MySQL 8.4 测试证明：后续版本冲突回滚先前检查点更新；正常路径两次写入一同提交。该测试不包含尚未实现的预算表或候选表，不宣称完整联合事务已完成。

## 后续状态写入顺序

1. 创建 Run：冻结完整评测 release identity、执行/门槛策略、租户/操作人及用例顺序。仅生成侧五项清单不足以创建可发布的评测。冻结 7 个生成 case × 5 slot 和 1 个 preflight，不能运行中更换策略。
2. 准备执行：读取一致的 Run 版本和 NextAction，确认没有在途执行及未解决的结果未知。写 prepared checkpoint，尚未发送，不按已发出调用消耗预算。失败时不创建另一份并行准备记录。
3. 发送许可：同一事务复核 Run 版本、owner、时间、预期 action 和单目标/整轮预算；写 dispatching checkpoint 与唯一 invocation 执行流水，并登记预算消耗。提交后才能调用模型，任何数据库错误都不发送。
4. 返回结果：外部网络调用不占用数据库事务。新事务按 invocation/owner/版本核对，将原始执行证据、分类失败或候选、语义回执、Run 进度和检查点清除一起提交。重放必须核对已提交内容，不能创建第二份候选或扣第二次预算。
5. 恢复：精确回收过期 prepared，不扣已发出预算；dispatching 无法确定结果时持久记录 result_unknown，保留预算消耗并阻止自动替换。人工确认必须引用具体 invocation 和当前版本，有授权及审计记录，不能传一个布尔值跳过。

所有对同一 Run 的状态变更必须受同一版本控制，不给候选、预算和检查点各设置独立成功条件。仓库原语不替代应用层主体授权与领域 NextAction。实际表结构与 Run 聚合实施时一并确定，避免把当前仅含 checkpoint 的记录冒充完整 Run。

## 联合事务验收要求

- 两个 worker 竞争同一 action：一个发送许可、一个冲突；模型网关调用恰好一次。
- 单目标或整轮只余一次预算：并发不能超限；失败/未知调用仍计入，prepared 回收不吞预算。
- 在写检查点、流水、候选、回执的任一步注入异常：该次 Run 变化全部回滚。
- 模型返回后提交确认丢失：按 invocation 重放得到相同证据，版本和预算不重复增加。
- 发送后进程终止：进入结果未知待确认；过期扫描不能恢复为可自动发送。
- 使用旧 owner、旧版本、旧 invocation 或旧 lease 扫描结果：更新失败，不影响新状态。
- 人工确认/取消与 worker 完成竞争：由 Run 版本决定唯一结果，历史执行和审计不覆盖。

上述完整 Run 验收仍待实现；M1 的真实授权/撤权、生产执行和最终成果验收也没有因本设计而完成。

## 发送预留实现进度

`0012_evaluation_dispatches` 增加 Run 冻结策略及只追加调用流水。`freeze_policy` 只接受当前已校验 v2 定义，不 upsert；原 Go JSON 字节保存在 LONGTEXT，保留指纹语义。`reserve_dispatch` 参与外层事务：锁定 Run 检查点，校验版本/owner/阶段/时间，用锁定读取得最新调用流水，检查单目标及整轮预算，要求目标执行序号严格递增，再一同写入 dispatching 与唯一 invocation、唯一 execution ID。锁定读避免调用方已有 REPEATABLE READ 快照时计数过期。

隔离 MySQL 8.4 验证并发发送一次、目标预算上限、整轮 70 次上限、跳号/重复序号拒绝、相同 execution ID 换 invocation 拒绝及事务中途异常全部回滚、提交确认丢失后重放拒绝及语义预算按候选独立计数（8 项测试）。整轮上限测试以合成历史流水隔离预算条件，不代表完整 Run 调度验收。所有流水都计入预算，没有按失败/未知状态返还的接口。尚未验证实际模型调用次数；调用器只能在外层事务成功提交后使用发送许可，确认不明不能直接重放。

当前函数是内部存储操作，不选择 NextAction、不校验业务主体或人工审批、不处理回执及候选接受。未接入评测 worker；完整 Run 冻结身份、状态机和结果联合事务仍待实现，生产尚未迁移。


## 完整发布身份实现进度

`EvidenceReleaseIdentity` 冻结 Suite、Prompt、Profile、输入/输出 Schema、生成路由、语义 Prompt/输出 Schema/路由、执行策略与门槛策略，共 11 项原 QS 引用。指纹保留 Go 外层 map 排序与内层结构字段顺序，已直接调用原 Go 实现验证一致。原始策略文档需同时匹配 ID、版本及字节指纹。

此领域值尚未接入 Run 创建、资产查询与审批；不替代策略 Schema 验证，也不表示引用已存在或已批准。下一步仍须联合冻结实际资产、用例槽位和业务主体，并实现 NextAction 与结果接收。

原 v6 用例集现可通过 `load_suite` 按完整身份读取；校验原文件指纹，保留原 JSON 字节和用例顺序，构造 7×5 槽位并单独提供预检用例。历史 v1–v5 仍保留原资源，本读取接口暂只注册 v6，不自动选择最新版本。5 项测试覆盖顺序、策略数量一致、身份错配和内容变化拒绝。此读取器不执行断言、预检或模型调用，也不替代 Run 创建事务。

`load_gate_policy` 现读取并冻结原 v2 发布门槛文档，与执行策略共用资源校验、原始指纹、Schema 和版本检查。返回原文及完整引用，不提前计算门槛或批准结果。测试包含缺失人工评审规则、未知版本在重新计算校验值后仍被拒绝；门槛执行器、评审记录及批准事务尚未实现。

## Run 创建事务实现进度

`0013_evaluation_runs` 保存不可变创建记录；`create_run(session, ...)` 在外层事务中写入完整发布身份、原执行/门槛策略 JSON、原 v6 suite JSON、35 个槽位、独立预检、requested 初态与请求审计，再写预算策略和 version=1 空检查点。不创建第二个版本计数器，不自行 commit，不启动执行。重复 Run ID 直接冲突，不覆盖历史。

隔离 MySQL 8.4 的 4 项测试覆盖成功冻结和重复创建，以及策略/检查点写入冲突、提交前异常时全部新增内容回滚。迁移及 Alembic 元数据检查通过。测试中的其他资产引用是占位值，仅验证存储原子性；当前内部操作要求调用方先完成主体授权及全部资产解析，尚未提供该应用入口。因此这不是可执行、可批准的真实 Run，不能通过此存储接口绕过后续身份解析与状态机。下一批需接入资产解析、创建入口和 NextAction，并对 Run 的 requested/collecting 等状态统一实施版本条件更新。

生成侧资产解析器 `resolve_generation_assets` 已复用不可变资产仓库解析 Profile/Prompt/route/input/output 五项引用，并逐项比较原发布身份。QS 评测 Schema 引用使用完整版本（如 `ai-explanation-input/v1`），资产表使用后缀 `v1`，此处显式转换，不能直接复制五项生成清单为评测身份。相关 13 项清单测试通过，包含每一项指纹不符拒绝。该解析器尚未串入完整创建应用入口；语义 Prompt/Schema/route 解析与主体授权仍待实现，存储创建方法仍是内部操作。

语义 v2 指令与输出 Schema 已有冻结读取器 `load_semantic_assets`：按原 Markdown 三个规范文本块还原 system/task/data preamble，保留原文和原 Schema 字节及引用。原 QS `Test(V2)?ExecutablePromptMatchesNormativeMarkdownAndFrozenHashes` 在本任务独立工作区通过，确认这些文本块与实际 Go 消息常量一致；Python 3 项测试通过，包括与原 QS 文件逐字节对照、重算 manifest 后 Prompt 指纹漂移拒绝。此处不迁移语义 payload 构造、路由选择、回执解析或评分判定，尚未形成独立模型评审执行器。

语义路由解析 `resolve_semantic_route` 按自身 ID/版本/指纹查询不可变路由，缺失或不符即拒绝，不使用生成路由作默认值、不选择 latest。原 QS `SemanticEvaluatorSpec.Validate` 不强制评审与生成使用不同供应商/模型，迁移保留该语义；两者即使共用模型也各自冻结引用。相关清单/引用测试现为 16 项通过。生产语义路由版本尚未确认和导入，解析函数本身不提供缺失配置，也不代表完整创建应用入口已接通。

`MySQLRunCreator.create` 现先调用统一 `validate_release_assets`，完成全部 11 项引用解析及 suite Profile/Prompt 对齐，成功后再打开 Run 创建事务。生成侧从不可变资产仓库读取；语义指令/Schema、suite 和策略来自固定资源，语义路由按显式引用读取。相关 23 项资产测试通过，包含六类非生成引用错误在事务打开前拒绝；五项生成引用错误已有对应测试。测试资产仓库使用替身及原资产包，尚未作为完整 MySQL 创建适配器端到端验收。此为内部适配器，调用方仍须完成管理授权；尚未接入 HTTP/gRPC/CLI 入口、依赖注入或 worker，不能将其视为生产可用管理功能。

完整创建适配器现补充隔离 MySQL 8.4 验证：导入原 Profile/Prompt/route/schema 到真实资产表，经真实资产仓库解析完整引用，再由 `MySQLRunCreator` 创建 Run；核对 35 槽位、策略原文、发布指纹与 version=1，并验证重复创建不改变版本。该文件共 5 项集成测试通过。测试明确选择生成路由同时作为评审路由，不声称这就是生产评审配置；没有模型调用、管理授权或公开入口验收。清理只删除本测试实际新增的资产及自身 Run，不删除已存在基线。

## 下一批：Run 状态与单一版本

PR #27 的精确提交 `9140a9e80ab433339b480e3ef2443c6e3f1d2249` 已通过 CI `34636173015`（MySQL 8.0.36、8.4 和镜像），随后合并。生产未部署这些增量。

下一批保留创建记录不变，另存当前进度；所有状态变更仍以已有 checkpoint version 为唯一条件锁，不能新增彼此独立的 Run version。普通推进遵循原 requested→collecting/canceled、collecting→blocked/awaiting_review/canceled、blocked→collecting/canceled、awaiting_review→approved/rejected/canceled；有在途执行时不能直接离开 collecting。允许边并不等于满足前置条件，仍须校验候选完整性、未知结果、评审及门槛。

特别保留原 `review_reopening.go` 的拒绝后重开规则：不能用普通 Transition 从 rejected 重回执行。只有 v2 且特定 G4 判定争议等完整条件满足时，允许最多三次带审计的评审重开；原评审、门槛和最终时间必须归档保留。该规则尚未迁移，不能把状态邻接表当成完整业务状态机。

### 初始状态推进实现

`0014_evaluation_progress` 为 Run 增加独立 progress JSON，保留创建 definition 原文不变。创建时写 requested 进度；旧记录只有在 checkpoint version=1 且空检查点时才可从创建记录初始化进度，更高版本须对账。`transition_requested` 在调用方事务中锁定同一 checkpoint version，按组织定位 Run，仅处理 requested→collecting/canceled，并同事务更新进度、审计和版本。已有在途检查点、非 requested 状态或旧版本均拒绝。

隔离 MySQL 8.4 迁移/元数据检查与创建/状态相关 7 项测试通过：开始与取消并发只能一方成功，创建原文不变；提交前异常和错误组织保持全部记录不变。尚未覆盖完整 collecting 生命周期、预检/候选/回执、管理授权、公开入口或生产执行；初始推进函数仍为内部原语，不替代完整 Run 状态机。

预检终态证据已迁入：必须零模型调用，通过需包含 provider_call_count/rejection_reason 两项 passed 断言，保留原断言身份、作用域、序号和结果。`complete_preflight` 同事务写证据/进度/版本；失败进入 blocked，通过保留 collecting，重复提交拒绝。隔离 MySQL 8.4 创建/进度测试与预检领域测试共 10 项通过，包含两类预检结果、提交前回滚和重放拒绝。测试为构造证据，尚未实现实际预检执行器及 NextAction/发送许可对该证据的联动，不能视为生成前门槛已完整接通。

发送许可已接入 Run 门槛：`reserve_dispatch` 在同一检查点锁下读取冻结 Run/当前进度，要求 collecting、预检 passed 且目标属于冻结槽位；不存在 Run、未通过预检、已取消或越界目标均拒绝，不更新检查点和流水。旧独立预算测试改用显式合成 Run 进度，避免保留无 Run 的发送旁路。隔离 MySQL 8.4 的发送/创建/进度共 24 项测试通过。尚未接入完整 NextAction（候选顺序、语义候选归属、结果未知处理）或真实预检执行器，这些仍是模型调用前的未完成条件。

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

固定预检执行器 `run_preflight` 现从已校验 v6 suite 读取原单维度输入与 Profile eligibility，按原 QS 规则计算拒绝原因，并与原用例期望比较后生成断言；无模型依赖。`execute_preflight` 按 Run 冻结 suite 执行后，通过共享版本的 `complete_preflight` 保存。隔离 MySQL 8.4 创建/预检/发送共 25 项测试通过，新增覆盖实际计算拒绝原因与持久化断言。仅支持原已注册预检用例，不覆盖所有生成用例的输入/渲染预检或真实报告授权；完整 NextAction 尚未实现。

自动 NextAction 计算已迁入 `domain/evaluation/actions.py`，按原顺序处理非 collecting、在途恢复、未知结果、预检、逐槽位生成/语义及候选就绪；使用已迁移的自动恢复策略，不提供任意 manual-authorized 布尔旁路。3 项测试覆盖未知/预检优先、失败不跳槽、允许的限流重试、已有候选只进入语义阶段。本函数输入是进度投影，尚未与完整 Run 证据校验及数据库加载接通；人工恢复和完整证据身份/顺序约束仍待实现，当前不可把该计算器直接作为模型发送授权。

NextAction 输入投影增加结构约束：冻结槽位须按每 case 的完整序号排列、case 不重复；候选须有成功生成记录，review_ready 须有成功语义记录，同一候选不能跨槽位复用。非法终态也拒绝。4 项规划测试通过，包含重复槽位、无证据就绪和候选重复归属拒绝。此为投影结构校验，仍不等于回执内容/调用身份/事实引用或人工恢复证据的完整验证。

自动规划现在同时扫描全部槽位的生成/语义历史，调用方传入未知计数 0 不能遮蔽其他槽位或已标记 review_ready 候选中的 result_unknown。6 项规划测试通过。由于当前投影尚不承载人工确认记录，含未知历史的投影仍阻止自动调用；后续须以具体执行身份及已校验审计记录解除，不通过全局布尔值或清除历史解除。

### 状态与预检批次合并

PR #28 精确提交 `3350fde937de98c3cef948347c1eb5b800d2d18d` 的 CI `34638065264`（MySQL 8.0.36、8.4 和镜像）全部通过，已合并。未部署生产。下一批将数据库冻结槽位/当前进度转为已校验规划投影，再以同一版本原子核对 NextAction 并写 prepared。不得由调用方指定任意后续槽位，不得把无终态回执的 dispatch ledger 转为成功候选。完整回执与候选写入、人工恢复以及生产验收仍待推进。

### 首个计划执行的原子准备

`prepare_execution` 锁定 Run 的 checkpoint version，读取冻结槽位与实际预检进度，调用 NextAction 选择首个目标后写 prepared；外部只能提供 owner/执行身份/租期，不能选择任意槽位。准备不扣预算。已有 dispatch ledger 时明确要求终态证据投影，不把历史调用当成空槽位重发；后续候选/回执投影仍待实现。

隔离 MySQL 8.4 创建/预检/发送相关 26 项测试通过，新增链路：创建→开始→实际固定预检→两个 worker 并发准备唯一获胜→首槽位发送预留；prepared 时无调用流水，发送后 version=5。没有实际模型调用或业务授权验收，仍为内部事务能力。下一步实现终态回执与候选的原子接受，再解除后续槽位准备限制。

### 执行准备批次合并与终态接受约束

PR #29 精确提交 `30df68154cfa650c952b46b832c725f4d4228b5f` 通过 CI `34638563617` 两版 MySQL 与镜像检查后合并；未部署生产。

终态接受继续对照原 `CandidateGenerationExecution.Validate` 与 `CompleteGenerationExecution`：匹配执行 ID、调用 ID、case/slot/执行序号及 owner；必须对应 dispatching，开始时间不早于发送记录，结束时间不早于开始。成功必须有调用回执、非空原始与规范化输出、候选及断言；失败/未知不得创建候选。原始/规范化输出各限 256 KiB，规范化内容须为合法 JSON，指纹绑定原保存字节，不能重新序列化后代替。保存终态、接受候选、推进状态、清检查点及版本必须同事务；尚未实现。

终态领域模型 `GenerationCompletion`/`ProviderReceipt` 已迁入：校验执行身份/序号、带时区的起止时间、调用次数、回执调用关联、各 256 KiB 输出边界、规范化 JSON 原字节指纹及失败分类一致性；成功必须包含回执和两份输出。10 项测试通过，覆盖缺失证据、错误指纹、非 JSON 数字常量、错调用及错 owner/槽位。规范化输出按 UTF-8 严格解析；非 UTF-8 数据拒绝，原始响应仍保留 bytes。延迟完成不单凭租约到期拒绝，后续事务仍须复核当前版本和 owner。尚未校验实际输出业务 Schema/断言或冻结路由匹配，也未接入数据库接受事务，不能凭此领域对象发布候选。

终态资产校验已补齐为应用服务 `validate_generation_completion`：按 Run 冻结的 generation route 精确查找版本和指纹，所有带供应商回执的终态都核对 provider/model；成功输出按冻结 output Schema 的完整版本映射到不可变存储版本后验证。结构不合格的失败输出可保留作为证据，不会因此升级成功；校验异常不包含模型输出。新增 6 项测试复用原 QS 路由、输出 Schema 与现有合法候选，覆盖缺失/漂移资产、错误供应商/模型及成功与失败证据的不同处理；连同终态领域共 16 项通过。此服务尚未接入终态事务，不代替事实引用校验、确定性断言、候选接受或生产验收。

### 终态接受事务

`complete_generation` 与新增迁移 `0015_generation_completions` 保存终态元数据、原始/规范化 BLOB 及候选断言。持同一 Run 检查点锁，复核版本、组织、collecting 状态、owner、调用/执行/槽位身份、发送流水与前序终态；调用冻结资产校验后，同事务插入证据、清检查点、递增版本并更新进度。成功接受一个尚未 review_ready 的候选；失败不得带候选，未知结果计数并 blocked，预算耗尽或不允许自动恢复也 blocked。事务由调用者提交，重放返回冲突，可读取既有证据确认。

隔离 MySQL 8.4 新增 9 项接受事务测试，覆盖成功原字节保存、调用者回滚、证据插入后异常回滚、并发仅一个候选、旧版本/错 owner/跨组织拒绝、路由不匹配和未知结果阻断；连同 Run 与终态测试共 36 项通过。Alembic upgrade/check 通过，Ruff/mypy 通过。测试中的资产读取为固定原始资产替身，候选断言为构造证据；尚需接入可信事实/断言计算、正式调用入口与读取投影，解除后续槽位准备限制。没有生产模型调用或部署，不代表 M3 验收。

### 终态记录驱动后续准备

`prepare_execution` 已解除“一旦有发送流水就停止”的首执行限制，改由 `project_slots` 复核每条冻结槽位的实际终态：重新验证保存字节/指纹、身份索引与发送检查点、连续执行序号、候选关联及成功后不得再生成。发送流水与终态数量/身份必须逐条一致，缺失证据不推断失败。成功候选进入同候选首次 semantic prepared；契约失败按冻结策略准备原槽位第二次 generation。未知结果、不可恢复失败、预算耗尽仍阻断；语义终态尚未接入投影，因此语义发送后不能推进更多槽位，等待后续实现。

隔离 MySQL 8.4 Run/终态与规划测试共 32 项通过：新增成功→同候选语义发送、契约失败→原槽位第二次生成、第二次失败→预算耗尽阻断，及丢失终态/修改字节/缺失候选时不得重发。Ruff/mypy 通过。尚未调用真实模型、未部署生产；候选断言计算和语义终态接受仍待接通。

### 生成终态批次合并与语义解析

PR #30 精确提交 `65d67755b34df8214d3b37e5b43abf7faec1886d` 的 CI `34640577486`（MySQL 8.0.36、8.4 和镜像）全部通过，已合并；未部署生产。

`parse_semantic_output` 复用原 QS v2 语义 Prompt 和输出 Schema，并精确匹配发布引用、调用 ID、评测供应商与模型。结果必须对待评断言按 type/scope/ordinal 一一覆盖，拒绝缺失、重复及未知决定；hard 标志来自原断言。分数和文本按原 Schema 校验，并保留 Go 文本字节上限；输出指纹绑定未重新序列化的字节。质量 failed 是有效评测结论，不转换为执行失败。17 项测试通过，覆盖上述边界及中文长度、无效 UTF-8/JSON、大小限制。解析器尚未接入语义终态保存或候选归属校验；此为下一批执行接受事务的组成部分，不构成完整语义执行或真实质量验收。

语义终态领域模型 `SemanticCompletion` 已补齐：包含被评候选 ID/输出指纹，匹配当前 semantic dispatching 检查点、owner、调用/执行身份及序号，开始时间不得早于发送；租约过期本身不替代事务版本检查。成功需完整回执与输出、合法 UTF-8 JSON，失败仅允许原 QS 语义执行/基础设施/协议/未知分类，质量 failed 不能转换为执行失败。失败响应可保留无法解析的原始字节。与语义输出解析共 29 项测试通过。候选匹配目前是领域检查，尚需由数据库接受事务传入实际保存的候选及指纹，不代表已完成持久关联或 review_ready 推进。

### 语义终态接受事务

新增 `0016_semantic_completions` 与 `complete_semantic`：共享 Run 锁和版本，读取数据库中的生成证据/候选，复核候选输出指纹、case/slot、语义检查点/发送流水、owner/组织及连续序号。成功解析精确覆盖候选的 pending_semantic 断言，保留确定性断言和生成原始字节，写入语义证据并设置 review_ready/accepted_semantic_execution_id；质量 failed 仍是有效结果。失败不改变候选审核状态，未知、预算耗尽及不可自动恢复分类按策略阻断。证据、候选更新、Run 进度和检查点清除同事务，调用方负责提交。

隔离 MySQL 8.4 新增 10 项语义事务测试：质量失败但可审核、候选更新后异常回滚、跨组织/错 owner/候选或指纹错误、未知断言拒绝、失败/未知保留证据与并发仅接受一次；连同生成事务和语义领域/解析共 54 项通过。Alembic upgrade/check、Ruff/mypy 通过。语义资产读取为固定原始资产替身，断言仍为构造数据。语义终态读取投影和全槽位 awaiting_review 转换尚未接通，暂无实际模型执行或生产部署。

### 语义投影与完整槽位推进

`project_slots` 现同时核对生成和语义终态与发送流水。语义记录重新验证保存字节指纹、索引、候选/输出/检查点身份和连续执行序号；成功结果的分数、理由和决定须与规范化字节及候选已接受断言相符。review_ready 与 accepted_semantic_execution_id 必须有成功终态支持，不能单靠标志跳过候选。语义失败可按策略重试同一候选；完成后准备下一槽位的生成。

最后一个语义结果接受时，在同事务重建全槽位投影并执行 NextAction；仅全部候选已完成语义评测时转换 awaiting_review，人工评审仍待进行。隔离 MySQL 8.4 生成/语义事务 26 项通过，包含按 7 case × 5 槽位逐次完成 35 个生成和 35 个语义结果、只有最后一次才待审核且不能再准备调用。另新增 4 项语义重试和缺失/字节/决定损坏检查通过。全部输出为构造证据，验证的是事务和调度，不是原案例事实正确性或生产质量。Ruff/mypy 通过；未部署生产。

### 冻结断言清单与独立语义证据修正

对照 QS `candidateReceiptsV2`/`frozenSemanticObligationsV2` 发现：只选 pending_semantic 会漏掉已确定性失败但仍须独立语义评测的断言。已新增 `assertion_inventory`，从原 v6 Suite 读取 default/case 断言、分作用域同类型序号、hard 标志及全部参数；接受语义结果前逐项核对冻结候选的清单顺序、身份及 hard，不允许遗漏或漂移。所有原 QS 指定独立语义类型都进入评测要求，与确定性结果是否失败无关。

语义决定额外保存在 candidate.semantic_assertions；pending 条目仍解析为决定，原已失败条目保持原状态/理由。读取投影从独立语义证据比对实际输出，不能以语义通过抹去确定性失败。隔离 MySQL 8.4 与清单测试共 26 项通过，包括使用各 case 原始清单的 35 候选全链路及“确定性失败、语义通过仍保留失败”。清单的原参数已完整保存，但确定性断言结果计算尚未迁入；测试输出与初始检查结果仍为构造数据，不是业务质量验收。

### 确定性断言计算

新增 `evaluate_candidate_assertions`，读取冻结清单并复用 QS 输出 Schema、引用/Profile 和安全校验，计算维度引用数量/组合、insight kind、建议来源、禁用来源引用、禁用文本和输出字符上限；禁用文本采用 NFC + casefold，字符数包含 Go JSON HTML 转义。按校验阶段生成 passed/failed/blocked；独立语义要求保持 pending，失败原因不回显模型内容。5 项针对性测试通过，覆盖合法输出、Schema 阻断、引用错误、案例组合/来源不满足、禁用文本和安全失败。Ruff/mypy 通过。

该函数目前接受调用方提供的 PreparedExplanation，尚未从冻结 Suite 构造对应案例输入，也未接入生成接受事务；测试复用合成报告输出，不能据此证明原案例逐项一致。后续需完成固定案例输入构造、原 Go 对照与正式入口，禁止将本批视为确定性验证全面验收。

### 冻结案例准备与计算后接受

`prepare_evaluation_case` 绑定原 v6 Suite 的生成案例、Profile 和 Prompt 指纹，用原 provider_payload 渲染 Prompt；7 个生成案例均测试通过，预检/未知案例及 Profile/Prompt 漂移拒绝。这里的 input fingerprint 指向合成 provider payload，不冒充 QS 授权业务报告指纹。

新增 `complete_evaluated_generation` 内部入口：读取本组织 Run 的冻结发布，准备对应案例并计算断言，再调用同事务生成接受；调用方不能传入固定通过的断言列表。失败终态继续原接受规则。隔离 MySQL 8.4 与案例准备共 43 项通过，包含结构正确但引用不同报告时，记录 all_references_resolve=failed 和 profile 检查 blocked。底层事务原语仍保留供测试/组合调用；常驻评测执行器尚需采用计算入口，尚未完成原 Go 全案例差异验证或真实模型质量验收。

### 原 Go 断言状态对照

新增测试桥接原 QS `EvaluateCandidate` 与真实 DeterministicGate，使用本任务独立 QS worktree（`87f9dbea6db8c5a832d788bbb91ee8c257d47bd9`），临时测试程序运行后自动清理。7 个原生成案例 × 正常/Schema/引用/安全/Profile/禁用文本共 42 组构造输出，逐条比较全部断言状态，Go/Python 一致；interop 测试通过。双方使用同一冻结 Profile、案例 facts 和断言参数，没有外部模型调用。

该证据证明所选 42 组输入的状态一致，不证明全部文本边界、故障分类、发布门槛或真实结果质量。仍需常驻评测执行器调用准备/发送/接受链路、人工审核与发布治理，M1–M5 未验收。

### 语义模型请求数据

新增 `prepare_semantic_messages`，复用原 v2 裁判 system/task/data preamble，投影原语义输入七个字段：schema_version/suite_id/case_id/attempt/assessment_input/candidate_output/assertions。attempt 使用候选槽位序号，与原 online_runner_v2 相同；完整断言参数从冻结 Suite 读取，确定性已失败的独立语义要求仍保留。候选文本只进入 data_json，不能改变固定裁判指令。3 项测试通过，覆盖字段/参数、候选指令隔离与清单/发布漂移拒绝。此步骤尚未调用模型，现有网络适配器仍需解除对生成 PreparedExplanation 的绑定后接入该请求；不能视为常驻评测执行器完成。

### 共用网络调用层

DeepSeek Responses 请求序列化拆出 `build_messages_request`，新增 `generate_messages` 接收阶段消息/路由/Schema，生成入口继续先校验 Profile 路由，再共用原 HTTP 发送、大小/超时/响应解析逻辑。语义请求无需伪造生成 PreparedExplanation 或更改生成 Profile。原生成测试与新增语义路由/Schema/消息投影、超时单次发送共 28 项通过，Ruff/mypy 通过。HTTP 测试使用 MockTransport，不代表真实供应商接入验收；持久评测执行器仍需在发送事务提交后调用此适配器。

### 调用错误进入冻结恢复策略

新增 `classify_provider_failure`，对照 QS classifyGenerationFailureV2/classifySemanticFailureV2 与语义 diagnostics 分支：生成保留原安全错误代码，语义限流映射 semantic_provider_rate_limited，语义未知映射 semantic_result_unknown；未知结果覆盖 retryable 并强制 manual_acknowledgement。原始错误代码作为受约束 diagnostics，异常正文不写入证据。8 项测试通过，覆盖限流允许恢复、未知禁止自动重试、一般错误不越过策略白名单及错误文本隔离。现有 Python ProviderFailure 缺少 completed/no_message 诊断，因此不会凭 cardinality 错误猜测并启用对应重试；该诊断能力仍需补齐。此映射尚待持久执行器调用。

### 持久模型执行步骤

新增内部 `execute_step`：从已 collecting/预检通过的 Run 规划下一执行，按冻结发布准备生成或语义消息、路由与 Schema，发送预留事务提交后才调用共用 Gateway；返回后执行计算断言的生成接受或语义接受事务。ProviderFailure 进入阶段分类和冻结恢复策略，未知结果 blocked。取消/异常或调用后保存失败保留 dispatching，重入不会重新调用。当前仅解析已迁移的 balanced_text_v1/v8 路由，组织范围由受信调用者提供，尚未装配管理授权入口。

隔离 MySQL 8.4 新增 4 项测试通过：生成→语义两步，Gateway 独立会话确认发送记录已提交；发送提交失败零调用；超时未知结果保存后禁止重发；接受失败保留发送检查点且禁止重发。模型为替身，没有生产调用。步骤尚未常驻轮询/心跳/恢复人工处置；非 ProviderFailure 的无效响应/输出校验异常当前保留 dispatching，需要补齐失败证据映射及恢复流程，不能宣称完整可运营执行器。

### 已收到异常输出的失败证据

`response_evidence` 接入执行步骤，校验回执身份/用量、输出大小、严格 JSON 与冻结 Schema，将明确不合格响应转为阶段失败。生成输出契约失败允许按冻结预算替换原槽位；回执错误不允许替换；语义 Schema 错误使用 semantic_output_schema_invalid。生成的非法规范化 JSON 不写入 JSON 证据字段，原响应字节保留；超出存储上限的内容不保存，并记录对应失败码。10 项响应证据测试及 5 项隔离 MySQL 执行步骤测试通过，包含非法输出原槽位重试、第二次预算耗尽阻断。Ruff/mypy 通过。

语义输出结构合法但决定不匹配等后续语义解析错误，目前仍由接受函数拒绝并保留 dispatching，需继续映射明确分类；数据库/提交失败仍不得误归类为输出失败。尚未常驻运行或生产部署。

M5 当前入口：仅 `V6_PUBLISHED` 及从它注册的原生套件可用于新评测；原 `V6` 只用于指纹、案例与评测义务验证。v1–v5 案例和 v1 语义提示已移除，原来源清单保持不变，实际资源见 `../retained-assets.json`。

# QS 评测资源基线

2026-09-13 集中发布说明：评测执行、持久管理、原生套件和后台入口已进入 M3/M4 审核包；下方早期“尚未实现”文字保留为分阶段记录，当前实现与验收以 [管理对照](../../../docs/m3-management-parity.md) 为准。生产语义路线已经只读核对为 `semantic_judge_v1/v5`，并作为可选评测依赖加入固定路线导入，操作见 [集中发布配置](../../../docs/deployment-verification.md#m3m4-集中发布配置)。套件登记、资源导入和 CI 通过均不代表生产模型或管理闭环已验收。

本目录保留六版用例集、两版语义评测模板原 Markdown、语义输出规范、两份策略规范，以及从 QS Go 函数直接导出的执行/门槛策略。固定来源提交与各文件校验和见 manifest.json。来源是本任务独立且干净的 QS worktree，不读取其他任务未提交改动。

`policies.json` 的 definition_json 保留 Go 序列化字节和原 fingerprint；当前执行策略 release-evaluation-bounded-recovery/v2、门槛策略 release-gates/v2。策略 Schema 的 v1 与策略实例的 v2 是不同版本维度，不能混淆。两份实例通过原策略 Schema 验证。

旧源码导出工具已退役，保留原始资产及其来源与摘要。身份、失败门槛和候选断言通过已捕获的原 Go 固定基准验证，不再依赖旧引擎工作区。生产评测执行必须使用完整冻结资产清单，缺失时拒绝执行。

边界：六版用例集仍为 planned，不是已运行通过；语义模板 Markdown 尚未证明与运行时 system/task 消息完全一致；语义模型路由实际生产配置尚未盘点。未提供 Python 评测执行器、评分/门槛实现、人工复核或发布审批，不将资源导入视为 M3 验收。资源随 wheel/镜像分发，尚未进入数据库资产表或自动触发评测。


## Python 执行策略投影

`load_execution_policy` 校验资源 checksum、原 Go fingerprint 与原 JSON Schema，显式只接受当前已迁入 v2，然后构造冻结 ExecutionPolicy。类型保留样本目标、单目标/整轮预算、自动与人工恢复名单及三个恢复开关。

`selects_automatic_retry` 只是 policy whitelist 判断，不包含 ClassifiedFailure 的完整合法性/处置判断，也不代表可以直接重试。`within_budget` 是预留前检查，计数由后续持久执行聚合提供，失败及结果未知调用不能从预算中扣除。尚未实现并发原子预留、完整 NextAction、人工确认或评测发布门槛；未连接生产生成 worker。

## 失败分类迁移

`ClassifiedFailure` 保留 QS taxonomy v1 的 stage/kind/disposition、结果未知一致性、消息/证据引用限制以及受限诊断元数据。与原 Go 的 6 stage × 6 kind × 8 disposition × 2 retryable × 2 result_unknown（1,152 组）比较合法性、候选存在、生成替换和语义重试，结果一致。

策略增加基于合法分类的自动恢复判断：输出契约不合格允许替换生成；限流等生成重试仍须命中策略名单；语义重试要求相应处置及名单；质量失败保留候选/拒绝发布，结果未知要求人工确认。判断本身不预留预算、不执行重试，也不接受未经持久化审核的“人工已授权”布尔开关。完整调度、人工确认及原子预留仍待实现。

## 在途检查点规则

`ExecutionCheckpoint` 保留 execution/case/slot/candidate/owner/invocation、阶段及带时区租约时间。generation 不允许提前关联 candidate，semantic 必须关联；槽位 1–5、执行序号 1–2 与原固定 QS v2 约束相同。

prepared → dispatching 要求 owner 匹配和原租约内时间；恢复只允许扫描到的相同 invocation、相同 expiry 的过期 prepared，dispatching 永远不能按此路径自动重放。保留 QS 的 inclusive dispatch endpoint：恰好到期时发送和回收检查都可能成立，持久化层必须用同一聚合版本 CAS 决定唯一赢家。本轮只迁移领域检查，没有实现该 CAS、预算预留、租约续期或完整 NextAction，不能当作并发执行已验收。

## 检查点版本条件写入

`0011_evaluation_checkpoints` 和 `MySQLCheckpoints` 保存 run UUID、版本和完整在途检查点 JSON。create 不覆盖已有记录，save 要求新版本恰为旧版本加一，UPDATE 的 run_id/version 条件不匹配即冲突。解析时重新执行领域约束，时间保留时区。

隔离 MySQL 8.4 并发发送/回收测试证明同一版本仅一个提交成功，旧 worker 写入和跳版本被拒绝，重新创建仓库读取仍为胜出状态。此表是内部检查点存储原语，不是完整评测 Run，也不校验外部调用者权限或替代 NextAction 决策。预算、候选、回执与检查点的联合事务仍待实现；在联合事务实现前不接入评测执行器。生产尚未迁移该表。

## 发布执行输入契约套件

`qs-ai-published-input-cases-v1.json` 是 qs-ai 自有的派生套件，不是对原六版 QS 导出资源的覆盖。其身份为 `cross-dimension-participant-scale-v6-published` / `qs-ai-evaluation-cases/v1`，SHA-256 为 `42afcc73db6fa272ab54aa17bcb9b30dc8797a382ee9329d9453aa9c9953e382`。`derived_from` 固定原 v6 的完整身份；`input_contract` 固定 `qs-published-snapshot-v1` 和原输入规范的版本、摘要。

原 v6 的 7 个生成案例、1 个预检案例、Profile、Prompt、执行次数和全部断言保持不变。原案例本身已有空引用数组；新增的是可追溯的执行输入版本身份，不是自动将旧批准升级成新批准。新身份导致 release fingerprint 变化，必须创建新的 Run，重新生成、语义评测、双职责人工审核并完成 G1–G5 批准后才可用于该执行版本。

创建 Run 前和准备生成消息时，根据冻结规范只校验发送给模型的 context/facts 投影，保留原 `$defs` 与字段约束，不虚构 QS report/source 身份，不在发送前偷偷修正案例数据。预检案例故意违反维度数下限，仍由 preflight 证明 provider_call_count 为 0；不能为通过输入验证而削弱该拒绝案例。

发布绑定执行拒绝缺少该输入构造版本或 Schema 身份不匹配的 publication；旧 Run、publication、确认回执及原始资源仍可查询。注册派生套件不会调模型、批准 Run、修改发布指针或启用生产开关。当前仍是有界注册目录；任意新 Prompt/套件的草稿、校验、版本化和管理入口仍待建设。

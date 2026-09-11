# 领域与数据设计

状态：目标设计。P1 已实现 Session/Question/EvidenceSet 及最小运行表；其余内容仍为规划。精确 DDL 见 `migrations/versions/0003_interpretation.py`，当前范围见 [P1 验证](p1-verification.md)。

## 上下文与语言

第一阶段一个核心限界上下文：AI 解读。会话、证据、提问和成果共同协作。Execution 是应用运行支持，不因有状态机就变成独立领域。后续 Memory 经应用端口协作，禁止与 interpretation 双向导入领域对象。

| 术语 | 含义 |
| --- | --- |
| Session | 围绕一个 Testee 和目标的解读过程 |
| EvidenceSet/Item | 一次运行采用的不可变证据集合及条目 |
| Clarification | 信息缺口对应的问题与回答/跳过 |
| Run | 开始或恢复后的执行尝试，可以含多个模型步骤 |
| Invocation | 对提供商的一次外部调用 |
| Artifact | 校验并接受的不可变正式成果 |
| MemoryItem | 允许跨会话复用且有适用边界的信息 |

## 聚合与不变量

| 模型 | 边界 | 不变量 |
| --- | --- | --- |
| InterpretationSession | 聚合根，保留身份、目标、状态、版本和当前问题/运行指针 | 一个 Testee；最多一个等待问题、一个有效执行令牌 |
| Clarification | Session 管控的实体，物理独立表 | 只能回答当前问题，一次接受回答或跳过 |
| EvidenceSet | 不可变聚合根，包含有界 Item 集合 | 主体一致；冻结后不替换事实 |
| InterpretationArtifact | 不可变聚合根 | 绑定来源、回答和模型/Prompt/流程版本及校验 |
| MemoryItem | 后续独立记录/聚合 | 来源、回答者、时间、可见性明确；推断不升格事实 |

消息与执行历史不无限装入 Session；仓储只加载操作所需状态，查询分页。分数与等级属于事实值对象，AI 不可修改。

## Session 状态

| 当前 | 输入 | 下一状态 | 条件 |
| --- | --- | --- | --- |
| created | StartInterpretation | queued | 参数适用、授权通过；任务同事务创建 |
| queued | 领取执行权 | running | 版本与运行令牌匹配 |
| running | 可恢复的问题已经持久化 | awaiting_answer | 检查点可恢复后才公开问题 |
| awaiting_answer | SubmitAnswer/SkipQuestion | queued | 当前问题、版本匹配；答案与恢复任务同事务 |
| running | AcceptArtifact | completed | 结构、来源及策略校验通过 |
| running | 不可重试/预算耗尽/结果未知 | blocked | 稳定原因；结果未知不自动再调用 |
| blocked | 受控 Retry | queued | 可重试或已对账，预算允许，新 Run |
| 任意非终态 | Cancel | cancelled | 递增版本使旧执行权失效 |
| completed/cancelled | 推进请求 | 不变 | 新目标/修正依据创建关联的新会话 |

明确可重试的基础设施故障有界重排队，session 回 queued。首版不编辑已接受回答；纠正时创建带 supersedes_session_id 的新会话。任务、运行及检查点细节见 [运行设计](runtime.md)。

## 证据可比较性

检查 Testee、模型/量表版本、计分/常模版本、分数种类及方向、时间、填写者和场景。仅同名不足以证明可比较。

- comparable：按发布规则描述变化，不用裸分数自创风险。
- contextual_only：并列解释场景/观察者差异，不直接判定趋势。
- not_comparable：拒绝该比较目标，或由用户选择缩小范围。
- 跨类型只组织互补、差异和不足，不加总新总分、不凭同名维度认定同一构念。

事实引用使用 source_id + fact_ref，区分报告事实、用户自述、AI 推断。报告换版后 Artifact 保持原始引用，另查 current/stale/unavailable/unknown。

## MySQL 逻辑模型

InnoDB、utf8mb4、UTC。自有 ID 用 UUID，首版 CHAR(36)；qs-server uint64 在库中用 BIGINT UNSIGNED，JSON 用十进制字符串，避免 JavaScript 精度丢失。

| 规划表 | 关键字段 | 约束/索引 |
| --- | --- | --- |
| interpretation_sessions | id, org_id, owner_subject_id, testee_id, goal, status, version, current_question_id, active_run_id, workflow_version, supersedes_session_id | org/owner/updated_at/id 分页索引；version CAS |
| session_messages | id, session_id, seq, speaker, author_subject_id, content, created_at | UNIQUE(session_id,seq) |
| clarifications | id, session_id, question_seq, gap_code, question, answer, answer_kind, answered_by, answered_at | UNIQUE(session_id,question_seq)；Session 锁校验回答一次 |
| evidence_sets | id, session_id, fingerprint, schema_version, frozen_at | 冻结后不可原地改内容 |
| evidence_items | id, evidence_set_id, source_kind, source_id, source_version, observed_at, provider_subject_id, fingerprint, payload_json | UNIQUE(evidence_set_id,source_kind,source_id,source_version) |
| interpretation_runs | id, session_id, evidence_set_id, session_version, status, workflow_version, release_id, checkpoint_thread_id, checkpoint_ref, parent_run_id | 与 Session 有效运行指针对应 |
| step_results | operation_id, input_fingerprint, output_ref, validation_status | operation_id 唯一；跨恢复复用有效步骤回执 |
| model_invocations | id, run_id, operation_id, attempt, provider, model, state, request_fingerprint, receipt, usage_json, failure_code | UNIQUE(operation_id,attempt)；发送前持久化身份 |
| interpretation_artifacts | id, session_id, run_id, evidence_set_id, content_json, validation_json, content_schema_version | 首版 UNIQUE(session_id)、UNIQUE(run_id) |
| execution_jobs | id, run_id, status, available_at, lease_until, fence_token, attempt, last_error | (status,available_at,id) 领取索引；UNIQUE(run_id) |
| idempotency_requests | scope_hash, key, request_hash, resource_id, response_code, expires_at | UNIQUE(scope_hash,key)；不同语义冲突 |
| memory_items（后续） | id, org_id, testee_id, author_subject_id, visibility, source_ref, content, valid_from/to, status, version | 授权先于检索；变更版本化 |
| checkpoint 包自有表 | 由锁定适配包定义 | 不自行改列，不作为业务查询接口 |

P1 当前实现与完整模型的差异：证据条目以有界 JSON 数组放在 evidence_sets.items，首版最多 10 份，尚不做逐条索引或来源状态查询；不提前生成 evidence_items。问题表保留原始回答/回答者/时间，尚无消息列表。Run/Job 只承载离线闭环所需字段；Invocation、Artifact、Memory、发布版本字段在后续批次落地。P1 的证据信封不是已对齐上游的完整事实契约。

本地引用用必要外键，外部 qs-server ID 不跨库建外键。常查字段独立列，JSON 存快照。DDL、列长度、保留期在对应批次形成机器契约；本文不是迁移脚本。

## 事务

开始运行：检查 Session 版本，写 Run/Job/幂等回执并更新 Session，同事务。提交回答：校验当前问题，写答案/消息/恢复 Run/Job/回执，同事务。接受成果：验证令牌，插入 Artifact 并完成 Session/Run/Job，同事务。

远端读取和模型调用在事务外，快照构造后用短事务冻结。断线但事务已提交时，相同幂等键返回同一资源。UoW 由应用明确 commit，异常 rollback。

## 记忆和生命周期

短期记忆是原始问答、结构化上下文、可追溯摘要和检查点；摘要不替换原文。长期记忆先按主体/时间/主题查询，不先引入向量库。

表达偏好归用户；孩子背景归 Testee 并带回答者；本次目标归 Session。Testee 相同不自动共享内容。读取、恢复、历史成果和记忆召回都重新验证授权；撤权不能通过快照绕过。

阶段性描述带时间；过期先核实，冲突保留时间线。记忆可查看、纠正、删除。删除覆盖业务、检查点、索引和缓存，备份按保留周期淘汰且恢复后须重放删除标记。正式保留时长及导出范围是上线前产品决定，不照搬旧系统值。

关联阅读：[架构](design.md)、[接口](contracts.md)、[计划](roadmap.md)。

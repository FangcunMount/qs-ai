# 业务额度、执行槽与模型容量

## 本文回答

如何防止生成与评测相互挤占、组织超额、数据库连接长期占用，以及扩容时哪些限制会随副本变化。

## 结论

qs-ai 使用三层控制：持久业务额度与预留、数据库执行所有权、单事件循环模型容量。三者保护不同资源。业务额度按组织/用户/测评及 UTC 日期约束；Claim/CAS 防止同一任务重复推进；`ModelCapacity` 控制当前进程的外部调用并为生成预留容量。

| 层 | 状态来源 | 何时取得 / 释放 | 能否跨副本协调 |
|---|---|---|---|
| Participant 日额度 | MySQL reservations、版本化 quota | 接单预留；历史消耗保留 | 可以，依赖组织锁 |
| Participant 活跃槽 | reservation active 状态 | Job 领取时取得，终态提交时释放 | 可以 |
| Evaluation 额度与活跃 Run | MySQL reservations/Run/dispatch records | 启动预留最坏调用预算；活跃数按持久生命周期查询 | 可以 |
| evaluation 候选槽 | checkpoint/slot Claim | 准备步骤与恢复/CAS | 可以 |
| provider/global evaluation 模型计数 | APP `ModelCapacity` | dispatch 前取得，I/O attempt finally 释放 | 仅本进程 |
| loop concurrency / max in flight | `Settings` | 进程启动设定 | 每副本独立 |

## Participant 额度与活跃槽

接单在原事务内锁组织容量记录，校验组织、用户和 assessment 维度的每日计数并插入 Run reservation；每日额度记录不因终态完成而删除。额度策略与快照来自 quota baseline 和组织版本化配置，实际限制不能超过宿主 ceiling。

worker 领取 Job 时检查活跃槽。没有空位则把 queued Job 延后并释放本轮锁，不在事务中等待。既有 active reservation 在租约恢复中复用，不重复消耗额度；终态结果提交时 release active 槽。旧迁移前 queued 工作也不能在首次调用时绕过预留。

## 模型容量主链

`ModelCapacity.try_acquire` 的计数检查与更新在同一事件循环内同步执行。provider total 约束该提供方全部在途调用；evaluation 还受全局 evaluation limit，以及 `total - generation_reserved` 限制。生成可以使用空余总容量，评测不能耗尽为生成预留的部分。

生成容量满时在 `acquire_generation` 等待：不持有数据库 Session、未建立新 dispatch marker，但 Job lease 由 heartbeat 继续维持。已经存在模型回执的生成恢复直接读取持久结果，不必等待新的 provider 容量。

评测在准备新的 dispatch 前非阻塞尝试取得容量，不把数据库事务或活跃候选 Claim 长时间挂在本地等待队列。Run 的执行模式、per-run 候选并发、global evaluation、provider total 共同约束可推进数量。

`CapacityToken.release` 幂等，正常、异常和取消均在 finally 释放；token 是本地资源，不代表业务预留已释放。Participant 活跃槽释放跟随持久终态事务，每日预算记录继续保留。

Evaluation 日预算按冻结执行策略中的 generation 与 semantic 上限预留，不仅按已完成的实际调用数计算。同一冻结 Run 恢复复用原预留，不能重新刷新日预算；有未知调用的待取消 Run 仍计入活跃集合，直到受控处置完成。

## 故障与背压

- 数据库容量拒绝是业务接单结果；持久回执保留原决定，重复命令不会重新计算后翻转。
- worker 活跃容量不足是暂时无进展，queued 工作延后；不是模型失败，也不产生虚假 dispatch。
- 提供方 429、余额或传输错误保留网关分类；不把 `retryable` 翻译成自动重发。
- 崩溃丢失本地计数，由进程重建；持久槽需要租约和终态恢复，不能直接清零。
- NSQ `max_in_flight` 限制接收器在途处理，与模型并发不同；增加它可能增加接单事务与池压力，不能替代 provider 限额。

## 选择、代价与限制

持久业务额度保证跨副本一致，本地模型容量避免持锁等待并简化生成/评测隔离。代价是增加副本后 provider 最大并发按副本累加，generation reserved 也按副本计算。当前设计明确不是分布式 limiter。

分布式 provider 限流可作为演进方向，但必须处理租约、令牌失联、提供方未知调用、额度恢复和低延迟；不能以 Redis counter 替代现有调用证据。调整 loop 并发应同时审查 provider total、数据库池预算、max_in_flight 与关闭预算。

## 实现与验证

- 本地容量：[`model_capacity.py`](../../../src/qs_ai/application/execution/model_capacity.py)、[`providers/core.py`](../../../src/qs_ai/bootstrap/providers/core.py)。
- 持久容量：[`participant_capacity.py`](../../../src/qs_ai/infrastructure/persistence/mysql/participant_capacity.py)、[`evaluation_capacity.py`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_capacity.py)、[`quotas.py`](../../../src/qs_ai/infrastructure/persistence/mysql/quotas.py)。
- 测试：[`test_model_capacity.py`](../../../tests/test_model_capacity.py) 覆盖预留、等待、并发与取消；[`test_participant_capacity.py`](../../../tests/integration/test_participant_capacity.py)、[`test_evaluation_capacity.py`](../../../tests/integration/test_evaluation_capacity.py)、[`test_evaluation_capacity_recovery.py`](../../../tests/integration/test_evaluation_capacity_recovery.py)、[`test_quotas.py`](../../../tests/integration/test_quotas.py) 覆盖持久约束与恢复。吞吐和提供方真实限额另需压测与部署证据。

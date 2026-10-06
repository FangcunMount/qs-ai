# 业务持久状态与 LangGraph 编排

## 结论与证据状态

当前 ReportWorkflow 和 evaluation step 真实使用 LangGraph，但编译时未配置独立 checkpointer。图组织一次 attempt 的阶段，MySQL 的业务状态、fence/CAS、dispatch ledger 与 response receipt 决定恢复和接受资格。本文是基于当前约束的设计分析，不声称还原框架引入的历史动机。

## 问题与约束

模型 I/O 可能收费且结果未知。业务必须保留请求所绑定的资产、输入事实、调用身份、提供方响应和成果关联；授权需要在执行和接受时复核。评测还有并发候选、取消、版本CAS与未知调用处置。图中某个节点已完成不等于上述事实已在原业务事务落库。

## 候选与选择

| 候选 | 优点 | 代价与风险 |
|---|---|---|
| 纯顺序调用 | 控制流简洁 | 阶段图、观测和未来分支需人工组织；仍要实现业务恢复 |
| 当前无独立checkpointer的图 | 阶段显式，可隔离并发图状态，与既有恢复协同 | 图重入要依赖持久回执；图本身没有完整恢复保证 |
| 独立图checkpoint作为恢复引擎 | 可复用图恢复能力 | 与业务版本/租约、调用账本、消息事务成为双状态协调问题 |

当前生成图为 prepare → generate → validate，评测图为 prepare_dispatch → invoke_model → commit_receipt。调用节点经 durable layer，恢复重入可复用原 response，不以图内存重跑授权第二次发送。存储或取消错误向外传播，图不暗中吞掉并重试。

## 不变量与失败窗口

- durable dispatch 先于外部调用，只有首次创建者获准发送；dispatched/unknown 不再自动发送。
- response 持久化先于最终成果接受，最后接受仍检查fence/CAS、冻结配置和权限。
- 图状态按 invocation 隔离，不依赖共享可变字典；取消不得继续构造并接受成果。
- LangSmith tracing 明确禁用，业务正文不因框架环境配置进入云追踪。

marker 已提交而send前崩溃与send后响应丢失无法区分；当前保守写unknown，代价是需要受控处置。独立checkpointer也不能独自消除供应商执行的这个窗口。

## 重新决策条件

当新增多轮、分支或长期挂起流程需要图自身持久化时，应先确定唯一权威状态，列出图checkpoint、业务事务、提供方发送及消息Outbox每一提交顺序。只有证明旧worker失权、重复节点、响应重放、取消与业务确认行为后，才可引入独立恢复路径。

## 实现与验证

- [`report.py`](../../src/qs_ai/infrastructure/workflows/report.py)、[`evaluation.py`](../../src/qs_ai/infrastructure/workflows/evaluation.py)、[`generation.py`](../../src/qs_ai/application/execution/generation.py)。
- [`test_report_graph.py`](../../tests/test_report_graph.py) 验证unknown不重发、取消和并发隔离；[`test_generation.py`](../../tests/integration/test_generation.py) 验证实际持久边界；[`test_evaluation_capacity_recovery.py`](../../tests/integration/test_evaluation_capacity_recovery.py) 验证候选恢复。
- 完整恢复设计与失败窗口见 [执行恢复](../03-基础设施/execution-recovery/README.md)。

# 执行所有权、调用回执与恢复

## 本文回答

如何在进程中断、模型超时、租约丢失和提交失败之后继续，同时避免重复发送收费模型调用或接受旧 worker 的结果。

## 结论

恢复依据是业务数据库中的 Job、Lease/fence、checkpoint/CAS、dispatch ledger 和 response receipt。LangGraph 编排一次执行的阶段，未配置独立 checkpointer。持久调用先登记 dispatched，再发模型；缺少响应会留下 unknown，不能据此发送第二次。已经持久化的响应可以重复进行本地验证与接受。

## 概念与不变量

| 概念 | 用途 | 不能替代的责任 |
|---|---|---|
| Claim / fence | 证明 worker 当前拥有解读 Session 和 Job | 不能证明模型没有执行 |
| evaluation checkpoint / slot claim | 证明某 Run 步骤或候选当前执行位置与版本 | 不能替代冻结评测证据 |
| invocation / execution ID | 标识一次调用、账本与回执 | 不是供应商侧通用幂等保证 |
| dispatched marker / ledger | 在外部 I/O 前记录发送资格 | 不是已拿到供应商响应 |
| response receipt | 持久化响应或明确失败 | 不是 Artifact 通过业务校验 |
| Artifact / completion | 接受有效内容并关联冻结事实 | 不是 QS 已持久接收 |

## 解读执行链

1. 存储锁 Session/Job/Lease，确认状态与期限，增加 fence，提交 Claim；旧 fence 即使响应晚到也不能写入。
2. worker 维持 TTL/3 heartbeat，执行前授权，读取冻结 EvidenceSet，解析该请求已经绑定的发布配置。
3. `DurableGeneration` 在首次调用前取得本地模型容量。存储再次 guard 并提交 `model_calls.status=dispatched`；只有创建该 marker 的调用者获准发一次。
4. 网关返回后校验 invocation ID 与 model；单独提交 `response_received` 或 `failed/unknown`。凭证和 endpoint 不进冻结请求。
5. 本地解析响应并构造 Artifact，接受前再次授权。最终事务再次验证租约、版本、冻结配置、响应身份和内容指纹，并提交 Artifact、终态、容量释放与状态事件。

`DurableGeneration` 看到旧 `response_received` 时解码原响应；看到 `dispatched/unknown` 时产生 `provider_result_unknown`；看到明确 `failed` 时沿用原失败。即使错误带 `retryable=True`，同一次持久调用也不会自动发送第二次。

## 评测执行与恢复

评测 worker 优先扫描恢复。`prepare_step` 持久化准备/调用账本，再由图执行模型 I/O，候选响应先存 `evaluation_response_receipts`，随后 `finish_step` 写完成证据与 checkpoint/progress。候选模式的 heartbeat 延长槽租约，CAS 防止并发生命周期命令和旧 worker 同时推进。

候选恢复按精确过期 Claim 判断：prepared 且无 dispatch 证据，可以释放槽；存在 dispatch 而缺 response receipt，则写 `result_unknown` 的完成证据并要求受控处理；存在 receipt，则按冻结配置重放完成，不调用模型。serial checkpoint 也由恢复流程推进，普通扫描不重复处理活跃 checkpoint。

unknown 列表只允许在执行已恢复、无活跃 Claim、版本仍匹配后读取。人工确认或替换操作还需组织范围、显式版本、冻结执行策略、调用预算和证据。替换代表一次新受控执行，不是抹掉旧调用或改变其未知结论；业务操作见 [评测模块](../../02-业务模块/evaluation/README.md)。

## 失败窗口

| 中断位置 | 可确认的事实 | 恢复动作 |
|---|---|---|
| marker 提交前 | 尚未取得持久发送资格 | 新所有者可以重新准备 |
| marker 提交后、真实 send 前 | 和已发送但响应丢失不可区分 | 保留 unknown，牺牲自动活性避免重复收费 |
| 请求发送后、response 持久化前 | 提供方可能已执行 | unknown；不自动重发 |
| response 已提交、接受前 | 原响应可复读 | 本地校验并在新所有权下提交成果 |
| heartbeat 或 fence 校验失败 | 当前 worker 失去写权限 | 取消本地工作，拒绝其后续提交 |
| 结果与状态提交后、事件发送前 | 业务完成且消息已持久 | MQ Relay 继续交接 |

冻结事实不等于冻结访问权；执行前和接受前重新授权。撤权时返回 `access_revoked`，不能以快照曾合法为由接受内容。

## 选择、代价与替代方案

当前策略优先单次 dispatch 与证据完整性，代价是 marker-before-send 的窗口可能把实际上未发出的调用保守地标为 unknown，需要受控处置。供应商如果提供可证明的调用查询或幂等请求接口，可以改善活性；在协议未证明该能力前不能推断。

独立 LangGraph checkpointer 会产生另一套恢复位置与提交顺序，需要与业务 fence/CAS、账本和状态消息协调。当前图只负责阶段编排，业务恢复继续由数据库承担，分析见 [决策记录](../../05-决策记录/02-业务持久状态与LangGraph编排.md)。

## 实现与验证

- 解读：[`worker.py`](../../../src/qs_ai/application/execution/worker.py)、[`generation.py`](../../../src/qs_ai/application/execution/generation.py)、[`execution.py`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py)。
- 图与评测：[`report.py`](../../../src/qs_ai/infrastructure/workflows/report.py)、[`evaluation.py`](../../../src/qs_ai/infrastructure/workflows/evaluation.py)、[`evaluation_candidate_recovery.py`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_candidate_recovery.py)、[`evaluation_recovery.py`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_recovery.py)。
- 单元验证：[`test_report_graph.py`](../../../tests/test_report_graph.py) 覆盖 unknown、取消和图状态隔离。
- 数据库验证：[`test_generation.py`](../../../tests/integration/test_generation.py) 覆盖终止和响应提交失败不重发；[`test_evaluation_capacity_recovery.py`](../../../tests/integration/test_evaluation_capacity_recovery.py)、[`test_evaluation_recovery.py`](../../../tests/integration/test_evaluation_recovery.py)、[`test_evaluation_unknowns.py`](../../../tests/integration/test_evaluation_unknowns.py) 覆盖评测证据恢复与未知处理。

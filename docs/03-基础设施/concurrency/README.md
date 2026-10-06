# 业务额度、执行所有权与模型容量

一个组织可以还有日额度，生成 Job 却拿不到活跃槽；评测 Run 可以处于 collecting，worker 却拿不到候选 Claim；拿到 Claim 后，也可能没有本地 Provider 容量。**这些限制分开维护：额度决定能否接单，持久所有权决定谁能推进，本地容量决定本进程何时能开始一次模型调用。** 释放一种资源，不会自动释放另外两种，更不会授予重发未知模型请求的权限。

本文按业务源码 `766b2aa` 核对。轮询、退避和关闭流程见 [后台执行与调度](../../01-运行时/03-后台执行与调度.md)；这里说明每个并发控制保护的不变量、跨实例边界和失败窗口。以下数值来自仓库配置或代码固定算法，未读取线上有效配置、实例数和供应商实时配额。

## 先区分六种“容量”

| 控制 | 计数单位与状态来源 | 取得与结束的时点 | 跨实例协调 |
| --- | --- | --- | --- |
| Participant 日额度 | `participant_capacity_reservations`，按组织 / subject / 每个 assessment、UTC 日计数 | 接单事务为 Run 预留；完成、失败、取消均保留当天记录 | 同组织 admission 锁与当前读 |
| Participant 活跃槽 | 同一 reservation 的 `active`，不按日期过滤 | 领取 Job 时取得；持久终态或取消事务释放 | MySQL 状态共享；租约重领复用 |
| Evaluation 日预算与活跃 Run | `evaluation_capacity_reservations` 和 Run 的持久 progress | Start 预留冻结策略最坏调用预算；活跃数按状态查询 | 同组织 admission 锁 |
| 执行所有权 | 生成的 Session / Job / Lease；评测的 checkpoint / slot Claim | 短事务领取，按租约续期，终态事务结束或受控恢复 | 行锁、版本检查、唯一键及 fence |
| 本地模型容量 | APP 作用域的 `ModelCapacity` 计数 | 新 dispatch 前取得；调用路径退出时归还 | 只在当前进程的同一事件循环共享 |
| 消费者数量 | `worker.concurrency`、`evaluation.concurrency`、Subscriber `max_in_flight` | 启动时创建；一个 consumer 同时执行一轮 attempt | 每实例独立，不是持久额度 |

[`RuntimeProvider`](../../../src/qs_ai/bootstrap/providers/core.py) 创建一个 APP `ModelCapacity`，生成和评测 REQUEST 作用域都借用它。数据库连接池也按实例创建；连接池的 `pool_size + max_overflow` 是可借连接数，不能当成模型并发配置。模型网络等待阶段关闭数据库 Session，而续租、记录响应和终态提交仍需要另借短事务连接。

正式入口由 [`serve`](../../../src/qs_ai/bootstrap/server.py) 装配：生成走 `ExecuteNext.once → MySQLExecutionStore.claim → DurableGeneration`；评测走 [`EvaluationWorker.once`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_worker.py)，先处理到期恢复与取消排空，再选择 Run，进入 [`execute_step`](../../../src/qs_ai/infrastructure/workflows/evaluation.py)。评测扫描只是线索，真正的 Claim、预算和 dispatch 检查发生在 [`prepare_step`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_step.py) 的短事务内。

## Participant：日额度一次预留，活跃槽随执行占用

[`participant_capacity.reserve`](../../../src/qs_ai/infrastructure/persistence/mysql/participant_capacity.py) 在接单事务中先锁 `participant_admission_locks` 的组织行，再读取同组织、同 UTC 日的 reservations。组织计数取全部行，用户计数按 subject，assessment 计数逐一检查 Session 中的测评 ID。只有三个维度都还有额度，才插入以 active Run ID 定位的 reservation；行内同时保存 Session、组织、subject、assessment 列表和接单时 quota 快照。

组织锁通过 insert/upsert 建立，所以“目前一条 reservation 都没有”也能串行接单。reservation 查询用 `FOR UPDATE` 的当前读，避免接单事务此前读取幂等回执时形成的旧快照漏掉刚提交的预留。重放同一 Run 会先校验既有 reservation 的身份，再复用原记录；不会跨到第二天重新收费。日额度拒绝被接单服务写成 blocked Session 和结果回执，不插入生成 Job；重复原命令返回原决定。

领取 Job 时，[`acquire`](../../../src/qs_ai/infrastructure/persistence/mysql/participant_capacity.py) 检查同组织所有 `active=true` 的 reservation，包括昨天开始、今天还没结束的工作。它读取当前 quota 策略；新的活跃槽不足就返回 `False`。[`MySQLExecutionStore.claim`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py) 随即把该 Job 的 `available_at` 延后一秒并提交，释放本轮锁，继续检查其他候选；这一分支不会增加 attempt、fence 或 Session 版本，也不会建立模型 dispatch。

例如把某 assessment 的活跃上限设为 1，J1、J2 都已通过日额度接单：J1 取得 active 槽，J2 延后。J1 的租约失效后被另一 worker 重领，原 reservation 仍是 active，直接复用这个槽；J1 提交终态并释放后，J2 才能取得槽。取消 J1 也释放 active，但两条日 reservation 都保留。这解释了为什么“取消后仍占今日额度”和“重启后没有空出执行槽”都是当前设计的一部分。

已 active 的 reservation 在重领时先复用，不因在线调低 quota 强制终止；尚未 active 的工作按当前策略检查。接单时的 quota 快照也不会被活跃槽获取覆盖。迁移前没有 reservation 的 queued 工作，首次领取要补做日预留；它不能绕过额度。持久槽只在业务事务中释放，不能靠清空进程计数回收。

## Evaluation：预留的是最坏调用预算，不是本轮并发数

[`evaluation_capacity.admit`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_capacity.py) 在 Start 的同一事务内检查活跃 Run 和日预算。调用者先取得组织 admission 锁，且该锁必须早于本事务第一次快照读和 checkpoint / Run 锁。容量不足会回滚 Start 的状态变更，不能留下“已经 collecting、却没有预算”的半成品。

当前活跃谓词是 **collecting，或 blocked 且 `cancel_requested.status=cancel_requested`**。awaiting_review 和没有待取消意图的普通 blocked 不计入此集合；待取消且仍有未知证据的 Run 保留槽，直到取消可以排空。这里的活跃 Run 数不是活跃模型调用数，也不是 Candidate 数。

日预算来自 Run 已冻结、且与 `evaluation_run_policies` 原字节和 hash 匹配的执行策略。当前注册的 [`release-evaluation-bounded-recovery@v2`](../../../integrations/qs_server/evaluation/policies.json) 有 7 个生成 case，每个 5 个 slot，共 35 个候选目标；每槽生成最多 2 次，每候选语义评测最多 2 次，两阶段各最多 70 次。因此：

```text
一次无恢复的完整链：35 generation + 35 semantic = 70 次
Start 日预算预留：70 generation 上限 + 70 semantic 上限 = 140 次
```

预留不是供应商账单，也不随着“只实际调用了 70 次”退回差额。以 default.yaml 的日预算 1024 为例，同一 UTC 日最多容纳 7 份这样的完整预留，合计 980，剩余 44；`max_active_runs=1` 另外限制同时活跃的 Run，原 Run 离开活跃谓词后，例如进入 awaiting_review，才能继续 Start 下一份。production.yaml 现覆盖日预算为 2048，同一冻结策略可容纳 14 份完整预留，合计 1960，剩余 88；已有当日预留不清零。这个算例只适用于上述冻结策略和额度，其他 Run 必须读自己的 policy。

恢复同一 Run 复用原来的预算日和预留量，但仍先检查其他活跃 Run，不能靠“我已有 reservation”越过当前活跃上限。取消不退还日预算。每次新 dispatch 还要在 coordinator 锁下，对最新的 dispatch ledger 检查阶段总量、slot/candidate 次数及下一个 execution ordinal；已失败和未知调用也计入消耗。`retryable=true` 只是失败分类的一项输入，自动或人工恢复还必须满足冻结策略和原预算，不能借租约重领重置计数。

## 行锁和版本各自保护什么

下面列的是实现实际采用的加锁顺序。它们约束同一业务事务内的读取和写入，不在网络调用期间持有。

| 写入路径 | 主要顺序与检查 | 防止什么 |
| --- | --- | --- |
| 生成领取 / 续租 / 接受结果 | Session → Job → `execution_leases`；容量操作再取 participant 组织锁 | 同一个 Session 被两个 worker 接受结果，或旧 Claim 写入新 active Run |
| Evaluation Start / 恢复重新准入 | evaluation 组织 admission → checkpoint → Run | 并发 Start 超过组织上限或重复日预留 |
| Evaluation 模型步骤 | checkpoint coordinator → Run → slot Claims → dispatch / completion 证据 | 同槽双主、阶段预算竞争、丢失并行完成结果 |
| 在线 quota 修改 | participant 组织锁 → evaluation 组织锁 → quota pointer / version；不锁 Session / Run | 修改与两个接单链交错读到不一致配置 |
| 发布 / 回滚 | 回滚时先锁目标 Publication；再锁 checkpoint / Run，最后锁 selector pointer | 治理证据与指针竞争更新；其 CAS 不是执行租约 |

Participant 和 evaluation 的组织锁是不同表，避免把两个业务链的所有事务合成一把全局锁。quota 修改同时取得它们，并固定上述顺序。[`MySQLQuotas.apply`](../../../src/qs_ai/infrastructure/persistence/mysql/quotas.py) 先检查同作用域的原命令回执，再比较 `expected_revision`；版本化配置、pointer 和命令回执同一提交。rollback 取历史配置内容并建立新 revision，不把 revision 倒拨。默认与 ceiling 来自实例 Settings；组织配置的有效值还受 ceiling 约束。

版本名称相似，也不能互换：

| 标识 | 何时变化 | 不能代表什么 |
| --- | --- | --- |
| Session `version` | queue、running、业务终态等状态操作；重领 running 也增加 | 不是某次 Provider invocation ID |
| Job `attempt` | 成功领取一次就加一；达到 3 后下一次领取转 dead / blocked | 不是允许发 3 次 HTTP |
| execution lease `fence` | 领取或取消废止所有权时增加；heartbeat 只续期 | 不是业务内容版本，也不能停止供应商执行 |
| evaluation checkpoint `version` | coordinator 每次保存严格 `expected+1` | 不是每个并行步骤必须一直保持相等的版本 |
| candidate Claim `version` | 创建 Claim 时取 coordinator 新版本；同槽重用取得更高版本 | heartbeat 更新 expiry 不改变 Claim 身份 |
| quota revision / publication pointer version | 接受相应管理命令时推进，校验用户提交的期望值 | 不参与模型续租或授权重发 |

执行 checkpoint 的 CAS 更新是 `WHERE run_id AND version=expected`，影响行数必须为 1，见 [`save_checkpoint`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_checkpoints.py)。publication 还校验 `expected_active_id`，同 selector 的 pointer 是全局共享；操作人所在组织不能把它变成组织私有指针，见 [`apply_publication`](../../../src/qs_ai/infrastructure/persistence/mysql/publications.py)。方案准备和治理版本的业务含义见 [方案资产与原子准备](../../02-业务模块/governance/01-方案资产与原子准备设计.md)。

## 生成：新 owner 能接手计算，不能接手旧调用的发送权限

生成 Claim 的完整 guard 是：Session 仍 running、active Run 和 Claim 一致、Session 版本一致；Job 仍 leased 且 fence 一致；lease fence 一致且未过期。lease 有效期使用 MySQL `UTC_TIMESTAMP(6)` 检查；响应与终态提交前还会再次检查有效期。候选扫描不授予所有权，领取事务在 Session / Job 上使用 `SKIP LOCKED` 后重新核验条件。

考虑一个初始 version=1 的 Session：queue 后 v2，A 领取后 v3 / fence=1 / attempt=1。A 停顿到租约失效，B 重领后 v4 / fence=2 / attempt=2；A 的旧响应和 finish 都不能通过 guard。B 的 heartbeat 更新 Job 与 lease expiry，不增加 v4、fence 或 attempt。B 正常完成才继续推进 Session 版本。

[`model_calls`](../../../src/qs_ai/infrastructure/persistence/mysql/schema.py) 以 run_id 为主键，另有唯一 invocation ID。首次发送前，[`begin_model_call`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py) 先提交 dispatched、原 request JSON、invocation 和 Claim fence；存在记录就返回原记录，不改成新 fence，也不为 B 生成新 invocation。于是 B 的后续权限由持久证据决定：

| A 中断前留下的证据 | B 能做什么 |
| --- | --- |
| 尚无 ModelCall，比如正在等本地容量 | 通过新 Claim 校验后进行该 Run 的首次发送 |
| 已有完整 `response_received` | 复用原响应继续计算和终态提交，不再申请调用容量 |
| dispatched 且没有持久响应，或已 unknown | 保留未知结果并阻断；缺回执不证明未发送 |
| 已 failed 且有 failure code | 复用原失败；不能因为重领获得另一次发送机会 |

尤其是 B 不能把自己收到的新响应写到 A 的原 dispatched 行：`record_model_response` 还要求 ModelCall 的 invocation、fence 和 dispatched 状态匹配。只有**早已成功持久化**的响应能被新 Claim 复用。终态事务原子提交 Artifact / Session / Run / Job、废止租约并释放 Participant active 槽；日预留留存。

[`ExecuteNext`](../../../src/qs_ai/application/execution/worker.py) 同时监督工作与 heartbeat，按 TTL/3 续租。heartbeat 失败会取消本地工作。取消 coroutine 不代表供应商已停止，因此 fence 提供的是旧 owner 的写入隔离；外部结果仍以 ModelCall 证据处理。

## 候选并行：共同 coordinator，独立 Claim，允许乱序完成

serial_v1 在 Run 的 checkpoint 中只保留一个正在执行的步骤，准备和 dispatch 都比较观测到的版本；固定五分钟租约，没有候选 heartbeat。candidate_v2 保留空的 coordinator checkpoint JSON，把正在执行的步骤放进 [`evaluation_slot_claims`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_slot_claims.py)。Claim 主键是 `(run_id, case_id, slot_ordinal)`，同一候选槽生成与后续语义步骤也按此槽串行，其他槽可并行。

[`prepare_candidate`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_candidate_plan.py) 在锁内重新投影全部完成证据、dispatch 和 active Claims，排除已有 owner 的槽，再按 per-run limit 选 ready 步骤。semantic 只有在自己的 generation 已有可接受候选后才 ready；其他槽的已知失败不会遮住独立工作，但任何未处置 unknown 会停止新的 dispatch。提高 limit 不会跳过依赖、制造替换候选或绕过执行预算。

例如 coordinator 当前 v10，A 领取槽 S1 得到 Claim v11，B 随后领取 S2 得到 Claim v12。B 先完成时锁住当前 coordinator v12，核对自己的 Claim 和 ledger，再提交终态与 coordinator v13。A 后完成时核对自己的 Claim v11，但读取**当前 coordinator v13**，提交为 v14；不能要求共享 coordinator 仍是 A 领取时的 v11，否则合法乱序完成会被当成过期结果。这个行为在 [`completion_owner` / `complete_claim`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_candidate_completion.py) 中实现。

Claim 身份包含槽索引、Claim version、execution ID、invocation ID、owner 和 phase；expiry 单独校验。heartbeat 每 30 秒把租约延到当前时钟后五分钟，不改变 Claim version；旧的内存 Claim 仍可定位相同 owner 的续期记录。同槽释放后重新领取使用更高 Claim version，旧 owner 因身份不匹配被挡住。恢复还要比较**本次扫描观察到的 expiry**与当前 expiry，防止把已经续租的步骤误判为过期。

candidate 路径在模型返回后先单独提交 [`evaluation_response_receipts`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_response_receipts.py)，再投影候选和 Run：新回执要求精确 Claim / dispatch、invocation，以及 `finished_at ≤ at < 当前 lease expiry`，不能倒填 finished_at 来接受晚到响应。已存在的完全相同回执可以重放，任何不同字节都冲突；回执保存成功、投影失败时，恢复用原回执完成本地计算，不调用模型。serial 路径没有这层独立响应保存，不能把候选模式的恢复能力套给它。

候选 heartbeat 和生成监督器也不同：候选模型 await 期间不是 `FIRST_COMPLETED` 竞争取消，完成阶段才检查已结束 heartbeat 的异常，并由保存回执的 ownership / expiry guard 挡住迟到新证据。因此不能承诺“评测续租失败会立即终止供应商请求”。有其他 active Claims 时，终态投影继续排空它们；只有有限计划证据齐备且没有 active Claim，才进入待审。取消意图与 unknown 处置也遵循这个排空边界。

[`recover_candidate`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_candidate_recovery.py) 要求精确过期 Claim，按故障窗口处理：prepared 且没有 ledger 只释放 Claim 并推进 coordinator；dispatching 有原响应回执则复用；dispatching 无回执则生成 execution_interrupted 的 unknown 终态。恢复函数没有模型调用入口。一个后来重新 ready 的步骤仍须重新经过冻结恢复政策、执行预算和新 Claim 检查，不能把释放旧 Claim 当成重发授权。

## 本地 ModelCapacity：为生成留空间，不保证抢占或全局限流

[`ModelCapacity.try_acquire`](../../../src/qs_ai/application/execution/model_capacity.py) 的检查与加计数同步执行，中间没有 await；它依赖单事件循环，不提供跨线程或跨进程锁。对 Provider P，新调用都要满足 `active[P] < total[P]`；evaluation 还要同时满足：

```text
所有 Provider 的 evaluation 计数总和 < evaluation.parallel_calls
evaluation[P] < total[P] - generation_reserved[P]
```

例如 total=4、generation_reserved=1、global evaluation=3，三条 deepseek evaluation 可以并行，第四条 evaluation 即使改走空闲的 zhipu 仍因全局 evaluation 上限拒绝；一条 deepseek generation 可以取得剩下的第四个 token。generation 可以使用全部空闲 total；reserved 只限制 evaluation，不能抢占已经运行的 generation，也不承诺等待公平或响应时限。

生成新发送使用 `acquire_generation` 等待释放事件。等待时没有数据库 Session，也未提交新 dispatch marker，**但已经占有 Job lease 和 Participant active 槽**，heartbeat 继续续租。等待者被取消不会消耗 token。已经存在 ModelCall 的恢复直接走持久证据，不等待本地容量。

评测在短准备事务中 `try_acquire`，满了就抛 `CheckpointConflict` 并回滚本轮准备，候选 Claim、coordinator 变更和 dispatch 一起撤回，不持锁等待 token。成功取得 token 后，准备事务提交结束才调用模型。评测 token 直到执行图的 finally 才释放，涵盖调用、独立回执保存和投影；生成 token 在 DurableGeneration 的 finally 释放，业务 finish 在其后。两者都以幂等 release 清理异常和取消，但这个 release 不会释放持久日预算或业务槽。

## 调整并发前要核对哪些实际值

| 配置 | 仓库默认与约束 | 控制范围 |
| --- | --- | --- |
| `worker.concurrency` / `evaluation.concurrency` | default.yaml 各 1，范围 1–32 | 单实例 consumer 数；一个 consumer 等本轮结束后再领取 |
| `worker.lease_seconds` | default.yaml 30 秒，须 ≥3 | 生成租约；与固定五分钟评测租约分开 |
| `evaluation.candidate_mode_enabled` | Python 默认 false | 新 Run 存 serial_v1 / candidate_v2；已有 Run 按原持久模式执行 |
| `evaluation.per_run_parallel_calls` | Python 默认 1，范围 1–32 | worker 传给 Claim planner 的每 Run active Claim 上限 |
| `evaluation.parallel_calls` | Python 默认 1，范围 1–32 | 本进程全部 Provider 的 evaluation token 上限 |
| `model_capacity.deepseek/zhipu` | Python 默认各 total=2、reserved=1；1 ≤ total ≤32，0 ≤ reserved < total | 本进程 Provider 总 token 和评测可用部分 |
| Participant quota | default.yaml 日 org/user/assessment=500/5/3，active=10/2/1 | 组织有效策略还结合在线版本化配置与宿主 ceiling |
| Evaluation quota | local 日预算=1024，production=2048，active Run=1 | Start 的持久准入，按 Run 冻结策略预留 |
| Database pool / MQ `max_in_flight` | default.yaml pool=5、overflow=5；MQ Python 默认 1，范围 1–32 | 短事务连接 / 每个 Subscriber 的接收在途处理，不是模型 token |

[`Settings`](../../../src/qs_ai/config.py) 由配置文件、环境变量和显式构造参数形成有效值；default.yaml 中生成、评测均关闭。单实例增加 per-run limit，却保留一个 evaluation consumer，不能凭空产生多个并行模型步骤。管理读视图使用的 `EvaluationRuntimeLimits.parallel_calls` 是 `min(global evaluation, per-run, consumer)`；实际 worker 传入 planner 的仍是原 `per_run_parallel_calls`，不是这个展示值。

多个实例共享同一数据库时，业务 reservation、Claim 唯一性和当前 Run coordinator 跨实例生效。per-run limit 则由每个 worker 的运行配置传入，混用不同值不能视为一个已冻结的集群配置；应核对所有实例。ModelCapacity 计数和 consumer 数分别在每实例独立，扩容会增加可取得的本地 token，不能据此保证供应商账户总限额。进程崩溃会丢掉本地 token；旧未知请求可能仍在供应商执行，重建本地计数不证明外部在途数量已经归零。

常见背压应沿发生的层判断：日额度拒绝读持久接单回执；active 槽不足读 reservation / Job 延后；没有候选容量读当前 Claims 和 planner；Provider 容量满读有效配置与本地占用；连接池超时检查事务等待；429 则保留网关原分类，再按冻结恢复政策处理。提高 Subscriber `max_in_flight` 可能先增加接单事务与连接压力，不会扩展模型容量。崩溃窗口和人工处置入口见 [执行恢复](../execution-recovery/README.md)。

## 验证能证明到哪一层

纯测试核验规则与进程内行为；MySQL 集成测试另核验锁、唯一键、事务竞争和崩溃窗口。实际执行结果记录在文档变更的验证证据中，不能凭测试文件存在就宣布数据库或供应商验收通过。

| 测试入口 | 核心断言与证明范围 |
| --- | --- |
| [`test_model_capacity.py`](../../../tests/test_model_capacity.py) | 两 Provider 共享 evaluation 上限、生成预留、三个本地任务重叠、幂等 release、等待与取消；只证明进程内计数 |
| [`test_evaluation_parallel_plan.py`](../../../tests/test_evaluation_parallel_plan.py)、[`test_evaluation_active_dispatches.py`](../../../tests/test_evaluation_active_dispatches.py) | 排除 active、保留候选依赖、unknown 停止新工作、降 limit 不取消已领取步骤；仅精确 live Claim 可隐藏未完成 dispatch，缺失或矛盾证据报冲突 |
| [`test_evaluation_checkpoint.py`](../../../tests/test_evaluation_checkpoint.py)、[`test_evaluation_response_receipts.py`](../../../tests/test_evaluation_response_receipts.py) | checkpoint 身份 / 时序与响应编码 hash；不证明数据库行锁或并发唯一键 |
| [`test_config.py`](../../../tests/test_config.py)、[`test_quotas.py`](../../../tests/test_quotas.py)、[`test_worker_frozen_evidence.py`](../../../tests/test_worker_frozen_evidence.py) | 配置范围与优先级、quota 值/ceiling 解析、worker 使用冻结证据；不证明部署有效配置 |
| [`test_participant_capacity.py`](../../../tests/integration/test_participant_capacity.py)、[`test_evaluation_capacity.py`](../../../tests/integration/test_evaluation_capacity.py)、[`test_evaluation_capacity_recovery.py`](../../../tests/integration/test_evaluation_capacity_recovery.py) | MySQL 竞争接单只准入一份、活跃槽延后与复用、UTC 日与取消不退款、恢复原预算和未知取消保留槽；须测试数据库实际执行 |
| [`test_interpretation.py`](../../../tests/integration/test_interpretation.py) | 单次 durable dispatch、响应跨重领复用、旧 fence 不能写、heartbeat 保持租约、失租取消本地工作、三次领取预算；使用测试工作流，非真实模型 |
| [`test_evaluation_slot_claims.py`](../../../tests/integration/test_evaluation_slot_claims.py)、[`test_evaluation_candidate_execution.py`](../../../tests/integration/test_evaluation_candidate_execution.py) | 同槽唯一 owner、续租与重用 fence、拒绝回填晚到回执、三候选重叠和乱序完成、容量不足完整回滚、响应恢复零额外调用；使用 MySQL 与 fake gateway |

这些测试不证明供应商账户的真实限流、扩容后的峰值吞吐或线上积压排空时间。部署有效配置、数据库压力和真实调用结果需要各自的运行证据，不能由一次本地容量测试替代。

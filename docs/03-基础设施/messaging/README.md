# 消息接单、Outbox 与业务确认

qs-ai 通过消息受理命令，把可见状态和成果作为持久事件交给 QS。一次本地提交证明“业务效果与待发消息同时存在”；Broker 的发布确认成功证明 Broker 接收；QS 对原事件返回 `STORED` 才证明其接收事务完成。这三个完成点分别有记录，不能合并成一个“发送成功”。

本文以一次 Start 产生 Session、最终形成 Artifact 并交付 QS 为主线。核对基线：qs-ai `d9c3155`，QS `2ccc2de44`，2026-10-06。上述日期记录原业务链路；2026-10-19 的 M3 接入基础固定依赖 `fangcun-reliable-messaging[nsq,rocketmq]==0.4.0a1`（SDK 源码 `bfaae2b82ee0d1229b8c965fce472cdf5dec486f`）。业务尚未切换事务入口，测试、部署与真实业务接收仍是独立证据。

## RocketMQ 普通消息

`messaging.provider=rocketmq` 显式选择普通消息；默认仍为 `nsq`。RocketMQ endpoint、私有凭据、CA 与 version1 逻辑/物理路由清单由宿主冻结并只读挂载到 `/run/qs-ai-rocketmq/`。Python 的 `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH` 必须在进程启动前绑定同一 CA。Docker 部署脚本与离线 preflight 校验路径、权限、内容摘要与路由，SDK 不发现生产配置或创建资源。

运行时复用当前应用循环和数据库，SDK 在显式 start 时创建客户端及有界 blocking-driver executor。客户端实际取得路由和服务端 settings 后才报告启动完成。普通消息不会改变原命令 Inbox、Outbox、本地事务、业务回执、冻结配置和 unknown 模型调用的恢复规则。`publish_enabled=false` 在 Relay 扫描前暂停，避免维护期间消耗发布预算。

RocketMQ 终态交接携带 provider、原始 Wire 和实际物理来源，宿主仍先验证受保护的原命令或 ACK，再提交原持久失败记录，成功后才允许消费 ACK。NSQ 的 failure topic 中转只用于 NSQ 分支。当前业务尚未接入事务发送器，发布型 Outbox 继续存在。源码与测试通过不代表生产已经切换，生产版本、有效绑定和 `system` 账号业务验收需独立记录。

## M3 事务装配基础（业务接入进行中）

[`transaction_messaging.py`](../../../src/qs_ai/bootstrap/transaction_messaging.py) 提供独立的 `make_transaction_sender` 和 `TransactionRuntime`，借用宿主 primary `async_sessionmaker`，构造不创建 session、线程、客户端或表。必须显式配置 `messaging.transactions["ai.mysql"]` 的 `store_id`、`writer_epoch`、`route_revision=m3-1`，并校验同一可信 CA。v2 manifest 中只选择本 owner 的 TRANSACTION 事件路由；v1 不得夹带 v2 字段。

默认发送并发 4、Checker worker 1 / 等待队列 16，DB 回查 2 秒、RPC 5 秒、lease 60 秒、每 5 秒最多维护 100 条；覆盖预算必须给出全部九个字段。`publish_enabled=false` 只封新发送并保留 Checker；最终关闭通过 StopAdmission / Close / Wait 排空 SDK，然后由宿主关闭 DB 和 loop。`ShutdownIncomplete` 原样保留，不能当作成功关闭。

宿主迁移 `0041_transaction_messaging` 显式安装独立 schema v1；owner binding 必须由冻结清单另行安装，不由构造/Start 推断。Down 保留证据并拒绝回退；隔离原事务安装测试见 `tests/integration/test_transaction_schema.py`。

此入口目前未接入 command admission、状态 finalization 或统一 server。旧 `MessagingRuntime.create()` 遇到非空 transactions 明确拒绝，避免继续由 MQRelay 发送事务 owner 的消息。原根事务、首 wire、业务 receipt 和模型 unknown 恢复仍按下文执行；后续批次逐项替换，不因 SDK 具备事务能力就宣称业务已迁移。全局进度以 [infra M3 总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m3-execution.md) 为准。

## command 冻结与原根执行器（生产装配尚未启用）

`transaction_command.py` 在认证和正文解析之后持有原始 command bytes，以 owner、组织、原 command ID 和稳定 input hash 标识请求。完整计划保存首次 receipt/state 的 ID、正文、wire 和 v2 路由。无本地上下文时 archive/v1 的 bytes 和摘要不变；带上下文时显式采用宿主 archive/v2，最多 16 KiB，上下文计入本地计划与消息指纹，原业务 wire 不变。SDK codec/schema 保持原版本。读取持久化赢家时，逐分支校验原 command、组织、路由以及业务 receipt/state 的版本和关联；同一计划所有分支的上下文必须相同。

[`AdmissionRoot`](../../../src/qs_ai/infrastructure/workflow_transport/transaction_admission.py) 是传给 SDK 的单次本地执行器：在宿主原根先取得 SDK guard，借用冻结的 Inbox reservation 身份，执行宿主业务 action，比较真实业务结果与冻结 receipt，再将 Inbox、全部首次档案和 witness 一同提交。HELD 不调用业务 action；旧 Inbox 不能被冒充为新 witness。事务入口的 Inbox 决策不新增旧 messaging_outbox 行。

该执行器不规划业务、不签名、不发送半消息，也不重试业务。根被替换、提交回复丢失或证据不足返回 Unknown；只有原根回滚完成且持久化中止成功，才返回 Aborted。回复丢失后的权威读取采用新 session 边界并有预算。SDK 继续分别报告本地结果与 Broker 最终确认。

`StartPlanFacts` 保存冻结时间、原 input hash、session/run/evidence/job/Inbox/state 身份及精确配置依据。它采用显式版本和规范 JSON，拒绝重复/未知字段、错误身份/证据及不一致 receipt/state；并发失败方必须读取赢家的事实，不能继续使用自己新生成的候选。`plan_configuration` 复用原配置解析和输入校验，只读生成绑定依据；原根 `bind_configuration(expected=...)` 锁定并核对 pointer、publication、evidence 后才写入。冲突不转换为业务拒绝。

解释业务与 SQL job 的身份、Inbox reservation 可以借用预分配值；历史入口默认分配行为不变。START 已提供只读 `ReadOnlyStartUnitOfWork` 和完整 `StartAdmissionPlanner`：复用原服务规则，预查幂等、配置和容量，读事务关闭后才冻结 receipt/state 首次 wire。执行时 `make_start_root` 从 reservation 赢家读取 `StartPlanFacts`，借用原服务与 SQL 根，重校验配置和 UTC 预算日，明确验证状态后再写 witness/提交；`EventSession.commit` 不再编排这次发送。状态历史仍保留必要拒绝证据并标记 MQ 责任，事务入口不创建 messaging_outbox 发布责任。已完成 Inbox 不能因 token 相同而再次取得业务执行资格。

两类评测命令已增加只读完整规划和原根装配，待新候选原生验证；实际 command receiver 装配和真实 Broker 验收仍待完成，旧 runtime 对 transactions 配置的拒绝继续保留。`tests/integration/test_transaction_admission_root.py` 提供两种宿主档案格式的隔离 MySQL HELD 根原子性、服务端拒绝和客户端丢弃已提交回复用例；`test_execution_configurations.py` 增加真实只读规划、匹配绑定及 pointer 冲突用例。候选80805fb的两档MySQL完整CI各3242项零失败/跳过，已下载核对八份分片覆盖；这是原根/配置与历史回归的证据。后续真实只读START、并发赢家、原业务+witness及显式Replan旧证据验证用例已加入，待新候选CI。它们仍不计为Broker、网络故障或完整业务链路验收。

## T17 独立状态冻结与原根基础（实施中）

T16 command 组件已合并并通过隔离原事务及固定发布 SDK/Broker 验证，统一生产装配仍保持关闭。下文的旧候选记录保留排查经过；当前任务、失败历史和验收状态只以 [infra M3 总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m3-execution.md) 为准。

[`state_projection.py`](../../../src/qs_ai/infrastructure/workflow_transport/state_projection.py) 共享原解释/评测可见状态投影，不读取数据库、不创建 ID 或时间、不签名。原普通消息 recorder 与事务状态核验使用同一投影；同评测版本复用原 sequence，版本回退拒绝。

[`EvaluationStateIntent`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_state_plan.py) 为非 command 状态提供独立稳定请求。宿主必须提供原授权操作键、原 expected version 和稳定业务输入摘要，不能使用候选时钟、密文或会在提交后变化的当前业务行代替原输入。冻结事实保存首次 UTC 时间、前序 sequence/version；完整计划只接受精确 owner/组织/Run/目标版本/物理路由及原首次 wire。读取持久化赢家不需要密钥，不重新 Seal，也不借用 command Inbox。

[`EvaluationStateRoot`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_state_root.py) 从赢家组装单次本地执行器。SDK 负责先准备半消息；宿主原根第一条条件写取得 guard，然后原业务适配器重校验授权和业务条件、写业务事实。keyless 状态转录精确核验可见状态、竞争原 sequence CAS，再显式写档案/witness 并提交；`EventSession.commit()` 不编排该新状态。未完成或失败的转录也不得移动到另一个根事务。

command 与独立状态共用 [`commit_original`](../../../src/qs_ai/infrastructure/workflow_transport/transaction_root.py) 的根提交/新读取边界：提交回复不明只查询精确 witness，原根回滚且持久化中止成功才证明 Aborted；取消和连接错误不构成回滚证明。宿主继续持有连接池、业务条件和生命周期。新状态入口不创建旧发布 Outbox，不自动 Retry/Replan 或调用模型。

worker 预检通过显式 `preflight_dispatch` 借用 [`PreflightStateDispatcher`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_preflight_dispatch.py)。先按原 Run/version/组织及稳定操作键查询 SDK 请求；已提交的旧 hint 只核验首次证据，不因当前 checkpoint 已推进而重新规划。未决或中止记录需要精确证明或显式恢复，不自动创建新执行。

[`PreflightStatePlanner`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_preflight_state.py) 只读当前 Run/checkpoint、原登记 suite 与序号，关闭读取后再计算原无模型预检、生成身份和第一次 Seal。赢家上下文冻结首 PreflightEvidence、时间及源快照摘要；原执行器锁定并重新核验源，再调用原 preflight 完成规则。稳定请求输入和可变源快照分别保存，避免已提交重投因业务推进而误报输入冲突。

空业务上下文保持已有状态 facts/v1 的每个字节；非空预检上下文采用显式 facts/v2，原始操作上下文最多8 KiB，整个宿主上下文仍最多16 KiB。读取支持两个显式版本，SDK codec/schema及原业务 wire 不变。worker 的新入口只接受精确原根提交证明；`LocalCommitted + FinalUnknown`（含SDK结构化异常）保留Broker确认责任，不授权模型重做。默认装配继续保留原入口，事务运行装配须等待全部状态、ACK和历史责任接入。

原 [`prepare_step`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_step.py) 的准备与 dispatch 会在一个根事务内连续推进两个 checkpoint 版本。独立状态计划允许冻结高于原 expected version 的最终版本，仍只递增一次消息 sequence、发布一条最终状态，不拆分原提交或发布中间版本。首次 wire、上下文 v1/v2 和 SDK codec 不变；原根转录仍精确比较最终数据库状态与冻结消息。

治理定稿与重开分别复用 [`finalized_progress`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_finalization.py) 和 [`reopened_progress`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_reopening.py) 的纯进度投影。宿主先取得有作用域的有效门槛快照；原事务继续持锁、重新验证门槛并提交。拒绝历史、首次审核和修正记录保持原规则。抽出投影不表示治理 RPC 已接入事务消息。

[`GovernanceStateOperation`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_governance_state.py) 冻结宿主已授权的定稿/重开操作、原版本、操作者、理由和明确确认。操作 ID 由宿主在数据库根事务外分配，不新增公共跨 RPC 幂等承诺。稳定业务输入摘要与可变源快照摘要分别保存；两者都绑定首次状态计划。

`GovernanceStatePlanner` 在显式 REPEATABLE READ 只读边界中复用原门槛/拒绝证据和纯投影，读取结束后才第一次 Seal。`GovernanceStateRoot` 在 guard 前设置同一隔离级别，在原根中锁定 Run/checkpoint、重验源快照和业务门槛，再提交状态、原审核历史、sequence、档案和 witness。构造不产生资源或 DDL；两种原治理操作仍不执行模型。原生验收用例现为12项，包含直接RPC处理函数及数据库原子性边界；数据库专用runtime不冒充SDK半消息或Broker证明，执行状态以infra总表为准。治理默认RPC装配与最终运行装配尚未切换。

[`GovernanceStateDispatcher`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_governance_dispatch.py) 先查原请求再规划/准备，使用持久化赢家并仅执行一次。只有精确 witness 和首次档案都证明原提交，才返回原根中取得的原响应；不读取今天的 Run 来替代该次响应。`LocalCommitted + FinalUnknown` 保留确认责任；原 CAS/业务错误只有在原根已证实 Aborted 时才能继续按原错误返回，未知结果不改写为冲突或授权业务重做。

[`TransactionalEvaluationManagement`](../../../src/qs_ai/infrastructure/workflow_transport/transactional_evaluation_management.py) 为定稿/重开提供显式管理适配器。原RPC处理函数继续校验QS工作负载、主体、版本和确认，服务端首次分配操作ID/时间；每个新RPC仍遵循原CAS行为，不新增公共幂等承诺。其余写入口继承原实现，因此默认容器/生产装配仍等待完整迁移，不能提前启用混合运行。

原 [`accept_reviews/plan_reviews`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_reviews.py) 共享评审准入与投影。`plan_reviews(lock=False)` 仅使用宿主明确的 RR 只读快照，不推进 checkpoint、写业务或提交；原写入口仍先锁 checkpoint/Run，再写原 CAS 与审核事实。原 1–35 项、单项理由和语义评审边界继续有效，不因消息元数据预算缩小合法输入。只读规划和原写入口共享规则；[`ReviewStatePlanner/ReviewStateRoot`](../../../src/qs_ai/infrastructure/workflow_transport/review_state.py) 已提供显式评审事务入口，但默认生产装配仍未启用。相关真实 MySQL 用例执行状态以 infra 总表为准。

本批尚未替换全部 worker、治理和终态入口。MySQL 原状态根、实际 worker 及固定 SDK/Broker profile 分别验证；当前进度和失败记录回链 infra 总表，不据单项通过启用统一 runtime 或标记 T17 完成。

## 当前服务怎样装配这条链路

统一入口 `bootstrap.server.serve()` 要求 `messaging.enabled=true`；缺少时直接启动失败。配置模型的默认值仍为 false，意味着调用方必须显式绑定所选 Broker、JOSE 密钥和 QS mTLS 载荷端点，不会自动启用开发传输。旧 `bootstrap.integration` 入口已经退役，当前没有并行运行的 gRPC 结果扫描器。

`MessagingRuntime.create()` 取得应用的 `Database`，创建一个使用该池的 `Transactions`，先在只读一致快照中检查必需的技术观测表及类别，再安装 `database.state_events`。随后装配命令 receiver、两个订阅器、Publisher、载荷端点及一个 MQRelay。资源由宿主生命周期启动和关闭：

| 资源或参数 | 实际所有者与限制 |
| --- | --- |
| MySQL engine/pool | APP 的 Database；SDK 和 MessagingRuntime 使用原池，运行时关闭不 dispose 该池 |
| NSQD 地址 | `messaging.nsqd` 显式提供 TCP 地址到 HTTP origin 的映射，最多 8 个；Relay 固定选择第一个配置地址，未实现轮换发往其他 Broker 的策略 |
| QS 载荷通道 | MessagingRuntime 自己创建并关闭到 `grpc.access_address` 的 mTLS channel；它与 EvidenceSource 的 QS 通道各自拥有生命周期 |
| 订阅和 Publisher | 所有 Publisher 就绪后才启动业务订阅；失败交接订阅先于业务消息。退出时先排空业务处理，再停止 Publisher，避免排空阶段失去回执及 failure handoff 通道 |
| Relay 调度 | 复用统一服务的后台循环；`concurrency` 强制为 1，MQRelay 的进程内 gate 拒绝同一扫描器重叠执行，没有独立守护进程或分布式领取租约 |

SDK 不替宿主创建表、迁移、连接池或接单事务。`bind(db)` 要求已存在且仍活跃的原 SQLAlchemy 根事务及相同 savepoint 身份，并验证真实 MySQL driver 为非 autocommit；不能因为 ORM 标记为“事务中”就接受另一个隐式提交边界。作用域和借用规则见 [资源所有权设计](../../01-运行时/02-Dishka作用域与资源所有权.md)。

## 一次 Start 在什么时候算持久受理

消息方向及固定主题为：

| 方向 | Topic / 消息 | 接收方必须提交的结果 |
| --- | --- | --- |
| QS → AI | `qs.ai.commands.v1`：Start、Change、ParticipantRetry、EvaluationStart、EvaluationCancel | 原命令 Inbox、accepted/rejected/held 决定和首 CommandReceipt；成功受理时还包括原业务效果及对应状态消息 |
| AI → QS | `qs.ai.events.v1`：CommandReceipt、InterpretationState、EvaluationState | 原事件 Inbox、接收投影或技术保留决定、首 EventAcknowledgement Outbox |
| QS → AI | `qs.ai.acks.v1`：EventAcknowledgement | 原事件确认或 held；解读事件的 STORED 还要同步原 `interpretation_result_outbox.delivered` |

`CommandReceiver.receive_command()` 先验证保护上下文，获取并核对原正文，然后建立根事务。`reserve_command()` 用 `(producer, message_id)` 插入 processing 保留，并锁原 Inbox；只有首次保留者进入业务 admission。

```mermaid
sequenceDiagram
    participant Q as QS命令
    participant R as AI Receiver
    participant B as 原业务用例
    participant D as 原MySQL事务
    participant N as NSQ
    Q->>R: 原message_id与受保护正文
    R->>D: Inbox保留并锁定
    alt 首次命令
        R->>B: 借用原Session，在savepoint内受理
        B->>D: Session/Job/冻结绑定与原状态
        R->>D: 首决定与首回执Outbox
    else 同身份与同正文重复
        D-->>R: 原决定及receipt_id
    end
    R->>D: 根commit
    R-->>N: handler返回，允许FIN
```

接单不等候模型。`WorkflowCommandAdmission` 在局部 savepoint 内调用原业务用例，把 `Transactions.borrowed(db)` 传进去；它的 `commit()` 只校验原事务，不提交或关闭根。确定性权限、资源、规则或版本拒绝撤销局部写入，并恢复评测事件标记集合，receiver 仍可提交 Inbox 与拒绝回执。数据库、网络和未知错误逃逸，整个根回滚，不把它们伪装为业务拒绝。

例如第一次 Start 因 `configuration_unavailable` 被拒绝，随后管理员完成发布：相同原命令重投仍返回原决定，不重新读取当前发布来改变结果。第一次已接受但 NSQ FIN 丢失也一样：Inbox 去重在业务 CAS 和再次冻结之前，不会再创建一个 Job。真正的 ParticipantRetry 必须作为另一条业务命令经过原资格校验；运输层重投不会获得这个权限。

## 原身份、正文指纹和首 wire 分别保护什么

业务重复按逻辑身份判断，NSQ 自己的物理 delivery ID、attempts 及新的加密字节都不能代替它。

| 记录 | 身份与冲突校验 |
| --- | --- |
| `messaging_inbox` | 主键 `(producer, message_id)`；原 `body_sha256`、body 字节、kind、aggregate_key 都必须一致，保存首 `receipt_id`；同 ID 换正文进入 quarantine |
| `messaging_outbox` | 主键 `(producer, destination, message_id)`；保存原 body、body_sha256、first wire、wire_sha256、topic/kind/组织、aggregate_key/sequence、stage、attempts 及时间 |
| `interpretation_result_outbox` | 原解读 `event_id` 主键，`(session_id, version)` 唯一；保存原 StateEvent JSON、delivered、mq_owned 和历史投递元数据 |
| CommandReceipt | 回执有自己的 event ID；正文携带原 `command_id` 和 `command_body_sha256`，envelope 的 `correlation_command_id` 必须等于原命令 ID |
| EventAcknowledgement | ACK envelope 有自己的 ID；正文的 `event_id/event_kind/event_body_sha256` 指向被确认的原 AI 事件，而不是把 ACK 自己当作成果事件 |

`body_sha256` 是 64 位十六进制 SHA-256，覆盖确定性序列化后的原 `MessagingBody` protobuf 字节。`wire_sha256` 覆盖包含保护封装的实际运输字节。JOSE 重新加密同一正文可以得到不同 wire，所以两者不等价；重复 stage 只核对原业务身份和正文，保留首次持久 wire，不把刚 seal 的新 wire 覆盖进去。

旧结果移交时还使用 `payload_json_sha256` 作为 MySQL JSON 源快照的审核摘要。它既不是 protobuf body hash，也不是 wire hash；不能拿一种指纹去确认另一种记录。

## 状态事件怎样与原业务一起提交

外部解读 Session 保存状态时，`stage_state(db, session)` 在原 Session 事务里构造 StateEvent。completed 状态必须已有持久 Artifact，事件中的 `artifact_json` 来自该原成果；缺少成果则拒绝完成写入。

`interpretation_result_outbox` 的重复插入保留首次 event ID。状态 recorder 随后读取这一原记录，用它生成 `INTERPRETATION_STATE`，同事务写 `messaging_outbox` 并设置 `mq_owned=true`。重复保存同一 Session/version 不更换 UUID、body 或 first wire，不增加模型调用。这个表仍是解读的原结果证据源，当前实际扫描与 PUB 由消息 Outbox 承担；SDK 中保留的 `MySQLPendingOutbox` 适配器没有接入当前 qs-ai 运行路径。

评测状态通过另一入口进入同一提交边界：`changed_evaluation()` 把变更的 Run ID 放入 Session 的 `evaluation_events` 集合。宿主 `EventSession.commit()` 在真正 commit **之前**调用 `StateEventRecorder.flush()`，读取已完成的 Run 进度与检查点版本，写 EvaluationState 和 sequence，再提交业务及消息；flush 失败则没有成功根提交。

评测 event ID 为 `uuid5(UUID(run_id), "state:{version}")`，`event_sequence` 在记录的新版本上递增，同版本重复保留 sequence。事件只含 Run/组织、version/sequence、status、release 指纹，是持久状态投影，不传活跃检查点或执行指令。当前 AI 事件统一 `ordered=false`，因此消息可能乱序到达；QS 用原 Session 版本或 Run event_sequence 控制投影推进，Broker 不负责替业务排序。

## Relay 的每一步与三个不同的预算

当前 MQRelay 每轮取最多 20 条 `staged/awaiting_receipt` 且到期的记录，按 available_at/message_id 排序，串行发送保存的原 wire。扫描在自己的短读事务中完成；外部 PUB 不持有业务事务，结算另开宿主短事务锁定原消息及 body hash。

| PUB 或确认结果 | 持久变化 | 后续处理 |
| --- | --- | --- |
| `CONFIRMED + BROKER` | 调用 `published()`，写 published_at；AI 事件要求业务回执，stage 变 `awaiting_receipt`、attempts+1 | 默认等待 30 秒，未收到业务 ACK 则再次发送原 wire |
| Unknown | attempts+1，保留原 stage，available_at 延后，error_code=`publish_unknown` | 退避 `min(60, 2^min(原attempts+1,6))` 秒；不能推断 Broker 没接收 |
| 明确 Rejected | stage=`held`、error_code=`publish_rejected` | 不再自动 PUB |
| 扫描到 attempts ≥ 8 | stage=`held`、error_code=`delivery_budget_exhausted` | 不发第九次；保留原记录等待受控处理 |
| 精确可信 STORED | stage=`confirmed`、confirmed_at | 完成原事件业务确认；晚到 PUB 结算不能把它退回等待状态 |

持久 stage 中没有名为 published 的状态；`published()` 是函数，业务确认前的状态是 `awaiting_receipt`。成功 PUB 也占用 AI 事件的投递次数：连续 8 次 Broker OK 却没有业务 ACK，仍会停止自动发送。held 停止自动 PUB，但之后精确可信的 STORED 仍可完成原确认；TECHNICALLY_HELD 则不能覆盖已 confirmed 的事实。

需要分别看待三类次数：

- **Outbox attempts**：当前 AI 事件外发及等待业务确认的预算，PUB 成功或 unknown 都推进。历史结果移交继承已有 attempts 和 available_at。
- **本地逻辑技术预算**：经过 JOSE 和正文核验的同一命令发生技术失败，admission 根回滚后，在另一个短事务把 `(logical_producer, logical_message_id, logical_body_sha256)` 的失败记到账；达到 8 次才允许持久 HELD 决定。
- **NSQ 物理 attempts**：订阅重投和 failure handoff 的运输计数。伪造高次数或重新生成 failure wrapper 都不能直接消耗本地逻辑预算，也不能产生业务拒绝。

SDK 订阅在 handler 返回之后才 FIN；处理异常会 REQ，Task 取消不 FIN。到物理阈值后先把原受保护消息交给预配置 failure topic/channel，Broker 确认交接才结束原 delivery。failed handler 从 wrapper 恢复原 wire 并重新认证，绝不再次调用 admission；只有本地已经记满的逻辑预算可形成 HELD。quarantine 或失败账本写不成功就继续传播错误，不能先 FIN 再期望补记。

进程内 gate 和单个宿主调度器提供当前单进程扫描约束；pending 查询没有分布式 claim/fencing。部署第二个独立 Relay 不能据此推断已经取得排他传输所有权。

## QS STORED 在两边各自落下什么

QS 的 [`MessagingEventReceiver`](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/infra/mysql/aibridge/messaging_receiver.go) 创建接收根事务；[`ReceiveEvent`](https://github.com/FangcunMount/qs-server/blob/2ccc2de44bbd45e26d29e7e130da518cc32426f0/internal/apiserver/infra/mysql/aibridge/messaging.go) 在同一事务保留原 Inbox、调用业务接收投影，再生成并保存最终 ACK Outbox。解读成果还校验原请求的 Actor、Testee、Session 和 Artifact 的 Assessment/Report/source_version，不能仅因消息解密成功就接受另一份报告。

同一事件重复到达时，QS 在业务投影之前识别首 Inbox，复用并重新准备发送原 ACK，不重做业务效果。较旧状态已被更高版本替代，也可作为合法原事件完成幂等接收；STORED 不表示页面当前一定展示这一版，更不表示用户验收。

AI 收到 ACK 后再建自己的根事务。若是 INTERPRETATION_STATE，按与移交一致的顺序先锁原 `interpretation_result_outbox`，再锁消息 Outbox，核对 event ID、kind、aggregate_key 与原 body hash；正常解读stage已在同一事务保留两层记录；读取到原源行时，STORED 还核对其 `mq_owned`、原 Session/version 和完整 StateEvent 正文，正确时同时设置消息 confirmed 和原结果 delivered。该函数没有把“缺少 legacy 源行”另设为拒绝条件，不能以消息 confirmed 单独证明源行完整。任一步失败，本次变化一起回滚。

重复 STORED 核对身份后保留第一次 confirmed_at/delivered_at。TECHNICALLY_HELD 只设置消息 held，原结果不变成 delivered。failed ACK 只记录技术隔离，不能制造 STORED。可靠消息 SDK 的 `Confirmation.BROKER` 与 `Confirmation.DURABLE_ACCEPTANCE` 也是不同确认类型，不能把 Publisher 返回值传给原业务接收回调充当持久确认。

## 四个故障窗口决定下一步

| 中断位置 | 已留下的事实 | 恢复动作 |
| --- | --- | --- |
| AI 业务写入后，首回执/状态 Outbox 写失败 | 根回滚，Inbox 与局部业务效果都没有成功接受 | 原命令可运输重投，再受理；不保存假的 accepted |
| 根 commit 成功，NSQ FIN 或命令回执丢失 | 原 Inbox、业务效果及首回执都存在 | 相同命令复用首决定；Relay 重发原回执 wire |
| PUB 已到 Broker，但进程在 Outbox 结算前消失 | 消息仍待发，是否收到运输确认未知 | 下次发送原 wire，QS 用原 Inbox 去重；不重新运行模型 |
| QS 接收及 ACK Outbox 已提交，但 ACK 未到 AI | QS 原业务效果存在，AI 仍 awaiting_receipt | AI 重发原事件，QS 复用原 ACK；直到 AI 原确认事务成功 |

消息传输 unknown 描述 PUB/接收结果不确定，可以在有界预算内重发同一已提交事件。模型 `provider_result_unknown` 描述供应商原调用的结果不确定，禁止据此自动二次调用；前者的重投不能成为后者的替代授权，详见 [解读执行边界](../../02-业务模块/interpretation/01-解读冻结输入与成果设计.md)及 [评测恢复](../../02-业务模块/evaluation/01-评测候选门槛与审核设计.md)。

## 保护和大正文仍绑定同一原消息

`MessagingEnvelope` 固定 `qs-ai-messaging/v1`，保护上下文同时绑定 producer、destination、实际 topic、message_id 和 `secure_profile=rm-secure-v1`。订阅前读取并验证 EC/P-256 JOSE 密钥、公私钥角色及 kid；TLS 端点与消息保护身份共同限制调用方，但不代替业务正文核验。

正文不超过 32 KiB 时 inline，更大时在首次 seal 之前选择 `MessagePayloadReference`，没有先加密再临时换载荷的路径。数据库 body 上限为 MEDIUMBLOB 的 `16 MiB-1`，wire 上限 262144 字节，并预留 failure handoff 编码余量；单项业务命令及 Artifact 的原大小限制仍适用。

reference 只有 producer/destination/message_id/hash/length/organization，没有 URL。AI 的 `PayloadResolver` 只从预配置的 QS mTLS stub 读取，最长 5 秒；返回的 reference、长度、原 hash 与正文组织必须一致。QS 读取 AI 事件大正文时也按准确原 Outbox reference 授权。ACK 必须 inline，不能夹带任意载荷访问指令。

载荷读取超时或存储不可用传播为技术 Unknown，允许运输 REQ；内容、组织或引用不符则隔离。观测计数只是安装以后记下的技术事实，不是完整历史、业务成功率或额外重试权限。

## 配置与旧结果移交的实际约束

运行配置必须提供 `messaging.enabled/nsqd/signing_key_file/decrypt_key_files/qs_signer_files/qs_recipient_key_file/max_in_flight`，以及 `grpc.access_address`、CA 和客户端证书。配置模型允许 max_in_flight 1–32，当前部署绑定生成器只接受 1，并要求显式 `binding_revision`、TCP 与同源 HTTP endpoint，以及 `/run/qs-ai-jose/` 下按角色和 kid 匹配的密钥文件。

统一服务使用 `delivery.idle_seconds/max_backoff_seconds/shutdown_seconds` 调度 Relay；保留字段 `delivery.batch_size/max_retry_seconds` 没有传入当前 MQRelay。实际每轮 20、外发 8 次、回执等待 30 秒和重投上限 60 秒来自当前实现，不能通过修改那两个旧字段宣称已经改变消息规则。

迁移 `0037_workflow_messaging` 创建 Inbox/Outbox/quarantine/evaluation_sequences 并增加 mq_owned，`0038_messaging_observations` 提供启动必需的技术观测；运行时不安装或修复 schema。服务启动比对镜像迁移 head，0037 downgrade 明确拒绝删除已持有的消息证据；回滚需要兼容这些记录的镜像，不能恢复旧 gRPC 扫描器与 MQ 同时发送。

普通 Relay 不扫描或接管历史未移交的 `interpretation_result_outbox`。受控 `messaging_handoff` 只处理显式选择的 1–20 个原 event ID：

1. dry-run 读取原库及 schema head、源 Session/version/JSON 摘要、attempts、available_at、创建时间、归属和 first wire，生成审核 manifest/digest，不 PUB。
2. apply 要求该 manifest/digest 和“所有领取者停止且准入关闭”的明确声明；逐条在短事务内锁源记录，再锁消息 Outbox，核对原快照与数据库头。
3. 使用原 event ID、原时间、原投递预算保存 first wire 和 mq_owned；已有 delivered 或 mq_owned 不复活，已有 wire 不重 seal，attempts ≥ 8 的原记录直接继承 held。

apply 是逐条提交，不是整批一个事务。第一条已提交而第二条源快照变化时，结果必须保留 partial；commit 返回失败则报告该条 commit_unknown，不能断言没移交，也不能用重置预算来“修复”。这个维护入口改变原消息的传输归属，不创建任务、不改 frozen configuration、不调用模型。

## 代码与测试怎样查证

| 机制 | 当前源码 | 测试边界 |
| --- | --- | --- |
| 原事务接单、首回执和重复 | [receiver](../../../src/qs_ai/infrastructure/workflow_transport/mq_receiver.py)、[admission](../../../src/qs_ai/infrastructure/workflow_transport/command_admission.py)、[宿主表及确认](../../../src/qs_ai/infrastructure/persistence/mysql/messaging.py) | [存储集成](../../../tests/integration/test_mq_storage.py)、[真实业务 admission 集成](../../../tests/integration/test_mq_admission.py)：拒绝/技术失败回滚、重复先于 CAS、logical budget、原业务 ACK 原子性 |
| 状态与原结果 | [stage_state](../../../src/qs_ai/infrastructure/persistence/mysql/result_outbox.py)、[recorder](../../../src/qs_ai/infrastructure/workflow_transport/state_events.py)、[EventSession](../../../src/qs_ai/infrastructure/persistence/mysql/database.py) | [原结果集成](../../../tests/integration/test_result_outbox.py)、[MQ 成果集成](../../../tests/integration/test_m7_delivery.py)、[评测 worker 事件集成](../../../tests/integration/test_mq_worker_events.py)：原 Artifact/first wire、事件序列、无额外模型调用 |
| PUB、等待回执与 gate | [MQRelay](../../../src/qs_ai/infrastructure/workflow_transport/mq_relay.py)、[SDK durable](https://github.com/FangcunMount/reliable-messaging/blob/8633ba585b5a1c8f95fb16b19469fbd0097611c7/python/src/reliable_messaging/durable.py)、[SDK NSQ](https://github.com/FangcunMount/reliable-messaging/blob/8633ba585b5a1c8f95fb16b19469fbd0097611c7/python/src/reliable_messaging/nsq.py) | [存储集成的 Relay 用例](../../../tests/integration/test_mq_storage.py)、[运行时单元](../../../tests/test_messaging_runtime.py)：Broker OK 仍等待业务 ACK、重发原 wire、8 次停止、Publisher 先启后停；NSQ 真实拓扑另见 [拓扑集成](../../../tests/integration/test_mq_topology.py) |
| 保护与载荷 | [保护契约](../../../src/qs_ai/infrastructure/workflow_transport/messaging.py)、[载荷解析](../../../src/qs_ai/infrastructure/workflow_transport/payloads.py)、[协议](../../../integrations/workflow/proto/messaging.proto) | [契约单元](../../../tests/test_mq_contract.py)、[失败交接单元](../../../tests/test_mq_failure.py)、存储集成：错 topic/key/kind、原大载荷、精确 ACK；Go/Python 互操作需要其显式环境 |
| 所有权、启动和迁移 | [运行时装配](../../../src/qs_ai/bootstrap/messaging.py)、[统一入口](../../../src/qs_ai/bootstrap/server.py)、[SDK 原事务校验](https://github.com/FangcunMount/reliable-messaging/blob/8633ba585b5a1c8f95fb16b19469fbd0097611c7/python/src/reliable_messaging/sqlalchemy.py)、[维护移交](../../../src/qs_ai/maintenance/legacy_results.py) | [旧入口退役](../../../tests/test_legacy_delivery_retirement.py)、[部署约束](../../../tests/test_mq_deployment.py)、[单记录移交集成](../../../tests/integration/test_mq_legacy_handoff.py)、[维护工具集成](../../../tests/integration/test_mq_handoff_tool.py)：原池保留、预算/首 wire 不改、源漂移拒绝、部分提交和进程中断 |

[ai-messaging CI](../../../.github/workflows/ai-messaging.yml) 与 [M7 perimeter CI](../../../.github/workflows/m7-messaging.yml) 分别覆盖存储、传输及原成果边界，必需验收报告拒绝 skip/空报告。文件里有测试入口不表示当前环境已运行；合成模型、假传输和一次性数据库也不证明线上 Broker 持久性或某个真实报告已经被 QS 接受。实际交付要保留原 event ID/hash、两侧接收记录、STORED 及运行版本，技术连通和人工业务验收继续分开。


## M3 事务档案基础尚未接入生产业务

0041显式迁移新增 `messaging_archive`，单独保存原组织、首次 header/body/wire 和 SDK 精确引用。构造不安装 schema。`TransactionArchive.bind` 首先借原 AsyncSession 根事务取得 guard，`put` 从 reservation 赢家的冻结计划读取，`commit_witness` 只暂存证据；宿主提交业务、档案和 witness，任何失败均须整根回滚。不会在 callback 内重新 Seal、分配身份或发消息。

本地 `qs-ai-archive/v1` codec 使用固定大端无符号整数、原 protobuf header/body 字节与显式长度。包含未知字段的原 header 不经重建，Broker 仍收到首次 wire。跨语言向量覆盖 uint64 上界和字节边界；其中假密文只证明 codec，不表示认证成功。冻结计划仍受 SDK 大小/分支上限约束。

`ArchivePayloadReader` 是显式可选 reader，先验证新档案的 owner/store、精确 SDK 指纹、wire hash、组织及 execution/branch 双 committed 证据；Prepared、缺证据、损坏或不匹配直接拒绝。只有新身份不存在才走旧 MessagingStore.payload，原 mTLS workload、组织、hash/length 授权仍有效。没有新扫描器、attempt、due、业务回执结算或证据清理；业务受理及 StateEventRecorder 仍沿原链路，后续任务才接入事务 Sender。

0041保留0040的旧业务/Outbox物理布局，旧消息移交和只读 inventory 可以读取这些保留表；不由此授权旧全库转换/备份工具操作0041。历史转换 CLI 测试显式指向0040，新镜像启动校验测试使用0041。真实数据库档案原子性由隔离 M3 schema 门禁验证，不能把本地 codec 测试当成生产验收。

T16 新原生 START 验收曾发现配额预览间接取得权威行锁；预览现在显式使用无锁配额读取，原提交仍保持锁。原生测试清理中误用 SDK `Request.key_digest` 属性也已修正。失败报告保留于 infra 总表；修复候选384a440的完整CI12项与消息兼容2项均通过；八份下载分片各档3339项零失败/跳过，无遗漏或重复，测试合并SHA f907af2c 的父提交已核对。每档包含8项HELD根、2项只读START及3项完整START原根证明。新CHANGE批次不沿用这次成功结论。CHANGE 已新增只读规则规划、完整首次 receipt/state、冻结身份与有界目标授权，以及原根执行器。规划使用原 answer/cancel 规则；原根锁定当前 Session 再核验主体与授权目标，不调用远程客户端。授权 Unknown 不构造允许/拒绝证明；60秒或跨预算日的过期观察中止，需宿主显式重建计划。缺少 Session 或当前主体不符，按原确定性拒绝生成实际结果，冻结结果不同时整根回滚。START 严格入口保留，通用解释状态转录共用第一历史记录核验；不创建新 messaging_outbox 发布责任。候选62f5053已通过完整12检查与消息兼容2检查，两档MySQL各3381项零失败/跳过，八份分片覆盖已核对，含新增4项真实CHANGE原根证明，也尚未接入实际 receiver/Broker。

PARTICIPANT_RETRY 已复用原风险确认、blocked Run/CAS、首次模型请求和业务重试授权，新增只读规划与原根冻结资源。完整冻结计划包含首次 receipt/state、当前原参与者授权观察及原重试档案摘要；原事务重新核验原主体、目标和档案，再共同提交业务/witness，不在消息回查或故障恢复时授权模型重做。候选8264093的完整12检查及消息兼容2检查成功，两档MySQL各3408项零失败/跳过，包含新增3项原生重试证明；只读/原根组件的定向通过不替代实际SDK/Broker验收。CHANGE与RETRY共用有明确类型版本的解释变更上下文，CHANGE既有bytes有固定摘要向量保护；SDK codec/schema和历史archive/v1未改动。

重试批次 b210b84 本地完整纯单元2527通过（881项integration/interop按标记未选，不当作真实依赖验收），mypy374源文件、Ruff和文档门禁通过。8264093的完整CI已覆盖原入口回归、共享上下文和三项原生重试证明；T16仍不勾选，实际receiver、两类评测命令原生复验与Broker验证待完成。


### 评测启动与取消的冻结原根（独立组件）

`EvaluationAdmissionProjection` 组合原 requested 转移、容量估算、审计视图与取消规则，SELECT 规划不取得行锁、不预占预算。`EvaluationAdmissionPlanner` 在独立一致性读取中保存原 Run/checkpoint 摘要、原事件序号和完整业务结果；关闭读取后才签名、加密 receipt/state。取消中的未知或已 dispatch 工作仍保留原 checkpoint，生成 `cancel_requested`，不会伪造终态或再次调用模型。

`EvaluationPlanFacts` 是有界宿主上下文，精确绑定原命令、UTC 时间、Inbox 身份、源摘要和前序序号；拒绝重复字段、非规范编码、错类型与错关联。状态身份沿用原 `uuid5(run_id, state:version)`，新事务事件的时间是这次已冻结操作时间；历史 wire 不改写。

`make_evaluation_root` 在首条 guard SQL 前显式设置本事务 READ COMMITTED；普通入口及其他三类 command 默认隔离不变。START 随后先取得原组织准入锁，再锁定 checkpoint/Run 并重新验证冻结摘要，避免 guard 的早期读取固定旧容量快照。这个顺序依据 [SQLAlchemy 单事务隔离设置](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#setting-isolation-for-individual-transactions)及 [MySQL 一致性读取规则](https://dev.mysql.com/doc/refman/8.4/en/innodb-consistent-read.html)。数据库连接池和连接生命周期仍由宿主拥有。

原服务只借用根事务；`FrozenEvaluationEvents` 对照计划核验业务投影，以原序号 CAS 转录，再显式写档案/witness，由宿主提交。序号或业务条件变化时整根中止，不能更换消息 ID、重新签名或自动执行业务。没有新的 messaging_outbox/Relay 发布责任。新增9项实际MySQL用例覆盖两类提交、源/序号竞争、服务端拒绝、客户端丢弃提交回复、组织容量竞争与 dispatch 取消 drain；仅收集待CI，不称为网络故障或Broker验收。


### 独立事务 command receiver（生产装配尚未启用）

`TransactionCommandReceiver` 复用原 `CommandReceiver.resolve_command` 的 JOSE 认证、受限载荷读取及拒绝隔离；先检查实际 provider、逻辑 Topic 和 `qs.mysql/m3-1` TRANSACTION 物理 Topic，再查 SDK 请求。同请求已提交时逐项核验权威 witness、首次档案与 Inbox 决策；未决或中止状态不自动创建新执行、重建计划或调用业务。首次请求先只读兼容旧 Inbox，旧决策不成为新事务 witness。

没有已有计划时才构建完整候选并 reserve，执行器只从持久化赢家组装。仅本地已提交且精确证据齐全才允许入站确认，`LocalCommitted + FinalUnknown` 保留 Checker 的 Broker 确认责任。取消不制造回滚证明。新组件不修改普通运行入口、连接池或全局事件循环，尚未启用生产装配。

原本地技术失败预算仍单独持久化。达到8次且没有已有事务计划时，只冻结一条原技术HELD receipt；原根以独立空状态转录替换 commit hook，不执行业务。物理失败包装只归档，不使用其 attempts 授予业务执行、重试或HELD。新 v2 失败读取由宿主显式绑定物理 Topic，旧 NSQ/普通消息默认读取不变。

评测候选fbb1bbd的两档原生CI各3485通过/2失败/0跳过；失败为新故障测试使用应用账号安装触发器而触发MySQL1419。98f4bf9改用现有显式隔离管理员连接，并验证相同回环host/port/database；应用事务仍用原账号/池，不新增权限或修改服务器参数。失败报告保留，等待修复候选复验，不将其他通过用例或M7成功视为完整门禁通过。

Receiver候选f7fbc89本地完整2624通过，381源文件类型检查通过；61c1cda的HELD转录补充后完整2624通过（895项integration/interop未选择），mypy381通过。新增4项数据库接收器用例显式使用database-only runtime，只验证原接收/SQL语义，不能冒充SDK半消息或真实Broker验收。


30b3d32的两档评测原生9项全部通过，故障管理员修复有效；整轮CI仍失败。新增receiver在读取技术失败预算之前遗漏原事务，现6597f45补齐短技术读取事务并保留故意red回归；这不是MQ准备前的业务执行。接收器夹具的自动清理还提前创建participant，导致publication早于执行配置清理，现改为显式receiver_host依赖并保留原publication→participant的建立/逆序释放，未关闭外键或放宽业务校验。f528e63本地完整2625通过、381源文件类型检查通过，新原生复验待完成。

真实SDK/Broker使用显式文件 `tests/integration/rocketmq_admission_host.py`，不进入普通依赖作业的自动收集。infra以 `make rocketmq-ai-admission-test` 新建隔离Docker项目、回环端口、账号和临时schema，显式安装本候选迁移，再运行8项具名宿主用例/15项检查。Source与固定0.4.0a1 wheel分开验证，Python client固定5.1.2；缺依赖失败、不得跳过。该文件目前只完成收集与静态检查，尚未验证通过。

该隔离profile通过实际Broker投递控制QS输入，调用真实AI接收器和发布SDK，以已完成的空poll响应验证半消息不可见，并核对原事务、首次wire、旧重复输入与技术HELD。QS输入witness及接收observer属于测试夹具，不表示成对QS业务、外部模型或system验收；最终回复丢失为客户端丢弃真实完成回复，不冒充网络故障。SDK客户端日志、JUnit、正文与凭据均留在本次私有目录，只导出摘要/具名结果并清理本次资源。


首轮infra Broker六档39535745152均失败并完整清理临时资源：控制QS输入在freeze_branch之前缺少original_occurred_at，SDK的不可变Message时间校验正确拒绝；未准备这些输入半消息。本地复现同一错误后，cde36ea仅为测试构造器增加显式时间参数，由Broker profile在首次签名前传入，未改应用/SDK或重新加密已冻结输入。五种控制命令的完整SDK分支冻结及历史默认行为6项回归、相关合计36项通过，9项interop按标记未选。此前2bfbb65源码CI14成功、两档各3520零失败/跳过的证据保持原SHA；新候选与Broker重跑仍待验证，T16未完成。


Broker第二轮39539009218暴露最后确认的适配缺口：固定SDK在最终RPC确认未知时抛出TransactionSendError，异常的result仍可能是LocalCommitted。3e6f178在receiver单独读取该结构化结果；仅本地已提交时继续逐项权威核验，完整witness/档案/Inbox缺一仍拒绝确认入站。未提交、回滚、Unknown结果继续抛出原异常，不重执行业务，也不把最终状态改成CommitConfirmed。该条件不再消耗新的业务技术失败预算。

故障profile的观察器也记录异常携带的真实SDK result后原样抛出，由实际receiver决定是否确认；不在测试观察器吞错或确认消息。故意red回归和5个SDK异常状态案例保留；3e6f178完整纯单元2636通过、895项真实依赖按标记未选，mypy381通过。真实Broker与新候选CI仍待重跑，不能把前两轮失败计通过。

[`ReviewStateInput/ReviewPlanFacts`](../../../src/qs_ai/infrastructure/workflow_transport/review_state_plan.py) 将完整的已授权评审输入绑定为稳定摘要，持久化源快照摘要及最多35项首次评审时间，不把完整正文塞入协议元数据，也不新增总计8KiB的RPC限制。原宿主持有不可变输入，原事务前恢复持久化赢家的首次时间和偏移；原事务重验输入摘要、源状态及全部原评审/审核规则，再共同提交业务、sequence、首次档案和witness。Checker仅读证据，不重建或补执行业务。

评审沿用同一显式管理适配器及发送编排：半消息未确认准备成功不执行原事务；只有精确提交证明与首次档案都存在才返回原事务内取得的响应。提交未知保留未决责任，不返回版本冲突、不自动重做评审。原RPC无跨请求幂等键，每次调用保留原版本CAS语义。新增12项原生用例覆盖单项/35项、档案拒绝、客户端丢弃已完成提交回复、并发版本变化及实际RPC处理函数；DB-only runtime与客户端故障注入不冒充真实Broker、网络或进程故障，最终验收以infra总表为准。

评审更正的原 [`correct_review/plan_review_correction`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_review_corrections.py) 共享只读业务规划。精确 command ID 和输入的历史重放在版本/状态CAS之前判断，覆盖当前和重开前归档的更正记录；重放不写新状态、不新增事件、不生成或收养witness。原写入口仍配置RR并按checkpoint/Run顺序加锁，仅新更正保存原审核链，根提交归宿主。此抽取不是更正入口已迁移事务SDK，相关真实依赖验证与后续接入仍以infra总表为准。

[`CorrectionStateOperation`](../../../src/qs_ai/infrastructure/workflow_transport/correction_state_plan.py) 冻结原command ID、版本、两项指纹、完整更正输入及首次审核时间；稳定输入摘要不包含后续RPC重新取得的服务器时间。最坏合法单项理由仍可放入有界事实，不收窄原公开输入。原 [`CorrectionStatePlanner/CorrectionStateRoot`](../../../src/qs_ai/infrastructure/workflow_transport/correction_state.py) 先查精确历史重放，再只读规划；全部半消息准备后，在原RR根先取SDK guard、重验源状态及全部原更正规则，共同提交审核链、sequence、首次档案和witness。原提交回复未知时只读新边界恢复，不重做业务。

更正沿用原公开command ID幂等：旧业务回执精确匹配且没有SDK执行记录时，只读返回当前业务视图，不发送半消息或收养witness；已有SDK记录时，只有精确Committed witness、首次档案和同一原业务更正都验证通过，才允许返回原重放结果，包括后续已定稿的当前视图。Prepared/Aborted记录不自动新建执行。只读预查与reservation之间的竞态只做有界证据重读，不补执行业务。新执行成功仍返回其原根内取得的响应。

[`input_conflicts`](../../../src/qs_ai/infrastructure/persistence/mysql/transaction_input.py) 只在SDK拒绝键复用时，以固定宿主主库中的绑定、schema、原字节身份及输入摘要证明冲突，保留原更正CAS错误语义；不依赖异常文本，不把解析或绑定失败冒充业务冲突，也不据此确认Commit/Rollback。协议读预算沿用显式2秒。新增14项真实MySQL用例覆盖原子提交、审核时间、档案拒绝、客户端丢弃提交回复、版本竞争、历史重放、实际RPC准备门禁及精确输入冲突；这些不是Broker/网络或生产证明，执行状态以infra总表为准。默认生产装配仍关闭。

隔离的 [`rocketmq_state_host.py`](../../../tests/integration/rocketmq_state_host.py) 扩展为6项实际SDK/Broker验收：原worker预检3项继续保留，新增定稿后重开、35项评审RPC处理函数和评审源冲突回滚。新增路径使用固定发布wheel及真实半消息，暂停在本地根前观察不可消费，提交后核对原审核事实、首次时间/wire、精确witness与实际消费；源冲突必须回滚且不可消费。二次确认回复丢失仍是客户端丢弃真实RPC回复，不冒充网络级提交故障或进程退出。该文件按infra显式profile执行，普通pytest收集不会替代真实Broker验收；执行结果与20项必需证据以infra总表为准。

显式真实Broker profile继续增加更正两项：准备半消息后验证不可见；首次审核时间/wire/witness、客户端丢弃真实Commit回复仍保留LocalCommitted；通过真实事务消息定稿后，重复原command只读返回当前结果且不再发送；并发源变化必须回滚且不可消费。该版共8宿主用例/25必需证据，保持原资源、固定wheel/client和两种Python/三档MySQL矩阵。新版本验收必须绑定本版源码和工具，旧6/20报告仍独立保留，不冒充新更正验证。

创建入口的原 [`create_run/plan_run_creation/apply_run_creation`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_runs.py) 分离只读计划与原事务写入。不可变计划持有原始definition JSON、原执行模式和冻结策略，隐藏大正文repr；原创建包装保持相同参数、SQL顺序和调用者提交归属。独立RPC与Solution Prepare均继续借用原会话，后者的资产注册和Run创建不拆成多个提交。原定义字节、时区时间格式、两种执行模式和重复创建行为由基线与原生测试保护；这一步尚未接通创建事务Sender，也不改变默认装配。


[`CreationStateInput/CreationPlanFacts`](../../../src/qs_ai/infrastructure/workflow_transport/creation_state_plan.py) 将原Run UUID、组织、操作者、完整release和规范化原因绑定为稳定创建请求；当前宿主模式与新采样时间不属于请求输入。持久化事实保留首次原始definition摘要、执行模式及带时区创建时间，完整首次wire仍归SDK计划。读取只接受初始version/sequence为1、requested状态及精确release；未知字段、重复字段、非规范编码或替换定义/策略被拒绝。

[`CreationStatePlanner/CreationStateRoot`](../../../src/qs_ai/infrastructure/workflow_transport/creation_state.py) 复用原MySQLRunCreator只读资产解析，会话关闭后才Seal；半消息之后的单次原RR根先取SDK guard，重读并核验原不可变合约，再共同写Run、冻结policy、checkpoint、sequence、档案和witness。原定义和首次模式、时区格式不重新生成，提交回复丢失只用精确持久证据判定。新增10项原生测试分别覆盖两种模式、档案/witness拒绝、客户端丢弃已完成提交回复及不能收养已存在的legacy Run；当前执行结果以infra总表为准，不能替代Broker、网络或生产证明。此批尚未接通公开创建Sender及Solution Prepare事务入口，默认装配继续关闭。


[`TransactionalEvaluationRequests`](../../../src/qs_ai/infrastructure/workflow_transport/transactional_evaluation_requests.py) 提供独立Create-once入口：先查原稳定请求和精确业务回执，再冻结候选；全部半消息成功后才执行单次原创建根。原RPC仍用调用方Run UUID幂等，同键不同输入仅在精确身份/输入证据成立时返回冲突；Prepared/Aborted责任不自动重建。新执行返回原根内捕获的响应；本地Committed而Broker最终确认Unknown时，逐项验证原witness和首次档案后保留业务成功，不重新创建。

重复创建继续返回当前业务视图和首次creation receipt，即使Run已启动或宿主模式配置已改变。legacy回执没有SDK记录时只读返回，不能被收养为新witness；已有SDK记录必须同时核对精确Committed证据、首次档案和原definition/mode，缺一保持未决。预查/回执读取竞态只有一次有界权威重查，不自动重执行业务。新增16项真实MySQL用例覆盖实际Create RPC、本地执行前准备门禁、两种首次模式、原回执、新旧输入冲突和并发赢家，以及业务已构造响应后档案/witness失败导致原根回滚时不返回成功；DB-only运行器不证明真实Broker，验收结果与总进度以infra记录为准。统一生产装配及Solution Prepare事务入口仍未启用。


Solution Prepare同时写资产和Run，仍须在同一原根接入事务消息，不能把资产提前提交。其前置只读规划现已提供 [`PromptFreezePlan/plan_freeze`](../../../src/qs_ai/infrastructure/persistence/mysql/prompt_freezes.py) 和 [`ProfileRegistrationPlan/plan_registration`](../../../src/qs_ai/infrastructure/persistence/mysql/profile_registrations.py)：复用原草稿、来源、不可变身份、内容、渲染和回执规则，规划不加写锁、不执行DML；原写路径及根提交归属保持不变。原路径仍读取数据库中的真实资产验证，不能用预览覆盖持久化核验。

[`PlannedAssetReader`](../../../src/qs_ai/infrastructure/persistence/mysql/asset_snapshot.py) 仅供宿主规划器显式组合未来的不可变资产，精确匹配封闭类型与身份，其他读取委托同一原快照。内存计划不是提交证明，不授权发布，不用于Checker、正文服务或历史回执权威读取。新增4项真实MySQL用例分别验证Prompt和Profile规划只有SELECT、原事务共同提交/回滚；测试结果以infra记录为准。Suite、完整Run计划和Solution原根发送尚待完成，默认装配仍关闭。


[`SuiteRegistrationPlan/plan_registration`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_suites.py) 继续把原Suite注册的所有只读校验放在DML之前，保存原案例、schema、manifest和不可变SuiteContracts；原apply仍只在同一根插入一份资产/回执，重放读取首次回执。显式未来资产只供预览，默认注册仍验证实际持久化资产。原 [`freeze_run_creation`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_runs.py) 从已核验suite/contracts冻结创建定义，不发起数据库、模型或网络调用；注册表规划仍先验证请求，再读取原库并调用同一冻结函数，原定义字节和SQL提交归属不变。

Prompt/Profile/Suite只读规划共有6项新真实MySQL用例，分别证明仅SELECT以及原根共同提交/回滚；本地仅收集，required CI结果另行记录。完整Solution Prepare的身份冻结、整批计划、发送器及原根仍待完成，不能把这些基础函数计作业务已迁移。


[`plan_prepare_assets/PreparedAssetPlan`](../../../src/qs_ai/infrastructure/persistence/mysql/solution_preparation.py) 把原Solution Prepare的整个资产图只读规划为不可变值：首次Prompt、两条Route、Profile、Suite、原Run创建定义及准备结果。各阶段复用原来源、模型选择、渲染、案例/schema和回执校验；既有冻结/注册不能被收养为新执行。资产正文和首次审核/配置事实纳入完整摘要，未写入也不构成提交证明。准备结果读取返回独立副本，原结果结构和创建定义字节保持不变。

原 [`PreparationIDs/prepare_assets`](../../../src/qs_ai/infrastructure/persistence/mysql/solution_assets.py) 可显式接收在事务前冻结的确定性身份，默认旧路径保持原派生规则；新执行传入完整身份后不在业务阶段生成UUID。原 [`contracts_from_binding`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_contracts.py) 允许规划器验证未来Suite的精确绑定，保留组织所有权与四项原ref校验，仍从同一原库读取固定policy/semantic字节，不能选择其他数据库或绕过源Suite规则。

新增4项真实MySQL用例覆盖两种首次模式、规划阶段仅SELECT、原资产与Run共同提交/回滚、定义/回执一致和执行阶段禁止新UUID；本地仅收集，实际结果以infra总表为准。完整Solution命令的SDK发送器、冻结源状态、原revision/命令回执与witness共同提交及真实Broker仍待接入，此批不改变默认DI。


[`MySQLSolutions.apply_in/SolutionApplyResult`](../../../src/qs_ai/infrastructure/persistence/mysql/solutions.py) 借用调用方已经存在的根事务，不配置连接、不提交/回滚/关闭，拒绝缺失、失效根及savepoint。原公开apply仍自行建立RC边界：新写入在响应校验后只提交一次，首次或加锁后命中的历史回执不写入也不提交。原稳定请求编码抽成solution_request，旧命令/回执字节及唯一冲突恢复不变。

wrote只表示本次在原根中暂存了业务写入，不是提交证明；SDK原根仍须统一写全部witness并提交，提交结果必须另行验证。Prepare可显式借用首次身份和执行模式，原其余写入口保持原语义。新增4项真实MySQL用例覆盖两种模式、原Solution revision/命令回执/全部资产/Run共同提交或回滚；本地仅收集，SDK原根和真实Broker仍待接通。


Solution Prepare 的显式事务适配现由 [`SolutionPreparationInput/Facts`](../../../src/qs_ai/infrastructure/workflow_transport/solution_preparation_plan.py)、[`Planner/Root`](../../../src/qs_ai/infrastructure/workflow_transport/solution_preparation_state.py) 和 [`Dispatcher`](../../../src/qs_ai/infrastructure/workflow_transport/solution_preparation_dispatch.py) 组成。稳定输入沿用原组织、操作者、命令及请求编码，首次源状态、草稿、完整资产图、业务回执和执行模式分别冻结摘要。规划仅在原主库短快照读取，关闭会话后才 Seal；取得完整半消息后，单次原 RC 根先取得 SDK guard，再锁原 Solution/草稿并核对冻结来源，借用原 apply_in 暂存全部资产、Run 和首次命令回执。原根核对实际资产、原 Run 定义、三份审核回执及目标状态后，共同提交首次档案、sequence 和 witness，不提前提交资产。

[`TransactionalSolutions`](../../../src/qs_ai/infrastructure/workflow_transport/transactional_solutions.py) 只显式替换 Prepare；Create/Save 继续委托原写入口，连接池和运行生命周期仍由宿主持有。重复 Prepare 先验证原回执的组织、操作者和输入，再读取精确 SDK 记录：没有协议记录的历史回执只读返回，已有 Prepared/Aborted 不自动重建或收养。Committed 必须重新核对精确 witness、首次 wire 档案和原回执摘要；不能用当前视图或内存中的结果证明提交。LocalCommitted + FinalUnknown 保留已证实的业务成功；模型调用、业务重试和自动回退均不在此适配中执行。

本批本地56项目标测试及3061项非集成、非interop回归通过。新增27项真实 MySQL 用例仅在本地收集，覆盖两种首次模式、档案/witness拒绝、客户端丢弃已完成提交回复、源冲突、实际 Prepare 处理器、并发、首次回执与越权保护；DB-only transport 不作为 Broker、mTLS、QS成对业务或网络级故障证明。执行结果及源 SHA 以 infra 总表为准，统一默认装配继续关闭。


显式真实 SDK/Broker 验收档新增 Solution Prepare 实际处理器：全部半消息准备后暂停原根，验证不可消费且资产/Run尚未提交；放行原根并在客户端丢弃已完成最终确认回复后，核对首次资产、业务回执、wire 与精确 witness，再用原命令重放证明没有新发送。另一用例在半消息后由原旧写者改变源状态，验证新执行回滚、无新档案/可消费消息且竞争事实保留。扩展后总计10个宿主用例、30项必需证据，本地仅收集；客户端丢弃回复不等同网络级提交故障，事件观察者也不等同 QS 成对业务或 system 验收。


剩余 ResolveUnknown 写入口先提供 [`ResolutionPlan/plan_resolution`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_resolution.py) 只读规划，复用原 checkpoint/Run 版本、组织、candidate drain、不可变未知执行证据、恢复预算和首次人工裁决时间规则。规划不加写锁，不写业务或消息，不调用模型；结果正文冻结为不可变 JSON，读取返回独立副本。原 accept_resolution 仍在同一个调用者根内按 checkpoint、Run 的原顺序取锁，重新规划后先执行原 checkpoint CAS 再更新进度；原容量准入及根提交仍归 management 所有。规划本身不表示新的执行或模型重试已获准。

7项原实现表征在抽取前通过，抽取后36项目标及3071项非集成/非interop回归通过。新增4项真实 MySQL 用例仅收集，覆盖 generation/semantic 未知证据的只读 SELECT、原裁决共同提交/回滚和不可变执行记录；本批尚未接通 ResolveUnknown 的半消息、SDK原根或默认DI，实际门禁与进度继续回链 infra。


ResolveUnknown 的显式事务入口由 [`ResolutionStateOperation`](../../../src/qs_ai/infrastructure/workflow_transport/resolution_state_plan.py) 与 [`ResolutionStatePlanner/Root`](../../../src/qs_ai/infrastructure/workflow_transport/resolution_state.py) 提供。首次人工裁决的操作者、目标执行、决策、风险确认、理由和时间连同源快照摘要冻结；同一调用身份的后续时钟不能改写首份审核事实。只读阶段复用原未知证据和恢复预算，并预检原宿主容量，关闭会话后才 Seal。完整半消息准备成功后，原根先取 SDK guard、再取原组织容量锁，核验版本及冻结源快照，借用同一事务调用原 resolver；原 CP/Run 锁、人工裁决、容量重验和响应规则均保留，借用 commit 不提交或关闭外层根。

[`GovernanceStateDispatcher`](../../../src/qs_ai/infrastructure/workflow_transport/evaluation_governance_dispatch.py) 必须显式绑定同一 Transactions 所有者的原 management，才能处理该操作；缺绑定在读取协议或准备消息前拒绝，不复制默认容量来替代宿主配置。[`TransactionalEvaluationManagement.resolve`](../../../src/qs_ai/infrastructure/workflow_transport/transactional_evaluation_management.py) 在事务前生成本次调用身份，保留原权限 scope 与风险确认。此接口不新增跨 RPC 重试幂等约定；已有调用保持未决，不重跑原裁决或用当前视图冒充首次响应。本地提交和首次档案/witness证实后，Broker最终确认Unknown仍返回原业务成功。人工 authorize_replacement 继续只授权原预算内的未来业务，Checker和协议维护不代替人工授权或调用模型。

本批27项协议/原根目标测试、3098项非集成/非interop回归、405源文件类型及lint/format通过。新增12项真实MySQL用例仅收集，包括原人工裁决与witness共同提交/回滚、档案/witness拒绝、客户端丢弃已完成提交回复、实际ResolveUnknown处理入口的准备门禁、并发裁决和真实第二个Run竞争原宿主容量；保留原未知执行账本并检查无新增模型调用。完整CI、真实Broker、成对QS及生产验收分别记入infra；默认DI继续关闭。


显式真实Broker验收档扩展原ResolveUnknown处理器：半消息后暂停原根，证明旧业务仍为blocked且事件不可消费；按首份人工审核时间放行原resolver并在客户端丢弃已完成最终确认回复后，核对原审核、首次wire、witness和未改动的未知执行账本。相同内部调用再次出现保持未决，不重发、不重做裁决或模型；另一场景由原写者先提交取消裁决，新执行须回滚半消息并保留竞争事实。额外fixture使用独立名字和显式关闭，保留旧worker/治理/Solution的原fixture形态。

扩展档总计12个宿主用例、35项必需证据，本地仅收集；实际运行由固定源码的infra矩阵验收。客户端丢弃已完成回复、直接处理器与事件观察者分别界定，不能代替网络故障、真实QS业务、system登录或真实模型验收；默认DI保持关闭。


可信主机上的语义契约恢复仍是活动维护写入口。其 [`ContractRecoveryPlan/plan_contract_recovery`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_contract_recovery.py) 现可只读冻结原恢复结果：精确 release、当前 block、失败执行及输出摘要、策略例外确认、首次审核时间、原恢复次数/预算和下一步精确候选均沿用原规则。结果正文为不可变 JSON，读取返回独立副本，不写业务或消息，不授予新的模型调用。原 authorize_contract_recovery 显式请求原 checkpoint→Run 锁，再执行原 CAS 和进度更新，可信调用者仍拥有根提交；维护CLI的旧预览回滚及 apply 调用尚未改接 SDK。

原实现7项表征先通过，抽取后25项目标、3106项非集成/非interop回归、405源类型及lint/format通过；新增2项真实MySQL用例仅收集，验证原失败账本的只读SELECT与原根共同提交/回滚。维护命令的事务发送、原witness、默认装配及生产操作仍须分别验收，不以提供计划函数计作迁移完成。


原串行worker的 [`plan_prepare_execution`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_preparation.py) 可只读计算下一份 prepared checkpoint。原版本、执行模式、取消标记、冻结策略、两种终态证据的kind作用域、原恢复授权和首次时间校验均保留；execution/invocation ID及lease由宿主事先提供。返回值本身是不可变CheckpointState，不写CP、不取写锁、不调用模型，也不表示已取得dispatch或模型执行资格。

原 prepare_execution 仍显式按checkpoint、Run、dispatch、generation、semantic的原顺序取锁，重新计算后执行一次原CP CAS；同一原 prepare_step 后续dispatch与第二次CAS的根提交边界未拆开，PreparedStep.cp的原prepared快照语义不变。生成与语义证据共用统一物理表时仍分别保留kind条件，不能用表名替代证据类型作用域。此基础函数尚未接通worker事务发送器、ModelCapacity转移、candidate并发或恢复路径。

抽取前10项原行为表征、抽取后28项目标与3117项非集成/非interop回归通过，405源文件类型、lint/format通过。新增4项真实MySQL用例仅收集，覆盖generation/semantic原准备的只读SELECT、冻结身份/时间与原CP共同提交/回滚；不是完整prepare+dispatch的SDK/Broker验收。执行结果仍以infra总表为准，原worker配置、默认DI和生产链路保持原状。


原串行及candidate共用的 [`DispatchRecordPlan/plan_dispatch_record`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_dispatches.py) 只读冻结原dispatch记录：沿用冻结策略、generation/semantic阶段预算、精确slot/candidate目标预算、下一execution ordinal及原checkpoint JSON。预览不加写锁或插入，不表示已取得dispatch或模型执行资格；返回记录为不可变JSON，每次读取均为独立副本。原record_dispatch仍在调用者原根内按原顺序读取冻结策略、锁定最新dispatch账本，再执行一次原不可变插入；原prepare+dispatch共同提交、两次checkpoint CAS及PreparedStep.cp快照语义不变。

抽取前6项原实现表征、抽取后19项目标和3125项非集成/非interop回归通过，405源文件类型与lint/format通过。新增4项真实MySQL用例仅收集，覆盖generation/semantic只读预览与原reserve_dispatch共同提交/回滚、不可变账本及原模型调用计数；不是worker SDK/Broker验收。ModelCapacity、外部模型调用、candidate claim及默认DI仍按原路径运行，真实CI与进度回链infra。


串行worker提供显式 [`WorkerDispatchPlanner/Write`](../../../src/qs_ai/infrastructure/workflow_transport/worker_dispatch_state.py) 和 [`WorkerDispatchSender`](../../../src/qs_ai/infrastructure/workflow_transport/worker_dispatch_sender.py)。原模型输入读取/渲染抽为load_step_inputs，旧路径保留原容量准入位置和原提交；新路径只在原主库快照读取资产（包括schema），关闭源会话后才生成首wire。首次prepared CP的ID、时间、lease、原模式/策略/源摘要、完整模型输入和不可变dispatch记录摘要持久化；过程lease owner是赢家事实，不改变同Run/version请求的稳定输入。事件仍沿用原最后业务transition时间，不用准备时间伪造新的业务transition。

全部半消息准备成功后，SDK guard先进入同一个原RR根；原prepare_execution和reserve_dispatch依次执行两次CP CAS，原策略/账本预算、模型输入、源摘要和完整最终投影重新核验，首次档案、sequence和witness与业务共同提交。PreparedStep.cp仍是原prepared快照，state.checkpoint是dispatching，version一次可见地增加2。根内不生成ID、Seal、取得模型permit或调用provider；schema同原session读取，不另开宿主连接。

发送器在新执行前申请原ModelCapacity，未决、拒绝、取消、并发败者或证明不足释放本次permit。只有明确LocalCommitted且精确新读取witness/首次档案吻合时，才向原graph移交一次permit；FinalUnknown不误报回滚。已有调用即使已提交也不返回新的模型执行资格；原恢复治理承担后续责任。实际graph在调用模型前验证Run、owner、两次CAS版本、checkpoint及invocation，finally释放permit；不将串行入口用于candidate claim。

本批29项原行为门禁、65项目标与3157项非集成/非interop回归零失败跳过，407源类型及lint/format通过。新增15项真实MySQL用例仅收集：generation/semantic原双CAS共同提交/回滚、archive/witness拒绝、客户端丢弃已完成提交回复、竞争取消、半消息门禁模拟、FinalUnknown及容量交接、只读单快照和源关闭后Seal。模拟sender不是实际Broker证明；真实Broker、进程和网络故障另验。本入口尚未接通默认装配；终态/响应持久化、candidate及恢复入口仍待完成，原finish路径不因此宣称已退出commit hook。


真实Broker隔离profile新增串行generation/semantic发送器与并发原取消三项用例，总计15项宿主/42项精确证据。直接调用显式WorkerDispatchSender，真实SDK先准备半消息；observer确认不可见后才放行原根，客户端丢弃已完成二次确认回复，核对首次模型输入、两次CAS、原不可变dispatch账本、witness及首次wire，并验证permit只移交一次、重复调用不再发送或调用模型。并发原取消获胜时只能回滚已准备半消息。本批仅完成用例收集，真实六档执行仍由infra报告给出；profile不调用真实provider，不代表完整graph终态、成对QS、进程退出或数据库断网验收。


可信主机的语义契约例外现有显式 [`ContractRecoveryInput/Planner`](../../../src/qs_ai/infrastructure/workflow_transport/contract_recovery_state.py) 与 [`Sender`](../../../src/qs_ai/infrastructure/workflow_transport/contract_recovery_sender.py)。输入绑定原Run/version、组织、release、失败执行/输出、成本例外确认和operator审核；首次时间不参与稳定业务hash，首份审核与源/规划结果摘要写入冻结context。只读规划复用原失败证据、未知排除、预算和精确下一动作，关闭来源后才Seal；原根guard后重新核验，再调用原authorize_contract_recovery的锁/CAS，将原业务、审核、档案、sequence和witness共同提交。Checker和协议维护不构造此授权或调用模型。

[`recover_semantic_contract`](../../../src/qs_ai/bootstrap/recover_semantic_contract.py) 保留原preview回滚与旧配置路径。显式配置事务owner时，apply仍校验当前Unix操作者、原preview输出指纹和成本确认，先关闭来源读取，再启动所属SDK；发布准入关闭直接拒绝，不降级旧提交路径。Start不安装schema，完整本owner Topic仍由固定工厂登记。关闭先排SDK，ShutdownIncomplete继续Wait之后才关闭命令自己的数据库；不关闭其他宿主资源。此源码未执行任何生产命令。

同操作只有精确首次witness/档案证明Committed后才返回首份操作元数据；FinalUnknown保持本地成功，后续Run变化不替换原响应或首审核。Prepared/Aborted/未知只返回未决，不自动重新授权；改变理由、操作者、输出或release不能复用原请求。此操作明确授权原预算内的一次未来业务，发送器本身不调用模型。

改动前25项既有规则与4项原CLI表征通过；本批57项目标、3189项非集成/非interop回归零失败跳过，409源类型与lint/format通过。9项真实MySQL用例仅收集，覆盖原失败证据/审核/witness共同提交回滚、archive/witness拒绝、客户端丢弃完成提交回复、竞争取消、FinalUnknown模拟、后续状态后的首次响应重读及实际CLI的预览/原库根；模拟transport不算真实Broker。默认应用装配、完整故障、成对QS和生产验收仍待完成，以infra总表为准。


interpretation原 [`MySQLExecutionStore.finish_in`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py) 提供借用根的暂存边界。原finish仍拥有同一个RR根和一次提交；finish_in拒绝普通handle、失效根与savepoint，验证根未替换，不提交、回滚或关闭调用者资源。原Artifact来源/持久response/冻结配置校验、Question顺序、失败规则、Run/job更新、最后活跃租约校验及容量释放次序保留。可显式传入首Question/Event ID及容量释放时间，旧入口继续使用原默认行为。

这是原业务事务基础，bind仅验证原SQLAlchemy边界，不是事务witness；返回None不证明业务提交。原Artifact校验/计算仍保留，完整只读计划、首次模型结果/ID冻结、keyless消息转录、SDK Sender以及claim/recovery入口尚未完成，不以本次抽取宣称完成interpretation迁移或模型恢复。

抽取前4项原行为表征、抽取后11目标与3200项非集成/非interop回归零失败跳过，409源类型、lint/format通过。6项真实MySQL用例仅收集，覆盖原Session/Question/Run/job/租约/首业务历史共同提交/回滚和真实失效/嵌套根拒绝；历史行本身不是MQ发布或witness证明。完整CI与进度仍由infra总表记录，未修改默认装配或生产。


interpretation首业务历史的keyless校验现由 [`InterpretationStateTranscription`](../../../src/qs_ai/infrastructure/workflow_transport/frozen_start_events.py) 复用；原FrozenInterpretationEvents/FrozenStartEvents仍负责Start/Change/Retry的完整command计划校验，实例类型和首次状态规则保留。独立原根适配器以后可在验证完整冻结计划后传入精确digest/状态，不伪造command或Inbox；primitive复制外部protobuf，调用方后续变更不影响首投影。

原history精确身份/正文/版本、零旧投递责任、同根检查和mq_owned CAS保持不变；primitive只暂存原业务历史归属，不生成首wire、发布消息、写witness、授权模型或确认消费者完成。独立state计划/Sender尚未接通。抽取前30项旧Start/Change/Retry门禁、抽取后35目标与3205项非集成/非interop回归零失败跳过、409源类型及lint/format通过。4项真实MySQL用例仅收集，验证同根归属共同提交/回滚和拒绝旧投递责任；不是SDK/Broker验收，默认装配与生产仍未变更。


[`ArtifactIdentity`](../../../src/qs_ai/application/execution/artifact.py) 在进入执行器前验证原Artifact ID与Run/invocation的原UUID5关系。原build_artifact默认行为和v1/v2结果不变；显式适配器可传入已验证身份，原内容、来源、配置及安全校验仍完整执行，校验期间不重新调用UUID生成。finish_in同时核对该身份与原claim/candidate，不能借用另一执行的身份。它只为完整冻结计划提供已有身份基础，不新增模型重试授权或替代witness；当前验收和完整Sender接入状态回链infra总表。


非command的interpretation状态由 [`InterpretationStateIntent/Facts`](../../../src/qs_ai/infrastructure/workflow_transport/interpretation_state_plan.py) 独立冻结：原组织、Session、request、业务输入/操作、预期version、首次时间及受限业务context绑定TRANSACTION/ai.mysql/m3-1路由；branch保留首次Event ID、原body/wire及version序号。不同候选的消息ID、时间或密文不改变稳定业务请求，读取赢家后才使用首事实。解码拒绝错scope/owner/type/route/正文、歧义字段和非规范context；不制造command或Inbox。

[`InterpretationStateRoot`](../../../src/qs_ai/infrastructure/workflow_transport/interpretation_state_root.py) 只接受一次owned执行，在原RR根先绑定SDK guard，再交由宿主业务暂存；同根检查后转录精确首历史，将档案和全部witness与业务共同提交。确认丢失通过新权威读取收敛，不以业务返回值、取消或lease到期猜测回滚。构造不打开连接或启动客户端，回查和关闭仍归宿主SDK生命周期。

这些通用基础不授权业务或模型；具体业务入口的首源读取、执行资格及Sender须分别接通并独立验证。原根故障用例直接调用存储/执行器，不能当作半消息、真实Broker、成对QS或system验收；当前源码/CI/合并/运行状态仅维护在infra总表。


原 [`MySQLExecutionStore.read_claim`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py) 提供只读来源视图，沿用原Session状态/version/active Run、job租用/fence、活跃lease/fence及session→job→lease的读取规则。原_guard显式lock=True，原锁顺序和业务条件保留；只读来源不取写锁、不提交，也不延长lease或赋予执行资格。规划后的真正根仍须重新竞争并验证原活跃所有权，不能把较早快照当作未来执行授权。完整接入、故障和部署状态回链infra总表。


[`InterpretationFinishSender`](../../../src/qs_ai/infrastructure/workflow_transport/interpretation_finish_sender.py) 提供显式的原解释任务完成入口：先读取原组织/request绑定并查询请求赢家，缺记录时才由 [`InterpretationFinishPlanner`](../../../src/qs_ai/infrastructure/workflow_transport/interpretation_finish_state.py) 在只读RR快照核对原Session/job/活跃lease，关闭来源后冻结首次Event/Question ID、Question序号、时间、完整结果投影与首wire。稳定输入包含原server Claim和WorkflowResult；首次ID、时钟与密文不参与业务输入冲突判断。已存在的未决或中止执行不自动授权新模型调用、重封消息或业务重做。

全部半消息成功后，原根先竞争SDK guard，再精确复核原Session/job/run/fence、答案/问题身份、请求绑定及源摘要，调用原finish_in暂存业务、首历史、档案和witness并共同提交。心跳延长租约不改变业务摘要；真正提交仍以原guard和末尾数据库活跃租约校验为准。Question序号须等于冻结快照和原根实际count+1，原调用未提供序号时保持旧语义。Artifact沿用原持久化response、来源、配置、内容与安全检查，执行器前验证原UUID5身份，原根不重新生成ID。

Sender只在LocalCommitted且新权威读取精确witness与首档案均匹配后完成；FinalUnknown仍可保留已证实的本地提交。重复完成读取首次结果，即使业务后来继续推进，也不重新规划、发送或执行模型。当前为显式接入组件，默认应用装配、claim/refusal、其余evaluation终态与恢复责任仍分别受迁移门禁保护。真实依赖报告与合并/部署进度统一回链infra总表；模拟发送器和客户端主动丢弃提交回复不算实际Broker或网络级故障证明。


Artifact的JSON对象在MySQL中不保留键顺序。完成状态转录仅对artifact_json按无重复键、全字符串字段对象核对原业务值，拒绝字段/类型/内容变化；其余事件身份、版本、投递责任和原根保护仍逐项精确校验。此比较不重建或替换首次消息正文、wire或档案哈希，历史读取继续保留原数据库投影。


隔离真实Broker档通过原InterpretationFinishSender检查问题、失败和Artifact完成：暂停原本地执行器时半消息不可见，根提交后精确首wire/witness可读取，最终确认客户端丢回复仍保留本地提交，重复完成不重规划或发送。源变化必须回滚准备分支并保留竞争事实。档案只使用事件观察者，不等同QS业务、真实模型、进程退出、网络故障或system验收；该档声明19宿主用例/51必需证据，实际执行结果回链infra总表。


评测终态在 [`prepare_step_completion / finish_step_in`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_step.py) 分开准备和原根写入：准备阶段沿用原响应证据、语义校验、结束时间及状态分类，首次generation候选ID在数据库根之前生成；原根入口仅借用活跃AsyncSession，拒绝savepoint和根替换，保留原claim/恢复所有权及generation/semantic完成接受规则。返回的state和applied只描述暂存写入，不能证明根提交或消息投递。原finish_step调用仍拥有其根提交，已完成candidate重读仍不重复提交或发出成功指标。

StepCompletion是宿主内存中的准备结果，未替代持久化SDK计划、原业务授权、witness或模型响应检查点。SDK完整终态Sender、其他活动写入口和T18确定性恢复须继续独立接通与验证；默认装配及生产状态回链infra总表。


[`SerialResponseReference / save_serial_response`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_serial_response_receipts.py) 为原serial_v1 dispatch保留首次provider响应。它使用原Run/组织、聚合checkpoint version、精确dispatching checkpoint和账本，不能伪造candidate SlotClaim。借用的原根只保存现有evaluation_response_receipts中的输入证据，不推进Run/attempt、发布消息或调用模型；根提交仍由宿主负责。claim_version物理列对这种显式串行记录保存原聚合版本，独立的serial-response/v1外层codec和完整引用使其不会被旧candidate v1读取器混用，既有schema和candidate路径不变。

首次正文、结束时间和SHA不可覆盖，原串行取消drain只允许原实现中同一cancel_requested版本边界。该输入证据即使重复读取也不构成业务提交、事务witness、业务receipt或模型重试授权。编码上限为4MiB，解码拒绝摘要、scope/dispatch、版本、歧义字段和非规范字节差异；响应正文不进入日志。新适配尚未接入默认工作流，完整终态Sender和T18恢复/调用次数故障验收仍须独立完成。


InterpretationStateFacts的expected_version表示真实源版本，target_version表示原事务最终可见版本。默认version_delta=1保持旧v1全部字节及原位置参数顺序；一次原事务连续推进两次版本时，可显式选version_delta=2并使用v2 context，冻结一个最终事件和对应最终sequence。解码同时支持原v1和显式v2，拒绝歧义字段、跨版本字段拼装、超出有符号协议范围或与最终正文/序号不符的计划。单次finish仍按原业务投影校验其一次推进。

此扩展仅提供兼容冻结与同根转录基础，完整claim/refusal/attempts-exhausted执行资格、租约/配额和Sender仍须分别接通并验收；四项原根协议用例不等同半消息、实际Broker、完整claim入口或system验收。


原 [`MySQLExecutionStore.claim_in`](../../../src/qs_ai/infrastructure/persistence/mysql/execution.py) 允许显式宿主在活跃 AsyncSession 根中暂存一次原候选领取。它保留 Session→Job→Lease 锁序、配额、fence、attempt、运行诊断及耗尽后的两次版本推进，并拒绝嵌套、结束或替换的根；不自行提交、回滚、开连接或关闭资源。首次事件 ID 和容量时钟可由事务外冻结。原 claim 仍扫描最多20个候选，在同一宿主 session 内提交延后后继续下一候选，提交和执行移交仍由原入口负责。

ClaimStage 仅描述 miss/deferred/exhausted/claimed 的暂存结果。returned Claim 不是根提交、witness 或模型执行授权；配额延后无状态事件，不能转录为 running。完整领取 Sender、半消息、精确提交后移交及恢复调度须分别接通。15项原行为表征、24目标、对齐main后的3336非集成/非interop单元零失败跳过；8项真实MySQL仅收集，CI结果与合并/运行进度回链infra总表。默认装配和生产未改。


[`InterpretationClaimPlanner / InterpretationClaimSender`](../../../src/qs_ai/infrastructure/workflow_transport/interpretation_claim_sender.py) 为显式原候选提供独立事务入口。只读来源核对精确Session/job/Run、到期资格及原lease/fence，关闭来源连接后冻结首次事件ID、时间和wire。正常领取仅推进一次版本，attempt耗尽保留原running→blocked两次推进和单份最终历史；原业务根先绑定SDK guard，再重新竞争原锁和来源，执行claim_in、转录、档案及witness共同提交。来源变化或配额延后不能提交预备的running事件。

只有首次取得执行资格的调用，在LocalCommitted且新权威读取精确witness/首档案之后，继续检查新边界的原活跃lease和原job输入，才返回Claim。FinalUnknown不推翻已证实本地提交；重复Committed只读首证明并返回None，不再次授权模型。Prepared/Aborted/未知保持未决，不自动生成新执行或模型重试。这里只接通显式单候选入口；无状态延后的调度责任、scanner交接、完整恢复及默认运行装配仍需独立门禁。

本地73组合目标和3385非集成/非interop回归零失败跳过，416源类型及lint/format通过；18项真实MySQL仅收集，覆盖原业务/租约/attempt/首历史/档案/witness共同提交、原数据库拒绝、客户端丢提交回复、来源竞争、真实配额拒绝、半消息模拟拒绝及首次/重复/失效租约移交。模拟运输和主动丢回复不算真实Broker或网络故障；CI、合并、部署和业务验收状态仅维护于infra总表。


隔离Broker档扩展原领取Sender的normal、耗尽、源冲突与真实配额拒绝四种输入：半消息不可见、原业务/租约/attempt/首档案/witness、最终确认客户端丢回复、首次移交和重复不再移交，以及无事件配额拒绝时精确回滚。该档要求23宿主用例/59项证据，保留固定SDK/Broker矩阵和完整清理；本地仅收集，实际结果回链infra总表。Observer不执行QS投影或provider，不能代替完整模型恢复、进程/网络故障及system验收。


[`InterpretationClaimDeferral`](../../../src/qs_ai/infrastructure/workflow_transport/interpretation_claim_deferral.py) 明确分开无状态的配额延后。显式宿主提供同一个冻结候选及旧执行，必须先从新权威边界读到全部精确Aborted witness且不存在Committed首档案，再开启独立的job维护根；它不再次调用SDK本地执行器。原Session/job/lease来源在原锁下重新核对，沿用原容量与daily reservation检查，仍被拒绝时只将job延后1秒；不创建模型租约、不增加attempt、不生成状态事件或消息、不改写旧协议终态。

容量恢复时，新根回滚所有暂存slot并返回False，不能借维护操作授予执行资格。新job维护根的提交回复丢失保留Pending，不声称回滚或消息提交，调用方只能核对现有job；这与旧事务已确认Aborted是两个独立事实。新操作不创建空的fence0租约占位；原claim默认实现和配额/租约权属保留。scanner责任交接、显式新执行授权及默认装配仍待完成。89组合目标/3401完整本地非集成回归零失败跳过，417源类型与lint/format通过；6真实MySQL仅收集，完整进度与CI回链infra总表。


无事件维护根在任何job或配额写入前，同根锁定并核对已安装的ai.mysql owner、store_id、writer epoch及schema版本，同时拒绝旧epoch的执行引用。协议证据存在于另一配置或旧writer，不构成当前业务库的维护授权；此保护不绑定新消息或改写旧Aborted状态。绑定不符必须拒绝而非降级，新增真实epoch变化拒绝用例与其余矩阵一并验收。


显式授权衔接正在开发：TransactionRuntime.retry/replan只向SDK公开Sender入口传递宿主决策，临时封闭和最终停止均先拒绝新准入；不直接调用Store绕过SDK在途/关闭记账。InterpretationClaimSender.retry要求显式旧执行和同一个冻结候选，先核对首计划并新读原到期来源，再由SDK原子验证精确旧Aborted和新fence。它复用首次完整计划，不签名加密、不重建ID或时间，不把重复lookup当作新业务授权。新的owned执行仍复用原guard/业务/档案/witness和首次提交后移交边界；未取得资格不移交模型。

本批当前仅在独立开发环境安装候选0.4.0a2 wheel（SHA256 5e9873836e393f9b28e214e7b8cb0e32d3046e2e90f498e2e6941d1f53267576），SDK从隔离site-packages导入，未使用SDK工作区或editable导入。仓库依赖锁和既有环境仍为已发布0.4.0a1；此候选覆盖不计required门禁或固定制品验收，固定修订发布并按真实URL/摘要重新安装后必须再验证。开发105组合目标、3417完整非集成/非interop单元零失败跳过、417类型通过，5原生仅收集。默认装配、scanner责任、其余终态及恢复、生产仍按infra总表独立推进。


显式 [`TransactionalInterpretationExecutionStore`](../../../src/qs_ai/infrastructure/workflow_transport/transactional_interpretation_execution.py) 接通原20候选/原顺序的只读扫描，查询根在规划和MQ前关闭。每个候选先检查宿主准入；Prepared跳过，已确认Aborted的原到期job由宿主明确调用SDK公共Retry，CAS成功才使用新fence继续首次冻结计划。普通重复请求和Checker不调用重试，首次wire不重新Seal。首次提交且新权威读取witness、档案和活跃lease通过后才移交Claim；重复或耗尽不再次移交模型。

只有原执行器实际发生配额拒绝且本地确认为Aborted时，才发出专用QuotaDenied信号。scanner还须重新查询精确中止证明，随后独立无事件job维护根沿用原绑定、锁和容量检查。半消息NotStarted/Unknown、取消或其他异常不能触发这项维护。renew/evidence继续委托原store，finish仍交给独立完成Sender；此组件显式opt-in，默认DI未启用，完整finalization/恢复与T17其他活动写入口仍需后续门禁。

本批锁定已发布Python `0.4.0a2` URL及wheel摘要`570822f9ed51eee25001624858d30b7433468eb7da59e34348289c50a37cabb2`；其他第三方锁项未变，底层客户端仍5.1.2。替代前述仅开发候选环境后，已重新安装正式发布制品，122组合目标及3427非integration/noninterop回归、418源类型与lint/format通过；6新增scanner原生MySQL及3新增Broker入口仅收集，实际门禁回链infra总表。新的Broker档要求26宿主用例/64精确证据，不能重用旧23/59证明新源。源码009b287，不表示生产、稳定SDK、system或T18模型故障验收完成。


完成阶段提供显式 `InterpretationFinishSender.retry(claim, result, previous)`：宿主必须持有原确定性结果和旧执行引用。它复用首次冻结消息、问题ID、时间和wire，不执行规划/签名/模型；新的只读根校验原活跃Claim、业务来源摘要、请求绑定及问题序号后关闭连接，再经SDK公共Retry竞争精确旧Aborted和新fence。原业务执行和根内重新校验、档案/witness共同提交、未知结果处理与首结果重复读取仍走同一实现。普通重复、Checker和维护器不调用这个入口。

此入口不授予模型重试，也不替代T18对持久化响应、恢复调度和provider调用次数的证明。已通过原74项表征、扩展后81目标和3434完整本地单元（1186 integration/interop排除、非执行通过）、418源类型、lint/format；7新增真实MySQL仅收集，源代码29b9a72，CI及生产状态回链总表。默认装配与生产保持原配置。

显式完成重试的Broker档在6f42998新增question/持久化Artifact两项入口：旧Aborted保持可回查、首次plan/wire与问题ID复用、新半消息提交前不可见、原根提交后首结果可重复读取，禁止再次调用DurableGeneration。新档28宿主用例/68证据；本地仅收集，实际执行和完整清理必须由对应新infra档核验，旧26/64报告不复用。


评测终态规划先复用原 [`generation_completion_progress`](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_completion_progress.py)：成功、未知结果、自动恢复许可和预算耗尽的原进度规则从SQL写入口抽出，串行路径保留blocked转换，candidate路径继续交给原complete_claim负责聚合。原complete_generation仍在同一根内校验来源、写完成证据、执行原checkpoint CAS和Run进度更新，提交归属不变；只读函数不访问DB、MQ或模型，也不授权重试。

抽取前42既有目标及10新增原入口表征通过，抽取后52目标、3444完整本地非integration/noninterop（1186排除，不算通过）、419源类型和lint/format通过，代码7d32da5。真实MySQL门禁仍须针对新候选重验；此处仅是规划计算基础，未完成评测终态Sender、candidate/recovery/取消drain全链路，也未启用默认装配或生产切换。

无事件配额维护根使用局部JobOnlyEvents保护：新根继承的旧提交钩子只在该session中替换，拒绝interpretation状态写入、非空evaluation事件集、原根或记录器替换；提交前和EventSession提交钩子均校验该边界。独立维护拒绝BorrowedTransactions，不能替宿主提交已有业务事务。全局Database.state_events保留，正常业务记录器不受该局部session影响。

该修订由真实Broker scanner配额用例发现：原fd141候选在已返回的Python3.11档中63/64证据通过，job-only根调用了被禁止的旧flush，隔离清理完整；失败报告保留在infra总表。54007b0修复候选通过127目标、3432完整本地单元（1182 integration/interop排除）、418源类型及lint/format；9项原生配额维护用例（新增3）仅收集，须按新源码重跑真实MySQL和26/64 Broker门禁，不能复用旧fd141通过项替代验收。生产和默认装配未变。


`EvaluationStateFacts.source_version` 显式冻结较稳定operation版本更晚的权威来源版本，使用v3 context；缺省None保持既有v1/v2编码字节。来源须为有符号范围内的整数且不小于原operation版本，旧序号版本不得晚于来源，冻结消息最终版本必须晚于来源。一次原根中多次checkpoint推进仍只形成一份最终状态事件；实际业务适配器继续校验精确来源、权限、目标投影和原CAS，字段本身不授权版本提升、重试或模型调用。

这样可以保留首次稳定request/input hash，同时为后续取消drain等来源推进冻结新的只读事实；新计划或执行仍须宿主显式授权，旧未决/Committed不能被覆盖。V3不修改SDK codec、wire、数据库schema或默认装配。22新单元覆盖跨版本/歧义字段、整数边界、来源/最终版本及多次推进；v1摘要f28022c5、v2摘要ecb9989a保持精确。74组合目标、3471完整本地单元（1194 integration/interop排除）、419源类型及lint/format通过。5项真实MySQL原根用例仅收集，包含原CAS、共同回滚、提交后客户端丢回复和单根两次推进；它们是协议/原根基础，不能替代完整评测终态Sender、真实Broker或T18模型恢复。代码d935f50，CI/合并和生产状态回链infra总表。


评测串行 generation 的终态来源读取与原写入共用 `load_serial_generation_source`：读取原checkpoint与组织内Run，校验精确dispatch身份，以及原版本或精确cancel_requested来源/目标版本。只读规划不加锁、不写入、不提交；原complete_generation按原checkpoint→Run顺序加锁重验，再执行既有ledger、冻结资产、历史终态与checkpoint CAS。只读结果不是提交证明或模型重试许可；lease到期不替代原证据规则。candidate路径继续由原completion_owner管理。

代码3c6e5ff：新增原入口9项表征先通过，再抽取；抽取后41组合目标、3486完整本地单元（1199 integration/interop明确排除）、419源类型和lint/format通过。5项真实MySQL用例仅收集，覆盖无写读取、取消drain、原根提交/回滚及旧观察不能重复提交；须按精确新源码完成required CI，不计完整terminal Sender、T18模型恢复或生产验收。


串行 generation 完成入口由 `SerialGenerationPlanner`、`SerialGenerationWrite` 和 `SerialGenerationSender` 负责：必须已有原SerialResponseReference对应的持久化首次响应；稳定request绑定原dispatch与响应摘要。完整规划冻结首次candidate、精确输入/终态证据/来源摘要及最终状态wire，RO连接关闭后才Seal。原根借用同session读取不可变资产，复算确定性证据并复用首次candidate，调用原complete_evaluated_generation提交完成记录、Run进度与原CAS，最后由外层归档及witness共同提交。没有provider调用或伪造SlotClaim。

重复已提交操作读取精确新witness、首次archive及原业务terminal receipt，返回首次checkpoint结果；Prepared/Aborted/Unknown不自动重做。LocalCommitted+FinalUnknown保留成功。retry只接受宿主明确决策并先读原来源/序号；原来源已改变时拒绝retry。明确replan另经公开SDK竞争精确旧Aborted，保留首次candidate和模型响应，并按新的权威来源冻结新plan；旧执行/消息证据保留。它不授权模型重试。

原prepare_step_completion新增可选显式candidate_id，默认旧行为不变；新根传入首次ID，成功要求原有受限身份，失败只能为空，semantic不得使用该覆盖。generation资产校验的参数收窄为get-only Reader契约，以借用原session读取，不需要写资产能力。85组合目标、3526完整本地单元（1212 integration/interop明确排除）、421源类型与lint/format通过；13项真实MySQL根用例仅收集，涵盖成功/未知、取消drain、witness拒绝、提交后客户端丢回复、首次结果重复及显式新plan保留candidate。原生与真实Broker仍待对应精确候选验收，不计T18的真实provider故障或默认装配/生产启用。


真实Broker档新增串行终态成功/未知两项及取消后显式replan一项入口，要求31宿主/76证据：半消息不可见、LocalCommitted+FinalUnknown、首次response/candidate/wire与精确witness、重复不重发，以及旧半消息回滚、新计划保留原candidate并在取消drain的原版本上提交。现阶段仅收集，不计实际Broker或T18真实provider/网络/进程故障验收。工具/source pin、安装SDK与清理报告须由infra另验，旧28/68不能复用。

原来源校验候选0ef23e8的实际MySQL8.4分片失败：foreign invocation测试只替换完成对象的invocation，未同步原provider receipt，被领域构造校验提前拒绝，未进入目标SQL冲突路径。修订使两者均为同一foreign身份，构造有效但仍必须被原dispatch校验拒绝；补两项只读/加锁负例，本地43组合目标、3488完整单元（1199 integration/interop排除）通过。保留失败报告，原领域校验与SQL条件不变，5项原生仍须按新源码完整重验。


串行终态候选已在de8a712合入17dc29a的foreign receipt夹具修正；业务/终态Sender、首次wire代码及Broker档与4c068f2逐字节保持，两个来源负例文件与3068e1一致。合并后87目标、3528完整本地单元（1212 integration/interop排除）、421源类型通过；13原生及31/76实际Broker仍须按本候选重验，前置旧0ef失败及修正证据保留。


semantic终态先抽取原有未知计数、固定预算及blocked转换为纯 `semantic_completion_progress`，供原写入与后续只读规划共用。candidate路径仍由complete_claim决定聚合，成功后的全部候选/dispatch投影、取消drain和awaiting_review判定仍在原complete_semantic路径，原SQL、加锁、证据、CAS及提交归属不变。不是新重试授权、完整semantic事务Sender或默认装配。

隔离分支以be28a30为基线，先增加14项原入口表征，覆盖未知、自动恢复许可/拒绝、预算耗尽、成功继续/候审、取消与串行/candidate职责；抽取前后61组合目标及421源类型通过。真实数据库/完整CI和源码合并仍待门禁，不复用父候选报告。


`load_serial_semantic_state` 复用原checkpoint→Run来源读取及精确取消版本校验；RO模式不加锁、不写入、不提交，原受理器使用加锁模式保持相同SQL顺序。返回值只是组织内来源数据，允许checkpoint为None；接受generation候选、精确candidate/dispatch/历史、冻结资产、业务CAS等权威校验继续由原complete_semantic负责，不能把读取成功当作模型或完成授权。新来源数据的repr不包含Run或checkpoint正文。

14项原终态表征、16项来源边界及其余回归共77目标、3558完整本地单元（1216 integration/interop明确排除）、421源类型通过。4项真实MySQL来源/取消/共同回滚用例仅收集；全量本地、真实依赖门禁、完整semantic事务Sender和默认装配仍待后续，不复用父候选验收。


`read_semantic_acceptance` 将原候选、dispatch、顺序、冻结策略/路由、obligation及判定解析/证据构造从首次SQL写入前抽出。RO模式复用原校验而不加锁/写入/提交，原complete_semantic使用同样原锁重读后保留原insert、候选更新、计数/候审投影、CAS与根提交。路由接口只需get-only Reader，语义准备可借用同session资产快照。返回值是确定性证据，不是提交、witness或模型授权。

原14种路径分别覆盖原写入、RO验收、加锁验收三个模式（42项），连同来源/领域回归共105目标、3586完整本地单元（1216 integration/interop排除）、421源类型通过；RO不得修改候选或数据库，原提交归属保持。4项真实MySQL用例还核对RO证据/候选投影与原根提交/回滚结果，但目前仅收集。实际依赖CI、完整semantic事务Sender及默认装配仍未完成。


语义终态的原insert值与后续候审计算分别复用semantic_terminal_values和project_semantic_progress。原写入口在原insert/候选更新后，用同样原三组锁定读取执行原project_slots与next_action；只读规划使用before_insert模式，仅在内存替换精确generation候选并追加虚拟semantic终态，不写数据库、不发布或授权模型。拒绝缺失/重复generation来源及已有同execution终态。candidate聚合和取消分支继续由原责任处理，虚拟投影不接管。

新增13项锁/RO、虚拟/已写入、继续/候审、缺失/重复来源及candidate/取消边界；118目标、3599完整本地单元（1216 integration/interop排除）、421源类型通过，4项真实MySQL原根用例扩展对比虚拟进度与原提交结果，目前仅收集。完整回归、required实际依赖及完整semantic Sender仍待门禁。


SerialSemanticPlanner/Sender/Write使用独立semantic context和原SerialResponseReference，原generation candidate ID不能改变。只读取原持久化响应，并在同session快照中重建原输入、判定和虚拟终态；源连接在Seal/半消息前关闭。全部半消息后，原根重验来源、调用原complete_semantic，将原判定/候选/进度、archive和witness共同提交。重复从精确witness/archive/不可变semantic receipt读取首次结果；未知不重做模型，显式Retry/Replan继续由宿主授权并竞争旧Aborted。没有伪造SlotClaim或新candidate，构造不启动网络/线程，也不接管宿主资源。

150组合目标、3631完整本地单元（1229 integration/interop明确排除）、423源类型通过；13实际MySQL根用例及新Broker档34宿主/84证据仅收集，覆盖成功/未知、取消drain、原根witness拒绝与提交回复丢弃、首结果/旧中止/首响应与candidate保留。尚不计实际依赖门禁、T18真实provider/网络/进程恢复或默认装配。旧31/76不能代替新档验收。


candidate终态的completion_owner新增显式只读观察：沿用原checkpoint→组织Run→全部active claim→精确claim读取顺序；默认加锁行为保持，原写者继续使用原coordinator/claim冲突保护。只读数据不是提交凭证、活跃租约证明或模型调用授权。原Run状态、candidate模式、serial checkpoint拒绝、claim版本/owner/phase/索引/执行身份检查保持；原complete_claim聚合与事务归属未改变。

本批先在原入口增加17项表征，再扩展为34项RO/锁定一致性；116组合目标、3665完整本地单元（1233 integration/interop明确排除）、423源类型、1099文件格式检查通过。4项真实MySQL用例仅收集，验证原根租约/Run共同提交回滚，以及旧观察不能跨越claim释放或slot重用。required实际依赖CI尚待通过；完整candidate终态Sender、聚合虚拟计划和默认装配仍待实施。


candidate_completion_progress共享原complete_claim的终态规则，仅对已验证的完成后数据计算原进度及读取精确失败证据；不释放claim、不修改数据库、不保存checkpoint或提交。原complete_claim仍先释放精确claim、锁定其他active claim/Run、用原candidate_plan验证全部dispatch，再执行原CAS与进度写入。其他在途claim存在时不提前全局阻断；最后unknown优先于取消，保留原失败证据时间、semantic恢复的evidence_at及候审责任。

新增11项原入口表征后以原根/RO两个入口覆盖22项，138组合目标、3687完整本地单元（1233 integration/interop排除）、423源类型、1100文件格式及文档门禁通过。原根与RO规则一致，不在测试中用快照冒充真实提交；实际存储仍由required完整CI证明。本批未实现虚拟pre-insert candidate聚合、完整candidate Sender或默认装配。

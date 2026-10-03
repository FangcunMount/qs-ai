# qs-ai MQ 原事务接入（候选交付）

本批仅实现和隔离测试。生产未启用；不包含真实模型调用、部署、业务发布或人工质量批准。MBTI 验收目标继续独立记录。

## 交付边界

基于 main `a0eae54`，复用外围 `31d0443`、`4b27fbc`。首批使用 SDK Git 候选 `9f16fa30dc43a50a2d4492f3e95c905a8f969331`；后续依赖收尾使用已发布 wheel；正常宿主兼容验证发现最终 ACK 成功 PUB 耗尽失败预算后，现固定为补丁提交 `8633ba585b5a1c8f95fb16b19469fbd0097611c7` 的 [Python 0.2.0a2 预发布](https://github.com/FangcunMount/reliable-messaging/releases/tag/python/v0.2.0a2) wheel，并保留 `[nsq]` 扩展。预发布不是稳定版或宿主生产切换证明。

- wheel URL：`https://github.com/FangcunMount/reliable-messaging/releases/download/python/v0.2.0a2/fangcun_reliable_messaging-0.2.0a2-py3-none-any.whl`
- SHA256：`a9c424de16b1701c52d04cb51f2898fa868488f110c5772f2fd360e1c543c932`，独立下载核验后固定在 `uv.lock`。
- `pyproject.toml` 不再引用 Git source。锁变更仅涉及本 SDK 来源及其 wheel 元数据，没有升级其他依赖。
- 删除临时 `deploy/candidate/Dockerfile.mq`，CI 恢复正常 Dockerfile；正常 Dockerfile 本文各批次均未改动。依赖构建和运行不再需要 Git。

本批维护原业务/storage 接缝、事件记录、增量迁移、统一生命周期和候选验证。MQ framing、JOSE、Inbox、Outbox、ACK、物理失败处理由另一会话维护；没有创建第二套业务重试、生成、审批或发布状态机。

SDK 补丁保持原 wire/schema/接口，只保护最终 ACK 的失败预算和已有技术挂起。历史计数可能包含成功 PUB，不自动清零、解挂、重封装或授权模型重试。宿主最终依赖、正常镜像及生产证据单独记录。

## 原事务与接单回执

`CommandReceiver` 建立根事务并锁定 Inbox，然后调用 `WorkflowCommandAdmission.admit`。

- Start、Change、ParticipantRetry、EvaluationStart、EvaluationCancel 均复用原业务方法。
- `Transactions.borrowed` 复用接收器的原 AsyncSession/root transaction；业务 commit 为无操作，不开启替代事务，不关闭会话。
- 确定性拒绝使用局部 SAVEPOINT 撤销本命令写入；返回时 SAVEPOINT 已关闭，根事务仍为原事务。评测事件的进程内待记录标记也恢复。
- 接收器统一提交业务、状态事件、Inbox 和首接单回执后才允许 FIN。技术故障整体回滚并由外围保存有界失败预算。
- 重复消息先复用 Inbox/原回执，不再经过业务 CAS。接单成功仅表示原业务请求已接受，不代表模型完成或人工质量通过。
- Start 原有“持久接单但准入 blocked”的行为保持不变，不将 blocked 会话误记为执行成功。

拒绝状态映射沿用 gRPC：权限 7、资源不存在 5、容量 8、状态冲突 10、非法命令 3；技术故障继续向外围传播，不记录异常正文。

## 状态事件

- 解读沿用原 `result_outbox` 首次 `event_id`、正文、request_id 和原时间。重复 save 读取首次已存行，不用新 UUID 覆盖。
- `mq_owned` 表示投递归属已转移，不表示 QS 已收到。旧 gRPC scanner 永久排除此类行；只有新 Outbox 收到验证过的 ACK 才确认。
- 既有未发送结果由有界 handoff 原事务移交。已经 delivered 的结果不移交。不制造历史评测事件。
- 评测创建/检查点变更登记待记录 Run；`EventSession.commit` 在原事务提交前读取完成的投影并 stage 事件，防止读取到投影前状态。
- 评测序号在 `ai_messaging_evaluation_sequences` 持久分配；首次实际事件从 1 开始。事件 UUID 由原 Run+提交版本固定。事件落库失败会撤销同事务投影和序号。
- 事件为非 Broker 强制顺序（`ordered=False`）；消费者按原 version/event_sequence 处理迟到状态。没有将当前状态补造成已发生的旧事件。
- 全部网络发送发生在业务事务提交之后，数据不删除，迁移不改历史冻结配置/指纹。

MQ 归属一旦转移，不得只关开关、让旧镜像接管投递。回滚需要支持新结构与归属的镜像，并核对未确认记录；不能 DROP 新表或清除归属标记来实现回滚。

## 启动装配与配置

默认 `messaging.enabled=false`，原 gRPC 模式保持可用。启用 MQ 时不装配 `DeliveryProvider`、不预解析 `DeliverResults`、不启动旧 delivery loop；改为唯一 MQ relay。gRPC 仍保留管理接口和精确引用正文读取。

MQ 启用时，统一 gRPC 装配在业务方法之前拒绝 `Commands.Start/Change`、`ParticipantManagement.Retry`、`EvaluationManagement.Start/Cancel` 五类旧运行写入口。可信 QS 收到 `FAILED_PRECONDITION` 和 `MQ admission required; gRPC execution command not accepted`，明确没有接单；不可信工作负载仍先收到 `PERMISSION_DENIED`。不会进入业务作用域、数据库事务或模型执行，也不产生替代回执。查询、治理/配置和方案/评测 `Create/Prepare` 继续使用原 gRPC 方法。MQ 关闭时不安装该边界，旧行为保留用于兼容期；这不改变已转移 MQ 归属数据的回滚限制。

配置结构示例（只有路径与地址，正文不含密钥）：

```yaml
messaging:
  enabled: false
  nsqd:
    nsqd:4150: http://nsqd:4151
  signing_key_file: /run/secrets/qs-ai-sign.jwk
  decrypt_key_files:
    ai.encrypt.1: /run/secrets/qs-ai-decrypt.jwk
  qs_signer_files:
    qs.sign.1: /run/secrets/qs-sign-public.jwk
  qs_recipient_key_file: /run/secrets/qs-encrypt-public.jwk
  max_in_flight: 1
```

沿用 `grpc.access_address` 和 mTLS 证书作为可信 QS payload endpoint。JOSE 私钥/公钥角色、kid、EC P-256 和文件上限启动校验，错误不携带原文件正文。显式提供 NSQD TCP→HTTP 映射，不猜端口、不扫描网络、不在启动中创建拓扑。

运维必须事先创建 commands/acks 原 Topic/Channel 及其 SDK failed Topic/Channel；拓扑检查每次只做有界读取。统一 supervisor 管理发布器、订阅器、relay；发布器先就绪，订阅器随后启动。停机先阻止新命令并排空，保留发布器完成回执/物理失败移交，最后关闭客户连接和数据库容器。没有新进程、信号处理器或内部 `asyncio.run`。

## 隔离验收与未完成门槛

五类旧写入口退役的独立回归：`pytest tests/test_grpc_mq_cutover.py`，真实 mTLS 装配覆盖五类方法开启/关闭、工作负载身份及查询/治理/准备保留，共 24 项，本地通过。相关既有 gRPC/单服务回归 57 项及 MySQL 8.4/真实 NSQ 原事务接入 8 项通过（CI 格式 `mysql://` 测试 DSN）。新边界尚待整合后的远程门禁，不作为部署证据。

可复现测试入口：

```text
uv sync --frozen
alembic upgrade head
pytest tests/integration/test_mq_admission.py tests/integration/test_mq_worker_events.py
pytest tests/test_messaging_runtime.py tests/test_single_server.py tests/test_runtime_lifecycle.py
pytest tests/test_mq_contract.py tests/test_mq_failure.py
```

测试必须使用独立数据库与 NSQ，配置 `QS_AI_TEST_MYSQL_DSN`、`QS_MQ_NSQ_TCP_ADDRESS`、`QS_MQ_NSQ_HTTP_URL`；跨语言测试须提供固定 `QS_MQ_CONTRACT_PROBE`。外围存储测试使用另一独立 `QS_MQ_MYSQL_URL`，其 fixture 创建/删除外围表，不能指向已迁移业务库。

验收覆盖原业务命令、局部拒绝、原事务全回滚、首次身份/密文重放、历史移交、事件失败回滚、完整 35 候选/70 次假模型评测、真实 NSQ 接入和统一停机。真实 NSQ 用例禁用生成/评测执行组件；其模型调用数为 0。

本地核证（2026-10-03）：

- MySQL 8.0.36／8.4 原事务接入与完整假模型 Worker：各 9 项通过；空库迁移、重复 upgrade 和 Alembic check 通过。
- 原有并发、取消和恢复回归 41 项通过；生命周期与共享单服务回归 25 项通过；最终 MQ 运行时回归 6 项通过。
- 固定 QS `f637b29dac8537e133559f3877fb82156611e0d9` 探针的跨语言契约及失败分类 22 项通过，业务互操作基线仍保留原 `e4c93f7`。
- 完整假模型评测 35 候选、70 次调用无重复，事件序号连续；真实 NSQ 接入仅接单，不调用模型。
- 最终候选镜像 SHA `fd009035cff95a58c9165bc684b53ed217e01efb8951028a0079fa0a5b34c998`：MQ 启用、MySQL 8.4、HTTP 健康、真实 mTLS、单进程、非特权运行与正常停机通过。镜像仅在本机构建，没有推送或部署。
- Ruff、Mypy、协议生成和文档链接检查通过。以上不是远程 CI 或生产证据。

wheel 收尾核证（同日）：`uv sync --frozen`、独立 wheel SHA256 校验及安装来源核对通过；全部锁定包版本保持不变。替换发行载体后，相关运行时/跨语言/失败分类 28 项、MySQL 8.4 原事务与完整假模型 Worker 9 项通过。正常 Dockerfile 的 Linux/amd64 镜像 SHA `15daa4f5325c88235a19755990fa72e53ba11e97baaffe71d5a6b41e9d44a0b0` 构建及实际启动通过：MQ 启用、单进程、非特权、健康、mTLS、运行层无 Git、正常退出 0（0.74 秒）。Dockerfile 和业务执行/恢复代码没有改变。

SDK 预发布发行和 wheel 摘要核验已完成。仍需独立完成：三端候选 CI、QS 联调与跨服务故障/恢复全链、生产切换授权和核证。本文件不把本地通过写成生产完成。

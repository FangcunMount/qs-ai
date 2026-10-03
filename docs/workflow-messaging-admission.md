# qs-ai MQ 原事务接入（候选交付）

本批仅实现和隔离测试。生产未启用；不包含真实模型调用、部署、业务发布或人工质量批准。MBTI 验收目标继续独立记录。

## 交付边界

基于 main `a0eae54`，复用外围 `31d0443`、`4b27fbc`。SDK 源码依赖与 lock 固定 `9f16fa30dc43a50a2d4492f3e95c905a8f969331`（Python 0.2.0a1 候选），不能当成正式 Release。正式发布前必须换成经审核的 wheel URL/摘要并完成相应回归。候选镜像使用独立 `deploy/candidate/Dockerfile.mq`，仅在依赖构建层安装 Git，最终运行层不携带 Git。正常生产 Dockerfile 保持不变；Git 候选依赖不能通过正常生产构建，正式 wheel 替换后再验收生产配方。

本批维护原业务/storage 接缝、事件记录、增量迁移、统一生命周期和候选验证。MQ framing、JOSE、Inbox、Outbox、ACK、物理失败处理由另一会话维护；没有创建第二套业务重试、生成、审批或发布状态机。

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

仍需独立完成：正式 SDK 发行审核、三端候选 CI、QS 联调与跨服务故障/恢复全链、生产切换授权和核证。本文件不把本地通过写成生产完成。

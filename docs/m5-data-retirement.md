# M5 定向清库工具

此工具属于独立维护批次，不随应用启动、普通迁移或部署自动执行。生产使用前必须完成真实 v6 管理/授权生成、撤权、可靠回传与恢复验收，并停止旧生产者、恢复任务及相关数据库写入，定向排空旧消息。无须等待 24 小时；本地恢复测试不能代替这些生产证据。

## 固定范围

策略位于 [policy.py](../scripts/retirement/policy.py)。只允许删除九个明确的旧 AI Mongo 集合，以及 Mongo `domain_event_outbox`、MySQL `domain_event_outbox` / `event_delivery_dead_letter` / `retry_event_hold` 内六种完整旧事件名称对应的记录。后两表按已核对的领域事件顶层 `eventType` 或传输信封顶层 `type` 匹配；二者同时存在但不一致时保留。无法识别、不合法或仅在嵌套字段出现的事件名不作为删除依据。SQL 删除使用清单中的原始主键；共享表保留结构及其他消息。

所有其他集合和表均盘点其数量、内容摘要及结构；SQL 同时记录本库和跨库外键关系，选中记录存在外键时停止。技术表全部只盘点，不自动删除：`checkpoint_leases` 曾承载生产 fence，应先经过 0028 原样迁成 `execution_leases`；不能因为暂时零行而删除。其余 LangGraph 技术表若在其他环境发现，另行确认来源；当前没有可批准删除的实际对象。没有明确调试记录 ID 清单时，不删除 qs-ai 业务数据。

工具不连接 NSQ 或 Redis，不清空共享 Topic/数据库，不改迁移账本或共享凭据。新链路及标准报告数据全部受保护。备份仅包含待删数据及恢复结构，不复制所有业务正文，但会读取受保护数据计算摘要；应在无相关写入的维护窗口执行，数据变动则重做清单，不放宽比较。

## 运行顺序

在维护机的 qs-ai 检出目录安装维护依赖：`uv sync --locked --group maintenance`。这组依赖不进入生产镜像。通过受控环境注入以下连接信息，不放入命令参数、Git、工单正文或 CI 日志：

- `M5_QS_MONGO_URI` 与 `M5_QS_MONGO_DATABASE`。
- `M5_QS_MYSQL_URL` 与 `M5_AI_MYSQL_URL`，格式为 SQLAlchemy MySQL URL。

MySQL URL 不支持查询选项，远程连接应通过受保护隧道；Mongo 可使用其标准 TLS URI。源账号需能读取目标及相关跨库外键元数据；删除阶段另需具备指定对象的删除权限。默认在源服务器创建随机恢复验证库；业务账号无跨库建库权限时，通过 `M5_RESTORE_MONGO_URI` 和 `M5_RESTORE_MYSQL_URL` 指定隔离恢复服务器。MySQL 恢复 URL 指向已存在的管理数据库（例如 `information_schema`），恢复账号只在该隔离服务器创建并删除随机库。源服务器不会承担恢复写入；不扩大生产业务账号权限。恢复回执记录隔离目标摘要和随机库名，保留原来的数量、内容及结构完整校验。恢复环境应匹配生产数据库版本与结构能力，不匹配则停止。正式执行前先用只读账号盘点；写账号切换不会改变目标身份摘要。

```sh
# 目录必须尚不存在，且在所有 Git 工作区之外。
uv run --group maintenance python -m scripts.retirement plan \
  --directory /data/backups/qs-ai-m5/<批次>

# 保存上一步返回的 plan_sha256；核对 plan.json 的对象、数量、引用和摘要。
uv run --group maintenance python -m scripts.retirement backup \
  --directory /data/backups/qs-ai-m5/<批次> --plan-sha256 <清单摘要>
```

`plan.json` 不包含测评正文。`backup.json` 保存 Mongo BSON、索引/选项，以及由参数绑定生成的 SQL 行与建表语句；目录权限 0700，文件 0600。备份后从磁盘重新读取，在独立的随机 `m5_restore_<UUID>` 数据库中恢复，核对数量、内容与结构，然后删除验证库。实际恢复失败不会签发 `restore-verification.json`。生产数据库和恢复库的连接信息不进入清单。

恢复凭据记录备份 SHA-256、验证时间和七天到期时间。备份七天内不得删除；工具不会自动销毁备份。到期后拒绝新删除，需重新备份验证。实际删除开始和完成时再次延长至至少七天，保留期不阻塞验收。

实际维护责任人核实停止写入、定向排空和生产验收后，在私有的 0600 JSON 文件中记录 `acceptance_verified`、`writers_stopped`、`old_events_drained` 三个布尔值，以及 `qs_sha`、`ai_sha`、`evidence_reference`。这些是可追溯的人工事实声明，工具不会把声明当成自动完成生产检查的证据。

```sh
uv run --group maintenance python -m scripts.retirement apply \
  --directory /data/backups/qs-ai-m5/<批次> --plan-sha256 <清单摘要> \
  --maintenance-evidence /data/backups/qs-ai-m5/<维护证据>.json
uv run --group maintenance python -m scripts.retirement verify \
  --directory /data/backups/qs-ai-m5/<批次> --plan-sha256 <清单摘要>
```

任何目标、内容、结构、外键或受保护数据变化都会停止。SQL 删除事务先锁共享表范围；Mongo 集合删除和跨数据库操作不是一个原子事务，因此必须保持维护窗口。`application.json` 记录开始、已完成数据库及结束状态；出现部分失败时保留备份并核对现场，不允许重跑同一清单掩盖部分执行。错误输出仅为安全原因或异常类型，不打印驱动 SQL、连接串和敏感正文。

## 恢复与收尾

```sh
uv run --group maintenance python -m scripts.retirement restore \
  --directory /data/backups/qs-ai-m5/<批次> --plan-sha256 <清单摘要>
```

该命令把原备份再次恢复到新建的隔离数据库，并保留验证通过的恢复库，输出库名。只包含原待删除对象/行；先核对现场删除进度，再由维护责任人从恢复库将缺失集合和明确消息主键恢复到目标库。禁止覆盖新链路数据、删除共享表或直接启动依赖旧表的旧镜像。已完成的数据库迁移历史不能倒改。七天到期只影响新删除授权，`restore` 和只读复验仍校验归档摘要，不受该期限限制。

恢复或清理后检查旧集合不会在服务重启后重建，复验标准报告、新生成/管理/回传链路与客户端展示，并保存证据。没有微信发布与真实业务验收证据时，不将工具通过视为 M5 完成。

## 自动验证

应用分片运行清单、权限、漂移、过期和恢复失败的单元测试；独立 CI job 在 MySQL 8.0.36/8.4 + Mongo 7.0 中运行 [真实恢复和删除测试](../scripts/retirement/test_live.py)。测试仅使用随机 `m5_test_` 数据库和合成数据，生产连接不进入 CI。

## 当前 v6 之外的 Prompt 清理

维护工具仅把 `cross-dimension-participant-scale` 的 `v1`–`v5` 列为候选，不按时间、状态或模糊版本删除。盘点所有 qs-ai 表中的版本引用、指纹、原包摘要和嵌套 JSON；未限定版本的引用也会保守保留。缺失完整资产身份或存在 SQL 外键（包括跨库外键）时保留。清单的 `selected_ids` 列出具体模板及版本，`retained_candidates` 列出保留原因和引用表，不输出 Prompt 正文。

与其他对象使用同一套 `plan → backup → 隔离恢复验证 → apply → verify`。删除前锁定 qs-ai 引用表并再次比较完整盘点；维护窗口仍必须暂停所有业务及后台写入。只删除同清单中的复合主键，v6、被引用旧版本及所有其他表数据保持不变。恢复验证重建原表结构并逐行核对选中记录，备份正文仍留在 Git 之外。

这些工具的本地测试使用隔离数据库；工具就绪不代表已完成生产清库。NSQ 原 Channel 定向排空和 Redis 精确键清单仍须完成后才能进入生产删除。

## 2026-09-15 生产只读盘点

9 个旧 AI 集合合计 59 条记录，Mongo `domain_event_outbox` 中六类旧 AI 事件共 1,145 条。QS MySQL 的 214,005 条死信使用 component-base 领域事件格式 `eventType`，其中标准报告事件 213,646 条、测评请求 354 条、答卷提交 5 条；本次全部保留。工具已补充实际字段格式识别和冲突保留测试。数字为盘点时快照，删除前仍须在停写窗口重新生成完整清单。

新链路已有 2 个请求、2 个会话、2 行执行租约，必须全部保留，不能只保留最早验收请求。`checkpoint_leases` 及四个样板检查点表已不存在，不能再次删除 `execution_leases`。

共享逻辑 Topic `assessment-lifecycle` 对应实际 NSQ Topic `qs.evaluation.lifecycle`。主 Channel `qs-worker` 及失败交接 Topic `cb.failed.b6abe5a59ae6cb4454f2a271` 的 `cb-failed-handler` 均需盘点。失败交接 Channel 尚有 16 条延迟消息；当前只读观察未获取消息内容、未确认任何消息。主 Channel 空不能证明这些消息已排空，不能将未知消息归为旧 AI 后删除。

生产 QS MySQL 盘点约 10 GB。清单版本 2 按主键顺序逐行读取 QS 表，流式计算所有受保护行的完整 SHA-256 和数量，Mongo 按 `_id` 顺序采用相同方式；不将受保护正文积存在内存。仅待删除记录留在备份中，qs-ai 小型资产引用检查仍完整遍历其业务数据。缺少稳定主键的 SQL 表停止盘点。旧版本 1 备份仍可恢复，但不再用于新的备份或删除，必须重新生成版本 2 清单。

## Mongo 性能日志保护

2026-09-15 盘点确认源库还存在 `system.profile` capped 集合，profiling level 为 1。它不是旧 AI 数据，必须保留。工具先检查系统集合清单，未知系统集合在读取大表前停止；已核对的 `system.profile` 按自然顺序完整计算受保护摘要，绝不删除或写入其正文。profiling 非 0 时拒绝稳定盘点；维护时记录原采样配置，仅暂停采样级别，完成后恢复原级别，阈值和过滤条件不改动。未暂停的性能日志写入不能被视为数据库停写。

NSQ 的 16 条延迟消息已在原失败交接 Channel 核对为旧 AI 评测步骤事件，归档 23,576 字节并完成隔离 Topic 的逐字节恢复验证后定向确认。清理后待处理数为 0，三个 Worker 已恢复；备份位于 serverD 的 `/data/backups/qs-ai-m5/nsq-inspection-20260915`，执行收据位于同级 `nsq-retirement-20260915`，至少保留到 2026-09-22 00:44:25 UTC。共享业务死信未删除。生产数据库集合尚未删除。

# MQ 外围验收与历史结果确认

范围：宿主 messaging store、跨语言传输、正文引用和隔离进程。模型执行、恢复状态机、冻结配置、bootstrap 和迁移由原负责人维护；本文件不代签这些功能的生产验收。

## 原结果的有限确认链

原 `result_outbox` 在业务事务中保存首次 StateEvent。StateEventRecorder 锁定原记录，保留身份、业务正文及原创建时间，持久保存一次安全封装后的新 Outbox，并标记 mq_owned。mq_owned 只表示传输归属，PUB/FIN 均不证明 QS 业务接收。

QS 在 Inbox、业务投影和最终 ACK 同事务提交后，AI 才能确认。`MessagingStore.confirm_event` 对 INTERPRETATION_STATE 先锁原 result_outbox，再锁新 Outbox，和移交保持同一锁顺序。验证原 event/session/version/request 与完整 Protobuf 正文一致，再核对确认摘要；在同一宿主事务中更新新 confirmed 和旧 delivered。原字节存储 hash 各自保留，不使用 JSON 重序列化字节替代跨语言正文 hash。

技术挂起、错摘要、未移交、身份/正文冲突不把旧结果标为 delivered。存储异常时整个事务回滚。重复确认保留首次 delivered_at，不重封装或重建任务。命令回执与评测事件不依赖旧结果表中的对应行。

## 必须通过的真实存储风险用例

`tests/integration/test_mq_admission.py` 验证有效/重复确认、错摘要、技术挂起、主动事务回滚、旧正文冲突、未移交记录、旧结果存储失败，以及移交与 ACK 并发的锁顺序。`scripts/ci/verify_mq_report.py` 必须找到全部参数场景，且报告非空、零失败、零错误、零跳过。

## 两端独立进程与正文读取

现有执行规则用例单独生成 `execution.xml`，由 `verify_mq_execution_report.py` 检查必需场景及零跳过：持久响应与原配置复用、发出后未知结果不再调用、提交失败/进程中断、取消竞争、原容量与额度保留。CI 在两组 MySQL/Python 组合执行原负责人维护的五个测试文件，使用已有模型替身，不修改其源码或测试。该证据与 MQ 外围报告分别报告；它不证明生产真实模型效果或完整跨服务成功接单。

`tests/probes/mq_fault_acceptance.py` 使用原 Go Runtime 与正常 AI server，验证命令、接单拒绝回执和最终 ACK 在 PUB 后丢失、两端和专属 Broker 强杀后的原身份恢复。它使用缺失 Run 的原业务拒绝，不证明成功接单/模型执行的完整业务验收。

`tests/probes/mq_payload_acceptance.py` 通过原 Go 业务存储建立隔离历史请求，保存原 128 KiB Unicode artifact 结果；不创建真实模型调用。正常 AI server 移交原结果并发布受保护引用。实际 mTLS Get 验证正文/长度/hash，以及错目标、组织、身份、hash、长度和错误 workload 拒绝。专属 Broker 丢失后重启两端，原 wire/body/time 保留，QS 仅一次持久效果，新旧 Outbox 同时确认，重启后引用正文仍可读取。标准失败中转的完整编码同样不得超过 262144 字节。

这两个探针仅可指向明确配置的一次性独立数据库和 `rm-ai-mq-dual-nsq-*` 的 NSQ 1.3.0。不执行共享/生产故障，不修改生产开关或冻结配置。CI 覆盖 Python3.11/MySQL8.0.36、Python3.13/MySQL8.4，上传报告/日志，不上传即使是一次性的私钥或证书。

## 分层证据及开放项

代码、静态检查、真实存储、隔离进程、镜像、浏览器、生产对账分别记录在独立 MQ 进度文档。浏览器使用合成 HTTP 后端不能证明生产 IAM/QS 授权。正常镜像构建或健康不能证明实际 MQ 业务完成。

生产必须先补齐两端承载/镜像/schema/停止预算、受保护正文 ACL、网络、Topic/Channel、独立密钥信任、历史移交清单和不可证明顺序的处置；完成 MQ 兼容镜像回退、最终依赖/正常镜像验收后，提交具体审核。旧生产镜像不认识 MQ 持久状态，不能作为首发 MQ 自动回退目标。未取得各消息族自然业务样本时，业务验收保持开放。


## R2-04 持久状态观测（首批，尚未关闭整个观测项）

复用 `/metrics` 的现 REQUEST Transactions、一致性 READ ONLY 快照与5秒总/每SQL1秒预算；不新增组件、线程、池或调度器。MySQLOperationalMetrics由既有DI注入必需Settings，MQoff不访问MQ表；MQon存储读取失败只返回database_up=0、配置enabled及observation_available=0，不输出空积压或部分/旧状态。

固定全局gauge区分staged（含未来可用）、due（到期，不是按序可领取）、awaiting_receipt（等待业务确认）、Outboxheld与Inboxheld、最老创建年龄以及五种保留quarantine分类记录。attempts上限8不作为终生counter；未来创建时间clamp0，确认历史不算积压。原pending_results语义保持不变，不与新MQ待投相加。

新增聚合函数仅借用caller已开始的只读快照，可按可信organization_id过滤Outbox。没有可信组织列的Inbox/quarantine不进入组织结果；renderer只认固定白名单，不输出身份、正文、hash、凭据或异常文本。当前组织 Runtime.Health 还未接该增量，不能宣称组织health已完成。

持久重复计数和payload读取失败分类账尚未实现，分别明确observations_available=0，并不输出伪造的0次计数。后续独立分类事实/迁移、原事务重复记录、只读payload技术审计和QS对应观测仍开放，不改保护0037/原业务schema或bootstrap。

真实MySQL定向回归验证已提交/未提交/回滚可见性、读者不等待写者行锁/不结算记录、阶段与年龄/未来时间/组织过滤、安全保留记录为gauge及无伪造历史；存储失败不泄露部分/正文。正常create_app的既有DI构造验证Settings解析与MQon缺数据库不可用，不读密钥/不连接NSQ。这些测试不替代正常镜像现场metrics及完整R2、生产验收。

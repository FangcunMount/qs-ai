# 事务消息、原业务事实与确认

当前服务只接受RocketMQ和唯一`ai.mysql` owner；本次可读命名候选固定已发布预发布0.5.0rc1，生产升级状态另由infra登记。宿主持有数据库池、业务根事务和生命周期，构造/启动不执行DDL。旧NSQ provider、普通发布Outbox、MQRelay及历史结果转移执行器均已退役，开发隔离依赖和SDK通用兼容包不在生产清理范围内。

## 事务装配与预算

[MessagingRuntime](../../../src/qs_ai/bootstrap/messaging.py)先校验最终配置，再借用APP TransactionServices和原primary pool；JOSE、CA、凭据、固定v2/v3 manifest和明确QS正文RPC地址均由宿主配置提供，不从消息属性选择数据库或动态发现端点。固定Python客户端要求原显式CA环境与配置一致；不能创建全局factory或跨线程共享AsyncSession。

| 逻辑方向 | 消息类型及owner | 原事务职责 |
| --- | --- | --- |
| QS→AI command | TRANSACTION，`qs.mysql` | QS operation、首次command档案和witness |
| AI→QS receipt/state | TRANSACTION，`ai.mysql` | AI业务事实、Inbox/状态、首次档案和witness |
| QS→AI event ACK | TRANSACTION，`qs.mysql` | QS Inbox、业务投影、首次ACK及witness |

三者使用manifest指定的活动物理路由和`m3-1` revision。v3逐条固定`naming_scheme=readable-v1`，v2哈希规则保持原行为。普通通知不能占用这些事务Topic；owner预算、原store/epoch及资源以infra冻结配置为准。启动会登记完整owner事务Topic，空闲/暂停实例也运行Checker；`publisher_ready`是历史命名的owner启动Event，不代表新写准入或业务已交付。

## 可读路由与历史证据

[路由装配](../../../src/qs_ai/bootstrap/messaging_config.py)使用SDK统一解析器；[事务运行时](../../../src/qs_ai/bootstrap/transaction_messaging.py)持有该不可变解析器。command admission、ACK接收及解释/评估状态计划从同一可信配置选择活动Topic，不在宿主运行路径重算哈希。新计划仅选择活动路由；旧冻结计划只在精确登记的历史映射中读取，正文、上下文、原消息ID、first-wire和witness不改写。

历史映射不增加发送、订阅、回查Topic或自动重试资格。缺映射、错误owner/正文/Topic均拒绝；Committed继续读取原结果，Unknown不授权重做模型。已中止执行若需跨路由继续，仍须宿主显式授权SDK Replan；本批真实故障及跨路由完整验收完成前，不把源码接入视为生产迁移已通过。

本次[可读命名执行总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-readable-names-execution.md)单独登记实现、CI、制品、生产切换和system验收；M2/M3/M4原计数保持。

## 显式事务模式的默认装配与回执

[TransactionCommandReceiver](../../../src/qs_ai/infrastructure/workflow_transport/transaction_receiver.py)先验证实际路由、签名和完整正文，冻结原业务ID、时间、计划及首wire；准备全部半消息后，才进入宿主原AsyncSession根事务。原业务、Inbox、首次receipt/state档案与全部witness共同提交。普通接收器的旧直接command入口明确拒绝，不能绕过事务规划继续Stage发布队列。

重复当前command读取精确SDK证据及首次业务决定，不重新Seal、不新建执行、不重复授权模型调用。只有历史Inbox而没有当前事务证明的身份被隔离，不解释成当前事务成功；Prepared/Unknown不能自动重试或重建计划。技术失败仍有原持久预算，物理重投次数不授权业务重做。

半消息、本地根提交和Broker最终确认分别记录。`LocalCommitted + FinalUnknown`保留真实业务提交；原进程退出后由相同owner的新实例回查。缺记录、数据库错误、超时、取消或lease到期本身都不是回滚证明。

## 当前状态与原业务ACK

当前[MessagingStore](../../../src/qs_ai/infrastructure/persistence/mysql/messaging.py)只承担Inbox、原事务决定及技术失败/隔离；发布队列的stage、decide、confirm/payload和扫描适配已退出。旧表的声明仍用于历史迁移及只读审计，不代表存在生产发布器。

[TransactionMessagingStore](../../../src/qs_ai/infrastructure/persistence/mysql/transaction_receipts.py)要求ACK匹配精确Committed档案，在原根写首个业务receipt；缺档案、身份冲突或证据未知均不回退旧发布记录。解释状态仍锁读并核对`interpretation_result_outbox`中的当前event/session/version/payload，STORED才更新原业务交付事实。它是当前状态历史与ACK依赖，不能按表名整表删除。

[evaluation_sequences](../../../src/qs_ai/infrastructure/workflow_transport/state_events.py)仍是当前状态序号声明；旧StateEventRecorder已删除。每笔可见状态写入安装其冻结的原事务transcription；EventSession.commit不做MQ网络I/O，也不在callback重入时生成新ID、重新加密或调用模型。

## 正文RPC与不可公开的计划

[ArchivePayloadReader](../../../src/qs_ai/infrastructure/persistence/mysql/transaction_archive.py)只读取当前精确Committed档案。RPC验证mTLS工作负载、收件方、组织、原消息ID、正文hash/长度；Prepared不公开。缺业务档案返回NotFound；已有档案但精确权威证明不可用，或数据库错误，保持Unavailable，不公开历史正文。原wire按bytes保留，不经JSON重建或重新加密。

错误路由、NSQ或错误物理Topic不能进入当前ACK处理。RocketMQ物理failure只做受保护原消息校验与隔离，不能当作业务ACK；包装中的attempts不是逻辑业务预算。原始failure仍未被签名，必须继续验证内部JOSE正文。

## T18 当前协议的解释终态恢复

模型调用和消息事务分离。已有response/checkpoint是确定性finalization的输入；MQ故障只恢复原终态提交，不增加provider invocation，不将dispatched/unknown变成可重做。原attempt、冻结配置、artifact、序号及首次wire继续保留；恢复授权与协议重试是不同责任。

旧`python -m qs_ai.maintenance.messaging_handoff`在读取参数/密钥/数据库前拒绝。只读[messaging_audit](../../../src/qs_ai/maintenance/messaging_audit.py)仍可核对保留资产与未知事实，输出不构成重发、业务重试或模型调用授权。不能通过改provider、换ID、改witness或恢复旧全量备份覆盖新事实。

## 生命周期与验证

封新准入后仍等待已接受业务，保留Checker处理未决事务；再排空Subscriber和所属SDK调用，最后关闭宿主池。`ShutdownIncomplete`保留真实在途容量并允许继续Wait，取消不提前释放线程或RPC。详见[生命周期](../../01-运行时/04-生命周期与优雅关闭.md)。

当前验收分别覆盖[原事务command](../../../tests/integration/test_transaction_command_receiver.py)、[业务ACK](../../../tests/integration/test_transaction_receipts.py)、[实际MySQL/mTLS正文](../../../tests/integration/rocketmq_payload_host.py)、[当前QS/AI成对](../../../tests/integration/rocketmq_paired_host.py)、[进程/Broker故障](../../../tests/integration/rocketmq_paired_faults.py)、[生命周期](../../../tests/test_transaction_transport_lifecycle.py)及[退役入口](../../../tests/test_mq_handoff_cli.py)。缺依赖不能算原生通过；源码CI、固定制品/镜像、生产及合法system验收由[infra总表](https://github.com/FangcunMount/infra/blob/main/docs/server/rocketmq-m4-execution.md)分别登记。

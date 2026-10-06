# 日志、指标与可诊断证据

## 本文回答

如何定位失败阶段、未知调用、积压与消息确认，同时不把测评正文、Prompt、凭证或高基数字段送到日志和指标。

## 结论

运行状态用健康与 readiness 表达，持久业务证据用数据库记录表达，诊断用结构化日志、固定聚合指标和有期限 milestone 表达。日志可以丢失，诊断可以过期，业务事实与调用回执不能靠它们替代。观测查询失败表示不可用，不能伪装为零积压。

## 分层责任

| 层 | 用途 | 限制 |
|---|---|---|
| `/healthz`、`/readyz` | 组件存活、启动和数据库可用 | 不证明模型/QS 业务完成 |
| 结构化 operation 事件 | 关联入口、阶段、耗时与错误分类 | 有界队列，可丢弃；无正文和异常文本 |
| `/metrics` | 固定名称聚合，积压与观测覆盖 | 不输出 Session/用户等动态标签 |
| `runtime_milestones` | 同业务事务记录小型阶段事实 | 到期可删除，无 heartbeat 和正文 |
| model calls / completion / Outbox / ACK | 权威调用与消息证据 | 由业务和基础设施事务维护，诊断清理不触碰 |

## 日志链与安全边界

应用 `diagnostics.context` 用 ContextVar 隔离并发上下文；`operation` 记录 started/completed/failed 和耗时，错误只保留分类、类型及本仓代码位置。`attempt_context` 建立 correlation ID，必要时通过出站 metadata 传播。

`safe_fields` 采用固定字段白名单、受限 token 和有限数字，任意对象不 stringify。`StructuredHandler` 只接受指定结构化 logger；第三方自由文本、异常对象和堆栈不会进入输出。每条事件补充 UTC timestamp、schema version、environment、release、instance 与独立 log event ID。release 来自运行环境，缺失则为 unknown。

适配器使用一个后台 writer 线程和有界队列，事件循环不做输出 I/O。警告以上保留队列余量；条目过大删去可选字段并标记 truncated；队列满/输出失败只计数，不阻断业务。只合并重复基础设施 `attempt_failed`，不合并业务状态转换。停止时限时 flush，不能保证异常退出时全部日志落盘。

## 指标语义

[`MySQLOperationalMetrics`](../../../src/qs_ai/infrastructure/persistence/mysql/metrics.py) 在有界、只读、repeatable-read consistent snapshot 中收集查询；失败只返回数据库与MQ观测不可用标志，不输出部分读数或驱动错误。查询采用超时预算，不建立新的池。

| 指标组 | 解释重点 |
|---|---|
| ready Jobs / oldest ready / expired leases | queued 到期与过期租约分开，不能据 expired 数直接宣称模型未发送 |
| unresolved model calls | 当前 unknown 或超过窗口的 dispatched；只表示需要查证 |
| pending results / awaiting receipt | 尚未获得 QS 业务确认，不能用 PUB confirmed 归零 |
| mq staged / due / held | due 含重试与等待回执候选，不是最终调度资格；held 是技术状态 |
| duplicate / payload error observations | 从观测安装起点起记录的持久事实，不补造旧历史 |
| provider samples / max seconds | 已持久响应样本，不覆盖丢失或 unknown 响应 |
| logging queued/dropped/failed/suppressed | 日志管道本身是否失真 |

消息 metrics 区分 `mq_enabled` 与 `mq_observation_available`。配置开启只说明配置，观测历史完整标志与 recording_since 才说明计数覆盖范围。quarantine count 是保留记录数，不是全生命周期错误总数。

## 诊断保留与故障排查

milestone 在原事务写入去重 key、Session/Run、invocation、attempt、UTC发生及过期时间。`prune` 每轮最多删除 1000 条过期诊断；调用、结果、审核、发布和消息证据不被清理。清理失败不触发模型重试或业务 loop 重启。

排查路径：先确认就绪与 release → 读取对应组织的运行投影 → 对照 Claim/dispatch/receipt 与未知状态 → 查原 Outbox/ACK → 用日志关联阶段与时间。恢复动作由受控业务命令承担，不能直接改日志或指标来解决状态。

## 选择、代价与限制

白名单和固定聚合减少信息泄漏及高基数成本，代价是无法从日志直接复原正文或完整驱动异常。需要进一步诊断时通过授权的业务证据读取，而不是临时打开第三方 request body 或云 tracing。

监控能暴露积压和异常，不等于 SLO 已定义或业务已验收；报警阈值需依据流量和现场证据建立。受控排障步骤由 [接口与运维](../../04-接口与运维/README.md) 维护。

## 实现与验证

- [`diagnostics.py`](../../../src/qs_ai/application/operations/diagnostics.py)、[`structured.py`](../../../src/qs_ai/infrastructure/observability/structured.py)、[`metrics.py`](../../../src/qs_ai/transport/http/metrics.py)、[`messaging_observability.py`](../../../src/qs_ai/infrastructure/persistence/mysql/messaging_observability.py)、[`runtime_milestones.py`](../../../src/qs_ai/infrastructure/persistence/mysql/runtime_milestones.py)。
- 单元：[`test_structured_logging.py`](../../../tests/test_structured_logging.py)、[`test_operational_metrics.py`](../../../tests/test_operational_metrics.py) 覆盖并发隔离、秘密不出日志、有界队列和观测失败。
- 集成：[`test_runtime.py`](../../../tests/integration/test_runtime.py)、[`test_mq_observations.py`](../../../tests/integration/test_mq_observations.py)、[`test_mq_observability.py`](../../../tests/integration/test_mq_observability.py) 覆盖只读投影、安装起点与持久计数。文档建设不产生新的生产监控结论。

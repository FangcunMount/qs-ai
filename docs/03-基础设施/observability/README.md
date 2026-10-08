# 日志、指标与可诊断证据

排障需要区分进程状态、阶段线索和业务事实。`readyz=200` 表示当前就绪条件成立，不表示某个请求已完成；日志中的 model_dispatched 是提交后的线索，不代替原调用账本；pending_results 归零要依赖 QS 业务确认，不能靠 Broker PUB 成功。

例如参与者看到结果仍在等待。先看服务与数据库是否可观测，再找其原请求/Session 和 ModelCall，确认是未生成、已生成未投递，还是 QS 已保存但页面未读到。日志丢失、诊断到期或指标查询失败都不能授权重发模型或改状态。

本文按 `766b2aa` 未变业务源码核对，日期为 2026-10-06。实际处置见 [排障与受控恢复](../../04-接口与运维/06-排障与受控恢复.md)，消息状态见 [可靠消息](../messaging/README.md)。本篇说明观测口径与证据局限，不设未经现场验证的 SLO，也不推断当前生产健康或已验收。

## 不同证据的寿命和用途不同

| 入口/记录 | 直接回答什么 | 不承担什么 |
| --- | --- | --- |
| /healthz | runtime 是否发生组件故障 | 不探测数据库、供应商或 QS 接收；启动早期可为200 |
| /readyz | 本次数据库探测与 runtime ready/healthy 是否成立 | 不验证模型key/余额、报告质量或某条消息已落库 |
| 结构化operation日志 | 安全身份、阶段、分类错误及耗时线索 | 可丢失/合并，不是事务提交证据 |
| /metrics | 当前一致数据库快照的固定聚合值及日志管道计数 | 不返回单请求身份/正文，不重建缺失历史 |
| operations_runtime_milestones | 与特定业务提交同事务的小型阶段事实 | 会按期限删除，不能替代永久调用/结果/发布证据 |
| 原Run/ModelCall/完成回执/Outbox/业务ACK | 精确执行、响应和业务接受事实 | 不能由日志/聚合值覆盖或自动清理 |

[HTTP健康实现](../../../src/qs_ai/transport/http/health.py) 的 /readyz 每次调用 [MySQLProbe](../../../src/qs_ai/infrastructure/persistence/mysql/database.py)：共享APP池连接执行 SELECT 1，最多五秒。未配置数据库为 not_configured，连接错误为 unavailable；再叠加 runtime.ready 和 healthy，成功200，否则503。启动的迁移和各组件检查由 runtime负责，SELECT 1 不重新完成全部启动预检。关闭过程的就绪窗口与组件故障见 [生命周期](../../01-运行时/04-生命周期与优雅关闭.md)。

/metrics 不进入参与者API文档，不接收一个参与者身份来缩小指标。监控访问边界由宿主网络/入口配置负责；治理诊断正文读取则使用可信管理scope，不能把这两类入口混为一条公共报告接口。

## 日志如何关联一次尝试，又排除正文

[diagnostics](../../../src/qs_ai/application/operations/diagnostics.py) 用 ContextVar 保存当前操作的字段，嵌套退出恢复原上下文；并发任务不共享可变日志字典。attempt_context 为一次后台尝试生成新的 correlation_id，出站 gRPC metadata 可带 x-correlation-id。入站 [Diagnostics interceptor](../../../src/qs_ai/transport/grpc/diagnostics.py) 只接受有限字符、最多128个字符的关联值，否则生成新UUID；它从不把关联值当成授权。

operation 记录 name.started/completed/failed。失败仍向上抛出，只输出 error_code、异常类型、仓库内最后一处代码位置及duration，不输出异常message或traceback正文。分类区分 cancelled、lease_lost、access_denied、dependency_unavailable、rule_violation、invalid_input、unexpected_error；这是一层诊断分类，不代替持久ProviderFailure的具体code。

safe_fields 是固定白名单：允许 request/command/session/run/invocation/delivery_event 等关联ID、task_kind/stage/status、provider/model、attempt、duration与有限错误字段；不允许 Testee/个人信息、报告正文、Prompt、model response、DSN、API key或任意对象。字符串只能是最多128个字符的有限token，数字必须有限，其他对象不stringify。白名单ID仍需按运维权限管理，不能因没有报告正文就视为公共信息。

[StructuredHandler](../../../src/qs_ai/infrastructure/observability/structured.py) 只接受 `qs_ai.structured` logger 的 diagnostic字典；第三方自由文本、异常对象和栈不会进入它的输出。每条增加：

```json
{
  "schema_version": 1,
  "timestamp": "2026-10-06T00:00:00+00:00",
  "service": "qs-ai",
  "environment": "production",
  "release": "example-sha",
  "instance_id": "example-instance",
  "log_event_id": "example-event",
  "level": "INFO",
  "event": "model_dispatched",
  "component": "generation",
  "session_id": "example-session",
  "run_id": "example-run",
  "invocation_id": "example-invocation"
}
```

这是合成字段示例。release 来自启动环境 `QS_AI_RELEASE_SHA`，未提供或非法则 unknown；日志不会自己证明该SHA已部署。instance_id 标识本次handler实例，log_event_id 每条生成，与业务event_id不同。生成 [events.emit](../../../src/qs_ai/infrastructure/observability/events.py) 在业务提交后输出线索；日志失败不回滚已提交事实。

## 日志管道怎样退化

handler 默认一个daemon writer线程、1024条队列，普通级别最多占896条，WARNING及以上可以使用剩余128条。事件循环只入队，不做stream输出I/O。默认单事件上限16384字节，过大时删除可选关联字段并标记truncated；不是截掉一部分秘密正文继续输出。

队列满或停止中入队计dropped；序列化/stream写失败计failed，均不阻断业务。仅 event=attempt_failed 的基础设施重复按component在30秒内合并，最多保留64个component的合并状态；下一条可带suppressed_count。不同错误若来自同component也可能合并，不能据一条日志推断这段时间只有一种故障；业务状态转换不使用这个合并规则。

shutdown 默认等writer两秒并报告是否排空。进程异常退出、阻塞输出、超时仍可能丢日志。qs_ai_logging_queued 是gauge，dropped/failed/suppressed_total 是进程counter，重启后归零，不是数据库累计事实。

## /metrics 的一致快照与缺失语义

[MySQLOperationalMetrics](../../../src/qs_ai/infrastructure/persistence/mysql/metrics.py) 每次借共享池打开新Session，设置REPEATABLE READ，启动 CONSISTENT SNAPSHOT、READ ONLY事务。整次最多五秒，各主要查询带1000毫秒执行提示；MQ查询沿用该Session。只输出完整成功快照，不缓存上一轮，不输出驱动错误和部分读数。

任一步失败，HTTP仍提供Prometheus文本和日志管道指标，但数据库值只有：

```text
qs_ai_database_up 0
qs_ai_mq_enabled 1
qs_ai_mq_observation_available 0
```

这里假定配置开启MQ。ready_jobs、pending_results和MQ积压读数均缺席，**不是0**。mq_enabled=1仅说明配置开启，mq_observation_available=1才表示本次读取成功。缺失seed类别还会分别让duplicate/payload观测不可用；不能因为Outbox读取成功就声称历史观测完整。

[render_metrics](../../../src/qs_ai/transport/http/metrics.py) 只输出固定名称，数据库项统一为gauge，无动态标签。即使adapter错误返回testee_id等未知key，也不渲染。重要口径如下：

| 指标组 | 真正取数边界 | 容易误读的地方 |
| --- | --- | --- |
| ready_jobs / oldest_ready_job_seconds | queued且available_at已到；年龄按available_at | 不含未来重试；不是从用户提交开始的总耗时 |
| expired_job_leases | leased且lease_until已到 | 租约过期不能证明供应商未执行 |
| unresolved_model_calls | 绑定Session当前active Run、Session非completed/cancelled；unknown或dispatched已超过五分钟 | 不含完整评测调用账本，不是全历史unknown总数；五分钟只是观测口径 |
| pending_results / oldest_pending_result_seconds | interpretation_result_outbox.delivered=0，年龄按created_at | 与RM staged/due不是同一层；历史NULL时间另计without_timestamp，不补造年龄 |
| provider_response_samples_5m / max_seconds_5m | execution_model_calls已有response_received且有response_json，按call.created_at进入近五分钟 | 不含丢响应/unknown，不代表所有请求时延，也不是收到响应时间的窗口 |
| capacity_rejections_24h | Session创建近24小时且failure_code=participant_daily_capacity_exceeded | 不是所有组织配额/模型容量拒绝总数 |
| database_account_connections | 当前数据库、当前MySQL账号的PROCESSLIST，含本次scrape连接 | 不是仅APP池连接数或全服务器连接数 |

[MQ快照](../../../src/qs_ai/infrastructure/persistence/mysql/messaging_observability.py) 区分 staged、awaiting_receipt、held；due只按stage/available_at，不计算顺序资格或最终gate。held_events与held_commands是技术处置，不能当作业务拒绝。组织范围读取只返回该组织的Outbox值；Inbox/quarantine没有可信组织列，不按消息body自报组织过滤后公开。

## 观测起点、保留记录与小型milestone

[messaging_observations](../../../src/qs_ai/infrastructure/persistence/mysql/messaging_observations.py) 固定八类：重复命令/ACK、payload fetch的不可用/引用错配/工作负载拒绝、payload serve的引用错配/工作负载拒绝/存储不可用。0038安装时seed计数0及recording_since，不回填之前历史，history_complete始终0；对外起点取八类中最新安装起点。数值只表示其覆盖区间内已提交观察。

重复观察借原接收事务追加，根回滚则不计；payload失败观察在失败后另开最多一秒的技术事务，失败不能掩盖原读取错误或改变重试资格。quarantine指标是当前保留的记录数量，不是全生命周期错误数，不能用它替代上述从安装起计的累计观察。

[runtime_milestones.record](../../../src/qs_ai/infrastructure/persistence/mysql/runtime_milestones.py) 保存Session/Run、kind、去重key、可选invocation/attempt、UTC occurred_at/expires_at。当前生成存储明确写execution_claimed、model_dispatched以及model_response_received/failed/unknown，同对应业务变更一并提交；不是每次heartbeat都落一条，也不是所有流程节点都有milestone。

默认保留30天，配置可设1–365天。prune每轮按expires_at最多删1000条已过期记录；有删除继续运行，无记录或清理故障后等一小时。它只删除该表，不清理调用、结果、审核、发布或MQ证据，清理失败不触发模型重试或重启业务loop。需要永久审计时回到原业务表，不能依靠延长诊断保留掩盖缺失回执。

## 单执行输出与Profile观察承担不同问题

[EvaluationDiagnostics](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_diagnostics.py) 按组织Run、expected_version和execution_id读取原dispatch/completion。列表按execution_id分页，默认20、最多50，查询正文length而不取raw/normalized正文；原创建、slot、执行索引与完成身份不一致则报冲突。单项读取才取得正文，重新核证原生成/语义候选绑定并返回两段原bytes及SHA-256。读取不调模型、不生成缺失completion、不替审核人判通过；版本变化应刷新原状态，不拼接不同版本分页当作一份最终清单。当前读取器对尚无completion的dispatch只核对Run全局checkpoint，不查candidate slot claim；candidate_v2仍活动且未提交completion时可报 Missing terminal execution evidence 的版本冲突。应先核对Get的活动执行和取消投影，再待claims恢复排空后读原执行，不能把冲突当作未调用。

[observe-cutover](../../../src/qs_ai/bootstrap/observe_cutover.py) 则按精确Profile fingerprint和不超过31天的带时区窗口做只读聚合。群体按Session创建时间 `[since, until)` 选取，但结局取observed_at当前值，**不是回放那个历史时刻**；Profile来自每个Session绑定的原Publication，不用今天的active指针。backlog覆盖该Profile全部绑定Session，包括窗口外旧请求。

其响应样本只输出状态与数字，每查询最多100000个样本，分位数使用nearest_rank，missing/invalid单列；无样本的总量/分位数为None，不补0。结果固定 `acceptance_passed=false`。CLI自身有只读观察进程的1连接池并最终关闭，不是运行服务又建一个第二APP池；观察报告也不能代表真实模型质量或业务验收通过。

## 验证入口

| 测试 | 核验范围 |
| --- | --- |
| [test_structured_logging.py](../../../tests/test_structured_logging.py) | 并发ContextVar、正文/秘密/异常不出sink、不stringify对象、有界队列、阻塞输出与停机、重复合并及固定指标 |
| [test_operational_metrics.py](../../../tests/test_operational_metrics.py) | 无数据库/查询失败不伪装空积压、不输出未认识的字段/秘密；MQ开关与可观测性分开 |
| [test_cutover_observation.py](../../../tests/test_cutover_observation.py) | missing不当0、nearest_rank、非法Profile/时间窗口不打开数据库 |
| [test_grpc_diagnostics.py](../../../tests/test_grpc_diagnostics.py) | 测试RPC成功/拒绝状态与关联字段，正文不进入日志；不证明人类访问授权 |
| [integration/test_runtime.py](../../../tests/integration/test_runtime.py)、[test_mq_observations.py](../../../tests/integration/test_mq_observations.py)、[test_mq_observability.py](../../../tests/integration/test_mq_observability.py) | 真实隔离MySQL事务、观测安装起点、原root回滚、组织边界和无副作用读取 |
| [integration/test_evaluation_diagnostics.py](../../../tests/integration/test_evaluation_diagnostics.py) | 列表不取正文、精确回执读取、指纹/版本/组织错误拒绝、不调供应商 |

测试保护口径和隐私边界；现网SLO、积压原因、实际已部署SHA及业务完成需要当时的运行证据。健康、日志、指标、原持久事实与人工验收应分别记录结论。

# 运行指标与初始告警门槛

qs-ai 的内部 HTTP 进程提供 `/metrics`，由 infra-network 内的 Prometheus 抓取 `qs-ai-api:8000`。宿主机仍只绑定 `127.0.0.1:18080`，不增加公网域名或业务入口。该端点只输出固定名称的汇总数值，不输出用户、受试者、请求 ID、Prompt、测评正文、凭据或动态标签。

每次采集在只读一致性事务内完成，整体超时五秒，单条 SQL 最长一秒。任一查询失败时仅返回 `qs_ai_database_up 0`，不把未采集到的积压伪装成零；HTTP 抓取失败由 Prometheus 的 `up` 判断。

| 指标 | 含义 | 初始处置门槛 |
| --- | --- | --- |
| `qs_ai_database_up` / `up{job="qs-ai"}` | 完整数据库采集 / HTTP 进程可达 | 连续 2 分钟失败 |
| `qs_ai_oldest_ready_job_seconds` | 已到执行时间的最老排队任务 | 超过 120 秒持续 2 分钟 |
| `qs_ai_expired_job_leases` | 过期但尚未恢复的执行租约 | 大于零持续 2 分钟 |
| `qs_ai_oldest_pending_result_seconds` | 未获 QS 确认的最老结果事件 | 超过 120 秒持续 2 分钟 |
| `qs_ai_pending_results_without_timestamp` | 无法计算投递年龄的异常记录 | 大于零持续 2 分钟 |
| `qs_ai_unresolved_model_calls` | 当前 Run 的未知调用或发出后超过 5 分钟未收据的调用 | 大于零持续 2 分钟；人工核对，禁止盲目重发 |
| `qs_ai_provider_response_max_seconds_5m` | 最近 5 分钟创建的、已记录响应的调用完整耗时最大值 | 有样本且超过 120 秒持续 2 分钟 |
| `qs_ai_capacity_rejections_24h` | 最近 24 小时创建的参与者日容量拒绝 | 大于零持续 2 分钟；检查容量策略，不能自动放宽 |
| `qs_ai_database_account_connections` | 同账号同数据库连接数，含观察连接 | 供容量盘点；不把跨进程总数当作单连接池占用率 |

这些门槛用于发现卡住、回传故障和容量拒绝，是首版运维阈值，不是已由大量样本证明的性能 SLA。正常生成会消耗模型时间，排队年龄只从任务的 `available_at` 到期时计算。已完成、取消或被新 Run 替代的旧未知调用仍保留审计数据，但不会持续触发当前执行告警。模型响应耗时为完整响应，不称为首 token 延迟；零样本必须结合样本数读取。

`/metrics` 覆盖生产生成和结果回传，评测进程、gRPC、执行与投递进程还保留现有 Docker 健康检查。Prometheus 规则和触发/恢复测试维护在 infra；端点测试通过不代表已完成生产抓取、告警加载或通知验证。未配置通知通道时只能声明规则状态可见，不能声明已通知值班人员。

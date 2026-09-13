# 当前 v6 切流观测

当前交付是只读观测入口和投递时间记录，尚未完成生产切流验收。它服务于 M4.4 的数值门槛、M4.5 的恢复检查及 24 小时观察，不替代真实管理、授权、模型输出与客户端验证。观察命令不修改发布指针、不创建测评、不调用模型。

## 已核实的基线

[2026-09-13 只读统计](evidence/2026-09-13-v6-runtime-baseline.json) 精确匹配当前已发布 v6 指纹及生成路线 v8。历史窗口为 2026-09-05 至 09-08，共 7 个 Run，6 成功、1 失败。6 次成功执行的 Run 耗时为 13.030–22.362 秒，nearest-rank P50 为 14.376 秒、P95 为 22.362 秒。样本小且场景组成未经逐条验收，不能把这组比例或 P95 当成生产 SLA。

旧 `receipt.latency_nanos` 在 HTTP 收到响应头时取值，未包括响应体读取；该组记录约 116–201 毫秒，不可拿来对比完整模型生成。qs-ai 的 ResponsesGateway 在响应体完全读取后记录 latency_milliseconds，观测入口使用这个字段。旧记录仍保留，不修写它们来制造统一口径。

## 观测入口

迁移 0027 为 result_outbox 增加 nullable 的 created_at / delivered_at。新事件和首次确认使用数据库 UTC；相同事件重放、投递重试、重复确认均保留原始时间。迁移不回填旧记录，缺失时间显示为缺失样本。

在已包含 0027 的镜像中运行，时间必须带 UTC 偏移，起止窗口不超过 31 天，结束时间不得晚于数据库当前时间：

```bash
python -m qs_ai.bootstrap.observe_cutover \
  --profile-fingerprint sha256:cc747df0a6ae4b02b65b7447ce08845fa8c4e91b9d674f51ac25fbcbffd2a2a2 \
  --since 2026-09-13T00:00:00Z \
  --until 2026-09-13T01:00:00Z
```

示例时间只说明格式，应改为实际验收窗口。命令复用服务的数据库配置；无需把数据库地址、密码或模型密钥写到命令行。使用只读一致性快照和参数绑定，单项结果最多 100000 条，数据查询设置 10 秒执行限制。输出只包含数量、状态和数值，不含操作者、受测者、报告、Prompt 或模型正文。超限或查询失败返回失败，不截取部分数据后宣称通过。

## 指标口径

Profile 按每个 Session 当时绑定的不可变发布记录选取。切换或禁用当前发布指针不会使旧请求从统计中消失。主体请求按 Session 创建时间的左闭右开窗口筛选，状态和结果取 observed_at 时刻；这不是历史时点回放。当前积压跨越接单窗口，以免漏掉更早的未完成任务。

| 输出 | 含义 |
| --- | --- |
| request_status_counts | 接单窗口内各状态请求数；单列未完成、失败、阻断和取消，不把当前完成比例直接称作可用率 |
| model_call_status_counts | 该请求集合的全部调用状态；明确暴露未知结果及没有成功回执的调用 |
| provider_full_response_ms | 已持久化模型回执的完整响应耗时 |
| input_tokens / output_tokens | 回执中已知 Token 用量；known_total 是已知部分之和，missing 表示未获知的用量，不计算货币费用 |
| request_to_artifact_ms | 从 Session 创建至成果落库，包含排队和该请求恢复等待，不等于单次 Provider 耗时 |
| completion_delivery_ms | completed 事件首次落库至 qs-ai 持久化确认；包括退避和重试，回执丢失时可能晚于 QS 实际收到成果 |
| request_to_acknowledged_completion_ms | 从接单至 completed 事件确认；不是浏览器或微信渲染完成时间 |
| ready_queue_overdue_ms | 当前可执行 queued Job 超过 available_at 的等待时间；延期任务单列，不能称作原始总排队时间 |
| expired_leases | 当前 leased Job 租约已过期数量，供恢复检查 |
| pending_result_age_ms | 所选 Profile 全部尚未确认事件的存续时间；旧时间缺失计入 missing |
| database_account_connections_including_observer | 当前数据库账号、当前数据库的可见连接数，包含观测连接；不是服务器全部连接容量 |

P50/P95 使用 nearest-rank，同时报告样本数 n、missing、invalid；无有效样本返回 null，不返回零。known_total 只合计有效已知值。输出固定 acceptance_passed=false，脚本不具有自动批准上线的职责。

## 切流前后使用

切流前固定唯一 v6 和实际验收窗口，核对旧在途任务、当天预算以及新生成/投递进程的配置。观察应同时记录实际请求成功/失败原因、事实与权限检查、上述数值及客户端展示。当前只有少量旧 Run 的执行耗时；新队列、投递和完整请求耗时的基线及放行阈值仍须由授权验收请求建立，不能用无流量查询补齐。

原定 24 小时、实际合法请求及正常/边界/缺失事实场景要求继续保留。重大事实错误、越权、任务丢失、终态覆盖或未知调用被盲目重发时停止新准入。发布暂停后继续保留任务和资产，实际发布版本回退与业务配置回退分别记录。

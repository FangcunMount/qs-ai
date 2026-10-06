# P1 第一批实现与验证

日期：2026-09-11。完成会话/执行骨架与本地协议传输验证，**尚未完成 P1 的真实 qs-server 授权和事实 RPC 联调**。本地代码未提交，未上传或部署。

## 已实现

- Session 聚合负责状态/版本/当前问题；EvidenceSet 校验主体、完整性和来源身份。
- 五个 HTTP 接口：创建、查询单会话、开始、回答/跳过、取消。认证与组织不取自请求 body；默认集成未配置时拒绝访问。
- MySQL UoW：会话、Run、Job、回答和幂等回执按命令原子提交。相同键同内容返回原回执，不同内容 409；即使回放也重新授权。
- MySQL Job Store：数据库时间、固定锁顺序、领取/续租/接管/取消、旧 fence 和旧版本拒绝、最多 3 次崩溃尝试。
- ExecuteNext：授权→读取/复用冻结证据→工作流→再次授权→提交；心跳失效取消工作，取消/撤权后不公开迟到问题。
- Worker `--once` 执行至多一个任务。默认只作数据库探测，无常驻守护进程。
- qs-server 现有 proto 固定提交与哈希、生成 Python stub；双向 TLS 传输探针完成本地验证。

P1 当前表：interpretation_sessions、clarifications、evidence_sets、interpretation_runs、execution_jobs、idempotency_requests。迁移 head 为 0003_interpretation。证据条目暂用有界 JSON 信封，不提前创建逐条索引；真实事实契约未完成。

## 验证结果

| 层 | 证据 |
| --- | --- |
| pytest | 40 项通过：20 项 MySQL 集成、5 项本地双向 TLS gRPC，其他为领域/架构/DI/HTTP 拒绝访问测试 |
| 原子提交 | 模拟插入 Job 后异常，Session/Run/Job/幂等回执一起回滚；原键可重新成功提交 |
| 并发 | 相同幂等键创建只产生一会话；两个不同键回答只接受一次；两个 Worker 只有一个取得执行权 |
| 恢复 | 强制终止发生在 checkpoint 已保存而问题尚未发布时；新进程接管并发布可恢复的问题 |
| 身份边界 | 同 Testee 的不同 owner/组织不能互读；撤权阻断查询、幂等回放、执行和发布 |
| 资源 | 心跳超过原 TTL 后仍保持执行权；丢失租约取消工作；旧 Worker 无法续租/发布 |
| MySQL | Docker MySQL 8.4；Alembic 升至 head，alembic check 无模型/迁移差异 |
| gRPC | 临时测试证书双向 TLS；验证 uint64、metadata、超时、权限拒绝、缺失/错误报告身份 |
| 静态与契约 | Ruff/格式/mypy 通过；proto 来源哈希及生成代码一致性通过；生成代码独立于手写代码检查 |
| 构建 | uv sync --locked、源码包与 wheel 构建通过；从 wheel 导入应用/生成协议成功且不包含测试替身；Worker --once 空队列正确退出 |
| Docker | 本轮再次构建仍在 Docker Hub 授权请求处 TLS handshake timeout；未验证镜像 |
| 发布 | 无远端 CI、云 MySQL、线上授权、模型或生产验收证据 |

测试事实和 OfflineWorkflow 均位于 tests/probes，不进入生产 DI。恢复后的最终状态为 blocked/model_not_connected，**没有制造已完成的 AI 成果**。原始回答、回答者和跳过标记已持久化；消息列表、成果和受控重试接口仍待后续建设。

## P1 尚未完成的接入条件

[qs-server 接入记录](../integrations/qs_server/README.md) 给出源码证据。当前委托链只信任 collection-server，显示 RPC 也缺少完整的不可变事实身份。新服务不能自行签发兼容凭据来绕过该边界。

下一批需落实：qs-ai 入站认证、Worker 主体授权复核、新事实 RPC、独立服务身份/ACL，以及真实 Python/Go 撤权和历史报告换版联调。在此之前保留拒绝访问的默认绑定，不启用真实模型，不宣称 P1 全部完成。

CI 已加入 proto 漂移与 Alembic 对齐检查，但未运行远端 CI。源码测试结果不能替代容器、云兼容或上线证据。Starlette/AnyIO 的已知弃用提示仍存在。

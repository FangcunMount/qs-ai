# Dishka 作用域与资源所有权

## 本文回答

共享容器如何避免跨请求污染，数据库池、Session、模型客户端和消息资源由谁创建、提交和关闭。

## 结论

APP 作用域承载进程资源和共享策略；REQUEST 作用域承载一次接口调用或一次后台 attempt 的用例及适配器。共享数据库池不等于共享数据库 Session。业务事务由发起当前操作的宿主拥有，借用事务的适配器不能隐式 begin、commit、rollback 或 close。

| 对象 | 作用域 / 所有者 | 结束方式 |
|---|---|---|
| `Settings`、`Database`、`ModelCapacity`、容量基线 | Dishka APP | 容器关闭时销毁；Database dispose engine |
| 评测恢复游标 | EvaluationProvider APP | 随容器结束，仅用于扫描公平性 |
| `Transactions`、存储适配器、用例 | Dishka REQUEST | 当前操作作用域结束 |
| SQLAlchemy `EventSession` | 每次 `Transactions.open` | 调用方显式 commit，退出兜底 rollback/close |
| 生成/评测 `httpx.AsyncClient` | 对应 REQUEST 生成器 | attempt 结束时关闭 |
| NSQ Publisher/Subscriber、载荷 mTLS 通道 | `MessagingRuntime` | 监督器排空后，由 runtime 关闭 |
| `BorrowedTransactions` | 原根事务的借用视图 | 不提交、不回滚、不关闭原 Session |

## 依赖装配与隔离

[`create_container`](../../src/qs_ai/bootstrap/container.py) 汇总 Runtime、Persistence、Operations、Interpretation、Generation、Integration、Asset Provider，并启用 Dishka `STRICT_VALIDATION`。短生命周期依赖不能被 APP 单例长期持有；缺失工厂在装配时暴露。

HTTP 使用 Dishka 的请求集成；gRPC 和后台执行以显式操作作用域解析用例。生成 loop 的一次 `generate()`、评测 loop 的一次 `evaluate()` 都使用 `async with container()`，并发 attempt 各自拥有 REQUEST 对象，不把前一次 Session 或上下文留给下一次。

`create_app` 支持独立创建容器和借用统一服务容器两种方式：独立创建时 HTTP lifespan 关闭它；统一服务传入容器时 HTTP lifespan 不关闭，由 `serve` 统一收尾。消息 runtime 借用 `Database`；关闭自身时移除 state-event recorder，关闭自有连接，但不 dispose 应用池。

## 事务所有权与不变量

`Transactions.open` 创建 fresh Session。调用方必须显式接受操作，退出时回滚尚未提交内容，包括异常和取消。`EventSession.commit` 在根提交前把待记录的评测状态事件写入同一事务；失败时整个业务提交失败，不能先提交业务再补消息。

MQ 接单由 receiver 创建根事务，再通过 `Transactions.borrowed(db)` 调用既有业务存储。借用对象记录原活动根事务，进入和退出时检查它仍然是同一事务；其 `commit` 只验证，并不完成事务。接收器最后一次提交业务、Inbox 和首回执 Outbox，之后才能让订阅处理完成。

这个接口避免“业务用例单独成功、Inbox 或首回执失败”的双写窗口。它也要求适配器遵守原事务与锁顺序，不能因为SQLAlchemy读取已经 autobegin，就另外打开或替换事务。

## 失败、代价与替代方案

容器关闭、REQUEST 生成器结束和 Session 回滚各自承担不同层级的清理；不能用关闭池代替完成事务。关闭顺序见 [生命周期](04-生命周期与优雅关闭.md)。

REQUEST 客户端使取消后的释放边界明确，代价是不能跨 attempt 复用其连接。把客户端提升到 APP 作用域可以作为性能演进，但必须证明并发安全、配置版本解析和关闭依赖，而不能只为减少对象创建改变所有权。

共享 APP 容器减少重复 engine；独立服务进程需要各自池预算。借用事务把原子性责任集中到宿主，代价是适配器实现必须显式区分自有与借用事务，不能假设所有 `commit()` 都真正提交。

## 实现与验证

- 作用域：[`providers/core.py`](../../src/qs_ai/bootstrap/providers/core.py)、[`providers/generation.py`](../../src/qs_ai/bootstrap/providers/generation.py)、[`providers/evaluation.py`](../../src/qs_ai/bootstrap/providers/evaluation.py)。
- 所有权：[`api.py`](../../src/qs_ai/bootstrap/api.py)、[`database.py`](../../src/qs_ai/infrastructure/persistence/mysql/database.py)、[`mq_receiver.py`](../../src/qs_ai/infrastructure/workflow_transport/mq_receiver.py)。
- 验证：[`test_container.py`](../../tests/test_container.py) 检查跨操作隔离与一次资源关闭；[`test_shared_runtime_pool.py`](../../tests/integration/test_shared_runtime_pool.py) 检查多职责共池而不共事务；[`test_mq_admission.py`](../../tests/integration/test_mq_admission.py) 检查业务、Inbox、状态与回执失败时共同回滚。

集成验证需要可销毁 MySQL。文档核对不替代数据库上的真实事务验证。

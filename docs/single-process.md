# 单进程服务

## 启动和职责

`python -m qs_ai.bootstrap.server` 是唯一生产常驻入口；不使用子进程或多 Uvicorn worker。HTTP 和 gRPC 与生成、评测、投递在同一 asyncio 循环运行。Dishka 容器及 Database 为应用级，每次请求/任务尝试各有作用域和事务。默认池 5+5，各后台并发默认1。

配置沿用 configs 和环境变量。QS_AI_EXECUTION_ENABLED、QS_AI_EVALUATION_ENABLED、QS_AI_GOVERNANCE_ENABLED 是 Actions 变量，分别映射生成、评测和治理开关，不选择容器。QS_AI_QS_ADDRESS 始终必需，结果投递不随生成关闭。启用生成或评测才要求模型凭据。生产评测排空190秒。

启动依次校验 TLS、共享连接池上的迁移 head、启用组件依赖；绑定监听并启动循环后才就绪。HTTP `/healthz` 检查运行器，`/readyz` 还检查数据库和必需组件。禁用组件不算故障；没有任务不是故障。已删除文件心跳。

SIGTERM/SIGINT 只由统一运行器管理。先取消就绪、停止 gRPC 准入与任务领取，在途任务继续续租、提交，期限后取消；HTTP 最后退出，依赖最后关闭。关键组件意外退出导致非零进程退出；单次业务异常仍按原退避规则。日志仅输出安全事件及分类，不包含密钥或完整任务正文。

## 部署与回滚

一个常驻容器 qs-ai，网络别名 qs-ai-grpc 和 qs-ai-api 保留。HTTP 本机18080、mTLS证书及gRPC端口不变。资源上限2 CPU/1GiB，停止宽限210秒，Docker日志轮转不变。

新发布目录独立保存配置。部署先预检和迁移，维护窗口内停止旧grpc/api，再排空后台进程；确认旧实例全停后启动新容器。部署核对镜像、就绪及mTLS。失败且schema兼容时先停新实例，再恢复旧目录；回滚可读取旧api/grpc五服务定义。不得覆盖旧runtime.json，不删除数据库、卷或备份。

## 验收状态

本次源代码实现不等同生产验收。需记录：完整CI双MySQL与互操作结果、隔离切换/回滚、混合负载连接等待、生产镜像和单进程、受控生成/评测/投递以及停止恢复证据。没有这些证据不得声称发布完成。

上线前隔离验证（2026-09-20）：旧镜像基于 `1cc7df4`，执行 `scripts/ci/rehearse_single_cutover.py OLD_IMAGE NEW_IMAGE`，真实完成五服务→单服务→五服务→单服务，最终一个 Python PID。另以真实 MySQL 8.4 运行100个混合请求作用域，共享默认5+5池，事务实例互不复用，连接归还及失效连接后恢复通过。此负载验证不包含真实模型吞吐，不据此宣称资源节省。完整业务、快照及恢复回归仍由双版本 MySQL 和 Go 互操作 CI 覆盖。

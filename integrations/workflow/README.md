# QS / AI 持久命令与结果契约

[workflow.proto](proto/workflow.proto) 由 qs-ai 持有；QS 复制到 `api/grpc/proto/aiworkflow/workflow.proto`。变更必须同步两端，兼容既有字段号。Python 生成及漂移检查：

```sh
uv run python scripts/generate_workflow_proto.py
uv run python scripts/generate_workflow_proto.py --check
```

QS 使用自身 `scripts/proto/generate.sh` 生成 Go 代码。当前版本是状态快照契约，尚无正式 Artifact 字段。不能把 `blocked/model_not_connected` 视为生成成功。

## 协议语义

- Start 的 request_id、Change 的 command_id 是稳定 UUID，重试不能重新生成。相同 ID 不同输入拒绝。
- Actor 只接受可信 QS 服务委托；服务证书不代替资源授权。真实 EvidenceSource 未配置时拒绝请求。
- AI 在同一事务保存外部请求关联、会话、任务和状态 outbox，提交后回复 Receipt。
- QS 在同一事务保存请求及 command outbox。网络调用发生在事务外，未确认可重投。
- StateEvent 的 event_id 和 session/version 不可变；QS 校验请求、主体、会话关联，并原子保存接收凭证及投影。只接受较新版本更新投影，旧事件仍可确认。
- 回调可先于 Start 确认到达；迟到的 Receipt 不覆盖结果投影。
- 双向 TLS 校验 CA、服务器名称和调用方工作负载 CN：QS 为 `qs-apiserver.svc`，AI 为 `qs-ai.svc`。

## 运行入口

先分别执行 AI Alembic head 和 QS 的 `000072_ai_bridge_delivery.up.sql`。两服务使用独立数据库；不在服务启动时自动建表。证书路径由部署环境提供。

```sh
uv run python -m qs_ai.bootstrap.integration serve --address localhost:50061 --ca "$CA_FILE" --cert "$AI_CERT_FILE" --key "$AI_KEY_FILE"
uv run python -m qs_ai.bootstrap.worker --once
uv run python -m qs_ai.bootstrap.integration deliver --address localhost:50062 --ca "$CA_FILE" --cert "$AI_CERT_FILE" --key "$AI_KEY_FILE"
```

`serve` 常驻；Worker 和 `deliver` 是单次有界运行，需外部调度反复执行。默认授权源和业务工作流仍不可用；合成实现仅在测试中注入。QS 对应入口为 `cmd/qs-ai-bridge`，尚未接入现有用户路由。

投递失败持久保留并指数退避，最大间隔 60 秒；当前没有死信队列、告警或运维重放入口。上线前需补齐这些运行能力。

## 跨语言回归

准备隔离的 `qs_ai` 与 `qs_gateway` 测试库，应用两侧迁移，在 QS 仓库构建 `go build -o /tmp/qs-ai-bridge ./cmd/qs-ai-bridge`。测试会自动签发临时证书、启动 Go/Python 服务及清理本次记录。

```sh
export QS_AI_TEST_MYSQL_DSN='mysql://qs_ai:qs_ai_local@127.0.0.1:13316/qs_ai'
export QS_AI_TEST_QS_MYSQL_DSN='mysql+asyncmy://qs_ai:qs_ai_local@127.0.0.1:13316/qs_gateway'
export QS_AI_TEST_QS_GO_DSN='qs_ai:qs_ai_local@tcp(127.0.0.1:13316)/qs_gateway?parseTime=true'
export QS_AI_BRIDGE_BIN=/tmp/qs-ai-bridge
uv run pytest -q
```

这些是本地开发凭据。缺少跨语言环境时 interop 测试跳过；单仓 CI 不代表跨仓联调已执行。

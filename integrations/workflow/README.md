# QS / AI 持久命令与结果契约

[workflow.proto](proto/workflow.proto) 由 qs-ai 持有；QS 复制到 `api/grpc/proto/aiworkflow/workflow.proto`。变更必须同步两端，兼容既有字段号。Python 生成及漂移检查：

```sh
uv run python scripts/generate_workflow_proto.py
uv run python scripts/generate_workflow_proto.py --check
```

QS 使用自身 `scripts/proto/generate.sh` 生成 Go 代码。当前 StateEvent 同时支持状态快照与完成态的 `artifact_json` 不可变成果；QS 接收端验证原请求、主体、版本及成果来源。不能把 `blocked/model_not_connected` 视为生成成功。

## 协议语义

- Start 的 request_id、Change 的 command_id 是稳定 UUID，重试不能重新生成。相同 ID 不同输入拒绝。
- Actor 只接受可信 QS 服务委托；服务证书不代替资源授权。带 evidence 的可信 QS 请求按提交时授权，快照与任务原子保存；不带快照的动态路径仍要求 EvidenceSource。
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

`serve` 常驻；Worker 与 `deliver` 支持显式持续运行配置及心跳，执行开关默认关闭。真实事实、授权、生成与回传仍须按 M1/M2 做生产验收；可信 QS 快照任务可被接收并保存。QS 新入口为 POST /api/v1/assessments/{id}/ai-workflows，独立开关默认关闭；cmd/qs-ai-bridge 负责持久命令投递。

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

这些是本地开发凭据。缺少跨语言环境时 interop 测试跳过；当前 CI 固定检出 QS 源版本并执行已有命令、成果及评测管理跨语言回归。新增发布管理的 Go 入口联调仍需后续接入，不能由 Python mTLS 测试替代。


## 配置发布管理

`PublicationManagement` 提供 `Publish`、`Rollback`、`Disable`、`Get`、`GetReceipt` 五个内部 RPC。与评测管理一样，只在 `grpc.governance_enabled=true` 时注册，并强制校验 mTLS 的 `qs-apiserver.svc` 工作负载；QS 必须先完成当前操作者的管理授权，再填入可信机构与操作者。qs-ai 不接收直接来自浏览器的用户声明，不自建用户或权限目录。

写操作必须有规范 UUID command_id、显式 expected 指针、原因和确认。发布绑定 Run UUID/版本/完整 release 摘要；回退只传原 publication UUID，不能上传拼装的审批或配置。AI 使用服务端时间，从冻结证据和全部 G1–G5 重算是否可发布。共享 selector 目录保持 QS 语义，不按机构复制一套生效配置。

`Get` 返回精确 selector 的当前版本、active_publication_id、原发布 JSON 和变更时间；没有发布时为 version=0，停用后的版本继续递增。JSON 是 `qs-ai-publication/v1`，包含原 Profile、五项生成资产清单、完整评测引用及批准/发布审计，供追溯使用。单个 State 上限 512 KiB，前后状态 Receipt 上限 1 MiB。

响应丢失后可用原机构/操作者和 command_id 调用 `GetReceipt`，不会新增发布或执行模型；其他机构/操作者与不存在命令统一返回 NOT_FOUND。相同原命令也可显式重放以取得原回执，服务器不自动重试写操作。版本/命令冲突映射 ABORTED，输入或证据不合格映射 INVALID_ARGUMENT，依赖异常映射 UNAVAILABLE 并提示查询原命令；错误响应不包含内部异常或资产正文。

这批只接通 AI 端管理入口。QS 管理客户端/REST 代理、实际用户权限联调、运行时按 publication 解析及在途版本冻结仍待完成；当前生产治理开关为 false，不会自动替换 QS 的旧管理或生成路径。

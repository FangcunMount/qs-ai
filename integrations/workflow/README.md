# QS / AI 机器契约

本目录持有 qs-ai 的工作流与消息协议；源码基线 `c977cb9`。协议存在不代表某入口可接单，当前接入语义见 [接口地图](../../docs/04-接口与运维/01-接口地图与契约所有权.md)。

## 所有权与生成

[workflow.proto](proto/workflow.proto)与[messaging.proto](proto/messaging.proto)由 qs-ai 持有，QS 同步到其 `api/grpc/proto/aiworkflow/`。变更必须同步两端、兼容既有字段号与可选字段存在性，不手改生成代码。

```sh
uv run python scripts/generate_workflow_proto.py
uv run python scripts/generate_workflow_proto.py --check
```

QS 按自身生成脚本生成 Go 代码；固定 QS 检出及跨语言测试由 [CI](../../.github/workflows/ci.yml) 管理。测试使用隔离存储、临时证书和合成输入，skip 不计通过，也不代替生产授权/模型/客户端验收。

## 当前通道

执行写命令 Start、Change、ParticipantRetry、EvaluationStart、EvaluationCancel 走 MQ，原 Protobuf 业务正文保留在 MessagingBody 中。Commands.Start/Change、ParticipantManagement.Retry、EvaluationManagement.Start/Cancel 的旧 gRPC 方法会先核对工作负载，再明确返回未接单拒绝。

mTLS gRPC 仍承载 CheckEligibility、治理/配置读写、运行诊断以及 MessagePayloads 正文读取。治理服务受 grpc.governance_enabled 控制，服务证书只识别工作负载；QS 每次校验管理/业务权限后才能传入机构与操作者。

统一常驻入口是 `python -m qs_ai.bootstrap.server`，必需 MQ、TLS、QS endpoint 与 exact Alembic heads。`bootstrap.integration` 已退役，不再 serve/deliver 或扫描历史结果；worker/evaluation 默认探测、`--once` 为单步维护，不能作为常驻替代。

## 稳定身份与业务确认

Start request_id、Change/Retry command_id 为稳定 UUID；同身份同正文取回原回执，不同正文冲突。AI 接单在宿主根事务内保存 Inbox、原业务变更、状态事件和首回执后才允许 FIN；接单成功不等于模型完成。

StateEvent.event_id、session/version、原业务正文不可替换；QS 核对原请求、主体、来源和较新版本，Inbox、业务投影与最终 ACK 原子提交。迟到事件不能覆盖较新投影，迟到接单回执不能覆盖完成状态。AI 验证最终 ACK 的种类、身份及原字节摘要后才确认事件；mq_owned、Broker PUB 和消费 FIN 都不证明 QS 业务接收。

大型正文使用精确引用及 mTLS MessagePayloads，返回原 Protobuf 字节并核对长度与 SHA-256，不重序列化 JSON 算摘要，不读取 latest。完整消息路由、JOSE、归属和恢复说明见 [QS 接入与消息契约](../../docs/04-接口与运维/02-QS接入与消息契约.md)。

## 治理兼容要点

PublicationManagement 的 Publish/Rollback/Disable 使用原 command_id、显式预期指针、原因与确认，重建冻结 Run 的完整质量门槛；Rollback 引用已有 publication，不能上传拼装审批。GetReceipt 只供原机构/操作者恢复命令结果；ListHistory/GetHistory 是有当前审计授权的历史读入口，不放宽回执归属或赋予写权限。

配置 selector 保留共享语义；机构 Run、方案、草稿与命令回执保持各自范围。历史响应缺少新增字段时需保留 absent，不能补成自动通过。can_reopen_review 是原 Run/版本的业务资格，不能当作用户权限或未来版本预留。

所有写超时先按原命令回读，不能自动发新命令。治理保存/冻结/准备、完整评测、实际审核、最终批准、配置发布和参与者完成分别记录证据。具体语义见 [治理接口与可信委托](../../docs/04-接口与运维/03-治理接口与可信委托.md)。

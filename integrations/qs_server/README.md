# qs-server gRPC 接入边界

当前已固定现有报告协议并生成 Python stub，完成本地双向 TLS 传输测试；尚未接入真实 qs-server。

## 协议来源

- 源提交：17344553f6d134dd5f00145ddf635ececb914af6。
- 源文件：api/grpc/proto/interpretation/interpretation.proto 及 evaluation/evaluation.proto。
- [版本与哈希清单](manifest.json)；`uv run python scripts/generate_qs_proto.py --check` 检查哈希和生成代码漂移。
- 修改协议必须先在 qs-server 定义，不手改 vendored proto 或生成文件。生成器仅调整 Python 包导入/消息模块命名空间。

`ParticipantReportProbe` 调用既有 GetAssessmentReport：双向 TLS、deadline、禁用隐式重试、2 MiB 响应上限；短期委托由调用方即时传入，不在客户端签发或持久化。

## 已核实的授权链

在上述提交中：collection-server 的 ParticipantReportClient 从经过 ProfileLink 校验的上下文构造委托，附加 `x-qs-delegated-subject`。qs-server 的 participant_report_guard 验证目的和 Testee 绑定；当前委托 verifier 的可信 workload 只有 qs-collection-server，服务身份清单没有 qs-ai。应用层随后检查 Testee 存在以及 Assessment 属于该 Testee。

核对路径：

- internal/collection-server/infra/grpcclient/evaluation_client.go
- internal/pkg/delegatedsubject/{subject,config,metadata}.go
- internal/pkg/serviceidentity/identity.go
- internal/apiserver/transport/grpc/service/participant_report_guard.go
- internal/apiserver/container/module_init.go

这不提供“qs-ai 只凭已保存 subject_id 就能刷新委托”的现成能力。不能复用 qs-worker/collection-server 证书冒充身份，也不能把短期 token 放入 Job 充当长期授权。

## 事实缺口

现有 AssessmentReport 显示 DTO 没有 report_id、outcome_id、source_version；reportprojection.Report 与 ReportRow 同样未暴露这些完整字段。因此传输探针没有实现 EvidenceSource，也不会把缺字段结果补成假版本。源端需新增有授权的事实快照服务，并区分当前报告与精确历史报告读取。

以下为早期接入顺序，已由 [迁移责任边界](../../docs/migration-boundary.md) 调整：先实现 QS→AI 发起和 AI→QS 回传，再接事实/授权复核；不新增 QS 代理用户认证 RPC。

早期接入顺序（保留背景）：

1. 明确 qs-ai 入站认证以及 Worker 持久主体授权复核端口，复用 IAM/业务关系权威；拒绝信任 body 里的主体信息。
2. 在 qs-server 新增事实 RPC，绑定组织/操作者/Testee、逐项报告权限与不可变来源身份；一项不可访问则整批拒绝。
3. 为 qs-ai 配置独立服务身份、证书和精确 ACL，完成 Python/Go 契约与撤权测试。
4. 将当前 UnconfiguredIdentity/UnconfiguredEvidenceSource 替换为真实实现，验证用户退出后恢复、撤权、历史报告换版。

本地 TLS 测试只证明 Python gRPC 传输、metadata、deadline 与错误映射，不证明 Go 服务授权或真实业务数据联调成功。

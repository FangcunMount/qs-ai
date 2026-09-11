# QS 报告快照接入验证

2026-09-11。本批实现 QS 产品业务入口 → 既有授权 → 标准报告/Outcome 解析 → 持久命令 → qs-ai 冻结快照及排队。没有新增用户、角色、Testee 权限表。

协议新增兼容字段 StartCommand.evidence。每项含 assessment_id、testee_id、report_id、source_version 和事实 ref/value；QS 首版发送 standard_report 内容。AI 校验输入关联，将快照纳入幂等指纹，并与会话、任务和结果 outbox 同事务保存。qs-snapshot-v1 使用提交时授权，不依赖动态 EvidenceSource。其他来源不放宽授权。

本地全量 86 项测试通过，包含两种命令的真实 Go/Python 联调；CI 固定 QS 提交 27972264bd5ca0d0386244f25e987b048acd2680。

本地验证覆盖：快照/任务回滚一致、不同主体拒绝读取、同 ID 改快照冲突、不依赖动态授权后端的 Worker 执行。Go/Python 真实双向 TLS、独立 MySQL 数据库联调同时覆盖旧命令与快照命令，验证丢确认重投、问答、乱序回调和进程重启后的可靠接收；使用测试报告和离线工作流。

QS 新增独立 ai-workflows 产品入口，复用原主体委托与归属判断；开关 workflow_enabled 默认 false。启用不依赖旧 Prompt/Profile 的发布状态。标准报告快照任务不会生成旧 Generation。现有线上入口不切流。

尚未验证：生产真实报告请求、生产服务证书及常驻 relay/Worker、真实模型、正式 Artifact 和前端成果展示。HTTP 202 仅代表 QS 入队，不能宣称 AI 生成成功。

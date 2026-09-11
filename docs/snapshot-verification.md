# QS 报告快照接入验证

2026-09-11。本批实现 QS 产品业务入口 → 既有授权 → 标准报告/Outcome 解析 → 持久命令 → qs-ai 冻结快照及排队。没有新增用户、角色、Testee 权限表。

协议新增兼容字段 StartCommand.evidence。每项含 assessment_id、testee_id、report_id、source_version 和事实 ref/value；QS 首版发送 standard_report 内容。AI 校验输入关联，将快照纳入幂等指纹，并与会话、任务和结果 outbox 同事务保存。qs-snapshot-v1 冻结事实但不冻结权限；执行前与接受结果前均复核当前授权。QS 持久主体授权适配未接通前，生产默认依赖失败将阻断执行，不得绕过。

本地全量 86 项测试通过，包含两种命令的真实 Go/Python 联调；CI 固定 QS 提交 27972264bd5ca0d0386244f25e987b048acd2680。

本地验证覆盖：快照/任务回滚一致、不同主体拒绝读取、同 ID 改快照冲突、不依赖动态授权后端的 Worker 执行。Go/Python 真实双向 TLS、独立 MySQL 数据库联调同时覆盖旧命令与快照命令，验证丢确认重投、问答、乱序回调和进程重启后的可靠接收；使用测试报告和离线工作流。

QS 新增独立 ai-workflows 产品入口，复用原主体委托与归属判断；开关 workflow_enabled 默认 false。启用不依赖旧 Prompt/Profile 的发布状态。标准报告快照任务不会生成旧 Generation。现有线上入口不切流。

尚未验证：生产真实报告请求、生产服务证书及常驻 relay/Worker、真实模型、正式 Artifact 和前端成果展示。HTTP 202 仅代表 QS 入队，不能宣称 AI 生成成功。

新增排队后撤权、执行中撤权及授权依赖不可用回归，验证模型不启动或结果不被接受，且不重新读取冻结事实。此验证使用测试授权源，不能代替真实 QS 授权接口验收。

2026-09-11 授权绕过修复验证：独立 MySQL 8.0.36，88 项非 interop 测试通过；新增四个执行前/后拒绝与依赖故障用例在旧实现上全部失败，修复后通过。Ruff、mypy、文档链接检查通过；未运行真实 QS 持久主体授权联调，不满足 M1 完成门槛。

当前授权适配：固定 QS 契约 96638d4039d7，QSAccessSource 调用 AIWorkflowAccessService.Authorize；Dishka 按 grpc.access_address 选择适配并管理 mTLS 通道生命周期。未配置默认拒绝；不持有用户令牌，不读取冻结快照之外的新事实。59 项非 integration 测试通过，含真实 TLS 的 Python 测试服务及状态映射；尚不代表 Go/Python 和生产 IAM 联调通过。

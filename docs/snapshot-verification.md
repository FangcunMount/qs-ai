# QS 报告快照接入验证

当前事实格式修复：QS `1a947bfd` 改为显式 [标准报告快照 v1](../integrations/qs_server/report-snapshot.md)。原先直接 JSON 序列化领域对象会丢失维度私有字段，不能据此前的关联/幂等测试判定事实完整。修复保留分数、等级、常模、层级与原文，并在 QS 出口应用参与者可见范围；新增事实内容回归和失败关闭测试通过。尚未接入 Python 输入组装或发布生产。

2026-09-11。本批实现 QS 产品业务入口 → 既有授权 → 标准报告/Outcome 解析 → 持久命令 → qs-ai 冻结快照及排队。没有新增用户、角色、Testee 权限表。

协议新增兼容字段 StartCommand.evidence。每项含 assessment_id、testee_id、report_id、source_version 和事实 ref/value；QS 首版发送 standard_report 内容。AI 校验输入关联，将快照纳入幂等指纹，并与会话、任务和结果 outbox 同事务保存。qs-snapshot-v1 冻结事实但不冻结权限；执行前与接受结果前均复核当前授权。QS 持久主体授权适配未接通前，生产默认依赖失败将阻断执行，不得绕过。

本地全量 86 项测试通过，包含两种命令的真实 Go/Python 联调；CI 固定 QS 提交 27972264bd5ca0d0386244f25e987b048acd2680。

本地验证覆盖：快照/任务回滚一致、不同主体拒绝读取、同 ID 改快照冲突、不依赖动态授权后端的 Worker 执行。Go/Python 真实双向 TLS、独立 MySQL 数据库联调同时覆盖旧命令与快照命令，验证丢确认重投、问答、乱序回调和进程重启后的可靠接收；使用测试报告和离线工作流。

QS 新增独立 ai-workflows 产品入口，复用原主体委托与归属判断；开关 workflow_enabled 默认 false。启用不依赖旧 Prompt/Profile 的发布状态。标准报告快照任务不会生成旧 Generation。现有线上入口不切流。

尚未验证：生产真实报告请求、生产服务证书及常驻 relay/Worker、真实模型、正式 Artifact 和前端成果展示。HTTP 202 仅代表 QS 入队，不能宣称 AI 生成成功。

新增排队后撤权、执行中撤权及授权依赖不可用回归，验证模型不启动或结果不被接受，且不重新读取冻结事实。此验证使用测试授权源，不能代替真实 QS 授权接口验收。

2026-09-11 授权绕过修复验证：独立 MySQL 8.0.36，88 项非 interop 测试通过；新增四个执行前/后拒绝与依赖故障用例在旧实现上全部失败，修复后通过。Ruff、mypy、文档链接检查通过；未运行真实 QS 持久主体授权联调，不满足 M1 完成门槛。

当前授权适配：固定 QS 契约 96638d4039d7，QSAccessSource 调用 AIWorkflowAccessService.Authorize；Dishka 按 grpc.access_address 选择适配并管理 mTLS 通道生命周期。未配置默认拒绝；不持有用户令牌，不读取冻结快照之外的新事实。59 项非 integration 测试通过，含真实 TLS 的 Python 测试服务及状态映射；尚不代表 Go/Python 和生产 IAM 联调通过。

Go/Python 授权联调已通过：Python QSAccessSource → Go AIWorkflowAccessService → CurrentAccess，真实 mTLS；同连接有效→撤销→恢复、跨组织/用户/Testee/测评、错误服务证书与关系源故障均验证。Go fixture 的 Testee/关系/归属数据为隔离替身，尚未证明真实 IAM 或生产调用。CI 固定 QS b9a2425260c629810f08c145cf479023fdeea90e 并构建专用测试进程，纳入每次 pytest。

生产只读核对（2026-09-11）：serverA QS 当前容器 ea166d2b，AI 仍 d39f55f；尚未发布本分支。QS 挂载的 apiserver.prod.yaml 中 AI enabled/participant_enabled 为 true，provider=deepseek、model=deepseek-v4-pro、protocol=responses、route_revision=v8、max_output_tokens=12000、timeout=120s。此为配置文件值，环境覆盖与实际 Profile/release 尚待核对，不直接作为最终案例锁定依据；未读取/输出密钥。

生产 Profile 只读查询成功（2026-09-11）：合并 QS 容器实际 Mongo 环境覆盖后，ai_explanation_profiles 中已发布记录 1 条：participant-scale-score-range-default，selector=participant/scale/score_range，无特定 model_code/version 限制，Prompt=cross-dimension-participant-scale/v6，provider_route=balanced_text_v1，输入/输出 Schema v1，上限 8000 字符。仅投影 definition/fingerprint/status，未读取测评者数据。迁移基线保存于 integrations/qs_server/prompts/published-profile-baseline.json；不是 qs-ai 当前运行配置。真实报告案例和 IAM 撤权验收仍待完成。

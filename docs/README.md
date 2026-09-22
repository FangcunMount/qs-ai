# qs-ai 设计与实施入口

当前执行以 [M1–M5 替代计划](migration-milestones.md) 和 [M5 退役清单](m5-retirement.md) 为准；退役准备不等于生产验收完成。原 P0–P5 文档保留技术设计和历史验证参考。

| 阅读顺序 | 文档 | 回答的问题 |
| --- | --- | --- |
| 0 | [迁移责任边界](migration-boundary.md) | 最新决定：QS 发起/接收，AI 独立执行及旧能力迁出 |
| 1 | [项目设计](design.md) | 系统边界、DDD 分层、六边形、Dishka、技术决定 |
| 2 | [领域与数据](domain-data.md) | 聚合、不变量、状态、MySQL 表、记忆 |
| 3 | [执行与恢复](runtime.md) | API/Worker、租约、模型调用、检查点协调与版本 |
| 4 | [接口与模型契约](contracts.md) | HTTP/gRPC、授权、Provider、Prompt、评测发布 |
| 5 | [替代里程碑](migration-milestones.md) | M1–M5 任务、依赖、验收与目标模式台账 |
| 技术参考 | [原实施路线](roadmap.md) | P0–P5 技术批次和验证矩阵 |
| 配置入口 | [统一配置](../configs/README.md) | 环境覆盖、启动入口与 Actions Secrets |
| 发布规划 | [CI/CD 建设方案](cicd-plan.md) | 对齐 QS 发布设施、MySQL Secrets、迁移与回滚的设计依据 |
| 发布证据 | [部署验证](deployment-verification.md) | CI、实际数据库与 serverA 版本验收 |
| 重构证据 | [命令入口整理](refactoring-verification.md) | 类型化输入、历史回执兼容与接口回归 |
| 当前证据 | [第 1 批验证](batch1-verification.md) | QS 发起、AI 持久执行、QS 可靠接收与剩余边界 |
| P1 证据 | [P1 验证](p1-verification.md) | 会话、任务、恢复、gRPC 传输与接入缺口 |
| P0 证据 | [P0 验证](p0-verification.md) | 分层、DI、进程恢复、旧写隔离及剩余限制 |
| 历史证据 | [初始化验证](verification.md) | 已经运行过什么、不能据此证明什么 |
| 历史 | [初始化架构边界](architecture.md) | 初始化时记录，完整设计以上述文档为准 |

本地运行见 [项目 README](../README.md)。文档维护规则见 [写作约定](CONTRIBUTING-DOCS.md)。

## 已实现与待实现

已实现范围见 [项目 README](../README.md)。M5 移除初期独立 HTTP 会话和 LangGraph 样板，保留 mTLS gRPC、健康检查、正式业务执行与恢复。历史验证文件中的框架、API 和测试数量只描述当时版本。

真实管理、授权生成展示、撤权与恢复验收仍需独立证据；未来产品能力不作为本轮退役门槛。

- [语义契约故障单次恢复](semantic-contract-recovery.md)：受信主机预览与授权、原证据保留和发布校验。

- [候选最终结果验收](candidate-completion-acceptance.md)：版本化规则、旧 Run 采用、发布门槛与调用观测。

- [AI 解读管理第二版](ai-governance-v2.md)：持久方案、模型参数、原子准备与验证边界。

- [配置中心后端契约与台账](config-center.md)

- [AI 解读管理第三版交付台账](governance-v3-delivery.md)：运行中心与流程工作区分批实施。

- [V3 运行中心契约与样例](governance-v3-contracts.md)

- [MBTI 接入前置基线](mbti-contract-baseline.md)：真实版本、脱敏报告及量表回执回放；新场景尚未实现。

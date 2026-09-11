# qs-ai 设计与实施入口

当前阶段：2026-09-11，P1 会话骨架及第 1 批跨服务持久投递本地联调已落地；真实跨服务授权/事实读取尚未接通。P2–P5 为设计。

| 阅读顺序 | 文档 | 回答的问题 |
| --- | --- | --- |
| 0 | [迁移责任边界](migration-boundary.md) | 最新决定：QS 发起/接收，AI 独立执行及旧能力迁出 |
| 1 | [项目设计](design.md) | 系统边界、DDD 分层、六边形、Dishka、技术决定 |
| 2 | [领域与数据](domain-data.md) | 聚合、不变量、状态、MySQL 表、记忆 |
| 3 | [执行与恢复](runtime.md) | API/Worker、租约、模型调用、检查点协调与版本 |
| 4 | [接口与模型契约](contracts.md) | HTTP/gRPC、授权、Provider、Prompt、评测发布 |
| 5 | [实施路线](roadmap.md) | P0–P5、验证矩阵、迁移部署、未决项 |
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

已实现：分层与 DI、五个会话 API、MySQL UoW/幂等/任务/心跳、证据冻结、恢复与旧写隔离；81 项 Python 测试通过，另有 3 项 Go 数据库测试；包含真实跨语言双向 TLS 与回传重放。

设计决定：Python/FastAPI/LangGraph/MySQL，DDD + 六边形，Dishka 统一装配，gRPC 访问 qs-server，API/Worker 共库分进程。

待实现：真实身份和证据接入、正式业务 Graph/模型/成果、常驻 Worker 运行策略、后续产品能力。容器和云资源验证仍需独立证据。

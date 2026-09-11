# 初始化验证记录

本地验证日期：2026-09-11。

- `uv sync`：成功，完整依赖锁定在 uv.lock。
- Ruff 检查与格式检查：通过。
- mypy：7 个源文件通过。
- pytest：4 项通过，其中 1 项使用 Docker MySQL 8.4 实测。测试覆盖无数据库时 readiness 失败、数据库错误脱敏、会话隔离以及关闭数据库连接后重新创建检查点适配器恢复问答。
- Alembic：迁移至 `0001_baseline`，此基线不包含业务表。
- 实际 HTTP 进程：`/healthz`、`/readyz`、`/openapi.json` 返回 200。
- Python 源码包和 wheel：构建成功。
- Docker 镜像构建：尝试两次，均在获取 Python 基础镜像时因 Docker Hub TLS 握手超时中断，尚未验证镜像构建成功。
- CI 配置已创建，尚未上传远端或运行 GitHub Actions。

环境中出现一个 Starlette/AnyIO 的弃用提示，不影响本次测试结果。

这些结果不证明云 MySQL 兼容、真实进程崩溃恢复、并发回答幂等、跨服务身份授权、真实模型输出质量或生产可用；相关业务尚待后续实现。

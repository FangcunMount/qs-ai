# P0 验证记录

日期：2026-09-11。本记录对应本地未提交代码；没有远端 CI 或部署证据。

## 结果

P0 分层、DI 与检查点技术适配已完成本地验证。业务会话、用户身份链、模型接入和任务消费尚未实现。原 [初始化记录](verification.md) 保留为历史证据。

| 检查 | 结果 |
| --- | --- |
| Ruff / 格式 / mypy | 通过；26 个源码文件类型检查 |
| pytest | 15 项通过，含 6 项 MySQL 集成测试 |
| DI | API/Worker 共用注册；APP 资源复用，操作对象隔离；异常关闭一次；缺失绑定和 APP 捕获 REQUEST 被拒绝 |
| 分层 | 静态导入边界、绝对导入约定、模块循环检查通过 |
| MySQL 8.4 | 本机 Docker 实例；迁移到 0002_checkpoint_leases；事务回滚、新 Session 隔离通过 |
| 进程恢复 | 子进程停在问答中断后被强制终止；新进程取得新租约，从 MySQL 恢复答案 |
| 旧写隔离 | 并发领取只有一个成功；旧 fence 不能 put/write/delete；旧 release 不释放新租约；错线程写被拒绝 |
| 事务中途失效 | 写入期间租约过期，提交前检查拒绝并回滚；取消时回滚后连接可复用 |
| 真实入口 | HTTP healthz/readyz/OpenAPI 为 200；Worker 探针输出 connected 并退出 0 |
| 源码包 / wheel | uv build 成功 |
| Docker 镜像 | 本轮重试失败：Docker Hub 授权服务 TLS handshake timeout，未进入应用构建 |
| 云 / CI / 生产 | 未验证；未上传远端、未部署 |

Starlette 的 AnyIO 弃用提示仍存在，不影响本轮结果。

## 实现约束

- 租约只保护 checkpoint 写入，尚无业务任务、续租心跳、成果 CAS 或模型外部副作用幂等。
- `FencedMySQLSaver` 依赖社区包的内部事务钩子，必须使用锁文件安装；升级时重跑以上集成测试。
- 检查点校验与写入共用行锁事务；业务步骤与 checkpoint 仍是分开的事务，后续按运行设计补对账。
- 技术租约行的 fence 不能重置。运行环境禁止直接绕过适配器写 checkpoint；测试清理使用独立测试线程。
- 静态检查约束正常源码导入，不是针对动态导入的安全沙箱。
- 云 MySQL 的版本、权限、事务及连接预算尚无证据，是并发接管上线前的验证项。

## 复现

从项目根目录按 README 启动开发 MySQL，执行 `uv sync --locked`、`uv run alembic upgrade head`，再运行：

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/check_docs.py
QS_AI_TEST_MYSQL_DSN=mysql://qs_ai:qs_ai_local@127.0.0.1:13316/qs_ai uv run pytest -q
uv run python -m qs_ai.bootstrap.worker
uv build
docker build -t qs-ai:p0 .
```

开发 DSN 仅用于本地测试库。镜像构建恢复后还需实测容器健康检查与退出释放，不能以 wheel 构建替代。

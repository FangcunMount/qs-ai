# qs-ai

独立的 AI 解读与交互服务。技术基线：Python、FastAPI、LangGraph、MySQL；目标是通过 gRPC 读取 qs-server 的授权业务事实；当前跨服务业务接入尚未完成。

目标架构采用 **DDD + 六边形 + Dishka 依赖注入**。完整设计及 P0–P5 计划见 [设计入口](docs/README.md)；P0 已落地，P1 已完成第一批业务骨架。最新已完成第 1 批跨服务持久投递联调，见 [验证记录](docs/batch1-verification.md) 和 [运行契约](integrations/workflow/README.md)。

## 当前范围

已实现分层与 DI、会话创建/查询/开始/回答/取消、不可变证据快照、MySQL 任务领取与心跳、幂等回执和旧执行隔离。测试中的离线图验证问答中断、进程终止后恢复，不调用模型。

真实身份、证据源和业务工作流默认绑定为不可用，接口不会信任传入的用户/组织标识：无身份返回 401，未配置的身份集成返回 503。尚无真实模型、正式成果、多报告解读、长期记忆或生产部署。`/readyz` 仅检查数据库，不证明这些业务能力就绪。

## 本地运行

先安装 Python 3.11 和 uv，然后在本目录执行：

```sh
export PATH="$HOME/.local/bin:$PATH"
uv sync --locked
cp -n .env.example .env
docker compose up -d --wait mysql
uv run alembic upgrade head
uv run uvicorn qs_ai.main:create_app --factory --reload
```

打开 http://127.0.0.1:8000/docs。`/healthz` 检查进程存活，`/readyz` 检查数据库连接。没有配置数据库时，进程可以启动，但 readiness 返回 503。

本地 MySQL 使用 8.4，端口为 `127.0.0.1:13316`。Compose 中的凭据仅供本机开发。生产应使用专用数据库和账号，并单独提供密钥。迁移通过独立发布步骤执行，不在服务启动时自动执行。

```sh
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/generate_qs_proto.py --check
uv run python scripts/generate_workflow_proto.py --check
uv run python scripts/check_docs.py
uv run pytest -m 'not integration'
QS_AI_TEST_MYSQL_DSN=mysql://qs_ai:qs_ai_local@127.0.0.1:13316/qs_ai uv run pytest -m integration
docker compose stop
```

集成测试会创建检查点表并写入临时线程，只能指向可用于测试的数据库；先执行 Alembic 迁移。业务表/技术租约表由 Alembic 管理，检查点表由适配包管理。租约校验和检查点写入共享事务，但业务步骤和检查点之间仍需恢复协议。

Worker 默认探测数据库：`uv run python -m qs_ai.bootstrap.worker`；加 `--once` 领取至多一个持久任务后退出。一次运行包含心跳和失效取消。当前无常驻轮询模式；缺失业务集成时已领取任务会进入 blocked，不会生成假成果。完整离线闭环由集成测试注入合成事实和测试工作流执行。

## 模块边界

- `src/qs_ai/main.py`：保留 HTTP 工厂入口，转发到 bootstrap。
- `src/qs_ai/domain/`：会话聚合、问题、证据与状态不变量。
- `src/qs_ai/application/`：会话命令/查询、工作执行和外部端口。
- `src/qs_ai/transport/`：健康与会话 HTTP 路由、内部 gRPC 命令入口。
- `src/qs_ai/bootstrap/`：Dishka Provider、容器和进程生命周期。
- `src/qs_ai/config.py`：`QS_AI_` 前缀配置，不在代码中保存密钥。
- `src/qs_ai/infrastructure/`：MySQL UoW/任务、检查点适配、已固定版本的 gRPC 传输探针。
- `integrations/qs_server/`：gRPC 契约接入说明。
- `migrations/`：当前 head 为 `0004_delivery`，包含外部请求关联与结果 outbox。
- `tests/`：分层、DI 生命周期、事务及检查点恢复验证；离线图位于 `tests/probes/`。
- `docs/README.md`：完整设计、领域数据、运行接口及实施计划入口。

## 持久化依赖

MySQL 检查点使用社区维护的 `langgraph-checkpoint-mysql`，具体依赖版本以 `uv.lock` 为准。适配器覆写了社区包内部事务钩子。升级 LangGraph 或适配包时必须重新运行恢复、租约竞争、旧写拒绝和取消回滚测试，不能仅以安装成功认定兼容。

本机 Compose 的数据保留在 Docker volume 中；`docker compose down` 删除容器但保留数据，只有确认不要数据时才使用 `down -v`。

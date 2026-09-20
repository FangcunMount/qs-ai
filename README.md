# qs-ai

独立 AI 后台服务，使用 Python、FastAPI、MySQL、gRPC 和 Dishka，按 DDD 与六边形架构组织。qs-server 负责身份、业务授权和报告事实，qs-ai 负责配置、评测发布、执行与结果投递。

当前实施以 [M1–M5 计划](docs/migration-milestones.md) 和 [M5 退役清单](docs/m5-retirement.md) 为准。代码检查、部署状态、真实业务验收分别记录；本分支的退役准备不代表生产已完成切换或清库。

## 当前范围

已实现报告事实快照、发布配置绑定、模型调用回执、MySQL 任务领取与续租、幂等、恢复、成果投递，以及配置/评测/审核/发布的内部管理接口。生产业务通过 mTLS gRPC 接入。HTTP 提供健康检查和内网汇总指标，初期独立会话路由已经移除；LangChain / LangGraph 样板和运行依赖已移除，生产恢复直接使用业务状态和执行租约。

多报告、主动追问、长期记忆和新 Prompt 编排仍属后续产品建设。`/readyz` 只检查数据库，不代表真实管理和测评闭环已经验收。

## 本地运行

先安装 Python 3.11 和 uv，然后在本目录执行：

```sh
export PATH="$HOME/.local/bin:$PATH"
uv sync --locked
export QS_AI_DATABASE_URL=mysql+asyncmy://qs_ai:qs_ai_local@127.0.0.1:13316/qs_ai
docker compose up -d --wait mysql
uv run alembic upgrade head
uv run python -m qs_ai.bootstrap.server
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

集成测试会写入合成业务数据，只能指向独立测试数据库；先执行 Alembic 迁移。迁移 `0028_execution_leases` 原样迁名正式使用的租约表，不删除 fence 数据；升级前停止旧 worker，迁移后使用新版本，详见退役清单。

统一入口 `python -m qs_ai.bootstrap.server` 在一个进程内运行 HTTP、gRPC、生成、评测和结果投递。Worker 与 evaluation 命令默认只读探测，`--once` 用于单次维护执行；不再支持独立常驻模式。生命周期、配置与切换见 [单进程运行说明](docs/single-process.md)。

## 模块边界

- `src/qs_ai/main.py`：保留 HTTP 工厂入口，转发到 bootstrap。
- `src/qs_ai/domain/`：会话聚合、问题、证据与状态不变量。
- `src/qs_ai/application/`：会话命令/查询、工作执行和外部端口。
- `src/qs_ai/transport/`：HTTP 健康/指标路由与内部 gRPC 业务入口。
- `src/qs_ai/bootstrap/`：Dishka Provider、容器和进程生命周期。
- `configs/`：默认、本地、生产配置；详见 [配置说明](configs/README.md)。
- `src/qs_ai/config.py`：统一加载、覆盖与类型校验；生产密钥经 Actions Secrets 注入。
- `src/qs_ai/infrastructure/`：MySQL UoW/任务/执行租约、模型适配与 gRPC 客户端。
- `integrations/qs_server/`：gRPC 契约接入说明。
- `migrations/`：Alembic 业务迁移账本；当前分支新增 `0028_execution_leases`。
- `tests/`：分层、DI、事务、授权边界、执行恢复、管理与跨语言契约验证。
- `docs/README.md`：完整设计、领域数据、运行接口及实施计划入口。

## 持久化依赖

MySQL 通过 SQLAlchemy/asyncmy 访问，所有业务结构由 Alembic 管理。业务检查点、执行租约、模型派发标记和结果 outbox 保留；未知模型调用不得直接重发。依赖版本以 `uv.lock` 为准。

本机 Compose 的数据保留在 Docker volume 中；`docker compose down` 删除容器但保留数据，只有确认不要数据时才使用 `down -v`。

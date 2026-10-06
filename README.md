# qs-ai

qs-ai 是 QS 的独立 AI 后台，负责解读方案与资产、评测审核、配置发布、持久执行和成果交付。IAM 提供身份与权限基础，qs-server 持有业务授权和标准报告事实；qs-ai 消费可信输入，不重新计分、不维护用户和 Testee。

统一常驻入口为 `python -m qs_ai.bootstrap.server`。一个进程内监督 HTTP、内部 mTLS gRPC、生成、评测、MQ 收发与 Relay。执行命令使用 MQ；旧 Start、Change、Participant Retry、Evaluation Start/Cancel 的 gRPC 写入口拒绝执行。HTTP 提供健康检查和内部汇总指标。LangChain 承担模型适配，LangGraph 编排生成与评测步骤；恢复仍由业务持久状态、租约与调用回执决定。

## 从哪里开始

- [文档地图](docs/README.md)：系统边界、两条主链和按角色阅读路径。
- [系统定位](docs/00-总览/01-系统定位与跨服务边界.md)、[核心概念](docs/00-总览/02-核心概念与对象关系.md)：先理解责任与对象。
- [本地开发与配置](docs/04-接口与运维/04-配置与本地开发.md)：完整服务的 MySQL、QS、NSQ、mTLS 与 JOSE 前置条件。
- [接口契约](docs/04-接口与运维/01-接口地图与契约所有权.md)、[部署说明](deploy/serverA/README.md)：接入与运维。

## 开发与检查

Python 和依赖范围以 [pyproject.toml](pyproject.toml)、[uv.lock](uv.lock) 为准。安装 uv 后执行：

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python scripts/generate_qs_proto.py --check
uv run python scripts/generate_workflow_proto.py --check
uv run python scripts/generate_messaging_proto.py --check
uv run python scripts/check_docs.py
uv run pytest -q tests/test_docs.py
```

完整服务启动需要显式配置的数据库、证书、消息源与密钥；默认配置不能直接作为完整服务配置。仅启动本地 MySQL 不足以通过服务启动检查。数据库迁移是独立发布步骤，启动时校验版本，不自动执行迁移。

业务回归与隔离 MySQL、跨语言、消息故障验证入口见 [测试与证据](docs/00-总览/04-代码地图与能力状态.md)。集成测试只能指向一次性测试数据库。文档检查、仓库测试、CI、部署健康、真实业务和设备验收分别报告。

## 源码布局

| 目录 | 职责 |
| --- | --- |
| `src/qs_ai/domain/` | 解读、治理、评测领域规则 |
| `src/qs_ai/application/` | 用例、外部端口与执行支持 |
| `src/qs_ai/infrastructure/` | MySQL、模型、工作流和传输适配 |
| `src/qs_ai/transport/` | HTTP、内部 gRPC 与入口映射 |
| `src/qs_ai/bootstrap/` | Dishka 装配和进程资源生命周期 |
| `integrations/` | 有来源的机器契约、资产和跨语言接入 |
| `configs/`、`deploy/` | 配置与部署说明 |
| `migrations/`、`tests/` | Alembic 迁移与分层验证 |

已实现范围与限制见能力状态正文；历史批次不再作为当前运行入口。

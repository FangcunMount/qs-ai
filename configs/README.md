# 统一启动配置

`default.yaml` 保存公共非敏感默认值；`local.yaml`、`production.yaml` 只保存环境差异。`src/qs_ai/config.py` 负责加载和类型校验，HTTP、Worker、gRPC、Alembic 共用同一 Settings。

优先级从低到高：默认 YAML → 环境 YAML → 环境变量 → 显式构造参数。嵌套对象逐字段合并。`QS_AI_ENVIRONMENT` 只允许 local/production，默认 local；拼写错误、缺失文件或非法参数会立即失败。配置路径不依赖当前工作目录；wheel 和镜像均携带 YAML。

不自动读取 `.env`。本地使用 shell 环境变量；生产敏感值通过 GitHub Actions 的组织/仓库 Secrets 注入，发布 job 使用 production Environment。分项 MYSQL Secrets 由部署脚本转换为应用的 QS_AI_DATABASE_URL。YAML 中禁止 database_url，避免在配置文件里保存凭据。

| 环境变量 | 用途 |
| --- | --- |
| `QS_AI_ENVIRONMENT` | 环境选择 |
| `QS_AI_DATABASE_URL` | mysql+asyncmy 连接 URL，密码须 URL 编码 |
| `QS_AI_DATABASE__POOL_SIZE` | 连接池大小 |
| `QS_AI_HTTP__PORT` | HTTP 监听端口 |
| `QS_AI_WORKER__LEASE_SECONDS` | 任务租约秒数，至少 3 秒 |
| `QS_AI_GRPC__RESULT_ADDRESS` | QS 回传目标 |
| `QS_AI_GRPC__CA_FILE` / `CERT_FILE` / `KEY_FILE` | 分别使用完整 QS_AI_GRPC__ 前缀的证书文件路径 |
| `QS_AI_DELIVERY__BATCH_SIZE` | 单次回传数量，1–100 |

HTTP 启动：`uv run python -m qs_ai.bootstrap.http`。直接使用 uvicorn CLI 会由 CLI 控制端口；需要统一配置时使用上述入口。gRPC 命令行参数可显式覆盖地址与证书路径，未提供时读取 Settings。

连接池、租约、投递与 gRPC 参数是启动配置；Prompt、模型路线与发布版本是后续业务治理数据，不放进这些文件。不提供热更新，修改配置后需重启相应进程。

生产部署操作和 Secret 名称见 [serverA 部署](../deploy/serverA/README.md)。

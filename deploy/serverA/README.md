# serverA 内部基础服务部署

首次仅部署 HTTP API 与独立 MySQL schema，不开放公网路由，不切换 QS 的现有 AI 流量。真实授权源、证据源和业务工作流尚未接通，业务请求会拒绝执行；健康检查仅证明数据库连接可用。

使用非 root 镜像进程、512 MB 内存限制、只读容器文件系统和有界日志。宿主机仅监听 `127.0.0.1:18080`，内部连接既有 `infra-network`。不启动离线测试工作流，也不启动尚未配置证书和事实源的 gRPC/Worker/relay。

## 准备

- 在云 MySQL 中准备独立 `qs_ai` 数据库和专用账号，不复用 QS 业务库。
- 在 serverA `/opt/qs-ai/.env` 安全配置 `QS_AI_ENVIRONMENT=production` 及 `QS_AI_DATABASE_URL`（mysql+asyncmy URL）；密码须 URL 编码。文件权限 0600，不提交仓库。
- 从通过 CI 的确定提交构建镜像，标签使用完整提交 SHA，设置 `QS_AI_IMAGE`。镜像来源、SHA、迁移版本和运行验证应单独留证。

## 操作

在 serverA 上，环境文件准备好后执行；源码或部署包位于 `/opt/qs-ai/releases/<SHA>`：

```sh
export QS_AI_IMAGE=qs-ai:<SHA>
export QS_AI_ENV_FILE=/opt/qs-ai/.env
sudo --preserve-env=QS_AI_IMAGE,QS_AI_ENV_FILE docker compose -f /opt/qs-ai/releases/<SHA>/deploy/serverA/compose.yaml run --rm --no-deps api /app/.venv/bin/alembic upgrade head
sudo --preserve-env=QS_AI_IMAGE,QS_AI_ENV_FILE docker compose -f /opt/qs-ai/releases/<SHA>/deploy/serverA/compose.yaml up -d --wait
curl --fail http://127.0.0.1:18080/healthz
curl --fail http://127.0.0.1:18080/readyz
```

启动不自动迁移。首次失败可停止本项目容器，保留数据库；后续版本回滚必须先确认 schema 兼容，再切回保留的旧镜像。不要自动回退或删除生产数据。

## 未完成的上线环节

服务证书签发、可信主体/事实读取、正式模型与成果、常驻任务及投递调度、告警和产品灰度另行验收。这里的 API 部署不能代表“真实 AI 解读已经上线”。

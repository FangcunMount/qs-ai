# serverA 内部基础服务部署

首次仅部署 HTTP API 与独立 MySQL schema，不开放公网路由，不切换旧 QS AI 流量。真实身份/事实和业务工作流仍未接通，readiness 仅证明数据库可用。

## GitHub Actions 配置

在仓库的 `production` Environment 配置以下 Secrets，不要在聊天或仓库中写入值：

| Secret | 内容 |
| --- | --- |
| `QS_AI_DATABASE_URL` | 独立 qs_ai 库的 mysql+asyncmy URL；密码 URL 编码 |
| `SVRA_HOST` | serverA 的可达地址 |
| `SVRA_USERNAME` | 具有既有 Docker/发布目录 sudo 权限的部署账号 |
| `SVRA_SSH_KEY` | 部署私钥 |
| `SVRA_KNOWN_HOSTS` | 已核验的 serverA SSH 主机公钥记录 |

部署工作流默认使用 `["self-hosted", "Linux", "X64"]` runner，需能通过 SSH 到 serverA，且已安装 git、gh、ssh 和 scp。可通过仓库变量 `DEPLOY_RUNNER_LABELS` 设置 JSON 标签列表。目前本任务查询时仓库没有可用的 self-hosted runner；启用部署前需配置 runner 或授权组织 runner 访问此仓库。

只允许在 main 上手动触发 `Deploy serverA`，并要求当前完整 SHA 的 CI 已通过。工作流归档确定版本源码，经 SSH 上传至 `/opt/qs-ai/releases/<SHA>`，使用完整 SHA 镜像标签，在独立数据库上执行迁移，再启动并等待 readiness。

敏感值只在部署步骤环境中提供，通过 SSH stdin 传给服务端脚本，不进入命令参数或源码归档。服务端临时目录权限 0700，配置文件权限 0600，迁移和启动结束后自动删除；容器保存运行所需环境变量。不会长期保留 `/opt/qs-ai/.env`。错误日志避免打印敏感命令输出。

## 配置与运行限制

运行参数统一读取 [configs](../../configs/README.md)，容器固定选择 production。512 MB 内存、1 CPU、UID 10001、只读根文件系统、有界日志。宿主机仅监听 `127.0.0.1:18080`，内部连接既有 `infra-network`。

不启动未配置证书、授权源和业务工作流的 gRPC/Worker/relay，不注入合成测试实现。模型凭据尚未接入，此工作流也不会假装已支持模型调用。

不自动回退数据库。失败保留数据；旧镜像回滚需先确认 schema 兼容。没有自动部署触发，也不会因为 push 自动执行生产迁移。

## 已有环境证据

2026-09-11 已确认 serverA 的 Docker 29.1.1、Compose v2.40.3、可附加的 infra-network，约 6 GB 可用内存、48 GB 可用磁盘。旧提交 `5ae60d76fed1ca9a28e239ccfedead4d1d3467dd` 的镜像构建与非 root 装配检查通过；这是历史镜像证据，不代表本次配置改动已经部署。

当前专用数据库及 Actions Secrets 尚未配置完成，持久服务未启动。本次配置变更的本地测试/CI 与生产验收分开记录。

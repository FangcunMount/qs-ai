# CI/CD 与 serverA 部署验证

2026-09-11：serverA 基础 API 首发已完成。自动发布与回滚演练分别记录；真实 AI 执行链路不在本批上线范围。

已实现固定 SHA 的 CI 门禁、ACR 镜像构建、qlume runner 上传、分项 MySQL Secrets 转换、serverA 镜像身份检查、独立迁移、就绪验收、版本记录和 schema 受限回滚。新增 CI 镜像启动检查与固定 QS 提交的跨语言数据库联调。

本地部署回归覆盖密码编码、配置冲突、镜像归档损坏、迁移失败保留旧服务、schema 不变时恢复旧服务、schema 改变时拒绝自动回退、首次失败清理和成功后记录版本。生产数据库、runner 权限和首次发布结果见下方分阶段证据。

## 已获得的远程证据

- [PR #1](https://github.com/FangcunMount/qs-ai/pull/1) 与 [PR #2](https://github.com/FangcunMount/qs-ai/pull/2) 已合并。81 项本地测试全部通过，包含真实 Go/Python 数据库联调。
- [主分支 CI](https://github.com/FangcunMount/qs-ai/actions/runs/34575183838) 通过；镜像在只读、非 root 配置下启动通过，缺失数据库时 readyz 正确返回 503。
- 初次 ACR 推送拒绝附加 OCI attestation；关闭仓库不支持的证明清单后，[后续发布](https://github.com/FangcunMount/qs-ai/actions/runs/34575326341) 的镜像构建、ACR 推送、qlume Mac mini runner 和 serverA SSH 校验均通过。
- 发布随后在 MYSQL_DATABASE 与 MYSQL_DBNAME 值不一致时明确停止。尚未连接生产数据库、执行迁移或启动 API；库名来源等待用户确认。
- production Environment 已限制 main；serverA 主机公钥已核验并固定。AUTO_DEPLOY_ENABLED 尚未开启。

## serverA 首发成功

用户确认以 MYSQL_DATABASE 为准，已移除冲突的仓库 Secret MYSQL_DBNAME。Mac runner 改用 QS 同款隔离 Docker auth 文件后，镜像拉取、上传与服务器部署通过。

[成功发布 34579267860](https://github.com/FangcunMount/qs-ai/actions/runs/34579267860) 部署提交 e3fd6305f9e5a0f2235df6f75e8b10feafc370d0。服务器独立核验：

- MySQL 8.0.36；当前与目标 Alembic head 均为 0004_delivery。
- 容器 qs-ai-api 的 image ID 与发布清单一致；UID 10001，根文件系统只读。
- 127.0.0.1:18080/readyz 返回 ready / database connected。
- /opt/qs-ai/state.json 已记录成功发布，首次发布 previous 为空。

[PR #4](https://github.com/FangcunMount/qs-ai/pull/4) 与对应主分支 CI 已通过。CI 随后增加 MySQL 8.0.36 / 8.4 双版本矩阵及发布数据库探针。首发成功后启用 AUTO_DEPLOY_ENABLED；后续 main checks 成功将自动发布该确切提交。

当前服务仅为内部 HTTP API 基础设施，未启用真实授权/事实、模型、常驻 worker 或 QS 业务流量切换。数据库连通和迁移完成不代表 AI 解读业务验收完成。

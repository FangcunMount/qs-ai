# CI/CD 与 serverA 部署验证

2026-09-11：实施中，尚未完成本批生产验收。

已实现固定 SHA 的 CI 门禁、ACR 镜像构建、qlume runner 上传、分项 MySQL Secrets 转换、serverA 镜像身份检查、独立迁移、就绪验收、版本记录和 schema 受限回滚。新增 CI 镜像启动检查与固定 QS 提交的跨语言数据库联调。

本地部署回归覆盖密码编码、配置冲突、镜像归档损坏、迁移失败保留旧服务、schema 不变时恢复旧服务、schema 改变时拒绝自动回退、首次失败清理和成功后记录版本。生产数据库、runner 权限和首次发布结果待下方补充。

## 已获得的远程证据

- [PR #1](https://github.com/FangcunMount/qs-ai/pull/1) 与 [PR #2](https://github.com/FangcunMount/qs-ai/pull/2) 已合并。81 项本地测试全部通过，包含真实 Go/Python 数据库联调。
- [主分支 CI](https://github.com/FangcunMount/qs-ai/actions/runs/34575183838) 通过；镜像在只读、非 root 配置下启动通过，缺失数据库时 readyz 正确返回 503。
- 初次 ACR 推送拒绝附加 OCI attestation；关闭仓库不支持的证明清单后，[后续发布](https://github.com/FangcunMount/qs-ai/actions/runs/34575326341) 的镜像构建、ACR 推送、qlume Mac mini runner 和 serverA SSH 校验均通过。
- 发布随后在 MYSQL_DATABASE 与 MYSQL_DBNAME 值不一致时明确停止。尚未连接生产数据库、执行迁移或启动 API；库名来源等待用户确认。
- production Environment 已限制 main；serverA 主机公钥已核验并固定。AUTO_DEPLOY_ENABLED 尚未开启。

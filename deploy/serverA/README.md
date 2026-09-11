# serverA 内部基础服务部署

当前发布范围是 HTTP API 和独立 MySQL schema。真实身份、事实与业务工作流仍未接通；readiness 证明数据库连接，发布探针另核对 schema 和镜像版本。

## 工作流

[Deploy serverA](../../.github/workflows/deploy.yml) 使用 GitHub runner 构建 linux/amd64 镜像并发布 ACR，qlume 组 Mac mini runner 导出镜像，以固定主机公钥验证 SSH 后上传 serverA。服务器校验镜像归档摘要、镜像 ID、架构与提交标签，再迁移、启动并核对运行版本。

手动操作要求工作流运行于 main；revision 留空选择当前提交，也可填写 main 上已通过 push CI 的完整 SHA。operation=rollback 恢复 serverA 记录的上一成功发布。只有数据库仍匹配旧镜像的迁移 head 时才允许回滚。

自动发布由 main 的 checks 成功触发，同时要求仓库变量 AUTO_DEPLOY_ENABLED=true。首次通过手动发布验收后再启用。并发部署串行，serverA 另外使用文件锁防止手动操作重叠。

## 配置

复用组织 Secrets：MYSQL_HOST、MYSQL_PORT、ALIYUN_ACR_REGISTRY、ALIYUN_ACR_NAMESPACE、ALIYUN_ACR_USERNAME、ALIYUN_ACR_PASSWORD、SVRA_HOST、SVRA_USERNAME、SVRA_SSH_PORT，以及优先使用的 SVR_MINI_SSH_KEY（缺失时用 SVRA_SSH_KEY）。serverA 优先使用组织变量 SVRA_PUBLIC_HOST；地址、用户、端口支持同名仓库变量覆盖。Mac mini 采用隔离的 Docker auth 文件，避免触发 macOS Keychain。

仓库 Secrets：MYSQL_DATABASE、MYSQL_USERNAME、MYSQL_PASSWORD。MYSQL_DBNAME 为数据库名兼容项，两者均配置却不一致时拒绝发布。连接 URL 由 SQLAlchemy 构造，不需要另建 QS_AI_DATABASE_URL Secret。

仓库变量 SVRA_HOST_PUBLIC_KEY 保存通过可信 SSH 连接核对的 serverA ed25519 主机公钥。production Environment 限定 main，qlume runner 组需允许本仓库使用。运行参数仍由 [configs](../../configs/README.md) 管理。

部署运行凭据保存在 /opt/qs-ai/releases/<SHA>-<run>-<attempt>/runtime.json，目录 0700，文件 0600，支持容器重建与旧版本恢复。它们不进入镜像、Actions artifacts 或普通日志。部署 runner 的 SSH、Docker 和凭据临时目录在结束时删除。

## 迁移、验收和失败处理

数据库探针核对 MySQL 版本、当前与目标 Alembic head。云实例须支持当前 MySQL 任务实现；CI 版本兼容性另行核验。迁移单独执行，不与 API 启动绑定。

迁移或迁移后 schema 检查失败时不切换服务。服务切换失败且 schema 未变化时恢复旧版本；schema 已变化时要求兼容性处理，不自动降级数据库。首次没有旧版本的服务启动失败会停止本次容器。

state.json 仅在全部验收通过后更新，保存 current/previous 发布目录。每个发布目录有 manifest.json（提交、镜像 ID/digest、归档摘要）、verification.json（迁移版本与阶段）；失败时另有 failure.json（脱敏阶段）。成功后删除大体积镜像归档，Docker 镜像和受限运行配置保留用于恢复。旧版本清理目前由运维按保留策略进行，须保留 current/previous。

回滚可由 GitHub 手动工作流 operation=rollback 发起，也可以使用成功发布目录保留的 deploy.py rollback。回滚不调用模型，不修改业务数据，不执行 Alembic downgrade。

## 运行范围

一个 API 容器，512 MB 内存、1 CPU、UID 10001、只读根文件系统和有界日志。仅监听 127.0.0.1:18080，连接现有 infra-network。worker/gRPC/relay 常驻运行与 QS 业务流量切换属于下一批。

建设背景及后续计划见 [CI/CD 方案](../../docs/cicd-plan.md)。实际发布结果见 [部署验证](../../docs/deployment-verification.md)。

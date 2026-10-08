# serverA 内部基础服务部署

当前 `0040_module_table_names` 工作区的发布目标为单容器、单进程 qs-ai：HTTP 运维端点、mTLS gRPC、生成、评测及 MQ relay/订阅/发布共享生命周期和连接池。常驻入口要求 MQ；readiness 同时检查组件和数据库，部署另核对 schema、镜像版本、TLS 与 MQ 密钥。具体生产运行状态由本次准确版本的部署证据确认。

## 工作流

[Deploy serverA](../../.github/workflows/deploy.yml) 使用 GitHub runner 构建 linux/amd64 镜像并发布 ACR，qlume 组 Mac mini runner 导出镜像，以固定主机公钥验证 SSH 后上传 serverA。服务器校验镜像归档摘要、镜像 ID、架构与提交标签，再迁移、启动并核对运行版本。

手动操作要求工作流运行于 main；revision 留空选择当前提交，也可填写 main 上已通过 push CI 的完整 SHA。operation=rollback 恢复 serverA 记录的上一成功发布。普通镜像回滚仅在数据库匹配旧镜像head和存储布局时允许。0038/0040任一方向必须进入[双向全量数据转换](../../docs/04-接口与运维/09-模块数据表与双向迁移.md)，完整保留当前新增事实。

[Schema maintenance serverA](../../.github/workflows/schema-maintenance.yml) 是这次结构切换的专用入口。手动依次执行 preflight、prepare、rehearse、switch，恢复使用 rollback，status 只读查询。每次保持相同 maintenance_id 和已通过 main CI 的完整 revision；prepare 中断后填写第一次生成的 release ID 继续，不重新构建或替换已绑定镜像。候选镜像准备不更新部署基线，切换完成并核验运行时后才更新 state.json。维护窗口内暂停自动部署。

自动发布由 main 的 checks 成功触发，同时要求仓库变量 AUTO_DEPLOY_ENABLED=true；是否已启用需读取仓库当前设置。并发部署串行，serverA 另外使用文件锁防止手动操作重叠。

## 配置

复用组织 Secrets：MYSQL_HOST、MYSQL_PORT、ALIYUN_ACR_REGISTRY、ALIYUN_ACR_NAMESPACE、ALIYUN_ACR_USERNAME、ALIYUN_ACR_PASSWORD、SVRA_HOST、SVRA_USERNAME、SVRA_SSH_PORT，以及优先使用的 SVR_MINI_SSH_KEY（缺失时用 SVRA_SSH_KEY）。serverA 优先使用组织变量 SVRA_PUBLIC_HOST；地址、用户、端口支持同名仓库变量覆盖。Mac mini 采用隔离的 Docker auth 文件，避免触发 macOS Keychain。

仓库 Secrets：MYSQL_DATABASE、MYSQL_USERNAME、MYSQL_PASSWORD。MYSQL_DBNAME 为数据库名兼容项，两者均配置却不一致时拒绝发布。连接 URL 由 SQLAlchemy 构造，不需要另建 QS_AI_DATABASE_URL Secret。

production Environment 的维护 Secrets 为 MYSQL_MAINTENANCE_USERNAME、MYSQL_MAINTENANCE_PASSWORD。它们保存独立迁移账号，RDS 地址和源库复用已绑定的生产 release 配置。维护权限在 RDS 授予，Secrets 不会自动增加账号权限。维护凭据只用于维护步骤，临时文件为 0600 并在结束时移除；完整数据备份和 journal 留在 serverA 的私有目录，Actions 仅保存不含正文的摘要证据。

仓库变量 SVRA_HOST_PUBLIC_KEY 保存通过可信 SSH 连接核对的 serverA ed25519 主机公钥。production Environment 限定 main，qlume runner 组需允许本仓库使用。运行参数仍由 [configs](../../configs/README.md) 管理。

部署运行凭据保存在 /opt/qs-ai/releases/<SHA>-<run>-<attempt>/runtime.json，目录 0700，文件 0600，支持容器重建与旧版本恢复。它们不进入镜像、Actions artifacts 或普通日志。部署 runner 的 SSH、Docker 和凭据临时目录在结束时删除。

生成、治理、评测、候选执行模式与模型目录写入分别使用 QS_AI_EXECUTION_ENABLED、QS_AI_GOVERNANCE_ENABLED、QS_AI_EVALUATION_ENABLED、QS_AI_CANDIDATE_MODE_ENABLED、QS_AI_MODEL_V2_WRITES_ENABLED，工作流默认 false。并发与供应商总容量另配置，已有 Run 模式不会随部署转换。精确变量与嵌套应用映射以[工作流](../../.github/workflows/deploy.yml)和[打包脚本](../../scripts/cd/deploy.py)为准。

QS_AI_QS_ADDRESS 提供 QS 授权/正文读取 endpoint。模型执行启用时，打包脚本要求 HTTPS 模型 endpoint 和 DeepSeek 凭据；QS_AI_MODEL_API_KEY 与 QS_AI_DEEPSEEK_API_KEY 同时配置须一致，智谱使用 QS_AI_ZHIPU_API_KEY。凭据不进入资产指纹。

QS_AI_MESSAGING_BINDING 是经审阅的非秘密 JSON，包含 binding_revision、enabled=true、nsqd、signing_key_file、decrypt_key_files、qs_signer_files、qs_recipient_key_file 和 max_in_flight=1。打包脚本限定 `/run/qs-ai-jose/` 的角色/kid 文件，从 `/data/infra/qs-ai-messaging/versions/<binding_revision>/` 单文件只读挂载，不自动创建缺失源文件。发布 manifest 固定绑定摘要，目标主机核对文件与运行身份。缺少绑定的历史发布不能满足当前常驻 server 的 MQ 要求。

## 迁移、验收和失败处理

数据库探针核对 MySQL 版本、当前与目标 Alembic heads；迁移后必须与镜像的全部 heads 完全相同。云实例须支持当前 MySQL 任务实现；CI 版本兼容性另行核验。普通部署在Alembic或停止旧服务前拒绝跨0038/0040及0039中间态；对应自动部署同样受阻。迁移单独执行，不与服务启动绑定；MySQL DDL 失败可能部分完成，须核对实际结构后处置。

迁移或迁移后 schema 检查失败时不切换服务。服务切换失败且 schema 未变化时恢复旧版本；schema 已变化时要求兼容性处理，不自动降级数据库。首次没有旧版本的服务启动失败会停止本次容器。

state.json 仅在脚本自动部署检查（readiness、准确镜像、mTLS和数据库）通过后更新，保存 current/previous 发布目录。每个发布目录有 manifest.json（提交、镜像 ID/digest、归档摘要）、verification.json（迁移版本与阶段）；失败时另有 failure.json（脱敏阶段）。成功后删除大体积镜像归档，Docker 镜像和受限运行配置保留用于恢复。旧版本清理目前由运维按保留策略进行，须保留 current/previous。

回滚可由 GitHub 手动工作流 operation=rollback 发起，也可以使用成功发布目录保留的 deploy.py rollback。回滚不调用模型，不修改业务数据，不执行 Alembic downgrade。目标镜像须识别当前持久契约，先比较current数据库与目标镜像离线head、通过自身exact-head结构探针及MQ密钥预检；已经 MQ-enabled 的发布不得回落 messaging-disabled。禁止删除消息表、mq_owned 或原回执来绕过归属限制。

## 运行范围

一个 qs-ai 容器，上限 1 GiB / 2 CPU，UID 10001、只读根文件系统和有界日志。HTTP 映射 127.0.0.1:18080，gRPC 仅容器网络可达。qs-ai-grpc / qs-ai-api 为网络别名。短维护窗口内先停止旧准入并排空全部旧进程，再启动新服务；回滚也先停止新服务，不允许执行器重叠。停止宽限 210 秒，MQ 发布器保留至业务回执和失败移交完成。

操作语义与证据分层见 [部署迁移与兼容回滚](../../docs/04-接口与运维/05-部署迁移与兼容回滚.md)。历史发布记录从 [文档总入口](../../docs/README.md)进入，不能把历史健康/验收刷新为当前状态。

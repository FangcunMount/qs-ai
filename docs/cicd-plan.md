# qs-ai CI/CD 建设方案

状态：设计基线，待实施。核对日期：2026-09-11。

建议沿用 qs-server 的基础设施链路：GitHub 托管 runner 检查和构建镜像，ACR 保存镜像，qlume 组 Mac mini runner 导出、上传并部署到 serverA。qs-ai 保持独立的工作流、数据库迁移、发布记录和回滚入口。

本次仅整理方案，未修改部署脚本、创建生产服务或验证新建云数据库连接。

## 事实依据与差距

qs-ai 源码核对基于 5a8bd6f475a90d73028a953262d7075f91638596；qs-server 参考 db3fa972e21a967493f867a203f20d6e58f0cf14。

| 项目 | 当前事实 | 建议 |
| --- | --- | --- |
| CI | [ci.yml](../.github/workflows/ci.yml) 已有锁定依赖、Ruff、mypy、协议生成检查、文档检查、MySQL 8.4、Alembic、pytest、打包检查 | 保留；增加容器启动检查和固定版本的跨项目联调 |
| CD 入口 | [deploy.yml](../.github/workflows/deploy.yml) 仅手动触发，检查当前 SHA 的 CI | 首次手动验证；之后支持 main CI 成功触发，以及指定已验证版本重发 |
| runner | qs-ai 默认 Linux/X64；QS 实际使用 qlume 组 macOS/ARM64 | 对齐 QS，确认该组允许 qs-ai 使用 |
| 镜像 | [deploy.py](../deploy/serverA/deploy.py) 把源码上传到 serverA 后构建 | 在 GitHub 构建 linux/amd64，发布到 ACR，由部署 runner 导出并上传 |
| 数据库凭据 | 脚本仅接收 QS_AI_DATABASE_URL；用户已经创建分项 MYSQL Secrets | 在部署边界将分项凭据转换为应用连接配置 |
| 迁移 | 当前已单独执行 Alembic upgrade head | 保留一次性迁移步骤，增加版本、并发及失败验收 |
| 健康检查 | [health.py](../src/qs_ai/transport/http/health.py) 的 readyz 仅检查数据库连接 | 增加部署版本和迁移版本核对；后续单独验收执行与回传 |
| 回滚 | 当前无完整应用版本回滚入口，临时配置执行后删除 | 保存受限运行配置和发布记录，支持上一版本应用重建 |

QS 参考入口：[cd.yml](https://github.com/FangcunMount/qs-server/blob/db3fa972e21a967493f867a203f20d6e58f0cf14/.github/workflows/cd.yml)、[上传与执行](https://github.com/FangcunMount/qs-server/blob/db3fa972e21a967493f867a203f20d6e58f0cf14/scripts/cd/runner-upload-and-deploy.sh)、[远端部署](https://github.com/FangcunMount/qs-server/blob/db3fa972e21a967493f867a203f20d6e58f0cf14/scripts/cd/remote-deploy.sh)。QS 已有配置备份和就绪检查，不将这些等同于完整的应用自动回滚。

## Secrets 与配置分工

已通过 GitHub 元数据确认 qs-ai 可见下列组织凭据；只读取名称与更新时间，不读取值。

| 来源 | 名称 | 用途 |
| --- | --- | --- |
| 组织 Secrets | MYSQL_HOST、MYSQL_PORT | 数据库连接目标 |
| 仓库 Secrets | MYSQL_DATABASE、MYSQL_DBNAME | 数据库名，前者为标准名称，后者兼容 |
| 仓库 Secrets | MYSQL_USERNAME、MYSQL_PASSWORD | qs-ai 数据库账户 |
| 组织 Secrets | ALIYUN_ACR_REGISTRY、ALIYUN_ACR_NAMESPACE、ALIYUN_ACR_USERNAME、ALIYUN_ACR_PASSWORD | 镜像仓库 |
| 组织 Secrets | SVRA_HOST、SVRA_USERNAME、SVRA_SSH_PORT | 部署目标 |
| 组织 Secrets | SVR_MINI_SSH_KEY、SVRA_SSH_KEY | 部署密钥，按 QS 的优先顺序选用 |

不要求再维护一份 QS_AI_DATABASE_URL Secret。部署配置渲染器用 SQLAlchemy URL 构造方式处理用户名、密码中的特殊字符，再向应用注入现有 QS_AI_DATABASE_URL。两个数据库名均非空但不一致时明确失败，只提示字段名。

[configs/](../configs/README.md) 继续保存非敏感默认值与环境差异；凭据仅由 Actions 注入。生产运行配置需支持重建容器和回滚，建议在 serverA 受限目录保存，文件权限 0600，避免进入镜像、普通构建产物和日志。配置版本记录只含标识与摘要，不包含凭据。

serverA 主机身份继续严格核验；现有脚本所需 SVRA_KNOWN_HOSTS 当前不在已查询到的 Secret 列表内。实施时可使用经过核对的 runner known_hosts 或配置主机公钥，不能仅为减少配置而关闭校验。sudo 优先沿用已经验证的最小免密命令集合。

当前仓库未查询到 production Environment；组织 runner 组查询返回权限不足，因此组访问权限尚未确认。实施时配置 production 环境及 main 发布限制，不需要为了读取组织配置扩大当前账号权限。

新建数据库的实际 MySQL 版本、连接策略和账户权限尚未验证。当前 CI 使用 8.4，任务领取依赖 SKIP LOCKED；首次部署需核对云实例兼容性，并使 CI 覆盖生产使用的主版本。共享实例时仍保持 qs-ai 独立数据库及账户，按 API、迁移与后续 worker 的总量核对连接预算。

## 目标发布链路

1. PR 和 main 的 CI 使用临时 MySQL，不连接生产库、不调用付费模型。保留当前检查，跨项目联调固定 qs-server 提交，使用独立测试库和测试证书。
2. CD 只接受通过 CI 的确切提交。自动入口限定本仓库 main；手动重发也校验提交来源及检查结果。并发发布串行，不中途取消正在执行的迁移。
3. GitHub runner 构建一次 linux/amd64 镜像，记录提交、镜像 digest 和架构。ACR 为部署来源，可同时发布 GHCR；GHCR 优先使用工作流 GITHUB_TOKEN。首次不要求增加 Docker Hub 发布。
4. Mac mini 使用隔离 Docker 配置读取 ACR，导出镜像、生成发布清单并上传 serverA。校验上传文件摘要和镜像身份，目标机不再安装依赖或构建代码。
5. 在与服务相同的网络和配置下检查 MySQL 连通性与迁移状态，使用目标镜像单独执行 Alembic。迁移失败即停止，不替换旧服务。
6. 更新 Compose 服务，等待健康和数据库就绪；核对镜像版本及 Alembic revision。全部通过后，才将该版本记录为成功版本。
7. 失败时保留脱敏诊断和发布阶段。若数据库变更与旧应用兼容，恢复上一成功镜像及对应配置；不自动执行 Alembic downgrade。首次部署没有上一版本，应明确报告首次部署失败。

MySQL DDL 不能假设整体可事务回滚。迁移采用向前兼容的增量方式，破坏性字段变更拆批；发生部分迁移失败时先检查实际 schema，不能仅凭 revision 推断安全。发布记录同时保留迁移前后版本。

## 首批部署与后续运行角色

首批沿用 [compose.yaml](../deploy/serverA/compose.yaml)：一个 API 容器、独立 MySQL 库、infra-network、127.0.0.1:18080。保留非 root、只读文件系统、资源和日志限制。

当前 [worker.py](../src/qs_ai/bootstrap/worker.py) 默认执行探针后退出，--once 只尝试一个任务。常驻 worker、gRPC 接收和回传进程的运维策略要随执行链路一起补齐，不能通过重复重启探针来模拟常驻 worker。

后续各角色复用同一应用镜像，以不同入口启动。启用跨服务链路前，需要补齐 mTLS 配置、QS 目标地址、真实授权和事实读取、执行与投递观测，并验收 QS 发起到可靠接收。API/数据库就绪不代表模型生成或业务解读已经可用。

## 实施顺序与验收

| 批次 | 改动 | 完成证据 |
| --- | --- | --- |
| 1：发布设施 | 改造现有工作流与 deploy/serverA 脚本；接入 MYSQL 分项配置、ACR、qlume runner；增加镜像检查 | CI、凭据缺失及冲突检查、特殊字符配置测试、镜像非 root 启动通过 |
| 2：serverA 首发 | 配置生产环境和主机信任；迁移、启动、版本记录、回滚入口 | 容器内连接新库、迁移版本正确、API 就绪、线上 SHA 正确；有上一成功版本后演练应用回滚 |
| 3：执行链路 | 常驻 worker、gRPC 与回传运行策略、证书和依赖检查 | 受控业务冒烟及重复投递、重启恢复验收，独立记录模型可用性 |

改造现有 .github/workflows 与 deploy/serverA 即可，暂不引入共享 Actions 仓库、Kubernetes 或新的发布平台。仅当多个项目的公共步骤稳定后，再考虑抽取可复用工作流。

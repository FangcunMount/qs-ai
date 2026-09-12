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


## gRPC 通信部署

新增 `qs-ai-grpc` 常驻进程，监听 `0.0.0.0:50061`，仅在 `infra-network` 内以 `qs-ai-grpc:50061` 访问，不发布宿主机端口。API 与 gRPC 使用同一不可变镜像和数据库配置。

三个独立只读挂载：

| serverA 源文件 | 容器路径 |
| --- | --- |
| `/data/infra/ssl/grpc/ca/ca-chain.crt` | `/run/qs-ai-tls/ca-chain.crt` |
| `/data/infra/ssl/grpc/server/qs-ai-fullchain.crt` | `/run/qs-ai-tls/qs-ai-fullchain.crt` |
| `/data/infra/ssl/grpc/server/qs-ai.key` | `/run/qs-ai-tls/qs-ai.key` |

私钥由 infra 签发并保存在服务器，不经 CI 传递；没有挂载 CA 私钥。部署先以容器实际 UID 检查证书可读及证书/私钥匹配，再迁移和启动。每个运行容器都核对镜像 ID，回滚到单 API 版本时移除新增 gRPC 容器。

健康探针使用 AI 证书建立 mTLS，要求业务入口返回 `PERMISSION_DENIED`。独立验收使用 QS 证书发送无效 Change，要求 `INVALID_ARGUMENT`，确认身份通过且不执行业务事务；无客户端证书要求连接拒绝。探针入口为 `python -m qs_ai.bootstrap.grpc_probe`，支持 `--address`、`--ca`、`--cert`、`--key`、`--anonymous` 和 `--expect`。

本次启动通信接收进程；常驻任务执行、结果投递及 QS 真实业务切换仍需后续部署和业务验收。

## M1 适配代码发布（2026-09-11）

AI PR #6 合并后的提交 `0cf33ae18ecff1bb2b575ba5538f8cacba5a236a` 已通过 [main CI 34614854646](https://github.com/FangcunMount/qs-ai/actions/runs/34614854646)，[部署 34615103573](https://github.com/FangcunMount/qs-ai/actions/runs/34615103573) 第二次尝试成功。第一次在镜像拉取阶段失败，尚未替换生产；未确定其具体根因，不能将重试成功视为已修复根因。

随后通过 serverA SSH 独立读取容器列表：qs-ai-api 与 qs-ai-grpc 均使用 `qs-ai:0cf33ae18ecff1bb2b575ba5538f8cacba5a236a` 且 healthy。此轮只证明适配代码镜像及进程健康，不证明真实报告授权、模型调用或结果展示已验收。PR #9 的调用记录及协调器尚未包含在该生产版本中。

QS [部署 34615668017](https://github.com/FangcunMount/qs-server/actions/runs/34615668017) 的服务替换步骤完成后，SSH 独立核验 qs-apiserver、qs-collection-server-1、qs-collection-server-2 均运行 `87edccd9a7a0f353645216c6ba31c8b621db581f` 且 healthy；当时发布后治理校验仍在运行，未据此宣称整个 CD 完成。

从生产 qs-ai-grpc 容器内，以 `/run/qs-ai-tls` 的 CA、AI 证书链和私钥建立 `qs-apiserver:9090` 安全通道，调用 `AIWorkflowAccessService.Authorize`，发送空 `AIWorkflowAccessRequest`，8 秒截止时间内收到 `INVALID_ARGUMENT`。该请求不创建业务数据；它证明生产 AI 服务身份被 QS 接受且新接口已到达请求校验。它不证明真实 Testee 权限、IAM 撤权或合法报告路径，后续 M1 必须补这些业务场景。

## M2 执行启用与回退门槛

迁移 0005/0006 增加调用记录和成果表，旧 0004 镜像的 database_check 不认识新 head，不能直接作为升级后的回退目标。保持现有精确迁移版本检查，不放宽它，也不降级或删除业务表。

1. 首先发布本批完整镜像，QS_AI_EXECUTION_ENABLED 保持 false；验证迁移 head、API/gRPC 镜像及健康，将该发布保存为执行启用前的基线。
2. 完成 M1 真实案例、权限与凭据配置后，用同一完整提交再次部署并启用 worker/delivery。新旧发布使用同一迁移 head，前一发布的 runtime.json 仅包含 API/gRPC。
3. 如启用失败，回退到上述基线；Compose --remove-orphans 停止 worker/delivery，保留新 API/gRPC、数据库中的任务、模型调用及成果，不重新打开 QS 旧生成路线。停止期间的新任务保留排队，恢复后仍遵守未知调用不得盲目重发。
4. 执行启用前确认 state.current 是已验证且关闭执行的兼容基线；启用成功后再确认 state.previous 指向该基线。不能仅凭时间先后推断。实际回退演练仍待执行，当前文档不是回退成功证据。

## 自动部署触发范围

自动部署将目标 main 版本与最近的生产成功回执比较，识别运行代码、配置、迁移、集成资产、镜像/依赖和部署脚本变化；没有运行差异的文档、测试或 CI 配置提交不会重启生产。这样即使后来的文档提交替换排队任务，也不会漏发之前尚未部署的代码。删除/移出运行目录同样算运行变化。

服务器完成健康、镜像及数据库校验后写入的 state 是回执来源，回执只含实际 revision 和 release 标识。CI 成功后上传 `qs-ai-deployed-<实际 SHA>-<run>-<attempt>` artifact，自动 gate 只读取同仓库 main 的未过期标记并按时间选最新。回退记录实际回退版本，而不是工作流源码 SHA。排队旧版本不得自动回退已更新的生产。

首次启用尚无回执、或标记超出保留窗口时，自动任务建立一次完整基线，不猜测未知生产差异。手动 deploy/rollback 保留显式行为与 main 祖先、精确版本 CI 门槛；范围判断不能替代运行和业务验收。回执上传失败时部署可能已完成，应先核对服务器再处理 CI。

## 2026-09-12 发布 runner 故障与暂停

AI 自动部署 `34622452715` 失败；GitHub check annotation 明确为 runner5 `_diag` 日志写入触发 `System.IO.IOException: No space left on device`，不是业务验收失败。QS 部署 `34620764830` 此前也在 runner3 导出原始 tar 时报磁盘不足。QS PR #85 的流式导出已合并，但不能修复已满的共享磁盘。

为避免继续堆积失败发布，qs-ai Repository Variable `AUTO_DEPLOY_ENABLED` 已从 true 临时设为 false，普通 CI 未关闭。新的运行 `34625252116` 仍显示 deploy in_progress 且无步骤信息；没有把观察缺失当成已终止，也没有重复发起或中断可能在操作服务器的任务。

恢复前：取得 runner SSH 连接、检查占用与运行任务，只清理已核实归属且可重建的本任务临时产物；再次核对 serverA 实际版本和服务健康。恢复后先完成一次明确版本发布与回执验证，再将 AUTO_DEPLOY_ENABLED 恢复 true。真实报告、授权/撤权及生成业务门槛仍独立验收。

镜像打包也改为 `docker save` 到 gzip 的流式传输，不再额外保留完整未压缩 tar。父进程分别确认两个子进程退出状态，再校验 gzip 和 tar，最后原子替换交付包；失败或超时终止并回收子进程、清理本次临时文件、保留已有完整包。这降低峰值磁盘占用，但不能替代对已满 runner 磁盘的实际修复。

## 2026-09-12 Runner 恢复与评测基础发布

Mac mini 已通过 SSH 连接；清理前数据卷可用约 88 GiB。经核对六份 Runner 升级暂存包与已安装二进制一致、超过 14 天且无进程使用，删除这些暂存包和 563 份超过 14 天的日志；没有删除项目、容器镜像、数据库或近期日志。Runner 目录约 23→20 GiB，文件系统实际新增可用空间约 381 MiB，最终约 89 GiB；六个 Listener 仍运行。目录统计下降不能等同物理空间回收量。

[主分支 CI 34699359651](https://github.com/FangcunMount/qs-ai/actions/runs/34699359651) 通过后，明确指定 `177f071b4d0a59cc68df799ca70b44e4e89824c2` 执行 [发布 34703453948](https://github.com/FangcunMount/qs-ai/actions/runs/34703453948)，结果 success。独立 SSH 核验：

- `qs-ai-api`、`qs-ai-grpc` 镜像均为该 SHA 且 healthy；readyz 为 ready/database connected。
- 容器虚拟环境的 database_check 核验 MySQL 8.0.36，当前及镜像预期均为 `0016_semantic_completions`。
- mTLS 探针以 AI 证书调用仅允许 QS 的入口，按预期得到 PERMISSION_DENIED；此为工作负载隔离探针，不代表真实业务授权。
- generation/evaluation/grpc.governance_enabled 均为 false，没有启动 worker、delivery 或 evaluation；QS 三个业务容器仍为 `b191a6bb72fe348072e2720beb31fb40aa184dd0` 且 healthy。
- 已下载并校验本次 deployment-receipt：revision 为上述 SHA，release 为 `177f071b4d0a59cc68df799ca70b44e4e89824c2-34703453948-1`。随后恢复 AUTO_DEPLOY_ENABLED=true；纯文档范围过滤仍适用。

此次完成此前积压的资产、评测及管理基础镜像发布；草稿 PR #39 的人工审核及候选查询不包含在该生产镜像中。M1 真实报告及撤权、M2 模型闭环与回退演练、M3 管理业务验收仍未完成。

## 2026-09-13 最终评审版本部署核验

AI 主分支 `9b19e6ce0ff001c7df283cade6bfd9574b224142` 的 [CI 34709398643](https://github.com/FangcunMount/qs-ai/actions/runs/34709398643) 与 [部署 34709815870](https://github.com/FangcunMount/qs-ai/actions/runs/34709815870) 均成功。独立 SSH 检查确认 qs-ai-api / qs-ai-grpc 使用该镜像且 healthy，MySQL 8.0.36 当前/预期迁移头均为 `0016_semantic_completions`，readyz 为 ready/database connected，gRPC 自身份探针取得预期 PERMISSION_DENIED。该探针证明通信和身份拒绝，不是授权主体业务验收。

QS 的 [CI 34709501962](https://github.com/FangcunMount/qs-server/actions/runs/34709501962) 与 [部署 34710122753](https://github.com/FangcunMount/qs-server/actions/runs/34710122753) 成功；现场 apiserver 及两个 collection 的 SHA 为 `a35a84e3f8a046154fda5050e37d54a45bfe172b`，均 healthy。本次只核实已合并最终评审版，没有部署本轮重开增量、开启治理/生成或宣称 M1–M5 真实验收通过。


## 2026-09-13 配置发布事务基础版部署核验

AI `6b803821e4e20abd93a757aaa4995b36bf683a30` 的 [主分支 CI 34714055083](https://github.com/FangcunMount/qs-ai/actions/runs/34714055083) 和 [部署 34714737930](https://github.com/FangcunMount/qs-ai/actions/runs/34714737930) 均成功。独立 SSH 确认 API/gRPC 使用该镜像且 healthy，MySQL 8.0.36 当前及预期迁移头为 `0017_configuration_publications`；readyz 为 ready/database connected，mTLS 自身份探针取得预期 PERMISSION_DENIED。

只检查三个布尔值确认 generation/evaluation/governance 均为 false，未输出凭据。QS apiserver 与两份 collection 仍为 `01fb6bb98f4bca4027e485874799d334ff21b579` 且 healthy。本次部署包含发布持久化基础，不包含未合并的管理 RPC/REST 增量；真实授权、生成、管理入口及恢复验收仍未完成。

## 2026-09-13 草稿持久化版本核验

自动部署 [34720405500](https://github.com/FangcunMount/qs-ai/actions/runs/34720405500) 成功，独立 SSH 确认 API/gRPC 实际镜像为 `6999edc61ac0c03796809789ba8fd633c8a3f8bf`，两者 healthy。该源码的主分支 CI 34719615120 成功；部署运行自身的 workflow head `9880f14b0e1ba9b5b81e81b4881607a2032eb667` 不代表交付镜像版本。

MySQL 8.0.36 当前与预期迁移头均为 `0019_prompt_drafts`，readyz 返回 ready/database connected，自身份 mTLS 探针按预期 PERMISSION_DENIED。generation、evaluation、grpc.governance_enabled、generation.use_publications 均为 false。QS serverA 三个容器仍为 `3e297f525df0b6434506472bf8a0a15246ab46f4` 且 healthy。本次不包含原生冻结 0020、QS 冻结代理、真实管理页面或业务生成验收。

## 2026-09-13 QS 草稿代理部署核验

QS 主分支 CI 34719983543 与 Production Deploy 34720568215 完成成功；独立 SSH 确認 serverA apiserver、两个 collection 实际镜像为 `4c3bd154b2b1aef0fbab0cf2d8683491c129b0ab` 且 healthy。未由这三个容器推断另一主机 worker 镜像。

AI 部署 34720860768 显示成功，但本次现场 API/gRPC 实际仍为 `6999edc61ac0c03796809789ba8fd633c8a3f8bf`，两者 healthy；MySQL 8.0.36 迁移头 0019、readyz connected 与 mTLS 自身份拒绝探针均通过。尚无原生冻结 0020 或评测资产快照增量的生产镜像证据；管理/业务验收保持独立。

## 2026-09-13 原生 Prompt 冻结版本部署核验

AI 主分支 CI 34720921863 与 [部署 34721663418](https://github.com/FangcunMount/qs-ai/actions/runs/34721663418) 成功。独立 SSH 确认 API/gRPC 实际镜像均为 `0d68334b24929373eb76f0e834895b7cff1e8bd5` 且 healthy；MySQL 8.0.36 当前/预期头均为 `0020_prompt_freezes`，readyz 为 ready/database connected，自身份 mTLS 探针取得预期 PERMISSION_DENIED。

generation、evaluation、grpc.governance_enabled、generation.use_publications 均为 false。QS serverA 三个容器仍为 `4c3bd154b2b1aef0fbab0cf2d8683491c129b0ab` 且 healthy。本次上线包含原生 Prompt 冻结基础，不包含本地 Profile 注册 0021 或待合并的评测资产快照增量，也不代表真实管理或生产生成验收。


## 2026-09-13 Profile 注册发布

Profile 注册主分支 `e836d32809ede4d162d68192a1a3e19ebe25561f` 的 [CI 34722810739](https://github.com/FangcunMount/qs-ai/actions/runs/34722810739) 和 [部署 34723438772](https://github.com/FangcunMount/qs-ai/actions/runs/34723438772) 均成功。独立 SSH 核验：

- API/gRPC 实际镜像同为 `e836d32809ede4d162d68192a1a3e19ebe25561f`，均 healthy。
- MySQL 8.0.36 当前/镜像预期迁移头均为 `0021_profile_registrations`；readyz 为 ready/database connected。
- mTLS 自身份拒绝探针符合预期 PERMISSION_DENIED；这不代替真实 QS 操作者授权。
- generation.enabled、evaluation.enabled、grpc.governance_enabled、generation.use_publications 均为 false；未启用生成、评测或管理写入流量。
- QS serverA apiserver/两个 collection 均为 `53e2f2cb1acabcb6a171372b0dd8bcb7bbbf3f7a` 且 healthy，对应 [部署 34722157688](https://github.com/FangcunMount/qs-server/actions/runs/34722157688)。没有核验另一主机 worker，也未将本轮合并的 QS Profile 代理视为已部署。

本版本包含 Profile 注册及评测资产快照执行，不包含本地 `0022_evaluation_suites` 原生套件增量。期间先观察到资产快照版 `8452d91e2cfb5819f16b197b154ec24cfbb5726f` 和数据库头 0020，随后上述部署完成才再次核对实际 Profile 镜像与 0021；未将工作流 head 直接视作镜像 SHA。M1–M5 真实业务与管理页面验收保持未完成。

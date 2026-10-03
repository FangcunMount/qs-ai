# M7 Python SDK 外围接入候选

本批基于 qs-ai `845bba412321d0a0348ec9f445354c00457f933d`，仅接入结果通知机制。
原多供应商／候选并发／恢复的完整业务验收版本仍待原会话确认；本批不代签通过。
M5～M6 Go 验收归原会话，不修改其夹具、生产配置、部署暂停或总进度文档。

## 实际替换

`DeliverResults.once` 保留原顺序扫描、上下文、关联字段及持久化后诊断日志；
接收异常→重排原 event_id、接收成功→确认原 event_id 的算法改用 SDK deliver_durable。
`MySQLResultOutbox` 删除 pending/delivered/retry SQL 副本，委托 MySQLPendingOutbox。
不设置新旧选择开关，不运行第二套投递路径或后台 Relay。

原 `stage_state` 仍由业务事务传入 db，构建原 StateEvent、JSON、原 UUID 和
session_id/version 唯一约束；只把 INSERT 交给 SDK 原事务 appender。
重存仍 no-op，保留首次 event_id/payload/时间。宿主显式开始、提交结算事务；SDK
不另开 staging 事务，不关闭 Database 或 gRPC channel。UTC_TIMESTAMP(6) 不变，
宿主运行时仍 UTC+8。

GRPCResultReceiver 与 StateEvent protobuf 零变更。只有匹配原 event_id 的业务
持久接收回执可完成 delivered；broker confirm 或未知调用不构成业务回执。
没有新增 NSQ、消息服务、数据库、表迁移或模型执行。

## 边界与验证

执行恢复相关文件只读：Job/Run、模型路由、LangChain/LangGraph/checkpoint、
execution/evaluation/interpretation、ModelCapacity、冻结配置、业务恢复授权及
单进程 bootstrap 装配均未修改。未启用 candidate_mode_enabled，未发起真实模型调用。

本地：82 项定向单元与 43 项存储回归通过，无跳过。存储场包括实际 protobuf/gRPC
到临时持久回执表、丢回执/错 event_id/同原 wire 重投、原事务回滚、冻结配置读回、
未知调用不重发、持久响应复用、取消和原进程中断恢复。全部使用合成资产和模型替身。
本地存储为隔离原生 MySQL 9.3，不能冒充 MySQL 8.0/8.4 证明；目标版本另由 M7
独立 CI 验证。gRPC 场使用 loopback 测试服务，并非真实 qs-server 投影、生产 mTLS
或真实参与者业务验收。现有 Go 互操作验证留给对应责任会话。

首次 Docker 测试因磁盘空间不足失败，未算通过；共享本地实例自行恢复后，本批
测试库与账号已删除。没有清理其它 Docker 资源，后续改为独立原生测试进程。

## 依赖与受控发布

候选依赖为 `fangcun-reliable-messaging==0.1.0a1`，uv.sources 和 lock 固定 SDK
`50c94db0aad89ef6d3ed9a454b1ad31861aa363d` 的 python 子目录，不使用工作树路径。
这是审查/CI 用的不可变 Git pin，Python 版本尚未正式发布。原生产 Dockerfile
没有增加 Git 工具，不宣称这份 Git 依赖候选已是可部署生产镜像。

审核顺序：核对 SDK 精确源码/22 项测试/目标版本 CI/独立 wheel 摘要 → 用户授权
正式 Python 发布 → 用 approved wheel/index 版本替换临时 Git source → 核验 lock、
正常 Docker 构建与单 PID、原配置/挂载/停止预算 → 用户审核具体部署与回退材料。

保留原镜像与配置。停止旧单进程准入并排空后才启动新实例；借用 pool/channel
在后台停止后由宿主关闭。旧 result_outbox schema/wire 不变，回退旧镜像继续扫描
原未完成记录，无双 writer、无清空或重建已接单任务。已接收但本地确认未知的
记录沿原 event_id 重投，通过持久接收端幂等恢复。正式发布和生产部署均未执行。

## 已确认的实施与关闭门槛

本批按 M7 行为保持重构实施，不扩大为 NSQ 接入。Python 首发采用 GitHub Release
`python/v0.1.0a1`（修正早期审核材料的 python-v 临时写法），保留独立 Python 版本。
正式发布批准后，删除 Git source，锁定该版本的 wheel URL 和 SHA256；不得引用
latest、分支或工作树。GitHub 资产不保证技术上不可替换，因此禁止覆盖并通过
lock hash 检测变化。包索引与新许可证不在本批。

M7 CI 对主线/候选分支和 PR 的外围代码、依赖、lock、M7 测试及文档变更运行。
报告须非空、有实际 test case 且 failures/errors/skipped 均为零，不能利用空报告
或跳过缺依赖通过门禁。独立 Python wheel 安装和最终资产/source CI 绑定由 SDK
workflow 负责；最终宿主变更后须取得本身的精确头 CI，不借用旧头结果。

| 项目 | 必须保留的验收证据 |
| --- | --- |
| M7-01 | 原 qs-ai 负责人确认承载 SHA、执行恢复状态和文件归属；未验收项仍开放 |
| M7-02/03 | 原事务原子性、真实非 AUTOCOMMIT、取消/失效绑定、借用资源可用；现有黄金契约和原 UTC+8/存储时钟 |
| M7-04 | 同一结果链路使用 SDK；原 protobuf/gRPC 回执与首次 event_id/payload/时间不变，无双 scheduler |
| M7-05 | 模型 dispatch 未知时新增调用为0；持久响应复用、冻结配置、取消终态、容量规则通过定向回归 |
| M7-06 | 正式发布、正常 amd64 镜像、隔离旧→新→旧切换、真实接收端 mTLS 双库联调、生产原消息对账 |

隔离演练只使用一次性独立存储和合成输入，不开启候选模式或发起真实模型调用。
真实 qs-server 联调须使用原负责人提供的固定构建，不改 Go 夹具，不跑 M0～M6
验收。模型执行与恢复代码仍由原会话负责；fixture存在、历史文档、空gRPC探针、
镜像健康及没有业务流量都不能代替实际业务验收。

生产 `AUTO_DEPLOY_ENABLED` 当前为 true；合并主线可能触发自动部署。候选验证
期间不提前合并，不修改部署/暂停开关。合并和部署一起审核，审核包绑定候选
源码树、最终CI、正式SDK资产、镜像、现行配置/schema/挂载/停止预算及旧镜像。
合并树超出批准范围或原负责人尚未确认承载基线时不得继续部署。

隔离证明旧镜像能够读取新路径留下的原未完成记录后，才准备生产切换。沿原
停止顺序排空旧实例，再启动新单容器/单Python进程，不让新旧实例同时写入。
schema、冻结绑定、配置/候选/容量值不变，不清空Outbox或重建Job/Run。
上线观察至少两个现有最大重试周期，需一条既有或自然到达的结果完成
原event_id、qs-server持久接收和qs-ai delivered对账；无样本保持业务验收开放。
故障注入仅在隔离环境；生产发现身份漂移、丢任务、未知调用重发或投递持续失败
时，先停新实例再按已审核的schema兼容条件恢复旧镜像和配置，保留全部数据。

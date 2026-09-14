# M5 退役执行清单

2026-09-13 用户批准：调试闭环通过即进入 M5，不等 24 小时；删除旧 AI 数据，保留新链路，专项备份保留 7 天；未用于生产的初期样板一并清理。本清单优先于历史记录中的旧数据保留和 24h 条款。

## 当前门槛

代码可以在独立 `codex/m5-retirement` 分支准备和验证。生产退役必须先具备真实管理、授权生成/展示、撤权、可靠回传及恢复证据。截至本清单建立时，这些证据仍有缺口；不得将准备工作标记为 M5 完成。

当前基线：AI `c56caa0ae0b3a1f0bf982d84d45046f72368a29b` / 0027，QS `bc1a3c4096810ea4a6b691817fd8357c9d7deecd`，Operating `b5d2aee2e6a98efc476df02db2450f73fcc1f05b`。小程序主干 `16bcdc51e2e5d33aca1ae56c1ce0ea9d7c795a26` 不代表微信已发布。当前 v6 指纹为 `sha256:cc747df0a6ae4b02b65b7447ce08845fa8c4e91b9d674f51ac25fbcbffd2a2a2`。

## 代码批次

1. QS 把当前标准报告与评分事实来源迁出旧 AI 目录，保留现有权限和事实一致性校验；新 aibridge 不再依赖旧引擎。
2. qs-ai 新准入强制绑定发布快照，删除固定配置回退、无清单评测回退及旧输入空值分支；保留当前 v6 及 V6_PUBLISHED 校验所需基准，不以新文件内容覆盖已有指纹。
3. 更新 Go→Python 契约测试，取消依赖旧引擎源码的 Prompt 导出/执行比较；保留新接口、投递、权限和 MySQL 8.0/8.4 覆盖。
4. Operating 删除不再挂载的旧五组工作区、复检组件及 aiGovernance 客户端。小程序内部使用 requestId，已有新 UUID 指针只做一次格式升级；保留重复请求、版本、账号和生命周期保护。
5. QS 删除旧领域/应用/模型与 Mongo 仓储、旧 REST/RPC/Worker/调度，移除旧配置和模型凭据注入；新通信配置独立，保留新 URL 和有效输出契约。
6. 删除 qs-ai 独立 HTTP 会话入口、未接入生产的 LangGraph Saver/技术租约和专属探针；先迁出生产仍用的 LeaseLost。保留健康接口、业务执行租约、evaluation_checkpoints 和提交校验。仅在无剩余引用时移除依赖。

## 数据白名单

QS MongoDB 只删除以下旧专属集合：

- ai_explanation_generations
- ai_explanation_runs
- ai_explanation_artifacts
- ai_explanation_profiles
- ai_explanation_prompt_evaluations
- ai_explanation_prompt_evaluation_rechecks
- ai_explanation_prompt_evaluation_daily_budgets
- ai_explanation_participant_daily_budgets
- ai_explanation_participant_active_capacity

AI MySQL：复核正式执行调用链发现 checkpoint_leases 是正在使用的永久 fence 行，生产暂时为空不代表未使用。0028 将其原样改名为 execution_leases，保留 thread_id、fence 和 expires_at；不是 DROP。迁移前必须停止 worker，迁移后只启动新版本，禁止旧 worker 与新结构混跑。降级必须停新 worker 后执行配套逆迁移，再启动旧版本。checkpoint_migrations、checkpoints、checkpoint_blobs、checkpoint_writes 在生产不存在。其他环境存在时必须先确认来源、引用和备份。新的调试数据只能按明确 ID 且无引用的清单清理，不能按年龄、状态或名称前缀批量判断。

必须保留 QS 的 ai_bridge_requests、ai_bridge_commands、ai_bridge_events、标准测评/评分/报告，以及 AI 全部有效资产、发布/审核证据、固定执行配置、任务、模型回执、成果、幂等和投递数据。两端迁移账本不得重写。

共享 outbox、死信、重试保留记录仅匹配以下完整事件名：

- interpretation.ai_explanation.requested
- interpretation.ai_explanation.retry.requested
- interpretation.ai_explanation.lease_recovery.requested
- interpretation.ai_explanation.generated
- interpretation.ai_explanation.failed
- interpretation.ai_explanation.prompt_evaluation.step_requested

assessment-lifecycle 是共享 Topic：不能清空/删除。先停旧生产者和调度，处置在途/未知调用、定向排空旧消息，再移除消费注册。Redis 仅清理确认归属的旧 AI 锁；不得清库或移除共享身份、授权、限流及凭据。

## 执行与完成

先生成对象/数量/引用/备份清单，验证备份恢复并记录校验和、到期时间；按相同清单执行，现场数据或引用变更则停止。敏感正文不得进入 Git/CI 日志。新增退役迁移与普通部署分开；删除后不允许直接回滚到依赖旧表的镜像。

逐项保存清理前后证据，验证冷启动不会重建旧集合，标准报告与新管理、生成、回传、展示仍通过，旧路径不可达。所有代码合并、生产部署、清库及复验通过即完成 M5；备份保留期不增加等待门槛。

## 已准备，尚未生产执行

- QS 报告来源迁到 reportsource，新桥接独立读取；后续删除批次已移除旧引擎、治理与参与者旧接口、Worker/事件/恢复器，通信配置独立为 ai_workflow，移除模型密钥注入。
- Operating 删除未挂载的旧治理实现；245 项相关测试、类型、lint 和生产构建通过。
- 小程序使用 requestId 和 v3 指针，v2 新链路 UUID 经写入回读验证后升级，保留幂等和账号隔离；完整前端校验通过。
- qs-ai 删除独立 HTTP 会话入口和 LangGraph 样板，生产 LeaseLost 迁入 execution.errors；执行租约原样迁名，保留当前模型回执和业务恢复验证。

上述准备不代表管理/生成/微信实机验收，也未删除生产数据。

## 本批验证与审核

2026-09-13，本机独立测试数据库（测试后已移除）：

- 清除框架依赖后，非集成测试 986 项通过，3 项环境跳过。
- MySQL 8.4：完整非 interop 回归 1460 项通过，3 项环境跳过，53 项跨语言测试由 CI 单独执行；新增租约迁移及健康检查另补跑 4 项通过。
- MySQL 8.0.36：空库升级、结构对账及 31 项执行/恢复/成果/重试/有数据迁移测试通过。
- ruff、mypy、两套协议生成、文档校验通过。
- 两个前端草稿 PR 的 CI 通过。QS 文档基线漂移已修正，本地文档门禁通过；QS 和 AI 后续 CI 结果以对应提交为准，不在此预记为通过。

本批草稿：[QS 报告事实迁出](https://github.com/FangcunMount/qs-server/pull/110)、[qs-ai 样板退役](https://github.com/FangcunMount/qs-ai/pull/86)、[Operating 旧工作区删除](https://github.com/FangcunMount/qs-operating-system/pull/35)、[小程序请求身份升级](https://github.com/FangcunMount/qs-collection-system/pull/2)。均未合并发布。

强制发布快照、互操作收敛和清理工具的后续完成情况见本页 2026-09-14 批次。QS 代码退役已在专属分支准备，发布前必须先定向排空六类旧事件。生产阶段另缺真实管理员与授权测评验收入口；未操作生产数据库、开关或备份。

## QS 后续删除批次（2026-09-14）

QS PR 110 已更新至 `af78e8f93611d932bc41f469d08800fe2263f44e`（业务源码 `9a470c83449866fea6f1a0520cc3544144f4020a`）。旧核心源码不再参与编译，新参与者服务保留三条 workflow RPC；旧 REST 返回 404。历史迁移和当前输出契约保留，旧 Prompt 文件暂作为迁移参考，不再编入运行时。

本机验证：全仓 350 个有测试的 Go 包通过；新桥接/来源/授权相关七组包的 race 测试通过；全部 integration-tag 包编译通过；三个服务构建、部署包、actionlint、Tier 1、生成 API 对账和文档门禁通过。当前注册为 14 类事件、100 条 RPC、7 个 apiserver scheduler。远端 CI 已触发，尚未将其记为成功。没有合并、部署或操作生产数据。

qs-ai 的 CI 仍分别固定旧 QS 快照做桥接、成果和管理互操作，部分迁移比对还编译旧 Prompt/验证器。需要把这些旧引擎比对转为原始资产与固定基准验证，并让新协议互操作使用退役后的 QS 版本；不能只删环境变量让测试跳过，也不能把原资产来源 SHA 改成新的仓库 SHA。

## 发布快照、互操作与清库工具批次（2026-09-14）

本批在 `codex/m5-retirement` 准备以下变更：

- QS 新接单必须提供标准报告快照，并在接单事务内绑定发布配置。移除 `generation.use_publications`、部署变量和固定 Profile/模型配置字段；生成、治理、评测三个正式运维开关保留。已绑定任务继续使用原发布快照，暂停/替换发布不改变任务；缺失或损坏配置以及旧工作流明确失败。
- 评测模型执行必须具备完整冻结资产清单；移除从旧文件补齐的分支。输入中的建议引用统一为空数组；不改写旧资产、来源提交、原始字节和审核证据。
- 从旧 QS `1b52081ea42c94dc5653ce91e8c7a1db9f85fc44` 实际运行捕获九组 Go 基准，再删除四个旧源码比对程序和两个导出工具。离线基准固定来源及整份摘要；新 Go→Python 管理、授权、接单与成果投递统一使用已删旧引擎的 QS `1ce436d7ec7c97a2647f3591eeddd9f7cab6aae6`，不再保留三个不同年代的 QS checkout。
- 新增[独立定向清库工具](m5-data-retirement.md)：固定白名单盘点、私有备份、真实隔离恢复、同清单删除和复验。保护 SQL 技术/执行表及所有新业务数据，只处理九个旧 Mongo 集合和六类共享旧消息；未提供调试 ID 清单，不自动删除调试记录。工具不改普通迁移、共享 NSQ/Redis或生产环境。

工具具备内容/结构/引用变化停止、备份摘要核对、恢复失败拒绝删除、重复执行阻止及部分操作日志；删除后备份至少保留七天，过期归档仍可用于恢复。Mongo 集合删除与跨数据库操作无法形成单一事务，真实执行必须有停止写入、定向排空和生产验收证据。

本机已完成 1006 项非集成测试（另 1 项 Go 权限互操作在专门命令运行）；MySQL 8.0 的发布配置、损坏资产及迁移 31 项通过，发布绑定下重试的 6 项通过；维护工具 14 项测试在 MySQL 8.0.36 + Mongo 7.0 通过，初版也在 MySQL 8.4 完成真实恢复与删除验证。完整 MySQL 8.4 回归及当前 QS 互操作的夹具适配按实际运行结果继续记录，不把初次失败写成一次全绿。ruff、mypy、协议、文档、结构对账及 wheel 构建通过。

这完成本批代码与工具准备，不代表生产 M5 已完成。真实管理/授权测评与展示、撤权、回传恢复、微信发布和定向排空仍需逐项实证；本批没有合并、部署或删除生产数据。清库只在真实验收与备份恢复核验通过后执行，无额外 24 小时门槛。

补跑结果：MySQL 8.4 的回归失败均为旧夹具缺少发布/清单，修正后评测完成相关 16 项、发布绑定重试 6 项通过；原已通过的 469 项保留。当前 QS 互操作首次 42 项通过，剩余 3 项与重试 6 项合并补跑全部通过，45 项互操作均获得执行证据、无跳过。清库工具最终 14 项在 MySQL 8.0.36 和 8.4（均配 Mongo 7.0）各通过一次。完整双版本分片仍由本次 PR CI 验证。

清库工具最后补入两种部分恢复失败注入：验证失败会清理自身创建的隔离恢复库，源库和已保存的备份不变。最终维护测试扩充为 16 项，在 MySQL 8.0.36/8.4 + Mongo 7.0 均通过。首轮远端镜像检查和两个维护数据库矩阵已通过；完整应用分片按最终提交继续核对。

最终引用边界复核另补跨数据库外键：其他数据库对待删共享消息存在引用（包括 CASCADE）时，在盘点阶段即拒绝。MySQL 8.4 + Mongo 7.0 的维护测试最终为 17 项全通过；MySQL 8.0 的新增场景继续由最终 CI 覆盖。

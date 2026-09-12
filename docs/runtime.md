# 执行、恢复与资源生命周期

状态：P1 已实现任务、心跳、幂等和会话状态骨架，离线测试验证恢复；正常流程中的真实授权、模型、正式成果仍为设计。详见 [P1 证据](p1-verification.md)。

## 正常执行

```mermaid
sequenceDiagram
    participant U as 用户
    participant A as API/应用
    participant D as MySQL
    participant W as Worker/LangGraph
    participant Q as qs-server
    participant M as 模型
    U->>A: 创建并开始解读
    A->>D: 事务写 Session/Run/Job/幂等回执
    A-->>U: 202 + session_id/run_id
    W->>D: 短事务领取任务与执行令牌
    W->>Q: 按授权读取事实
    W->>D: 冻结证据与步骤身份
    W->>M: 判断信息缺口
    W->>D: 保存步骤输出/检查点
    W->>D: 发布当前问题，结束本次任务
    U->>A: 回答当前 question_id
    A->>D: 事务写回答与恢复任务
    W->>D: 读取回答与匹配的检查点
    W->>M: 综合解读
    W->>D: 校验后事务接受成果
    U->>A: 查询成果
```

没有信息缺口可直接综合。每轮最多一个待回答问题；问题含 gap_code、提问原因和可跳过标识。达到发布策略的轮次/预算上限时停止追问，生成明确限制的结果，或因证据不足进入 blocked，不能无限循环。

## 执行记录与任务

Run 状态为 queued/running/awaiting_answer/succeeded/failed/result_unknown/cancelled。一个 Run 表示一次开始/恢复尝试；Job 为该尝试的投递记录，状态 queued/leased/done/dead。明确未产生外部不确定性的调度重试可更新同一个 Job；重新执行失败业务步骤创建新 Run，记录 parent_run_id，保留旧失败。

P1 任务领取用短事务、按 Session→Job→Lease 的顺序加锁，并用 SKIP LOCKED 避免等待已占用行；候选扫描当前有界为 20 条。一次进程运行至多领取一个任务。普通运行租约默认 30 秒、每 1/3 TTL 续租；这是本地骨架默认值，不是生产负载调优结果。崩溃接管最多 3 次，超限进入 blocked/attempts_exhausted；业务依赖不可用直接 blocked，受控重试接口尚待实现。实际云版本必须验证。领取后提交事务、开始心跳，模型调用不持有行锁。available_at 表达退避。租约长度需大于心跳间隔，并为调度抖动留余量；具体数值在负载测试后冻结。

每次有效领取递增 fence_token。所有业务写入验证 active_run_id、session_version、fence_token；过期 Worker 的结果只能记为未接受回执，不能覆盖当前状态或成果。取消递增 Session 版本并撤销推进权；外部调用不一定能取消，仍记用量，但不能发布迟到成果。

## 外部调用与幂等

每个逻辑步骤生成稳定 step_operation_id，由 session_id、evidence_set 指纹、已接受回答版本、步骤/轮次及 release_id 派生；它跨 Worker 重投递和恢复保持稳定。调用尝试 Invocation 独立编号，不能仅靠新 run_id 去重。

发送前记录 request_fingerprint 和 prepared 状态；即将调用时记录 dispatching，收到回执后 recorded。已记录的有效步骤结果按 operation_id 复用，不重新调用。不同模型/Prompt/输入变更必须形成新操作身份。

网络超时可能已由提供商完成：标记 result_unknown。只有提供商支持且经验证的请求幂等或回执查询才自动对账；否则进入受控恢复，不能盲目补发。SDK 隐式重试需统一关闭或纳入预算/调用身份管理。

步骤回执与调用尝试保存在 [数据模型](domain-data.md) 中的 step_results/model_invocations。候选有效性不因“重试”而放宽。应用预算在发送前原子预留，并在回执后结算；并发调用不能各自读取余额后超额发送。预算预留需有持久化身份并支持对账，不能仅用进程内计数。

## 检查点与业务记录协调

LangGraph 中断恢复会重新执行所在节点，不能把中断前的模型调用或数据库写入当作只执行一次。节点必须先查稳定步骤回执；详见 [官方中断语义](https://docs.langchain.com/oss/python/langgraph/interrupts)。

检查点和业务表不是一个天然事务。采用“步骤结果先落库、检查点推进、业务状态后发布”的可对账协议：

1. 问题内容以 operation_id 幂等存为内部步骤结果；不立即对用户发布。
2. Graph 中断完成并返回后，取得可恢复 checkpoint 身份。
3. 在短事务中记录 checkpoint_ref，发布 question_id，把 Session 改为 awaiting_answer 并结束 Job。
4. 回答用事务写入业务记录和恢复 Job；Worker 从业务记录取答案，不信任客户端传入恢复游标。
5. 最终候选先持久化；应用校验后，以唯一约束和 CAS 接受 Artifact。即使框架再执行完成步骤，也只能返回已接受成果。

| 崩溃窗口 | 恢复规则 |
| --- | --- |
| 业务任务已提交，客户端未收到 202 | 相同幂等键查询/返回原任务 |
| 步骤输出已保存，checkpoint 未推进 | 复用 operation_id 的输出后继续 |
| checkpoint 已暂停，问题未公开 | 恢复器读取匹配检查点与步骤回执，幂等发布 |
| 回答已提交，Worker 未恢复 | Job 仍持久存在，重新领取 |
| 模型已发送，回执未记录 | result_unknown，先对账 |
| Artifact 已接受，任务确认丢失 | 返回已有 Artifact，幂等结束任务 |
| 租约过期后旧 Worker 写回 | fence/version 拒绝，不推进状态 |

**检查点自身也需要防旧 Worker 覆写。** 不能只给业务表加 fence。首版要求持久化适配层支持：检查点写入时校验有效令牌，或使用每尝试隔离的 checkpoint 命名空间并只发布有效引用。选定具体实现前必须验证适配包的能力；仅在写前读一次租约存在竞态，不算验收通过。P0 已选择同事务行锁校验方案：`FencedMySQLSaver` 将 thread_id/fence/数据库时间校验包在 checkpoint 写入事务内，提交前再校验过期；接管更新同一行，旧写不得提交。P0 的独立技术探针只提供领取与释放；P1 Job Store 已在同事务更新 Job 与 checkpoint lease，并提供续租、取消及业务提交令牌检查。所有运行写入必须经此适配器，基础 AsyncMySaver 仅用于管理迁移和测试清理；它不能替代资源授权。云 MySQL 未验证前，仍禁止据此宣称并发接管已可上线。

## 版本

Session 冻结 workflow_version；Run 冻结 release_id、模型路线、Prompt/Schema 版本与证据指纹。新版本默认只服务新会话；旧会话路由旧执行版本，或通过显式迁移/终止策略处理。不能在恢复时静默使用新图。

保留旧版本容器/执行代码直到旧会话排空或按保留策略终止。LangGraph 包升级、checkpoint 包升级、流程节点重命名分别验证。社区 MySQL 适配支持范围以锁定版本为准，基础验证不外推到所有 MySQL 版本。[适配包说明](https://github.com/tjni/langgraph-checkpoint-mysql)

## 资源与授权

API 的 ActorContext 在认证适配后构建；Worker 从持久化主体引用重新取得当前授权，不把短期 access token 存进任务或 checkpoint。访问身份系统/qs-server 的具体委托协议是 P1 联调项。授权服务不可用时阻断新的读取与成果访问，不能把缓存视为永久授权。

HTTP/Worker 使用同一 Dishka 注册定义。每次操作独立作用域，每次事务新 Session，退出释放。模型调用时不占用事务；等待用户时没有活跃 Worker/连接。并行图步骤有独立 UoW。APP Channel/Client 在对应事件循环创建/关闭。

## 容量、观测与交付

首版限制每组织/主体活跃任务、每会话提问轮次、每步骤输出 token、总调用预算与总执行时限。阈值为发布配置，P2 实测确定，不能写成无限默认值。SSE 如加入只传安全进度/已发布问题，未校验的正式解读内容不提前作为成果展示；轮询始终可恢复状态。

关联字段：trace_id、session_id、run_id、operation_id、invocation_id、job_id、release_id。日志默认不包含原始问答、报告、密钥和 Authorization；受控证据库保留必要回执。指标：队列等待、执行耗时、模型用量、unknown 比例、校验失败、问题完成率、恢复失败、过期租约、连接池使用。高基数字段用于日志，不作为无限指标标签。

API/Worker 同镜像不同入口独立扩容。部署：兼容性检查→备份/迁移→部署 Worker/API→基础健康→合成业务验收→逐步开量。回滚依赖兼容 schema 和旧图，不能对有数据的迁移盲目 downgrade。云 MySQL 备份恢复和删除重放在上线前演练。

关联阅读：[领域与表](domain-data.md)、[接口](contracts.md)、[发布计划](roadmap.md)。

## 导入既有 Profile 资产（不发布）

迁移 `0007_profile_assets` 增加不可变定义库。配置已有 MySQL 环境变量并完成迁移后，可使用：

```sh
uv run python -m qs_ai.bootstrap.import_profiles --imported-by <操作人标识>
```

该维护命令只导入仓库已固定的 `published-profile-baseline.json`，使用既有严格解析器校验定义、版本与原指纹，并记录原 QS SHA 和操作人。它没有接收任意 HTTP 上传或绕过管理权限的新入口。导入按版本持久化，失败后可重跑：完全一致的版本不重复写入，同版本异内容拒绝，不覆盖首次导入审计。多个版本中途失败时，先前成功的导入会保留，重跑可继续对账。

`activated: false` 表示只完成资产保留，不能当作评测批准或运行发布。当前 GenerationProvider 继续使用冻结包；发布状态、评测证据门槛、原子激活与管理转发仍在后续批次。数据库升级后使用匹配迁移头的镜像，不将旧迁移头镜像当成自动回退方案。本轮不自动执行生产导入或删除旧资产。

## 导入既有 Prompt 资产（不发布）

迁移 `0008_prompt_assets` 增加不可变 Prompt 包存储。完成迁移并配置数据库后执行：

```sh
uv run python -m qs_ai.bootstrap.import_prompts --imported-by <操作人标识>
```

命令只导入已校验 manifest 的 v1–v6 固定包；原 Prompt fingerprint、GitBlobSHA 和导出 JSON 字节全部保留，另外保存 package_sha256。原指纹并不是导出 JSON 的 SHA-256，不能相互替换。相同 ID/version 的完全相同包可重复导入；异内容报冲突，不覆盖首次来源及操作人。各版本独立提交，部分导入后可重放恢复。

本地隔离 MySQL 8.4 首次导入 6 条、再次 0 条，均为 `activated: false`。生产尚未导入；运行时仍读取冻结包，后续发布流程完成后才能切换资产解析来源。此命令不提供审批、激活或删除能力。

导入后可运行只读对账：`uv run python -m qs_ai.bootstrap.audit_assets`。命令比较固定基线中 1 个 Profile、6 个 Prompt 的完整内容，并核对 Profile 的 Prompt 引用。缺失或不一致返回非零状态；不输出正文，不执行写入或激活。`matched` 只代表该固定基线相符，不代表全部生产资产、route/schema 或发布审批已验收。

## 导入模型路由资产（不发布）

迁移 `0009_route_assets` 保存 route/revision、非秘密定义、原算法指纹与首次导入审计。配置 MySQL 并完成迁移后执行 `uv run python -m qs_ai.bootstrap.import_routes --imported-by <操作人标识>`。固定 v8 首次导入 1 条、重复 0 条，同版异内容报错；不覆盖已有审计。来源标识为先前 QS 运行配置观测，不冒充数据库发布记录。

模型 endpoint 和密钥不允许进入该资产。运行时仍读取冻结包并比对配置；数据库入库不批准、不激活。路由动态发布、审批证据和与 Profile 的完整 release 绑定仍待实现。生产尚未导入 `0009`，升级需使用匹配迁移头的镜像。

路由资产加入后，`bootstrap.audit_assets` 的范围为 `fixed_profile_prompt_route_baseline`：额外比较 1 个 route/revision 的完整定义及指纹，并检查 Profile 的路由名称存在于基线。未导入路由会失败。该检查不选择运行时生效 revision，也不代替包含 revision/fingerprint 的完整发布清单和审批。

## 导入输入、输出规范（不发布）

迁移 `0010_schema_assets` 保存原规范字节、schema_id/version、SHA-256 和首次导入审计。命令为 `uv run python -m qs_ai.bootstrap.import_schemas --imported-by <操作人标识>`。两份 v1 规范从固定 QS 提交提取，先验证文件校验和及 JSON Schema，再插入不可变资产；重复导入不覆盖，同版异内容报冲突。

只读对账范围现为 `fixed_profile_prompt_route_schema_baseline`，包括 1 个 Profile、6 个 Prompt、1 个 route/revision 和 2 份规范，以及 Profile 对输入/输出规范版本的引用。隔离 MySQL 8.4 整套导入后 matched；规范首次插入 2 条、再次 0 条。matched 只证明基线内容与引用相符，已知输入 null/array 契约差异仍存在，不能当作 release 审批或运行验收。生产尚未迁移或导入。

## 构建生成清单（不审批、不激活）

导入所有基线资产后，可只读构建：

```sh
uv run python -m qs_ai.bootstrap.build_manifest \
  --profile-id participant-scale-score-range-default \
  --profile-version v6 --route-revision v8
```

清单绑定 Profile、Prompt、generation route、input schema、output schema 的明确版本、原指纹及内容校验和。缺失、仓库返回身份不符或未知版本失败，不回退 latest；Prompt 包校验和独立绑定，避免同原指纹不同导出内容被视为同一清单。资产 insert-only，因此各次只读查询不会遭遇内容原地替换。

这是 `qs-ai-generation-manifest/v1`，不是 QS 的完整评测 release identity；后者还需用例集、语义评测 Prompt/schema/route、执行策略及门槛策略。命令返回 `approved: false`、`activated: false`，不持久保存发布状态，也不验证已知输入兼容问题。运行时尚未切换为清单驱动。

## Prompt 评测进程（M3 增量，未部署）

`python -m qs_ai.bootstrap.evaluation` 默认只检查数据库，`--once` 推进至多一个评测步骤，`--serve` 常驻轮询。执行模式必须显式设置 `QS_AI_EVALUATION__ENABLED=true`；默认及当前生产保持关闭，不因开启报告生成自动开启评测。评测只处理已由管理入口启动的 collecting Run，不自动启动 requested Run。

循环参数集中在 `configs/default.yaml` 的 `evaluation`：并发、空闲等待、异常退避、退出排空时限和独立健康文件。模型连接复用 `generation.endpoint` 与环境变量 `QS_AI_MODEL_API_KEY`，数据库使用 `QS_AI_DATABASE_URL`；实际模型路由和输出规范来自 Run 冻结身份及已导入资产。启动检查 HTTPS 地址、非空凭据和数据库配置；每轮独立 Dishka 作用域，关闭连接后再进入下一轮。配置检查与数据库连通不能替代资产完整性和真实业务验收。

轮询先接受预检，再推进生成或语义步骤。每轮先扫描至多 64 条 collecting Run 的活动检查点，使用进程内游标跨轮次遍历并在末尾回绕。过期 prepared 经版本校验后释放，过期 dispatching 保存结果未知并阻断；恢复和新发送不在同一轮进行。任何活动检查点都不直接重发。游标不代表执行权，进程重启后重新扫描仍由数据库 CAS 防止重复接受。收到 SIGTERM/SIGINT 后停止领取、限时等待在途工作，超时取消时保留持久检查点。尚未配置生产 Compose/CI 开关，真实进程崩溃恢复验收、未知结果人工处置、管理授权、人工审核及发布门禁仍需继续建设。

### 内部评测管理接口（默认不注册）

`EvaluationManagement.Get` 返回 Run 版本、状态、未知数量与处置审计，`ResolveUnknown` 接受人工决定。候选列表和详情由 `ListCandidates` / `GetCandidate` 提供，`Review` 追加人工审核。`grpc.governance_enabled` 默认 false；在 QS 的治理转发完成并验收前保持关闭。接口只接受 mTLS 认证的 `qs-apiserver.svc` 工作负载；QS 从当前用户授权上下文检查权限，状态/候选读取复用解读审计权限，创建、启动、人工审核和未知结果处置要求 OrgAdmin，再传递组织和操作人 ID。qs-ai 校验 Run 组织、版本与持久证据，不建立自己的用户/角色库。用户不能直接向此接口声明管理员身份。

`PreviewGates` 接受 scope 和显式 expected_version，使用同一 REPEATABLE READ 快照重建 G1–G5：冻结身份与策略、预检和执行账本、恢复授权、质量指标及人工审核。仅允许没有活动执行的 awaiting_review Run；机构不可见返回 NOT_FOUND，旧版本或进行中状态返回 ABORTED。响应包含 Run 版本、发布摘要及 `qs-ai-evaluation-gate-preview/v1` JSON，时间由 AI 服务端生成，大小上限 256 KiB。它不写入最终门槛记录，不批准或发布；后续批准必须在写事务中重新计算，不能接受客户端回传的预览作为授权。QS 转发适配见 [PR #90](https://github.com/FangcunMount/qs-server/pull/90)：`GET /internal/v2/interpretation/ai-workflow/evaluations/{run_id}/gates?expected_version=N`，沿用解读审计权限；合并、部署和真实业务验收分别跟踪，默认治理开关仍保持关闭。

审计 actor 按 QS 现有 `user:<operator_user_id>` 规则生成，处置时间由服务端生成。回读只包含处置审计，不包含报告、Prompt 或模型正文。调用结果不明时先回读核对版本和决定；重复提交旧版本会冲突，不能据网络错误创建新的人工授权。当前仅完成 AI 侧接口与替身鉴权上下文测试，QS 协议同步、OrgAdmin 转发和跨服务真实 mTLS 管理验收待完成。

### 最终评审事务（默认关闭）

`EvaluationManagement.Finalize` 接受同一可信 QS scope、显式 `expected_version`、有存在性检查的布尔 `expected_passed`、原因及 `confirm=true`。时间和 actor 由服务端确定；预期结果仅确认预览，不能指定批准。事务先锁定共享 checkpoint 和 scoped Run，再建立证据快照、重算 G1–G5。未收齐冻结要求的 70 条有效人工评审时不能最终拒绝；结果与确认不同或版本已变时返回 ABORTED。

接受后，在同一事务内保存 approved/rejected、完整门槛、冻结发布摘要、原版本/新版本、操作人/原因/时间及最终状态迁移，并将 checkpoint 版本精确加一。原始生成/语义输出、调用账本和审核历史保持原样。`Get` 增加可选 `finalization_json`，回读时从原始证据重算并验证最终门槛与审计一致，损坏状态不能作为审批证据。配置发布指针及旧管理退役仍待实现；最终批准也不会开启生成或发布配置。

QS 转发路径为 `POST /internal/v2/interpretation/ai-workflow/evaluations/{run_id}/finalize`，要求现有机构 OrgAdmin；只读审计权限不能执行。两端写调用不自动重试，结果未知时先回读。新增字段保持旧的非终态响应兼容。所有生产治理、生成和评测开关继续关闭，真实管理入口验收与发布仍需按 M1–M5 顺序推进。

### 语义复核重开（默认关闭）

`EvaluationManagement.ReopenReview` 接受可信 scope、当前 `expected_version`、理由及 `confirm=true`。仅允许已拒绝且 G3/G5 通过、G4 只因可双人复核的 `failed/default/forbidden_claims_absent` 失败的 Run，最多三轮。基础调用失败、分数不足、确定性失败及人工拒绝不能通过此路径重开。QS 的 `POST /internal/v2/interpretation/ai-workflow/evaluations/{run_id}/reopen-review` 复用 OrgAdmin，操作者来自受保护身份，时间由 AI 生成。

事务先锁定共享 checkpoint 与 Run，重算旧最终门槛，保存上一轮完整审核、门槛、版本和迁移边界后，移出本轮需要重新签名的候选审核，返回 awaiting_review 并加一版本。其他候选签名保持不变，原始模型输出不变，不重新调用模型。当前轮的签名不得早于重开时间；完整双职责审核后须再次 Finalize。

状态新增 `reopenings_json`（QS JSON 为 `review_reopenings`），保存至多三轮、合计不超过 2 MiB 的完整历史。读取状态、候选详情、预览和再次写入时，逐轮以原始候选/调用重算旧门槛，核对保留签名及版本/迁移边界；清空历史而遗留重开迁移也会被拒绝。旧 AI 未返回该字段时 QS 兼容为空数组。超时后先回读，不自动重试重开。该能力尚需精确提交 CI、发布及真实管理验收，不改变 M1–M5 的生产准入要求。

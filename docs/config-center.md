# 配置中心后端契约与实施台账

范围：qs-ai 后端与必要 QS 代理；Operating 由独立会话维护。基线 0f9d7c6；不另建方案或发布状态机。

## 第一批接口

QuotaManagement: Get / Update / History / Rollback / GetReceipt。QS 组织及操作者来自可信上下文，读使用审计权限，写使用组织管理员权限。

字段：participant（daily_org、daily_user、daily_assessment、active_org、active_user、active_assessment），evaluation（daily_provider_calls、max_active_runs），均为正整数。Get 返回 defaults、ceilings、configured、effective、revision、source、constrained_fields。未配置 revision=0/source=deployment_default；损坏或数据库失败不回退。

Update 是完整替换，携带 command_id、expected_revision、reason、values。Rollback 携带 command_id、expected_revision、target_revision、reason，生成新版本；不删除历史。命令同 ID 不同正文拒绝，回执仅原组织原操作者可读。错误：INVALID_ARGUMENT 非法配置，ABORTED 版本冲突，NOT_FOUND 不存在或不属于当前范围，UNAVAILABLE 依赖失败/结果待核对。

额度修改提交后对后续准入/槽位获取生效；每日预留不重复扣减，已有活跃槽位继续运行。UTC 日界线不变。部署默认值与硬上限初始相同，组织只能调整到上限以内。部署降低上限按项限幅并返回 constrained_fields，不改写历史。

锁顺序：配置写先 participant_admission_locks 后 evaluation_admission_locks；读取有效配置复用业务准入事务，不先锁配置指针再获取业务锁。禁止跨请求额度缓存。

## 第二批边界

语义 Prompt、案例可派生并冻结；执行/门槛策略只读。按原身份、版本、摘要导入；运行不回退当前文件。历史证据不重写。接口扩展复用现有方案协议，只增加可选精确引用，省略时继承来源。

## 验收台账

- [x] 在线额度存储、管理接口、准入与槽位读取（代码与隔离验收）
- [x] QS 授权代理与互操作（代码与隔离验收）
- [x] 配置状态脱敏读取（额度来源与生效边界）
- [x] 新增方案读取和准备的文件 I/O 移入线程池
- [ ] 第二批运行时资产读取全部脱离文件
- [ ] 评测资产初始化、草稿及固定版本读取
- [x] 第一批 MySQL 8.0/8.4、并发及历史回归
- [ ] 第二批完整 CI 与发布回归
- [ ] CI、生产部署与运行时核证
- 页面接入、实际人工审核不计为后端自动通过。

## 当前第一批接线

REST 基址 `/internal/v2/interpretation/ai-workflow/quotas`：GET 根路径、GET `/history?before_revision=...`、GET `/commands/{uuid}`、POST `/update`、POST `/rollback`。

写入示例（无需组织或操作者字段）：

```json
{"command_id":"00000000-0000-4000-8000-000000000001","expected_revision":0,"reason":"控制测试期间用量","values":{"participant":{"daily_org":100,"daily_user":5,"daily_assessment":3,"active_org":10,"active_user":2,"active_assessment":1},"evaluation":{"daily_provider_calls":1024,"max_active_runs":1}}}
```

回退只提供 target_revision，不提供 values；历史分页每页至多20项，next_revision=0 表示结束。UTC 日界线不变。预留表 quota_snapshot=NULL 表示迁移前记录，不以新策略回填历史。

本轮新增迁移 0030，不能直接回滚到只认可 0029 的镜像；需使用兼容该结构的修复版本，不删除额度历史。

## 第二批：资产存储准备阶段

迁移 `0031_evaluation_policy_assets` 增加类型化的执行／门槛策略与语义裁判
Prompt 不可变资产。语义输出 Schema 复用现有 Schema 资产表。此阶段不改变
运行时读取、不批准资产、不切换发布，也不删除文件。

受控初始化命令：

```sh
python -m qs_ai.bootstrap.import_evaluation_assets --imported-by <operator-reference>
```

命令保留原始字节、身份、版本、摘要及来源提交；同版本不同正文拒绝，重复导入
相同正文不改写来源。输出只有导入计数与错误分类。数据库凭据沿用部署配置。
语义 Prompt 使用显式所有者：0 表示初始化的共享基准，正整数表示组织资产；
读取组织资产必须同时匹配调用者组织，不按身份猜测或回退其他组织。

本阶段尚未完成：套件／案例的数据库来源、裁判与案例草稿接口、全部执行路径
切换、现有评测与发布引用核对。仅完成上述存储不能视为第二批验收通过。

历史任务策略读取已开始收敛：执行准备、结果提交、未知调用恢复、人工审核、
门槛重算和每日预算预留使用 Run 中冻结的策略正文，并逐项校验发布引用与摘要。
缺少或损坏正文明确失败，不读取当前文件替代。策略投影保留七组、五候选、
预检及未知结果人工确认的强制约束。

本地验证：MySQL 8.4 上上述路径 131 项回归通过；MySQL 8.0 的预算、恢复和
准入 14 项回归通过。资产初始化首次写入四项、重复写入零项。上述是隔离环境
证据，尚不代表第二批生产切换完成。

## 第一批交付状态

- qs-ai #106 已合并，提交 `ef58d1f9a3c0f21568c022a0a66c7e7b8642090c`；PR 与主干检查通过。
- QS #118 全部检查通过，按 qs-ai 先行顺序发布。
- 第二批存储准备与历史策略恢复位于 qs-ai #107，尚未完成第二批交付。
- 页面接入、实际管理员操作及第二批生产资产核对仍单列验收。

## 策略只读目录接口（第二批）

复用 `AssetCatalog.List/Get` 和现有 QS 资产目录代理，新增 `execution_policy`、
`gate_policy` 两种 kind。查询、分页、版本与摘要字段保持原协议。正文是原始
策略 JSON；不提供写入或发布门槛调整接口。读取沿用审计权限，策略类型之间
不能通过同名身份混读。初始化前目录可以为空，按指定版本读取不存在记录则
返回 NOT_FOUND，不回退文件。

## 裁判 Prompt 草稿契约（第二批，尚未发布）

新增 `SemanticPromptDrafts`：Create、Get、Revise、Validate、Freeze、GetReceipt。
读复用审计权限，Create/Revise/Freeze 使用组织管理员权限；组织、操作者由 QS
可信上下文提供。草稿 Get 可指定 revision，0 表示当前草稿版本。资产运行读取
仍必须指定精确版本及摘要，不使用该草稿读取语义。

Create：draft_id、command_id、source_prompt、source_schema（完整 id/version/fingerprint）、
source_owner_organization_id（0=共享基准，或当前组织）、target_version、reason。
Revise：draft_id、command_id、expected_revision、markdown、reason。
Freeze：draft_id、command_id、expected_revision、reason。
Validate：读取指定草稿版本并校验三个文本块、唯一 payload 变量和原输出契约，
不调用模型、不创建评测、不写入审核。

所有写操作原命令重试返回原回执；同命令不同正文冲突。冻结生成组织私有不可变
Prompt，既不审批也不发布，冻结后编辑需从该版本另建草稿。正文／命令上限分别
128 KiB／256 KiB。跨组织和非原操作者的命令回执返回 NOT_FOUND。并发编辑返回
ABORTED；格式错误 INVALID_ARGUMENT；依赖不可用返回 UNAVAILABLE，不回退文件。

## 套件初始化与切换前置条件

新增 `0034_evaluation_suite_sources`：扩展现有套件表，保存精确裁判／策略绑定及
初始化来源。共享原始套件的组织为 0，不伪造管理员、命令或审核回执；既有套件
的原始正文、摘要和登记回执保持不变。裁判草稿迁移已顺延到 `0033`，接在主干
诊断迁移 `0032_runtime_milestones` 之后。

受控执行 `python -m qs_ai.bootstrap.import_evaluation_suites --imported-by <运维标识>`。
在单次事务中核对所有既有 Run 与原始静态契约的对应关系，再导入共享套件并为
已核对的原生套件补齐绑定。出现不一致立即回滚，不以当前版本覆盖历史引用。
初始化前必须已完成四项策略／裁判资产导入；此命令不发起评测、不批准、不发布。

这仍是切换准备阶段。初始化后目录暂时可同时读到字节相同的文件／数据库来源，
只合并完全一致的记录，冲突报错。后续切换必须去掉文件读取；不得据此认为运行时
文件依赖已全部清除。受约束案例编辑、方案可选引用和源套件继承尚待完成。

新评测创建已经按精确引用读取 MySQL 策略与裁判并冻结裁判正文；执行和语义结果
校验读取这些正文。历史缺少裁判正文时只允许按原摘要从资产库补齐，拒绝部分正文
和损坏摘要。当前生产三条 Run 的原策略及裁判引用核对通过，发布记录摘要保持一致。

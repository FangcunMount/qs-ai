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

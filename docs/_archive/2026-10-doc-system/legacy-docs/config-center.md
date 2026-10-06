# 配置中心后端与运行时治理

本文是当前契约与验收状态；历史实施过程见 [归档](archive/config-center-implementation-20260920.md)。
范围为 qs-ai 后端、数据库与 QS 授权代理。Operating 页面由独立工作流维护。

## 交付状态

第一批在线额度、第二批不可变评测资产与固定版本读取已合并发布。
2026-09-20 最近核证：qs-ai `ac4e9ef70151b19764e0b1580722da047b86bea0`，
QS `74f31d47dc8426c53abbb673c8ee9a2e1eab0559`。这是当次核证版本，不代表未来实时状态。
生产初始化检查 3 条历史评测、1 条发布及当前指针；引用与发布内容保持不变。
套件初始化重复执行写入为零，不自动批准或发布。

以下收尾尚未计为生产通过：

- 策略引用查询与完整脱敏配置状态：实现与测试中，详见 [增量契约](config-center-closure.md)。
- 真实管理员额度更新、回退与未发布资产操作：待取得操作回执；登录成功不等于验收通过。
- 页面接入和实际管理员产品操作：独立记录，不以接口自动化替代。

## 权威来源与生效

- 部署配置与 Secrets：数据库、通信、证书、池、并发、排空、已验证模型及额度基线/上限；重启生效。
- MySQL 组织额度：后续准入和槽位获取生效，已有预留与槽位保持原语义。
- MySQL 不可变资产：Prompt、路线、Profile、套件、裁判及策略；冻结评测或发布后按固定版本使用。
- 协议 Schema 与核心安全规则：工程契约，不提供任意文件或通用键值编辑。

运行时资产缺失、摘要损坏或数据库异常均明确失败，不回退文件或“最新版本”。
文件仅用于受控初始化、固定基准和测试。已接单执行及历史回执继续使用原始快照。

## 第一批接口

QuotaManagement: Get / Update / History / Rollback / GetReceipt。QS 组织及操作者来自可信上下文，读使用审计权限，写使用组织管理员权限。

字段：participant（daily_org、daily_user、daily_assessment、active_org、active_user、active_assessment），evaluation（daily_provider_calls、max_active_runs），均为正整数。Get 返回 defaults、ceilings、configured、effective、revision、source、constrained_fields。未配置 revision=0/source=deployment_default；损坏或数据库失败不回退。

Update 是完整替换，携带 command_id、expected_revision、reason、values。Rollback 携带 command_id、expected_revision、target_revision、reason，生成新版本；不删除历史。命令同 ID 不同正文拒绝，回执仅原组织原操作者可读。错误：INVALID_ARGUMENT 非法配置，ABORTED 版本冲突，NOT_FOUND 不存在或不属于当前范围，UNAVAILABLE 依赖失败/结果待核对。

额度修改提交后对后续准入/槽位获取生效；每日预留不重复扣减，已有活跃槽位继续运行。UTC 日界线不变。部署默认值与硬上限初始相同，组织只能调整到上限以内。部署降低上限按项限幅并返回 constrained_fields，不改写历史。

锁顺序：配置写先 participant_admission_locks 后 evaluation_admission_locks；读取有效配置复用业务准入事务，不先锁配置指针再获取业务锁。禁止跨请求额度缓存。

## 裁判 Prompt 草稿契约

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

## 套件及方案派生

套件注册接受可选 `case_edits_json`、`semantic_prompt`、`semantic_owner_organization_id`。
省略时继承来源套件固定绑定。案例只能修订现有生成案例的输入、说明和断言；
不得删除七组案例、减少五候选、修改预检或核心安全断言。
方案 Save 的可选 `evaluation_suite`、`semantic_prompt`、`semantic_owner_organization_id`
只允许共享或当前组织资产；执行策略、门槛和输出契约继承来源，不隐式选择最新版本。

执行/门槛策略通过 AssetCatalog.List/Get 只读访问，类型为 execution_policy、gate_policy。
策略写入仅通过受控初始化，不开放业务修改门槛。

## 初始化与升级

依次执行 `python -m qs_ai.bootstrap.import_evaluation_assets --imported-by <运维标识>`
和 `python -m qs_ai.bootstrap.import_evaluation_suites --imported-by <运维标识>`。
同身份版本内容不同拒绝；相同内容重复执行无副作用。套件导入核对既有 Run，
不一致时回滚，不改写历史证据。初始化不发起评测、不审核、不发布。

迁移 0030 为额度，0031 为策略/裁判资产，0033 为裁判草稿，0034 为套件来源；
0032 和 0035 属于主干其他增量。保留完整迁移账本，回滚应用不得删除新增配置记录。

## 验证边界

已发布批次覆盖 MySQL 8.0/8.4、Go→Python 互操作、组织隔离、并发额度、固定快照、
恢复和幂等回归。增量接口需另行通过这些适用检查及生产部署核证。
管理员验收只创建未发布版本，额度有效值保持不变；当前 v6 发布不受影响。

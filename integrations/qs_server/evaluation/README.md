# 评测初始化资源与固定来源

本目录保存原 QS 来源资产、qs-ai 派生量表套件和自主 MBTI 根版本。源码基线 `c977cb9`；它是初始化与对照材料，MySQL 的精确冻结套件、策略和 Run 才是执行权威。

## 原始来源与实际保留

[manifest.json](manifest.json)保留 QS 提交 `87f9dbea6db8c5a832d788bbb91ee8c257d47bd9` 导出的 v1–v6 完整来源记录，[retained-assets.json](../retained-assets.json)声明实际保留文件。量表保留 v6、v2 语义 Prompt、语义输出 Schema、两份策略 Schema/实例与发布输入派生套件；历史文件不因 manifest 留有来源记录而被重新导入。

[policies.json](policies.json)保留原 definition_json 字节和 fingerprint：执行策略 `release-evaluation-bounded-recovery/v2`、门槛 `release-gates/v2`。策略 Schema v1 与策略实例 v2 是不同版本维度。旧源码导出工具已退役；固定 Go 输出与合法性对照留在测试资源，不再依赖旧生成引擎编译。

可执行 Prompt Markdown/JSON 和案例是摘要绑定资产，不能以文档整理修改原字节。新内容、案例编辑、路线或场景变化须新版本、匹配完整套件与独立 Run，不能继承旧批准。

## 执行与恢复规则

冻结 ExecutionPolicy 保留样本义务、单候选/整轮调用预算、自动/人工恢复名单与开关。白名单和 within_budget 只是规则判断，不能替代 ClassifiedFailure、持久预算预留、未知结果处置或实际审核授权。

ClassifiedFailure 约束 stage/kind/disposition、候选存在性、retryable/result_unknown 及安全证据引用。质量失败保留候选和失败证据；未知结果需显式处置。不能从预算中抹掉失败/未知调用，不能使用外部“人工已授权”布尔值绕过持久审计。

ExecutionCheckpoint 绑定 Run、execution/case/slot/candidate/owner/invocation 与租约时间。prepared 的恢复核对原 invocation/expiry；dispatching 不按此路径重发。数据库 CAS、同一根事务的证据/投影/预算更新与原回执决定唯一持久结果；图编排不取得恢复权。

[EvaluationWorker](../../../src/qs_ai/infrastructure/persistence/mysql/evaluation_worker.py)执行冻结 Run，[评测图](../../../src/qs_ai/infrastructure/workflows/evaluation.py)组织准备、调用、接受节点。数据库事务不跨模型请求。候选模式由新 Run 的创建记录冻结，部署调整不转换已有 Run；共享容量、取消排空与原 budget 仍须原版本规则核对。

## 发布执行输入契约套件

[qs-ai-published-input-cases-v1.json](qs-ai-published-input-cases-v1.json)为 qs-ai 自主派生套件，身份 `cross-dimension-participant-scale-v6-published / qs-ai-evaluation-cases/v1`。derived_from 固定原 v6，input_contract 固定 `qs-published-snapshot-v1` 与原输入规范版本/摘要；不覆盖原 QS 文件。

原 v6 的七组生成案例、每组五候选、一个预检，以及 Profile/Prompt/全部断言保持。新增可追溯的输入构造版本会改变 release fingerprint，必须新建 Run，重新生成、语义评测、双职责审核并满足完整 G1–G5。旧批准不能自动升级为新输入版本批准。

生成前按冻结 Schema 校验 context/facts 投影，不虚构 QS report/source 身份。预检故意违反维度下限，要证明 provider_call_count=0，不能为通过 Schema 而删减拒绝义务。注册/导入不会调用模型、批准 Run 或修改发布指针。

## 原生资产与 MBTI

治理接口可注册新 Prompt/Profile/完整套件，案例修订必须保留完整义务和核心安全断言；裁判 Prompt 与策略使用精确版本。MySQL 资产缺失或损坏拒绝，不从本目录选择 latest 或旧基线兜底。

[MBTI 报告解读根](mbti/manifest.json)和[三主题根](mbti-themes/README.md)是 qs-ai 自主契约，适用于精确 `MBTI_OEJTS / v64-report-202608-v1`。报告解读与三主题套件不自动互换，不回落量表配置。根导入不产生审核、发布或业务请求。

治理语义见 [治理接口](../../../docs/04-接口与运维/03-治理接口与可信委托.md)，受控初始化与分层验收见 [MBTI 操作](../../../docs/04-接口与运维/08-MBTI管理与验收.md)。测试对照、合成审核、资源导入、CI 或健康服务均不证明真实模型质量、实际参与者结果或正式设备展示。
